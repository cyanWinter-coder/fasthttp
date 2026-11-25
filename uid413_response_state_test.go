package fasthttp

import (
	"errors"
	"sync"
	"testing"
)

func TestUID413ResponseStateLifecycle(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("connection reset by peer")
	s := &responseState{}

	if err := s.beginResponse(); err != nil {
		t.Fatalf("beginResponse() = %v", err)
	}
	s.beginStreaming()
	if ok, err := s.canWrite(); !ok || err != nil {
		t.Fatalf("canWrite() = %v, %v; want true, nil", ok, err)
	}

	s.recordProgress(3)
	s.recordProgress(2)
	if got := s.Progress(); got != 5 {
		t.Fatalf("Progress() = %d; want 5", got)
	}

	if got := s.fail(wantErr); !errors.Is(got, wantErr) {
		t.Fatalf("fail() = %v; want %v", got, wantErr)
	}
	if got := s.fail(errors.New("second error")); !errors.Is(got, wantErr) {
		t.Fatalf("second fail() = %v; want original %v", got, wantErr)
	}
	if ok, err := s.canWrite(); ok || !errors.Is(err, wantErr) {
		t.Fatalf("canWrite() after failure = %v, %v; want false, %v", ok, err, wantErr)
	}
	if err := s.beginResponse(); !errors.Is(err, wantErr) {
		t.Fatalf("beginResponse() after failure = %v; want %v", err, wantErr)
	}
	if err := s.hijack(); !errors.Is(err, wantErr) {
		t.Fatalf("hijack() after failure = %v; want %v", err, wantErr)
	}
	if got := s.Progress(); got != 5 {
		t.Fatalf("Progress() after failure = %d; want real progress 5", got)
	}

	outcome, outcomeErr, progress, ok := s.Result()
	if !ok || outcome != responseOutcomeFailed || !errors.Is(outcomeErr, wantErr) || progress != 5 {
		t.Fatalf("Result() = %v, %v, %d, %v", outcome, outcomeErr, progress, ok)
	}

	// A terminal result cannot be replaced by a later successful finish.
	s.finish()
	outcome, outcomeErr, _, _ = s.Result()
	if outcome != responseOutcomeFailed || !errors.Is(outcomeErr, wantErr) {
		t.Fatalf("finish overwrote failed result: %v, %v", outcome, outcomeErr)
	}

	s.reset()
	if status := s.Status(); status != responseStatusOpen {
		t.Fatalf("Status() after reset = %v; want open", status)
	}
	if err := s.beginResponse(); err != nil {
		t.Fatalf("beginResponse() after reset = %v", err)
	}
}

func TestUID413ResponseStateHijackTerminal(t *testing.T) {
	t.Parallel()

	s := &responseState{}
	if err := s.beginResponse(); err != nil {
		t.Fatal(err)
	}
	if err := s.hijack(); err != nil {
		t.Fatalf("hijack() = %v", err)
	}
	if err := s.hijack(); !errors.Is(err, errHijacked) {
		t.Fatalf("second hijack() = %v; want %v", err, errHijacked)
	}
	if ok, err := s.canWrite(); ok || !errors.Is(err, errHijacked) {
		t.Fatalf("canWrite() = %v, %v; want false, %v", ok, err, errHijacked)
	}
	if got := s.fail(errors.New("late flush error")); !errors.Is(got, errHijacked) {
		t.Fatalf("fail() after hijack = %v; want %v", got, errHijacked)
	}
	if !s.isTerminal() {
		t.Fatal("isTerminal() = false after hijack")
	}
	outcome, _, _, ok := s.Result()
	if !ok || outcome != responseOutcomeHijacked {
		t.Fatalf("Result() = %v, ok=%v; want hijacked", outcome, ok)
	}
}

func TestUID413ResponseStateClosingDoesNotInterruptStartedStream(t *testing.T) {
	t.Parallel()

	s := &responseState{}
	s.beginStreaming()
	s.beginClosing()
	if ok, err := s.canWrite(); !ok || err != nil {
		t.Fatalf("started stream blocked by closing: %v, %v", ok, err)
	}
	if err := s.hijack(); err != nil {
		t.Fatalf("hijack of started/closing response = %v", err)
	}

	fresh := &responseState{}
	fresh.beginClosing()
	if err := fresh.beginResponse(); !errors.Is(err, errResponseClosing) {
		t.Fatalf("new response during closing = %v; want %v", err, errResponseClosing)
	}
	if !fresh.isTerminal() {
		t.Fatal("closing state must suppress a fallback response")
	}
}

func TestUID413ResponseStateConcurrentOutcomeRecordedOnce(t *testing.T) {
	t.Parallel()

	const goroutines = 64
	wantErr := errors.New("client disconnected")

	for range 20 {
		s := &responseState{}
		start := make(chan struct{})
		var wg sync.WaitGroup
		errs := make(chan error, goroutines)

		for i := range goroutines {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				if i%2 == 0 {
					errs <- s.fail(wantErr)
				} else {
					errs <- s.hijack()
				}
			}(i)
		}
		close(start)
		wg.Wait()
		close(errs)

		var hijackWinners int
		for err := range errs {
			switch {
			case err == nil:
				hijackWinners++
			case errors.Is(err, wantErr), errors.Is(err, errHijacked):
				// Terminal losers return either the stored write error or the
				// hijack sentinel depending on when they arrived.
			default:
				t.Fatalf("unexpected state transition error: %v", err)
			}
		}

		status := s.Status()
		outcome, outcomeErr, _, recorded := s.Result()
		if !recorded {
			t.Fatal("terminal result was not recorded")
		}
		s.mu.Lock()
		outcomeCount := s.outcomeCount
		s.mu.Unlock()
		if outcomeCount != 1 {
			t.Fatalf("outcome recorded %d times; want exactly once", outcomeCount)
		}
		switch status {
		case responseStatusFailed:
			if outcome != responseOutcomeFailed || !errors.Is(outcomeErr, wantErr) {
				t.Fatalf("failed status has result %v/%v", outcome, outcomeErr)
			}
			if !errors.Is(s.Err(), wantErr) {
				t.Fatalf("Err() = %v; want %v", s.Err(), wantErr)
			}
			if hijackWinners != 0 {
				t.Fatalf("%d hijacks succeeded after fail won", hijackWinners)
			}
		case responseStatusHijacked:
			if outcome != responseOutcomeHijacked {
				t.Fatalf("hijacked status has outcome %v", outcome)
			}
		default:
			t.Fatalf("status = %v; want failed or hijacked", status)
		}
	}
}

func TestUID413ResponseStateRequestCtxWritesReturnStoredError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("write pipe broken")
	ctx := &RequestCtx{}
	if _, err := ctx.Write([]byte("buffered before failure")); err != nil {
		t.Fatalf("Write before terminal failure: %v", err)
	}

	ctx.response.recordProgress(7)
	if err := ctx.response.fail(wantErr); !errors.Is(err, wantErr) {
		t.Fatalf("fail() = %v; want %v", err, wantErr)
	}

	for _, write := range []func() (int, error){
		func() (int, error) { return ctx.Write([]byte("after failure")) },
		func() (int, error) { return ctx.WriteString("after failure") },
	} {
		n, err := write()
		if n != 0 || !errors.Is(err, wantErr) {
			t.Fatalf("terminal Write = (%d, %v); want (0, %v)", n, err, wantErr)
		}
	}
	if got := ctx.Response.Body(); string(got) != "buffered before failure" {
		t.Fatalf("body = %q; late terminal writes must not append", got)
	}
}

func TestUID413ResponseStateConcurrentFailureAndCompletionRecordOnce(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("client disconnected")
	for range 30 {
		s := &responseState{}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range 64 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				if i%2 == 0 {
					_ = s.fail(wantErr)
				} else {
					s.finish()
				}
			}(i)
		}
		close(start)
		wg.Wait()

		outcome, outcomeErr, _, recorded := s.Result()
		if !recorded {
			t.Fatal("handler return/disconnect result was not recorded")
		}
		s.mu.Lock()
		count := s.outcomeCount
		s.mu.Unlock()
		if count != 1 {
			t.Fatalf("outcome count = %d; want 1", count)
		}
		switch outcome {
		case responseOutcomeCompleted:
			if outcomeErr != nil {
				t.Fatalf("completed outcome has error %v", outcomeErr)
			}
		case responseOutcomeFailed:
			if !errors.Is(outcomeErr, wantErr) {
				t.Fatalf("failed outcome error = %v; want %v", outcomeErr, wantErr)
			}
		default:
			t.Fatalf("outcome = %v; want completed or failed", outcome)
		}
	}
}

func TestUID413ResponseStateConcurrentProgressAndFailureRace(t *testing.T) {
	t.Parallel()

	s := &responseState{}
	wantErr := errors.New("broken connection")
	start := make(chan struct{})
	var wg sync.WaitGroup

	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 100 {
				s.recordProgress(1)
				_, _ = s.canWrite()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		_ = s.fail(wantErr)
	}()

	close(start)
	wg.Wait()

	if !errors.Is(s.Err(), wantErr) {
		t.Fatalf("Err() = %v; want %v", s.Err(), wantErr)
	}
	if got := s.Progress(); got < 0 || got > 32*100 {
		t.Fatalf("Progress() = %d outside recorded range", got)
	}
}
