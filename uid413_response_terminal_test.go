package fasthttp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/valyala/fasthttp/fasthttputil"
)

func uid413Await[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(testTimeout(10 * time.Second)):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func uid413StartServer(t *testing.T, s *Server) (*fasthttputil.InmemoryListener, <-chan error) {
	t.Helper()
	ln := fasthttputil.NewInmemoryListener()
	serveErr := make(chan error, 1)
	go func() { serveErr <- s.Serve(ln) }()
	t.Cleanup(func() {
		_ = ln.Close()
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout(5*time.Second))
		defer cancel()
		_ = s.ShutdownWithContext(ctx)
	})
	return ln, serveErr
}

// After a streamed write hits a disconnected client, every following write on
// the same stream writer must observe the first connection error. The producer
// goroutine and server connection must both exit so Shutdown can reclaim them.
func TestUID413ResponseTerminalDisconnectKeepsSameErrorAndReleasesStream(t *testing.T) {
	t.Parallel()

	var producers sync.WaitGroup
	producers.Add(1)
	firstFlush := make(chan struct{})
	writeErrors := make(chan [2]error, 1)

	s := &Server{
		Logger: &testLogger{},
		Handler: func(ctx *RequestCtx) {
			ctx.SetBodyStreamWriter(func(w *bufio.Writer) {
				defer producers.Done()

				if _, err := w.WriteString("first-committed-frame"); err != nil {
					writeErrors <- [2]error{err, err}
					return
				}
				if err := w.Flush(); err != nil {
					writeErrors <- [2]error{err, err}
					return
				}
				close(firstFlush)

				payload := bytes.Repeat([]byte("terminal-state-progress-"), 4096)
				var firstErr error
				for range 1000 {
					if _, err := w.Write(payload); err != nil {
						firstErr = err
						break
					}
					if err := w.Flush(); err != nil {
						firstErr = err
						break
					}
				}
				if firstErr == nil {
					writeErrors <- [2]error{nil, nil}
					return
				}

				_, secondWriteErr := w.Write([]byte("write after terminal failure"))
				secondFlushErr := w.Flush()
				secondErr := secondWriteErr
				if secondErr == nil {
					secondErr = secondFlushErr
				}
				writeErrors <- [2]error{firstErr, secondErr}
			})
		},
	}

	ln, serveErr := uid413StartServer(t, s)
	client, err := ln.Dial()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.SetDeadline(time.Now().Add(testTimeout(10 * time.Second))); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: terminal.test\r\n\r\n")); err != nil {
		t.Fatal(err)
	}

	uid413Await(t, firstFlush, "first stream flush")
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	errs := uid413Await(t, writeErrors, "stream write errors")
	if errs[0] == nil {
		t.Fatal("stream kept writing after the client closed")
	}
	if errs[1] == nil {
		t.Fatal("write after the first failure succeeded; want the stored connection error")
	}
	if !errors.Is(errs[1], errs[0]) {
		t.Fatalf("later write error = %v; want same terminal error %v", errs[1], errs[0])
	}

	producerDone := make(chan struct{})
	go func() { producers.Wait(); close(producerDone) }()
	uid413Await(t, producerDone, "stream producer exit")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), testTimeout(5*time.Second))
	defer cancel()
	if err := s.ShutdownWithContext(shutdownCtx); err != nil {
		t.Fatalf("Shutdown after disconnect: %v", err)
	}
	if err := uid413Await(t, serveErr, "Serve return"); err != nil {
		t.Fatalf("Serve: %v", err)
	}
}

// Hijack with no pre-response hands exactly the raw bytes written by the
// hijacker to the client; the server must not append a chunk/body ending.
func TestUID413ResponseTerminalHijackWritesNoHTTPEnding(t *testing.T) {
	t.Parallel()

	const raw = "RAW-HIJACK-PAYLOAD"
	hijackReturned := make(chan struct{})
	hijackState := make(chan struct{}, 1)

	s := &Server{
		Logger: &testLogger{},
		ConnState: func(_ net.Conn, state ConnState) {
			if state == StateHijacked {
				select {
				case hijackState <- struct{}{}:
				default:
				}
			}
		},
		Handler: func(ctx *RequestCtx) {
			ctx.HijackSetNoResponse(true)
			ctx.Hijack(func(c net.Conn) {
				defer close(hijackReturned)
				_, _ = c.Write([]byte(raw))
			})
		},
	}

	ln, serveErr := uid413StartServer(t, s)
	client, err := ln.Dial()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.SetDeadline(time.Now().Add(testTimeout(10 * time.Second))); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: terminal.test\r\n\r\n")); err != nil {
		t.Fatal(err)
	}

	uid413Await(t, hijackState, "StateHijacked")
	uid413Await(t, hijackReturned, "hijack handler return")

	got, err := io.ReadAll(client)
	if err != nil {
		t.Fatalf("ReadAll hijacked conn: %v", err)
	}
	if string(got) != raw {
		t.Fatalf("hijacked stream = %q; want exactly %q (no HTTP ending)", got, raw)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), testTimeout(5*time.Second))
	defer cancel()
	if err := s.ShutdownWithContext(shutdownCtx); err != nil {
		t.Fatalf("Shutdown after hijack: %v", err)
	}
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(testTimeout(2 * time.Second)):
		t.Fatal("Serve did not return after listener close")
	}
}

// A stream already running when Shutdown starts may finish and is waited for
// within the Shutdown context deadline.
func TestUID413ResponseTerminalShutdownAllowsCommittedStreamToFinish(t *testing.T) {
	t.Parallel()

	firstFlush := make(chan struct{})
	releaseStream := make(chan struct{})
	streamFinished := make(chan struct{})

	s := &Server{
		Logger: &testLogger{},
		Handler: func(ctx *RequestCtx) {
			ctx.SetBodyStreamWriter(func(w *bufio.Writer) {
				if _, err := w.WriteString("first\n"); err != nil {
					t.Errorf("first write: %v", err)
					return
				}
				if err := w.Flush(); err != nil {
					t.Errorf("first flush: %v", err)
					return
				}
				close(firstFlush)
				<-releaseStream
				if _, err := w.WriteString("last\n"); err != nil {
					t.Errorf("last write: %v", err)
					return
				}
				if err := w.Flush(); err != nil {
					t.Errorf("last flush: %v", err)
					return
				}
				close(streamFinished)
			})
		},
	}

	ln, serveErr := uid413StartServer(t, s)
	client, err := ln.Dial()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.SetDeadline(time.Now().Add(testTimeout(10 * time.Second))); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: terminal.test\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	uid413Await(t, firstFlush, "committed first frame")

	shutdownErr := make(chan error, 1)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), testTimeout(5*time.Second))
	defer cancel()
	go func() { shutdownErr <- s.ShutdownWithContext(shutdownCtx) }()

	deadline := time.Now().Add(testTimeout(2 * time.Second))
	for s.stop.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.stop.Load() != 1 {
		t.Fatal("Shutdown did not enter stopping state")
	}

	close(releaseStream)
	uid413Await(t, streamFinished, "committed stream completion")
	if err := uid413Await(t, shutdownErr, "Shutdown completion"); err != nil {
		t.Fatalf("Shutdown before deadline: %v", err)
	}

	body, err := io.ReadAll(client)
	if err != nil {
		t.Fatalf("read completed stream: %v", err)
	}
	if !bytes.Contains(body, []byte("first")) || !bytes.Contains(body, []byte("last")) {
		t.Fatalf("response = %q; want both committed frames", body)
	}

	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(testTimeout(2 * time.Second)):
		t.Fatal("Serve did not return")
	}
}

// If the deadline expires first, Shutdown reports context.DeadlineExceeded;
// releasing the stuck producer afterwards must let a second Shutdown reclaim
// the connection.
func TestUID413ResponseTerminalShutdownDeadlineThenRelease(t *testing.T) {
	t.Parallel()

	firstFlush := make(chan struct{})
	releaseStream := make(chan struct{})
	var producers sync.WaitGroup
	producers.Add(1)

	s := &Server{
		Logger: &testLogger{},
		Handler: func(ctx *RequestCtx) {
			ctx.SetBodyStreamWriter(func(w *bufio.Writer) {
				defer producers.Done()
				_, _ = w.WriteString("first\n")
				_ = w.Flush()
				close(firstFlush)
				<-releaseStream
			})
		},
	}

	ln, _ := uid413StartServer(t, s)
	client, err := ln.Dial()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: terminal.test\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	uid413Await(t, firstFlush, "first frame")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.ShutdownWithContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v; want context.DeadlineExceeded", err)
	}

	close(releaseStream)
	producerDone := make(chan struct{})
	go func() { producers.Wait(); close(producerDone) }()
	uid413Await(t, producerDone, "stuck producer exit")

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), testTimeout(5*time.Second))
	defer cleanupCancel()
	if err := s.ShutdownWithContext(cleanupCtx); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
}

type uid413PartialWriteFlusher struct {
	n   int
	err error
}

func (w *uid413PartialWriteFlusher) Write([]byte) (int, error) { return w.n, w.err }
func (w *uid413PartialWriteFlusher) Flush() error              { return nil }

func TestUID413FlushWriterKeepsValidPartialProgress(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("partial connection write")
	fw := flushWriter{
		wf: &uid413PartialWriteFlusher{n: 3, err: wantErr},
		bw: bufio.NewWriter(io.Discard),
	}
	n, err := fw.Write(make([]byte, 5))
	if n != 3 || !errors.Is(err, wantErr) {
		t.Fatalf("Write() = (%d, %v); want (3, %v)", n, err, wantErr)
	}

	// A writer returning an impossible stale count does not get to call that
	// progress; existing normalization to zero remains.
	fw.wf = &uid413PartialWriteFlusher{n: 64, err: wantErr}
	n, err = fw.Write(make([]byte, 5))
	if n != 0 || !errors.Is(err, wantErr) {
		t.Fatalf("invalid partial Write() = (%d, %v); want (0, %v)", n, err, wantErr)
	}
}

type uid413CloseCounter struct {
	closed chan struct{}
	once   sync.Once
}

func (c *uid413CloseCounter) Read([]byte) (int, error) { return 0, io.EOF }
func (c *uid413CloseCounter) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func TestUID413WriteResponseTerminalStateDoesNotStartAgain(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("same connection error")
	ctx := &RequestCtx{}
	if err := ctx.response.fail(wantErr); !errors.Is(err, wantErr) {
		t.Fatalf("fail() = %v; want %v", err, wantErr)
	}

	err := writeResponse(ctx, bufio.NewWriter(io.Discard))
	if !errors.Is(err, wantErr) {
		t.Fatalf("writeResponse after fail = %v; want %v", err, wantErr)
	}
}

func TestUID413WriteResponseClosingClosesAbandonedBodyStream(t *testing.T) {
	t.Parallel()

	body := &uid413CloseCounter{closed: make(chan struct{})}
	ctx := &RequestCtx{}
	ctx.Response.SetBodyStream(body, -1)
	ctx.response.beginClosing()

	err := writeResponse(ctx, bufio.NewWriter(io.Discard))
	if !errors.Is(err, errResponseClosing) {
		t.Fatalf("writeResponse during closing = %v; want %v", err, errResponseClosing)
	}
	select {
	case <-body.closed:
	case <-time.After(time.Second):
		t.Fatal("body stream was not closed when response start was rejected")
	}
}

// Once shutdown has started, a pipelined follow-up request must not produce a
// second response after the in-flight response finishes.
func TestUID413ResponseTerminalShutdownDoesNotStartQueuedResponse(t *testing.T) {
	t.Parallel()

	firstInHandler := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{}, 1)

	s := &Server{
		Logger: &testLogger{},
		Handler: func(ctx *RequestCtx) {
			if bytes.Equal(ctx.Path(), []byte("/one")) {
				close(firstInHandler)
				<-releaseFirst
				_, _ = ctx.WriteString("FIRST-BODY")
				return
			}
			select {
			case secondStarted <- struct{}{}:
			default:
			}
			_, _ = ctx.WriteString("SECOND-BODY")
		},
	}

	ln, serveErr := uid413StartServer(t, s)
	client, err := ln.Dial()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.SetDeadline(time.Now().Add(testTimeout(10 * time.Second))); err != nil {
		t.Fatal(err)
	}
	req := "GET /one HTTP/1.1\r\nHost: terminal.test\r\n\r\n" +
		"GET /two HTTP/1.1\r\nHost: terminal.test\r\n\r\n"
	if _, err := client.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	uid413Await(t, firstInHandler, "first handler")

	shutdownErr := make(chan error, 1)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), testTimeout(5*time.Second))
	defer cancel()
	go func() { shutdownErr <- s.ShutdownWithContext(shutdownCtx) }()

	deadline := time.Now().Add(testTimeout(2 * time.Second))
	for s.stop.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.stop.Load() != 1 {
		t.Fatal("Shutdown did not enter stopping state")
	}
	close(releaseFirst)

	data, err := io.ReadAll(client)
	if err != nil {
		t.Fatalf("read first response: %v", err)
	}
	if !bytes.Contains(data, []byte("FIRST-BODY")) {
		t.Fatalf("response = %q; want first response", data)
	}
	if bytes.Contains(data, []byte("SECOND-BODY")) {
		t.Fatalf("response = %q; queued second response was started", data)
	}
	select {
	case <-secondStarted:
		t.Fatal("queued second handler ran during shutdown")
	default:
	}

	if err := uid413Await(t, shutdownErr, "Shutdown"); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(testTimeout(2 * time.Second)):
		t.Fatal("Serve did not return")
	}
}

// Concurrent abrupt disconnects and handler returns must not panic or leave
// stream producer goroutines behind. Run with -race as well.
func TestUID413ResponseTerminalConcurrentDisconnectAndReturn(t *testing.T) {
	t.Parallel()

	const n = 32
	var producers sync.WaitGroup
	producers.Add(n)
	release := make(chan struct{})

	s := &Server{
		Logger: &testLogger{},
		Handler: func(ctx *RequestCtx) {
			waitHandler := bytes.Equal(ctx.Path(), []byte("/handler"))
			ctx.SetBodyStreamWriter(func(w *bufio.Writer) {
				defer producers.Done()
				_, _ = w.Write([]byte("start\n"))
				_ = w.Flush()
				if !waitHandler {
					<-release
				}
				payload := bytes.Repeat([]byte("x"), 16*1024)
				for range 100 {
					if _, err := w.Write(payload); err != nil {
						return
					}
					if err := w.Flush(); err != nil {
						return
					}
				}
			})
			if waitHandler {
				<-release
			}
		},
	}

	ln, _ := uid413StartServer(t, s)
	clients := make([]net.Conn, 0, n)
	for i := range n {
		c, err := ln.Dial()
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, c)
		path := "/stream"
		if i%2 == 0 {
			path = "/handler"
		}
		req := "GET " + path + " HTTP/1.1\r\nHost: terminal.test\r\n\r\n"
		if _, err := c.Write([]byte(req)); err != nil {
			t.Fatal(err)
		}
	}

	// Let handlers/producers reach their gates, then disconnect every client
	// and release every gated handler concurrently.
	time.Sleep(20 * time.Millisecond)
	var closers sync.WaitGroup
	closers.Add(n)
	for _, c := range clients {
		go func(c net.Conn) {
			defer closers.Done()
			_ = c.Close()
		}(c)
	}
	close(release)
	closers.Wait()

	producerDone := make(chan struct{})
	go func() { producers.Wait(); close(producerDone) }()
	uid413Await(t, producerDone, "all stream producers exit")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout(10*time.Second))
	defer cancel()
	if err := s.ShutdownWithContext(ctx); err != nil {
		t.Fatalf("Shutdown after concurrent disconnects: %v", err)
	}
}
