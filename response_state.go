package fasthttp

import (
	"errors"
	"sync"
	"sync/atomic"
)

// responseStatus is the single lifecycle state used by ordinary buffered
// responses, streaming body writes, flushes and connection hijacking.
//
// A response moves forward through the non-terminal states at most once.
// Failed and hijacked are terminal: every later write observes the same stored
// outcome instead of attempting to finish the HTTP response a second time.
type responseStatus uint32

const (
	responseStatusOpen responseStatus = iota
	responseStatusStarted
	responseStatusStreaming
	responseStatusEncoded
	responseStatusClosing
	responseStatusFailed
	responseStatusHijacked
	responseStatusDone
)

// responseOutcome is the exactly-once result recorded when a response ends.
type responseOutcome uint8

const (
	responseOutcomeNone responseOutcome = iota
	responseOutcomeCompleted
	responseOutcomeFailed
	responseOutcomeHijacked
)

// responseState coordinates the one terminal outcome of a server response.
//
// Its zero value is a valid open state. reset prepares the embedded value for
// the next pooled request context.
type responseState struct {
	// progress is the real body/write progress recorded by the serializer.
	// Keep 64-bit atomics first for 32-bit alignment.
	progress atomic.Int64

	// status is read on every body-write iteration without taking mu.
	status atomic.Uint32

	mu sync.Mutex
	// The following fields are guarded by mu.
	err        error
	outcome    responseOutcome
	outcomeErr error
	recorded   bool
	// outcomeCount exists to make the exactly-once terminal transition
	// observable in race and lifecycle tests.
	outcomeCount int
}

// reset returns the tracker to its zero-value state. The server calls this only
// after no response-writing goroutine owns the context anymore.
func (s *responseState) reset() {
	s.status.Store(uint32(responseStatusOpen))
	s.progress.Store(0)

	s.mu.Lock()
	s.err = nil
	s.outcome = responseOutcomeNone
	s.outcomeErr = nil
	s.recorded = false
	s.outcomeCount = 0
	s.mu.Unlock()
}

// beginResponse marks the start of Response serialization. A shutdown that
// started before a new response was admitted wins: in-flight streams have
// usually already moved past this state and are left running.
func (s *responseState) beginResponse() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch responseStatus(s.status.Load()) {
	case responseStatusFailed:
		return s.err
	case responseStatusHijacked:
		return errHijacked
	case responseStatusClosing:
		return errResponseClosing
	case responseStatusOpen:
		s.status.Store(uint32(responseStatusStarted))
		return nil
	default:
		// A repeated serialization is a bug in the server loop, but make the
		// state machine idempotent rather than writing two HTTP endings.
		return s.terminalWriteLocked()
	}
}

// beginStreaming records that response headers are being committed for a body
// stream. This is the point after which an error cannot be replaced by an
// ordinary error response.
func (s *responseState) beginStreaming() {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch responseStatus(s.status.Load()) {
	case responseStatusOpen, responseStatusStarted, responseStatusEncoded:
		s.status.Store(uint32(responseStatusStreaming))
	}
}

// beginClosing prevents a fresh response from starting during graceful
// shutdown. It deliberately does not interrupt a response that has already
// started or begun streaming.
func (s *responseState) beginClosing() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if responseStatus(s.status.Load()) == responseStatusOpen {
		s.status.Store(uint32(responseStatusClosing))
	}
}

// canWrite reports whether another body write/flush is permitted. On failure it
// returns the first stored connection error so every later write is identical.
func (s *responseState) canWrite() (bool, error) {
	switch responseStatus(s.status.Load()) {
	case responseStatusFailed:
		s.mu.Lock()
		err := s.err
		s.mu.Unlock()
		return false, err
	case responseStatusHijacked:
		return false, errHijacked
	case responseStatusDone:
		return false, errResponseDone
	default:
		return true, nil
	}
}

// recordProgress adds bytes that have made real progress through the response
// writer. Progress is retained even when the same write later supplies the
// terminal error.
func (s *responseState) recordProgress(n int64) {
	if n > 0 {
		s.progress.Add(n)
	}
}

// Progress returns the recorded write progress.
func (s *responseState) Progress() int64 {
	return s.progress.Load()
}

// complete marks response serialization as complete. The final network flush
// may still fail and move the state to failed.
func (s *responseState) complete() {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch responseStatus(s.status.Load()) {
	case responseStatusStarted, responseStatusStreaming:
		s.status.Store(uint32(responseStatusEncoded))
	}
}

// finish records the final successful flush exactly once.
func (s *responseState) finish() {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch responseStatus(s.status.Load()) {
	case responseStatusFailed, responseStatusHijacked, responseStatusDone:
		return
	}
	s.status.Store(uint32(responseStatusDone))
	s.recordOutcomeLocked(responseOutcomeCompleted, nil)
}

// fail stores the first terminal write error. Concurrent failures collapse to
// that same error; a response that was hijacked first reports errHijacked.
func (s *responseState) fail(err error) error {
	if err == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	switch responseStatus(s.status.Load()) {
	case responseStatusFailed:
		if s.err != nil {
			return s.err
		}
		s.err = err
		s.recordOutcomeLocked(responseOutcomeFailed, err)
		return err
	case responseStatusHijacked:
		return errHijacked
	case responseStatusDone:
		// A successful terminal result was already recorded. Do not replace
		// it with a late teardown error.
		return nil
	default:
		s.err = err
		s.status.Store(uint32(responseStatusFailed))
		s.recordOutcomeLocked(responseOutcomeFailed, err)
		return err
	}
}

// hijack commits the terminal transition to handler ownership. After it
// succeeds the server must not write an HTTP ending or error response.
func (s *responseState) hijack() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch responseStatus(s.status.Load()) {
	case responseStatusFailed:
		return s.err
	case responseStatusHijacked:
		return errHijacked
	case responseStatusDone:
		return errResponseDone
	default:
		s.status.Store(uint32(responseStatusHijacked))
		s.recordOutcomeLocked(responseOutcomeHijacked, nil)
		return nil
	}
}

// isTerminal reports whether error-response fallback must be suppressed.
func (s *responseState) isTerminal() bool {
	switch responseStatus(s.status.Load()) {
	case responseStatusFailed, responseStatusHijacked, responseStatusClosing, responseStatusDone:
		return true
	default:
		return false
	}
}

// Err returns the first terminal connection error, if any.
func (s *responseState) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Status returns the current state. It is primarily useful in focused tests.
func (s *responseState) Status() responseStatus {
	return responseStatus(s.status.Load())
}

// Result returns the exactly-once terminal outcome. Before termination ok is
// false.
func (s *responseState) Result() (outcome responseOutcome, err error, progress int64, ok bool) {
	s.mu.Lock()
	outcome = s.outcome
	err = s.outcomeErr
	ok = s.recorded
	s.mu.Unlock()
	progress = s.progress.Load()
	return
}

func (s *responseState) recordOutcomeLocked(outcome responseOutcome, err error) {
	if s.recorded {
		return
	}
	s.recorded = true
	s.outcomeCount++
	s.outcome = outcome
	s.outcomeErr = err
}

func (s *responseState) terminalWriteLocked() error {
	switch responseStatus(s.status.Load()) {
	case responseStatusFailed:
		return s.err
	case responseStatusHijacked:
		return errHijacked
	case responseStatusDone:
		return errResponseDone
	case responseStatusClosing:
		return errResponseClosing
	default:
		return nil
	}
}

var (
	errResponseClosing = errors.New("fasthttp: server is closing; response not started")
	errResponseDone    = errors.New("fasthttp: response already finished")
)
