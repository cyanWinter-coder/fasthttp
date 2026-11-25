package fasthttp

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valyala/fasthttp/fasthttputil"
)

func waitForInFlightCount(t *testing.T, s *Server, want int64) {
	t.Helper()

	deadline := time.Now().Add(testTimeout(2 * time.Second))
	for time.Now().Before(deadline) {
		if got := s.GetInFlightRequestsCount(); got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("in-flight requests = %d, want %d", s.GetInFlightRequestsCount(), want)
}

func TestServerBoundedDrainClosesLongRequest(t *testing.T) {
	t.Parallel()

	ln := fasthttputil.NewInmemoryListener()
	handlerEntered := make(chan struct{})
	releaseHandler := make(chan struct{})
	writeErrCh := make(chan error, 1)
	var cancellationErr atomic.Value

	s := &Server{
		Handler: func(ctx *RequestCtx) {
			close(handlerEntered)
			<-releaseHandler

			select {
			case <-ctx.Done():
				cancellationErr.Store(ctx.Err())
			default:
			}

			_, err := ctx.Conn().Write([]byte("response after drain deadline"))
			writeErrCh <- err
		},
		Logger: &testLogger{},
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- s.Serve(ln) }()

	clientConn, err := ln.Dial()
	if err != nil {
		t.Fatalf("Dial() error: %v", err)
	}
	defer clientConn.Close()

	if _, err := clientConn.Write([]byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")); err != nil {
		t.Fatalf("client Write() error: %v", err)
	}

	select {
	case <-handlerEntered:
	case <-time.After(testTimeout(time.Second)):
		t.Fatal("handler did not start")
	}
	waitForInFlightCount(t, s, 1)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), testTimeout(150*time.Millisecond))
	defer cancel()

	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- s.ShutdownWithContext(shutdownCtx) }()

	select {
	case err := <-shutdownErr:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("ShutdownWithContext() error = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(testTimeout(2 * time.Second)):
		t.Fatal("bounded ShutdownWithContext() did not return at its deadline")
	}

	// The same lifecycle must publish the same terminal result to repeated
	// callers instead of returning nil once the listener map is gone.
	if err := s.ShutdownWithContext(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second ShutdownWithContext() error = %v, want context.DeadlineExceeded", err)
	}

	clientEnded := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, clientConn)
		close(clientEnded)
	}()

	close(releaseHandler)

	select {
	case err := <-writeErrCh:
		if err == nil {
			t.Fatal("handler write after forced close succeeded; want a write error")
		}
	case <-time.After(testTimeout(2 * time.Second)):
		t.Fatal("handler did not observe the closed connection")
	}

	if err, _ := cancellationErr.Load().(error); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("RequestCtx.Err() = %v, want context.Canceled", err)
	}

	select {
	case <-clientEnded:
	case <-time.After(testTimeout(2 * time.Second)):
		t.Fatal("client connection was not closed after the drain deadline")
	}

	waitForInFlightCount(t, s, 0)

	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve() error: %v", err)
		}
	case <-time.After(testTimeout(2 * time.Second)):
		t.Fatal("Serve() did not stop")
	}
}

func TestServerDrainClosesIdleKeepAliveConnection(t *testing.T) {
	t.Parallel()

	ln := fasthttputil.NewInmemoryListener()
	s := &Server{
		Handler: func(ctx *RequestCtx) {
			ctx.Success("text/plain", []byte("ok"))
		},
		Logger: &testLogger{},
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- s.Serve(ln) }()

	conn, err := ln.Dial()
	if err != nil {
		t.Fatalf("Dial() error: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")); err != nil {
		t.Fatalf("Write() error: %v", err)
	}
	var resp Response
	if err := resp.Read(bufio.NewReader(conn)); err != nil {
		t.Fatalf("first response Read() error: %v", err)
	}
	if resp.StatusCode() != StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode(), StatusOK)
	}
	waitForInFlightCount(t, s, 0)

	shutdownDone := make(chan error, 1)
	start := time.Now()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout(5*time.Second))
		defer cancel()
		shutdownDone <- s.ShutdownWithContext(ctx)
	}()

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("ShutdownWithContext() error: %v", err)
		}
		if elapsed := time.Since(start); elapsed > testTimeout(2*time.Second) {
			t.Fatalf("idle keep-alive connection took %v to drain", elapsed)
		}
	case <-time.After(testTimeout(3 * time.Second)):
		t.Fatal("ShutdownWithContext() did not close the idle keep-alive connection")
	}

	oneByte := make([]byte, 1)
	if _, err := conn.Read(oneByte); err == nil {
		t.Fatal("idle keep-alive connection remained readable after Shutdown")
	}

	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve() error: %v", err)
		}
	case <-time.After(testTimeout(time.Second)):
		t.Fatal("Serve() did not stop")
	}
}

func TestServerDrainResponseEndsKeepAliveReuse(t *testing.T) {
	t.Parallel()

	ln := fasthttputil.NewInmemoryListener()
	handlerWoke := make(chan struct{})
	s := &Server{
		Handler: func(ctx *RequestCtx) {
			<-ctx.Done()
			close(handlerWoke)
			ctx.Success("text/plain", []byte("bye"))
		},
		Logger: &testLogger{},
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- s.Serve(ln) }()

	conn, err := ln.Dial()
	if err != nil {
		t.Fatalf("Dial() error: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")); err != nil {
		t.Fatalf("Write() error: %v", err)
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- s.Shutdown() }()

	select {
	case <-handlerWoke:
	case <-time.After(testTimeout(2 * time.Second)):
		t.Fatal("handler did not observe drain cancellation")
	}

	br := bufio.NewReader(conn)
	var resp Response
	if err := resp.Read(br); err != nil {
		t.Fatalf("drain response Read() error: %v", err)
	}
	if resp.StatusCode() != StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode(), StatusOK)
	}
	// The default header representation stays compatible with the existing
	// Shutdown behavior; the server still closes this connection after the
	// response because drain mode stops the keep-alive loop.

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown() error: %v", err)
		}
	case <-time.After(testTimeout(2 * time.Second)):
		t.Fatal("Shutdown() did not return after the final response")
	}

	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection was reused after drain completed its response")
	}

	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve() error: %v", err)
		}
	case <-time.After(testTimeout(time.Second)):
		t.Fatal("Serve() did not stop")
	}
}

func TestServerDrainStopsNewConnections(t *testing.T) {
	t.Parallel()

	ln := fasthttputil.NewInmemoryListener()
	s := &Server{Handler: func(ctx *RequestCtx) {}}

	serveErr := make(chan error, 1)
	go func() { serveErr <- s.Serve(ln) }()

	// Wait until Serve has registered the listener.
	deadline := time.Now().Add(testTimeout(time.Second))
	for time.Now().Before(deadline) {
		s.mu.Lock()
		ready := len(s.ln) != 0
		s.mu.Unlock()
		if ready {
			break
		}
		time.Sleep(time.Millisecond)
	}

	if err := s.Shutdown(); err != nil {
		t.Fatalf("Shutdown() error: %v", err)
	}
	if _, err := ln.Dial(); err == nil {
		t.Fatal("Dial() succeeded after drain stopped the listener")
	}

	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve() error: %v", err)
		}
	case <-time.After(testTimeout(time.Second)):
		t.Fatal("Serve() did not stop")
	}
}

func TestServerConcurrentShutdownSharesDeadlineResult(t *testing.T) {
	t.Parallel()

	const callers = 16

	ln := fasthttputil.NewInmemoryListener()
	handlerEntered := make(chan struct{})
	releaseHandler := make(chan struct{})
	var enteredOnce sync.Once

	s := &Server{
		Handler: func(ctx *RequestCtx) {
			enteredOnce.Do(func() { close(handlerEntered) })
			<-releaseHandler
		},
		Logger: &testLogger{},
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- s.Serve(ln) }()

	conn, err := ln.Dial()
	if err != nil {
		t.Fatalf("Dial() error: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")); err != nil {
		t.Fatalf("Write() error: %v", err)
	}
	<-handlerEntered

	results := make(chan error, callers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), testTimeout(250*time.Millisecond))
			defer cancel()
			results <- s.ShutdownWithContext(ctx)
		}()
	}
	close(start)

	done := make(chan struct{})
	go func() { wg.Wait(); close(results); close(done) }()

	select {
	case <-done:
	case <-time.After(testTimeout(5 * time.Second)):
		t.Fatal("concurrent ShutdownWithContext calls did not all return")
	}

	for err := range results {
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("concurrent ShutdownWithContext() error = %v, want %v", err, context.DeadlineExceeded)
		}
	}

	close(releaseHandler)
	waitForInFlightCount(t, s, 0)

	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve() error: %v", err)
		}
	case <-time.After(testTimeout(2 * time.Second)):
		t.Fatal("Serve() did not stop after releasing the handler")
	}
}

type closeCountingListener struct {
	net.Listener
	closeCount atomic.Int32
}

func (l *closeCountingListener) Close() error {
	l.closeCount.Add(1)
	return l.Listener.Close()
}

func TestServerConcurrentShutdownCleansListenerOnce(t *testing.T) {
	t.Parallel()

	baseLn := fasthttputil.NewInmemoryListener()
	ln := &closeCountingListener{Listener: baseLn}
	s := &Server{Handler: func(ctx *RequestCtx) {}}

	serveErr := make(chan error, 1)
	go func() { serveErr <- s.Serve(ln) }()

	deadline := time.Now().Add(testTimeout(time.Second))
	for time.Now().Before(deadline) {
		s.mu.Lock()
		ready := len(s.ln) != 0
		s.mu.Unlock()
		if ready {
			break
		}
		time.Sleep(time.Millisecond)
	}

	const callers = 16
	results := make(chan error, callers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- s.Shutdown()
		}()
	}
	close(start)

	done := make(chan struct{})
	go func() { wg.Wait(); close(results); close(done) }()
	select {
	case <-done:
	case <-time.After(testTimeout(5 * time.Second)):
		t.Fatal("concurrent Shutdown calls did not all return")
	}

	for err := range results {
		if err != nil {
			t.Fatalf("Shutdown() error: %v", err)
		}
	}
	if got := ln.closeCount.Load(); got != 1 {
		t.Fatalf("listener Close() count = %d, want 1", got)
	}

	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve() error: %v", err)
		}
	case <-time.After(testTimeout(2 * time.Second)):
		t.Fatal("Serve() did not stop")
	}
}

type blockingTerminalWriteConn struct {
	net.Conn
	firstWrite   chan struct{}
	releaseWrite chan struct{}
	once         sync.Once
}

func (c *blockingTerminalWriteConn) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.firstWrite) })
	select {
	case <-c.releaseWrite:
	case <-time.After(5 * time.Second):
		return 0, errors.New("timed out waiting for terminal write release")
	}
	return c.Conn.Write(p)
}

func TestServerInFlightRequestReleasedAfterTerminalWrite(t *testing.T) {
	t.Parallel()

	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	blockingConn := &blockingTerminalWriteConn{
		Conn:         serverConn,
		firstWrite:   make(chan struct{}),
		releaseWrite: make(chan struct{}),
	}

	s := &Server{
		Handler: func(ctx *RequestCtx) {
			ctx.SetBodyString("terminal")
		},
		Logger: &testLogger{},
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- s.ServeConn(blockingConn) }()

	requestWritten := make(chan error, 1)
	go func() {
		_, err := clientConn.Write([]byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"))
		requestWritten <- err
	}()

	select {
	case err := <-requestWritten:
		if err != nil {
			t.Fatalf("request Write() error: %v", err)
		}
	case <-time.After(testTimeout(2 * time.Second)):
		t.Fatal("request was not written")
	}

	select {
	case <-blockingConn.firstWrite:
	case <-time.After(testTimeout(2 * time.Second)):
		t.Fatal("terminal response write did not start")
	}
	waitForInFlightCount(t, s, 1)

	readErr := make(chan error, 1)
	go func() {
		var resp Response
		readErr <- resp.Read(bufio.NewReader(clientConn))
	}()

	close(blockingConn.releaseWrite)

	select {
	case err := <-readErr:
		if err != nil {
			t.Fatalf("terminal response Read() error: %v", err)
		}
	case <-time.After(testTimeout(2 * time.Second)):
		t.Fatal("client did not receive the released terminal response")
	}
	waitForInFlightCount(t, s, 0)

	if err := clientConn.Close(); err != nil {
		t.Fatalf("client Close() error: %v", err)
	}
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("ServeConn() error: %v", err)
		}
	case <-time.After(testTimeout(2 * time.Second)):
		t.Fatal("ServeConn() did not return after client close")
	}
}
