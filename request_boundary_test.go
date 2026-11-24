package fasthttp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/valyala/bytebufferpool"
)

// The declared Content-Length must be rejected before a single body byte is
// read or allocated, and the unread body must stay in the reader so the
// caller can decide what to do with the connection.
func TestRequestBoundaryContentLengthCheckedBeforeRead(t *testing.T) {
	t.Parallel()

	body := strings.Repeat("a", 100)
	raw := "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 100\r\n\r\n" + body
	r := bufio.NewReader(strings.NewReader(raw))

	var req Request
	err := req.ReadLimitBody(r, 10)
	if !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("expected ErrBodyTooLarge, got %v", err)
	}
	if buffered := r.Buffered(); buffered != len(body) {
		t.Fatalf("body bytes consumed on rejection: %d of %d left in reader", buffered, len(body))
	}

	// The failed read must leave the request in a clean state.
	if len(req.Body()) != 0 {
		t.Fatalf("unexpected body after ErrBodyTooLarge: %q", req.Body())
	}
}

// A declared chunk size over the limit must be rejected before the chunk
// data is read.
func TestRequestBoundaryChunkSizeCheckedBeforeRead(t *testing.T) {
	t.Parallel()

	chunk := strings.Repeat("b", 100)
	raw := "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n" +
		"64\r\n" + chunk + "\r\n0\r\n\r\n"
	r := bufio.NewReader(strings.NewReader(raw))

	var req Request
	err := req.ReadLimitBody(r, 10)
	if !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("expected ErrBodyTooLarge, got %v", err)
	}
	// The chunk data and everything after it must be untouched.
	if buffered := r.Buffered(); buffered != len(chunk)+len("\r\n0\r\n\r\n") {
		t.Fatalf("chunk data consumed on rejection: %d left in reader", buffered)
	}
}

// With no configured limit, a declared size is still attacker-controlled:
// an astronomical Content-Length or chunk size must not turn into a giant
// up-front allocation. The read fails with io.ErrUnexpectedEOF once the
// peer stops sending.
func TestRequestBoundaryHugeDeclaredSizeNoLimit(t *testing.T) {
	t.Parallel()

	t.Run("content-length", func(t *testing.T) {
		t.Parallel()
		raw := "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 4611686018427387904\r\n\r\nshort"
		var req Request
		err := req.ReadLimitBody(bufio.NewReader(strings.NewReader(raw)), 0)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
		}
	})

	t.Run("chunk-size", func(t *testing.T) {
		t.Parallel()
		raw := "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n" +
			"fffffffffffffff\r\nshort"
		var req Request
		err := req.ReadLimitBody(bufio.NewReader(strings.NewReader(raw)), 0)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
		}
	})
}

// A peer that actually sends a body larger than the pre-allocation cap with
// no limit configured must still be read in full.
func TestRequestBoundaryLargeBodyNoLimit(t *testing.T) {
	t.Parallel()

	body := make([]byte, maxBodyPreallocSize+12345)
	for i := range body {
		body[i] = byte(i)
	}
	raw := fmt.Sprintf("POST / HTTP/1.1\r\nHost: a\r\nContent-Length: %d\r\n\r\n", len(body)) + string(body)

	var req Request
	if err := req.ReadLimitBody(bufio.NewReader(strings.NewReader(raw)), 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(req.Body(), body) {
		t.Fatalf("body mismatch: got %d bytes", len(req.Body()))
	}
}

// Truncated chunks, invalid extensions and the zero-length final chunk must
// each produce a stable result.
func TestRequestBoundaryChunkedFramingTable(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		body       string
		want       string
		wantErr    error // nil means the read must succeed
		wantBroken bool  // the read must fail with ErrBrokenChunk
	}{
		{name: "valid", body: "5\r\nhello\r\n0\r\n\r\n", want: "hello"},
		{name: "valid-multi", body: "2\r\nhe\r\n3\r\nllo\r\n0\r\n\r\n", want: "hello"},
		{name: "valid-extension", body: "5;foo=bar\r\nhello\r\n0\r\n\r\n", want: "hello"},
		{name: "zero-final-with-trailer", body: "5\r\nhello\r\n0\r\nX-T: v\r\n\r\n", want: "hello"},
		{name: "zero-final-only", body: "0\r\n\r\n", want: ""},
		{name: "zero-final-with-extension", body: "0;done=yes\r\n\r\n", want: ""},
		{name: "truncated-chunk-data", body: "5\r\nhe", wantErr: io.ErrUnexpectedEOF},
		{name: "truncated-chunk-crlf", body: "5\r\nhello\r", wantErr: io.ErrUnexpectedEOF},
		{name: "truncated-size-line", body: "5", wantBroken: true},
		{name: "missing-crlf-after-chunk", body: "5\r\nhelloXX0\r\n\r\n", wantBroken: true},
		{name: "newline-in-extension", body: "5;bad\next\r\nhello\r\n0\r\n\r\n", wantBroken: true},
		{name: "junk-after-size", body: "5x\r\nhello\r\n0\r\n\r\n", wantBroken: true},
		{name: "truncated-final-chunk", body: "5\r\nhello\r\n0\r\n", wantBroken: true},
		{name: "truncated-before-final-chunk", body: "5\r\nhello\r\n", wantErr: io.ErrUnexpectedEOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw := "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n" + tc.body

			// The result must be identical on repeated reads of the same
			// wire bytes: the framing error is a property of the bytes,
			// not of parser state.
			for attempt := 0; attempt < 2; attempt++ {
				var req Request
				err := req.ReadLimitBody(bufio.NewReader(strings.NewReader(raw)), 1024)
				if tc.wantErr == nil && !tc.wantBroken {
					if err != nil {
						t.Fatalf("attempt %d: unexpected error: %v", attempt, err)
					}
					if string(req.Body()) != tc.want {
						t.Fatalf("attempt %d: body %q, want %q", attempt, req.Body(), tc.want)
					}
					continue
				}
				if err == nil {
					t.Fatalf("attempt %d: expected an error, got nil", attempt)
				}
				if tc.wantBroken {
					var broken ErrBrokenChunk
					if !errors.As(err, &broken) {
						t.Fatalf("attempt %d: expected ErrBrokenChunk, got %v", attempt, err)
					}
				} else if !errors.Is(err, tc.wantErr) {
					t.Fatalf("attempt %d: expected error %v, got %v", attempt, tc.wantErr, err)
				}
				// A failed read resets the request: no partial body leaks.
				if len(req.Body()) != 0 {
					t.Fatalf("attempt %d: partial body leaked after error: %q", attempt, req.Body())
				}
			}
		})
	}
}

// A valid streamed body stays readable in segments, and once a read fails
// the stream keeps returning the same terminal error instead of resuming
// parsing at an unknown offset.
func TestRequestStreamTerminalStateAfterError(t *testing.T) {
	t.Parallel()

	newStream := func(t *testing.T, wire string) *requestStream {
		t.Helper()
		r := bufio.NewReader(strings.NewReader(wire))
		var req Request
		req.Header.SetMethod(MethodPost)
		if err := req.Header.Read(r); err != nil {
			t.Fatalf("reading header: %v", err)
		}
		if err := req.ContinueReadBodyStream(r, 1024); err != nil {
			t.Fatalf("ContinueReadBodyStream: %v", err)
		}
		rs, ok := req.bodyStream.(*requestStream)
		if !ok {
			t.Fatalf("expected *requestStream, got %T", req.bodyStream)
		}
		return rs
	}

	t.Run("segmented-read-then-eof", func(t *testing.T) {
		t.Parallel()
		rs := newStream(t, "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n"+
			"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n")
		defer releaseRequestStream(rs)
		var got []byte
		buf := make([]byte, 3)
		for {
			n, err := rs.Read(buf)
			got = append(got, buf[:n]...)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}
		if string(got) != "hello world" {
			t.Fatalf("body %q, want %q", got, "hello world")
		}
		// EOF is terminal too.
		if n, err := rs.Read(buf); n != 0 || err != io.EOF {
			t.Fatalf("read after EOF: (%d, %v)", n, err)
		}
	})

	t.Run("broken-extension", func(t *testing.T) {
		t.Parallel()
		rs := newStream(t, "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n"+
			"5;bad\next\r\nhello\r\n0\r\n\r\n")
		defer releaseRequestStream(rs)
		buf := make([]byte, 16)
		_, err1 := rs.Read(buf)
		if err1 == nil {
			t.Fatal("expected framing error")
		}
		for i := 0; i < 3; i++ {
			n, err := rs.Read(buf)
			if n != 0 || err != err1 {
				t.Fatalf("read %d after error: (%d, %v), want (0, %v)", i, n, err, err1)
			}
		}
	})

	t.Run("truncated-chunk-data", func(t *testing.T) {
		t.Parallel()
		rs := newStream(t, "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n"+
			"5\r\nhel")
		defer releaseRequestStream(rs)
		buf := make([]byte, 16)
		n, err1 := rs.Read(buf)
		if n != 3 || err1 != nil {
			t.Fatalf("first read: (%d, %v), want (3, nil)", n, err1)
		}
		_, err2 := rs.Read(buf)
		if !errors.Is(err2, io.ErrUnexpectedEOF) {
			t.Fatalf("second read: %v, want io.ErrUnexpectedEOF", err2)
		}
		if n, err := rs.Read(buf); n != 0 || err != err2 {
			t.Fatalf("read after error: (%d, %v), want (0, %v)", n, err, err2)
		}
	})

	t.Run("truncated-fixed-length", func(t *testing.T) {
		t.Parallel()
		// A declared Content-Length the peer never finishes sending. The
		// stream is built directly because ContinueReadBodyStream pre-reads
		// small bodies eagerly and would fail before a stream exists.
		var h RequestHeader
		h.SetMethod(MethodPost)
		h.SetContentLength(10)
		r := bufio.NewReader(strings.NewReader("abc"))
		rs := acquireRequestStream(&bytebufferpool.ByteBuffer{}, r, &h)
		rs.strictEOF = true
		defer releaseRequestStream(rs)
		buf := make([]byte, 16)
		n, err := rs.Read(buf)
		if n != 3 || err != nil {
			t.Fatalf("first read: (%d, %v), want (3, nil)", n, err)
		}
		if n, err := rs.Read(buf); n != 0 || !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("second read: (%d, %v), want (0, io.ErrUnexpectedEOF)", n, err)
		}
		if n, err := rs.Read(buf); n != 0 || !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("read after error: (%d, %v), want (0, io.ErrUnexpectedEOF)", n, err)
		}
	})
}

// Releasing a stream to the pool and acquiring a new one must clear all
// framing state: terminal error, failure flag, EOF, chunk accounting.
func TestRequestStreamPoolReuseClearsFramingState(t *testing.T) {
	t.Parallel()

	r := bufio.NewReader(strings.NewReader("5;bad\next\r\nhello\r\n0\r\n\r\n"))
	var h RequestHeader
	h.SetMethod(MethodPost)
	h.Set("Transfer-Encoding", "chunked")
	if err := h.Read(bufio.NewReader(strings.NewReader("POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n"))); err != nil {
		t.Fatal(err)
	}

	rs := acquireRequestStream(&bytebufferpool.ByteBuffer{}, r, &h)
	buf := make([]byte, 16)
	if _, err := rs.Read(buf); err == nil {
		t.Fatal("expected framing error")
	}
	if !rs.failed {
		t.Fatal("stream not marked failed")
	}
	releaseRequestStream(rs)

	rs2 := acquireRequestStream(&bytebufferpool.ByteBuffer{}, r, &h)
	defer releaseRequestStream(rs2)
	if rs2.err != nil || rs2.failed || rs2.eof || rs2.chunkLeft != 0 || rs2.chunkOverhead != 0 || rs2.totalBytesRead != 0 {
		t.Fatalf("pooled stream kept framing state: %+v", rs2)
	}
}

// Reading a request must reset all framing state of the previous one, so a
// pooled Request never leaks a body stream, trailer or content length into
// the next request.
func TestRequestReuseClearsFramingState(t *testing.T) {
	t.Parallel()

	var req Request

	// 1. A chunked request with a trailer.
	chunked := "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n" +
		"5\r\nhello\r\n0\r\nX-Trailer: v\r\n\r\n"
	if err := req.ReadLimitBody(bufio.NewReader(strings.NewReader(chunked)), 1024); err != nil {
		t.Fatalf("chunked read: %v", err)
	}
	if string(req.Body()) != "hello" {
		t.Fatalf("body %q", req.Body())
	}

	// 2. Reuse for a fixed-length request: no trailer, no chunked framing.
	req.Reset()
	fixed := "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\n\r\nabc"
	if err := req.ReadLimitBody(bufio.NewReader(strings.NewReader(fixed)), 1024); err != nil {
		t.Fatalf("fixed read: %v", err)
	}
	if string(req.Body()) != "abc" {
		t.Fatalf("body %q", req.Body())
	}
	if got := string(req.Header.Peek("X-Trailer")); got != "" {
		t.Fatalf("trailer leaked across reuse: %q", got)
	}
	if req.Header.ContentLength() != 3 {
		t.Fatalf("content length %d, want 3", req.Header.ContentLength())
	}

	// 3. A failed read leaves no framing state behind either.
	req.Reset()
	tooBig := "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 100\r\n\r\n" + strings.Repeat("a", 100)
	if err := req.ReadLimitBody(bufio.NewReader(strings.NewReader(tooBig)), 10); !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("expected ErrBodyTooLarge, got %v", err)
	}
	if err := req.ReadLimitBody(bufio.NewReader(strings.NewReader(fixed)), 1024); err != nil {
		t.Fatalf("fixed read after error: %v", err)
	}
	if string(req.Body()) != "abc" {
		t.Fatalf("body %q after reuse", req.Body())
	}
	if req.bodyStream != nil || req.serverStream != nil {
		t.Fatal("body stream state leaked across reuse")
	}
}

// A streamed request that fails mid-body must not poison the next request
// read through the same Request object.
func TestRequestReuseAfterStreamError(t *testing.T) {
	t.Parallel()

	var req Request
	broken := "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n5;bad\next\r\n"
	r := bufio.NewReader(strings.NewReader(broken))
	if err := req.Header.Read(r); err != nil {
		t.Fatal(err)
	}
	if err := req.ContinueReadBodyStream(r, 1024); err != nil {
		t.Fatal(err)
	}
	if _, err := req.BodyStream().Read(make([]byte, 16)); err == nil {
		t.Fatal("expected framing error from stream")
	}

	req.Reset()
	if req.bodyStream != nil || req.serverStream != nil {
		t.Fatal("stream state leaked across Reset")
	}
	fixed := "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\n\r\nabc"
	if err := req.ReadLimitBody(bufio.NewReader(strings.NewReader(fixed)), 1024); err != nil {
		t.Fatalf("fixed read after stream error: %v", err)
	}
	if string(req.Body()) != "abc" {
		t.Fatalf("body %q", req.Body())
	}
}

// serveBoundaryTestConn runs one connection through s and returns the raw
// response bytes plus how often the handler ran.
func serveBoundaryTestConn(t *testing.T, s *Server, wire string, handlerCalls *int) []byte {
	t.Helper()
	rw := &readWriter{}
	rw.r.WriteString(wire)
	_ = s.ServeConn(rw)
	return rw.w.Bytes()
}

// A body over the limit must produce one error response and a closed
// connection; the leftover body bytes must not be parsed as the next
// request.
func TestServerBodyTooLargeClosesConnection(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		wire string
	}{
		{
			name: "content-length",
			wire: "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 100\r\n\r\n" + strings.Repeat("a", 100) +
				"GET /next HTTP/1.1\r\nHost: a\r\n\r\n",
		},
		{
			name: "chunked",
			wire: "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n" +
				"64\r\n" + strings.Repeat("b", 100) + "\r\n0\r\n\r\n" +
				"GET /next HTTP/1.1\r\nHost: a\r\n\r\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			handlerCalls := 0
			s := &Server{
				MaxRequestBodySize: 10,
				Handler: func(ctx *RequestCtx) {
					handlerCalls++
				},
			}
			out := serveBoundaryTestConn(t, s, tc.wire, &handlerCalls)

			if handlerCalls != 0 {
				t.Fatalf("handler ran %d times for an over-limit body", handlerCalls)
			}
			br := bufio.NewReader(bytes.NewReader(out))
			var resp Response
			if err := resp.Read(br); err != nil {
				t.Fatalf("reading error response: %v", err)
			}
			if resp.StatusCode() != StatusBadRequest {
				t.Fatalf("status %d, want %d", resp.StatusCode(), StatusBadRequest)
			}
			if !resp.Header.ConnectionClose() {
				t.Fatal("error response must carry Connection: close")
			}
			// The pipelined second request must not get a response: the
			// unread body bytes were not reinterpreted as a request.
			if br.Buffered() != 0 {
				t.Fatalf("unexpected extra response bytes: %q", out)
			}
		})
	}
}

// A broken chunked body must close the connection after the error response
// for the same reason: the framing of everything after it is unknown.
func TestServerBrokenChunkClosesConnection(t *testing.T) {
	t.Parallel()

	handlerCalls := 0
	s := &Server{
		Handler: func(ctx *RequestCtx) {
			handlerCalls++
		},
	}
	wire := "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n" +
		"5;bad\next\r\nhello\r\n0\r\n\r\n" +
		"GET /next HTTP/1.1\r\nHost: a\r\n\r\n"
	out := serveBoundaryTestConn(t, s, wire, &handlerCalls)

	if handlerCalls != 0 {
		t.Fatalf("handler ran %d times for a broken chunked body", handlerCalls)
	}
	br := bufio.NewReader(bytes.NewReader(out))
	var resp Response
	if err := resp.Read(br); err != nil {
		t.Fatalf("reading error response: %v", err)
	}
	if resp.StatusCode() != StatusBadRequest {
		t.Fatalf("status %d, want %d", resp.StatusCode(), StatusBadRequest)
	}
	if !resp.Header.ConnectionClose() {
		t.Fatal("error response must carry Connection: close")
	}
	if br.Buffered() != 0 {
		t.Fatalf("unexpected extra response bytes: %q", out)
	}
}

// A well-formed chunked request keeps the connection alive: a pipelined
// request right after the last chunk is served normally.
func TestServerKeepAliveAfterValidChunked(t *testing.T) {
	t.Parallel()

	var paths []string
	s := &Server{
		Handler: func(ctx *RequestCtx) {
			paths = append(paths, string(ctx.Path()))
			ctx.SetBody(ctx.Request.Body())
		},
	}
	wire := "POST /upload HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n" +
		"5\r\nhello\r\n0\r\n\r\n" +
		"GET /next HTTP/1.1\r\nHost: a\r\n\r\n"
	out := serveBoundaryTestConn(t, s, wire, nil)

	if len(paths) != 2 || paths[0] != "/upload" || paths[1] != "/next" {
		t.Fatalf("handler paths %v", paths)
	}
	br := bufio.NewReader(bytes.NewReader(out))
	var resp1, resp2 Response
	if err := resp1.Read(br); err != nil {
		t.Fatalf("reading first response: %v", err)
	}
	if resp1.Header.ConnectionClose() {
		t.Fatal("keep-alive response must not carry Connection: close")
	}
	if string(resp1.Body()) != "hello" {
		t.Fatalf("first body %q", resp1.Body())
	}
	if err := resp2.Read(br); err != nil {
		t.Fatalf("reading second response: %v", err)
	}
}

// With StreamRequestBody, a handler that hits a framing error mid-body gets
// its response out, but the connection still closes: the unread rest of the
// body cannot be framed as the next request.
func TestServerStreamBodyErrorClosesConnection(t *testing.T) {
	t.Parallel()

	var streamErr error
	s := &Server{
		StreamRequestBody: true,
		Handler: func(ctx *RequestCtx) {
			buf := make([]byte, 16)
			for {
				_, err := ctx.RequestBodyStream().Read(buf)
				if err != nil {
					streamErr = err
					break
				}
			}
			ctx.SetBodyString("seen")
		},
	}
	wire := "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n" +
		"5\r\nhello\r\n3;bad\next\r\nfoo\r\n0\r\n\r\n" +
		"GET /next HTTP/1.1\r\nHost: a\r\n\r\n"
	out := serveBoundaryTestConn(t, s, wire, nil)

	if streamErr == nil {
		t.Fatal("handler saw no stream error")
	}
	br := bufio.NewReader(bytes.NewReader(out))
	var resp Response
	if err := resp.Read(br); err != nil {
		t.Fatalf("reading response: %v", err)
	}
	if string(resp.Body()) != "seen" {
		t.Fatalf("body %q", resp.Body())
	}
	if !resp.Header.ConnectionClose() {
		t.Fatal("response after a stream framing error must carry Connection: close")
	}
	if br.Buffered() != 0 {
		t.Fatalf("unexpected extra response bytes: %q", out)
	}
}

// A valid streamed body that the handler reads to the end keeps the
// connection alive for the pipelined request.
func TestServerStreamBodyKeepAlive(t *testing.T) {
	t.Parallel()

	handlerCalls := 0
	s := &Server{
		StreamRequestBody: true,
		Handler: func(ctx *RequestCtx) {
			handlerCalls++
			stream := ctx.RequestBodyStream()
			if stream == nil {
				return
			}
			body, err := io.ReadAll(stream)
			if err != nil {
				t.Errorf("reading stream: %v", err)
			}
			ctx.SetBody(body)
		},
	}
	wire := "POST /upload HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n" +
		"5\r\nhello\r\n0\r\n\r\n" +
		"GET /next HTTP/1.1\r\nHost: a\r\n\r\n"
	out := serveBoundaryTestConn(t, s, wire, nil)

	if handlerCalls != 2 {
		t.Fatalf("handler ran %d times, want 2", handlerCalls)
	}
	br := bufio.NewReader(bytes.NewReader(out))
	var resp1, resp2 Response
	if err := resp1.Read(br); err != nil {
		t.Fatalf("reading first response: %v", err)
	}
	if resp1.Header.ConnectionClose() {
		t.Fatal("keep-alive response must not carry Connection: close")
	}
	if string(resp1.Body()) != "hello" {
		t.Fatalf("first body %q", resp1.Body())
	}
	if err := resp2.Read(br); err != nil {
		t.Fatalf("reading second response: %v", err)
	}
}
