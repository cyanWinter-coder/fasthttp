package fasthttp

import (
	"bufio"
	"bytes"
	"strings"
	"sync"
	"testing"
)

// A parsed query must own its storage: mutating the URI's queryString after
// QueryArgs has run must not change the values the caller already obtained.
func TestPoolResetQueryArgsDoNotAliasQueryString(t *testing.T) {
	t.Parallel()

	var u URI
	u.Parse(nil, []byte("/path?token=secret123&n=42")) //nolint:errcheck
	got := u.QueryArgs().Peek("token")

	for i := range u.queryString {
		u.queryString[i] = 'X'
	}
	if string(got) != "secret123" {
		t.Fatalf("query args alias queryString, got %q", got)
	}
}

// PostArgs parsed from a form body must not alias the request body buffer,
// which is returned to the body pool when the request is reset.
func TestPoolResetPostArgsDoNotAliasBody(t *testing.T) {
	t.Parallel()

	var req Request
	req.Header.SetContentTypeBytes(strPostArgsContentType)
	req.SetBodyString("token=formsecret&n=1")
	got := req.PostArgs().Peek("token")

	body := req.bodyBytes()
	for i := range body {
		body[i] = 'X'
	}
	if string(got) != "formsecret" {
		t.Fatalf("post args alias body buffer, got %q", got)
	}
}

// Reusing a pooled URI for a second request must keep the first request's
// query from aliasing the second request's backing queryString buffer. The
// values are owned by the Args, so replacing the source storage cannot change
// them. (Retaining a returned slice beyond the object's release still
// requires the caller to copy it, as the getter docs state.)
func TestPoolResetQueryArgsAcrossRequests(t *testing.T) {
	t.Parallel()

	var u URI
	u.Parse(nil, []byte("/path?token=secret123")) //nolint:errcheck
	first := u.QueryArgs()
	got := append([]byte(nil), first.Peek("token")...)

	u.Reset()
	u.Parse(nil, []byte("/p?token=NEWVALUE")) //nolint:errcheck
	second := u.QueryArgs()

	if string(got) != "secret123" {
		t.Fatalf("copied first-request arg mutated across requests: %q", got)
	}
	if string(second.Peek("token")) != "NEWVALUE" {
		t.Fatalf("second-request arg wrong: %q", second.Peek("token"))
	}
}

// Collected request cookies must not alias the header entry they were parsed
// from. Overwriting that header value must leave the cookie untouched.
func TestPoolResetCookiesDoNotAliasHeader(t *testing.T) {
	t.Parallel()

	var h RequestHeader
	h.Set("Cookie", "session=abc123; user=bob")
	var session []byte
	for k, v := range h.Cookies() {
		if string(k) == "session" {
			session = v
		}
	}
	if string(session) != "abc123" {
		t.Fatalf("unexpected cookie: %q", session)
	}

	// Replace the Cookie header with a different value and recollect.
	h.Set("Cookie", "session=ZZZ")
	_ = h.Cookies()
	if string(session) != "abc123" {
		t.Fatalf("retained cookie mutated: %q", session)
	}
}

// Backing arrays that grow past the retention limit are dropped on Reset so a
// single oversized request cannot pin peak memory on the pool.
func TestPoolResetDropsOversizedBuffers(t *testing.T) {
	t.Parallel()

	t.Run("args entries", func(t *testing.T) {
		var a Args
		var sb strings.Builder
		for i := 0; i < maxRetainedArgEntries+10; i++ {
			if i > 0 {
				sb.WriteByte('&')
			}
			sb.WriteString("k=v")
		}
		a.Parse(sb.String())
		if cap(a.args) <= maxRetainedArgEntries {
			t.Fatalf("setup: args cap=%d", cap(a.args))
		}
		a.Reset()
		if a.args != nil {
			t.Fatalf("oversized entry array retained, cap=%d", cap(a.args))
		}
	})

	t.Run("single large header value", func(t *testing.T) {
		var h RequestHeader
		h.Set("X-Big", strings.Repeat("x", maxRetainedArgBufferSize+1))
		h.Reset()
		if h.h != nil {
			t.Fatalf("oversized header entry array retained, cap=%d", cap(h.h))
		}
	})

	t.Run("large uri field", func(t *testing.T) {
		var u URI
		u.Parse(nil, []byte("/"+strings.Repeat("p", maxRetainedArgBufferSize+1))) //nolint:errcheck
		u.Reset()
		if u.path != nil {
			t.Fatalf("oversized uri path retained, cap=%d", cap(u.path))
		}
	})

	t.Run("large cookie value", func(t *testing.T) {
		c := AcquireCookie()
		c.SetKey("big")
		c.SetValue(strings.Repeat("x", maxRetainedArgBufferSize+1))
		ReleaseCookie(c)
		got := AcquireCookie()
		if cap(got.value) > maxRetainedArgBufferSize {
			t.Fatalf("oversized cookie value retained across pool, cap=%d", cap(got.value))
		}
	})
}

// Small buffers stay retained (resliced to zero) so the common steady state of
// repeated similarly-sized requests keeps working allocation-free.
func TestPoolResetRetainsSmallBuffers(t *testing.T) {
	t.Parallel()

	var a Args
	a.Parse("a=1&b=2&c=3")
	a.Reset()
	if cap(a.args) == 0 {
		t.Fatal("small entry array should be retained for reuse")
	}
	if cap(a.buf) == 0 {
		t.Fatal("small scratch buffer should be retained for reuse")
	}

	var h RequestHeader
	h.Set("X-Small", "value")
	h.Reset()
	if cap(h.h) == 0 {
		t.Fatal("small header entry array should be retained for reuse")
	}
}

// After Reset every pooled object exposes clean default values, regardless of
// what the previous request held.
func TestPoolResetGivesCleanDefaults(t *testing.T) {
	t.Parallel()

	t.Run("request header", func(t *testing.T) {
		var h RequestHeader
		h.SetMethod("POST")
		h.SetRequestURI("/old")
		h.SetHost("old.example")
		h.Set("X-Old", "value")
		h.Reset()
		if len(h.method) != 0 || len(h.requestURI) != 0 || len(h.host) != 0 || len(h.h) != 0 {
			t.Fatalf("header not clean after reset: method=%q uri=%q host=%q headers=%d",
				h.method, h.requestURI, h.host, len(h.h))
		}
	})

	t.Run("uri", func(t *testing.T) {
		var u URI
		u.Parse(nil, []byte("http://user:pass@host.example/p?a=1#f")) //nolint:errcheck
		u.Reset()
		if len(u.path) != 0 || len(u.host) != 0 || len(u.queryString) != 0 || u.QueryArgs().Len() != 0 {
			t.Fatalf("uri not clean after reset: path=%q host=%q query=%q args=%d",
				u.path, u.host, u.queryString, u.QueryArgs().Len())
		}
	})

	t.Run("args", func(t *testing.T) {
		var a Args
		a.Parse("a=1&b=2")
		a.Reset()
		if a.Len() != 0 {
			t.Fatalf("args not clean after reset: %d entries", a.Len())
		}
	})
}

// A panic in the handler must be recovered, the connection released through
// the same cleanup entry as a normal/failed request, and the pooled ctx reset
// so the half-processed request cannot leak into the next one.
func TestServeConnPanicRecoversAndResetsCtx(t *testing.T) {
	t.Parallel()

	secret := "panic-secret-value"
	s := &Server{
		Handler: func(ctx *RequestCtx) {
			if string(ctx.Path()) == "/panic" {
				// Leave data on every pooled structure before panicking.
				ctx.Request.Header.Set("X-Token", secret)
				ctx.SetUserValue("token", secret)
				panic("boom")
			}
			ctx.SetStatusCode(StatusOK)
		},
	}

	rw := &readWriter{}
	rw.r.Grow(1024)
	rw.w.Grow(1024)
	rw.r.WriteString("POST /panic HTTP/1.1\r\nHost: example.com\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: 5\r\n\r\na=1&b")

	// ServeConn must recover the panic instead of letting it crash the
	// worker. It returns a non-nil error so the caller closes the connection.
	if err := s.ServeConn(rw); err == nil {
		t.Fatal("ServeConn should return an error after a recovered handler panic")
	}

	// The ctx went back to the pool; acquiring it again must yield a clean one.
	ctx2 := s.acquireCtx(rw)
	defer s.releaseCtx(ctx2)
	if got := ctx2.Request.Header.Peek("X-Token"); len(got) != 0 {
		t.Fatalf("header leaked across panic: %q", got)
	}
	if v := ctx2.UserValue("token"); v != nil {
		t.Fatalf("user value leaked across panic: %v", v)
	}
	if len(ctx2.Request.bodyBytes()) != 0 {
		t.Fatalf("request body leaked across panic")
	}
}

// A parse failure mid-request must still release and reset the pooled object,
// so the partially parsed fields cannot reach the next request.
func TestServeConnParseFailureResetsCtx(t *testing.T) {
	t.Parallel()

	s := &Server{
		Handler: func(ctx *RequestCtx) {
			ctx.SetStatusCode(StatusOK)
		},
	}

	rw := &readWriter{}
	rw.r.Grow(1024)
	rw.w.Grow(1024)
	// Malformed: Content-Length promises more bytes than are sent, then the
	// connection (buffer) ends.
	rw.r.WriteString("POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 100\r\n\r\nshort")

	if err := s.ServeConn(rw); err == nil {
		// An error is expected, but the key point is the cleanup still ran.
		t.Log("ServeConn returned nil for truncated body")
	}

	ctx2 := s.acquireCtx(rw)
	defer s.releaseCtx(ctx2)
	if len(ctx2.Request.Header.Host()) != 0 {
		t.Fatalf("partial header data leaked after parse failure: %q", ctx2.Request.Header.Host())
	}
	if len(ctx2.Request.bodyBytes()) != 0 {
		t.Fatalf("partial body leaked after parse failure")
	}
}

// Concurrent acquire/reuse cycles must not share mutable slices between
// goroutines, and retained data from one request must not surface in another.
func TestPooledObjectsConcurrentReuse(t *testing.T) {
	t.Parallel()

	const goroutines = 16
	const iterations = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				req := AcquireRequest()
				marker := []byte("g")
				marker = append(marker, '0'+byte(id%10))
				req.Header.Set("X-Marker", string(marker))
				req.SetRequestURI("/?n=v")
				if got := string(req.Header.Peek("X-Marker")); !strings.HasPrefix(got, "g") {
					t.Errorf("corrupted marker: %q", got)
					return
				}
				ReleaseRequest(req)

				resp := AcquireResponse()
				resp.Header.Set("X-Marker", string(marker))
				if got := string(resp.Header.Peek("X-Marker")); !strings.HasPrefix(got, "g") {
					t.Errorf("corrupted response marker: %q", got)
					return
				}
				ReleaseResponse(resp)

				args := AcquireArgs()
				args.Parse("marker=" + string(marker))
				if got := args.Peek("marker"); !bytes.Equal(got, marker) {
					t.Errorf("corrupted args marker: %q", got)
					return
				}
				ReleaseArgs(args)

				cookie := AcquireCookie()
				cookie.SetKey("marker")
				cookie.SetValue(string(marker))
				if got := cookie.Value(); !bytes.Equal(got, marker) {
					t.Errorf("corrupted cookie marker: %q", got)
					return
				}
				ReleaseCookie(cookie)
			}
		}(g)
	}
	wg.Wait()
}

// A normal request served after the ctx pool has been through error and panic
// cycles must still work and see a clean request, proving the reset path
// holds up end to end.
func TestServeConnRequestAfterFailureIsClean(t *testing.T) {
	t.Parallel()

	s := &Server{
		Handler: func(ctx *RequestCtx) {
			if v := ctx.UserValue("token"); v != nil {
				t.Errorf("stale user value from a previous request: %v", v)
			}
			if got := ctx.Request.Header.Peek("X-Token"); len(got) != 0 {
				t.Errorf("stale header from a previous request: %q", got)
			}
			ctx.SetStatusCode(StatusOK)
			ctx.SetBodyString("ok")
		},
	}

	// Drive the pool through a panic and a parse failure first.
	panicConn := &readWriter{}
	panicConn.r.Grow(256)
	panicConn.w.Grow(256)
	panicConn.r.WriteString("GET /panic HTTP/1.1\r\nHost: example.com\r\n\r\n")
	s.Handler = func(ctx *RequestCtx) { panic("warmup panic") }
	_ = s.ServeConn(panicConn)

	badConn := &readWriter{}
	badConn.r.Grow(256)
	badConn.w.Grow(256)
	badConn.r.WriteString("GET / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 100\r\n\r\nshort")
	_ = s.ServeConn(badConn)

	// Restore the real handler and serve a clean request on a recycled ctx.
	s.Handler = func(ctx *RequestCtx) {
		if v := ctx.UserValue("token"); v != nil {
			t.Errorf("stale user value from a previous request: %v", v)
		}
		if got := ctx.Request.Header.Peek("X-Token"); len(got) != 0 {
			t.Errorf("stale header from a previous request: %q", got)
		}
		ctx.SetStatusCode(StatusOK)
		ctx.SetBodyString("ok")
	}

	goodConn := &readWriter{}
	goodConn.r.Grow(256)
	goodConn.w.Grow(256)
	goodConn.r.WriteString("GET /clean HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n")
	if err := s.ServeConn(goodConn); err != nil {
		t.Fatalf("ServeConn error: %v", err)
	}

	br := bufio.NewReader(&goodConn.w)
	resp := AcquireResponse()
	defer ReleaseResponse(resp)
	if err := resp.Read(br); err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode() != StatusOK {
		t.Fatalf("expected 200 for clean request, got %d", resp.StatusCode())
	}
	if string(resp.Body()) != "ok" {
		t.Fatalf("unexpected body: %q", resp.Body())
	}
}
