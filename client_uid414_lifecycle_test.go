package fasthttp_test

import (
	"bytes"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	fasthttp "github.com/valyala/fasthttp"
)

var errUID414Retry = errors.New("uid414 retryable failure")

type uid414RoundTripper struct {
	fn func(hc *fasthttp.HostClient, req *fasthttp.Request, resp *fasthttp.Response) (bool, error)
}

func (rt *uid414RoundTripper) RoundTrip(
	hc *fasthttp.HostClient, req *fasthttp.Request, resp *fasthttp.Response,
) (bool, error) {
	return rt.fn(hc, req, resp)
}

type uid414BodyCloser struct {
	closed *atomic.Bool
}

func (b *uid414BodyCloser) Read([]byte) (int, error) { return 0, io.EOF }
func (b *uid414BodyCloser) Close() error {
	b.closed.Store(true)
	return nil
}

func TestUID414NonReplayableBodyStopsRetry(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	hc := &fasthttp.HostClient{
		Addr:                      "example.com:80",
		MaxIdemponentCallAttempts: 3,
		Transport: &uid414RoundTripper{fn: func(_ *fasthttp.HostClient, _ *fasthttp.Request, _ *fasthttp.Response) (bool, error) {
			calls.Add(1)
			return true, errUID414Retry
		}},
	}

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI("http://example.com/upload")
	req.Header.SetMethod(fasthttp.MethodPost)
	req.SetBodyStream(bytes.NewBufferString("already consumed"), 16)

	err := hc.Do(req, resp)
	if calls.Load() != 1 {
		t.Fatalf("attempts = %d, want 1", calls.Load())
	}
	if !errors.Is(err, fasthttp.ErrRequestBodyNotReplayable) {
		t.Fatalf("error %v does not contain %v", err, fasthttp.ErrRequestBodyNotReplayable)
	}
	if !errors.Is(err, errUID414Retry) {
		t.Fatalf("error %v does not contain last failure %v", err, errUID414Retry)
	}
}

func TestUID414BufferedBodyIsReplayed(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	var seenBodies [][]byte
	hc := &fasthttp.HostClient{
		Addr:                      "example.com:80",
		MaxIdemponentCallAttempts: 2,
		Transport: &uid414RoundTripper{fn: func(_ *fasthttp.HostClient, req *fasthttp.Request, resp *fasthttp.Response) (bool, error) {
			calls.Add(1)
			seenBodies = append(seenBodies, bytes.Clone(req.Body()))
			if calls.Load() == 1 {
				return true, errUID414Retry
			}
			resp.SetStatusCode(fasthttp.StatusNoContent)
			return false, nil
		}},
		RetryIfErr: func(_ *fasthttp.Request, _ int, _ error) (bool, bool) {
			return false, true
		},
	}

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	const payload = "replayable payload"
	req.SetRequestURI("http://example.com/upload")
	req.Header.SetMethod(fasthttp.MethodPost)
	req.SetBodyString(payload)

	if err := hc.Do(req, resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("attempts = %d, want 2", calls.Load())
	}
	if len(seenBodies) != 2 || string(seenBodies[0]) != payload || string(seenBodies[1]) != payload {
		t.Fatalf("replayed bodies = %q, %q; want both %q", seenBodies, payload, payload)
	}
}

func TestUID414ResponseBodyClosedBeforeRetry(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	var closed atomic.Bool
	hc := &fasthttp.HostClient{
		Addr:                      "example.com:80",
		MaxIdemponentCallAttempts: 2,
		Transport: &uid414RoundTripper{fn: func(_ *fasthttp.HostClient, _ *fasthttp.Request, resp *fasthttp.Response) (bool, error) {
			call := calls.Add(1)
			if call == 1 {
				resp.SetBodyStream(&uid414BodyCloser{closed: &closed}, -1)
				return true, errUID414Retry
			}
			if !closed.Load() {
				t.Error("second attempt started before previous response body was closed")
			}
			resp.SetStatusCode(fasthttp.StatusOK)
			return false, nil
		}},
		RetryIfErr: func(_ *fasthttp.Request, _ int, _ error) (bool, bool) {
			return false, true
		},
	}

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI("http://example.com/")

	if err := hc.Do(req, resp); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if !closed.Load() {
		t.Fatal("retry response body stream was not closed")
	}
}

func TestUID414CrossDomainCookiesSurviveRetryButNotLeak(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	var seenCookies []string
	c := &fasthttp.Client{
		MaxIdemponentCallAttempts: 2,
		Transport: &uid414RoundTripper{fn: func(_ *fasthttp.HostClient, req *fasthttp.Request, resp *fasthttp.Response) (bool, error) {
			call := calls.Add(1)
			seenCookies = append(seenCookies, string(req.Header.Cookie("session")))
			switch call {
			case 1:
				resp.SetStatusCode(fasthttp.StatusTemporaryRedirect)
				resp.Header.Set(fasthttp.HeaderLocation, "http://unrelated.example/next")
				return false, nil
			case 2:
				return true, errUID414Retry
			default:
				resp.SetStatusCode(fasthttp.StatusOK)
				return false, nil
			}
		}},
	}

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI("http://allowed.example/start")
	req.Header.SetCookie("session", "super-secret")

	if err := c.DoRedirects(req, resp, 2); err != nil {
		t.Fatalf("DoRedirects: %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls = %d, want initial redirect plus two target attempts", calls.Load())
	}
	want := []string{"super-secret", "", ""}
	if len(seenCookies) != len(want) {
		t.Fatalf("cookie observations = %q, want %q", seenCookies, want)
	}
	for i := range want {
		if seenCookies[i] != want[i] {
			t.Fatalf("cookie observations = %q, want %q", seenCookies, want)
		}
	}
}

func TestUID414CookiePolicyForRedirectTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		location string
		want     string
	}{
		{name: "exact domain", location: "http://allowed.example/next", want: "secret"},
		{name: "subdomain", location: "http://api.allowed.example/next", want: "secret"},
		{name: "unrelated domain", location: "http://evil.example/next", want: ""},
		{name: "parent domain", location: "http://example/next", want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var gotCookie string
			calls := 0
			c := &fasthttp.Client{Transport: &uid414RoundTripper{fn: func(_ *fasthttp.HostClient, req *fasthttp.Request, resp *fasthttp.Response) (bool, error) {
				calls++
				if calls == 1 {
					resp.SetStatusCode(fasthttp.StatusFound)
					resp.Header.Set(fasthttp.HeaderLocation, tc.location)
					return false, nil
				}
				gotCookie = string(req.Header.Cookie("session"))
				resp.SetStatusCode(fasthttp.StatusOK)
				return false, nil
			}}}

			req := fasthttp.AcquireRequest()
			resp := fasthttp.AcquireResponse()
			defer fasthttp.ReleaseRequest(req)
			defer fasthttp.ReleaseResponse(resp)
			req.SetRequestURI("http://allowed.example/start")
			req.Header.SetCookie("session", "secret")

			if err := c.DoRedirects(req, resp, 1); err != nil {
				t.Fatalf("DoRedirects: %v", err)
			}
			if gotCookie != tc.want {
				t.Fatalf("target cookie = %q, want %q", gotCookie, tc.want)
			}
		})
	}
}

func TestUID414CookiePolicyReevaluatedForEachRedirectDomain(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	var seenCookies []string
	c := &fasthttp.Client{Transport: &uid414RoundTripper{fn: func(_ *fasthttp.HostClient, req *fasthttp.Request, resp *fasthttp.Response) (bool, error) {
		call := calls.Add(1)
		seenCookies = append(seenCookies, string(req.Header.Cookie("session")))
		resp.SetStatusCode(fasthttp.StatusFound)
		switch call {
		case 1:
			resp.Header.Set(fasthttp.HeaderLocation, "http://evil.example/next")
		case 2:
			resp.Header.Set(fasthttp.HeaderLocation, "http://allowed.example/return")
		default:
			resp.SetStatusCode(fasthttp.StatusOK)
		}
		return false, nil
	}}}

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI("http://allowed.example/start")
	req.Header.SetCookie("session", "secret")

	if err := c.DoRedirects(req, resp, 2); err != nil {
		t.Fatalf("DoRedirects: %v", err)
	}
	want := []string{"secret", "", "secret"}
	if len(seenCookies) != len(want) {
		t.Fatalf("cookie observations = %q, want %q", seenCookies, want)
	}
	for i := range want {
		if seenCookies[i] != want[i] {
			t.Fatalf("cookie observations = %q, want %q", seenCookies, want)
		}
	}
}

func TestUID414BudgetsPreserveCallerConfiguration(t *testing.T) {
	t.Parallel()

	t.Run("retry budget", func(t *testing.T) {
		t.Parallel()

		var calls atomic.Int32
		hc := &fasthttp.HostClient{
			Addr:                      "example.com:80",
			MaxIdemponentCallAttempts: 3,
			Transport: &uid414RoundTripper{fn: func(*fasthttp.HostClient, *fasthttp.Request, *fasthttp.Response) (bool, error) {
				calls.Add(1)
				return true, errUID414Retry
			}},
		}

		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)
		req.SetRequestURI("http://example.com/")

		err := hc.Do(req, resp)
		if calls.Load() != 3 {
			t.Fatalf("calls = %d, want 3", calls.Load())
		}
		if !errors.Is(err, errUID414Retry) {
			t.Fatalf("error = %v, want last failure", err)
		}
	})

	t.Run("redirect budget", func(t *testing.T) {
		t.Parallel()

		var calls atomic.Int32
		c := &fasthttp.Client{Transport: &uid414RoundTripper{fn: func(_ *fasthttp.HostClient, _ *fasthttp.Request, resp *fasthttp.Response) (bool, error) {
			calls.Add(1)
			resp.SetStatusCode(fasthttp.StatusFound)
			resp.Header.Set(fasthttp.HeaderLocation, "/next")
			return false, nil
		}}}

		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)
		req.SetRequestURI("http://example.com/start")

		err := c.DoRedirects(req, resp, 2)
		if calls.Load() != 3 {
			t.Fatalf("calls = %d, want initial plus two redirects", calls.Load())
		}
		if !errors.Is(err, fasthttp.ErrTooManyRedirects) {
			t.Fatalf("error = %v, want %v", err, fasthttp.ErrTooManyRedirects)
		}
	})
}

func TestUID414DeadlineDoesNotStartAnotherAttempt(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		hc := &fasthttp.HostClient{
			Addr:                      "example.com:80",
			MaxIdemponentCallAttempts: 5,
			Transport: &uid414RoundTripper{fn: func(_ *fasthttp.HostClient, _ *fasthttp.Request, resp *fasthttp.Response) (bool, error) {
				calls.Add(1)
				time.Sleep(100 * time.Millisecond)
				resp.SetStatusCode(fasthttp.StatusBadGateway)
				return true, errUID414Retry
			}},
			RetryIfErr: func(*fasthttp.Request, int, error) (bool, bool) {
				return false, true
			},
		}

		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)
		req.SetRequestURI("http://example.com/")
		req.SetTimeout(50 * time.Millisecond)

		err := hc.Do(req, resp)
		if calls.Load() != 1 {
			t.Fatalf("calls after deadline = %d, want 1", calls.Load())
		}
		if !errors.Is(err, fasthttp.ErrTimeout) {
			t.Fatalf("error %v does not contain %v", err, fasthttp.ErrTimeout)
		}
		if !errors.Is(err, errUID414Retry) {
			t.Fatalf("error %v does not contain last failure %v", err, errUID414Retry)
		}
		if !bytes.Contains([]byte(err.Error()), []byte("502")) {
			t.Fatalf("error %v does not contain last response status", err)
		}
	})
}

func TestUID414TimeoutSharedAcrossRedirects(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const total = time.Second
		var calls atomic.Int32
		hc := &fasthttp.HostClient{
			Addr: "example.com:80",
			Transport: &uid414RoundTripper{fn: func(_ *fasthttp.HostClient, _ *fasthttp.Request, resp *fasthttp.Response) (bool, error) {
				calls.Add(1)
				time.Sleep(total / 2)
				resp.SetStatusCode(fasthttp.StatusFound)
				resp.Header.Set(fasthttp.HeaderLocation, "/next")
				return false, nil
			}},
		}

		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)
		req.SetRequestURI("http://example.com/start")
		req.SetTimeout(total)

		err := hc.DoRedirects(req, resp, 3)
		if !errors.Is(err, fasthttp.ErrTimeout) {
			t.Fatalf("DoRedirects error = %v, want %v", err, fasthttp.ErrTimeout)
		}
		if calls.Load() != 2 {
			t.Fatalf("calls = %d, want two started hops; the expired third hop must be hidden", calls.Load())
		}
	})
}

func TestUID414RedirectErrorClosesResponseBody(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	var closed atomic.Bool
	c := &fasthttp.Client{Transport: &uid414RoundTripper{fn: func(_ *fasthttp.HostClient, _ *fasthttp.Request, resp *fasthttp.Response) (bool, error) {
		calls.Add(1)
		resp.SetStatusCode(fasthttp.StatusFound)
		resp.Header.Set(fasthttp.HeaderLocation, "/loop")
		resp.SetBodyStream(&uid414BodyCloser{closed: &closed}, -1)
		return false, nil
	}}}

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI("http://example.com/start")

	err := c.DoRedirects(req, resp, 0)
	if !errors.Is(err, fasthttp.ErrTooManyRedirects) {
		t.Fatalf("error = %v, want %v", err, fasthttp.ErrTooManyRedirects)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
	if !closed.Load() {
		t.Fatal("error redirect response body stream was not closed")
	}
}
