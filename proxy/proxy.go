package proxy

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
)

func NewHandler(targetURL string) (http.Handler, error) {
	target, err := url.Parse(targetURL)
	if err != nil {
		return nil, fmt.Errorf("parse target URL: %w", err)
	}
	if target.Scheme == "" || target.Host == "" {
		return nil, fmt.Errorf("target URL must include a scheme and host")
	}

	reverseProxy := httputil.NewSingleHostReverseProxy(target)
	originalDirector := reverseProxy.Director
	reverseProxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = target.Host
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		if req.URL.Path == "" {
			req.URL.Path = "/"
		}
	}

	reverseProxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		http.Error(w, fmt.Sprintf("proxy error: %v", err), http.StatusBadGateway)
	}

	return reverseProxy, nil
}
