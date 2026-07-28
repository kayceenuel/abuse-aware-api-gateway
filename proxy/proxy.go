package proxy 

import (
	"net/http"
	"net/http/httputil"
	"net/url"
)

func main() {
	target, _ := url.Parse("http://localhost:8080") 
	proxy := httputil.NewSingleHostReverseProxy(target)

	http.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		proxy.ServeHTTP(w,r)
	})
}