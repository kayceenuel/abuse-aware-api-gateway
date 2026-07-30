package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/kayceenuel/abuse-aware-api-gateway/handlers"
	"github.com/kayceenuel/abuse-aware-api-gateway/proxy"
)

func main() {
	proxyHandler, err := proxy.NewHandler("http://localhost:8080")
	if err != nil {
		log.Fatal(err)
	}

	// handle the routes: /login, /search, /purchase
	http.Handle("/login", handlers.LoginHandler(proxyHandler))
	http.Handle("/search", handlers.SearchHandler(proxyHandler))
	http.Handle("/purchase", handlers.PurchaseHandler(proxyHandler))

	// set up the HTTP server
	fmt.Println("Server is running on http://localhost:2121")
	log.Fatal(http.ListenAndServe(":2121", nil))
}
