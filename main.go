package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/kayceenuel/abuse-aware-api-gateway/handlers"
)

func main() {

	// handle the routes: /login, /search, /purchase
	http.HandleFunc("/login", handlers.LoginHandler)
	http.HandleFunc("/search", handlers.SearchHandler)
	http.HandleFunc("/purchase", handlers.PurchaseHandler)

	// Wire up the proxy: connect the handlers to the proxy.

	// set up the HTTP server
	fmt.Println("Server is running on http://localhost:2121")
	log.Fatal(http.ListenAndServe(":2121", nil))
}
