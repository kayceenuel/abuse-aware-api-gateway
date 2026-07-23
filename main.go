package main

import (
	"fmt"
	"log"
	"net/http"
)

func main() {

	// handle the routes: /login, /search, /purchase
	http.HandleFunc("/login", loginHandler)
	http.HandleFunc("/search", searchHandler)
	http.HandleFunc("/purchase", purchaseHandler)

	// set up the HTTP server
	fmt.Println("Server is running on htt://localhost:2121")
	log.Fatal(http.ListenAndServe(":2121", nil))
}
