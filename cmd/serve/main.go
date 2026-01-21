package main

import (
	"fmt"
	"net/http"
	"strings"
)

func main() {
	fs := http.FileServer(http.Dir("wiki_index"))
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".gz") {
			w.Header().Set("Content-Encoding", "gzip")
			w.Header().Set("Content-Type", "application/octet-stream")
		}
		fs.ServeHTTP(w, r)
	})
	fmt.Println("serving on http://localhost:8080")
	http.ListenAndServe(":8080", nil)
}
