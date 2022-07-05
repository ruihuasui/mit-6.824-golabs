// 
// From Golang Tutorial
// Exercise: Web Crawler - https://go.dev/tour/concurrency/10
//
// In this exercise you'll use Go's concurrency features to parallelize a web crawler.
// Modify the Crawl function to fetch URLs in parallel without fetching the same URL twice.
// Hint: you can keep a cache of the URLs that have been fetched on a map, but maps alone are not safe for concurrent use!
// 

package main

import (
	"fmt"
	"sync"
)

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

func quitCrawl(quit chan bool) {
	quit <- true
}

// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func Crawl(url string, depth int, fetcher Fetcher, quit chan bool) {
	// TODO: Fetch URLs in parallel.
	// TODO: Don't fetch the same URL twice.
	// This implementation doesn't do either:
	defer quitCrawl(quit)
	urlVisited := urlCache.Reserve(url)
	fmt.Println(url, urlVisited)
	if depth <= 0 || urlVisited {
		return
	}
	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("found: %s %q\n", url, body)
	urlCache.SetBody(url, body)
	for _, u := range urls {
		quitChild := make(chan bool)
		go Crawl(u, depth-1, fetcher, quitChild)
		<-quitChild
	}

	return
}

func main() {
	quit := make(chan bool)
	go Crawl("https://golang.org/", 4, fetcher, quit)
	<-quit

	fmt.Println(urlCache.results)
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}

// SafeCounter is safe to use concurrently.
type SafeURLCache struct {
	mu sync.Mutex
	results  map[string]string
}

// Value returns the current value of the counter for the given key.
func (uc *SafeURLCache) Reserve(url string) bool {
	uc.mu.Lock()
	defer uc.mu.Unlock()
	_, visited := uc.results[url]
	if !visited {
		uc.results[url] = "<EMPTY BODY>"
	}
	return visited
}

// Value returns the current value of the counter for the given key.
func (uc *SafeURLCache) SetBody(url, body string) {
	uc.mu.Lock()
	defer uc.mu.Unlock()
	uc.results[url] = body
}

var urlCache = &SafeURLCache{results:make(map[string]string)}