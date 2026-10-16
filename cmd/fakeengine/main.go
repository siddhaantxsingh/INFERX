// fakeengine: a simulated LLM engine (see internal/fake).
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"inferx/internal/fake"
)

func main() {
	port := flag.Int("port", 9100, "listen port")
	slots := flag.Int("slots", 8, "concurrent sequences")
	blocks := flag.Int("cache-blocks", 256, "prefix cache capacity in 64-byte blocks")
	prefill := flag.Duration("prefill-per-byte", 20*time.Microsecond, "prefill time per uncached prompt byte")
	decode := flag.Duration("decode-per-token", 2*time.Millisecond, "decode time per token")
	flag.Parse()
	e := fake.New(*slots, *blocks, *prefill, *decode)
	log.Fatal(http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", *port), e))
}
