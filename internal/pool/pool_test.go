package pool

import (
	"context"
	"net/http"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"inferx/internal/router"
)

// Launch and retire real fakeengine processes.
func TestLaunchAndGracefulRetire(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "fakeengine")
	if out, err := exec.Command("go", "build", "-o", bin, "../../cmd/fakeengine").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	rt := router.New(router.LeastLoaded)
	p := New(rt, []string{bin, "-port", "{port}", "-decode-per-token", "20ms"})
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := p.Launch(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if p.Size() != 2 || len(rt.Replicas()) != 2 {
		t.Fatalf("size %d, router %d", p.Size(), len(rt.Replicas()))
	}
	// a request in flight on the newest replica must finish before it stops
	newest := p.Members()[1]
	newest.inflightHold()
	if !p.Retire() {
		t.Fatal("retire failed")
	}
	if len(rt.Replicas()) != 1 {
		t.Fatal("draining replica still routable")
	}
	time.Sleep(200 * time.Millisecond)
	if resp, err := http.Get(newest.URL + "/healthz"); err != nil || resp.StatusCode != 200 {
		t.Fatal("replica stopped while a request was in flight")
	}
	newest.inflightRelease()
	deadline := time.Now().Add(5 * time.Second)
	for len(p.Members()) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("drained replica not removed")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := http.Get(newest.URL + "/healthz"); err == nil {
		t.Fatal("retired process still serving")
	}
	p.Shutdown()
}
