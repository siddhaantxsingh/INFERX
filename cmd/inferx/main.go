// inferx: inference gateway in front of LLM engine replicas.
//
//	inferx -config inferx.json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"inferx/internal/admit"
	"inferx/internal/autoscale"
	"inferx/internal/gateway"
	"inferx/internal/pool"
	"inferx/internal/router"
)

type Config struct {
	Listen           string            `json:"listen"`
	Policy           string            `json:"policy"`
	PrefixBytes      int               `json:"prefix_bytes"`
	Static           []string          `json:"static_replicas"`
	Command          []string          `json:"replica_command"` // with {port}
	MinReplicas      int               `json:"min_replicas"`
	MaxReplicas      int               `json:"max_replicas"`
	TargetPerReplica float64           `json:"target_inflight_per_replica"`
	MaxPerReplica    int               `json:"max_inflight_per_replica"`
	MaxQueue         int               `json:"max_queue"`
	MaxWaitMs        int               `json:"max_wait_ms"`
	ScaleDownAfterS  int               `json:"scale_down_after_s"`
	Tenants          map[string]string `json:"tenants"` // API key -> tenant
	TenantCaps       map[string]int    `json:"tenant_max_inflight"`
}

func main() {
	path := flag.String("config", "inferx.json", "config file")
	flag.Parse()
	raw, err := os.ReadFile(*path)
	if err != nil {
		log.Fatal(err)
	}
	var c Config
	if err := json.Unmarshal(raw, &c); err != nil {
		log.Fatal(err)
	}
	rt := router.New(router.Policy(c.Policy))
	if c.PrefixBytes > 0 {
		rt.PrefixBytes = c.PrefixBytes
	}
	p := pool.New(rt, c.Command)
	for _, u := range c.Static {
		p.AddStatic(u)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	for i := 0; i < c.MinReplicas && len(c.Command) > 0; i++ {
		if err := p.Launch(ctx); err != nil {
			log.Fatal(err)
		}
	}
	ad := admit.New(c.MaxPerReplica*p.Size(), c.MaxQueue, time.Duration(c.MaxWaitMs)*time.Millisecond, c.TenantCaps)
	go p.HealthLoop(ctx, time.Second)
	if len(c.Command) > 0 && c.MaxReplicas > c.MinReplicas {
		s := &autoscale.Scaler{Pool: p, TargetPerReplica: c.TargetPerReplica, Min: c.MinReplicas, Max: c.MaxReplicas,
			DownAfter: time.Duration(c.ScaleDownAfterS) * time.Second,
			Load:      func() (int, int) { in, q, _ := ad.Stats(); return in, q },
			OnResize:  func(n int) { ad.SetLimit(c.MaxPerReplica * n) }}
		go s.Run(ctx, time.Second)
	}
	g := gateway.New(rt, p, ad, c.Tenants)
	srv := &http.Server{Addr: c.Listen, Handler: g.Handler()}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	log.Printf("inferx listening on %s (policy %s, %d replicas)", c.Listen, c.Policy, p.Size())
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
	p.Shutdown()
}
