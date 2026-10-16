// Package pool manages replicas: launching engine processes, health
// checks, passive outlier ejection and graceful removal.
package pool

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"inferx/internal/router"
)

// Member is a replica plus how it is run.
type Member struct {
	*router.Replica
	cmd      *exec.Cmd
	exited   chan struct{} // closed once the process has been waited for
	draining bool
	failures int
	ejected  time.Time
}

type Pool struct {
	mu         sync.Mutex
	members    []*Member
	rt         *router.Router
	command    []string // with {port}; empty for static replicas
	client     *http.Client
	nextID     int
	EjectAfter int           // consecutive failures before ejection
	EjectFor   time.Duration // how long an ejected replica is skipped
	events     []string      // scale and health events (see Events)
}

func New(rt *router.Router, command []string) *Pool {
	return &Pool{rt: rt, command: command, client: &http.Client{Timeout: 2 * time.Second}, EjectAfter: 3, EjectFor: 5 * time.Second}
}

func (p *Pool) event(format string, a ...any) {
	msg := time.Now().Format("15:04:05.000") + " " + fmt.Sprintf(format, a...)
	log.Print(msg)
	p.events = append(p.events, msg)
}

// Events returns the scale and health event log (caller must not hold mu).
func (p *Pool) Events() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.events...)
}

func (p *Pool) publish() {
	var rs []*router.Replica
	for _, m := range p.members {
		if !m.draining {
			rs = append(rs, m.Replica)
		}
	}
	p.rt.SetReplicas(rs)
}

// AddStatic registers an already running replica.
func (p *Pool) AddStatic(url string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nextID++
	p.members = append(p.members, &Member{Replica: router.NewReplica(fmt.Sprintf("static-%d", p.nextID), strings.TrimRight(url, "/"))})
	p.publish()
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// Launch starts one engine process and adds it once /healthz answers.
func (p *Pool) Launch(ctx context.Context) error {
	if len(p.command) == 0 {
		return fmt.Errorf("no replica command configured")
	}
	port, err := freePort()
	if err != nil {
		return err
	}
	args := make([]string, len(p.command))
	for i, a := range p.command {
		args[i] = strings.ReplaceAll(a, "{port}", strconv.Itoa(port))
	}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	deadline := time.Now().Add(60 * time.Second)
	for {
		if resp, err := p.client.Get(url + "/healthz"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			cmd.Process.Kill()
			cmd.Wait()
			return fmt.Errorf("replica on port %d did not become healthy", port)
		}
		time.Sleep(50 * time.Millisecond)
	}
	p.mu.Lock()
	p.nextID++
	m := &Member{Replica: router.NewReplica(fmt.Sprintf("replica-%d", p.nextID), url), cmd: cmd, exited: make(chan struct{})}
	p.members = append(p.members, m)
	p.publish()
	p.event("launched %s (pid %d, %s)", m.ID, cmd.Process.Pid, url)
	p.mu.Unlock()
	// the only Wait on this process (exec.Cmd.Wait must not be called twice)
	go func() {
		cmd.Wait()
		close(m.exited)
		p.mu.Lock()
		defer p.mu.Unlock()
		if !m.draining {
			p.event("%s exited unexpectedly", m.ID)
			m.SetHealthy(false)
			p.removeLocked(m)
		}
	}()
	return nil
}

func (p *Pool) removeLocked(m *Member) {
	for i, x := range p.members {
		if x == m {
			p.members = append(p.members[:i], p.members[i+1:]...)
			break
		}
	}
	p.publish()
}

// Retire removes the newest launched replica gracefully: it stops getting
// new requests at once and is stopped when its in-flight requests finish.
func (p *Pool) Retire() bool {
	p.mu.Lock()
	var m *Member
	for i := len(p.members) - 1; i >= 0; i-- {
		if p.members[i].cmd != nil && !p.members[i].draining {
			m = p.members[i]
			break
		}
	}
	if m == nil {
		p.mu.Unlock()
		return false
	}
	m.draining = true
	p.publish()
	p.event("draining %s", m.ID)
	p.mu.Unlock()
	go func() {
		for m.Inflight() > 0 {
			time.Sleep(20 * time.Millisecond)
		}
		m.cmd.Process.Signal(os.Interrupt)
		select {
		case <-m.exited:
		case <-time.After(5 * time.Second):
			m.cmd.Process.Kill()
			<-m.exited
		}
		p.mu.Lock()
		p.removeLocked(m)
		p.event("stopped %s", m.ID)
		p.mu.Unlock()
	}()
	return true
}

// Size counts replicas that take traffic (launched or static, not draining).
func (p *Pool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, m := range p.members {
		if !m.draining {
			n++
		}
	}
	return n
}

func (p *Pool) Members() []*Member {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*Member(nil), p.members...)
}

// Report records the outcome of a proxied request (passive health).
func (p *Pool) Report(r *router.Replica, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, m := range p.members {
		if m.Replica != r {
			continue
		}
		if ok {
			m.failures = 0
			return
		}
		m.failures++
		if m.failures >= p.EjectAfter && m.Healthy() {
			m.SetHealthy(false)
			m.ejected = time.Now()
			p.event("ejected %s after %d consecutive failures", m.ID, m.failures)
		}
	}
}

// HealthLoop probes every replica; an ejected replica is readmitted after
// EjectFor once its /healthz answers again.
func (p *Pool) HealthLoop(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		for _, m := range p.Members() {
			p.mu.Lock()
			draining := m.draining
			p.mu.Unlock()
			if draining {
				continue // it is shutting down on purpose
			}
			resp, err := p.client.Get(m.URL + "/healthz")
			up := err == nil && resp.StatusCode == 200
			if resp != nil {
				resp.Body.Close()
			}
			p.mu.Lock()
			switch {
			case !up && m.Healthy():
				m.SetHealthy(false)
				m.ejected = time.Now()
				p.event("%s failed its health check", m.ID)
			case up && !m.Healthy() && time.Since(m.ejected) >= p.EjectFor:
				m.SetHealthy(true)
				m.failures = 0
				p.event("%s healthy again", m.ID)
			}
			p.mu.Unlock()
		}
	}
}

// Shutdown stops every launched replica.
func (p *Pool) Shutdown() {
	p.mu.Lock()
	for _, m := range p.members {
		if m.cmd != nil {
			m.draining = true
			m.cmd.Process.Signal(os.Interrupt)
		}
	}
	p.mu.Unlock()
	time.Sleep(200 * time.Millisecond)
	for _, m := range p.Members() {
		if m.cmd != nil {
			m.cmd.Process.Kill()
		}
	}
}

// test hooks: simulate an in-flight request on a member
func (m *Member) inflightHold()    { m.Replica.Hold() }
func (m *Member) inflightRelease() { m.Replica.Release() }
