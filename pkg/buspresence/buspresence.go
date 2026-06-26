// Package buspresence is the canonical HTTP presence client for cofiswarm components that
// reach the observer bus via cofiswarm-zmq-bridge's HTTP API (no NATS client). It publishes
// online/offline presence and alerts, and re-announces on swarm.observer.hello. It replaces
// the near-duplicate per-repo internal/buspresence copies (dispatch, agent-registry); a
// single component uses Announce/Goodbye, a roster owner uses AnnounceMembers/GoodbyeMembers.
package buspresence

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/keepdevops/cofiswarm-observer-sdk/pkg/contract"
)

// Member is one entry a roster owner announces (e.g. agent-registry's agents).
type Member struct {
	ID   string         // component_id, e.g. "agent-architect"
	Info map[string]any // optional descriptive info (name, engine, ...)
}

// Publisher posts presence/alerts to the bus and re-announces on hello.
type Publisher struct {
	base   string
	client *http.Client
}

// New builds a publisher targeting the bridge base URL (e.g. http://127.0.0.1:5555).
func New(bridgeBase string) *Publisher {
	return &Publisher{
		base:   strings.TrimRight(bridgeBase, "/"),
		client: &http.Client{Timeout: 5 * time.Second},
	}
}

// Announce publishes an "online" presence event for a single component.
func (p *Publisher) Announce(id string, info map[string]any) {
	p.publishPresence(id, "online", info)
	log.Printf("buspresence: announced %s", id)
}

// Goodbye publishes an "offline" presence event so a clean shutdown removes the component from
// every observer's roster immediately, instead of waiting out the observer's liveness TTL.
func (p *Publisher) Goodbye(id string) {
	p.publishPresence(id, "offline", nil)
	log.Printf("buspresence: goodbye %s", id)
}

// AnnounceMembers publishes an "online" presence event for each roster member.
func (p *Publisher) AnnounceMembers(members []Member) {
	for _, m := range members {
		p.publishPresence(m.ID, "online", m.Info)
	}
	log.Printf("buspresence: announced %d members", len(members))
}

// GoodbyeMembers publishes an "offline" presence event for each roster member.
func (p *Publisher) GoodbyeMembers(members []Member) {
	for _, m := range members {
		p.publishPresence(m.ID, "offline", nil)
	}
	log.Printf("buspresence: goodbye %d members", len(members))
}

// Alert publishes a dependency-aware alert (e.g. a needed relay is down).
func (p *Publisher) Alert(id, message string) {
	p.publish(contract.TopicAlert, map[string]any{"message": message, "component_id": id})
	log.Printf("buspresence: alert: %s", message)
}

func (p *Publisher) publishPresence(id, status string, info map[string]any) {
	payload := map[string]any{"component_id": id, "status": status}
	if info != nil {
		payload["info"] = info
	}
	p.publish(contract.TopicPresence, payload)
}

// publish posts a {topic, payload} envelope to the bridge. Failures are logged (never silent);
// a non-2xx from the bridge is surfaced too, so a dropped presence event is visible in logs.
func (p *Publisher) publish(topic string, payload map[string]any) {
	body, err := json.Marshal(map[string]any{"topic": topic, "payload": payload})
	if err != nil {
		log.Printf("buspresence: marshal %s: %v", topic, err)
		return
	}
	resp, err := p.client.Post(p.base+"/v1/publish", "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("buspresence: publish %s: %v", topic, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusMultipleChoices {
		log.Printf("buspresence: publish %s: bridge returned %s", topic, resp.Status)
	}
}

// StartPresence is the one-call, broker-free presence lifecycle for a single component:
// it announces id "online" to the bridge carrier, re-announces on every observer hello, and
// returns a stop func that publishes a final "offline" goodbye. It is meant for components
// that already serve HTTP health/info and just need to appear in the observer's live roster
// without a NATS broker. A blank base (COFISWARM_BRIDGE_URL unset) makes it a no-op, so it is
// safe to call unconditionally:
//
//	defer buspresence.StartPresence(os.Getenv("COFISWARM_BRIDGE_URL"), id, info)()
//
// Transport: if COFISWARM_ZMQ_PUBLISH_ADDR is set (e.g. tcp://zmq-bridge:5556) presence is
// published over a native ZMQ PUB socket to the bridge ingress wire and re-announced on a timer;
// otherwise it uses the HTTP control plane (/v1/publish) and re-announces on observer hello.
func StartPresence(base, id string, info map[string]any) (stop func()) {
	if addr := os.Getenv("COFISWARM_ZMQ_PUBLISH_ADDR"); addr != "" {
		return startPresenceZmq(addr, id, info)
	}
	if base == "" {
		return func() {}
	}
	p := New(base)
	announce := func() { p.Announce(id, info) }
	announce()
	ctx, cancel := context.WithCancel(context.Background())
	go p.WatchHello(ctx, announce)
	return func() {
		cancel()
		p.Goodbye(id) // synchronous goodbye so the roster clears before the process exits
	}
}

// WatchHello re-announces (via the given callback) whenever the observer broadcasts hello.
// The callback is invoked at hello time, so a reloaded roster is reflected. Reconnects with
// capped backoff until ctx is cancelled.
func (p *Publisher) WatchHello(ctx context.Context, reannounce func()) {
	url := p.base + "/v1/subscribe?topic=" + contract.SubjHello
	backoff := time.Second
	for ctx.Err() == nil {
		if err := p.streamHello(ctx, url, reannounce); err != nil && ctx.Err() == nil {
			log.Printf("buspresence: hello watch error: %v (retry %s)", err, backoff)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (p *Publisher) streamHello(ctx context.Context, url string, reannounce func()) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := (&http.Client{}).Do(req) // no timeout: long-lived SSE
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "data:") {
			log.Printf("buspresence: hello received -> re-announcing")
			reannounce()
		}
	}
	return sc.Err()
}
