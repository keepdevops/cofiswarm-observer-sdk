package buspresence

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// collect runs fn against a stub bridge and returns every published envelope.
func collect(t *testing.T, fn func(p *Publisher)) []map[string]any {
	t.Helper()
	var msgs []map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/publish" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Errorf("bad publish body: %v", err)
			return
		}
		msgs = append(msgs, m)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer ts.Close()
	fn(New(ts.URL))
	return msgs
}

func TestAnnouncePublishesOnlinePresence(t *testing.T) {
	msgs := collect(t, func(p *Publisher) {
		p.Announce("dispatch", map[string]any{"name": "dispatch", "engine": "orchestrator"})
	})
	if len(msgs) != 1 || msgs[0]["topic"] != "swarm.observer.presence" {
		t.Fatalf("msgs = %v", msgs)
	}
	payload := msgs[0]["payload"].(map[string]any)
	if payload["component_id"] != "dispatch" || payload["status"] != "online" {
		t.Fatalf("payload = %v", payload)
	}
	if info := payload["info"].(map[string]any); info["engine"] != "orchestrator" {
		t.Fatalf("info = %v", info)
	}
}

func TestGoodbyePublishesOfflinePresence(t *testing.T) {
	msgs := collect(t, func(p *Publisher) { p.Goodbye("dispatch") })
	payload := msgs[0]["payload"].(map[string]any)
	if payload["component_id"] != "dispatch" || payload["status"] != "offline" {
		t.Fatalf("payload = %v", payload)
	}
	if _, hasInfo := payload["info"]; hasInfo {
		t.Fatalf("offline presence should omit info: %v", payload)
	}
}

func TestStartPresenceAnnouncesThenGoodbye(t *testing.T) {
	var mu sync.Mutex
	var msgs []map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/publish" { // ignore the WatchHello /v1/subscribe long-poll
			w.WriteHeader(http.StatusNotFound)
			return
		}
		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		mu.Lock()
		msgs = append(msgs, m)
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer ts.Close()

	stop := StartPresence(ts.URL, "mode-flat", map[string]any{"name": "mode-flat"})
	stop() // synchronous goodbye

	mu.Lock()
	defer mu.Unlock()
	if len(msgs) < 2 {
		t.Fatalf("want online+offline, got %v", msgs)
	}
	first := msgs[0]["payload"].(map[string]any)
	last := msgs[len(msgs)-1]["payload"].(map[string]any)
	if first["status"] != "online" || first["component_id"] != "mode-flat" {
		t.Errorf("first should be online mode-flat: %v", first)
	}
	if last["status"] != "offline" || last["component_id"] != "mode-flat" {
		t.Errorf("last should be offline mode-flat: %v", last)
	}
}

func TestStartPresenceBlankBaseIsNoop(t *testing.T) {
	StartPresence("", "x", nil)() // no base => no-op; stop must be safe to call
}

func TestAlertPublishesAlert(t *testing.T) {
	msgs := collect(t, func(p *Publisher) { p.Alert("dispatch", `mode "flat" unavailable`) })
	if msgs[0]["topic"] != "swarm.observer.alert" {
		t.Fatalf("topic = %v", msgs[0]["topic"])
	}
	payload := msgs[0]["payload"].(map[string]any)
	if payload["component_id"] != "dispatch" || payload["message"] == "" {
		t.Fatalf("payload = %v", payload)
	}
}

func TestAnnounceAndGoodbyeMembers(t *testing.T) {
	members := []Member{
		{ID: "agent-architect", Info: map[string]any{"name": "architect"}},
		{ID: "agent-coder", Info: map[string]any{"name": "coder"}},
	}
	online := collect(t, func(p *Publisher) { p.AnnounceMembers(members) })
	if len(online) != 2 {
		t.Fatalf("got %d announce publishes, want 2", len(online))
	}
	if p0 := online[0]["payload"].(map[string]any); p0["component_id"] != "agent-architect" || p0["status"] != "online" {
		t.Fatalf("payload[0] = %v", p0)
	}

	offline := collect(t, func(p *Publisher) { p.GoodbyeMembers(members) })
	if len(offline) != 2 {
		t.Fatalf("got %d goodbye publishes, want 2", len(offline))
	}
	if p1 := offline[1]["payload"].(map[string]any); p1["component_id"] != "agent-coder" || p1["status"] != "offline" {
		t.Fatalf("payload[1] = %v", p1)
	}
}
