package buspresence

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-zeromq/zmq4"
)

// StartPresence in native-PUB mode publishes an online presence frame to the ingress wire that a
// SUB (standing in for the bridge ingress) receives as [topic, json-payload].
func TestStartPresenceZmqPublishesOnline(t *testing.T) {
	const addr = "tcp://127.0.0.1:55661"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := zmq4.NewSub(ctx)
	if err := sub.Listen(addr); err != nil {
		t.Skipf("cannot bind SUB: %v", err)
	}
	defer sub.Close()
	if err := sub.SetOption(zmq4.OptionSubscribe, "swarm."); err != nil {
		t.Fatal(err)
	}

	t.Setenv("COFISWARM_ZMQ_PUBLISH_ADDR", addr)
	stop := StartPresence("", "responder-test", map[string]any{"name": "test"})
	defer stop()

	got := make(chan map[string]any, 1)
	go func() {
		for {
			msg, err := sub.Recv()
			if err != nil || len(msg.Frames) < 2 {
				return
			}
			var p map[string]any
			if json.Unmarshal(msg.Frames[1], &p) == nil {
				got <- p
				return
			}
		}
	}()

	select {
	case p := <-got:
		if p["component_id"] != "responder-test" || p["status"] != "online" {
			t.Fatalf("presence payload = %v", p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("did not receive presence frame over PUB")
	}
}

// A Publisher built via New() with COFISWARM_ZMQ_PUBLISH_ADDR set routes Announce over the native
// PUB socket (the path dispatch/agent-registry use, not StartPresence).
func TestPublisherAnnounceOverZmq(t *testing.T) {
	const addr = "tcp://127.0.0.1:55663"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := zmq4.NewSub(ctx)
	if err := sub.Listen(addr); err != nil {
		t.Skipf("cannot bind SUB: %v", err)
	}
	defer sub.Close()
	if err := sub.SetOption(zmq4.OptionSubscribe, "swarm."); err != nil {
		t.Fatal(err)
	}

	t.Setenv("COFISWARM_ZMQ_PUBLISH_ADDR", addr)
	p := New("http://unused")
	if p.pub == nil {
		t.Fatal("New did not configure a PUB sender from the env")
	}

	got := make(chan map[string]any, 1)
	go func() {
		for {
			msg, err := sub.Recv()
			if err != nil || len(msg.Frames) < 2 {
				return
			}
			var pl map[string]any
			if json.Unmarshal(msg.Frames[1], &pl) == nil {
				got <- pl
				return
			}
		}
	}()

	// Re-announce a few times to clear the PUB->SUB slow joiner.
	for i := 0; i < 10; i++ {
		p.Announce("agent-registry", map[string]any{"name": "agent-registry"})
		select {
		case pl := <-got:
			if pl["component_id"] != "agent-registry" || pl["status"] != "online" {
				t.Fatalf("payload = %v", pl)
			}
			return
		case <-time.After(150 * time.Millisecond):
		}
	}
	t.Fatal("Publisher.Announce did not arrive over PUB")
}
