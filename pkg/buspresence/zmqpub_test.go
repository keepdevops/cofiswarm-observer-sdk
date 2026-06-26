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
