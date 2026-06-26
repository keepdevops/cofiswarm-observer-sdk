package buspresence

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/go-zeromq/zmq4"

	"github.com/keepdevops/cofiswarm-observer-sdk/pkg/contract"
)

// reAnnounceEvery re-publishes presence on a timer — the PUB/SUB analogue of the HTTP path's
// hello-watch, since a native PUB socket has no reply channel to receive hello on. It must stay
// well under the observer's liveness TTL (45s) so a healthy component never drops off the roster.
const reAnnounceEvery = 20 * time.Second

// goodbyeGrace gives the final "offline" PUB frame a moment to flush before the socket closes
// (PUB has no delivery handshake; closing immediately can drop the last frame).
const goodbyeGrace = 100 * time.Millisecond

// zmqPublisher publishes presence frames over a native ZMQ PUB socket to the bridge ingress wire
// (COFISWARM_ZMQ_PUBLISH_ADDR, e.g. tcp://zmq-bridge:5556) — the native-transport alternative to
// HTTP /v1/publish. Frames are multipart [topic, json-payload], matching the bridge ingress SUB.
type zmqPublisher struct {
	sock zmq4.Socket
	mu   sync.Mutex // serializes Send (zmq4 sockets are not concurrency-safe)
}

func (z *zmqPublisher) publish(id, status string, info map[string]any) {
	payload := map[string]any{"component_id": id, "status": status}
	if info != nil {
		payload["info"] = info
	}
	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("buspresence(zmq): marshal presence: %v", err)
		return
	}
	z.mu.Lock()
	defer z.mu.Unlock()
	if err := z.sock.Send(zmq4.NewMsgFrom([]byte(contract.TopicPresence), data)); err != nil {
		log.Printf("buspresence(zmq): publish %s for %s: %v", status, id, err)
	}
}

// startPresenceZmq announces id online over a native PUB socket, re-announces periodically, and
// returns a stop func that publishes offline and closes the socket. A dial failure logs and
// degrades to a no-op so callers stay safe (mirrors the HTTP path's blank-base behavior).
func startPresenceZmq(addr, id string, info map[string]any) func() {
	ctx, cancel := context.WithCancel(context.Background())
	sock := zmq4.NewPub(ctx)
	if err := sock.Dial(addr); err != nil {
		log.Printf("buspresence(zmq): dial %s: %v (presence disabled)", addr, err)
		cancel()
		return func() {}
	}
	z := &zmqPublisher{sock: sock}
	go z.announceLoop(ctx, id, info)
	log.Printf("buspresence(zmq): announcing %s via PUB %s", id, addr)
	return func() {
		cancel()
		z.publish(id, "offline", nil) // final goodbye so the roster clears before TTL
		time.Sleep(goodbyeGrace)
		_ = sock.Close()
	}
}

// announceLoop sends an initial burst (absorbing the PUB->SUB slow-joiner so the component
// appears promptly) then re-announces on reAnnounceEvery until ctx is cancelled.
func (z *zmqPublisher) announceLoop(ctx context.Context, id string, info map[string]any) {
	for i := 0; i < 3 && ctx.Err() == nil; i++ {
		z.publish(id, "online", info)
		select {
		case <-ctx.Done():
			return
		case <-time.After(150 * time.Millisecond):
		}
	}
	ticker := time.NewTicker(reAnnounceEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			z.publish(id, "online", info)
		}
	}
}
