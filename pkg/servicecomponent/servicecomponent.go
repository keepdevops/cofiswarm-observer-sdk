// Package servicecomponent is the canonical NATS "service component" for cofiswarm: it
// announces presence on the observer bus, serves a set of capability subjects behind a
// schema-major gate with loud-error replies, re-announces on hello, and says goodbye on
// shutdown. It replaces the byte-identical per-repo internal/bus copies (launcher,
// slot-manager, kvpool) with one shared source, so the wire schema can no longer drift.
package servicecomponent

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"

	"github.com/nats-io/nats.go"

	"github.com/keepdevops/cofiswarm-observer-sdk/pkg/contract"
)

// Re-exported so callers that only build replies/subjects need just this package.
const (
	SchemaVersion = contract.SchemaVersion
	Prefix        = contract.Prefix
)

type modelInfo struct {
	Name   string   `json:"name"`
	Engine string   `json:"engine"`
	Tags   []string `json:"tags,omitempty"`
}

type announceMsg struct {
	SchemaVersion string    `json:"schema_version"`
	ComponentID   string    `json:"component_id"`
	Kind          string    `json:"kind"`
	Info          modelInfo `json:"info"`
	InferSubject  string    `json:"infer_subject"`
}

type goodbyeMsg struct {
	SchemaVersion string `json:"schema_version"`
	ComponentID   string `json:"component_id"`
	Reason        string `json:"reason"`
}

// Handler decodes a request and returns a reply struct; a non-nil error is replied loud.
type Handler func(data []byte) (any, error)

// Component is a generic bus capability process (the Go ServiceComponent).
type Component struct {
	nc      *nats.Conn
	name    string
	kind    string
	cid     string
	routes  map[string]Handler
	primary string
}

// Connect opens a NATS connection with infinite reconnect (broker-bounce resilience).
func Connect(url, name string) (*nats.Conn, error) {
	return nats.Connect(url, nats.Name(name), nats.MaxReconnects(-1))
}

// New builds a component serving routes. The lexically-first subject is the announced primary.
func New(nc *nats.Conn, name, kind string, routes map[string]Handler) *Component {
	keys := make([]string, 0, len(routes))
	for s := range routes {
		keys = append(keys, s)
	}
	sort.Strings(keys) // deterministic primary subject for the announce
	primary := ""
	if len(keys) > 0 {
		primary = keys[0]
	}
	return &Component{nc: nc, name: name, kind: kind, cid: kind, routes: routes, primary: primary}
}

// Start subscribes the routes plus hello (for re-announce) and publishes the initial announce.
func (c *Component) Start() error {
	for subj, h := range c.routes {
		if _, err := c.nc.Subscribe(subj, c.makeCB(subj, h)); err != nil {
			return fmt.Errorf("subscribe %s: %w", subj, err)
		}
	}
	if _, err := c.nc.Subscribe(contract.SubjHello, func(*nats.Msg) { c.announce() }); err != nil {
		return fmt.Errorf("subscribe hello: %w", err)
	}
	c.announce()
	log.Printf("%s (%s) serving %d subjects (id=%s)", c.name, c.kind, len(c.routes), c.cid)
	return nil
}

func (c *Component) makeCB(subj string, h Handler) nats.MsgHandler {
	return func(m *nats.Msg) {
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(m.Data, &raw); err != nil {
			log.Printf("%s dropped malformed message on %s", c.name, subj)
			c.replyErr(m, "invalid json")
			return
		}
		if !contract.MajorSupported(raw) {
			log.Printf("%s rejected %s: unsupported schema_version", c.name, subj)
			c.replyErr(m, "unsupported schema_version")
			return
		}
		reply, err := h(m.Data)
		if err != nil {
			log.Printf("%s handler %s failed: %v", c.name, subj, err)
			c.replyErr(m, err.Error())
			return
		}
		b, err := json.Marshal(reply)
		if err != nil {
			log.Printf("%s marshal reply failed on %s: %v", c.name, subj, err)
			c.replyErr(m, "marshal reply")
			return
		}
		_ = m.Respond(b)
	}
}

func (c *Component) replyErr(m *nats.Msg, msg string) {
	b, _ := json.Marshal(map[string]any{
		"schema_version": contract.SchemaVersion, "ok": false, "error": msg,
	})
	_ = m.Respond(b)
}

func (c *Component) announce() {
	a := announceMsg{
		SchemaVersion: contract.SchemaVersion, ComponentID: c.cid, Kind: c.kind,
		Info:         modelInfo{Name: c.name, Engine: c.kind, Tags: []string{"resource"}},
		InferSubject: c.primary,
	}
	if b, err := json.Marshal(a); err == nil {
		_ = c.nc.Publish(contract.SubjAnnounce, b)
	}
}

// Shutdown publishes a graceful goodbye so presence flips offline quietly.
func (c *Component) Shutdown() {
	b, _ := json.Marshal(goodbyeMsg{SchemaVersion: contract.SchemaVersion, ComponentID: c.cid, Reason: "shutdown"})
	_ = c.nc.Publish(contract.SubjGoodbye, b)
}
