package servicecomponent

import "testing"

// New picks the lexically-first subject as the announced primary, deterministically.
func TestNewPicksDeterministicPrimary(t *testing.T) {
	c := New(nil, "kv", "kvpool", map[string]Handler{
		"swarm.observer.kv.policy":   nil,
		"swarm.observer.kv.admit":    nil,
		"swarm.observer.kv.evaluate": nil,
	})
	if c.primary != "swarm.observer.kv.admit" {
		t.Fatalf("primary = %q, want swarm.observer.kv.admit", c.primary)
	}
	if c.cid != "kvpool" {
		t.Fatalf("cid = %q, want kvpool (defaults to kind)", c.cid)
	}
}

// An empty route set yields an empty primary rather than panicking.
func TestNewEmptyRoutes(t *testing.T) {
	if c := New(nil, "x", "x", nil); c.primary != "" {
		t.Fatalf("primary = %q, want empty", c.primary)
	}
}
