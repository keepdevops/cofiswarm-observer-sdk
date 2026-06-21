package contract

import (
	"encoding/json"
	"testing"
)

func TestMajorGate(t *testing.T) {
	mk := func(v string) map[string]json.RawMessage {
		return map[string]json.RawMessage{"schema_version": json.RawMessage(`"` + v + `"`)}
	}
	if !MajorSupported(map[string]json.RawMessage{}) {
		t.Fatal("unversioned should be tolerated")
	}
	if !MajorSupported(mk("1.4.0")) {
		t.Fatal("matching major should pass")
	}
	if MajorSupported(mk("2.0.0")) {
		t.Fatal("future major must be rejected")
	}
}

func TestSubjectsDeriveFromPrefix(t *testing.T) {
	if SubjAnnounce != "swarm.observer.announce" || SubjGoodbye != "swarm.observer.goodbye" {
		t.Fatalf("subjects drifted: %s %s", SubjAnnounce, SubjGoodbye)
	}
	if TopicPresence != "swarm.observer.presence" || TopicAlert != "swarm.observer.alert" {
		t.Fatalf("topics drifted: %s %s", TopicPresence, TopicAlert)
	}
}
