// Package contract is the single source of truth for the cofiswarm observer bus wire
// contract: the schema version, the swarm.observer.* subjects/topics, and the schema-major
// compatibility gate. It has no third-party dependencies, so both the NATS service component
// and the HTTP bridge presence client share it without dragging in a NATS client.
package contract

import (
	"encoding/json"
	"strconv"
	"strings"
)

// SchemaVersion is the wire schema version every component stamps and gates on. Bumping the
// major rejects messages from components still on the old major (see MajorSupported). This
// constant is the one place the version is defined — repos must not hardcode their own copy.
const SchemaVersion = "1.0.0"

// Prefix is the observer control-plane subject prefix.
const Prefix = "swarm.observer"

// Subjects and topics carried on the observer bus.
const (
	SubjAnnounce  = Prefix + ".announce" // component -> bus: join / re-announce
	SubjGoodbye   = Prefix + ".goodbye"  // component -> bus: graceful leave
	SubjHello     = Prefix + ".hello"    // observer -> components: re-announce
	TopicPresence = Prefix + ".presence" // bus -> observers: online/offline
	TopicAlert    = Prefix + ".alert"    // bus -> observers: alerts
)

// Major returns the major component of a dotted version, or 0 if unparseable.
func Major(v string) int {
	n, _ := strconv.Atoi(strings.SplitN(v, ".", 2)[0])
	return n
}

// MajorSupported reports whether a decoded message is compatible with SchemaVersion. A message
// without a schema_version is tolerated (legacy); a mismatched major is rejected.
func MajorSupported(raw map[string]json.RawMessage) bool {
	v, ok := raw["schema_version"]
	if !ok {
		return true
	}
	var s string
	if json.Unmarshal(v, &s) != nil {
		return true
	}
	return Major(s) == Major(SchemaVersion)
}
