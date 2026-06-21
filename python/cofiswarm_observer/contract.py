"""Single source of truth for the cofiswarm observer bus wire contract (Python port of
cofiswarm-observer-sdk/pkg/contract). Mirrors the schema version and the swarm.observer.*
subjects so Python and Go components speak exactly the same protocol.
"""
from __future__ import annotations

SCHEMA_VERSION = "1.0.0"
PREFIX = "swarm.observer"

SUBJ_ANNOUNCE = f"{PREFIX}.announce"  # component -> bus: join / re-announce
SUBJ_GOODBYE = f"{PREFIX}.goodbye"    # component -> bus: graceful leave
SUBJ_HELLO = f"{PREFIX}.hello"        # observer -> components: re-announce
TOPIC_PRESENCE = f"{PREFIX}.presence" # bus -> observers: online/offline
TOPIC_ALERT = f"{PREFIX}.alert"       # bus -> observers: alerts


def major(version: str) -> int:
    """Return the major component of a dotted version, or 0 if unparseable."""
    try:
        return int(str(version).split(".", 1)[0])
    except (ValueError, TypeError):
        return 0


def major_supported(msg: dict) -> bool:
    """Whether a decoded message is compatible with SCHEMA_VERSION. A message without a
    schema_version is tolerated (legacy); a mismatched major is rejected."""
    version = msg.get("schema_version")
    if version is None:
        return True
    return major(version) == major(SCHEMA_VERSION)
