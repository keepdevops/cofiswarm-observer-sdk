"""Pure unit tests for the wire contract (no NATS broker needed)."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from cofiswarm_observer import contract  # noqa: E402


def test_major_gate():
    assert contract.major_supported({}) is True, "unversioned tolerated"
    assert contract.major_supported({"schema_version": "1.4.0"}) is True, "matching major passes"
    assert contract.major_supported({"schema_version": "2.0.0"}) is False, "future major rejected"


def test_major_unparseable_is_zero():
    assert contract.major("garbage") == 0
    assert contract.major(None) == 0


def test_subjects_derive_from_prefix():
    assert contract.SUBJ_ANNOUNCE == "swarm.observer.announce"
    assert contract.SUBJ_GOODBYE == "swarm.observer.goodbye"
    assert contract.TOPIC_PRESENCE == "swarm.observer.presence"


if __name__ == "__main__":
    test_major_gate()
    test_major_unparseable_is_zero()
    test_subjects_derive_from_prefix()
    print("ok")
