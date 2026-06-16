ROLE := observer-sdk
.PHONY: test test-standalone-layout test-gate
test: test-standalone-layout test-gate
test-standalone-layout:
	./test/scripts/assert-layout.sh $(ROLE)
test-gate:
	test -f examples/structlog_setup.py
	test -f examples/sink-agent-logs.yaml
	echo "ok: observer-sdk examples"
