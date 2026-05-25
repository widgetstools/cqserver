# tests/cloud — replication test harness. See CLOUD_REPLICATION_TEST_WORKLOG.md.
#
# C0 (loopback docker-compose):
#   make build        — cargo build --release -p cq-server (host)
#   make local-up     — build + docker compose up -d
#   make local-test   — run convergence + failure-injection assertions
#   make local-down   — tear down + remove volumes
#   make local-logs   — tail every cqserver container's logs
#   make local-status — one-line summary per container
#
# Two-Mac lab (real-network test, $0): see lab/README.md.
#   make lab-help     — print the two-Mac setup procedure

REPO_ROOT := $(shell git rev-parse --show-toplevel 2>/dev/null || (cd ../.. && pwd))
COMPOSE   := docker compose -f docker-compose.local.yml
BINARY    := $(REPO_ROOT)/target/release/cqserver

.PHONY: build local-up local-test local-down local-logs local-status help lab-help

help:
	@awk 'BEGIN{FS=" — "} /^#   make/{sub(/^#   /,"  "); print}' $(MAKEFILE_LIST)

lab-help:
	@cat lab/README.md

build:
	@if [ ! -x "$(BINARY)" ] || [ "$(BINARY)" -ot "$(REPO_ROOT)/crates/cq-server/src/main.rs" ]; then \
		echo "→ cargo build --release -p cq-server"; \
		( cd "$(REPO_ROOT)" && cargo build --release -p cq-server ); \
	else \
		echo "→ binary up-to-date at $(BINARY)"; \
	fi

local-up: build
	@echo "→ docker compose up"
	@$(COMPOSE) up -d --build
	@echo "→ waiting for healthchecks…"
	@for svc in leader follower1 follower2; do \
		echo -n "  $$svc: "; \
		for _ in $$(seq 1 30); do \
			state=$$(docker inspect --format='{{.State.Health.Status}}' cq-c0-$$svc 2>/dev/null || echo unknown); \
			if [ "$$state" = "healthy" ]; then echo "$$state"; break; fi; \
			sleep 1; \
		done; \
		if [ "$$state" != "healthy" ]; then \
			echo "$$state — see 'make local-logs'"; exit 1; \
		fi; \
	done
	@echo "→ admin UIs:"
	@echo "    leader     http://127.0.0.1:8085/ui/"
	@echo "    follower1  http://127.0.0.1:8086/ui/"
	@echo "    follower2  http://127.0.0.1:8087/ui/"

local-test:
	@./scripts/assert-converged.sh

local-down:
	@$(COMPOSE) down -v --remove-orphans

local-logs:
	@$(COMPOSE) logs --tail=200 -f

local-status:
	@docker ps --filter "name=cq-c0-" \
	  --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
