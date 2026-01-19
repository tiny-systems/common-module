# Tiny Systems Common Module

Contains basic components for flow-based programming.

## Components

### Signal

Flow trigger with blocking output. Click Send to emit context on Out port and start the flow. The Out port is a **blocking port** - Signal keeps running (edge animated) until the connected component stops or Reset is clicked.

**Use cases:**
- Start/stop HTTP servers
- Control long-running operations
- Lifecycle management of downstream components

### Blocking Edges

The `Blocking: true` flag on a port creates a persistent, lifecycle-controlled connection:

| Aspect | Regular Edge | Blocking Edge |
|--------|--------------|---------------|
| Wait behavior | Waits for response (ms-seconds) | Waits until target stops (minutes-hours) |
| Persistence | None | TinyState in K8s |
| Survives restarts | No | Yes |
| Cascade deletion | No | Yes (source deleted → target stops) |
| Edge animation | Brief | Continuous while active |

**When to use Blocking:**
- Target runs continuously (servers, watchers, connections)
- Source controls target's lifecycle
- Need restart tolerance
- Want cascade deletion

**When to use Regular:**
- Data processing (request → transform → response)
- One-time operations
- Fast request-response patterns

**Example:**
```
Signal (blocking) → HTTP Server    # Server runs while Signal is active
Cron (regular) → Debug            # Each tick is independent
```

## Build locally

```shell
go run cmd/main.go tools build --devkey abcd11111e --name github.com/tiny-systems/common-module --version v1.0.5 --platform-api-url http://localhost:8281
```

## Run locally

```shell
OTLP_DSN=http://test.token@localhost:2345 HOSTNAME=pod2 go run cmd/main.go run --name localsecond/common-module-v1 --namespace=tinysystems --version=1.0.5
```
