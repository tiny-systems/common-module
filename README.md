# Tiny Systems Common Module

Core building blocks for flow-based automations on the Tiny Systems platform.

## Components

| Component | Description |
|-----------|-------------|
| Router | Conditional message routing based on expressions |
| Debug | Inspect and log messages passing through a flow |
| Cron | Schedule-based flow trigger (cron expressions) |
| Signal | Manual flow trigger with configurable context |
| Delay | Pause execution for a specified duration |
| Array Get | Extract elements from an array by index |
| Ticker | Interval-based recurring trigger |
| Group By | Group incoming messages by key |
| Async | Non-blocking message passthrough (fire-and-forget) |
| Split Array | Split an array into individual messages |
| Inject | Data enrichment -- merge additional data into messages |
| Modify | Transform and reshape message data |
| Key-Value Store | In-memory key-value storage for flow state |
| Retry | Explicit retry supervisor with bounded attempts and configurable backoff. Wire to the error port of `llm_*` / `http_request` / database components; routes back to the original input until success or attempt limit. |
| Ask a human | Presents a form and waits for a person to answer. The form is a JSON Schema you author, so the same component asks "approve this restart?" or "how many replicas?". Put it in front of anything destructive. |
| Budget Guard | Bounds an agent loop by iterations, tokens and cost. Emits on `proceed` while within budget, `exceeded` once past it. |
| Flow Telemetry | Reads the platform's own execution traces, so a flow can inspect how flows are running — list runs in a window, or fetch one run's hops to find where it broke. |

## Notes

**`ask` does not park the run.** A request publishes the form and returns; the
answer arrives later as a separate hop and starts the downstream branch itself.
Continuity is the published form, not a held message.

**`ask` renders through the generic JSON editor.** It is functional rather than
pretty — a dedicated form widget is not built yet. Because `build_flow`
validates against the component catalog rather than a node's live settings,
edges reading a custom form's own fields need a follow-up `configure_edge` once
the node has reconciled.

**`budget_guard` only counts if you thread its counters back.** Map
`iteration` / `spentTokens` / `spentUSD` from `proceed` into the guard on each
pass. Miss that and it reports iteration 1 forever — a guard that guards
nothing.

**`flow_telemetry` needs the otel-collector** the operator installs, and reads
it over its service address. Its scan is bounded: check `truncated` before
reading an empty `errorsOnly` result as "nothing failed".

## Installation

```shell
helm repo add tinysystems https://tiny-systems.github.io/module/
helm install common-module tinysystems/tinysystems-operator \
  --set controllerManager.manager.image.repository=ghcr.io/tiny-systems/common-module
```

## Run locally

```shell
go run cmd/main.go run --name=common-module --namespace=tinysystems --version=1.0.0
```

## Part of Tiny Systems

This module is part of the [Tiny Systems](https://github.com/tiny-systems) platform -- a visual flow-based automation engine running on Kubernetes.

## License

This module's source code is MIT-licensed. It depends on the [Tiny Systems Module SDK](https://github.com/tiny-systems/module) (BSL 1.1). See [LICENSE](LICENSE) for details.
