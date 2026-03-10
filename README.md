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
