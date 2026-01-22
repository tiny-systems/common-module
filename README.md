# Tiny Systems Common Module

Contains basic components for flow-based programming.

## Components

### Signal

Flow trigger. Click Send to emit context on Out port.

**Use cases:**
- Trigger flows manually
- Send configuration to downstream components

## Build locally

```shell
go run cmd/main.go tools build --devkey abcd11111e --name github.com/tiny-systems/common-module --version v1.0.5 --platform-api-url http://localhost:8281
```

## Run locally

```shell
OTLP_DSN=http://test.token@localhost:2345 HOSTNAME=pod2 go run cmd/main.go run --name localsecond/common-module-v1 --namespace=tinysystems --version=1.0.5
```
