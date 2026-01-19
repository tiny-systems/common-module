# Claude Code Rules for Common Module

## Code Style

- Early returns, no nested ifs
- Extract logic into small, focused functions
- Flat structure over deep nesting
- Idiomatic Go - if err != nil { return } pattern

## Component Design

- Handle() switch cases should be minimal - delegate to functions
- No JSON parsing in components - SDK handles deserialization
- No knowledge of other modules' metadata keys
