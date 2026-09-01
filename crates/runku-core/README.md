# runku-core

Stable domain contracts shared by Runku components: typed identifiers, Environment purpose and
protection, canonical Channel and Workspace references, code targets, and server-side target-policy
validation.

This crate is internal to the workspace and intentionally has no storage, network, or runtime
dependency.

Changes to ID syntax, Environment policy, target parsing, or scope validation are public/
compatibility changes even though the crate is internal. Add property/adversarial tests and update
protocol/client/CLI/docs together. Higher layers may depend on core; core never depends on adapters.
