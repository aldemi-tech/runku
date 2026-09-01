# Security Policy

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use GitHub private vulnerability
reporting for this repository. Include affected versions, prerequisites, impact, and a minimal
reproduction when possible.

The maintainers will acknowledge a valid report, coordinate a fix, and publish remediation details
after affected users have a reasonable opportunity to upgrade.

## Supported versions

Runku is pre-1.0. Security fixes are provided for the latest published `0.x` release and current
`main`; older `0.x` versions do not receive a guaranteed maintenance window. The affected release
and required upgrade are stated in each advisory.

## Scope

Reports about cross-Project access, credential disclosure, sandbox escape, SSRF, artifact tampering,
signature bypass, unsafe restore or upgrade behavior, and realtime authorization are in scope.

Also in scope: credential-role/JWT/JWKS bypass, pre-commit disclosure, cross-runtime escalation,
queue/replay isolation, malicious source/artifacts, diagnostics/backup secret leakage, and supply
chain integrity.

Reports include commit/version, topology/prerequisites, exact boundary, minimal reproduction,
expected/actual behavior, impact, and confidentiality needs. Remove real secrets and user/private
infrastructure data. Do not test systems/data without permission or perform destructive availability
testing.

Maintainers reproduce privately, scope affected versions, add regression/adversarial tests, fix the
owning contract, assess migration/deployment impact, and publish upgrade/mitigation guidance before
coordinated disclosure.
