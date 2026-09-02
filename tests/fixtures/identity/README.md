# Identity cryptography fixture

`rsa-private.pkcs1.der.b64` is a repository-public RSA test key used only to create deterministic
JWT signatures in unit tests. It is intentionally not secret and must never be accepted by an
installation, identity provider, release, image, or conformance environment.

Production OIDC trust comes exclusively from the configured issuer, audience, discovery document,
allowed JWKS origin, selected asymmetric algorithm, and fetched public keys. No runtime code reads
this directory.
