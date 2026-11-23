# Changelog

Notable changes to Roxygen are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Roxygen has not made a
release yet, and its APIs may change in any commit until it does.

## Unreleased

The first public version of Roxygen:

- HTTP/1.1 and HTTP/2 client and server, with worker-local connections.
- An HTTP/1 parser generated from llhttp 9.4.4, and native HTTP/2 framing,
  HPACK and flow control.
- TCP and rustls-based TLS transports, byte events, and `MSG_ZEROCOPY` sends on
  Linux.
- Adapters for the `http`, `http-body` and Tower crates.
- HTTP CONNECT tunnels, and a curl-like command-line client.

### Changed

- TLS uses the aws-lc-rs crypto provider by default instead of ring, and offers
  the post-quantum X25519MLKEM768 key exchange first. Every crate that reaches
  Roxygen's TLS has `aws-lc-rs` and `ring` features. With
  `default-features = false` and `ring`, Roxygen uses ring and does not build
  aws-lc-rs.
