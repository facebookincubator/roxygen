# Contributing to Roxygen

## Development process

Roxygen is developed in Meta's internal repository and exported to this one.
A pull request is reviewed here, then imported into the internal repository,
where it goes through internal review and CI before it lands. The change then
appears here with you as its author.

## Pull requests

1. Fork the repository and branch from `main`.
2. Add tests for new behavior, and update the docs when you change an API.
3. Run the checks that CI runs:

   ```sh
   cargo build --workspace --all-targets
   cargo test --workspace
   cargo clippy --workspace --all-targets
   cargo doc --workspace --no-deps
   ```

4. Format with `cargo +nightly fmt`. Some settings in `rustfmt.toml`, such as
   import grouping, are only available in nightly rustfmt.
5. If you have not already, complete the Contributor License Agreement.

## Contributor License Agreement ("CLA")

We need a signed CLA before we can accept your pull request. You only need to
sign it once for any of Meta's open source projects:
<https://code.facebook.com/cla>

## Issues

We use GitHub issues to track public bugs. Please include the Roxygen commit,
your platform and Rust version, and steps to reproduce the problem.

Do not file a public issue for a security bug. See [SECURITY.md](SECURITY.md).

## License

By contributing to Roxygen, you agree that your contributions will be licensed
under the [MIT license](LICENSE).
