# Contributing to xtsdb

Thank you for helping. Bug reports and pull requests are welcome on GitHub;
for larger changes, open an issue first to agree on the approach, in
particular for anything that changes the storage format, the configuration
or the command line (see [docs/releasing.md](docs/releasing.md#versions)).
Security issues go through [SECURITY.md](SECURITY.md), not public issues.

## Making a change

1. Read the parts of [docs/architecture.md](docs/architecture.md) that your
   change touches: the hand-off between the tiers relies on invariants that
   tests alone do not make obvious.
2. Add or update tests with the change. Parsers and decoders have fuzz
   targets; protocol changes in the hot tier need an integration test.
3. Run `make check` (lint, workflow lint, unit tests with the race detector,
   cross builds). If you touched the hot tier, the flusher or a backend, also
   run `make test-integration` (Docker) or `make test-tikv`.
4. Describe user-visible changes in [CHANGELOG.md](CHANGELOG.md) under
   `## [Unreleased]`, and update README.md and `examples/xtsdb.yaml` when
   behaviour or options change.

Commit subjects are short imperative sentences that say what changes
("Split the hot tier's Append into its three steps"), with the reason in the
body.

[AGENTS.md](AGENTS.md) lists every command, test suite and convention in
detail.
