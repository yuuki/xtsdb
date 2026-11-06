# Releasing xtsdb

Releases are cut from `master` by pushing a version tag. The
[release workflow](../.github/workflows/release.yml) then:

1. runs the unit tests and extracts the release notes for the version from
   [CHANGELOG.md](../CHANGELOG.md) (it fails if the version has no section);
2. builds `xtsdb` for Linux and macOS on amd64 and arm64 with
   [GoReleaser](../.goreleaser.yaml), and creates the GitHub release with the
   archives, `sha256sums.txt` and the release notes;
3. builds the container image for `linux/amd64` and `linux/arm64` and pushes
   it to `ghcr.io/yuuki/xtsdb` as `X.Y.Z`, `X.Y` and `X`, plus `latest` for
   releases that are not pre-releases.

Binaries and images report the version (without the leading `v`) and the
commit in `xtsdb version`, `/api/v1/status/buildinfo` and the
`xtsdb_build_info` metric.

## Versions

xtsdb follows [Semantic Versioning](https://semver.org/). Its public
interface is the storage format (Redis keys, Cassandra schema), the
configuration file and the command line:

- a **major** version may change the storage format in a way that needs a
  migration, or remove configuration options or commands;
- a **minor** version adds features and options; data written by an older
  minor version keeps working;
- a **patch** version only fixes bugs.

Pre-releases are tagged `vX.Y.Z-rc.N` and need their own CHANGELOG.md
section (`## [X.Y.Z-rc.N] - YYYY-MM-DD`); GitHub marks them as pre-releases
and the image is not tagged `latest`, `X.Y` or `X`. A release candidate can
be promoted by tagging the same commit `vX.Y.Z`.

The tags are not Go module versions: xtsdb is an application, and its module
path has no `/v2` suffix, so `go install github.com/yuuki/xtsdb/cmd/xtsdb@vX.Y.Z`
does not work. Install a release binary or the image, or build from a
checkout of the tag with `make build`.

## Steps

1. Make sure `master` is green in CI (unit and integration tests, lint,
   image build).
2. In `CHANGELOG.md`, rename `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD`,
   add a new empty `## [Unreleased]` section above it, and update the links
   at the bottom:

   ```markdown
   [Unreleased]: https://github.com/yuuki/xtsdb/compare/vX.Y.Z...HEAD
   [X.Y.Z]: https://github.com/yuuki/xtsdb/compare/vPREVIOUS...vX.Y.Z
   ```

   (for the first release: `[2.0.0]: https://github.com/yuuki/xtsdb/releases/tag/v2.0.0`).
   Check the notes with `scripts/release-notes.sh X.Y.Z`.
3. Commit the changelog on `master`, then tag that commit and push the tag:

   ```bash
   git tag -a vX.Y.Z -m "xtsdb X.Y.Z"
   git push origin vX.Y.Z
   ```

4. Watch the workflow. The image is pushed only after the GitHub release
   was created. If the workflow fails before the release exists, nothing
   was published: fix the cause, delete the tag (`git push --delete origin
   vX.Y.Z`, `git tag -d vX.Y.Z`) and tag again. If it fails after, re-run the
   failed job. Never move a tag that was published.
5. The first time an image is pushed, the `xtsdb` package on ghcr.io is
   private: make it public in the package settings
   (<https://github.com/users/yuuki/packages/container/xtsdb/settings>).

A local dry run of the binaries, without publishing anything, is
`make snapshot` (needs GoReleaser v2); it writes to `dist/`.
