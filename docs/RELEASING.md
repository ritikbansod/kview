# Releasing Kview

Cutting a release is a version bump + one tag; the release workflow does the rest.

## One-time setup (per channel)

| Channel | Setup | Notes |
|---|---|---|
| Docker (GHCR) | nothing | pushes with `GITHUB_TOKEN` on every tag |
| Docker Hub | repo secrets `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN` (a Docker Hub access token), `DOCKERHUB_REPOSITORY` (e.g. `youruser/kview`) | the multi-arch image is **mirrored from GHCR** with `buildx imagetools` — same digests, no rebuild; the step is skipped while the secrets are absent |
| GitHub bundles | nothing | jar + Linux/macOS/Windows jpackage bundles + Linux `.deb` attach to the release |
| npm | add a repo secret `NPM_TOKEN` (an npm automation token for the `kview` package) | the `Publish the npm package` step is skipped while the secret is absent |
| Homebrew | create a tap repo (default `ritikbansod/homebrew-kview`) and add repo secrets `HOMEBREW_TAP_TOKEN` (a fine-grained PAT with write access to the tap) and optionally `HOMEBREW_TAP_REPO` | the formula is rendered from `packaging/homebrew/kview.rb.tmpl` and pushed to `tap/Formula/kview.rb` |
| SDKMAN | **deferred** — nothing to configure; submit later at https://vendors.sdkman.io (candidate `kview`, version, `https://github.com/ritikbansod/kview/releases/download/vX.Y.Z/kview-X.Y.Z-linux-x64.tar.gz`) if/when there is user demand | manual, one form per release; the release pipeline has no dependency on it |

## Release checklist

1. Bump the version in **three** places — the workflow guard enforces all of them:
   - `pom.xml` `<version>`
   - `cli/kview.mjs` `const VERSION`
   - `mcp/kview-mcp.js` `const VERSION`
2. Update `CHANGELOG.md` (move `[Unreleased]` to the version + date).
3. Commit, then `git tag vX.Y.Z && git push origin vX.Y.Z`.

## What the workflow produces

- **jar**: tested build (`mvn clean package`, tests green) attached to the release with checksums; tag/version guard fails the release on mismatch.
- **Docker**: `ghcr.io/ritikbansod/kview:{version,latest}` for `linux/amd64` + `arm64`. The image also serves the direct CLI: `docker run --rm ghcr.io/ritikbansod/kview --cli topics --bootstrap-server host:9092`.
- **Native bundles** (jpackage app-images with a bundled JRE — no Java install needed):
  - `kview-X.Y.Z-linux-x64.tar.gz` (+ `kview_X.Y.Z_amd64.deb`)
  - `kview-X.Y.Z-macos-aarch64.tar.gz` (unsigned — `xattr -cr kview.app` on first run if Gatekeeper complains)
  - `kview-X.Y.Z-windows-x64.zip`
  Each bundle contains the server launcher (`bin/kview` / `kview.exe`) and a `kview`/`kview-cli` direct-CLI wrapper.
- **npm**: the `kview` package (thin client + `kview-mcp`) is published with the release version; the dispatcher auto-uses a local jar for direct mode when present.

## After the release

1. Verify the release assets and the Docker tags.
2. Submit the SDKMAN candidate (see the table above).
3. Announce per `docs/RELEASE-PLAN.md`.
