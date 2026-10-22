#!/usr/bin/env bash
# 仅在完整验证通过后发布；已有 Release 保留原样，上传失败的草稿保留供排查。
set -euo pipefail

: "${GH_TOKEN:?GitHub token is required}"
: "${GITHUB_REPOSITORY:?repository is required}"
: "${RELEASE_TAG:?release tag is required}"
: "${RELEASE_COMMIT:?release commit is required}"
: "${RELEASE_ASSETS:?release assets are required}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"
python3 scripts/release_package.py check-tag --tag "$RELEASE_TAG"
python3 scripts/release_package.py verify --directory "$RELEASE_ASSETS" \
  --commit "$RELEASE_COMMIT" --tag "$RELEASE_TAG" --require-full

server_commit="$(gh api "repos/$GITHUB_REPOSITORY/commits/$RELEASE_TAG" --jq .sha)"
test "$server_commit" = "$RELEASE_COMMIT"
if gh release view "$RELEASE_TAG" --repo "$GITHUB_REPOSITORY" >/dev/null 2>&1; then
  echo 'Release already exists; refusing to replace existing assets or publication state.' >&2
  exit 1
fi

gh release create "$RELEASE_TAG" "$RELEASE_ASSETS"/* \
  --repo "$GITHUB_REPOSITORY" --verify-tag --draft --title "SelfIPCs $RELEASE_TAG" \
  --notes 'Linux x86_64 C++17 IPC SDK with SPSC latest-value channels, bounded MPMC queues and shared buffer pools. Version 0.5.0 adds queue close/cancellation/batches, Pool waiting, optional eventfd/driver notifications and a source-build C++20 Asio component. Includes the installed SDK, code-only source archive, SHA256SUMS and build manifest. Debug, ASan/UBSan, the complete C01-C23 crash matrix, relocated downstream consumers and benchmark integrity checks run on Ubuntu 22.04. Real-driver lifecycle/concurrency and three-backend measurements were separately validated in the Linux VM described in README.md. KASAN/lockdep are deferred driver-enhancement checks. Verify downloads with sha256sum -c SHA256SUMS. Usage, ABI 3 migration and measurement conditions are in README.md.'

download_dir="$(mktemp -d "${RUNNER_TEMP:-/tmp}/selfipcs-release-verify.XXXXXXXX")"
gh release download "$RELEASE_TAG" --repo "$GITHUB_REPOSITORY" --dir "$download_dir"
python3 scripts/release_package.py verify --directory "$download_dir" \
  --commit "$RELEASE_COMMIT" --tag "$RELEASE_TAG" --require-full
server_commit="$(gh api "repos/$GITHUB_REPOSITORY/commits/$RELEASE_TAG" --jq .sha)"
test "$server_commit" = "$RELEASE_COMMIT"
gh release edit "$RELEASE_TAG" --repo "$GITHUB_REPOSITORY" --draft=false --latest
