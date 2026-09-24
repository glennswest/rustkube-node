#!/usr/bin/env bash
# The project's build under sc-build: `sc-build scripts/sc-build.sh [cmd]`.
#
# The workspace depends on `../rustkube/pkg/apimachinery`, a sibling checkout
# (Cargo.toml, `apimachinery`). sc-build builds in a fresh scratch directory
# with no sibling, and the build box must not keep a checkout. So this clones
# rustkube *inside* the scratch tree and points the path there. The edit is
# made to the scratch copy of Cargo.toml and is deleted with it. The committed
# manifest still names the sibling, which is what a developer checkout has.
#
# RUSTKUBE_REF picks the rustkube commit or branch (default: main).
# With no arguments it runs `cargo build && cargo test`.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
ref="${RUSTKUBE_REF:-main}"
if [[ ! -d ../rustkube/pkg/apimachinery ]]; then
  rm -rf .deps/rustkube
  git init -q .deps/rustkube
  git -C .deps/rustkube fetch -q --depth 1 https://github.com/glennswest/rustkube "$ref"
  git -C .deps/rustkube checkout -q FETCH_HEAD
  echo "sc-build.sh: rustkube@$(git -C .deps/rustkube rev-parse --short HEAD) for apimachinery"
  sed -i 's#path = "\.\./rustkube/#path = ".deps/rustkube/#' Cargo.toml
  # A path dependency under the workspace directory is taken as a member of
  # it, and apimachinery would then inherit *this* workspace's dependencies.
  # Excluded, it finds its own workspace root, which is .deps/rustkube.
  sed -i 's#^\[workspace\]$#[workspace]\nexclude = [".deps"]#' Cargo.toml
fi
cmd="${*:-cargo build && cargo test}"
bash -c "$cmd"
