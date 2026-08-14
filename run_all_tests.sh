#!/usr/bin/env bash
#
# Runs the test suite. The integration tests set up a veth pair, which
# needs root.
#
# Run this WITHOUT sudo. It builds as the invoking user, so nothing
# under target/ ends up root owned, and elevates only the test binaries
# themselves.
#
# Any arguments are forwarded to every test binary, so
#
#     ./run_all_tests.sh --nocapture
#     ./run_all_tests.sh device_can_be_bound_again
#
# work as they would under `cargo test`.

set -uo pipefail

# Build first, with cargo's usual output, so that a compile error is
# reported as one.
cargo test --no-run || exit 1

# Then ask cargo which binaries the current sources built to. Globbing
# target/debug/deps instead would also turn up binaries left behind by
# earlier builds - they linger under different hash suffixes rather
# than being replaced - and silently run stale code. Filtering on the
# test profile keeps the examples, which are also built by `cargo test`
# and would otherwise be run as if they were tests, out of the list.
mapfile -t bins < <(
    cargo test --no-run --message-format=json 2>/dev/null |
        grep '"test":true' |
        grep -o '"executable":"[^"]*"' |
        cut -d'"' -f4
)

if [ "${#bins[@]}" -eq 0 ]; then
    echo "run_all_tests.sh: no test binaries found" >&2
    exit 1
fi

# Prime sudo's credential cache up front, so a password prompt cannot
# appear in the middle of a test binary's output.
sudo -v || exit 1

status=0
failed=()

for bin in "${bins[@]}"; do
    echo
    echo "=== ${bin##*/} ==="

    # -E so that RUST_LOG and the like survive into the test.
    if ! sudo -E "$bin" "$@"; then
        status=1
        failed+=("${bin##*/}")
    fi
done

echo

if [ "$status" -ne 0 ]; then
    echo "run_all_tests.sh: ${#failed[@]} test binary/binaries failed:" >&2
    printf '  %s\n' "${failed[@]}" >&2
fi

exit "$status"
