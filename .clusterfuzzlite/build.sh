#!/bin/bash
set -euo pipefail

export CXX="${CXX:-} -lresolv" # required by Go 1.20

# Fail closed if the module path changed: the fuzzer targets below are
# module paths, and a stale path would silently disable fuzzing.
go list -m | grep -qx 'github.com/olicesx/quic-go' || {
  echo "unexpected module path: $(go list -m)" >&2
  exit 1
}

compile_go_fuzzer github.com/olicesx/quic-go/fuzzing/frames Fuzz frame_fuzzer
compile_go_fuzzer github.com/olicesx/quic-go/fuzzing/header Fuzz header_fuzzer
compile_go_fuzzer github.com/olicesx/quic-go/fuzzing/transportparameters Fuzz transportparameter_fuzzer
compile_go_fuzzer github.com/olicesx/quic-go/fuzzing/tokens Fuzz token_fuzzer
compile_go_fuzzer github.com/olicesx/quic-go/fuzzing/handshake Fuzz handshake_fuzzer
