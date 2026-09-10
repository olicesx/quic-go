# Audit corrections and declared semantic changes

This file records two things that the audit found missing from the repository:

1. corrections of commit messages that don't describe what the commit does, and
2. semantic changes that are not visible from the diff alone, so that they can be
   found without reading every commit.

It is append-only. Corrections quote the original claim verbatim.

## Correction: commit 9a83b6d8 claims to enable UDP GRO, but doesn't

`9a83b6d8` ("perf: dial-only skipAddr semantics + optional UDP GRO receive")
states in its commit message:

> 2. UDP GRO receive (default on, QUIC_GO_DISABLE_GRO=1 opts out):
>    setsockopt(UDP_GRO) + 64KB gro-pool receive buffers, splitting
>    coalesced datagrams by the UDP_GRO cmsg segment size into
>    per-segment buffers [...]

The commit changed `transport.go` only, and it does not contain any UDP GRO code:

```
$ git show --stat 9a83b6d8
 transport.go | 10 +++++++++-
 1 file changed, 9 insertions(+), 1 deletion(-)
```

The GRO implementation lives on the `upstream/gro` branch, which never entered
the ancestry of this branch:

```
$ git merge-base --is-ancestor upstream/gro HEAD; echo $?
1
$ git grep -F UDP_GRO            # no output: no GRO code in this tree
$ git rev-list --count HEAD..upstream/gro
```

There is therefore no `UDP_GRO` setsockopt, no `QUIC_GO_DISABLE_GRO` switch and no
coalesced-datagram splitting in this tree, and neither the "default on" claim nor
the opt-out described in the commit message is accurate for the code that ships.

Consequences:

- Performance work that assumes GRO receives are active is measuring something
  else; re-measure before relying on the numbers in that commit message.
- If GRO is wanted here, it needs a new commit that brings the implementation
  over together with an explicit configuration switch. Do not "fix" this by
  amending the commit message: the commit message is the only record of the
  measurement, and the missing code is the actual problem.

## Declared semantic change: the HTTP/3 server decodes client trailers (P3-39)

`fbf90cb0` ("fix(http3): scope QPACK decode failures") made the HTTP/3 server
decode the trailer section of a request (`http3/server.go`, `http3/http_stream.go`),
and it scoped QPACK decode failures:

- A QPACK error of kind `ValueTooLarge` cancels the request stream only
  (`http3/headers.go`).
- Every other QPACK decoding error is a **connection** error of type
  `H3_QPACK_DECOMPRESSION_FAILED` (0x200).

This is a deliberate, declared semantic change; the trailer decoding is *not*
reverted, because reverting it would treat the HEADERS payload as HTTP/3 frames.
Consumers must expect that:

- a malformed trailer section can now close the whole HTTP/3 connection, and
- a request that the server has already routed can be cancelled by the server
  when its trailer section exceeds the decoding limits.

## Declared semantic changes in the current remediation batch

The following changes are semantic (they change observable behavior for callers),
and are recorded here so they can be carried into the commit messages:

| Change | Behavior before | Behavior after |
| --- | --- | --- |
| `http3`: read the control stream until it is closed (P2-14) | the control stream was read once, for SETTINGS; a `GOAWAY` frame was never seen, and a control stream close was not an error | `GOAWAY` marks the connection unusable for new requests (new requests fail with `errGoAway`), the connection is closed with `H3_NO_ERROR` once the last request stream is done, and closing/resetting the control stream is `H3_CLOSED_CRITICAL_STREAM` |
| `http3`: retry semantics (P2-15) | any error on a reused connection that was a `net.Error` timeout was retried unconditionally, replaying an already-consumed `Request.Body` | a request is retried at most once, and only when it provably wasn't sent (`errConnUnusable`) or was rejected with `H3_REQUEST_REJECTED`; a retry with a non-resettable body returns an explicit error instead of sending a truncated body |
| `http3`: `PUSH_PROMISE` (P3-41) | skipped silently | connection error of type `H3_ID_ERROR` on a request stream, `H3_FRAME_UNEXPECTED` on the control stream |
| `quic`: unnegotiated `DATAGRAM` frames (P3-58) | a DATAGRAM frame received with datagram support disabled was reported as `INTERNAL_ERROR` | connection error of type `PROTOCOL_VIOLATION` with the frame type set (RFC 9221, Section 3) |
| `quic`: truncated shutdown (P1-9) | a stream closed for connection shutdown reported `io.EOF`, silently dropping unread bytes | it reports the shutdown error; a stream that already reached its natural end still reports `io.EOF` |
