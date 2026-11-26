# go-diskqueue, under deterministic simulation

A fork of [nsqio/go-diskqueue](https://github.com/nsqio/go-diskqueue),
NSQ's filesystem-backed FIFO queue, made to run under deterministic
simulation testing (DST) with [go-dst-kit](https://github.com/prince776/go-dst-kit).
It is an experiment, not a maintained replacement.

## Why fork it

go-dst-kit lets ordinary goroutine-and-channel Go code run in a
deterministic simulation: goroutine order, time, crashes and disk faults
are all chosen by a seed, so a failure replays exactly. This fork is its
first adoption by a real project: a small library (about 700 lines) that
waits on goroutines, channels and timers, writes files, fsyncs, and makes
durability promises that crashes and power loss can break.

## What the fork does

- Runs the queue on a go-dst-kit World: real files and goroutines in
  production, simulated ones in tests.
- States the queue's durability contract (upstream doesn't write it
  down) and tests it under crashes, power loss and torn writes.
- Fixes the four bugs those tests found.

## Migration

The changes are mechanical; the queue's logic is untouched.

| Upstream | Fork |
|---|---|
| `go d.ioLoop()` | `w.Go(d.ioLoop)` |
| `chan` and `select` | `dst.Chan` and `dst.Select` |
| `sync.RWMutex`, `time.NewTicker` | `dst.RWMutex`, `w.NewTicker` |
| `os.OpenFile`, `Remove`, `Rename`, `Stat` | the World's filesystem, `w.FS()` |
| `math/rand` | `w.Rand()` |

API:

- `New(name, dataPath, ...)` keeps its signature and runs on the real
  World, rooted at `dataPath`.
- `NewWorld(w, opts, name, dataPath, ...)` takes a World; `dataPath` is
  relative to its filesystem. `Options.OnSync` is called after each
  successful sync (the tests use it to know what is durable).
- `ReadChan()` and `PeekChan()` return `dst.Chan[[]byte]`; in production,
  `.Raw()` is the native `<-chan []byte` to select on.
- Go 1.25 (go-dst-kit's minimum).

Two frictions found, both now handled by go-dst-kit (v0.2.0): APIs that
return channels (`Chan.Raw`), and World filesystems taking only relative
paths (so `New` roots a World at `dataPath`).

The upstream tests pass on the real World exactly as before.

## The contract

- Every delivered message is one that was put, byte for byte.
- Messages are delivered in the order they were put. After a crash,
  delivery may restart from an earlier message, but never skips one.
- A message put before a sync completed survives a crash, unless it was
  consumed before that sync. (`Put` returns before its data is synced:
  messages put since the last sync may be lost.)
- A message whose consumption was synced is never delivered again.
- Close then reopen loses nothing and redelivers nothing.
- With no disk faults, no file is ever set aside as bad.
- Once faults stop, every surviving message is delivered.

One exemption: after a crash, the message being handed to the consumer
at that moment may be lost.

## Tests

`dst_test.go` runs a workload of puts, receives and clean reopens on a
simulated machine, against a model that lives outside the machine (so it
survives crashes) and checks every delivery against the contract.

- `TestDSTNoFaults` explores goroutine scheduling only.
- `TestDSTCrashes` adds process crashes with power loss (unsynced writes
  lost), torn writes, and power loss inside I/O calls.
- `TestDSTConcurrent*` run two producers and two consumers at once, with
  clean reopens; `TestDSTProducersPeek*` two producers and one consumer
  that also peeks (exact checks on gaps and peeks need one consumer: with
  two, the order of handoffs isn't observable).

```sh
go test -run TestDST                                   # 300 seeds each, a few seconds
go test -run TestDST -dst.seeds=20000 -dst.first=random
go test -run '^TestDSTCrashes$' -dst.seed=32 -dst.v    # replay one seed with its event log
```

Both pass 2,000 random seeds (about 5.5 simulated hours each), and the
whole suite passes with `-race`.

## Bugs found

Each was fixed in its own commit, and each is caught again if its fix is
reverted.

| # | Bug | Effect | Found by |
|---|---|---|---|
| 1 | No directory fsync after renaming the metadata into place | power loss can undo the rename: consumption that was synced is redelivered (duplicates; NSQ is at-least-once) | crash sweep, seed 1 |
| 2 | `Close` persisted metadata without fsyncing the data it covers | power loss soon after a clean shutdown loses acknowledged messages | crash sweep, seed 32 |
| 3 | A data file left by a crash before the first sync was reused in place | after restart, the reader parses the old run's leftover bytes, declares the file corrupt, and acknowledged messages are lost | crash sweep, seed 236 |
| 4 | Reading past the end of a file the writer just rolled away from ([#38](https://github.com/nsqio/go-diskqueue/issues/38)) | a false corruption report and a `.bad` file; nothing lost | no-fault sweep, seed 18 |
| 5 | After a read error the writer moves to the next file number at offset 0, writing over a file a crashed run left there | the reader takes the old run's torn tail for corruption and sets the file aside: an acked, synced message is lost (bug 3's mechanism, by another path) | concurrent crash sweep, seed 4741809724240498 (two crashes in a row) |

**Bug 3 needs no power loss.** Any crash (a panic, an OOM kill) in a new
queue's first sync interval (by default its first 2 seconds or 2,500
messages) leaves data without metadata. It reproduces on upstream
`master` with real files: `TestRestartAfterCrashBeforeFirstSync` on the
`upstream-fixes` branch reads `invalid message read size (1094795585)`,
which is the old run's `AAAA`, and loses the restarted queue's messages.

Bug 4 is a known upstream issue; its test (`TestWriteRollReadEOF`) fails
on upstream `master` and passes here. The DST sweep found it without
being told where to look.

Also seen: after crash recovery, `Depth()` counts only what the synced
metadata knew of, so it under-reports until the queue is drained.

## Benchmark

The five bugs can be re-inserted one at a time (build tag `dstbugs`,
`DSTBUG=<name>`), and `dstbench.json` lists them with the sweeps that
should catch each, for go-dst-kit's `dstbench` (bug 5 held out).
Baseline, 300 seeds per sweep: `rename-without-dir-sync` 45% of seeds,
`close-without-fsync` 9.3% (the concurrent sweep: 0%),
`reuse-file-without-metadata` 0.3% (concurrent: 0%), and
`read-past-rolled-file` 4% (concurrent: 100%). With each bug re-inserted,
its sweep first fails at the seed that originally found it.

## Branches

- `master`: the go-dst-kit migration, the fixes and the DST tests.
- `upstream-fixes`: the same five fixes on upstream `master`, in
  upstream's style (plain `os`, no go-dst-kit), with the bug 3
  reproduction test, ready to propose upstream.

## Not covered yet

- Corruption of data already on disk (bit rot): the queue has no
  checksums, so a flipped bit is delivered as a real message.
- `Depth()` after a crash (checked only for runs without one).

---

Upstream: *A Go package providing a filesystem-backed FIFO queue. Pulled
out of https://github.com/nsqio/nsq*
