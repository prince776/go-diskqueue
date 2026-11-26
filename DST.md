# Deterministic simulation testing

This fork runs go-diskqueue on a [go-dst-kit](https://github.com/prince776/go-dst-kit)
World, so its tests can run it in a deterministic simulation: the queue's
goroutine, channels, timers and files are simulated, and each seed picks
a goroutine schedule, crash times and disk faults. A failing seed replays
exactly:

```sh
go test -run TestDST                                   # 300 seeds each
go test -run TestDST -dst.seeds=20000 -dst.first=random
go test -run '^TestDSTCrashes$' -dst.seed=32 -dst.v    # replay one seed with its event log
```

`New` is unchanged for callers (it runs on the real World, rooted at
`dataPath`); `NewWorld` takes a World and `Options` (`OnSync`, a hook
after each successful sync). `ReadChan` and `PeekChan` return
`dst.Chan`; `Raw()` is the native channel.

## The contract the tests hold it to

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

The one exemption: after a crash, the message being handed to the
consumer at that moment may be lost.

`TestDSTNoFaults` explores scheduling only; `TestDSTCrashes` adds process
crashes with power loss (unsynced writes lost), torn writes and power loss
inside I/O calls.

## What it found

Each was fixed in its own commit, and each is caught again if its fix is
reverted:

| Bug | Effect | Found at |
|---|---|---|
| No directory fsync after renaming the metadata into place | power loss can undo the rename: synced consumption redelivered | crash sweep, seed 1 |
| `Close` persisted metadata without fsyncing the data it covers | after a later power loss, acked and synced messages lost | crash sweep, seed 32 |
| A data file left by a crash before the first sync was reused in place | the reader took its stale tail for corruption: new messages lost | crash sweep, seed 236 |
| Reading past the end of a file the writer had just rolled away from (upstream #38) | a false corruption report and a `.bad` file | no-fault sweep, seed 18 |

Also seen: after crash recovery `Depth()` counts only what the synced
metadata knew of, so it under-reports until the queue is drained.
