package diskqueue

import (
	"bytes"
	"os"
	"testing"
	"time"
)

// A process that crashes after writing but before the queue's first sync
// leaves a data file and no metadata. The data survives (the OS has it;
// no power loss is needed). On restart the queue starts at file 0,
// position 0 and writes over that file in place, leaving the old bytes
// after what it writes; the reader, reading ahead with a bufio.Reader,
// buffers those stale bytes and parses them instead of the new messages.
func TestRestartAfterCrashBeforeFirstSync(t *testing.T) {
	dir, err := os.MkdirTemp("", "dq-repro")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	l := NewTestLogger(t)

	// The first process: writes, never syncs (large syncEvery and timeout),
	// then "crashes": it is abandoned, never closed.
	old := New("q", dir, 1<<20, 0, 1<<10, 1<<30, time.Hour, l)
	for i := 0; i < 5; i++ {
		if err := old.Put(bytes.Repeat([]byte{'A'}, 100)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(old.(*diskQueue).metaDataFileName()); !os.IsNotExist(err) {
		t.Fatalf("the scenario needs no metadata yet: %v", err)
	}

	// The restarted process: puts ten small messages and reads them back.
	dq := New("q", dir, 1<<20, 0, 1<<10, 1<<30, time.Hour, l)
	defer dq.Close()
	for i := 0; i < 10; i++ {
		want := bytes.Repeat([]byte{'0' + byte(i)}, 10)
		if err := dq.Put(want); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-dq.ReadChan():
			if !bytes.Equal(got, want) {
				t.Fatalf("message %d: read %q, want %q", i, got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("message %d (put and acknowledged) was never delivered", i)
		}
	}
}
