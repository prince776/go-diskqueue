package diskqueue_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	diskqueue "github.com/nsqio/go-diskqueue"
	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
)

// The queue's contract, as these tests state it (diskqueue doesn't write
// it down):
//
//   - Every delivered message is one that was put, byte for byte.
//   - Messages are delivered in the order they were put. After a crash,
//     delivery may start again from an earlier message (consumption since
//     the last sync isn't durable), but never skips one.
//   - A message put before a sync completed survives a crash, unless it
//     was consumed before that sync. (Put returns before its data is
//     synced: messages put since the last sync may be lost.)
//   - A message whose consumption was synced is never delivered again.
//   - Close then reopen loses nothing and redelivers nothing.
//   - Once faults stop, every message that survived is delivered.
//
// One message is exempt from the no-skip rule after a crash: the one the
// queue was handing to the consumer when it crashed.

const (
	maxBytesPerFile = 1024 // small, so files roll often
	minMsgSize      = 8
	maxMsgSize      = 8 + 56
	syncEvery       = 10
	syncTimeout     = 50 * time.Millisecond
)

// message is seq's payload: the sequence number, then filler derived from
// it (0 to 56 bytes), so a delivery can be checked byte for byte.
func message(seq uint64) []byte {
	b := binary.BigEndian.AppendUint64(nil, seq)
	for i := range int(seq % 57) {
		b = append(b, byte(seq*7+uint64(i)))
	}
	return b
}

func seqOf(data []byte) uint64 {
	if len(data) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(data)
}

// model is what the test knows, kept outside the machine so it survives
// crashes.
type model struct {
	next       uint64          // next sequence number to put (from 1)
	acked      map[uint64]bool // Put returned nil
	lastAcked  uint64
	durable    uint64          // every acked seq up to here was synced
	mayLose    map[uint64]bool // acked but unsynced when a crash came
	delivered  map[uint64]int
	last       uint64 // the last seq delivered
	lastBoot   uint64 // the last seq delivered by this incarnation (0: none yet)
	syncedRead uint64 // consumption up to here was synced: never redelivered
	inFlight   uint64 // after a crash: the seq that may have been lost in the handoff
	restarted  bool   // a crash since the last delivery
	clean      bool   // a clean reopen since the last delivery
	drained    bool
}

func newModel() *model {
	return &model{next: 1, acked: map[uint64]bool{}, mayLose: map[uint64]bool{}, delivered: map[uint64]int{}}
}

// onSync: everything acked so far is durable (a Put still waiting may not
// have been written yet), and so is consumption up to this incarnation's
// last recorded delivery (deliveries from before a crash weren't).
func (m *model) onSync() {
	m.durable = m.lastAcked
	m.syncedRead = max(m.syncedRead, m.lastBoot)
}

// crashed: acked messages not yet synced may be gone, and so may the one
// being handed to the consumer.
func (m *model) crashed() {
	for s := m.durable + 1; s < m.next; s++ {
		if m.acked[s] {
			m.mayLose[s] = true
		}
	}
	m.inFlight = m.last + 1
	m.restarted = true
	m.lastBoot = 0
}

// deliver checks one delivery against the contract.
func (m *model) deliver(data []byte) error {
	if len(data) < 8 {
		return fmt.Errorf("garbage: a %d-byte message", len(data))
	}
	q := binary.BigEndian.Uint64(data)
	if q == 0 || q >= m.next || !bytes.Equal(data, message(q)) {
		return fmt.Errorf("garbage: delivered %x, which was never put", data)
	}
	switch {
	case q <= m.last:
		if !m.restarted {
			return fmt.Errorf("redelivered %d (last delivered %d) without a crash", q, m.last)
		}
		if q <= m.syncedRead {
			return fmt.Errorf("redelivered %d, whose consumption was synced (up to %d)", q, m.syncedRead)
		}
	default:
		for r := m.last + 1; r < q; r++ {
			if !m.acked[r] || m.mayLose[r] || m.delivered[r] > 0 || (r == m.inFlight && m.restarted) {
				continue
			}
			if m.clean {
				return fmt.Errorf("lost %d across a clean close and reopen (delivered %d next)", r, q)
			}
			return fmt.Errorf("lost %d: acked and synced, never delivered (delivered %d next)", r, q)
		}
	}
	m.delivered[q]++
	m.last, m.lastBoot = q, q
	m.restarted, m.clean = false, false
	return nil
}

// missing lists acked messages that survived but were never delivered.
func (m *model) missing() []uint64 {
	var out []uint64
	for s := uint64(1); s < m.next; s++ {
		if m.acked[s] && m.delivered[s] == 0 && !m.mayLose[s] && s != m.inFlight {
			out = append(out, s)
		}
	}
	return out
}

// strictFiles: with no disk faults nothing can corrupt a file, so a file
// set aside as bad is the queue's own mistake.
var strictFiles bool

func open(w *dst.World, h *dsttest.Harness, m *model) diskqueue.Interface {
	logf := func(lvl diskqueue.LogLevel, f string, args ...interface{}) {
		msg := fmt.Sprintf(f, args...)
		switch {
		case strings.Contains(msg, "opened") && strings.Contains(msg, "writeOne"):
			h.Sometimes("write file opened")
		case strings.Contains(msg, "saving bad file"):
			h.Sometimes("bad file set aside")
			if strictFiles {
				h.Failf("a file was set aside as bad with no disk faults: %s", msg)
			}
		case strings.Contains(msg, "skipping to new file"):
			h.Sometimes("recovery skipped to a new write file")
		}
		w.Logf("%v: %s", lvl, msg)
	}
	onSync := func() {
		m.onSync()
		w.Logf("model: synced; durable up to %d, consumption up to %d", m.durable, m.syncedRead)
	}
	return diskqueue.NewWorld(w, diskqueue.Options{OnSync: onSync}, "q", ".",
		maxBytesPerFile, minMsgSize, maxMsgSize, syncEvery, syncTimeout, logf)
}

// harness runs the queue on one machine with a workload that mixes puts,
// receives and clean reopens; the model checks every delivery.
func harness(h *dsttest.Harness, expect ...string) {
	m := newModel()
	boots := 0
	h.Machine("q", func(w *dst.World) {
		boots++
		if boots > 1 {
			m.crashed()
			h.Sometimes("restarted after a crash")
		}
		q := open(w, h, m)
		r := w.Rand()
		for {
			if h.Phase() != dsttest.Faulty {
				// Recovery: drain what survived, until the queue has been
				// quiet for a while. (Depth isn't the test: after a crash it
				// counts only what the synced metadata knew of.)
				var data []byte
				got := false
				dst.Select(w,
					dst.OnRecv(q.ReadChan(), func(b []byte, _ bool) { data, got = b, true }),
					dst.OnRecv(w.After(200*time.Millisecond), func(time.Time, bool) {}))
				if !got {
					if d := q.Depth(); d != 0 {
						h.Sometimes("Depth() wrong once drained")
						w.Logf("drained, but Depth() = %d", d)
					}
					m.drained = true
					return
				}
				w.Logf("model: delivered %d", seqOf(data))
				if err := m.deliver(data); err != nil {
					h.Fail(err)
					return
				}
				continue
			}
			switch x := r.IntN(100); {
			case x < 50:
				seq := m.next
				m.next++
				err := q.Put(message(seq))
				w.Logf("model: put %d: %v", seq, err)
				if err == nil {
					m.acked[seq] = true
					m.lastAcked = seq
				}
			case x < 98:
				var data []byte
				got := false
				dst.Select(w,
					dst.OnRecv(q.ReadChan(), func(b []byte, _ bool) { data, got = b, true }),
					dst.Default(nil))
				if got {
					w.Logf("model: delivered %d", seqOf(data))
					if err := m.deliver(data); err != nil {
						h.Fail(err)
						return
					}
				}
			default:
				q.Close()
				m.clean = true
				h.Sometimes("clean close and reopen")
				q = open(w, h, m)
			}
			if r.IntN(4) == 0 {
				w.Sleep(time.Duration(r.IntN(20)) * time.Millisecond)
			}
		}
	})
	h.Final("every surviving message was delivered", func(w *dst.World) error {
		if !m.drained {
			return fmt.Errorf("the queue never drained")
		}
		if miss := m.missing(); len(miss) > 0 {
			return fmt.Errorf("never delivered %d acked, surviving messages (first %d)", len(miss), miss[0])
		}
		return nil
	})
	h.Expect(append([]string{"write file opened", "clean close and reopen"}, expect...)...)
}

func opts(f dsttest.Faults) dsttest.Options {
	return dsttest.Options{Seeds: 300, Faults: f, FaultPhase: 5 * time.Second, RecoveryPhase: 5 * time.Second}
}

// TestDSTNoFaults: the contract with only scheduling explored.
func TestDSTNoFaults(t *testing.T) {
	strictFiles = true
	defer func() { strictFiles = false }()
	dsttest.Run(t, opts(dsttest.Faults{}), func(h *dsttest.Harness) { harness(h) })
}

// TestDSTCrashes: process crashes with power loss (unsynced data lost),
// torn writes and power loss inside I/O calls.
func TestDSTCrashes(t *testing.T) {
	dsttest.Run(t, opts(dsttest.Faults{Crash: true, Disk: true}), func(h *dsttest.Harness) {
		harness(h, "restarted after a crash")
	})
}
