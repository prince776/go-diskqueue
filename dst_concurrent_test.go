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

// The contract with several producers and consumers at once: each
// consumer receives each producer's messages in the order they were put;
// a message is delivered to one consumer (again only after a crash, and
// never if its consumption was synced); every acked, surviving message is
// delivered; and once the queue has been drained without a crash, Depth()
// is 0. With one consumer, deliveries are also checked for gaps (as in
// dst_test.go) and a peeked message must be the next one delivered.
//
// With several consumers the order of handoffs isn't observable: a
// consumer records a message after its receive returns, and another
// consumer may receive and record the next one first. So the gap and peek
// checks need a single consumer.

const producers = 2

// pmessage is producer p's message seq: p, seq, then filler from both.
func pmessage(p int, seq uint64) []byte {
	b := append([]byte{byte(p)}, binary.BigEndian.AppendUint64(nil, seq)...)
	for i := range int(seq % 41) {
		b = append(b, byte(seq*7+uint64(p)*13+uint64(i)))
	}
	return b
}

type pmodel struct {
	next       [producers]uint64
	acked      [producers]map[uint64]bool
	lastAcked  [producers]uint64
	durable    [producers]uint64
	mayLose    [producers]map[uint64]bool
	delivered  [producers]map[uint64]int
	last       [producers]uint64
	lastBoot   [producers]uint64
	syncedRead [producers]uint64
	restarted  [producers]bool
	clean      [producers]bool
	handoff    bool   // after a crash: one message may be lost in the handoff
	peeked     []byte // the last message peeked, until the next delivery
	crashes    int
	drained    bool

	strict        bool                       // one consumer: gaps and peeks are checked
	lastC         map[int]*[producers]uint64 // per consumer: last delivered per producer
	lastCBoot     map[int]*[producers]int    // per consumer and producer: crashes when lastC was set
	deliveredBoot [producers]map[uint64]int  // crashes when each message was last delivered
}

func newPModel(consumers int) *pmodel {
	m := &pmodel{strict: consumers == 1, lastC: map[int]*[producers]uint64{}, lastCBoot: map[int]*[producers]int{}}
	for p := range producers {
		m.next[p] = 1
		m.acked[p], m.mayLose[p], m.delivered[p] = map[uint64]bool{}, map[uint64]bool{}, map[uint64]int{}
		m.deliveredBoot[p] = map[uint64]int{}
	}
	for c := range consumers {
		m.lastC[c], m.lastCBoot[c] = &[producers]uint64{}, &[producers]int{}
	}
	return m
}

func (m *pmodel) onSync() {
	for p := range producers {
		m.durable[p] = m.lastAcked[p]
		m.syncedRead[p] = max(m.syncedRead[p], m.lastBoot[p])
	}
}

func (m *pmodel) crashed() {
	for p := range producers {
		for s := m.durable[p] + 1; s < m.next[p]; s++ {
			if m.acked[p][s] {
				m.mayLose[p][s] = true
			}
		}
		m.restarted[p] = true
		m.lastBoot[p] = 0
	}
	m.handoff = true
	m.peeked = nil
	m.crashes++
}

func (m *pmodel) deliver(c int, data []byte) error {
	if len(data) < 9 || int(data[0]) >= producers {
		return fmt.Errorf("garbage: %x", data)
	}
	p, q := int(data[0]), binary.BigEndian.Uint64(data[1:9])
	if q == 0 || q >= m.next[p] || !bytes.Equal(data, pmessage(p, q)) {
		return fmt.Errorf("garbage: delivered %x, which was never put", data)
	}
	if m.strict && m.peeked != nil && !bytes.Equal(m.peeked, data) {
		return fmt.Errorf("[peek] peeked %x but the next delivery was %d/%d", m.peeked, p, q)
	}
	m.peeked = nil
	if m.delivered[p][q] > 0 {
		if m.crashes == m.deliveredBoot[p][q] {
			return fmt.Errorf("redelivered %d/%d without a crash since it was delivered", p, q)
		}
		if q <= m.syncedRead[p] {
			return fmt.Errorf("redelivered %d/%d, whose consumption was synced (up to %d)", p, q, m.syncedRead[p])
		}
	}
	if last := m.lastC[c][p]; q <= last && m.lastCBoot[c][p] == m.crashes {
		return fmt.Errorf("consumer %d got %d/%d after %d/%d, with no crash between", c, p, q, p, last)
	}
	if m.strict && q > m.last[p] {
		for r := m.last[p] + 1; r < q; r++ {
			if !m.acked[p][r] || m.mayLose[p][r] || m.delivered[p][r] > 0 {
				continue
			}
			if m.handoff {
				m.handoff = false // the one message the crash may have lost in the handoff
				m.mayLose[p][r] = true
				continue
			}
			if m.clean[p] {
				return fmt.Errorf("lost %d/%d across a clean close and reopen (delivered %d next)", p, r, q)
			}
			return fmt.Errorf("lost %d/%d: acked and synced, never delivered (delivered %d next)", p, r, q)
		}
	}
	m.delivered[p][q]++
	m.deliveredBoot[p][q] = m.crashes
	m.lastC[c][p], m.lastCBoot[c][p] = q, m.crashes
	m.last[p] = max(m.last[p], q)
	m.lastBoot[p] = max(m.lastBoot[p], q)
	m.restarted[p], m.clean[p] = false, false
	return nil
}

func (m *pmodel) missing() []string {
	var out []string
	for p := range producers {
		for s := uint64(1); s < m.next[p]; s++ {
			if m.acked[p][s] && m.delivered[p][s] == 0 && !m.mayLose[p][s] {
				out = append(out, fmt.Sprintf("%d/%d", p, s))
			}
		}
	}
	return out
}

func concurrentHarness(h *dsttest.Harness, consumers int, expect ...string) {
	m := newPModel(consumers)
	boots := 0
	h.Machine("q", func(w *dst.World) {
		boots++
		if boots > 1 {
			m.crashed()
			h.Sometimes("restarted after a crash")
		}
		logf := func(lvl diskqueue.LogLevel, f string, args ...interface{}) {
			msg := fmt.Sprintf(f, args...)
			if strings.Contains(msg, "saving bad file") {
				h.Sometimes("bad file set aside")
				if strictFiles {
					h.Failf("a file was set aside as bad with no disk faults: %s", msg)
				}
			}
			w.Logf("%v: %s", lvl, msg)
		}
		open := func() diskqueue.Interface {
			return diskqueue.NewWorld(w, diskqueue.Options{OnSync: m.onSync}, "q", ".",
				maxBytesPerFile, minMsgSize, maxMsgSize, syncEvery, syncTimeout, logf)
		}
		var mu dst.RWMutex // reopening takes it exclusively
		q := open()
		current := func() diskqueue.Interface { mu.RLock(); defer mu.RUnlock(); return q }

		// Workload features (swarm testing: each seed picks its pace).
		bursty := h.Feature("bursty-producers")   // no pause between puts
		lagging := h.Feature("lagging-consumers") // a backlog builds, across files
		frequent := h.Feature("frequent-reopens") // clean closes every 20-200ms
		var wg dst.WaitGroup
		for p := range producers {
			wg.Add(1)
			w.Go(func() {
				defer wg.Done()
				r := w.Rand()
				for h.Phase() == dsttest.Faulty {
					seq := m.next[p]
					m.next[p]++
					if err := current().Put(pmessage(p, seq)); err == nil {
						m.acked[p][seq] = true
						m.lastAcked[p] = seq
					}
					if bursty && r.IntN(10) != 0 {
						w.Sleep(time.Duration(r.IntN(1000)) * time.Microsecond) // in a burst
					} else if bursty {
						w.Sleep(time.Duration(20+r.IntN(80)) * time.Millisecond) // between bursts
					} else {
						w.Sleep(time.Duration(r.IntN(15)) * time.Millisecond)
					}
				}
			})
		}
		quiet := 0 // consumers that have seen the queue quiet in recovery
		for c := range consumers {
			wg.Add(1)
			w.Go(func() {
				defer wg.Done()
				r := w.Rand()
				for {
					cur := current()
					var data []byte
					got, peek := false, consumers == 1 && r.IntN(5) == 0
					ch := cur.ReadChan()
					if peek {
						ch = cur.PeekChan()
					}
					dst.Select(w,
						dst.OnRecv(ch, func(b []byte, _ bool) { data, got = b, true }),
						dst.OnRecv(w.After(200*time.Millisecond), func(time.Time, bool) {}))
					if !got {
						// Quiet only counts on the current queue: a wait
						// on one closed meanwhile proves nothing.
						if h.Phase() != dsttest.Faulty && cur == current() {
							quiet++
							return
						}
						continue
					}
					if peek {
						h.Sometimes("peeked")
						m.peeked = data
						continue
					}
					if err := m.deliver(c, data); err != nil {
						h.Fail(err)
						return
					}
					switch {
					case lagging && h.Phase() == dsttest.Faulty: // catching up in recovery
						w.Sleep(time.Duration(r.IntN(60)) * time.Millisecond)
					case r.IntN(4) == 0:
						w.Sleep(time.Duration(r.IntN(20)) * time.Millisecond)
					}
				}
			})
		}
		// Clean closes and reopens while producers and consumers run.
		r := w.Rand()
		for h.Phase() == dsttest.Faulty {
			if frequent {
				w.Sleep(time.Duration(20+r.IntN(180)) * time.Millisecond)
			} else {
				w.Sleep(time.Duration(200+r.IntN(800)) * time.Millisecond)
			}
			if h.Phase() != dsttest.Faulty {
				break
			}
			mu.Lock()
			q.Close()
			for p := range producers {
				m.clean[p] = true
			}
			m.peeked = nil
			q = open()
			mu.Unlock()
			h.Sometimes("clean close and reopen")
		}
		wg.Wait()
		if quiet == consumers {
			m.drained = true
			if d := current().Depth(); d != 0 {
				if m.crashes == 0 {
					h.Failf("[depth] drained with no crash, but Depth() = %d", d)
				} else {
					h.Sometimes("Depth() wrong once drained after a crash")
				}
			}
		}
	})
	h.Final("every surviving message was delivered", func(w *dst.World) error {
		if !m.drained {
			return fmt.Errorf("the queue never drained")
		}
		if miss := m.missing(); len(miss) > 0 {
			return fmt.Errorf("never delivered %d acked, surviving messages (first %s)", len(miss), miss[0])
		}
		return nil
	})
	h.Expect(append([]string{"clean close and reopen"}, expect...)...)
}

// TestDSTConcurrentNoFaults: two producers and two consumers at once,
// scheduling only.
func TestDSTConcurrentNoFaults(t *testing.T) {
	strictFiles = true
	defer func() { strictFiles = false }()
	dsttest.Run(t, opts(dsttest.Faults{}), func(h *dsttest.Harness) { concurrentHarness(h, 2) })
}

// TestDSTConcurrentCrashes: the same with crashes, power loss and torn
// writes.
func TestDSTConcurrentCrashes(t *testing.T) {
	dsttest.Run(t, opts(dsttest.Faults{Crash: true, Disk: true}), func(h *dsttest.Harness) {
		concurrentHarness(h, 2, "restarted after a crash")
	})
}

// TestDSTProducersPeekNoFaults: two producers, one consumer that also
// peeks: deliveries are checked for gaps and peeks exactly.
func TestDSTProducersPeekNoFaults(t *testing.T) {
	strictFiles = true
	defer func() { strictFiles = false }()
	dsttest.Run(t, opts(dsttest.Faults{}), func(h *dsttest.Harness) { concurrentHarness(h, 1, "peeked") })
}

// TestDSTProducersPeekCrashes: the same with crashes, power loss and torn
// writes.
func TestDSTProducersPeekCrashes(t *testing.T) {
	dsttest.Run(t, opts(dsttest.Faults{Crash: true, Disk: true}), func(h *dsttest.Harness) {
		concurrentHarness(h, 1, "peeked", "restarted after a crash")
	})
}

// TestDSTConcurrentFixedPace: TestDSTConcurrentNoFaults at the original,
// fixed pace (every workload feature off), a floor under the bugs the pace
// features make rarer (lagging consumers hide the #38 bug).
func TestDSTConcurrentFixedPace(t *testing.T) {
	strictFiles = true
	defer func() { strictFiles = false }()
	o := opts(dsttest.Faults{})
	o.Features = map[string]bool{"*": false}
	dsttest.Run(t, o, func(h *dsttest.Harness) { concurrentHarness(h, 2) })
}
