package sludge

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
)

func uuid1() uuid.UUID { return uuid.New() }

const (
	lineA model.LineID = "line-1"
	lineB model.LineID = "line-2"
	pumpA              = "pump-line-1-A"
	pumpB              = "pump-line-1-B"
)

func newTestBook(t *testing.T) (*SequenceBook, *PumpController, *journal.CompensationQueue, time.Time) {
	t.Helper()
	now := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	pumps, err := NewPumpController([]Pump{
		{ID: pumpA, LineID: lineA, Kind: model.DevicePump},
		{ID: pumpB, LineID: lineA, Kind: model.DevicePump},
		{ID: "pump-line-2-A", LineID: lineB, Kind: model.DevicePump},
	})
	if err != nil {
		t.Fatalf("pump controller: %v", err)
	}
	queue := journal.NewCompensationQueue()
	book, err := NewSequenceBook(pumps, queue)
	if err != nil {
		t.Fatalf("sequence book: %v", err)
	}
	return book, pumps, queue, now
}

func pumpState(c *PumpController, id string) Pump {
	for _, p := range c.Snapshot() {
		if p.ID == id {
			return p
		}
	}
	return Pump{}
}

func runDue(t *testing.T, book *SequenceBook, queue *journal.CompensationQueue, now time.Time) {
	t.Helper()
	if _, err := queue.ExecuteDue(context.Background(), now, func(ctx context.Context, item journal.Compensation) error {
		return book.ExecuteCompensation(ctx, item, now)
	}); err != nil {
		t.Fatalf("execute compensation: %v", err)
	}
}

// A normal current-generation sequence starts, confirms flow and runs.
func TestSequence_LegitFlowRuns(t *testing.T) {
	book, pumps, _, now := newTestBook(t)

	seq, err := book.Start(lineA, pumpA, 7, 1, now)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	confirmed, err := book.ConfirmFlow(lineA, seq.ID, seq.Generation, 12.5, now.Add(time.Second))
	if err != nil {
		t.Fatalf("confirm flow: %v", err)
	}
	if confirmed.Status != SequenceRunning {
		t.Fatalf("status = %s, want running", confirmed.Status)
	}
	p := pumpState(pumps, pumpA)
	if !p.Running || p.Flow != 12.5 {
		t.Fatalf("pump not running with confirmed flow: %+v", p)
	}
}

// The core regression: emergency isolation right after creation, then a late
// positive-flow receipt for the old generation must not flip the sequence to
// running nor restart the isolated pump. A genuinely newer generation still runs.
func TestSequence_LateReceiptAfterIsolationRejected(t *testing.T) {
	book, pumps, queue, now := newTestBook(t)

	old, err := book.Start(lineA, pumpA, 7, 1, now) // switching, gen 1
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	if _, err := book.Isolate(lineA, 2, now.Add(2*time.Second)); err != nil { // gen 1 -> 2
		t.Fatalf("isolate: %v", err)
	}
	if p := pumpState(pumps, pumpA); p.Running || p.FenceGeneration != 2 {
		t.Fatalf("pump not fenced/stopped on isolation: %+v", p)
	}
	if got := len(queue.Pending()); got != 1 {
		t.Fatalf("pending compensations = %d, want 1", got)
	}

	// Late positive-flow receipt carrying the old generation.
	if _, err := book.ConfirmFlow(lineA, old.ID, old.Generation, 9.0, now.Add(3*time.Second)); err == nil {
		t.Fatal("stale positive-flow receipt after isolation was accepted")
	}
	if p := pumpState(pumps, pumpA); p.Running || p.Flow != 0 {
		t.Fatalf("old-generation receipt restarted isolated pump: %+v", p)
	}
	if cur, ok := book.Current(lineA); !ok || cur.Status != SequenceFailed {
		t.Fatalf("old sequence status = %v, want failed", cur.Status)
	}

	// A future era (generation above the fence) is accepted and still runs.
	newSeq, err := book.Start(lineA, pumpB, 8, 3, now.Add(4*time.Second))
	if err != nil {
		t.Fatalf("start new-generation sequence: %v", err)
	}
	if _, err := book.ConfirmFlow(lineA, newSeq.ID, newSeq.Generation, 11.0, now.Add(5*time.Second)); err != nil {
		t.Fatalf("confirm new-generation flow: %v", err)
	}
	if p := pumpState(pumps, pumpB); !p.Running || p.Flow != 11.0 {
		t.Fatalf("new-generation pump did not run: %+v", p)
	}

	// Starting a sequence inside the exact isolated (fence) generation is rejected.
	if _, err := book.Start(lineA, pumpA, 9, 2, now.Add(6*time.Second)); err == nil {
		t.Fatal("sequence at isolated generation was accepted")
	}
}

// An old-era stop compensation must never disturb a pump that has since been
// claimed by a newer era, and must not jam the compensation queue.
func TestCompensation_OldEraDoesNotDisturbNewPump(t *testing.T) {
	book, pumps, queue, now := newTestBook(t)

	old, err := book.Start(lineA, pumpA, 1, 1, now)
	if err != nil {
		t.Fatalf("start old: %v", err)
	}
	if _, err := book.ConfirmFlow(lineA, old.ID, 1, 8.0, now); err != nil {
		t.Fatalf("confirm old: %v", err)
	}

	if _, err := book.Isolate(lineA, 2, now); err != nil {
		t.Fatalf("isolate: %v", err)
	}
	if p := pumpState(pumps, pumpA); p.Running {
		t.Fatal("pump still running right after isolation")
	}

	// Same pump is claimed and run by a newer era.
	newSeq, err := book.Start(lineA, pumpA, 2, 3, now.Add(time.Second))
	if err != nil {
		t.Fatalf("start new era on same pump: %v", err)
	}
	if _, err := book.ConfirmFlow(lineA, newSeq.ID, 3, 14.0, now.Add(time.Second)); err != nil {
		t.Fatalf("confirm new era: %v", err)
	}

	// Old-era compensation falls due: it must no-op (ownership moved), drain
	// from the queue without error and leave the new-era pump running.
	runDue(t, book, queue, now.Add(2*time.Second))
	if len(queue.Pending()) != 0 {
		t.Fatalf("compensation queue not drained: %d pending", len(queue.Pending()))
	}
	if p := pumpState(pumps, pumpA); !p.Running || p.Flow != 14.0 {
		t.Fatalf("old-era compensation disturbed newer pump: %+v", p)
	}
}

func TestAssign_RejectsCrossLineAndStale(t *testing.T) {
	_, pumps, _, now := newTestBook(t)

	if _, err := pumps.Assign(lineB, pumpA, uuid1(), 1, now); err == nil {
		t.Fatal("cross-line pump assignment accepted")
	}
	if _, err := pumps.Assign(lineA, pumpA, uuid1(), 1, now); err != nil {
		t.Fatalf("initial assignment: %v", err)
	}
	if _, err := pumps.Assign(lineA, pumpA, uuid1(), 1, now); err == nil {
		t.Fatal("same-generation takeover accepted")
	}
	pumps.IsolateLine(lineA, 2, now)
	if _, err := pumps.Assign(lineA, pumpA, uuid1(), 2, now); err == nil {
		t.Fatal("assignment at fenced generation accepted")
	}
	if _, err := pumps.Assign(lineA, pumpA, uuid1(), 3, now); err != nil {
		t.Fatalf("newer-than-fence assignment rejected: %v", err)
	}
}

func TestIsolate_Idempotent(t *testing.T) {
	book, _, queue, now := newTestBook(t)
	if _, err := book.Start(lineA, pumpA, 1, 1, now); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := book.Isolate(lineA, 2, now); err != nil {
		t.Fatalf("isolate: %v", err)
	}
	if _, err := book.Isolate(lineA, 2, now.Add(time.Second)); err != nil {
		t.Fatalf("re-isolate same generation: %v", err)
	}
	if got := len(queue.Pending()); got != 1 {
		t.Fatalf("expected a single stop compensation, got %d", got)
	}
}
