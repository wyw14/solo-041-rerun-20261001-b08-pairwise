package process_test

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/cry-97/internal/model"
	"github.com/wyw14/cry-97/internal/process"
	"github.com/wyw14/cry-97/internal/sludge"
)

type testClock struct{ now time.Time }

func (c *testClock) Now() time.Time { return c.now }

func newTestPlant(t *testing.T, clock *testClock, lines ...model.LineID) *process.Plant {
	t.Helper()
	plant, err := process.NewPlant(t.TempDir(), lines, clock.Now)
	if err != nil {
		t.Fatalf("new plant: %v", err)
	}
	return plant
}

func lineSequence(t *testing.T, plant *process.Plant, lineID model.LineID) sludge.Sequence {
	t.Helper()
	status, err := plant.Status(lineID)
	if err != nil {
		t.Fatalf("line status: %v", err)
	}
	if status.Sequence == nil {
		t.Fatalf("line %s has no sequence", lineID)
	}
	return *status.Sequence
}

func linePump(t *testing.T, plant *process.Plant, lineID model.LineID, pumpID string) sludge.Pump {
	t.Helper()
	status, err := plant.Status(lineID)
	if err != nil {
		t.Fatalf("line status: %v", err)
	}
	for _, pump := range status.Pumps {
		if pump.ID == pumpID {
			return pump
		}
	}
	t.Fatalf("pump %s is not found on line %s", pumpID, lineID)
	return sludge.Pump{}
}

// Regression: a sequence is created, the line is emergency-isolated, and then
// the old sequence's positive flow confirmation arrives late. The stale
// receipt must not flip the sequence to running or restart the pump.
func TestStaleSequenceConfirmationCannotRestartIsolatedLine(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)}
	plant := newTestPlant(t, clock, "line-1")

	sequence, err := plant.StartSludgeSequence("line-1", "pump-line-1-A", 1)
	if err != nil {
		t.Fatalf("start sequence: %v", err)
	}
	if sequence.Generation != 1 || sequence.Status != sludge.SequenceSwitching {
		t.Fatalf("unexpected sequence after start: %+v", sequence)
	}

	state, err := plant.EmergencyStop(context.Background(), "line-1", "test isolation")
	if err != nil {
		t.Fatalf("emergency stop: %v", err)
	}
	if state.Generation != 2 || !state.Emergency {
		t.Fatalf("unexpected line state after isolation: %+v", state)
	}

	// The late confirmation from the old generation must be rejected.
	if _, err := plant.ConfirmSludgeFlow("line-1", sequence.ID, 1, 12.5); err == nil {
		t.Fatal("stale generation confirmation was accepted on an isolated line")
	}
	// A confirmation naming the new generation must be rejected as well: the
	// line is isolated and the sequence belongs to the old generation.
	if _, err := plant.ConfirmSludgeFlow("line-1", sequence.ID, 2, 12.5); err == nil {
		t.Fatal("confirmation was accepted on an isolated line")
	}

	current := lineSequence(t, plant, "line-1")
	if current.Status == sludge.SequenceRunning {
		t.Fatalf("stale receipt moved sequence to running: %+v", current)
	}
	if current.Status != sludge.SequenceFailed {
		t.Fatalf("isolated sequence should be failed, got %+v", current)
	}
	pump := linePump(t, plant, "line-1", "pump-line-1-A")
	if pump.Running || pump.Flow != 0 {
		t.Fatalf("isolated line pump is running: %+v", pump)
	}
	if pump.FenceGeneration != 2 {
		t.Fatalf("pump is not fenced at the emergency generation: %+v", pump)
	}
}

// Emergency isolation must stop equipment that was already running, not only
// block future confirmations.
func TestEmergencyIsolationStopsRunningPump(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)}
	plant := newTestPlant(t, clock, "line-1")

	sequence, err := plant.StartSludgeSequence("line-1", "pump-line-1-A", 1)
	if err != nil {
		t.Fatalf("start sequence: %v", err)
	}
	if _, err := plant.ConfirmSludgeFlow("line-1", sequence.ID, 1, 12.5); err != nil {
		t.Fatalf("confirm flow: %v", err)
	}
	if pump := linePump(t, plant, "line-1", "pump-line-1-A"); !pump.Running {
		t.Fatalf("pump should be running after confirmation: %+v", pump)
	}

	if _, err := plant.EmergencyStop(context.Background(), "line-1", "test isolation"); err != nil {
		t.Fatalf("emergency stop: %v", err)
	}
	pump := linePump(t, plant, "line-1", "pump-line-1-A")
	if pump.Running || pump.Flow != 0 {
		t.Fatalf("running pump was not stopped by isolation: %+v", pump)
	}
}

// A healthy line must keep working: a sequence started at the current
// generation confirms and runs, and its fail compensation stops the pump.
func TestCurrentGenerationSequenceStillRuns(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)}
	plant := newTestPlant(t, clock, "line-1")

	sequence, err := plant.StartSludgeSequence("line-1", "pump-line-1-A", 1)
	if err != nil {
		t.Fatalf("start sequence: %v", err)
	}
	confirmed, err := plant.ConfirmSludgeFlow("line-1", sequence.ID, 1, 12.5)
	if err != nil {
		t.Fatalf("confirm flow: %v", err)
	}
	if confirmed.Status != sludge.SequenceRunning {
		t.Fatalf("sequence is not running: %+v", confirmed)
	}
	if pump := linePump(t, plant, "line-1", "pump-line-1-A"); !pump.Running || pump.Flow != 12.5 {
		t.Fatalf("pump is not running with confirmed flow: %+v", pump)
	}

	if _, err := plant.FailSludgeSequence("line-1"); err != nil {
		t.Fatalf("fail sequence: %v", err)
	}
	// A late confirmation for the failed sequence must not revive it.
	if _, err := plant.ConfirmSludgeFlow("line-1", sequence.ID, 1, 12.5); err == nil {
		t.Fatal("failed sequence accepted a late flow confirmation")
	}
	clock.now = clock.now.Add(2 * time.Second)
	executed, err := plant.RunCompensations(context.Background())
	if err != nil {
		t.Fatalf("run compensations: %v", err)
	}
	if executed != 1 {
		t.Fatalf("expected 1 compensation, got %d", executed)
	}
	if pump := linePump(t, plant, "line-1", "pump-line-1-A"); pump.Running || pump.Flow != 0 {
		t.Fatalf("compensation stop did not stop the pump: %+v", pump)
	}
}

// A stop compensation scheduled for an old sequence must neither stop the
// pump's new owner nor poison the compensation queue.
func TestStaleCompensationStopSkipsNewOwner(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)}
	plant := newTestPlant(t, clock, "line-1")

	first, err := plant.StartSludgeSequence("line-1", "pump-line-1-A", 1)
	if err != nil {
		t.Fatalf("start first sequence: %v", err)
	}
	if _, err := plant.FailSludgeSequence("line-1"); err != nil {
		t.Fatalf("fail first sequence: %v", err)
	}
	second, err := plant.StartSludgeSequence("line-1", "pump-line-1-A", 2)
	if err != nil {
		t.Fatalf("start second sequence: %v", err)
	}
	if second.ID == first.ID {
		t.Fatal("sequence IDs must differ")
	}
	if _, err := plant.ConfirmSludgeFlow("line-1", second.ID, 1, 9.5); err != nil {
		t.Fatalf("confirm second sequence: %v", err)
	}

	clock.now = clock.now.Add(2 * time.Second)
	executed, err := plant.RunCompensations(context.Background())
	if err != nil {
		t.Fatalf("stale compensation stop must not fail the queue: %v", err)
	}
	if executed != 1 {
		t.Fatalf("expected 1 compensation, got %d", executed)
	}
	pump := linePump(t, plant, "line-1", "pump-line-1-A")
	if !pump.Running || pump.OwnerSequence != second.ID {
		t.Fatalf("stale stop touched the new owner's pump: %+v", pump)
	}
}
