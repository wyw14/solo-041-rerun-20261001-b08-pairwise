package process

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/cry-97/internal/model"
	"github.com/wyw14/cry-97/internal/sludge"
)

func testPlant(t *testing.T) (*Plant, *time.Time) {
	t.Helper()
	now := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	plant, err := NewPlant(t.TempDir(), []model.LineID{"line-1", "line-2"}, func() time.Time { return now })
	if err != nil {
		t.Fatalf("new plant: %v", err)
	}
	return plant, &now
}

func findPump(pumps []sludge.Pump, id string) sludge.Pump {
	for _, p := range pumps {
		if p.ID == id {
			return p
		}
	}
	return sludge.Pump{}
}

// Normal, non-isolated sequence confirms flow and runs the pump.
func TestEmergency_NormalSequenceStillRuns(t *testing.T) {
	plant, _ := testPlant(t)

	seq, err := plant.StartSludgeSequence("line-1", "pump-line-1-A", 1)
	if err != nil {
		t.Fatalf("start sequence: %v", err)
	}
	if _, err := plant.ConfirmSludgeFlow("line-1", seq.ID, seq.Generation, 10.0); err != nil {
		t.Fatalf("confirm flow: %v", err)
	}
	if p := findPump(plant.Pumps(), "pump-line-1-A"); !p.Running {
		t.Fatalf("legit pump is not running: %+v", p)
	}
}

// Emergency isolation immediately after sequence creation, then a late
// positive-flow receipt for the old generation: the sequence must not become
// running and the isolated pump must stay stopped; the backstop compensation
// must drain without error.
func TestEmergency_StaleReceiptCannotRestartIsolatedPump(t *testing.T) {
	plant, now := testPlant(t)
	ctx := context.Background()

	seq, err := plant.StartSludgeSequence("line-1", "pump-line-1-A", 1)
	if err != nil {
		t.Fatalf("start sequence: %v", err)
	}
	beforeGen := seq.Generation

	state, err := plant.EmergencyStop(ctx, "line-1", "return header rupture")
	if err != nil {
		t.Fatalf("emergency stop: %v", err)
	}
	if state.Generation <= beforeGen || !state.Emergency {
		t.Fatalf("line not isolated into a new generation: %+v", state)
	}

	if p := findPump(plant.Pumps(), "pump-line-1-A"); p.Running || p.Flow != 0 {
		t.Fatalf("isolated pump still running before late receipt: %+v", p)
	}

	if _, err := plant.ConfirmSludgeFlow("line-1", seq.ID, beforeGen, 10.0); err == nil {
		t.Fatal("late old-generation positive-flow receipt was accepted after isolation")
	}
	if p := findPump(plant.Pumps(), "pump-line-1-A"); p.Running || p.Flow != 0 {
		t.Fatalf("stale receipt restarted isolated pump: %+v", p)
	}
	if cur, ok := plant.sequences.Current("line-1"); !ok || cur.Status != sludge.SequenceFailed {
		t.Fatalf("current sequence status = %v, want failed", cur.Status)
	}

	// Let the scheduled stop compensation fall due; it must not error.
	*now = now.Add(2 * time.Second)
	if _, err := plant.RunCompensations(ctx); err != nil {
		t.Fatalf("run compensations: %v", err)
	}
	if p := findPump(plant.Pumps(), "pump-line-1-A"); p.Running {
		t.Fatalf("pump running after compensation: %+v", p)
	}
}
