package sludge

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
)

type SequenceStatus string

const (
	SequenceSwitching SequenceStatus = "switching"
	SequenceRunning   SequenceStatus = "running"
	SequenceFailed    SequenceStatus = "failed"
)

type Sequence struct {
	ID         uuid.UUID      `json:"id"`
	LineID     model.LineID   `json:"line_id"`
	Number     uint64         `json:"number"`
	Generation uint64         `json:"generation"`
	PumpID     string         `json:"pump_id"`
	Status     SequenceStatus `json:"status"`
	StartedAt  time.Time      `json:"started_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

type SequenceBook struct {
	mu     sync.RWMutex
	byLine map[model.LineID]Sequence
	// fence holds, per line, the generation recorded at the most recent
	// emergency isolation. No sequence or receipt at or below that generation
	// may (re)start equipment: it belongs to a superseded era.
	fence map[model.LineID]uint64
	pumps *PumpController
	queue *journal.CompensationQueue
}

func NewSequenceBook(pumps *PumpController, queue *journal.CompensationQueue) (*SequenceBook, error) {
	if pumps == nil || queue == nil {
		return nil, errors.New("sludge sequence dependencies are required")
	}
	return &SequenceBook{
		byLine: make(map[model.LineID]Sequence), fence: make(map[model.LineID]uint64),
		pumps: pumps, queue: queue,
	}, nil
}

func (b *SequenceBook) Start(lineID model.LineID, pumpID string, number, generation uint64, now time.Time) (Sequence, error) {
	if lineID == "" || pumpID == "" || number == 0 || generation == 0 {
		return Sequence{}, errors.New("sludge sequence is invalid")
	}
	sequence := Sequence{
		ID: uuid.New(), LineID: lineID, Number: number, Generation: generation,
		PumpID: pumpID, Status: SequenceSwitching, StartedAt: now.UTC(), UpdatedAt: now.UTC(),
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if generation <= b.fence[lineID] {
		return Sequence{}, errors.New("sludge sequence belongs to an isolated generation")
	}
	// Assign takes book->pump lock ordering and independently enforces the
	// fence, line ownership and era monotonicity of the pump.
	if _, err := b.pumps.Assign(lineID, pumpID, sequence.ID, generation, now); err != nil {
		return Sequence{}, err
	}
	b.byLine[lineID] = sequence
	return sequence, nil
}

func (b *SequenceBook) ConfirmFlow(lineID model.LineID, sequenceID uuid.UUID, generation uint64, flow float64, now time.Time) (Sequence, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sequence, ok := b.byLine[lineID]
	if !ok || sequence.ID != sequenceID || sequence.Generation != generation {
		return Sequence{}, errors.New("sludge sequence confirmation is stale")
	}
	if generation <= b.fence[lineID] || sequence.Status == SequenceFailed {
		return Sequence{}, errors.New("sludge sequence confirmation belongs to an isolated or failed generation")
	}
	if _, err := b.pumps.Ready(sequence.PumpID, sequence.ID, sequence.Generation, flow, now); err != nil {
		return Sequence{}, err
	}
	sequence.Status = SequenceRunning
	sequence.UpdatedAt = now.UTC()
	b.byLine[lineID] = sequence
	return sequence, nil
}

func (b *SequenceBook) Fail(lineID model.LineID, now time.Time) (Sequence, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sequence, ok := b.byLine[lineID]
	if !ok {
		return Sequence{}, errors.New("sludge sequence is not active")
	}
	if sequence.Status == SequenceFailed {
		// A stop compensation is already pending; do not schedule a duplicate.
		return sequence, nil
	}
	return b.failLocked(lineID, sequence, now)
}

// Isolate supersedes the line's current sequence for the given (new, larger)
// line generation: it raises the era fence, marks the running sequence failed,
// schedules a compensating stop and stops the line's pumps immediately. A late
// receipt from the old generation can no longer restart anything, while a
// future sequence with a generation above the fence is unaffected. Calling it
// again with the same generation is idempotent.
func (b *SequenceBook) Isolate(lineID model.LineID, generation uint64, now time.Time) (Sequence, error) {
	if lineID == "" || generation == 0 {
		return Sequence{}, errors.New("sludge isolation is invalid")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if generation > b.fence[lineID] {
		b.fence[lineID] = generation
	}
	sequence, ok := b.byLine[lineID]
	if ok && sequence.Generation < generation && sequence.Status != SequenceFailed {
		failed, err := b.failLocked(lineID, sequence, now)
		if err != nil {
			return Sequence{}, err
		}
		sequence = failed
	}
	// Raise the pump fence and stop old-era pumps right away; the scheduled
	// compensation is only a backstop.
	b.pumps.IsolateLine(lineID, generation, now)
	return sequence, nil
}

// failLocked marks the current sequence failed and schedules a stop
// compensation. Callers must hold b.mu.
func (b *SequenceBook) failLocked(lineID model.LineID, sequence Sequence, now time.Time) (Sequence, error) {
	sequence.Status = SequenceFailed
	sequence.UpdatedAt = now.UTC()
	b.byLine[lineID] = sequence
	compensation, err := journal.NewCompensation(lineID, sequence.ID, sequence.Generation, sequence.PumpID, "stop", now.Add(time.Second))
	if err != nil {
		return Sequence{}, err
	}
	b.queue.Schedule(compensation)
	return sequence, nil
}

func (b *SequenceBook) ExecuteCompensation(ctx context.Context, compensation journal.Compensation, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if compensation.Action != "stop" {
		return errors.New("unsupported sludge compensation action")
	}
	// StopOwned never restarts a pump and no-ops once ownership has moved to a
	// newer generation, so an old-era compensation cannot disturb current
	// equipment nor jam the queue.
	_, err := b.pumps.StopOwned(compensation.DeviceID, compensation.SequenceID, compensation.Generation, now)
	return err
}

func (b *SequenceBook) Current(lineID model.LineID) (Sequence, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	sequence, ok := b.byLine[lineID]
	return sequence, ok
}
