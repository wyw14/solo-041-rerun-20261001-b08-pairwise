package sludge

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

type Pump struct {
	ID              string           `json:"id"`
	LineID          model.LineID     `json:"line_id"`
	Kind            model.DeviceKind `json:"kind"`
	Running         bool             `json:"running"`
	Primed          bool             `json:"primed"`
	Flow            float64          `json:"flow"`
	OwnerSequence   uuid.UUID        `json:"owner_sequence,omitempty"`
	OwnerGeneration uint64           `json:"owner_generation"`
	// FenceGeneration is the line generation recorded at the last emergency
	// isolation. A pump whose owner generation is at or below this fence belongs
	// to a superseded (isolated) era and must never be (re)started by it.
	FenceGeneration uint64    `json:"fence_generation"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type PumpController struct {
	mu    sync.RWMutex
	pumps map[string]Pump
}

func NewPumpController(pumps []Pump) (*PumpController, error) {
	if len(pumps) < 2 {
		return nil, errors.New("at least two return pumps are required")
	}
	controller := &PumpController{pumps: make(map[string]Pump, len(pumps))}
	for _, pump := range pumps {
		if pump.ID == "" || pump.LineID == "" {
			return nil, errors.New("pump identity is incomplete")
		}
		controller.pumps[pump.ID] = pump
	}
	return controller, nil
}

// Assign transfers a pump to the owner identified by (lineID, sequenceID,
// generation). It refuses cross-line pumps and any generation that is not
// strictly newer than both the current owner and the isolation fence, so an
// old-era sequence can never reclaim a pump that belongs to an isolated or
// newer era.
func (c *PumpController) Assign(lineID model.LineID, pumpID string, sequenceID uuid.UUID, generation uint64, now time.Time) (Pump, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pump, ok := c.pumps[pumpID]
	if !ok || lineID == "" || sequenceID == uuid.Nil || generation == 0 {
		return Pump{}, errors.New("pump assignment is invalid")
	}
	if pump.LineID != lineID {
		return Pump{}, errors.New("return pump does not belong to process line")
	}
	if generation <= pump.FenceGeneration || generation <= pump.OwnerGeneration {
		return Pump{}, errors.New("pump assignment generation is stale")
	}
	pump.OwnerSequence = sequenceID
	pump.OwnerGeneration = generation
	pump.Primed = false
	pump.Running = false
	pump.Flow = 0
	pump.UpdatedAt = now.UTC()
	c.pumps[pumpID] = pump
	return pump, nil
}

func (c *PumpController) Ready(pumpID string, sequenceID uuid.UUID, generation uint64, flow float64, now time.Time) (Pump, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pump, ok := c.pumps[pumpID]
	if !ok {
		return Pump{}, errors.New("return pump is not found")
	}
	if pump.OwnerSequence != sequenceID || pump.OwnerGeneration != generation {
		return Pump{}, errors.New("pump ready confirmation is stale")
	}
	if generation <= pump.FenceGeneration {
		return Pump{}, errors.New("pump ready confirmation belongs to an isolated generation")
	}
	if flow <= 0 {
		return Pump{}, errors.New("pump ready confirmation has no flow")
	}
	pump.Primed = true
	pump.Running = true
	pump.Flow = flow
	pump.UpdatedAt = now.UTC()
	c.pumps[pumpID] = pump
	return pump, nil
}

// StopOwned stops the pump only when it is still owned by exactly
// (sequenceID, generation). When ownership has already moved to a newer
// generation it returns without changing anything and without an error: a
// compensation from a superseded era must never fault the queue nor touch the
// device now belonging to another era.
func (c *PumpController) StopOwned(pumpID string, sequenceID uuid.UUID, generation uint64, now time.Time) (Pump, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pump, ok := c.pumps[pumpID]
	if !ok {
		return Pump{}, errors.New("return pump is not found")
	}
	if pump.OwnerSequence != sequenceID || pump.OwnerGeneration != generation {
		// Ownership moved on; the old-era compensation has nothing to stop.
		return pump, nil
	}
	pump.Running = false
	pump.Flow = 0
	pump.UpdatedAt = now.UTC()
	c.pumps[pumpID] = pump
	return pump, nil
}

// IsolateLine raises the isolation fence to fenceGeneration for every pump on
// the line and immediately stops those still owned by an era at or below the
// fence. Pumps already owned by a strictly newer generation are left running.
// This is idempotent for the same fence generation.
func (c *PumpController) IsolateLine(lineID model.LineID, fenceGeneration uint64, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, pump := range c.pumps {
		if pump.LineID != lineID {
			continue
		}
		if fenceGeneration > pump.FenceGeneration {
			pump.FenceGeneration = fenceGeneration
		}
		if pump.OwnerGeneration <= pump.FenceGeneration {
			pump.Running = false
			pump.Flow = 0
			pump.UpdatedAt = now.UTC()
		}
		c.pumps[id] = pump
	}
}

func (c *PumpController) Snapshot() []Pump {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]Pump, 0, len(c.pumps))
	for _, pump := range c.pumps {
		result = append(result, pump)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}
