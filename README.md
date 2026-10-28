# chronos

`chronos` is a lightweight, zero-dependency fixed-timestep game loop for server-authoritative simulations in Go.

It drives an update callback at a configurable tick rate (`Hz`), manages background execution via `context.Context`, and provides a completion barrier so your server can shut down cleanly without racing in-flight simulation steps.

## Features

- **Zero dependencies** — built strictly on the Go standard library (`context`, `sync`, `time`).
- **Single-goroutine simulation callback** — `updateFn` is invoked sequentially on a dedicated goroutine, eliminating lock contention on world state.
- **Measured delta time (`dt`)** — passes the actual elapsed time in seconds (`float64`) since the previous tick while exposing the target `Interval()` for deterministic fixed-step math.
- **Idempotent startup** — `Start(ctx)` is guarded by `sync.Once`; duplicate calls are safe no-ops.
- **Race-free cancellation & shutdown** — double-checks context cancellation before invoking `updateFn` and closes `Done()` only after the final tick completes.

---

## Requirements & Installation

Requires **Go 1.21+**.

---

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"time"

	"chronos"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	tick := 0
	// Create a 20 Hz simulation loop (50ms per tick).
	loop := chronos.New(20, func(dt float64) {
		tick++
		fmt.Printf("tick #%d (dt = %.4fs)\n", tick, dt)
	})

	fmt.Printf("Starting loop with interval %v\n", loop.Interval())
	loop.Start(ctx)

	// Wait until the context expires and the loop goroutine exits cleanly.
	<-loop.Done()
	fmt.Println("Simulation stopped cleanly.")
}
```

---

## Examples

### 1. Server-Authoritative Physics Simulation

In a server-authoritative game, the loop steps world state forward at a constant rate (e.g., 60 Hz). You can use either the measured `dt` passed into `updateFn` or the fixed target timestep from `loop.Interval().Seconds()`.

```go
package main

import (
	"context"
	"fmt"
	"time"

	"chronos"
)

type Entity struct {
	X, Y   float64
	VX, VY float64
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	player := &Entity{X: 0, Y: 0, VX: 10.0, VY: 5.0}

	loop := chronos.New(60, func(dt float64) {
		player.X += player.VX * dt
		player.Y += player.VY * dt
	})

	loop.Start(ctx)
	<-loop.Done()

	fmt.Printf("Final position: (%.2f, %.2f)\n", player.X, player.Y)
}
```

### 2. Lock-Free Command Queue Ingestion

Because `chronos` runs `updateFn` on a single goroutine, network handlers can push player inputs onto a buffered channel and the simulation loop can drain them each tick without mutexes guarding the game state.

```go
package main

import (
	"context"
	"fmt"
	"time"

	"chronos"
)

type MoveCommand struct {
	PlayerID int
	DX, DY   float64
}

type World struct {
	positions map[int][2]float64
	commands  chan MoveCommand
}

func NewWorld() *World {
	return &World{
		positions: make(map[int][2]float64),
		commands:  make(chan MoveCommand, 256),
	}
}

func (w *World) EnqueueMove(cmd MoveCommand) {
	select {
	case w.commands <- cmd:
	default:
		// Drop or handle backpressure if command buffer is full.
	}
}

func (w *World) Step(dt float64) {
	// 1. Drain all queued commands received since the last tick.
	for {
		select {
		case cmd := <-w.commands:
			pos := w.positions[cmd.PlayerID]
			pos[0] += cmd.DX * dt
			pos[1] += cmd.DY * dt
			w.positions[cmd.PlayerID] = pos
		default:
			// 2. Advance world simulation once queue is drained.
			return
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	world := NewWorld()
	loop := chronos.New(30, world.Step)
	loop.Start(ctx)

	// Simulate incoming client packets from network goroutines.
	world.EnqueueMove(MoveCommand{PlayerID: 1, DX: 30, DY: 0})
	world.EnqueueMove(MoveCommand{PlayerID: 1, DX: 0, DY: 60})

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-loop.Done()

	// Safe to read world.positions without locks after <-loop.Done().
	fmt.Printf("Player 1 position: %v\n", world.positions[1])
}
```

### 3. Graceful Server Shutdown with OS Signals

When stopping a game server on `SIGINT` or `SIGTERM`, waiting on `<-loop.Done()` guarantees that any in-progress tick finishes before you persist world state to disk or close database connections.

```go
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"chronos"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	loop := chronos.New(20, func(dt float64) {
		// Step simulation and broadcast state snapshots to connected clients.
	})

	loop.Start(ctx)
	fmt.Println("Game loop running at 20 Hz. Press Ctrl+C to stop.")

	// Block until OS signal cancels ctx AND the current tick finishes.
	<-loop.Done()

	// Safe to flush state here: updateFn will never be called again.
	fmt.Println("Flushing world state to storage and exiting...")
}
```

### 4. Detecting Tick Overruns and Clamping `dt`

If an update step takes longer than the tick interval (e.g., due to a GC pause or expensive physics step), `dt` will reflect the longer elapsed duration. You can compare `dt` against `loop.Interval()` to log server hitches and clamp `dt` to prevent physics tunneling.

```go
package main

import (
	"context"
	"log"
	"time"

	"chronos"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	const hz = 50 // 20ms target interval
	var loop *chronos.Loop

	loop = chronos.New(hz, func(dt float64) {
		target := loop.Interval().Seconds()
		maxDelta := target * 2.0

		if dt > maxDelta {
			log.Printf("WARN: tick overrun (dt=%.4fs, target=%.4fs); clamping", dt, target)
			dt = maxDelta
		}

		// Step simulation using clamped dt...
	})

	loop.Start(ctx)
	<-loop.Done()
}
```

### 5. Managing Multiple Match Rooms

Each `chronos.Loop` is self-contained and lightweight (one goroutine and one `time.Ticker` while running). You can spin up an independent loop per match or room and tear each one down when its match ends.

```go
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"chronos"
)

type MatchRoom struct {
	id     string
	cancel context.CancelFunc
	loop   *chronos.Loop
}

func StartMatch(parent context.Context, id string, hz int) *MatchRoom {
	ctx, cancel := context.WithCancel(parent)
	loop := chronos.New(hz, func(dt float64) {
		// Update match-specific state...
	})
	loop.Start(ctx)

	return &MatchRoom{
		id:     id,
		cancel: cancel,
		loop:   loop,
	}
}

func (m *MatchRoom) Stop() {
	m.cancel()
	<-m.loop.Done()
}

func main() {
	ctx := context.Background()

	rooms := []*MatchRoom{
		StartMatch(ctx, "room-alpha", 30),
		StartMatch(ctx, "room-bravo", 60),
	}

	time.Sleep(100 * time.Millisecond)

	var wg sync.WaitGroup
	for _, r := range rooms {
		wg.Add(1)
		go func(room *MatchRoom) {
			defer wg.Done()
			room.Stop()
			fmt.Printf("Stopped %s (interval %v)\n", room.id, room.loop.Interval())
		}(r)
	}
	wg.Wait()
}
```

---

## API Reference

### `func New(hz int, updateFn func(dt float64)) *Loop`

Creates a new `Loop` configured to call `updateFn` `hz` times per second (`interval = time.Second / time.Duration(hz)`).

- **`hz`**: Target tick frequency in ticks per second. Must be `> 0`.
- **`updateFn`**: Callback invoked on every tick with `dt`, the elapsed time in seconds since the previous tick (or since `Start` for the first tick). Must not be `nil`.
- **Panics**: Panics with `"chronos: tick rate must be positive"` if `hz <= 0`, or `"chronos: update callback must not be nil"` if `updateFn == nil`.

### `func (l *Loop) Start(ctx context.Context)`

Starts the loop in a background goroutine and returns immediately.

- The loop runs until `ctx` is cancelled.
- Calling `Start` multiple times (even concurrently) is a safe no-op; only the first call launches the loop.
- If `ctx` is already cancelled when a tick arrives, `chronos` skips `updateFn` and exits immediately.

### `func (l *Loop) Done() <-chan struct{}`

Returns a channel that is closed after `ctx` is cancelled and the loop goroutine has exited.

- Receiving from `<-l.Done()` establishes a happens-before relationship with the completion of the last `updateFn` call, making it safe to inspect or persist state mutated by `updateFn` after `Done()` closes.
- Note: `Done()` only closes if `Start` has been called and the loop has terminated.

### `func (l *Loop) Interval() time.Duration`

Returns the fixed timestep duration (`time.Second / time.Duration(hz)`).

| Tick Rate (`hz`) | `Interval()` |
| :--- | :--- |
| `20` | `50ms` |
| `30` | `33.333333ms` |
| `60` | `16.666666ms` |
| `128` | `7.8125ms` |

