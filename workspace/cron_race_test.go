package cron

import (
	"sync/atomic"
	"testing"
	"time"
)

type immediateSchedule struct{}

func (s immediateSchedule) Next(t time.Time) time.Time {
	return t
}

func TestRemoveRace(t *testing.T) {
	for i := 0; i < 100; i++ {
		c := New()
		var runCount int32
		id := c.Schedule(immediateSchedule{}, FuncJob(func() {
			atomic.AddInt32(&runCount, 1)
		}))

		c.Start()
		c.Remove(id)
		time.Sleep(5 * time.Millisecond)
		c.Stop()

		if atomic.LoadInt32(&runCount) > 0 {
			t.Fatalf("iteration %d: job executed even though it was removed immediately", i)
		}
	}
}