package failover

import (
	"sync/atomic"
	"time"
)

const scheduledRunInterval = time.Minute

var scheduledRunAnchorUnixNano atomic.Int64

func ReloadSchedule() error {
	scheduledRunAnchorUnixNano.Store(time.Now().Add(scheduledRunInterval).UnixNano())
	return nil
}

func NextScheduledRunAtOrAfter(target time.Time) (time.Time, bool) {
	anchorUnixNano := scheduledRunAnchorUnixNano.Load()
	if anchorUnixNano == 0 {
		return time.Time{}, false
	}

	anchor := time.Unix(0, anchorUnixNano)
	if !target.After(anchor) {
		return anchor, true
	}

	elapsed := target.Sub(anchor)
	steps := elapsed / scheduledRunInterval
	next := anchor.Add(steps * scheduledRunInterval)
	if next.Before(target) {
		next = next.Add(scheduledRunInterval)
	}
	return next, true
}
