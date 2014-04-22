package main

import (
	"sync"
	"time"
)

const (
	maxGuiEvents  = 100
	maxEventPollS = 300
)

type eventType string

const (
	eventTimeout      eventType = "TIMEOUT"
	eventConnected              = "NODE_CONNECTED"
	eventDisconnected           = "NODE_DISCONNECTED"
	eventIndex                  = "NODE_INDEX"
	eventPullStart              = "PULL_START"
	eventPullComplete           = "PULL_COMPLETE"
	eventPullError              = "PULL_ERROR"
)

type event struct {
	ID        int                    `json:"id"`
	Timestamp time.Time              `json:"timestamp"`
	Type      eventType              `json:"type"`
	Params    map[string]interface{} `json:"params"`
}

var newEvent = make(chan event)
var newPoller = make(chan chan event)
var nextEventId int = 1
var events []event
var eventsMut sync.RWMutex

func eventPollLoop() {
	var pollers []chan event

	for {
		select {
		case ch := <-newPoller:
			pollers = append(pollers, ch)

		case ev := <-newEvent:
			ev.ID = nextEventId
			nextEventId++
			eventsMut.Lock()
			events = append(events, ev)
			if len(events) > maxGuiEvents {
				events = events[len(events)-maxGuiEvents:]
			}
			eventsMut.Unlock()
			for _, ch := range pollers {
				ch <- ev
			}
			pollers = nil
		}
	}
}

func waitForEvent() event {
	ch := make(chan event, 1)
	newPoller <- ch
	select {
	case ev := <-ch:
		return ev

	case <-time.After(maxEventPollS * time.Second):
		return event{
			ID:        -1,
			Timestamp: time.Now(),
			Type:      eventTimeout,
		}
	}
}

func guiEvent(etype eventType, params map[string]interface{}) {
	newEvent <- event{
		// Id will be set by eventPoolLoop
		Timestamp: time.Now(),
		Type:      etype,
		Params:    params,
	}
}
