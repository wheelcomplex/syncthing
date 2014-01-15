package watch

import (
	"log"
	"os"
	"path/filepath"
	"time"

	"code.google.com/p/go.exp/fsnotify"
)

const (
	maxEventsBuffer = 100
	maxEventDelay   = 5 * time.Second
)

type Event struct {
	Name string
}

func debounce(in chan Event) chan Event {
	out := make(chan Event)
	evs := make(map[Event]time.Time, maxEventsBuffer)
	evCnt := 0

	flush := func() {
		for ev, t0 := range evs {
			if time.Since(t0) > maxEventDelay {
				out <- ev
				delete(evs, ev)
			}
		}
		evCnt = 0
	}

	go func() {
		for {
			if evCnt > maxEventsBuffer {
				flush()
			}
			select {
			case ev := <-in:
				evs[ev] = time.Now()
				evCnt++
			case <-time.After(maxEventDelay):
				flush()
			}
		}
	}()
	return out
}

func Watch(dir string) (chan Event, error) {
	out := make(chan Event)
	w, err := fsnotify.NewWatcher()

	if err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case ev := <-w.Event:
				if ev.IsCreate() {
					info, err := os.Stat(ev.Name)
					if err == nil && info.IsDir() {
						w.Watch(ev.Name)
					}
				}
				out <- Event{ev.Name}
			case err := <-w.Error:
				log.Println("WARNING: fsnotify:", err)
			}
		}
	}()

	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return w.Watch(path)
		}
		return nil
	})

	return debounce(out), nil
}
