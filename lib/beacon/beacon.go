// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package beacon

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/thejerf/suture"

	"github.com/syncthing/syncthing/lib/util"
)

type recv struct {
	data []byte
	src  net.Addr
}

type Interface interface {
	suture.Service
	fmt.Stringer
	Send(data []byte)
	Recv() ([]byte, net.Addr)
	Error() error
}

type cast struct {
	*suture.Supervisor
	name    string
	reader  util.ServiceWithError
	writer  util.ServiceWithError
	outbox  chan recv
	inbox   chan []byte
	stopped chan struct{}
}

// newCast creates a base object for multi- or broadcasting. Afterwards the
// caller needs to set reader and writer with the addReader and addWriter
// methods to get a functional implementation of Interface.
func newCast(name string) *cast {
	return &cast{
		Supervisor: suture.New(name, suture.Spec{
			// Don't retry too frenetically: an error to open a socket or
			// whatever is usually something that is either permanent or takes
			// a while to get solved...
			FailureThreshold: 2,
			FailureBackoff:   60 * time.Second,
			// Only log restarts in debug mode.
			Log: func(line string) {
				l.Debugln(line)
			},
			PassThroughPanics: true,
		}),
		name:    name,
		inbox:   make(chan []byte),
		outbox:  make(chan recv, 16),
		stopped: make(chan struct{}),
	}
}

func (c *cast) addReader(svc func(context.Context) error) {
	c.reader = c.createService(svc, "reader")
	c.Add(c.reader)
}

func (c *cast) addWriter(svc func(ctx context.Context) error) {
	c.writer = c.createService(svc, "writer")
	c.Add(c.writer)
}

func (c *cast) createService(svc func(context.Context) error, suffix string) util.ServiceWithError {
	return util.AsServiceWithError(func(ctx context.Context) error {
		l.Debugln("Starting", c.name, suffix)
		err := svc(ctx)
		l.Debugf("Stopped %v %v: %v", c.name, suffix, err)
		return err
	}, fmt.Sprintf("%s/%s", c, suffix))
}

func (c *cast) Stop() {
	c.Supervisor.Stop()
	close(c.stopped)
}

func (c *cast) String() string {
	return fmt.Sprintf("%s@%p", c.name, c)
}

func (c *cast) Send(data []byte) {
	select {
	case c.inbox <- data:
	case <-c.stopped:
	}
}

func (c *cast) Recv() ([]byte, net.Addr) {
	select {
	case recv := <-c.outbox:
		return recv.data, recv.src
	case <-c.stopped:
	}
	return nil, nil
}

func (c *cast) Error() error {
	if err := c.reader.Error(); err != nil {
		return err
	}
	return c.writer.Error()
}
