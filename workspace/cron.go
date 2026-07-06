package cron

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries   []*Entry
	chain     Chain
	stop      chan struct{}
	add       chan *Entry
	remove    chan EntryID
	snapshot  chan chan []Entry
	running   bool
	logger    Logger
	runningMu sync.Mutex
	nextID    EntryID
	parser    ScheduleParser
	location  *time.Location
}

// Job is an interface for submitted jobs.
type Job interface {
	Run()
}

// Schedule describes a job's duty cycle.
type Schedule interface {
	// Next returns the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// ID is the unique identifier of this entry.
	ID EntryID

	// Schedule on which this job should be run.
	Schedule Schedule

	// Next time the job will run, or the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// Prev is the last time this job was run, or the zero time if never.
	Prev time.Time

	// WrappedJob is the thing to run when the Schedule is activated.
	WrappedJob Job

	// Job is the thing that was submitted to cron.
	// It is kept around so that it may be looked up.
	Job Job
}

// Valid returns true if this is not the zero entry.
func (e Entry) Valid() bool { return e.ID != 0 }

// byTime is a wrapper for sorting the entry array by time
// (with zero times at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, sort zero times to the end.
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

// New returns a new Cron job runner, modified by the given options.
//
// Available Settings
//
//   WithLocation
//     Override the timezone of the cron instance.
//
//   WithParser
//     Override the parser used for interpreting templates.
//
//   WithChain
//     Specify a Job wrapper to act on all jobs added to this cron.
//
// See "Option" for more details.
func New(opts ...Option) *Cron {
	c := &Cron{
		entries:   nil,
		chain:     StandardChain,
		add:       make(chan *Entry),
		remove:    make(chan EntryID),
		stop:      make(chan struct{}),
		snapshot:  make(chan chan []Entry),
		running:   false,
		logger:    DefaultLogger,
		location:  time.Local,
		parser:    standardParser,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// FuncJob is a wrapper that turns a func() into a cron.Job
type FuncJob func()

func (f FuncJob) Run() { f() }

// AddFunc adds a func to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddFunc(spec string, cmd func()) (EntryID, error) {
	return c.AddJob(spec, FuncJob(cmd))
}

// AddJob adds a Job to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddJob(spec string, cmd Job) (EntryID, error) {
	schedule, err := c.parser.Parse(spec)
	if err != nil {
		return 0, err
	}
	return c.Schedule(schedule, cmd), nil
}

// Schedule adds a Job to the Cron to be run on the given schedule.
// The job is wrapped with the configured Chain.
func (c *Cron) Schedule(schedule Schedule, cmd Job) EntryID {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	c.nextID++
	entry := &Entry{
		ID:         c.nextID,
		Schedule:   schedule,
		WrappedJob: c.chain.Then(cmd),
		Job:        cmd,
	}
	if !c.running {
		c.entries = append(c.entries, entry)
	} else {
		c.add <- entry
	}
	return entry.ID
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []Entry {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		replyChan := make(chan []Entry)
		c.snapshot <- replyChan
		return <-replyChan
	}
	return c.entrySnapshot()
}

// Location returns the time zone of this Cron instance.
func (c *Cron) Location() *time.Location {
	return c.location
}

// Entry returns a duplicate of the Entry with the given time.
// If it is not found, a zero Entry is returned.
func (c *Cron) Entry(id EntryID) Entry {
	for _, entry := range c.Entries() {
		if entry.ID == id {
			return entry
		}
	}
	return Entry{}
}

// Remove an entry from being run in the future.
func (c *Cron) Remove(id EntryID) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.remove <- id
	} else {
		c.removeEntry(id)
	}
}

// Start the cron scheduler in its own goroutine, or no-op if already started.
func (c *Cron) Start() {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		return
	}
	c.running = true
	go c.run()
}

// Run the cron scheduler, or no-op if already running.
func (c *Cron) Run() {
	c.runningMu.Lock()
	if c.running {
		c.runningMu.Unlock()
		return
	}
	c.running = true
	c.runningMu.Unlock()
	c.run()
}

// run the scheduler.. this is private as it is started as a goroutine
func (c *Cron) run() {
	c.logger.Info("start")

	// Figure out the next activation times for each entry.
	now := c.now()
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
	}

	for {
		// Determine the next entry to run.
		sort.Sort(byTime(c.entries))

		var timer *time.Timer
		if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop commands.
			timer = time.NewTimer(100000 * time.Hour)
		} else {
			timer = time.NewTimer(c.entries[0].Next.Sub(now))
		}

		for {
			select {
			case now = <-timer.C:
				// Check if any control channels are ready before running jobs.
				select {
				case newEntry := <-c.add:
					timer.Stop()
					now = c.now()
					newEntry.Next = newEntry.Schedule.Next(now)
					c.entries = append(c.entries, newEntry)
				case id := <-c.remove:
					timer.Stop()
					now = c.now()
					c.removeEntry(id)
				case <-c.stop:
					timer.Stop()
					c.logger.Info("stop")
					return
				default:
					// Run every entry whose next time has passed
					for _, e := range c.entries {
						if e.Next.After(now) || e.Next.IsZero() {
							break
						}
						go c.startJob(e.WrappedJob)
						e.Prev = e.Next
						e.Next = e.Schedule.Next(now)
					}
				}

			case newEntry := <-c.add:
				timer.Stop()
				now = c.now()
				newEntry.Next = newEntry.Schedule.Next(now)
				c.entries = append(c.entries, newEntry)

			case replyChan := <-c.snapshot:
				replyChan <- c.entrySnapshot()
				continue

			case id := <-c.remove:
				timer.Stop()
				now = c.now()
				c.removeEntry(id)

			case <-c.stop:
				timer.Stop()
				c.logger.Info("stop")
				return
			}
			break
		}
	}
}

// startJob runs the given job in a new goroutine.
func (c *Cron) startJob(j Job) { 
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error(nil, "panic", "panic", r)
		}
	}()
	j.Run()
}

// Stop stops the cron scheduler if it is running; otherwise it does nothing.
// A context is returned so the caller can wait for running jobs to complete.
func (c *Cron) Stop() context.Context {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.stop <- struct{}{}
		c.running = false
	}
	ctx, _ := context.WithCancel(context.Background())
	return ctx
}

// entrySnapshot returns a copy of the current cron entries.
func (c *Cron) entrySnapshot() []Entry {
	var entries []Entry
	for _, e := range c.entries {
		entries = append(entries, Entry{
			ID:         e.ID,
			Schedule:   e.Schedule,
			Next:       e.Next,
			Prev:       e.Prev,
			WrappedJob: e.WrappedJob,
			Job:        e.Job,
		})
	}
	return entries
}

func (c *Cron) removeEntry(id EntryID) {
	var entries []*Entry
	for _, e := range c.entries {
		if e.ID != id {
			entries = append(entries, e)
		}
	}
	c.entries = entries
}

// now returns current time in localized time zone.
func (c *Cron) now() time.Time {
	if c.location == nil {
		return time.Now()
	}
	return time.Now().In(c.location)
}