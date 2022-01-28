package postgres

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/timsolov/psql-streamer/mux"

	"github.com/spf13/viper"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	uuid "github.com/satori/go.uuid"
	"github.com/timsolov/pgoutput"
	"github.com/timsolov/psql-streamer/common"
	"github.com/timsolov/psql-streamer/db"
	"github.com/timsolov/psql-streamer/event"
	"github.com/timsolov/psql-streamer/sink"
	"github.com/timsolov/psql-streamer/source/prom"
)

// PSQL is a PostgreSQL source
type PSQL struct {
	name string

	cfg psqlConfig

	conn        *pgconn.PgConn
	connConfig  *pgconn.Config
	relationSet *pgoutput.RelationSet
	sub         *pgoutput.Subscription
	mux         *mux.Mux

	boltDB     *db.DB
	boltBucket string

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	walPosition          uint64 // current WAL position
	walPositionPersisted uint64 // WAL position saved to BoltDB

	promTags []string

	sinks map[string]sink.Sink

	err error

	stats struct {
		events, replicationErrors, persistErrors, eventErrors uint64
	}

	*common.Logger
	sync.RWMutex
}

type psqlConfig struct {
	dsn             string
	publication     string
	replicationSlot string

	timeout time.Duration

	startRetryInterval time.Duration

	walPositionOverride uint64
	walRetain           uint64
}

// New creates a new PostgreSQL source
func New(name string, v *viper.Viper) (s *PSQL, err error) {
	v.SetDefault("startRetryInterval", 5*time.Second)
	v.SetDefault("timeout", 2*time.Second)

	cf := psqlConfig{}

	if cf.dsn = v.GetString("dsn"); cf.dsn == "" {
		return nil, fmt.Errorf("dsn should be defined")
	}

	if cf.publication = v.GetString("publication"); cf.publication == "" {
		return nil, fmt.Errorf("publication should be defined")
	}

	if cf.replicationSlot = v.GetString("replicationSlot"); cf.replicationSlot == "" {
		return nil, fmt.Errorf("replicationSlot should be defined")
	}

	if cf.startRetryInterval = v.GetDuration("startRetryInterval"); cf.startRetryInterval <= 0 {
		return nil, fmt.Errorf("startRetryInterval should be > 0")
	}

	if cf.timeout = v.GetDuration("timeout"); cf.timeout <= 0 {
		return nil, fmt.Errorf("timeout should be > 0")
	}

	if v.GetInt64("walPositionOverride") < 0 {
		return nil, fmt.Errorf("walPositionOverride should be >= 0")
	}
	cf.walPositionOverride = uint64(v.GetInt64("walPositionOverride"))

	if v.GetInt64("walRetain") < 0 {
		return nil, fmt.Errorf("walRetain should be >= 0")
	}
	cf.walRetain = uint64(v.GetInt64("walRetain"))

	s = &PSQL{
		name:        name,
		boltBucket:  "source_" + name,
		relationSet: pgoutput.NewRelationSet(nil),
		sinks:       make(map[string]sink.Sink),
		cfg:         cf,
	}

	s.Logger = common.LoggerCreate(s, v)

	if s.boltDB, err = db.GetHandleFromViper(v); err != nil {
		return nil, err
	}

	s.promTags = []string{s.Name(), s.Type()}

	if err = s.boltDB.BucketInit(s.boltBucket); err != nil {
		return nil, fmt.Errorf("Unable to init Bolt bucket: %s", err)
	}

	// Check if we have a WAL position overridden from the config file
	if s.cfg.walPositionOverride == 0 { // not overriden
		// Read WAL position from boltDB if it's there
		if s.cfg.walPositionOverride, err = s.boltDB.CounterGet(s.boltBucket, db.CounterWALPos); err != nil {
			return nil, fmt.Errorf("Unable to read bolt counter: %s", err)
		}
	}

	s.walPosition, s.walPositionPersisted = s.cfg.walPositionOverride, s.cfg.walPositionOverride

	s.ctx, s.cancel = context.WithCancel(context.Background())

	if s.connConfig, err = pgconn.ParseConfig(s.cfg.dsn); err != nil {
		return nil, fmt.Errorf("Unable to parse DSN: %s", err)
	}
	// Use custom dialer to be able to cancel the dial process with context
	s.connConfig.DialFunc = func(ctx context.Context, n, a string) (c net.Conn, err error) {
		dialer := &net.Dialer{
			Timeout:   s.cfg.timeout,
			KeepAlive: 30 * time.Second,
			DualStack: false,
		}

		return dialer.DialContext(ctx, n, a)
	}

	if err = s.setup(); err != nil {
		return nil, err
	}

	s.mux, err = mux.New(s.ctx, v,
		mux.Config{
			Callback:     s.flushWalPosition,
			ErrorCounter: &s.stats.eventErrors,
			Logger:       s.Logger,
		},
	)

	if err != nil {
		return nil, fmt.Errorf("Unable to create Mux: %s", err)
	}

	return
}

// Start instructs source to begin streaming the events
func (src *PSQL) Start() {
	src.mux.Start()
	src.wg.Add(1)
	go src.fetch()
}

// Close stops the replication and exits
func (src *PSQL) Close() error {
	src.cancel()
	src.Logf("Closing...")
	src.wg.Wait()

	src.mux.Close()

	if src.conn != nil {
		return src.conn.Close(src.ctx)
	}

	return nil
}

// setup creates Replication Connection, creates replication slot if it doesn't exist
func (src *PSQL) setup() (err error) {
	// Close the connection if it was already set up
	if src.conn != nil {
		if err = src.conn.Close(src.ctx); err != nil {
			src.Errorf("Unable to close connection: %s", err)
		}
	}

	if src.conn, err = pgconn.ConnectConfig(src.ctx, src.connConfig); err != nil {
		return fmt.Errorf("Unable to open replication connection: %s", err)
	}

	src.sub = pgoutput.NewSubscription(
		src.conn,
		src.cfg.replicationSlot,
		src.cfg.publication,
		pgoutput.SetWALRetain(src.cfg.walRetain),
		pgoutput.SetFailOnHandler(true),
		pgoutput.SetLogger(src),
	)

	//TODO REMOVE
	sysident, err := pglogrepl.IdentifySystem(src.ctx, src.conn)
	if err != nil {
		return fmt.Errorf("IdentifySystem failed: %v", err)
	}
	src.Errorf(fmt.Sprintf("SystemID: %s  Timeline: %d XLogPos: %d (%s) DBName: %s", sysident.SystemID, sysident.Timeline, uint64(sysident.XLogPos), sysident.XLogPos, sysident.DBName))
	//**********

	if err = src.sub.CreateSlot(); err != nil {
		if err == pgoutput.ErrorSlotExist {
			src.Errorf("slot already exists: skip creation")
			err = nil
		} else {
			return fmt.Errorf("Unable to create replication slot: %s", err)
		}
	}

	return
}

// Name returns source name
func (src *PSQL) Name() string {
	return src.name
}

// Type returns source type
// Always "Source-PSQL"
func (src *PSQL) Type() string {
	return "Source-Postgres2"
}

// Subscribe registers a function to be called on event arrival
func (src *PSQL) Subscribe(sink sink.Sink) {
	src.mux.Subscribe(sink)
}

// SetLogger sets a logger
func (src *PSQL) SetLogger(l *common.Logger) {
	src.Logger = l
}

// Main execution loop
func (src *PSQL) fetch() {
	var err error

	defer src.wg.Done()

	first := true
	for {
		walPos := atomic.LoadUint64(&src.walPositionPersisted)

		select {
		// Return if the context is canceled
		// This should be caught by (err == nil) below, but just in case
		case <-src.ctx.Done():
			return

		// Try to start replication indefinitely
		default:
			if first {
				first = false
				goto start
			}

			// If it's not the first iteration - try to recreate the connection from scratch
			if err = src.setup(); err != nil {
				if err == context.Canceled {
					return
				}

				goto oops
			}

		start:
			src.Logf("Starting replication at WAL position %d (%s)", walPos, pglogrepl.LSN(walPos))
			src.setError(nil)
			// err will be nil only in case of context cancellation (correct shutdown)
			if err = src.sub.Start(src.ctx, walPos, time.Millisecond*100, src.process); err == nil {
				return
			}

		oops:
			atomic.AddUint64(&src.stats.replicationErrors, 1)
			src.setError(err)
			walPos = atomic.LoadUint64(&src.walPositionPersisted)
			// Send the error to listeners
			src.Errorf("Replication error (walPositionPersisted: %d): %s", walPos, err)

			// Wait to retry
			select {
			case <-src.ctx.Done():
				return
			case <-time.After(src.cfg.startRetryInterval):
			}
		}
	}
}

func (src *PSQL) process(messages []pgoutput.Message) (err error) {
	for _, m := range messages {
		// These come with walPos == 0
		switch v := m.(type) {
		// Relation is the metadata of the table - we cache it locally and then look up on the receipt of the row
		// Potential unbounded map growth, but in practice shouldn't happen as the table count is limited
		case *pgoutput.Relation:
			src.Logf("Relation: %+v", v)
			src.relationSet.Add(*v)
			continue
		// This is not used now
		case *pgoutput.Type:
			continue
		}

		// Check just in case
		if m.WalStart() == 0 {
			continue
		}

		var ev event.Event
		t := time.Now()
		switch v := m.(type) {
		case *pgoutput.Insert:
			ev, err = src.generateEvent(event.ActionInsert, v.RelationID, v.Row)
		case *pgoutput.Update:
			ev, err = src.generateEvent(event.ActionUpdate, v.RelationID, v.Row)
		case *pgoutput.Delete:
			ev, err = src.generateEvent(event.ActionDelete, v.RelationID, v.Row)
		default:
			// Ignore all other events for now (Begin, Commit etc)
			// Their WAL position is equal to an actual INSERT/UPDATE/DELETE event, so don't persist it
			continue
		}

		promTags := append(src.promTags, ev.Table)

		// Report the error if any
		if err != nil {
			atomic.AddUint64(&src.stats.eventErrors, 1)
			src.Errorf("Error generating event: %s", err)
			continue
		}

		defer func() {
			dur := time.Since(t)
			prom.Observe(promTags, dur)
			atomic.AddUint64(&src.stats.events, 1)
			src.LogVerboseEv(ev.UUID, "Event (%+v): %.4f sec", ev, dur.Seconds())
		}()

		ev.UUID = uuid.Must(uuid.NewV4()).String()
		ev.WALEndPosition = m.WalEnd()
		ev.Ack = func() {
			endLSN := m.WalEnd()
			atomic.StoreUint64(&src.walPosition, endLSN)
			src.persistWAL()
		}

		src.LogDebugEv(ev.UUID, "Got message '%#v' (wal %d)", m, m.WalStart())
		src.mux.Push(ev, nil)
	}

	return
}

// Persist the WAL log position to Bolt
func (src *PSQL) persistWAL() {
	// For testing purposes moslty
	if src.boltDB == nil {
		return
	}

	walPos := atomic.LoadUint64(&src.walPosition)

	// Retry forever in case of some Bolt errors (out of disk space?)
	_ = common.RetryForever(src.ctx, common.RetryParams{
		F:            func() error { return src.boltDB.CounterSet(src.boltBucket, db.CounterWALPos, walPos) },
		Interval:     time.Second,
		Logger:       src.Logger,
		ErrorMsg:     "Unable to flush WAL position to Bolt (retry %d): %s",
		ErrorCounter: &src.stats.persistErrors,
	})

	atomic.StoreUint64(&src.walPositionPersisted, walPos)
	src.Debugf("WAL persisted at position %d", walPos)
}

func (src *PSQL) generateEvent(action string, relationID uint32, row []pgoutput.Tuple) (ev event.Event, err error) {
	rel, ok := src.relationSet.Get(relationID)
	if !ok {
		err = fmt.Errorf("Relation with ID '%d' not found in relationSet", relationID)
		return
	}

	ev = event.Event{
		Host:      src.connConfig.Host,
		Database:  src.connConfig.Database,
		Table:     rel.Name,
		Action:    action,
		Timestamp: time.Now(),
		Columns:   map[string]interface{}{},
	}

	if ev.Host == "" {
		ev.Host = "unknown"
	}

	vals, err := src.relationSet.Values(relationID, row)
	if err != nil {
		err = fmt.Errorf("Unable to decode values: %s", err)
		return
	}

	for n, v := range vals {
		// Process only some basic column types
		switch val := v.Get().(type) {
		case string, nil, time.Time, bool,
			uint, uint8, uint16, uint32, uint64,
			int, int8, int16, int32, int64,
			float32, float64:
			ev.Columns[n] = val

		case []byte:
			// It might contain unprintable chars, but JSON should handle them in hex notaion
			ev.Columns[n] = string(val)

		case *net.IPNet:
			ev.Columns[n] = val.String()

		default:
			// Just skip unsupported columns for now, while notifying the user
			src.Errorf("Table %s column %s: unsupported type (%T)", rel.Name, n, val)
		}
	}

	return
}

// Stats returns source's statistics
func (src *PSQL) Stats() string {
	return fmt.Sprintf("events: %d, eventErrors: %d, replicaErrors: %d, persistErrors: %d, walPos: %d, walPosPersist: %d",
		atomic.LoadUint64(&src.stats.events),
		atomic.LoadUint64(&src.stats.eventErrors),
		atomic.LoadUint64(&src.stats.replicationErrors),
		atomic.LoadUint64(&src.stats.persistErrors),
		atomic.LoadUint64(&src.walPosition),
		atomic.LoadUint64(&src.walPositionPersisted),
	)
}

// Status returns the status for the source
func (src *PSQL) Status() error {
	src.RLock()
	defer src.RUnlock()
	return src.err
}

func (src *PSQL) flushWalPosition() {
	src.Flush()
}

// Flush reports to PostgreSQL that all events are consumed and applied
// This allows it to remove logs.
func (src *PSQL) Flush() (err error) {
	src.Lock()
	defer src.Unlock()

	if src.err != nil {
		return fmt.Errorf("Source is not in a stable state")
	}

	if err = src.sub.FlushWalPosition(atomic.LoadUint64(&src.walPositionPersisted)); err != nil {
		src.Errorf("Unable to Flush() subscription: %s", err)
	} else {
		src.Logf("Flushed: %s", src.Stats())
	}

	return
}

func (src *PSQL) setError(err error) {
	src.Lock()
	src.err = err
	src.Unlock()
}
