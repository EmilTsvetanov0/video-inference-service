package orchestrator

import (
	"context"
	"github.com/looplab/fsm"
	"log"
	"sync"
	"time"
)

const (
	StInitStartup          = "init_startup"
	StInStartupProcessing  = "in_startup_processing"
	StActive               = "active"
	StInitShutdown         = "init_shutdown"
	StInShutdownProcessing = "in_shutdown_processing"
	StInactive             = "inactive"

	HeartbeatTTL = 5 * time.Second
	MaxHBRetries = 3
)

type RunnerService interface {
	StartRunner(id string)
	StopRunner(id string)
}

type Scenario struct {
	ID          string
	FSM         *fsm.FSM
	hbMu        sync.Mutex
	lastHB      time.Time
	cancelHB    context.CancelFunc
	rs          RunnerService
	needRestart bool
	restartMu   sync.Mutex
}

func NewScenario(id string, enterState func(ctx context.Context, id string, dst string), runnerService RunnerService) *Scenario {
	s := &Scenario{ID: id, rs: runnerService, needRestart: false}
	s.Reset()

	s.FSM = fsm.NewFSM(
		StInitStartup,
		fsm.Events{
			{Name: "begin_startup", Src: []string{StInitStartup, StInactive}, Dst: StInStartupProcessing},
			{Name: "complete_startup", Src: []string{StInStartupProcessing}, Dst: StActive},
			{Name: "begin_shutdown", Src: []string{StActive, StInStartupProcessing}, Dst: StInitShutdown},
			{Name: "process_shutdown", Src: []string{StInitShutdown}, Dst: StInShutdownProcessing},
			{Name: "complete_shutdown", Src: []string{StInShutdownProcessing}, Dst: StInactive},
		},
		fsm.Callbacks{
			// ---- запуск ----
			"enter_in_startup_processing": s.cbEnterStartupProcessing,
			"before_complete_startup":     s.cbBeforeCompleteStartup,
			"enter_active":                s.cbEnterActive,

			// ---- остановка ----
			"enter_init_shutdown":          s.cbEnterInitShutdown,
			"enter_in_shutdown_processing": s.cbEnterInShutdownProcessing,
			"enter_inactive":               s.cbEnterInactive,

			// ---- для логов и сохранения состояния
			"enter_state": func(ctx context.Context, e *fsm.Event) {
				enterState(ctx, s.ID, e.Dst)
				log.Printf("[orchestrator] [FSM] %s : %s -> %s  (by %s)", s.ID, e.Src, e.Dst, e.Event)
			},
		},
	)
	return s
}

// Колбеки FSM

func (s *Scenario) cbEnterStartupProcessing(ctx context.Context, e *fsm.Event) {
	s.SetNeedRestart(false)

	log.Printf("[orchestrator] [%s] entering startup processing, current state: %s",
		s.ID, s.FSM.Current())

	s.stopWatchdog()

	s.rs.StartRunner(s.ID)

	go func() {
		time.Sleep(100 * time.Millisecond)
		if err := s.FSM.Event(context.Background(), "complete_startup"); err != nil {
			log.Printf("[orchestrator] [%s] complete_startup error: %v", s.ID, err)
		}
	}()
}

func (s *Scenario) cbBeforeCompleteStartup(ctx context.Context, e *fsm.Event) {
	if !s.waitForHeartbeat(MaxHBRetries) {
		log.Printf("[orchestrator] [%s] heartbeat not received, triggering shutdown", s.ID)

		if s.FSM.Current() == StInStartupProcessing {
			e.Cancel(nil)

			go func() {
				time.Sleep(100 * time.Millisecond)
				s.SetNeedRestart(true)
				if err := s.FSM.Event(context.Background(), "begin_shutdown"); err != nil {
					log.Printf("[orchestrator] [%s] begin_shutdown triggered with error: %v", s.ID, err)
				}
			}()
		}
	}
}

func (s *Scenario) cbEnterActive(ctx context.Context, e *fsm.Event) {
	watchdogCtx, cancel := context.WithCancel(context.Background())
	s.cancelHB = cancel
	go s.heartbeatWatchdog(watchdogCtx)
}

func (s *Scenario) cbEnterInitShutdown(ctx context.Context, e *fsm.Event) {
	log.Printf("[orchestrator] [%s] initiating shutdown: notifying runner. Current state: %s",
		s.ID, s.FSM.Current())

	s.rs.StopRunner(s.ID)

	go func() {
		time.Sleep(100 * time.Millisecond)
		if err := s.FSM.Event(context.Background(), "process_shutdown"); err != nil {
			log.Printf("[orchestrator] [%s] process_shutdown error: %v", s.ID, err)
		}
	}()
}

func (s *Scenario) cbEnterInShutdownProcessing(ctx context.Context, e *fsm.Event) {
	log.Printf("[orchestrator] [%s] shutdown processing: stopping watchdog", s.ID)

	s.stopWatchdog()

	go func() {
		time.Sleep(100 * time.Millisecond)
		if err := s.FSM.Event(context.Background(), "complete_shutdown"); err != nil {
			log.Printf("[orchestrator] [%s] complete_shutdown failed: %v", s.ID, err)
		}
	}()
}

func (s *Scenario) cbEnterInactive(ctx context.Context, e *fsm.Event) {
	log.Printf("[orchestrator] [%s] scenario is now inactive", s.ID)

	if s.NeedRestart() {
		log.Printf("[orchestrator] [%s] restarting scenario after going inactive", s.ID)
		s.SetNeedRestart(false)

		go func() {
			time.Sleep(100 * time.Millisecond)
			if err := s.FSM.Event(context.Background(), "begin_startup"); err != nil {
				log.Printf("[orchestrator] [%s] begin_startup failed after restart: %v", s.ID, err)
			}
		}()
	}
}

// Heartbeats

func (s *Scenario) AcceptHeartbeat() {
	s.hbMu.Lock()
	s.lastHB = time.Now()
	s.hbMu.Unlock()
}

// Внутренняя логика

func (s *Scenario) SetNeedRestart(v bool) {
	s.restartMu.Lock()
	defer s.restartMu.Unlock()
	s.needRestart = v
}

func (s *Scenario) NeedRestart() bool {
	s.restartMu.Lock()
	defer s.restartMu.Unlock()
	return s.needRestart
}

func (s *Scenario) Reset() {
	s.hbMu.Lock()
	s.lastHB = time.Time{}
	s.hbMu.Unlock()
}

func (s *Scenario) waitForHeartbeat(maxRetries int) bool {
	for i := 0; i < maxRetries; i++ {
		if s.IsOk() {
			return true
		}
		log.Printf("[orchestrator] [%s] no heartbeat, retry %d", s.ID, i+1)
		time.Sleep(HeartbeatTTL)
	}
	return false
}

func (s *Scenario) IsOk() bool {
	s.hbMu.Lock()
	ok := time.Since(s.lastHB) <= HeartbeatTTL
	s.hbMu.Unlock()
	if ok {
		return true
	}
	return false
}

func (s *Scenario) stopWatchdog() {
	if s.cancelHB != nil {
		s.cancelHB()
		s.cancelHB = nil
	}
}

func (s *Scenario) heartbeatWatchdog(ctx context.Context) {
	log.Printf("[orchestrator] [FSM] [%s] heartbeat watchdog running", s.ID)
	t := time.NewTicker(HeartbeatTTL)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.hbMu.Lock()
			lastHB := s.lastHB
			s.hbMu.Unlock()

			expired := time.Since(lastHB) > HeartbeatTTL

			currentState := s.FSM.Current()
			if expired && currentState == StActive {
				log.Printf("[orchestrator] [%s] watchdog check: state=%s, lastHB=%v",
					s.ID, s.FSM.Current(), time.Since(s.lastHB))

				log.Printf("[orchestrator] [%s] heartbeat lost -> restart runner", s.ID)

				s.SetNeedRestart(true)
				_ = s.FSM.Event(context.Background(), "begin_shutdown")
			}
		}
	}
}
