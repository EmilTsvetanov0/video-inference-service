package orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"
)

// ---- fakes & helpers ----

type fakeRunnerService struct {
	mu         sync.Mutex
	startCalls []string
	stopCalls  []string
	startCh    chan string
	stopCh     chan string
}

func newFakeRunnerService() *fakeRunnerService {
	return &fakeRunnerService{
		startCh: make(chan string, 10),
		stopCh:  make(chan string, 10),
	}
}

func (f *fakeRunnerService) StartRunner(id string) {
	f.mu.Lock()
	f.startCalls = append(f.startCalls, id)
	f.mu.Unlock()
	f.startCh <- id
}

func (f *fakeRunnerService) StopRunner(id string) {
	f.mu.Lock()
	f.stopCalls = append(f.stopCalls, id)
	f.mu.Unlock()
	f.stopCh <- id
}

func waitForState(t *testing.T, s *Scenario, want string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.FSM.Current() == want {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return s.FSM.Current() == want
}

func collectTransitions() (func(context.Context, string, string), *[]string, *sync.Mutex) {
	var (
		transitions []string
		mu          sync.Mutex
	)
	cb := func(_ context.Context, _ string, dst string) {
		mu.Lock()
		transitions = append(transitions, dst)
		mu.Unlock()
	}
	return cb, &transitions, &mu
}

// ---- tests ----

func TestScenario_StartupWithHeartbeat_ToActive(t *testing.T) {
	t.Parallel()

	frs := newFakeRunnerService()
	enterCB, transitions, trMu := collectTransitions()

	jobName := "job-startup"
	s := NewScenario(jobName, enterCB, frs)

	t.Cleanup(func() {
		if err := s.FSM.Event(context.Background(), "begin_shutdown"); err != nil {
			t.Fatalf("begin_shutdown error: %v", err)
		}
	})

	if err := s.FSM.Event(context.Background(), "begin_startup"); err != nil {
		t.Fatalf("begin_startup error: %v", err)
	}

	select {
	case id := <-frs.startCh:
		if id != jobName {
			t.Fatalf("StartRunner for wrong id: %s", id)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("StartRunner was not called")
	}

	s.AcceptHeartbeat()

	if !waitForState(t, s, StActive, 1*time.Second) {
		trMu.Lock()
		got := append([]string{}, (*transitions)...)
		trMu.Unlock()
		t.Fatalf("state is %q, want %q, transitions: %v", s.FSM.Current(), StActive, got)
	}
}

// Heartbeat не получен в in_startup_processing -> Restart
func TestScenario_StartupWithoutHeartbeat_Restarts(t *testing.T) {
	frs := newFakeRunnerService()
	enterCB, _, _ := collectTransitions()

	jobName := "job-startup-restart"
	s := NewScenario(jobName, enterCB, frs)

	t.Cleanup(func() {
		if err := s.FSM.Event(context.Background(), "begin_shutdown"); err != nil {
			t.Fatalf("begin_shutdown error: %v", err)
		}
	})

	if err := s.FSM.Event(context.Background(), "begin_startup"); err != nil {
		t.Fatalf("begin_startup error: %v", err)
	}

	select {
	case id := <-frs.startCh:
		if id != jobName {
			t.Fatalf("StartRunner for wrong id: %s", id)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("StartRunner was not called")
	}

	// Не отправляем heartbeat

	var (
		sawStop       bool
		sawStartAgain bool
	)
	timeout := time.NewTimer(HeartbeatTTL*MaxHBRetries + 2*time.Second)
	defer timeout.Stop()

	for !(sawStop && sawStartAgain) {
		select {
		case <-timeout.C:
			t.Fatalf("watchdog did not trigger restart in time; stop=%v startAgain=%v state=%q",
				sawStop, sawStartAgain, s.FSM.Current())
		case id := <-frs.stopCh:
			if id != jobName {
				t.Fatalf("StopRunner for wrong id: %s", id)
			}
			sawStop = true
		case id := <-frs.startCh:
			if id != jobName {
				t.Fatalf("StartRunner for wrong id: %s", id)
			}
			if sawStop {
				sawStartAgain = true
			}
		}
	}

}

// Пропал heartbeat в Active -> Watchdog вызывает StopRunner -> Restart
func TestScenario_Watchdog_RestartsOnHbLoss(t *testing.T) {
	frs := newFakeRunnerService()
	enterCB, _, _ := collectTransitions()

	jobName := "job-active-restart"
	s := NewScenario(jobName, enterCB, frs)

	t.Cleanup(func() {
		if err := s.FSM.Event(context.Background(), "begin_shutdown"); err != nil {
			t.Fatalf("begin_shutdown error: %v", err)
		}
	})

	if err := s.FSM.Event(context.Background(), "begin_startup"); err != nil {
		t.Fatalf("begin_startup error: %v", err)
	}
	select {
	case id := <-frs.startCh:
		if id != jobName {
			t.Fatalf("StartRunner for wrong id: %s", id)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("StartRunner was not called (initial)")
	}

	s.AcceptHeartbeat()
	if !waitForState(t, s, StActive, 1*time.Second) {
		t.Fatalf("did not reach %q, current=%q", StActive, s.FSM.Current())
	}

	// Не отправляем heartbeat, ждём срабатывания watchdog

	var (
		sawStop       bool
		sawStartAgain bool
	)
	timeout := time.NewTimer(HeartbeatTTL + 2*time.Second)
	defer timeout.Stop()

	for !(sawStop && sawStartAgain) {
		select {
		case <-timeout.C:
			t.Fatalf("watchdog did not trigger restart in time; stop=%v startAgain=%v state=%q",
				sawStop, sawStartAgain, s.FSM.Current())
		case id := <-frs.stopCh:
			if id != jobName {
				t.Fatalf("StopRunner for wrong id: %s", id)
			}
			sawStop = true
		case id := <-frs.startCh:
			if id != jobName {
				t.Fatalf("StartRunner for wrong id: %s", id)
			}
			if sawStop {
				sawStartAgain = true
			}
		}
	}
}

// AcceptHeartbeat делает IsOk()=true
func TestScenario_IsOk_AfterAcceptHeartbeat(t *testing.T) {
	t.Parallel()

	frs := newFakeRunnerService()
	jobName := "job-is-ok"
	s := NewScenario(jobName, func(context.Context, string, string) {}, frs)

	if s.IsOk() {
		t.Fatal("IsOk() should be false initially")
	}
	s.AcceptHeartbeat()
	if !s.IsOk() {
		t.Fatal("IsOk() should be true right after AcceptHeartbeat()")
	}
}
