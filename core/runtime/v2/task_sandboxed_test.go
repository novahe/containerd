package v2

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	apitypes "github.com/containerd/containerd/api/types"
	"github.com/containerd/containerd/v2/core/sandbox"
	imagespec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestWithSandboxLockSerializesAndCleansUp(t *testing.T) {
	manager := &SandboxedTaskManager{
		sandboxLocks: make(map[string]*refMutex),
	}

	started := make(chan struct{})
	blocking := make(chan struct{})
	done := make(chan struct{})

	go func() {
		_ = manager.withSandboxLock("ns-1", "sandbox-1", func() error {
			close(started)
			<-blocking
			return nil
		})
		close(done)
	}()

	<-started

	// While the first execution is blocked inside the lock, the map should contain the initialized lock with refs=1
	manager.sandboxMu.Lock()
	rm, ok := manager.sandboxLocks["ns-1/sandbox-1"]
	manager.sandboxMu.Unlock()

	if !ok || rm.refs != 1 {
		t.Fatalf("expected sandbox lock to be registered and refs=1 during execution")
	}

	close(blocking)
	<-done

	// After the closed execution completes, the lock should be garbage collected
	manager.sandboxMu.Lock()
	_, ok = manager.sandboxLocks["ns-1/sandbox-1"]
	manager.sandboxMu.Unlock()

	if ok {
		t.Fatalf("expected sandbox lock to be cleaned up after execution")
	}
}

func TestWithSandboxLockABAScenario(t *testing.T) {
	manager := &SandboxedTaskManager{
		sandboxLocks: make(map[string]*refMutex),
	}

	var wg sync.WaitGroup
	var executedCount int32
	var cacheChecks int32

	blockFirst := make(chan struct{})
	startedFirst := make(chan struct{})
	waitersReady := make(chan struct{}, 2)

	// Thread 1: Acquire and hold lock
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = manager.withSandboxLock("ns-1", "sandbox-1", func() error {
			close(startedFirst)
			<-blockFirst
			atomic.AddInt32(&executedCount, 1)
			return nil
		})
	}()

	<-startedFirst

	// Thread 2 and Thread 3: Arrive and block on the same lock
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			manager.sandboxMu.Lock()
			rm, ok := manager.sandboxLocks["ns-1/sandbox-1"]
			if !ok {
				rm = &refMutex{}
				manager.sandboxLocks["ns-1/sandbox-1"] = rm
			}
			rm.refs++
			manager.sandboxMu.Unlock()

			waitersReady <- struct{}{}

			defer func() {
				manager.sandboxMu.Lock()
				defer manager.sandboxMu.Unlock()
				rm.refs--
				if rm.refs == 0 {
					delete(manager.sandboxLocks, "ns-1/sandbox-1")
				}
			}()

			rm.Lock()
			defer rm.Unlock()

			manager.sandboxMu.Lock()
			current, ok := manager.sandboxLocks["ns-1/sandbox-1"]
			refs := 0
			if ok {
				refs = current.refs
			}
			manager.sandboxMu.Unlock()

			if !ok {
				t.Errorf("aba error: lock not found in cache while executing")
			} else if refs < 1 {
				t.Errorf("aba error: lock execution with refs < 1")
			}

			atomic.AddInt32(&cacheChecks, 1)
			atomic.AddInt32(&executedCount, 1)
		}()
	}

	<-waitersReady
	<-waitersReady

	manager.sandboxMu.Lock()
	rm, ok := manager.sandboxLocks["ns-1/sandbox-1"]
	refs := 0
	if ok {
		refs = rm.refs
	}
	manager.sandboxMu.Unlock()

	if !ok || refs != 3 {
		t.Fatalf("expected 1 active lock with 3 waiters, got ok=%v refs=%d", ok, refs)
	}

	// Unblock T1, which will trigger sequential unstubbing of T2 and T3.
	close(blockFirst)
	wg.Wait()

	if executedCount != 3 {
		t.Fatalf("expected executedCount to be 3")
	}

	if cacheChecks != 2 {
		t.Fatalf("expected cacheChecks to be 2")
	}

	manager.sandboxMu.Lock()
	_, stillExists := manager.sandboxLocks["ns-1/sandbox-1"]
	manager.sandboxMu.Unlock()

	if stillExists {
		t.Fatalf("expected sandbox lock to be deleted permanently after ABA race")
	}
}

type testSandboxStore struct {
	sandbox  sandbox.Sandbox
	getCount int
}

func (s *testSandboxStore) Create(ctx context.Context, sandbox sandbox.Sandbox) (sandbox.Sandbox, error) {
	panic("unexpected call to Create")
}

func (s *testSandboxStore) Update(ctx context.Context, sandbox sandbox.Sandbox, fieldpaths ...string) (sandbox.Sandbox, error) {
	panic("unexpected call to Update")
}

func (s *testSandboxStore) Get(ctx context.Context, id string) (sandbox.Sandbox, error) {
	s.getCount++
	return s.sandbox, nil
}

func (s *testSandboxStore) List(ctx context.Context, filters ...string) ([]sandbox.Sandbox, error) {
	panic("unexpected call to List")
}

func (s *testSandboxStore) Delete(ctx context.Context, id string) error {
	panic("unexpected call to Delete")
}

type testSandboxController struct{}

func (testSandboxController) Create(ctx context.Context, sandboxInfo sandbox.Sandbox, opts ...sandbox.CreateOpt) error {
	panic("unexpected call to Create")
}

func (testSandboxController) Start(ctx context.Context, sandboxID string) (sandbox.ControllerInstance, error) {
	panic("unexpected call to Start")
}

func (testSandboxController) Platform(ctx context.Context, sandboxID string) (imagespec.Platform, error) {
	return imagespec.Platform{}, nil
}

func (testSandboxController) Stop(ctx context.Context, sandboxID string, opts ...sandbox.StopOpt) error {
	panic("unexpected call to Stop")
}

func (testSandboxController) Wait(ctx context.Context, sandboxID string) (sandbox.ExitStatus, error) {
	panic("unexpected call to Wait")
}

func (testSandboxController) Status(ctx context.Context, sandboxID string, verbose bool) (sandbox.ControllerStatus, error) {
	panic("unexpected call to Status")
}

func (testSandboxController) Shutdown(ctx context.Context, sandboxID string) error {
	panic("unexpected call to Shutdown")
}

func (testSandboxController) Metrics(ctx context.Context, sandboxID string) (*apitypes.Metric, error) {
	panic("unexpected call to Metrics")
}

func (testSandboxController) Update(ctx context.Context, sandboxID string, sandboxInfo sandbox.Sandbox, fields ...string) error {
	panic("unexpected call to Update")
}

var _ sandbox.Store = (*testSandboxStore)(nil)
var _ sandbox.Controller = testSandboxController{}
