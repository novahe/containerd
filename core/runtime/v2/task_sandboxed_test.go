package v2

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	taskapi "github.com/containerd/containerd/api/runtime/task/v3"
	apitypes "github.com/containerd/containerd/api/types"
	"github.com/containerd/containerd/v2/core/runtime"
	"github.com/containerd/containerd/v2/core/sandbox"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/containerd/v2/pkg/protobuf"
	ptypes "github.com/containerd/containerd/v2/pkg/protobuf/types"
	"github.com/containerd/errdefs"
	"github.com/containerd/typeurl/v2"
	imagespec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/protobuf/types/known/emptypb"
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

func TestSandboxedTaskCreateRollbackIgnoresCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(testNamespacedContext())
	store := newTestSandboxStore(t, "sandbox-create")
	store.failOnCanceled = true
	taskClient := &testTaskServiceClient{
		createFn: func(context.Context, *taskapi.CreateTaskRequest) (*taskapi.CreateTaskResponse, error) {
			cancel()
			return nil, errors.New("create failed")
		},
	}
	st := &sandboxedTask{
		namespace: testNamespace,
		sandboxHandle: &sandboxClient{
			id:         "sandbox-create",
			store:      store,
			controller: &testSandboxController{failOnCanceled: true},
			manager:    newTestSandboxedTaskManager(),
			ns:         testNamespace,
		},
		remoteTask: &remoteTask{id: "task-create", client: taskClient},
		bundle:     &Bundle{Path: t.TempDir()},
	}

	err := st.Create(ctx, st.bundle.Path, runtime.CreateOpts{
		Spec: mustMarshalAny(t, &oci.Spec{}),
	})
	if err == nil {
		t.Fatalf("expected create to fail")
	}

	tasks := getSandboxTasks(t, store.sandbox)
	if len(tasks.Tasks) != 0 {
		t.Fatalf("expected rollback to remove task metadata, got %+v", tasks.Tasks)
	}
}

func TestSandboxedTaskManagerCleanupFailedCreateIgnoresCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(testNamespacedContext())
	store := newTestSandboxStore(t, "sandbox-cleanup-failed-create")
	store.failOnCanceled = true
	store.mustSetTasks(t, Tasks{Tasks: []Task{{TaskID: "task-cleanup"}}})
	manager := newTestSandboxedTaskManager()
	closer := &testCloser{}
	deleteCalls := 0
	st := &sandboxedTask{
		namespace: testNamespace,
		sandboxHandle: &sandboxClient{
			id:         "sandbox-cleanup-failed-create",
			store:      store,
			controller: &testSandboxController{failOnCanceled: true},
			manager:    manager,
			ns:         testNamespace,
		},
		connection: closer,
		remoteTask: &remoteTask{
			id: "task-cleanup",
			client: &testTaskServiceClient{
				deleteFn: func(ctx context.Context, req *taskapi.DeleteRequest) (*taskapi.DeleteResponse, error) {
					deleteCalls++
					if ctx.Err() != nil {
						t.Fatalf("expected cleanup delete context to ignore cancellation, got %v", ctx.Err())
					}
					if req.ID != "task-cleanup" {
						t.Fatalf("unexpected delete id %q", req.ID)
					}
					return &taskapi.DeleteResponse{}, nil
				},
			},
		},
	}

	cancel()
	manager.cleanupFailedCreate(ctx, "task-cleanup", st)

	if deleteCalls != 1 {
		t.Fatalf("expected one delete call, got %d", deleteCalls)
	}
	if !closer.closed {
		t.Fatalf("expected connection to be closed")
	}
	tasks := getSandboxTasks(t, store.sandbox)
	if len(tasks.Tasks) != 0 {
		t.Fatalf("expected cleanup to remove task metadata, got %+v", tasks.Tasks)
	}
}

func TestSandboxedTaskExecRollbackIgnoresCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(testNamespacedContext())
	store := newTestSandboxStore(t, "sandbox-exec")
	store.failOnCanceled = true
	store.mustSetTasks(t, Tasks{Tasks: []Task{{TaskID: "task-exec"}}})
	taskClient := &testTaskServiceClient{
		execFn: func(context.Context, *taskapi.ExecProcessRequest) (*emptypb.Empty, error) {
			cancel()
			return nil, errors.New("exec failed")
		},
	}
	st := &sandboxedTask{
		namespace: testNamespace,
		sandboxHandle: &sandboxClient{
			id:         "sandbox-exec",
			store:      store,
			controller: &testSandboxController{failOnCanceled: true},
			manager:    newTestSandboxedTaskManager(),
			ns:         testNamespace,
		},
		remoteTask: &remoteTask{id: "task-exec", client: taskClient},
	}

	_, err := st.Exec(ctx, "exec-1", runtime.ExecOpts{
		Spec: mustMarshalAnyProto(t, &specs.Process{}),
	})
	if err == nil {
		t.Fatalf("expected exec to fail")
	}

	tasks := getSandboxTasks(t, store.sandbox)
	if len(tasks.Tasks) != 1 || len(tasks.Tasks[0].Processes) != 0 {
		t.Fatalf("expected rollback to remove exec metadata, got %+v", tasks.Tasks)
	}
}

func TestSandboxedProcessDeleteRemovesMetadataOnNotFound(t *testing.T) {
	ctx, cancel := context.WithCancel(testNamespacedContext())
	store := newTestSandboxStore(t, "sandbox-process-delete")
	store.failOnCanceled = true
	store.mustSetTasks(t, Tasks{Tasks: []Task{{
		TaskID: "task-delete",
		Processes: []Process{{
			ExecID: "exec-1",
		}},
	}}})

	proc := &sandboxedProcess{
		ExecProcess: &testExecProcess{
			id: "exec-1",
			deleteFn: func(context.Context) (*runtime.Exit, error) {
				cancel()
				return nil, errdefs.ErrNotFound
			},
		},
		task: &sandboxedTask{
			namespace: testNamespace,
			sandboxHandle: &sandboxClient{
				id:         "sandbox-process-delete",
				store:      store,
				controller: &testSandboxController{failOnCanceled: true},
				manager:    newTestSandboxedTaskManager(),
				ns:         testNamespace,
			},
			remoteTask: &remoteTask{id: "task-delete"},
		},
	}

	exit, err := proc.Delete(ctx)
	if err == nil || err.Error() != errdefs.ErrNotFound.Error() {
		t.Fatalf("expected not found, got exit=%+v err=%v", exit, err)
	}

	tasks := getSandboxTasks(t, store.sandbox)
	if len(tasks.Tasks) != 1 || len(tasks.Tasks[0].Processes) != 0 {
		t.Fatalf("expected metadata cleanup on not found, got %+v", tasks.Tasks)
	}
}

func TestSandboxedTaskManagerDelete(t *testing.T) {
	type testCase struct {
		name           string
		taskID         string
		sandboxID      string
		setupStore     func(context.Context, context.CancelFunc, *testSandboxStore)
		controller     *testSandboxController
		deleteFn       func(context.Context, context.CancelFunc) func(context.Context, *taskapi.DeleteRequest) (*taskapi.DeleteResponse, error)
		checkResult    func(*testing.T, *runtime.Exit, error, *testSandboxStore)
		checkPostState func(*testing.T, context.Context, *SandboxedTaskManager, *Bundle, *testCloser)
	}

	cases := []testCase{
		{
			name:      "ignores canceled context for metadata cleanup",
			taskID:    "task-delete",
			sandboxID: "sandbox-manager-delete",
			setupStore: func(_ context.Context, _ context.CancelFunc, store *testSandboxStore) {
				store.failOnCanceled = true
				store.mustSetTasks(t, Tasks{Tasks: []Task{{TaskID: "task-delete"}}})
			},
			controller: &testSandboxController{failOnCanceled: true},
			deleteFn: func(_ context.Context, cancel context.CancelFunc) func(context.Context, *taskapi.DeleteRequest) (*taskapi.DeleteResponse, error) {
				return func(context.Context, *taskapi.DeleteRequest) (*taskapi.DeleteResponse, error) {
					cancel()
					return &taskapi.DeleteResponse{
						ExitStatus: 23,
						ExitedAt:   protobuf.ToTimestamp(time.Unix(10, 0)),
						Pid:        99,
					}, nil
				}
			},
			checkResult: func(t *testing.T, exit *runtime.Exit, err error, _ *testSandboxStore) {
				t.Helper()
				if err != nil {
					t.Fatalf("expected delete to succeed, got %v", err)
				}
				if exit == nil || exit.Status != 23 || exit.Pid != 99 {
					t.Fatalf("unexpected exit: %+v", exit)
				}
			},
			checkPostState: func(t *testing.T, ctx context.Context, manager *SandboxedTaskManager, bundle *Bundle, closer *testCloser) {
				t.Helper()
				if !closer.closed {
					t.Fatalf("expected connection to be closed")
				}
				if _, statErr := os.Stat(bundle.Path); !os.IsNotExist(statErr) {
					t.Fatalf("expected bundle path to be removed, stat err=%v", statErr)
				}
				if _, err := manager.tasks.Get(ctx, "task-delete"); !errdefs.IsNotFound(err) {
					t.Fatalf("expected task to be removed from manager, got %v", err)
				}
			},
		},
		{
			name:      "preserves state when metadata update fails",
			taskID:    "task-keep",
			sandboxID: "sandbox-manager-update-fail",
			setupStore: func(_ context.Context, _ context.CancelFunc, store *testSandboxStore) {
				store.mustSetTasks(t, Tasks{Tasks: []Task{{TaskID: "task-keep"}}})
				store.updateErr = errors.New("update failed")
			},
			controller: &testSandboxController{},
			deleteFn: func(_ context.Context, _ context.CancelFunc) func(context.Context, *taskapi.DeleteRequest) (*taskapi.DeleteResponse, error) {
				return func(context.Context, *taskapi.DeleteRequest) (*taskapi.DeleteResponse, error) {
					return &taskapi.DeleteResponse{}, nil
				}
			},
			checkResult: func(t *testing.T, _ *runtime.Exit, err error, store *testSandboxStore) {
				t.Helper()
				if !errors.Is(err, store.updateErr) {
					t.Fatalf("expected update error, got %v", err)
				}
			},
			checkPostState: func(t *testing.T, ctx context.Context, manager *SandboxedTaskManager, bundle *Bundle, closer *testCloser) {
				t.Helper()
				if closer.closed {
					t.Fatalf("expected connection to remain open for retry")
				}
				if _, statErr := os.Stat(bundle.Path); statErr != nil {
					t.Fatalf("expected bundle to be preserved, got %v", statErr)
				}
				if _, err := manager.tasks.Get(ctx, "task-keep"); err != nil {
					t.Fatalf("expected task to remain in manager, got %v", err)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(testNamespacedContext())
			store := newTestSandboxStore(t, tc.sandboxID)
			tc.setupStore(ctx, cancel, store)
			manager := newTestSandboxedTaskManager()
			bundle := newTestBundle(t, testNamespace, tc.taskID)
			closer := &testCloser{}
			st := &sandboxedTask{
				namespace: testNamespace,
				sandboxHandle: &sandboxClient{
					id:         tc.sandboxID,
					store:      store,
					controller: tc.controller,
					manager:    manager,
					ns:         testNamespace,
				},
				bundle:     bundle,
				connection: closer,
				remoteTask: &remoteTask{
					id: tc.taskID,
					client: &testTaskServiceClient{
						deleteFn: tc.deleteFn(ctx, cancel),
					},
				},
			}
			if err := manager.tasks.Add(ctx, st); err != nil {
				t.Fatalf("failed to add task: %v", err)
			}

			exit, err := manager.Delete(ctx, tc.taskID)
			tc.checkResult(t, exit, err, store)
			tc.checkPostState(t, ctx, manager, bundle, closer)
		})
	}
}

type testSandboxStore struct {
	sandbox        sandbox.Sandbox
	getCount       int
	updateErr      error
	failOnCanceled bool
}

func (s *testSandboxStore) Create(ctx context.Context, sandbox sandbox.Sandbox) (sandbox.Sandbox, error) {
	panic("unexpected call to Create")
}

func (s *testSandboxStore) Update(ctx context.Context, sandbox sandbox.Sandbox, fieldpaths ...string) (sandbox.Sandbox, error) {
	if s.failOnCanceled && ctx.Err() != nil {
		return s.sandbox, ctx.Err()
	}
	if s.updateErr != nil {
		return s.sandbox, s.updateErr
	}
	old := s.sandbox
	s.sandbox = sandbox
	return old, nil
}

func (s *testSandboxStore) Get(ctx context.Context, id string) (sandbox.Sandbox, error) {
	if s.failOnCanceled && ctx.Err() != nil {
		return sandbox.Sandbox{}, ctx.Err()
	}
	s.getCount++
	return s.sandbox, nil
}

func (s *testSandboxStore) List(ctx context.Context, filters ...string) ([]sandbox.Sandbox, error) {
	panic("unexpected call to List")
}

func (s *testSandboxStore) Delete(ctx context.Context, id string) error {
	panic("unexpected call to Delete")
}

type testSandboxController struct {
	updateErr      error
	failOnCanceled bool
}

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

func (c *testSandboxController) Update(ctx context.Context, sandboxID string, sandboxInfo sandbox.Sandbox, fields ...string) error {
	if c.failOnCanceled && ctx.Err() != nil {
		return ctx.Err()
	}
	return c.updateErr
}

var _ sandbox.Store = (*testSandboxStore)(nil)
var _ sandbox.Controller = (*testSandboxController)(nil)

const testNamespace = "testns"

func newTestSandboxedTaskManager() *SandboxedTaskManager {
	return &SandboxedTaskManager{
		tasks:        runtime.NewNSMap[*sandboxedTask](),
		sandboxLocks: make(map[string]*refMutex),
	}
}

func testNamespacedContext() context.Context {
	return namespaces.WithNamespace(context.Background(), testNamespace)
}

func newTestSandboxStore(t *testing.T, sandboxID string) *testSandboxStore {
	t.Helper()
	return &testSandboxStore{
		sandbox: sandbox.Sandbox{ID: sandboxID},
	}
}

func (s *testSandboxStore) mustSetTasks(t *testing.T, tasks Tasks) {
	t.Helper()
	if err := s.sandbox.AddExtension(TasksKey, &tasks); err != nil {
		t.Fatalf("failed to set tasks extension: %v", err)
	}
}

func getSandboxTasks(t *testing.T, sb sandbox.Sandbox) Tasks {
	t.Helper()
	var tasks Tasks
	if err := sb.GetExtension(TasksKey, &tasks); err != nil && !errdefs.IsNotFound(err) {
		t.Fatalf("failed to read tasks extension: %v", err)
	}
	return tasks
}

func mustMarshalAny(t *testing.T, v interface{}) typeurl.Any {
	t.Helper()
	any, err := typeurl.MarshalAny(v)
	if err != nil {
		t.Fatalf("failed to marshal any: %v", err)
	}
	return any
}

func mustMarshalAnyProto(t *testing.T, v interface{}) *ptypes.Any {
	t.Helper()
	any, err := typeurl.MarshalAnyToProto(v)
	if err != nil {
		t.Fatalf("failed to marshal proto any: %v", err)
	}
	return any
}

func newTestBundle(t *testing.T, namespace, taskID string) *Bundle {
	t.Helper()
	root := t.TempDir()
	bundlePath := filepath.Join(root, namespace, taskID)
	workPath := filepath.Join(root, "work", namespace, taskID)
	if err := os.MkdirAll(filepath.Join(bundlePath, "rootfs"), 0o755); err != nil {
		t.Fatalf("failed to create rootfs: %v", err)
	}
	if err := os.MkdirAll(workPath, 0o755); err != nil {
		t.Fatalf("failed to create work dir: %v", err)
	}
	if err := os.Symlink(workPath, filepath.Join(bundlePath, "work")); err != nil {
		t.Fatalf("failed to create work symlink: %v", err)
	}
	return &Bundle{
		ID:        taskID,
		Path:      bundlePath,
		Namespace: namespace,
	}
}

type testTaskServiceClient struct {
	createFn func(context.Context, *taskapi.CreateTaskRequest) (*taskapi.CreateTaskResponse, error)
	deleteFn func(context.Context, *taskapi.DeleteRequest) (*taskapi.DeleteResponse, error)
	execFn   func(context.Context, *taskapi.ExecProcessRequest) (*emptypb.Empty, error)
}

func (c *testTaskServiceClient) State(context.Context, *taskapi.StateRequest) (*taskapi.StateResponse, error) {
	panic("unexpected call to State")
}

func (c *testTaskServiceClient) Create(ctx context.Context, req *taskapi.CreateTaskRequest) (*taskapi.CreateTaskResponse, error) {
	if c.createFn != nil {
		return c.createFn(ctx, req)
	}
	panic("unexpected call to Create")
}

func (c *testTaskServiceClient) Start(context.Context, *taskapi.StartRequest) (*taskapi.StartResponse, error) {
	panic("unexpected call to Start")
}

func (c *testTaskServiceClient) Delete(ctx context.Context, req *taskapi.DeleteRequest) (*taskapi.DeleteResponse, error) {
	if c.deleteFn != nil {
		return c.deleteFn(ctx, req)
	}
	panic("unexpected call to Delete")
}

func (c *testTaskServiceClient) Pids(context.Context, *taskapi.PidsRequest) (*taskapi.PidsResponse, error) {
	panic("unexpected call to Pids")
}

func (c *testTaskServiceClient) Pause(context.Context, *taskapi.PauseRequest) (*emptypb.Empty, error) {
	panic("unexpected call to Pause")
}

func (c *testTaskServiceClient) Resume(context.Context, *taskapi.ResumeRequest) (*emptypb.Empty, error) {
	panic("unexpected call to Resume")
}

func (c *testTaskServiceClient) Checkpoint(context.Context, *taskapi.CheckpointTaskRequest) (*emptypb.Empty, error) {
	panic("unexpected call to Checkpoint")
}

func (c *testTaskServiceClient) Kill(context.Context, *taskapi.KillRequest) (*emptypb.Empty, error) {
	panic("unexpected call to Kill")
}

func (c *testTaskServiceClient) Exec(ctx context.Context, req *taskapi.ExecProcessRequest) (*emptypb.Empty, error) {
	if c.execFn != nil {
		return c.execFn(ctx, req)
	}
	panic("unexpected call to Exec")
}

func (c *testTaskServiceClient) ResizePty(context.Context, *taskapi.ResizePtyRequest) (*emptypb.Empty, error) {
	panic("unexpected call to ResizePty")
}

func (c *testTaskServiceClient) CloseIO(context.Context, *taskapi.CloseIORequest) (*emptypb.Empty, error) {
	panic("unexpected call to CloseIO")
}

func (c *testTaskServiceClient) Update(context.Context, *taskapi.UpdateTaskRequest) (*emptypb.Empty, error) {
	panic("unexpected call to Update")
}

func (c *testTaskServiceClient) Wait(context.Context, *taskapi.WaitRequest) (*taskapi.WaitResponse, error) {
	panic("unexpected call to Wait")
}

func (c *testTaskServiceClient) Stats(context.Context, *taskapi.StatsRequest) (*taskapi.StatsResponse, error) {
	panic("unexpected call to Stats")
}

func (c *testTaskServiceClient) Connect(context.Context, *taskapi.ConnectRequest) (*taskapi.ConnectResponse, error) {
	panic("unexpected call to Connect")
}

func (c *testTaskServiceClient) Shutdown(context.Context, *taskapi.ShutdownRequest) (*emptypb.Empty, error) {
	panic("unexpected call to Shutdown")
}

type testExecProcess struct {
	id       string
	deleteFn func(context.Context) (*runtime.Exit, error)
}

func (p *testExecProcess) ID() string {
	return p.id
}

func (p *testExecProcess) State(context.Context) (runtime.State, error) {
	panic("unexpected call to State")
}

func (p *testExecProcess) Kill(context.Context, uint32, bool) error {
	panic("unexpected call to Kill")
}

func (p *testExecProcess) ResizePty(context.Context, runtime.ConsoleSize) error {
	panic("unexpected call to ResizePty")
}

func (p *testExecProcess) CloseIO(context.Context) error {
	panic("unexpected call to CloseIO")
}

func (p *testExecProcess) Start(context.Context) error {
	panic("unexpected call to Start")
}

func (p *testExecProcess) Wait(context.Context) (*runtime.Exit, error) {
	panic("unexpected call to Wait")
}

func (p *testExecProcess) Delete(ctx context.Context) (*runtime.Exit, error) {
	if p.deleteFn != nil {
		return p.deleteFn(ctx)
	}
	panic("unexpected call to Delete")
}

type testCloser struct {
	closed bool
}

func (c *testCloser) Close() error {
	c.closed = true
	return nil
}
