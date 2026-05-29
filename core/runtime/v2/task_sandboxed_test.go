/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package v2

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	taskapi "github.com/containerd/containerd/api/runtime/task/v3"
	"github.com/containerd/containerd/api/types"
	"github.com/containerd/errdefs"
	"github.com/containerd/errdefs/pkg/errgrpc"
	"github.com/containerd/typeurl/v2"
	"github.com/opencontainers/image-spec/specs-go/v1"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/protobuf/types/known/emptypb"

	ctrmount "github.com/containerd/containerd/v2/core/mount"
	ctrruntime "github.com/containerd/containerd/v2/core/runtime"
	"github.com/containerd/containerd/v2/core/sandbox"
	"github.com/containerd/containerd/v2/pkg/namespaces"
)

func TestSandboxedTaskProcessDeleteRemovesExecExtension(t *testing.T) {
	ctx := namespaces.WithNamespace(context.Background(), "default")
	store := newTestSandboxStore(t, Tasks{
		Tasks: []Task{
			{
				TaskID: "task",
				Processes: []Process{
					{ExecID: "exec"},
				},
			},
		},
	})
	st := newTestSandboxedTask(store, &testTaskServiceClient{})

	p, err := st.Process(ctx, "exec")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.(*sandboxedProcess); !ok {
		t.Fatalf("expected sandboxed process, got %T", p)
	}
	if _, err := p.Delete(ctx); err != nil {
		t.Fatal(err)
	}

	tasks := getTestSandboxTasks(t, store.sb)
	if got := len(tasks.Tasks[0].Processes); got != 0 {
		t.Fatalf("expected exec process extension to be removed, got %d", got)
	}
}

func TestSandboxedTaskExecRollsBackExtensionOnFailure(t *testing.T) {
	ctx := namespaces.WithNamespace(context.Background(), "default")
	store := newTestSandboxStore(t, Tasks{
		Tasks: []Task{{TaskID: "task"}},
	})
	st := newTestSandboxedTask(store, &testTaskServiceClient{
		execErr: errors.New("exec failed"),
	})

	specAny, err := typeurl.MarshalAny(&specs.Process{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Exec(ctx, "exec", ctrruntime.ExecOpts{
		Spec: typeurl.MarshalProto(specAny),
	}); err == nil {
		t.Fatal("expected exec error")
	}

	tasks := getTestSandboxTasks(t, store.sb)
	if got := len(tasks.Tasks[0].Processes); got != 0 {
		t.Fatalf("expected failed exec extension to be rolled back, got %d", got)
	}
}

func TestSandboxedTaskExecRestoresReplacedExtensionOnFailure(t *testing.T) {
	ctx := namespaces.WithNamespace(context.Background(), "default")
	store := newTestSandboxStore(t, Tasks{
		Tasks: []Task{
			{
				TaskID: "task",
				Processes: []Process{
					{ExecID: "exec", Stdin: "old"},
				},
			},
		},
	})
	st := newTestSandboxedTask(store, &testTaskServiceClient{
		execErr: errors.New("exec failed"),
	})

	specAny, err := typeurl.MarshalAny(&specs.Process{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Exec(ctx, "exec", ctrruntime.ExecOpts{
		Spec: typeurl.MarshalProto(specAny),
		IO:   ctrruntime.IO{Stdin: "new"},
	}); err == nil {
		t.Fatal("expected exec error")
	}

	tasks := getTestSandboxTasks(t, store.sb)
	if got := len(tasks.Tasks[0].Processes); got != 1 {
		t.Fatalf("expected existing exec process extension to remain, got %d", got)
	}
	if got := tasks.Tasks[0].Processes[0].Stdin; got != "old" {
		t.Fatalf("expected existing exec process extension to be restored, got stdin %q", got)
	}
}

func TestSandboxedTaskCreateRestoresReplacedExtensionOnFailure(t *testing.T) {
	ctx := namespaces.WithNamespace(context.Background(), "default")
	store := newTestSandboxStore(t, Tasks{
		Tasks: []Task{{TaskID: "task", Stdin: "old"}},
	})
	st := newTestSandboxedTask(store, &testTaskServiceClient{
		createErr: errors.New("create failed"),
	})

	specAny, err := typeurl.MarshalAny(&specs.Spec{})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Create(ctx, t.TempDir(), ctrruntime.CreateOpts{
		Spec: typeurl.MarshalProto(specAny),
		IO:   ctrruntime.IO{Stdin: "new"},
	}); err == nil {
		t.Fatal("expected create error")
	}

	tasks := getTestSandboxTasks(t, store.sb)
	if got := len(tasks.Tasks); got != 1 {
		t.Fatalf("expected existing task extension to remain, got %d", got)
	}
	if got := tasks.Tasks[0].Stdin; got != "old" {
		t.Fatalf("expected existing task extension to be restored, got stdin %q", got)
	}
}

func TestSandboxedTaskCreateRollsBackExtensionWithCanceledContext(t *testing.T) {
	baseCtx := namespaces.WithNamespace(context.Background(), "default")
	ctx, cancel := context.WithCancel(baseCtx)
	store := newTestSandboxStore(t, Tasks{
		Tasks: []Task{},
	})
	store.failOnCanceledContext = true
	st := newTestSandboxedTask(store, &testTaskServiceClient{
		createHook: cancel,
		createErr:  context.Canceled,
	})

	specAny, err := typeurl.MarshalAny(&specs.Spec{})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Create(ctx, t.TempDir(), ctrruntime.CreateOpts{
		Spec: typeurl.MarshalProto(specAny),
	}); err == nil {
		t.Fatal("expected create error")
	}

	tasks := getTestSandboxTasks(t, store.sb)
	if got := len(tasks.Tasks); got != 0 {
		t.Fatalf("expected task extension to be rolled back after context cancellation, got %d", got)
	}
}

func TestSandboxClientUpdateTasksExtensionRollsBackStoreOnControllerFailure(t *testing.T) {
	ctx := namespaces.WithNamespace(context.Background(), "default")
	store := newTestSandboxStore(t, Tasks{
		Tasks: []Task{{TaskID: "task"}},
	})
	mu := &sync.Mutex{}
	sb := &sandboxClient{
		id:         "sandbox",
		store:      store,
		controller: testSandboxController{updateErr: errors.New("controller update failed")},
		lock: func() func() {
			mu.Lock()
			return mu.Unlock
		},
	}

	if err := sb.UpdateTasksExtension(ctx, func(ts *Tasks) error {
		return ts.updateTask("task", func(t *Task) error {
			t.addProcess(Process{ExecID: "exec"})
			return nil
		})
	}); err == nil {
		t.Fatal("expected controller update error")
	}

	tasks := getTestSandboxTasks(t, store.sb)
	if got := len(tasks.Tasks[0].Processes); got != 0 {
		t.Fatalf("expected sandbox store to roll back exec process extension, got %d", got)
	}
}

func TestSandboxedTaskManagerDeleteKeepsTaskWhenExtensionCleanupFails(t *testing.T) {
	ctx := namespaces.WithNamespace(context.Background(), "default")
	store := newTestSandboxStore(t, Tasks{
		Tasks: []Task{{TaskID: "task"}},
	})
	st := newTestSandboxedTask(store, &testTaskServiceClient{})
	st.sandboxHandle.controller = testSandboxController{updateErr: errors.New("controller update failed")}

	manager := &SandboxedTaskManager{
		tasks: ctrruntime.NewNSMap[*sandboxedTask](),
	}
	if err := manager.tasks.Add(ctx, st); err != nil {
		t.Fatal(err)
	}

	if _, err := manager.Delete(ctx, "task"); err == nil {
		t.Fatal("expected extension cleanup error")
	}
	if _, err := manager.tasks.Get(ctx, "task"); err != nil {
		t.Fatalf("expected task to remain registered for retry: %v", err)
	}

	tasks := getTestSandboxTasks(t, store.sb)
	if got := len(tasks.Tasks); got != 1 {
		t.Fatalf("expected sandbox store rollback to keep task extension, got %d", got)
	}
}

func TestTaskManagerDeleteSandboxedTaskRemoteNotFoundCleansUpLocalState(t *testing.T) {
	ctx := namespaces.WithNamespace(context.Background(), "default")
	store := newTestSandboxStore(t, Tasks{
		Tasks: []Task{{TaskID: "task"}},
	})
	st := newTestSandboxedTask(store, &testTaskServiceClient{
		deleteErr: errgrpc.ToGRPC(errdefs.ErrNotFound),
	})
	bundle, err := NewBundle(ctx, t.TempDir(), t.TempDir(), "task", nil)
	if err != nil {
		t.Fatal(err)
	}
	st.bundle = bundle
	closer := &testCloser{}
	st.closer = closer

	sandboxedTaskManager := &SandboxedTaskManager{
		tasks: ctrruntime.NewNSMap[*sandboxedTask](),
	}
	if err := sandboxedTaskManager.tasks.Add(ctx, st); err != nil {
		t.Fatal(err)
	}
	mounts := &testMountManager{}
	manager := &TaskManager{
		sandboxedTaskManager: sandboxedTaskManager,
		shimTaskManager:      &ShimTaskManager{},
		mounts:               mounts,
	}

	exit, err := manager.Delete(ctx, "task")
	if err != nil {
		t.Fatal(err)
	}
	if exit == nil {
		t.Fatal("expected synthetic exit after local cleanup")
	}
	if !closer.closed {
		t.Fatal("expected sandboxed task connection to be closed")
	}
	if got := mounts.deactivated; len(got) != 1 || got[0] != "task" {
		t.Fatalf("expected task mounts to be deactivated, got %v", got)
	}
	if _, err := sandboxedTaskManager.tasks.Get(ctx, "task"); !errdefs.IsNotFound(err) {
		t.Fatalf("expected task to be removed from sandboxed task manager, got %v", err)
	}

	tasks := getTestSandboxTasks(t, store.sb)
	if got := len(tasks.Tasks); got != 0 {
		t.Fatalf("expected task extension to be removed, got %d", got)
	}
}

func TestTaskManagerLoadExistingTasksKeepsNamespacedWorkDir(t *testing.T) {
	ctx := context.Background()
	nsCtx := namespaces.WithNamespace(ctx, "default")
	rootDir := t.TempDir()
	stateDir := t.TempDir()
	workDir := filepath.Join(rootDir, "default", "task")
	if err := os.MkdirAll(workDir, 0711); err != nil {
		t.Fatal(err)
	}

	sandboxedTaskManager := &SandboxedTaskManager{
		tasks: ctrruntime.NewNSMap[*sandboxedTask](),
	}
	if err := sandboxedTaskManager.tasks.Add(nsCtx, newTestSandboxedTask(newTestSandboxStore(t, Tasks{}), &testTaskServiceClient{})); err != nil {
		t.Fatal(err)
	}
	manager := &TaskManager{
		sandboxedTaskManager: sandboxedTaskManager,
	}

	if err := manager.loadExistingTasks(ctx, stateDir, rootDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(workDir); err != nil {
		t.Fatalf("expected existing task workdir to remain: %v", err)
	}
}

func TestSandboxClientUpdateTasksExtensionCleansUpLock(t *testing.T) {
	ctx := namespaces.WithNamespace(context.Background(), "default")
	store := newTestSandboxStore(t, Tasks{})
	manager := &SandboxedTaskManager{
		sandboxStore: store,
		sandboxControllers: map[string]sandbox.Controller{
			"shim": testSandboxController{},
		},
	}

	sb, err := manager.loadSandbox(ctx, "sandbox_cleanup")
	if err != nil {
		t.Fatal(err)
	}

	manager.lockMu.Lock()
	if got := len(manager.sandboxLocks); got != 0 {
		t.Fatalf("expected 0 locks, got %d", got)
	}
	manager.lockMu.Unlock()

	err = sb.UpdateTasksExtension(ctx, func(ts *Tasks) error {
		manager.lockMu.Lock()
		lock, ok := manager.sandboxLocks["sandbox_cleanup"]
		if !ok || lock.refs != 1 {
			t.Errorf("expected lock to exist with refs=1 during update")
		}
		manager.lockMu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	manager.lockMu.Lock()
	if got := len(manager.sandboxLocks); got != 0 {
		t.Fatalf("expected lock to be cleaned up after update, got %d lock(s)", got)
	}
	manager.lockMu.Unlock()
}

func TestSandboxClientUpdateTasksExtensionFailureCleansUpLock(t *testing.T) {
	ctx := namespaces.WithNamespace(context.Background(), "default")
	store := newTestSandboxStore(t, Tasks{})
	manager := &SandboxedTaskManager{
		sandboxStore: store,
		sandboxControllers: map[string]sandbox.Controller{
			"shim": testSandboxController{},
		},
	}

	sb, err := manager.loadSandbox(ctx, "sandbox_cleanup_failure")
	if err != nil {
		t.Fatal(err)
	}

	err = sb.UpdateTasksExtension(ctx, func(ts *Tasks) error {
		return errors.New("simulated failure")
	})
	if err == nil {
		t.Fatal("expected update to fail")
	}

	manager.lockMu.Lock()
	if got := len(manager.sandboxLocks); got != 0 {
		t.Fatalf("expected lock to be cleaned up after failure, got %d lock(s)", got)
	}
	manager.lockMu.Unlock()
}

func TestSandboxClientUpdateTasksExtensionConcurrency(t *testing.T) {
	ctx, cancel := context.WithTimeout(namespaces.WithNamespace(context.Background(), "default"), 2*time.Second)
	defer cancel()

	store := newTestSandboxStore(t, Tasks{})
	manager := &SandboxedTaskManager{
		sandboxStore: store,
		sandboxControllers: map[string]sandbox.Controller{
			"shim": testSandboxController{},
		},
	}

	sb, err := manager.loadSandbox(ctx, "sandbox_concurrent")
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	const numRoutines = 50

	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func(taskID string) {
			defer wg.Done()
			err := sb.UpdateTasksExtension(ctx, func(ts *Tasks) error {
				ts.addTask(Task{TaskID: taskID})
				return nil
			})
			if err != nil {
				t.Errorf("concurrent update failed: %v", err)
			}
		}(fmt.Sprintf("task-%d", i))
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		t.Fatal("concurrent updates did not finish within timeout, potential blocking or deadlock in sharded lock")
	case <-done:
	}

	tasks := getTestSandboxTasks(t, store.sb)
	if got := len(tasks.Tasks); got != numRoutines {
		t.Fatalf("expected %d tasks to be added, got %d", numRoutines, got)
	}

	manager.lockMu.Lock()
	if got := len(manager.sandboxLocks); got != 0 {
		t.Fatalf("expected all locks to be cleaned up after concurrent updates, got %d lock(s)", got)
	}
	manager.lockMu.Unlock()
}

func newTestSandboxedTask(store *testSandboxStore, client TaskServiceClient) *sandboxedTask {
	return &sandboxedTask{
		namespace: "default",
		sandboxHandle: &sandboxClient{
			id:         "sandbox",
			store:      store,
			controller: testSandboxController{},
			lock: func() func() {
				mu := &sync.Mutex{}
				mu.Lock()
				return mu.Unlock
			},
		},
		remoteTask: &remoteTask{
			id:     "task",
			client: client,
		},
	}
}

func newTestSandboxStore(t *testing.T, tasks Tasks) *testSandboxStore {
	t.Helper()

	sb := sandbox.Sandbox{
		ID:        "sandbox",
		Sandboxer: "shim",
	}
	if err := sb.AddExtension(TasksKey, &tasks); err != nil {
		t.Fatal(err)
	}
	return &testSandboxStore{sb: sb}
}

func getTestSandboxTasks(t *testing.T, sb sandbox.Sandbox) Tasks {
	t.Helper()

	var tasks Tasks
	if err := sb.GetExtension(TasksKey, &tasks); err != nil {
		t.Fatal(err)
	}
	return tasks
}

type testSandboxStore struct {
	mu                    sync.Mutex
	sb                    sandbox.Sandbox
	failOnCanceledContext bool
}

func (s *testSandboxStore) Create(context.Context, sandbox.Sandbox) (sandbox.Sandbox, error) {
	return sandbox.Sandbox{}, nil
}

func (s *testSandboxStore) Update(ctx context.Context, sb sandbox.Sandbox, _ ...string) (sandbox.Sandbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failOnCanceledContext {
		if err := ctx.Err(); err != nil {
			return sandbox.Sandbox{}, err
		}
	}
	s.sb = sb
	return s.sb, nil
}

func (s *testSandboxStore) Get(ctx context.Context, _ string) (sandbox.Sandbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failOnCanceledContext {
		if err := ctx.Err(); err != nil {
			return sandbox.Sandbox{}, err
		}
	}
	return s.sb, nil
}

func (s *testSandboxStore) List(context.Context, ...string) ([]sandbox.Sandbox, error) {
	return nil, nil
}

func (s *testSandboxStore) Delete(context.Context, string) error {
	return nil
}

type testSandboxController struct {
	updateErr error
}

func (testSandboxController) Create(context.Context, sandbox.Sandbox, ...sandbox.CreateOpt) error {
	return nil
}

func (testSandboxController) Start(context.Context, string) (sandbox.ControllerInstance, error) {
	return sandbox.ControllerInstance{}, nil
}

func (testSandboxController) Platform(context.Context, string) (v1.Platform, error) {
	return v1.Platform{}, nil
}

func (testSandboxController) Stop(context.Context, string, ...sandbox.StopOpt) error {
	return nil
}

func (testSandboxController) Wait(context.Context, string) (sandbox.ExitStatus, error) {
	return sandbox.ExitStatus{}, nil
}

func (testSandboxController) Status(context.Context, string, bool) (sandbox.ControllerStatus, error) {
	return sandbox.ControllerStatus{}, nil
}

func (testSandboxController) Shutdown(context.Context, string) error {
	return nil
}

func (testSandboxController) Metrics(context.Context, string) (*types.Metric, error) {
	return nil, nil
}

func (c testSandboxController) Update(context.Context, string, sandbox.Sandbox, ...string) error {
	return c.updateErr
}

type testTaskServiceClient struct {
	taskapi.UnimplementedTaskServer
	createHook func()
	createErr  error
	deleteErr  error
	execErr    error
}

func (c *testTaskServiceClient) State(context.Context, *taskapi.StateRequest) (*taskapi.StateResponse, error) {
	return &taskapi.StateResponse{}, nil
}

func (c *testTaskServiceClient) Create(context.Context, *taskapi.CreateTaskRequest) (*taskapi.CreateTaskResponse, error) {
	if c.createHook != nil {
		c.createHook()
	}
	if c.createErr != nil {
		return nil, c.createErr
	}
	return &taskapi.CreateTaskResponse{}, nil
}

func (c *testTaskServiceClient) Delete(context.Context, *taskapi.DeleteRequest) (*taskapi.DeleteResponse, error) {
	if c.deleteErr != nil {
		return nil, c.deleteErr
	}
	return &taskapi.DeleteResponse{}, nil
}

func (c *testTaskServiceClient) Exec(context.Context, *taskapi.ExecProcessRequest) (*emptypb.Empty, error) {
	if c.execErr != nil {
		return nil, c.execErr
	}
	return &emptypb.Empty{}, nil
}

func (c *testTaskServiceClient) Connect(context.Context, *taskapi.ConnectRequest) (*taskapi.ConnectResponse, error) {
	return &taskapi.ConnectResponse{Version: "3"}, nil
}

type testCloser struct {
	closed bool
}

func (c *testCloser) Close() error {
	c.closed = true
	return nil
}

type testMountManager struct {
	deactivated []string
}

func (m *testMountManager) Activate(context.Context, string, []ctrmount.Mount, ...ctrmount.ActivateOpt) (ctrmount.ActivationInfo, error) {
	return ctrmount.ActivationInfo{}, nil
}

func (m *testMountManager) Deactivate(_ context.Context, id string) error {
	m.deactivated = append(m.deactivated, id)
	return nil
}

func (m *testMountManager) Info(context.Context, string) (ctrmount.ActivationInfo, error) {
	return ctrmount.ActivationInfo{}, nil
}

func (m *testMountManager) Update(context.Context, ctrmount.ActivationInfo, ...string) (ctrmount.ActivationInfo, error) {
	return ctrmount.ActivationInfo{}, nil
}

func (m *testMountManager) List(context.Context, ...string) ([]ctrmount.ActivationInfo, error) {
	return nil, nil
}
