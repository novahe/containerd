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
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"

	bootapi "github.com/containerd/containerd/api/runtime/bootstrap/v1"
	"github.com/containerd/containerd/api/runtime/task/v3"
	"github.com/containerd/containerd/api/types"
	"github.com/containerd/errdefs"
	"github.com/containerd/errdefs/pkg/errgrpc"
	"github.com/containerd/log"
	"github.com/containerd/plugin"
	"github.com/containerd/ttrpc"
	"github.com/containerd/typeurl/v2"
	"github.com/opencontainers/runtime-spec/specs-go"

	"github.com/containerd/containerd/v2/core/runtime"
	"github.com/containerd/containerd/v2/core/sandbox"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/containerd/v2/pkg/protobuf"
	shimclient "github.com/containerd/containerd/v2/pkg/shim"
	"github.com/containerd/containerd/v2/pkg/timeout"
	"github.com/containerd/containerd/v2/plugins"
)

const (
	// TasksKey is the key used for storing tasks information in the sandbox extensions.
	TasksKey = "tasks"
)

func init() {
	typeurl.Register(&Tasks{}, "github.com/containerd/containerd", "Tasks")
}

var ErrCanNotHandle = errors.New("can not handle this task")

type sandboxLock struct {
	sync.Mutex
	refs int
}

type SandboxedTaskManager struct {
	sandboxStore       sandbox.Store
	sandboxControllers map[string]sandbox.Controller
	lockMu             sync.Mutex
	sandboxLocks       map[string]*sandboxLock
	tasks              *runtime.NSMap[*sandboxedTask]
}

func NewSandboxedTaskManager(ic *plugin.InitContext, sandboxStore sandbox.Store) (*SandboxedTaskManager, error) {
	scs, err := ic.GetByType(plugins.SandboxControllerPlugin)
	if err != nil && !errors.Is(err, plugin.ErrPluginNotFound) {
		return nil, err
	}
	sandboxControllers := make(map[string]sandbox.Controller)
	for name, p := range scs {
		sandboxControllers[name] = p.(sandbox.Controller)
	}

	return &SandboxedTaskManager{
		sandboxStore:       sandboxStore,
		sandboxControllers: sandboxControllers,
		sandboxLocks:       make(map[string]*sandboxLock),
		tasks:              runtime.NewNSMap[*sandboxedTask](),
	}, nil
}

func (s *SandboxedTaskManager) Create(ctx context.Context, taskID string, bundle *Bundle, opts runtime.CreateOpts) (runtime.Task, error) {
	if opts.SandboxID == "" {
		return nil, fmt.Errorf("no sandbox id specified for task %s", taskID)
	}
	sb, err := s.loadSandbox(ctx, opts.SandboxID)
	if err != nil {
		return nil, err
	}

	if _, err := namespaces.NamespaceRequired(ctx); err != nil {
		return nil, err
	}

	if err := os.WriteFile(filepath.Join(bundle.Path, "sandbox"), []byte(opts.SandboxID), 0600); err != nil {
		return nil, err
	}

	if opts.Address == "" {
		return nil, fmt.Errorf("address of task api should not be empty for sandboxed task")
	}

	protocol, address, ok := strings.Cut(opts.Address, "+")
	if !ok {
		return nil, fmt.Errorf("the scheme of sandbox address should be in the form of <protocol>+<unix|vsock|tcp>, i.e. ttrpc+unix or grpc+vsock")
	}
	params := &bootapi.BootstrapResult{
		Version:  int32(opts.Version),
		Protocol: protocol,
		Address:  address,
	}

	if err := writeBootstrapParams(filepath.Join(bundle.Path, "bootstrap.json"), params); err != nil {
		return nil, fmt.Errorf("failed to write bootstrap.json: %w", err)
	}

	sandboxedTask, err := newSandboxedTask(ctx, sb, taskID, bundle, params)
	if err != nil {
		return nil, fmt.Errorf("failed to new sandboxed task: %w", err)
	}
	if err := sandboxedTask.Create(ctx, bundle.Path, opts); err != nil {
		sandboxedTask.close(ctx)
		return nil, fmt.Errorf("failed to create sandboxed task: %w", err)
	}
	if err := s.tasks.Add(ctx, sandboxedTask); err != nil {
		cleanupCtx, cancel := timeout.WithContext(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		if _, deleteErr := sandboxedTask.client.Delete(cleanupCtx, &task.DeleteRequest{ID: taskID}); deleteErr != nil {
			log.G(ctx).WithField("id", taskID).WithError(deleteErr).Warn("failed to rollback sandboxed task after registration failure")
		}
		if removeErr := sandboxedTask.sandboxHandle.UpdateTasksExtension(cleanupCtx, func(ts *Tasks) error {
			ts.removeTask(taskID)
			return nil
		}); removeErr != nil {
			log.G(ctx).WithField("id", taskID).WithError(removeErr).Warn("failed to rollback sandbox task extension after registration failure")
		}
		sandboxedTask.close(ctx)
		return nil, err
	}
	return sandboxedTask, nil
}

func (s *SandboxedTaskManager) Load(ctx context.Context, sandboxID string, bundle *Bundle) error {
	sb, err := s.loadSandbox(ctx, sandboxID)
	if err != nil {
		return fmt.Errorf("failed to get sandbox %s: %w", sandboxID, err)
	}

	params, err := readBootstrapParams(filepath.Join(bundle.Path, "bootstrap.json"))
	if err != nil {
		return fmt.Errorf("failed to restore connection parameter from %s: %w", bundle.Path, err)
	}

	sandboxedTask, err := newSandboxedTask(ctx, sb, bundle.ID, bundle, params)
	if err != nil {
		return fmt.Errorf("failed to new sandboxed task: %w", err)
	}
	if err := s.tasks.Add(ctx, sandboxedTask); err != nil {
		sandboxedTask.close(ctx)
		return err
	}
	return nil
}

func (s *SandboxedTaskManager) Get(ctx context.Context, id string) (runtime.Task, error) {
	return s.tasks.Get(ctx, id)
}

func (s *SandboxedTaskManager) GetAll(ctx context.Context, all bool) ([]runtime.Task, error) {
	ts, err := s.tasks.GetAll(ctx, all)
	if err != nil {
		return nil, err
	}
	tasks := make([]runtime.Task, 0, len(ts))
	for _, t := range ts {
		tasks = append(tasks, t)
	}
	return tasks, nil
}

func (s *SandboxedTaskManager) Delete(ctx context.Context, taskID string) (*runtime.Exit, error) {
	st, err := s.tasks.Get(ctx, taskID)
	if err != nil {
		return nil, err
	}

	resp, taskErr := st.client.Delete(ctx, &task.DeleteRequest{
		ID: taskID,
	})
	if taskErr != nil {
		log.G(ctx).WithField("id", taskID).WithError(taskErr).Debug("failed to delete task")
		if !errors.Is(taskErr, ttrpc.ErrClosed) {
			taskErr = errgrpc.ToNative(taskErr)
			if !errdefs.IsNotFound(taskErr) {
				return nil, taskErr
			}
		}
	}

	cleanupCtx, cancel := timeout.WithContext(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	if err := st.sandboxHandle.UpdateTasksExtension(cleanupCtx, func(ts *Tasks) error {
		ts.removeTask(taskID)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to remove task from sandbox extension: %w", err)
	}

	if err := st.bundle.Delete(); err != nil {
		log.G(ctx).WithField("id", taskID).WithError(err).Error("failed to delete bundle")
	}

	s.tasks.Delete(ctx, taskID)
	st.close(ctx)

	if taskErr != nil {
		return &runtime.Exit{}, nil
	}
	return &runtime.Exit{
		Status:    resp.ExitStatus,
		Timestamp: protobuf.FromTimestamp(resp.ExitedAt),
		Pid:       resp.Pid,
	}, nil
}

func (s *SandboxedTaskManager) lockSandbox(sandboxID string) func() {
	s.lockMu.Lock()
	if s.sandboxLocks == nil {
		s.sandboxLocks = make(map[string]*sandboxLock)
	}
	mu, ok := s.sandboxLocks[sandboxID]
	if !ok {
		mu = &sandboxLock{}
		s.sandboxLocks[sandboxID] = mu
	}
	mu.refs++
	s.lockMu.Unlock()

	mu.Lock()

	return func() {
		mu.Unlock()
		s.lockMu.Lock()
		defer s.lockMu.Unlock()
		mu.refs--
		if mu.refs == 0 {
			delete(s.sandboxLocks, sandboxID)
		}
	}
}

func (s *SandboxedTaskManager) loadSandbox(ctx context.Context, sandboxID string) (*sandboxClient, error) {
	sb, err := s.sandboxStore.Get(ctx, sandboxID)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil, ErrCanNotHandle
		}
		return nil, fmt.Errorf("failed to get sandbox %s: %w", sandboxID, err)
	}
	if sb.Sandboxer == "podsandbox" || sb.Sandboxer == "" {
		return nil, ErrCanNotHandle
	}
	sbController, ok := s.sandboxControllers[sb.Sandboxer]
	if !ok {
		return nil, fmt.Errorf("can not find sandbox controller by %s", sb.Sandboxer)
	}

	return &sandboxClient{
		id:         sandboxID,
		store:      s.sandboxStore,
		controller: sbController,
		lock: func() func() {
			return s.lockSandbox(sandboxID)
		},
	}, nil
}

func newSandboxedTask(
	ctx context.Context,
	sandboxHandle *sandboxClient,
	taskID string,
	bundle *Bundle,
	params *bootapi.BootstrapResult,
) (*sandboxedTask, error) {
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return nil, err
	}

	conn, err := makeConnection(ctx, taskID, params, func() {}, shimclient.AnonReconnectDialer)
	if err != nil {
		return nil, fmt.Errorf("can not connect %v: %w", params, err)
	}

	taskClient, err := NewTaskClient(conn, int(params.Version))
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &sandboxedTask{
		namespace:     ns,
		sandboxHandle: sandboxHandle,
		remoteTask: &remoteTask{
			id:     taskID,
			client: taskClient,
		},
		bundle: bundle,
		closer: conn,
	}, nil
}

type sandboxedTask struct {
	namespace     string
	sandboxHandle *sandboxClient
	bundle        *Bundle
	closer        io.Closer
	*remoteTask
}

type sandboxClient struct {
	id         string
	store      sandbox.Store
	controller sandbox.Controller
	lock       func() func()
}

func (s *sandboxedTask) ID() string {
	return s.remoteTask.id
}

func (s *sandboxedTask) Namespace() string {
	return s.namespace
}

func (s *sandboxedTask) close(ctx context.Context) {
	if s.closer == nil {
		return
	}
	if err := s.closer.Close(); err != nil {
		log.G(ctx).WithError(err).WithField("id", s.ID()).Warn("failed to close sandboxed task connection")
	}
}

func (s *sandboxedTask) Create(ctx context.Context, bundle string, opts runtime.CreateOpts) error {
	var previous Task
	var replaced bool
	if err := s.sandboxHandle.UpdateTasksExtension(ctx, func(ts *Tasks) error {
		var spec oci.Spec
		if err := typeurl.UnmarshalTo(opts.Spec, &spec); err != nil {
			return err
		}
		rootfs := make([]*types.Mount, 0, len(opts.Rootfs))
		for _, m := range opts.Rootfs {
			rootfs = append(rootfs, &types.Mount{
				Type:    m.Type,
				Source:  m.Source,
				Target:  m.Target,
				Options: m.Options,
			})
		}
		previous, replaced = ts.addTask(Task{
			TaskID: s.id,
			Spec:   &spec,
			Rootfs: rootfs,
			Stdin:  opts.IO.Stdin,
			Stdout: opts.IO.Stdout,
			Stderr: opts.IO.Stderr,
		})
		return nil
	}); err != nil {
		return err
	}

	if err := s.remoteTask.Create(ctx, bundle, opts); err != nil {
		cleanupCtx, cancel := timeout.WithContext(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		if e := s.sandboxHandle.UpdateTasksExtension(cleanupCtx, func(ts *Tasks) error {
			if replaced {
				ts.addTask(previous)
			} else {
				ts.removeTask(s.id)
			}
			return nil
		}); e != nil {
			log.G(ctx).Warnf("failed to rollback task extension %s in sandbox %s: %v", s.ID(), s.sandboxHandle.id, e)
		}
		return err
	}
	return nil
}

func (s *sandboxedTask) Exec(ctx context.Context, id string, opts runtime.ExecOpts) (runtime.ExecProcess, error) {
	var previous Process
	var replaced bool
	if err := s.sandboxHandle.UpdateTasksExtension(ctx, func(ts *Tasks) error {
		return ts.updateTask(s.id, func(t *Task) error {
			var spec specs.Process
			if err := typeurl.UnmarshalTo(opts.Spec, &spec); err != nil {
				return err
			}
			previous, replaced = t.addProcess(Process{
				ExecID: id,
				Spec:   &spec,
				Stdin:  opts.IO.Stdin,
				Stdout: opts.IO.Stdout,
				Stderr: opts.IO.Stderr,
			})
			return nil
		})
	}); err != nil {
		return nil, err
	}

	p, err := s.remoteTask.Exec(ctx, id, opts)
	if err != nil {
		cleanupCtx, cancel := timeout.WithContext(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		if removeErr := s.rollbackProcess(cleanupCtx, id, previous, replaced); removeErr != nil {
			log.G(ctx).Warnf("failed to remove exec task resource %s %s in sandbox %s: %v", s.ID(), id, s.sandboxHandle.id, removeErr)
		}
		return nil, err
	}
	return &sandboxedProcess{
		ExecProcess: p,
		task:        s,
	}, nil
}

func (s *sandboxedTask) Process(ctx context.Context, id string) (runtime.ExecProcess, error) {
	p, err := s.remoteTask.Process(ctx, id)
	if err != nil {
		return nil, err
	}
	return &sandboxedProcess{
		ExecProcess: p,
		task:        s,
	}, nil
}

func (s *sandboxedTask) removeProcess(ctx context.Context, id string) error {
	return s.rollbackProcess(ctx, id, Process{}, false)
}

func (s *sandboxedTask) rollbackProcess(ctx context.Context, id string, previous Process, replaced bool) error {
	return s.sandboxHandle.UpdateTasksExtension(ctx, func(ts *Tasks) error {
		return ts.updateTask(s.id, func(t *Task) error {
			if replaced {
				t.addProcess(previous)
			} else {
				t.removeProcess(id)
			}
			return nil
		})
	})
}

type sandboxedProcess struct {
	runtime.ExecProcess
	task *sandboxedTask
}

func (p *sandboxedProcess) Delete(ctx context.Context) (*runtime.Exit, error) {
	exit, err := p.ExecProcess.Delete(ctx)
	if err != nil {
		if errdefs.IsNotFound(err) {
			cleanupCtx, cancel := timeout.WithContext(context.WithoutCancel(ctx), cleanupTimeout)
			defer cancel()
			if removeErr := p.task.removeProcess(cleanupCtx, p.ID()); removeErr != nil {
				log.G(ctx).WithField("id", p.ID()).WithError(removeErr).Warn("failed to remove missing exec process from sandbox extension")
			}
		}
		return nil, err
	}
	cleanupCtx, cancel := timeout.WithContext(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	if err := p.task.removeProcess(cleanupCtx, p.ID()); err != nil {
		return nil, err
	}
	return exit, nil
}

func (s *sandboxClient) UpdateTasksExtension(ctx context.Context, update func(ts *Tasks) error) error {
	unlock := s.lock()
	defer unlock()

	var tasks Tasks
	sb, err := s.store.Get(ctx, s.id)
	if err != nil {
		return err
	}
	err = sb.GetExtension(TasksKey, &tasks)
	if err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	previous := sb
	previous.Extensions = maps.Clone(sb.Extensions)
	if err = update(&tasks); err != nil {
		return err
	}
	if err = sb.AddExtension(TasksKey, &tasks); err != nil {
		return err
	}
	if _, err := s.store.Update(ctx, sb, "extensions."+TasksKey); err != nil {
		return err
	}
	if err := s.controller.Update(ctx, sb.ID, sb, "extensions."+TasksKey); err != nil {
		if _, err := s.store.Update(ctx, previous, "extensions."+TasksKey); err != nil {
			log.G(ctx).Warnf("failed to rollback when update tasks of sandbox %s extensions: %v", s.id, err)
		}
		return err
	}
	return nil
}

type Tasks struct {
	Tasks []Task `json:"tasks,omitempty"`
}

func (ts *Tasks) addTask(t Task) (Task, bool) {
	if idx := ts.findTaskByID(t.TaskID); idx >= 0 {
		previous := ts.Tasks[idx]
		ts.Tasks[idx] = t
		return previous, true
	}
	ts.Tasks = append(ts.Tasks, t)
	return Task{}, false
}

func (ts *Tasks) removeTask(taskID string) {
	idx := ts.findTaskByID(taskID)
	if idx < 0 {
		return
	}
	ts.Tasks = append(ts.Tasks[:idx], ts.Tasks[idx+1:]...)
}

func (ts *Tasks) updateTask(taskID string, update func(*Task) error) error {
	idx := ts.findTaskByID(taskID)
	if idx < 0 {
		return errdefs.ErrNotFound
	}
	return update(&ts.Tasks[idx])
}

func (ts *Tasks) findTaskByID(taskID string) int {
	for idx, t := range ts.Tasks {
		if t.TaskID == taskID {
			return idx
		}
	}
	return -1
}

type Task struct {
	TaskID    string         `json:"task_id,omitempty"`
	Spec      *oci.Spec      `json:"spec,omitempty"`
	Rootfs    []*types.Mount `json:"rootfs,omitempty"`
	Stdin     string         `json:"stdin,omitempty"`
	Stdout    string         `json:"stdout,omitempty"`
	Stderr    string         `json:"stderr,omitempty"`
	Processes []Process      `json:"processes,omitempty"`
}

func (t *Task) addProcess(process Process) (Process, bool) {
	if idx := t.findProcessByID(process.ExecID); idx >= 0 {
		previous := t.Processes[idx]
		t.Processes[idx] = process
		return previous, true
	}
	t.Processes = append(t.Processes, process)
	return Process{}, false
}

func (t *Task) removeProcess(execID string) {
	idx := t.findProcessByID(execID)
	if idx < 0 {
		return
	}
	t.Processes = append(t.Processes[:idx], t.Processes[idx+1:]...)
}

func (t *Task) findProcessByID(execID string) int {
	for idx, p := range t.Processes {
		if p.ExecID == execID {
			return idx
		}
	}
	return -1
}

type Process struct {
	ExecID string         `json:"exec_id,omitempty"`
	Spec   *specs.Process `json:"spec,omitempty"`
	Stdin  string         `json:"stdin,omitempty"`
	Stdout string         `json:"stdout,omitempty"`
	Stderr string         `json:"stderr,omitempty"`
}
