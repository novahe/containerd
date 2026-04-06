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
	"testing"
	"time"

	taskapi "github.com/containerd/containerd/api/runtime/task/v3"
	"github.com/containerd/containerd/v2/core/runtime"
	"google.golang.org/protobuf/types/known/emptypb"
)

type testTaskServiceClient struct {
	stateFn func(context.Context, *taskapi.StateRequest) (*taskapi.StateResponse, error)
	statsFn func(context.Context, *taskapi.StatsRequest) (*taskapi.StatsResponse, error)
}

func (c *testTaskServiceClient) State(ctx context.Context, req *taskapi.StateRequest) (*taskapi.StateResponse, error) {
	if c.stateFn != nil {
		return c.stateFn(ctx, req)
	}
	panic("unexpected call to State")
}

func (c *testTaskServiceClient) Create(context.Context, *taskapi.CreateTaskRequest) (*taskapi.CreateTaskResponse, error) {
	panic("unexpected call to Create")
}

func (c *testTaskServiceClient) Start(context.Context, *taskapi.StartRequest) (*taskapi.StartResponse, error) {
	panic("unexpected call to Start")
}

func (c *testTaskServiceClient) Delete(context.Context, *taskapi.DeleteRequest) (*taskapi.DeleteResponse, error) {
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

func (c *testTaskServiceClient) Exec(context.Context, *taskapi.ExecProcessRequest) (*emptypb.Empty, error) {
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

func (c *testTaskServiceClient) Stats(ctx context.Context, req *taskapi.StatsRequest) (*taskapi.StatsResponse, error) {
	if c.statsFn != nil {
		return c.statsFn(ctx, req)
	}
	panic("unexpected call to Stats")
}

func (c *testTaskServiceClient) Connect(context.Context, *taskapi.ConnectRequest) (*taskapi.ConnectResponse, error) {
	panic("unexpected call to Connect")
}

func (c *testTaskServiceClient) Shutdown(context.Context, *taskapi.ShutdownRequest) (*emptypb.Empty, error) {
	panic("unexpected call to Shutdown")
}

type testExecProcess struct {
	id      string
	stateFn func(context.Context) (runtime.State, error)
}

func (p *testExecProcess) ID() string {
	return p.id
}

func (p *testExecProcess) State(ctx context.Context) (runtime.State, error) {
	if p.stateFn != nil {
		return p.stateFn(ctx)
	}
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

func (p *testExecProcess) Delete(context.Context) (*runtime.Exit, error) {
	panic("unexpected call to Delete")
}

func TestSandboxedTaskStateAppliesTimeout(t *testing.T) {
	st := &sandboxedTask{
		remoteTask: &remoteTask{
			id: "task-state-timeout",
			client: &testTaskServiceClient{
				stateFn: func(ctx context.Context, req *taskapi.StateRequest) (*taskapi.StateResponse, error) {
					if req.ID != "task-state-timeout" {
						t.Fatalf("unexpected state request id %q", req.ID)
					}
					deadline, ok := ctx.Deadline()
					if !ok {
						t.Fatal("expected state call deadline to be set")
					}
					remaining := time.Until(deadline)
					if remaining <= 0 || remaining > taskStateTimeout {
						t.Fatalf("expected state timeout within (0, %s], got %s", taskStateTimeout, remaining)
					}
					return &taskapi.StateResponse{}, nil
				},
			},
		},
	}

	if _, err := st.State(context.Background()); err != nil {
		t.Fatalf("State returned error: %v", err)
	}
}

func TestSandboxedTaskStatsAppliesTimeout(t *testing.T) {
	st := &sandboxedTask{
		remoteTask: &remoteTask{
			id: "task-stats-timeout",
			client: &testTaskServiceClient{
				statsFn: func(ctx context.Context, req *taskapi.StatsRequest) (*taskapi.StatsResponse, error) {
					if req.ID != "task-stats-timeout" {
						t.Fatalf("unexpected stats request id %q", req.ID)
					}
					deadline, ok := ctx.Deadline()
					if !ok {
						t.Fatal("expected stats call deadline to be set")
					}
					remaining := time.Until(deadline)
					if remaining <= 0 || remaining > taskStatsTimeout {
						t.Fatalf("expected stats timeout within (0, %s], got %s", taskStatsTimeout, remaining)
					}
					return &taskapi.StatsResponse{}, nil
				},
			},
		},
	}

	if _, err := st.Stats(context.Background()); err != nil {
		t.Fatalf("Stats returned error: %v", err)
	}
}

func TestSandboxedTaskProcessHonorsShorterCallerDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	st := &sandboxedTask{
		remoteTask: &remoteTask{
			id: "task-process-state",
			client: &testTaskServiceClient{
				stateFn: func(ctx context.Context, req *taskapi.StateRequest) (*taskapi.StateResponse, error) {
					if req.ID != "task-process-state" || req.ExecID != "exec-1" {
						t.Fatalf("unexpected state request id=%q execID=%q", req.ID, req.ExecID)
					}
					deadline, ok := ctx.Deadline()
					if !ok {
						t.Fatal("expected process state call deadline to be set")
					}
					remaining := time.Until(deadline)
					if remaining <= 0 || remaining > 100*time.Millisecond {
						t.Fatalf("expected shorter caller deadline to be preserved, got %s", remaining)
					}
					return &taskapi.StateResponse{}, nil
				},
			},
		},
	}

	if _, err := st.Process(ctx, "exec-1"); err != nil {
		t.Fatalf("Process returned error: %v", err)
	}
}

func TestSandboxedProcessStateAppliesTimeout(t *testing.T) {
	p := &sandboxedProcess{
		ExecProcess: &testExecProcess{
			id: "exec-timeout",
			stateFn: func(ctx context.Context) (runtime.State, error) {
				deadline, ok := ctx.Deadline()
				if !ok {
					t.Fatal("expected sandboxed process state call deadline to be set")
				}
				remaining := time.Until(deadline)
				if remaining <= 0 || remaining > taskStateTimeout {
					t.Fatalf("expected sandboxed process timeout within (0, %s], got %s", taskStateTimeout, remaining)
				}
				return runtime.State{}, nil
			},
		},
	}

	if _, err := p.State(context.Background()); err != nil {
		t.Fatalf("State returned error: %v", err)
	}
}
