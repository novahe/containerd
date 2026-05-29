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

	"github.com/containerd/errdefs"
	"github.com/containerd/errdefs/pkg/errgrpc"
	"github.com/containerd/log"
	"github.com/containerd/ttrpc"

	"github.com/containerd/containerd/api/runtime/task/v3"
	"github.com/containerd/containerd/v2/core/runtime"
	"github.com/containerd/containerd/v2/pkg/protobuf"
	"github.com/containerd/containerd/v2/pkg/timeout"
)

type ShimTaskManager struct {
	shimManager *ShimManager
}

func (m *ShimTaskManager) Create(ctx context.Context, taskID string, bundle *Bundle, opts runtime.CreateOpts) (runtime.Task, error) {
	shim, err := m.shimManager.Start(ctx, taskID, bundle, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to start shim: %w", err)
	}

	shimTask, err := newShimTask(shim)
	if err != nil {
		return nil, err
	}

	t, err := func() (runtime.Task, error) {
		if err := shimTask.Create(ctx, bundle.Path, opts); err == nil || !errdefs.IsNotImplemented(err) {
			return shimTask, err
		}

		downgrader, ok := shim.(clientVersionDowngrader)
		if ok {
			if derr := downgrader.Downgrade(); derr == nil {
				log.G(ctx).WithError(err).WithField("id", taskID).
					Warning("failed to call task.Create, downgrading client API version to try again")

				shimTask, err = newShimTask(shim)
				if err != nil {
					return nil, fmt.Errorf("failed to create shim task after downgrading: %w", err)
				}
				return shimTask, shimTask.Create(ctx, bundle.Path, opts)
			}
		}
		return nil, err
	}()
	if err != nil {
		m.shimManager.shims.Delete(ctx, taskID)

		dctx, cancel := timeout.WithContext(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()

		sandboxed := opts.SandboxID != ""
		_, errShim := shimTask.delete(dctx, sandboxed, func(context.Context, string) {})
		if errShim != nil {
			if errdefs.IsDeadlineExceeded(errShim) {
				dctx, cancel = timeout.WithContext(context.WithoutCancel(ctx), cleanupTimeout)
				defer cancel()
			}

			shimTask.Shutdown(dctx)
			shimTask.Close()
		}

		return nil, fmt.Errorf("failed to create shim task: %w", err)
	}

	return t, nil
}

func (m *ShimTaskManager) Load(ctx context.Context, bundle *Bundle) error {
	return m.shimManager.loadShim(ctx, bundle)
}

func (m *ShimTaskManager) Get(ctx context.Context, id string) (runtime.Task, error) {
	shim, err := m.shimManager.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return newShimTask(shim)
}

func (m *ShimTaskManager) GetAll(ctx context.Context, all bool) ([]runtime.Task, error) {
	shims, err := m.shimManager.shims.GetAll(ctx, all)
	if err != nil {
		return nil, err
	}
	out := make([]runtime.Task, 0, len(shims))
	for _, s := range shims {
		t, err := newShimTask(s)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, nil
}

func (m *ShimTaskManager) Delete(ctx context.Context, taskID string) (*runtime.Exit, error) {
	shim, err := m.shimManager.shims.Get(ctx, taskID)
	if err != nil {
		return nil, err
	}

	container, err := m.shimManager.containers.Get(ctx, taskID)
	if err != nil {
		return nil, err
	}

	shimTask, err := newShimTask(shim)
	if err != nil {
		return nil, err
	}

	sandboxed := container.SandboxID != ""
	exit, err := shimTask.delete(ctx, sandboxed, func(ctx context.Context, id string) {
		m.shimManager.shims.Delete(ctx, id)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to delete task: %w", err)
	}

	return exit, nil
}

var _ runtime.Task = &shimTask{}

// shimTask wraps shim process and adds task service client for compatibility with existing shim manager.
type shimTask struct {
	ShimInstance
	*remoteTask
}

func newShimTask(shim ShimInstance) (*shimTask, error) {
	_, version := shim.Endpoint()
	taskClient, err := NewTaskClient(shim.Client(), version)
	if err != nil {
		return nil, err
	}

	return &shimTask{
		ShimInstance: shim,
		remoteTask: &remoteTask{
			id:     shim.ID(),
			client: taskClient,
		},
	}, nil
}

func (s *shimTask) Shutdown(ctx context.Context) error {
	_, err := s.remoteTask.client.Shutdown(ctx, &task.ShutdownRequest{
		ID: s.ID(),
	})
	if err != nil && !errors.Is(err, ttrpc.ErrClosed) {
		return errgrpc.ToNative(err)
	}
	return nil
}

func (s *shimTask) waitShutdown(ctx context.Context) error {
	ctx, cancel := timeout.WithContext(ctx, shutdownTimeout)
	defer cancel()
	return s.Shutdown(ctx)
}

func (s *shimTask) delete(ctx context.Context, sandboxed bool, removeTask func(ctx context.Context, id string)) (*runtime.Exit, error) {
	response, shimErr := s.remoteTask.client.Delete(ctx, &task.DeleteRequest{
		ID: s.ID(),
	})
	if shimErr != nil {
		log.G(ctx).WithField("id", s.ID()).WithError(shimErr).Error("failed to delete task")
		if !errors.Is(shimErr, ttrpc.ErrClosed) {
			shimErr = errgrpc.ToNative(shimErr)
			if !errdefs.IsNotFound(shimErr) {
				return nil, shimErr
			}
		}
	}

	if shimErr == nil {
		removeTask(ctx, s.ID())
	}

	const supportSandboxAPIVersion = 3
	if _, apiVer := s.ShimInstance.Endpoint(); apiVer < supportSandboxAPIVersion {
		sandboxed = false
	}

	// Do not shutdown a sandbox shim while other containers may still run in it.
	if !sandboxed {
		if err := s.waitShutdown(ctx); err != nil {
			log.G(ctx).WithField("id", s.ID()).WithError(err).Error("failed to shutdown shim task and the shim might be leaked")
		}
	}

	if err := s.ShimInstance.Delete(ctx); err != nil {
		log.G(ctx).WithField("id", s.ID()).WithError(err).Error("failed to delete shim")
	}

	removeTask(ctx, s.ID())

	if shimErr != nil {
		return nil, shimErr
	}

	return &runtime.Exit{
		Status:    response.ExitStatus,
		Timestamp: protobuf.FromTimestamp(response.ExitedAt),
		Pid:       response.Pid,
	}, nil
}
