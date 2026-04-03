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

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/errdefs"
	"github.com/containerd/log"
)

var errSkipShimLoad = errors.New("skip shim load")

// LoadExistingShims loads existing shims from the path specified by stateDir
func (m *ShimManager) LoadExistingShims(ctx context.Context, stateDir string, rootDir string) error {
	if err := TraversBundles(ctx, stateDir, func(ctx context.Context, bundle *Bundle) error {
		if err := m.loadShim(ctx, bundle, func(instance ShimInstance) error {
			taskShim, err := newLoadedShimTask(ctx, instance)
			if err != nil {
				return err
			}

			_, sandboxErr := m.sandboxStore.Get(ctx, instance.ID())
			pids, pidErr := taskShim.Pids(ctx)
			if errors.Is(sandboxErr, errdefs.ErrNotFound) &&
				(len(pids) == 0 || errors.Is(pidErr, errdefs.ErrNotFound)) {
				log.G(ctx).WithField("id", taskShim.ID()).Info("cleaning leaked shim process")
				return cleanupLeakedTaskShim(ctx, taskShim)
			}

			return nil
		}); err != nil {
			log.G(ctx).WithError(err).Errorf("failed to load shim %s", bundle.Path)
			bundle.Delete()
		}
		return nil
	}); err != nil {
		return err
	}
	TraversWorkDirs(ctx, rootDir, func(ctx context.Context, ns, dir string) error {
		if _, err := m.shims.Get(ctx, dir); err != nil {
			path := filepath.Join(rootDir, ns, dir)
			if err := os.RemoveAll(path); err != nil {
				log.G(ctx).WithError(err).Errorf("cleanup working dir %s", path)
			}
		}
		return nil
	})
	return nil
}

func (m *ShimManager) loadShim(ctx context.Context, bundle *Bundle, check func(ShimInstance) error) error {
	var (
		runtime string
		id      = bundle.ID
	)

	if data, err := os.ReadFile(filepath.Join(bundle.Path, "shim-binary-path")); err == nil {
		runtime = string(data)
	} else if err != nil && !os.IsNotExist(err) {
		log.G(ctx).WithError(err).Error("failed to read `runtime` path from bundle")
	}

	if runtime == "" {
		container, err := m.containers.Get(ctx, id)
		if err != nil {
			log.G(ctx).WithError(err).Errorf("loading container %s", id)
			if err := mount.UnmountRecursive(filepath.Join(bundle.Path, "rootfs"), 0); err != nil {
				log.G(ctx).WithError(err).Errorf("failed to unmount of rootfs %s", id)
			}
			return err
		}
		runtime = container.Runtime.Name
	}

	runtime, err := m.resolveRuntimePath(runtime)
	if err != nil {
		bundle.Delete()
		return fmt.Errorf("failed to resolve runtime path: %w", err)
	}

	binaryCall := shimBinary(bundle,
		shimBinaryConfig{
			runtime:      runtime,
			address:      m.containerdAddress,
			ttrpcAddress: m.containerdTTRPCAddress,
			env:          m.env,
		})
	shim, err := loadShim(ctx, bundle, func() {
		log.G(ctx).WithField("id", id).Info("shim disconnected")
		cleanupAfterDeadShim(context.WithoutCancel(ctx), id, m.shims, m.events, binaryCall)
		m.shims.Delete(ctx, id)
	})
	if err != nil {
		cleanupAfterDeadShim(ctx, id, m.shims, m.events, binaryCall)
		return fmt.Errorf("unable to load shim %q: %w", id, err)
	}

	if err := check(shim); err != nil {
		if errors.Is(err, errSkipShimLoad) {
			return nil
		}
		cleanupAfterDeadShim(ctx, id, m.shims, m.events, binaryCall)
		return fmt.Errorf("unable to verify shim %q: %w", id, err)
	}

	if err := m.shims.Add(ctx, shim); err != nil {
		return err
	}
	return nil
}
