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
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	apitypes "github.com/containerd/containerd/api/types"
	"github.com/containerd/errdefs"
	"github.com/containerd/log"
	"github.com/containerd/plugin"
	"github.com/containerd/plugin/registry"
	"github.com/containerd/typeurl/v2"

	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/core/events/exchange"
	"github.com/containerd/containerd/v2/core/metadata"
	"github.com/containerd/containerd/v2/core/runtime"
	"github.com/containerd/containerd/v2/core/sandbox"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/protobuf/proto"
	shimbinary "github.com/containerd/containerd/v2/pkg/shim"
	"github.com/containerd/containerd/v2/pkg/timeout"
	"github.com/containerd/containerd/v2/plugins"
	"github.com/containerd/containerd/v2/version"
)

const allowedMounts = "io.containerd.runtime.v2.allowed_mounts"

// ShimConfig for the shim
type ShimConfig struct {
	// Env is environment variables added to shim processes
	Env []string `toml:"env"`
}

func init() {
	registry.Register(&plugin.Registration{
		Type: plugins.ShimPlugin,
		ID:   "manager",
		Requires: []plugin.Type{
			plugins.EventPlugin,
			plugins.MetadataPlugin,
		},
		Config: &ShimConfig{},
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			config := ic.Config.(*ShimConfig)

			m, err := ic.GetSingle(plugins.MetadataPlugin)
			if err != nil {
				return nil, err
			}
			ep, err := ic.GetByID(plugins.EventPlugin, "exchange")
			if err != nil {
				return nil, err
			}
			events := ep.(*exchange.Exchange)
			cs := metadata.NewContainerStore(m.(*metadata.DB))
			ss := metadata.NewSandboxStore(m.(*metadata.DB))
			return NewShimManager(&ManagerConfig{
				Address:      ic.Properties[plugins.PropertyGRPCAddress],
				TTRPCAddress: ic.Properties[plugins.PropertyTTRPCAddress],
				Events:       events,
				Store:        cs,
				ShimEnv:      config.Env,
				SandboxStore: ss,
			})
		},
		ConfigMigration: func(ctx context.Context, configVersion int, pluginConfigs map[string]interface{}) error {
			if configVersion >= version.ConfigVersion {
				return nil
			}
			const originalPluginName = string(plugins.RuntimePluginV2) + ".task"
			original, ok := pluginConfigs[originalPluginName]
			if !ok {
				return nil
			}
			src := original.(map[string]interface{})
			dest := map[string]interface{}{}

			if v, ok := src["sched_core"]; ok {
				if schedCore, ok := v.(bool); schedCore {
					dest["env"] = []string{"SCHED_CORE=1"}
				} else if !ok {
					log.G(ctx).Warnf("skipping migration for non-boolean 'sched_core' value %v", v)
				}

				delete(src, "sched_core")
			}

			const newPluginName = string(plugins.ShimPlugin) + ".manager"
			pluginConfigs[originalPluginName] = src
			pluginConfigs[newPluginName] = dest
			return nil
		},
	})
}

type ManagerConfig struct {
	Store        containers.Store
	Events       *exchange.Exchange
	Address      string
	TTRPCAddress string
	SandboxStore sandbox.Store
	ShimEnv      []string
}

func NewShimManager(config *ManagerConfig) (*ShimManager, error) {
	m := &ShimManager{
		containerdAddress:      config.Address,
		containerdTTRPCAddress: config.TTRPCAddress,
		shims:                  runtime.NewNSMap[ShimInstance](),
		events:                 config.Events,
		containers:             config.Store,
		env:                    config.ShimEnv,
		sandboxStore:           config.SandboxStore,
	}

	return m, nil
}

type ShimManager struct {
	containerdAddress      string
	containerdTTRPCAddress string
	env                    []string
	shims                  *runtime.NSMap[ShimInstance]
	events                 *exchange.Exchange
	containers             containers.Store
	runtimePaths           sync.Map
	sandboxStore           sandbox.Store
	shimInfos              sync.Map
}

func (m *ShimManager) ID() string {
	return plugins.ShimPlugin.String() + ".manager"
}

func (m *ShimManager) Start(ctx context.Context, id string, bundle *Bundle, opts runtime.CreateOpts) (_ ShimInstance, retErr error) {
	shouldInvokeShimBinary := false

	var params shimbinary.BootstrapParams
	if opts.SandboxID != "" {
		_, sbErr := m.sandboxStore.Get(ctx, opts.SandboxID)
		if sbErr != nil {
			if !errors.Is(sbErr, errdefs.ErrNotFound) {
				return nil, sbErr
			}

			log.G(ctx).WithField("id", id).Warningf("sandbox (id=%s) not found, maybe created from v1.x", opts.SandboxID)
			shouldInvokeShimBinary = true
		} else {
			if opts.Address != "" {
				protocol, address, ok := strings.Cut(opts.Address, "+")
				if !ok {
					return nil, fmt.Errorf("the scheme of sandbox address should be in " +
						" the form of <protocol>+<unix|vsock|tcp>, i.e. ttrpc+unix or grpc+vsock")
				}
				params = shimbinary.BootstrapParams{
					Version:  int(opts.Version),
					Protocol: protocol,
					Address:  address,
				}
			} else {
				process, err := m.Get(ctx, opts.SandboxID)
				if err != nil {
					return nil, fmt.Errorf("can't find shim for sandbox %s: %w", opts.SandboxID, err)
				}

				p, err := restoreBootstrapParams(process.Bundle())
				if err != nil {
					return nil, fmt.Errorf("failed to get bootstrap params of sandbox %s: %w", opts.SandboxID, err)
				}
				params = p
			}
		}
	}

	const supportSandboxAPIVersion = 3
	if params.Version < supportSandboxAPIVersion {
		shouldInvokeShimBinary = true
	}

	if !shouldInvokeShimBinary {
		if err := os.WriteFile(filepath.Join(bundle.Path, "sandbox"), []byte(opts.SandboxID), 0600); err != nil {
			return nil, err
		}

		if err := writeBootstrapParams(filepath.Join(bundle.Path, "bootstrap.json"), params); err != nil {
			return nil, fmt.Errorf("failed to write bootstrap.json for bundle %s: %w", bundle.Path, err)
		}

		shim, err := loadShim(ctx, bundle, func() {})
		if err != nil {
			return nil, fmt.Errorf("failed to load sandbox task %q: %w", opts.SandboxID, err)
		}

		if err := m.shims.Add(ctx, shim); err != nil {
			return nil, err
		}

		return shim, nil
	}

	shim, err := m.startShim(ctx, bundle, id, opts)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			m.cleanupShim(ctx, shim)
		}
	}()

	if err := m.shims.Add(ctx, shim); err != nil {
		return nil, fmt.Errorf("failed to add task: %w", err)
	}

	return shim, nil
}

func (m *ShimManager) startShim(ctx context.Context, bundle *Bundle, id string, opts runtime.CreateOpts) (*shim, error) {
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return nil, err
	}
	ctx = log.WithLogger(ctx, log.G(ctx).WithField("namespace", ns))

	topts := opts.TaskOptions
	if topts == nil || topts.GetValue() == nil {
		topts = opts.RuntimeOptions
	}

	runtimePath, err := m.resolveRuntimePath(opts.Runtime)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve runtime path: %w", err)
	}

	b := shimBinary(bundle, shimBinaryConfig{
		runtime:      runtimePath,
		address:      m.containerdAddress,
		ttrpcAddress: m.containerdTTRPCAddress,
		env:          m.env,
	})
	shim, err := b.Start(ctx, typeurl.MarshalProto(topts), func() {
		log.G(ctx).WithField("id", id).Info("shim disconnected")

		cleanupAfterDeadShim(context.WithoutCancel(ctx), id, m.shims, m.events, b)
		m.shims.Delete(ctx, id)
	})
	if err != nil {
		return nil, fmt.Errorf("start failed: %w", err)
	}

	return shim, nil
}

func restoreBootstrapParams(bundlePath string) (shimbinary.BootstrapParams, error) {
	filePath := filepath.Join(bundlePath, "bootstrap.json")

	if _, err := os.Stat(filePath); err == nil {
		return readBootstrapParams(filePath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return shimbinary.BootstrapParams{}, fmt.Errorf("failed to stat %s: %w", filePath, err)
	}

	address, err := shimbinary.ReadAddress(filepath.Join(bundlePath, "address"))
	if err != nil {
		return shimbinary.BootstrapParams{}, fmt.Errorf("unable to migrate shim: failed to get socket address for bundle %s: %w", bundlePath, err)
	}

	params := shimbinary.BootstrapParams{
		Version:  2,
		Address:  address,
		Protocol: "ttrpc",
	}

	if err := writeBootstrapParams(filePath, params); err != nil {
		return shimbinary.BootstrapParams{}, fmt.Errorf("unable to migrate: failed to write bootstrap.json file: %w", err)
	}

	return params, nil
}

func (m *ShimManager) PluginInfo(ctx context.Context, request interface{}) (interface{}, error) {
	req, ok := request.(*apitypes.RuntimeRequest)
	if !ok {
		return nil, fmt.Errorf("unknown request type %T: %w", request, errdefs.ErrNotImplemented)
	}

	runtimePath, err := m.resolveRuntimePath(req.RuntimePath)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve runtime path: %w", err)
	}
	var optsB []byte
	if req.Options != nil {
		optsB, err = proto.Marshal(req.Options)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal %s: %w", req.Options.TypeUrl, err)
		}
	}
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, runtimePath, "-info")
	cmd.Stdin = bytes.NewReader(optsB)
	cmd.Stderr = &stderr
	stdout, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to run %v: %w (stderr: %q)", cmd.Args, err, stderr.String())
	}
	var info apitypes.RuntimeInfo
	if err = proto.Unmarshal(stdout, &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal stdout from %v into %T: %w", cmd.Args, &info, err)
	}
	return &info, nil
}

func (m *ShimManager) resolveRuntimePath(runtime string) (string, error) {
	if runtime == "" {
		return "", fmt.Errorf("no runtime name")
	}

	if filepath.IsAbs(runtime) {
		if _, err := os.Stat(runtime); err != nil {
			return "", fmt.Errorf("invalid custom binary path: %w", err)
		}

		return runtime, nil
	}

	if strings.Contains(runtime, "/") {
		return "", fmt.Errorf("invalid runtime name %s, correct runtime name should be either format like `io.containerd.runc.v2` or a full path to the binary", runtime)
	}

	name := shimbinary.BinaryName(runtime)
	if name == "" {
		return "", fmt.Errorf("invalid runtime name %s, correct runtime name should be either format like `io.containerd.runc.v2` or a full path to the binary", runtime)
	}

	if path, ok := m.runtimePaths.Load(name); ok {
		return path.(string), nil
	}

	var (
		cmdPath string
		lerr    error
	)

	binaryPath := shimbinary.BinaryPath(runtime)
	if _, serr := os.Stat(binaryPath); serr == nil {
		cmdPath = binaryPath
	}

	if cmdPath == "" {
		if cmdPath, lerr = exec.LookPath(name); lerr != nil {
			if eerr, ok := lerr.(*exec.Error); ok {
				if eerr.Err == exec.ErrNotFound {
					self, err := os.Executable()
					if err != nil {
						return "", err
					}

					testPath := filepath.Join(filepath.Dir(self), name)
					if _, serr := os.Stat(testPath); serr == nil {
						cmdPath = testPath
					}
					if cmdPath == "" {
						return "", fmt.Errorf("runtime %q binary not installed %q: %w", runtime, name, os.ErrNotExist)
					}
				}
			}
		}
	}

	cmdPath, err := filepath.Abs(cmdPath)
	if err != nil {
		return "", err
	}

	if path, ok := m.runtimePaths.LoadOrStore(name, cmdPath); ok {
		cmdPath = path.(string)
	}

	return cmdPath, nil
}

func (m *ShimManager) cleanupShim(ctx context.Context, shim *shim) {
	dctx, cancel := timeout.WithContext(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()

	_ = shim.Delete(dctx)
	m.shims.Delete(dctx, shim.ID())
}

func (m *ShimManager) Get(ctx context.Context, id string) (ShimInstance, error) {
	return m.shims.Get(ctx, id)
}

func (m *ShimManager) Delete(ctx context.Context, id string) error {
	shim, err := m.shims.Get(ctx, id)
	if err != nil {
		return err
	}

	err = shim.Delete(ctx)
	m.shims.Delete(ctx, id)

	return err
}

type shimInfo struct {
	handledMounts []string
}

func (m *ShimManager) loadShimInfo(ctx context.Context, shim string) (*shimInfo, error) {
	if i, ok := m.shimInfos.Load(shim); ok {
		return i.(*shimInfo), nil
	}
	if shim == "io.containerd.runc.v2" || shim == "io.containerd.runhcs.v1" {
		sinfo := &shimInfo{}
		m.shimInfos.Store(shim, sinfo)
		return sinfo, nil
	}

	pInfo, err := m.PluginInfo(ctx, &apitypes.RuntimeRequest{RuntimePath: shim})
	if err != nil {
		return nil, err
	}
	rinfo, ok := pInfo.(*apitypes.RuntimeInfo)
	if !ok {
		return nil, fmt.Errorf("invalid runtime info type: %T", pInfo)
	}

	sinfo := &shimInfo{}
	if rinfo.Annotations != nil {
		if v, ok := rinfo.Annotations[allowedMounts]; ok {
			sinfo.handledMounts = strings.Split(v, ",")
		}
	}

	fields := log.Fields{}
	for k, v := range rinfo.Annotations {
		fields[k] = v
	}
	log.G(ctx).WithFields(fields).WithField("shim", shim).Debug("loaded shim info")

	m.shimInfos.Store(shim, sinfo)
	return sinfo, nil
}
