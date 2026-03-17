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

package sbserver

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/containerd/cgroups/v3"
	"github.com/containerd/cgroups/v3/cgroup1"
	cg1 "github.com/containerd/cgroups/v3/cgroup1/stats"
	cg2 "github.com/containerd/cgroups/v3/cgroup2/stats"
	"github.com/containerd/log"
	"github.com/containernetworking/plugins/pkg/ns"
	"github.com/vishvananda/netlink"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"

	"github.com/containerd/containerd/errdefs"
	sandboxstore "github.com/containerd/containerd/pkg/cri/store/sandbox"
)

func (c *criService) podSandboxStats(
	ctx context.Context,
	sandbox sandboxstore.Sandbox) (*runtime.PodSandboxStats, error) {
	meta := sandbox.Metadata

	if sandbox.Status.Get().State != sandboxstore.StateReady {
		return nil, fmt.Errorf("failed to get pod sandbox stats since sandbox container %q is not in ready state: %w", meta.ID, errdefs.ErrUnavailable)
	}

	stats, err := metricsForSandbox(sandbox)
	if err != nil {
		return nil, fmt.Errorf("failed getting metrics for sandbox %s: %w", sandbox.ID, err)
	}

	podSandboxStats := &runtime.PodSandboxStats{
		Linux: &runtime.LinuxPodSandboxStats{},
		Attributes: &runtime.PodSandboxAttributes{
			Id:          meta.ID,
			Metadata:    meta.Config.GetMetadata(),
			Labels:      meta.Config.GetLabels(),
			Annotations: meta.Config.GetAnnotations(),
		},
	}

	if stats != nil {
		timestamp := time.Now()

		cpuStats, err := c.cpuContainerStats(meta.ID, true /* isSandbox */, stats, timestamp)
		if err != nil {
			return nil, fmt.Errorf("failed to obtain cpu stats: %w", err)
		}
		podSandboxStats.Linux.Cpu = cpuStats

		memoryStats, err := c.memoryContainerStats(meta.ID, stats, timestamp)
		if err != nil {
			return nil, fmt.Errorf("failed to obtain memory stats: %w", err)
		}
		podSandboxStats.Linux.Memory = memoryStats

		
	}

	return podSandboxStats, nil
}

// https://github.com/cri-o/cri-o/blob/74a5cf8dffd305b311eb1c7f43a4781738c388c1/internal/oci/stats.go#L32
func getContainerNetIO(ctx context.Context, netNsPath string) (rxBytes, rxErrors, txBytes, txErrors uint64) {
	ns.WithNetNSPath(netNsPath, func(_ ns.NetNS) error {
		link, err := netlink.LinkByName(defaultIfName)
		if err != nil {
			log.G(ctx).WithError(err).Errorf("unable to retrieve network namespace stats for netNsPath: %v, interface: %v", netNsPath, defaultIfName)
			return err
		}
		attrs := link.Attrs()
		if attrs != nil && attrs.Statistics != nil {
			rxBytes = attrs.Statistics.RxBytes
			rxErrors = attrs.Statistics.RxErrors
			txBytes = attrs.Statistics.TxBytes
			txErrors = attrs.Statistics.TxErrors
		}
		return nil
	})

	return rxBytes, rxErrors, txBytes, txErrors
}

func metricsForSandbox(sandbox sandboxstore.Sandbox) (interface{}, error) {
	cgroupPath := sandbox.Config.GetLinux().GetCgroupParent()

	if cgroupPath == "" {
		return nil, fmt.Errorf("failed to get cgroup metrics for sandbox %v because cgroupPath is empty", sandbox.ID)
	}

	if cgroups.Mode() == cgroups.Unified {
		// cgroup v2
		path := filepath.Join("/sys/fs/cgroup", cgroupPath)

		cpuStat, _ := readKVStats(path, "cpu.stat")
		memoryCurrent, _ := readUint64(path, "memory.current")
		memoryMax, _ := readUint64(path, "memory.max")
		memoryStat, _ := readKVStats(path, "memory.stat")

		return &cg2.Metrics{
			CPU: &cg2.CPUStat{
				UsageUsec: cpuStat["usage_usec"],
			},
			Memory: &cg2.MemoryStat{
				Usage:        memoryCurrent,
				UsageLimit:   memoryMax,
				InactiveFile: memoryStat["inactive_file"],
			},
		}, nil

	} else {
		control, err := cgroup1.Load(cgroup1.StaticPath(cgroupPath))
		if err != nil {
			return nil, fmt.Errorf("failed to load sandbox cgroup %v: %w", cgroupPath, err)
		}

		var cpuUsage, memoryUsage, inactiveFile uint64
		for _, s := range control.Subsystems() {
			if p, ok := s.(pather); ok {
				subPath := p.Path(cgroupPath)
				switch s.Name() {
				case cgroup1.Cpuacct:
					cpuUsage, _ = readUint64(subPath, "cpuacct.usage")
				case cgroup1.Memory:
					memoryUsage, _ = readUint64(subPath, "memory.usage_in_bytes")
					memStat, _ := readKVStats(subPath, "memory.stat")
					inactiveFile = memStat["total_inactive_file"]
				}
			}
		}

		return &cg1.Metrics{
			CPU: &cg1.CPUStat{
				Usage: &cg1.CPUUsage{
					Total: cpuUsage,
				},
			},
			Memory: &cg1.MemoryStat{
				Usage: &cg1.MemoryEntry{
					Usage: memoryUsage,
				},
				TotalInactiveFile: inactiveFile,
			},
		}, nil
	}
}
