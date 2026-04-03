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
	"archive/tar"
	"context"
	"os"
	"path/filepath"
	"testing"

	crmetadata "github.com/checkpoint-restore/checkpointctl/lib"
	"github.com/stretchr/testify/require"
)

func TestShimTaskRestoreFromPath(t *testing.T) {
	bundleDir := t.TempDir()
	checkpointDir := filepath.Join(t.TempDir(), "checkpoint")

	require.NoError(t, os.MkdirAll(filepath.Join(bundleDir, "rootfs"), 0o755))
	require.NoError(t, os.MkdirAll(checkpointDir, 0o755))

	rootfsDiff := filepath.Join(checkpointDir, "..", crmetadata.RootFsDiffTar)
	require.NoError(t, writeTarFile(rootfsDiff, "restored/file.txt", "checkpoint-data"))

	task := &shimTask{
		ShimInstance: &shim{
			bundle: &Bundle{
				ID:   "test-task",
				Path: bundleDir,
			},
		},
	}

	require.NoError(t, task.restoreFromPath(context.Background(), checkpointDir))

	content, err := os.ReadFile(filepath.Join(bundleDir, "rootfs", "restored", "file.txt"))
	require.NoError(t, err)
	require.Equal(t, "checkpoint-data", string(content))
}

func writeTarFile(path string, name string, content string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	tw := tar.NewWriter(f)
	defer tw.Close()

	if err := tw.WriteHeader(&tar.Header{
		Name: name,
		Mode: 0o644,
		Size: int64(len(content)),
		Uid:  os.Getuid(),
		Gid:  os.Getgid(),
	}); err != nil {
		return err
	}

	_, err = tw.Write([]byte(content))
	return err
}
