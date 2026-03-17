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
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadKVStats(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test-kv-stats")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	content := `usage_usec 12345
user_usec 1000
system_usec 500
`
	err = os.WriteFile(filepath.Join(tmpDir, "cpu.stat"), []byte(content), 0644)
	assert.NoError(t, err)

	stats, err := readKVStats(tmpDir, "cpu.stat")
	assert.NoError(t, err)
	assert.Equal(t, uint64(12345), stats["usage_usec"])
	assert.Equal(t, uint64(1000), stats["user_usec"])
	assert.Equal(t, uint64(500), stats["system_usec"])
}

func TestReadUint64(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test-uint64")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	err = os.WriteFile(filepath.Join(tmpDir, "memory.current"), []byte("123456789\n"), 0644)
	assert.NoError(t, err)

	val, err := readUint64(tmpDir, "memory.current")
	assert.NoError(t, err)
	assert.Equal(t, uint64(123456789), val)

	err = os.WriteFile(filepath.Join(tmpDir, "memory.max"), []byte("max"), 0644)
	assert.NoError(t, err)

	val, err = readUint64(tmpDir, "memory.max")
	assert.NoError(t, err)
	assert.Equal(t, uint64(math.MaxUint64), val)
}
