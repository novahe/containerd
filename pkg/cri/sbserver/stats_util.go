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
	"bufio"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const maxStatsConcurrency = 20

type pather interface {
	Path(path string) string
}

func readKVStats(path string, fileName string) (map[string]uint64, error) {
	f, err := os.Open(filepath.Join(path, fileName))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	stats := make(map[string]uint64)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			val, _ := strconv.ParseUint(parts[1], 10, 64)
			stats[parts[0]] = val
		}
	}
	return stats, scanner.Err()
}

func readUint64(path string, fileName string) (uint64, error) {
	data, err := os.ReadFile(filepath.Join(path, fileName))
	if err != nil {
		return 0, err
	}
	val, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		// handle "max" case for memory limit
		if strings.TrimSpace(string(data)) == "max" {
			return math.MaxUint64, nil
		}
		return 0, err
	}
	return val, nil
}
