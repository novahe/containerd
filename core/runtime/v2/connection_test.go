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
	"net"
	"testing"
	"time"

	shimclient "github.com/containerd/containerd/v2/pkg/shim"
	"github.com/containerd/ttrpc"
)

func TestMakeConnectionTTRPCUsesShortDialTimeout(t *testing.T) {
	originalDialer := shimReconnectDialer
	defer func() {
		shimReconnectDialer = originalDialer
	}()

	var gotTimeout time.Duration
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()

	shimReconnectDialer = func(address string, timeout time.Duration) (net.Conn, error) {
		gotTimeout = timeout
		return clientConn, nil
	}

	conn, err := makeConnection(context.Background(), "test-task", shimclient.BootstrapParams{
		Version:  3,
		Protocol: "ttrpc",
		Address:  "hvsock:///tmp/hvsock:1024",
	}, func() {})
	if err != nil {
		t.Fatalf("makeConnection returned error: %v", err)
	}
	defer conn.Close()

	if _, ok := conn.(*ttrpc.Client); !ok {
		t.Fatalf("expected *ttrpc.Client, got %T", conn)
	}
	if gotTimeout <= 0 || gotTimeout > shimDialTimeout {
		t.Fatalf("expected dial timeout within (0, %s], got %s", shimDialTimeout, gotTimeout)
	}
}

func TestDialShimAddressReturnsContextErrorWhenDeadlineExpires(t *testing.T) {
	originalDialer := shimReconnectDialer
	defer func() {
		shimReconnectDialer = originalDialer
	}()

	connClosed := make(chan struct{}, 1)
	shimReconnectDialer = func(address string, timeout time.Duration) (net.Conn, error) {
		time.Sleep(50 * time.Millisecond)
		serverConn, clientConn := net.Pipe()
		go func() {
			<-time.After(10 * time.Millisecond)
			serverConn.Close()
		}()
		return &trackingConn{
			Conn:    clientConn,
			onClose: func() { connClosed <- struct{}{} },
		}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := dialShimAddress(ctx, "hvsock:///tmp/hvsock:1024")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}

	select {
	case <-connClosed:
	case <-time.After(time.Second):
		t.Fatal("expected late dialed connection to be closed")
	}
}

type trackingConn struct {
	net.Conn
	onClose func()
}

func (c *trackingConn) Close() error {
	if c.onClose != nil {
		c.onClose()
	}
	return c.Conn.Close()
}
