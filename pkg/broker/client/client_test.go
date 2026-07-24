package client

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClient_DialFailure(t *testing.T) {
	cli := NewClient("127.0.0.1:1", 50*time.Millisecond)
	err := cli.Connect()
	require.Error(t, err)
}

func TestClient_TimeoutExpiry(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		time.Sleep(200 * time.Millisecond)
	}()

	cli := NewClient(ln.Addr().String(), 20*time.Millisecond)
	require.NoError(t, cli.Connect())
	_, err = cli.Produce("t", 0, []byte("x"))
	require.Error(t, err)
	require.NoError(t, cli.Close())
}

func TestClient_HungServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 64)
		_, _ = conn.Read(buf)
	}()

	cli := NewClient(ln.Addr().String(), 30*time.Millisecond)
	require.NoError(t, cli.Connect())
	_, err = cli.Produce("t", 0, []byte("payload"))
	require.Error(t, err)
}
