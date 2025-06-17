package geeRPC

import (
	"net"
	"strings"
	"testing"
	"time"
)

func TestClient_dialTimeout(t *testing.T) {
	// 并行测试
	t.Parallel()
	// 创建一个随机端口
	l, _ := net.Listen("tcp", ":0")
	// 创建一个函数，用于测试连接超时
	f := func(conn net.Conn, opt *Option) (client *Client, err error) {
		_ = conn.Close()
		time.Sleep(time.Second * 2)
		return nil, nil
	}
	// 超时情况
	t.Run("timeout", func(t *testing.T) {
		_, err := dialTimeout(f, "tcp", l.Addr().String(), &Option{ConnectTimeout: time.Second})
		_assert(err != nil && strings.Contains(err.Error(), "connect timeout"), "expect a timeout error")
	})
	// 无超时限制情况
	t.Run("0", func(t *testing.T) {
		_, err := dialTimeout(f, "tcp", l.Addr().String(), &Option{ConnectTimeout: 0})
		_assert(err == nil, "0 means no limit")
	})
}
