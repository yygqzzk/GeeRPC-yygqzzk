package main

import (
	"encoding/json"
	"fmt"
	"geeRPC"
	"geeRPC/codec"
	"log"
	"net"
	"time"
)

func startServer(addr chan string) {
	// 让系统自动分配一个可用端口，并监听
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal("network error: ", err)
	}
	log.Println("start rpc server on", listener.Addr())
	addr <- listener.Addr().String()
	geeRPC.Accept(listener)
}

func main() {
	addr := make(chan string)
	go startServer(addr)

	// 简易RPC客户端
	// 连接到服务器
	conn, _ := net.Dial("tcp", <-addr)
	defer func() { _ = conn.Close() }()

	time.Sleep(time.Second)
	// RPC第一次请求先回发送JSON选项，用于确定后续的通信方式，包括（魔数、编码方式）
	// 将默认参数选项编码为json格式，并发送给服务器
	_ = json.NewEncoder(conn).Encode(geeRPC.DefaultOption)

	// 创建一个gob编码器
	cc := codec.NewGobCodec(conn)
	// 发送请求 和 接收数据
	for i := 0; i < 5; i++ {
		h := &codec.Header{
			ServiceMethod: "Foo.Sum",
			Seq:           uint64(i),
		}
		// 发送请求头
		_ = cc.Write(h, fmt.Sprintf("geerpc req %d", h.Seq))
		// 读取响应头
		_ = cc.ReadHeader(h)
		// 读取响应体
		var reply string
		_ = cc.ReadBody(&reply)
		log.Println("reply:", reply)
	}
}
