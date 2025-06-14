package geeRPC

import (
	"encoding/json"
	"fmt"
	"geeRPC/codec"
	"io"
	"log"
	"net"
	"reflect"
	"sync"
)

type Server struct{}

func NewServer() *Server {
	return &Server{}
}

// 默认的 Server 实例
var DefaultServer = NewServer()

// 默认的 Accept 方法
func Accept(listener net.Listener) {
	DefaultServer.Accept(listener)
}

// 接受连接并提供服务
func (server *Server) Accept(listener net.Listener) {
	for {
		// conn 可以看作tcp连接建立后的socket
		conn, err := listener.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		// 一个connection 对应一个 goroutine
		go server.ServeConn(conn)
	}
}

const (
	MagicNumber   = 0x3bef5c
	CodecTypeGob  = "application/gob"
	CodecTypeJson = "application/json"
)

// 定义 Option 结构体
type Option struct {
	MagicNumber uint64     // 用于识别不同的协议
	CodecType   codec.Type // 客户端可以指定使用哪种 Codec 编码
}

// 默认的 Option
var DefaultOption = &Option{
	MagicNumber: MagicNumber,
	CodecType:   CodecTypeGob,
}

// 连接参数校验、获取 Codec 实例、处理请求
func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() {
		_ = conn.Close()
	}()

	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error:", err)
		return
	}
	// 检查 MagicNumber 是否合法
	if opt.MagicNumber != MagicNumber {
		msg := fmt.Sprintf("rpc server: invalid magic number %x", opt.MagicNumber)
		log.Println(msg)
		return
	}
	// 根据 CodecType 获取对应的 Codec 构造函数
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: codec %s not found", opt.CodecType)
		return
	}
	coder := f(conn)
	server.serveCodec(coder)
}

// 无效的请求
var invalidRequest = struct{}{}

// Codec 处理请求
func (server *Server) serveCodec(cc codec.Codec) {
	sending := new(sync.Mutex) // 互斥锁，用于保护 sending 变量
	wg := new(sync.WaitGroup)  // 用于等待所有请求处理的 goroutine 完成

	for {
		req, err := server.readRequest(cc)
		if err != nil {
			if req == nil {
				// 表示断开连接，退出循环
				break
			}
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		// connection 中存在多个请求，每个请求由一个goroutine处理
		go server.handleRequest(cc, req, sending, wg)
	}
	// 若关闭连接前，还有未发送完数据，先等待处理完所有数据
	wg.Wait()
	_ = cc.Close()
}

// 请求体
type request struct {
	h      *codec.Header // 请求头
	argv   reflect.Value // 参数
	replyv reflect.Value // 返回值
}

// 读取请求头
func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

// 读取请求
func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: h}
	// TODO 目前仅支持字符串参数
	req.argv = reflect.New(reflect.TypeOf(""))
	if err := cc.ReadBody(req.argv.Interface()); err != nil {
		log.Println("rpc server: read argv error:", err)
	}
	return req, nil
}

// 发送响应
func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	defer sending.Unlock()

	// 发送响应时，需要加锁，防止多个响应交错
	sending.Lock()
	if err := cc.Write(h, body); err != nil {
		log.Printf("rpc server: write response error: %v", err)
	}
}

// 处理请求
func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Println(req.h, req.argv.Elem())
	// TODO: 仅打印argv 和 发送简单响应
	req.replyv = reflect.ValueOf(fmt.Sprintf("geeRPC resp %d", req.h.Seq))
	server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
}
