package geeRPC

import (
	"encoding/json"
	"errors"
	"fmt"
	"geeRPC/codec"
	"go/ast"
	"io"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MagicNumber   = 0x3bef5c
	CodecTypeGob  = "application/gob"
	CodecTypeJson = "application/json"
)

// RPC 服务端
type Server struct {
	serviceMap sync.Map // 存储服务实例 key: 服务名称serviceName, value: 服务实例service
}

// 默认的 Server 实例
var DefaultServer = NewServer()

// 定义 Option 结构体
type Option struct {
	MagicNumber    uint64        // 用于识别不同的协议
	CodecType      codec.Type    // 客户端可以指定使用哪种 Codec 编码
	ConnectTimeout time.Duration // 连接超时时间
	HandleTimeout  time.Duration // 处理超时时间
}

// 默认的 Option
var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      CodecTypeGob,
	ConnectTimeout: time.Second * 10,
}

// 请求体
type request struct {
	h      *codec.Header // 请求头
	argv   reflect.Value // 参数
	replyv reflect.Value // 返回值
	mtype  *MethodType   // 方法类型
	svc    *service      // 所属服务实例
}

// 无效的请求
var invalidRequest = struct{}{}

// 反射方法类型
type MethodType struct {
	method    reflect.Method // 方法本身
	ArgType   reflect.Type   // 参数类型
	ReplyType reflect.Type   // 返回值类型
	numCalls  uint64         // 调用次数
}

// 服务实例
type service struct {
	name   string                 // 映射的服务结构体的名称
	typ    reflect.Type           // 服务结构体的类型
	rcvr   reflect.Value          // 服务结构体的实例本身
	method map[string]*MethodType // 存储映射的服务结构体的所有符合条件的方法
}

// 创建 Server 实例
func NewServer() *Server {
	return &Server{}
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

// 默认的 Accept 方法
func Accept(listener net.Listener) {
	DefaultServer.Accept(listener)
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
	server.serveCodec(coder, opt.HandleTimeout)
}

// Codec 处理请求
func (server *Server) serveCodec(cc codec.Codec, timeout time.Duration) {
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
		go server.handleRequest(cc, req, sending, wg, timeout)
	}
	// 若关闭连接前，还有未发送完数据，先等待处理完所有数据
	wg.Wait()
	_ = cc.Close()
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
	req.svc, req.mtype, err = server.findService(req.h.ServiceMethod)
	if err != nil {
		return nil, err
	}

	req.argv = req.mtype.newArgv()
	req.replyv = req.mtype.newReplyv()

	argvi := req.argv.Interface()
	// 确保argvi 是一个指针类型
	if req.argv.Type().Kind() != reflect.Pointer {
		argvi = req.argv.Addr().Interface()
	}
	// 将请求体的数据反序列化到argvi中
	if err := cc.ReadBody(argvi); err != nil {
		log.Println("rpc server: read argv error:", err)
		return req, err
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
func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	// 创建两个通道，用于通知调用方和发送方
	called := make(chan struct{})
	sent := make(chan struct{})

	go func() {
		err := req.svc.call(req.mtype, req.argv, req.replyv)
		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		sent <- struct{}{}
	}()
	// 若timeout为0，则一直阻塞等待
	if timeout == 0 {
		<-called
		<-sent
		return
	}

	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		server.sendResponse(cc, req.h, invalidRequest, sending)
	// 仅实现调用超时处理，不处理发送超时
	case <-called:
		<-sent
	}

}

// 注册服务
func (server *Server) Register(rcvr interface{}) error {
	s := newService(rcvr)
	if _, dup := server.serviceMap.LoadOrStore(s.name, s); dup {
		return errors.New("rpc: service already defined: " + s.name)
	}
	return nil
}

// 默认的 Register 方法
func Register(rcvr interface{}) error {
	return DefaultServer.Register(rcvr)
}

// 服务发现
func (server *Server) findService(serviceMethod string) (svc *service, mtype *MethodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	svc = svci.(*service)
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}

// 获取调用次数
func (m *MethodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls)
}

// 创建参数实例
func (m *MethodType) newArgv() reflect.Value {
	var argv reflect.Value

	// 如果参数是指针类型，则返回指针类型
	if m.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(m.ArgType.Elem())
	} else {
		// 如果参数不是指针类型，则返回值类型
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

// 创建返回值实例
func (m *MethodType) newReplyv() reflect.Value {
	// replyv 必须是一个指针类型
	replyv := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	}

	return replyv
}

func newService(rcvr interface{}) *service {
	s := new(service)
	s.rcvr = reflect.ValueOf(rcvr)
	s.name = reflect.Indirect(reflect.ValueOf(rcvr)).Type().Name()
	s.typ = reflect.TypeOf(rcvr)
	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server: %s is not a valid service name", s.name)
	}
	s.registerMethods()
	return s
}

func (s *service) registerMethods() {
	s.method = make(map[string]*MethodType)
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mType := method.Type
		// 方法必须有三个参数, 第一个参数是自身；一个返回值
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}
		// 返回值必须是 error 类型
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		// 参数和返回值必须是导出的
		argType, replyType := mType.In(1), mType.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		s.method[method.Name] = &MethodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("rpc server: register %s.%s\n", s.name, method.Name)
	}
}

// 判断类型是否是导出的或者内置的
func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}

// 调用方法
func (s *service) call(m *MethodType, argv, replyv reflect.Value) error {
	atomic.AddUint64(&m.numCalls, 1)
	f := m.method.Func

	returnValues := f.Call([]reflect.Value{s.rcvr, argv, replyv})
	if errInter := returnValues[0].Interface(); errInter != nil {
		return errInter.(error)
	}
	return nil
}
