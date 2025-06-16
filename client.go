package geeRPC

import (
	"encoding/json"
	"errors"
	"fmt"
	"geeRPC/codec"
	"io"
	"log"
	"net"
	"sync"
)

type Call struct {
	Seq           uint64      // 请求序号
	ServiceMethod string      // 服务名和方法名
	Args          interface{} // 请求参数
	Reply         interface{} // 响应数据
	Error         error       // 错误信息
	Done          chan *Call  // 完成通知
}

func (call *Call) done() {
	call.Done <- call
}

// Client represents an RPC Client.
// There may be multiple outstanding Calls associated
// with a single Client, and a Client may be used by
// multiple goroutines simultaneously.
type Client struct {
	cc       codec.Codec      // 消息的编解码器，和服务端类似，用来序列化将要发送出去的请求，以及反序列化接收到的响应。
	opt      *Option          // 选项
	sending  sync.Mutex       // 互斥锁, 为了保证请求的有序发送，即防止出现多个请求报文混淆。
	header   codec.Header     // 请求头, header 只有在请求发送时才需要，而请求发送是互斥的，因此每个客户端只需要一个，声明在 Client 结构体中可以复用。
	mu       sync.Mutex       // 互斥锁, 保护 Client 结构体中的其他字段
	seq      uint64           // 请求序号,每个请求拥有唯一编号
	pending  map[uint64]*Call // 未处理的请求, 存储未处理完的请求，键是编号，值是 Call 实例。
	closing  bool             // 是否关闭, 客户端主动关闭
	shutdown bool             // 是否停止, 服务器主动关闭, shutdown 置为 true 一般是有错误发生
}

// 自定义错误
var ErrShutdown = errors.New("connection is shut down")

// 客户端关闭连接
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.cc.Close()
}

var _ io.Closer = (*Client)(nil)

// closing 和 shutdown 任意一个值置为 true，则表示 Client 处于不可用的状态
func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

// 注册请求, 将参数 call 添加到 client.pending 中，并更新 client.seq。
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	// 这里如果调用client.IsAvailable()，则会导致死锁
	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = client.seq
	client.pending[call.Seq] = call
	client.seq++
	return call.Seq, nil
}

// 删除请求，根据 seq，从 client.pending 中移除对应的 call，并返回。
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// 终止所有请求，服务端或客户端发生错误时调用，将 shutdown 设置为 true，且将错误信息通知所有 pending 状态的 call。
func (client *Client) terminateCalls(err error) {
	// 锁住sending, 防止请求在发送过程中被终止
	client.sending.Lock()
	defer client.sending.Unlock()
	// 锁住mu, 防止访问 Client 结构体中的其他字段
	client.mu.Lock()
	defer client.mu.Unlock()

	// 将 shutdown 置为 true, 表示 Client 处于不可用的状态
	client.shutdown = true
	// 遍历 pending 中的所有请求，设置错误信息并通知完成
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

/*
call 不存在，可能是请求没有发送完整，或者因为其他原因被取消，但是服务端仍旧处理了。
call 存在，但服务端处理出错，即 h.Error 不为空。
call 存在，服务端处理正常，那么需要从 body 中读取 Reply 的值。
*/
func (client *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}
		call := client.removeCall(h.Seq)
		switch {
		case call == nil:
			// 如果 call 不存在，说明请求没有发送完整，或者因为其他原因被取消，但是服务端仍旧处理了。
			err = client.cc.ReadBody(nil)
		case h.Error != "":
			// 如果 call 存在，但服务端处理出错，即 h.Error 不为空。
			call.Error = fmt.Errorf("rpc server: %s", h.Error)
			call.done()
		default:
			// 如果 call 存在，服务端处理正常，那么需要从 body 中读取 Reply 的值。
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
	// 如果发生错误，则终止所有请求
	client.terminateCalls(err)
}

// 创建客户端
func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	// 根据编码方式创建编解码器Codec构造函数
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error: ", err)
		return nil, err
	}

	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error: ", err)
		return nil, err
	}
	coder := f(conn)
	return newClientCodec(coder, opt), nil
}

// 创建goroutine 接收异步响应
func newClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		seq:     1,
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	go client.receive()
	return client
}

// 解析Option选项
func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, fmt.Errorf("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

// 连接服务器
func Dial(network, address string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}

	defer func() {
		if client == nil {
			conn.Close()
		}
	}()

	return NewClient(conn, opt)
}

// 发送请求
func (client *Client) send(call *Call) {
	client.sending.Lock()
	defer client.sending.Unlock()

	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	// 设置请求头
	client.header.ServiceMethod = call.ServiceMethod
	client.header.Seq = seq
	client.header.Error = ""

	// 发送请求
	if err := client.cc.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)
		if call != nil {
			call.Error = err
			call.done()
		}
	}

}

// 异步接口，返回 call 实例。
func (client *Client) Go(serviceMethod string, args interface{}, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}
	// 创建Call实例
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	client.send(call)
	return call
}

// 同步接口，等待 call 实例完成并返回错误信息。
func (client *Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	call := <-client.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
}
