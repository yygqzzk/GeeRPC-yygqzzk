package codec

import "io"

type NewCodecFunc func(io.ReadWriteCloser) Codec

type Type string

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json"
)

type Header struct {
	ServiceMethod string // 服务名.方法名
	Seq           uint64 // 客户端请求序号
	Error         string // 错误信息
}

// 抽象出对消息体进行编解码的接口
type Codec interface {
	io.Closer                         // 关闭连接
	ReadHeader(*Header) error         // 读取请求头
	ReadBody(interface{}) error       // 读取请求体
	Write(*Header, interface{}) error // 写入响应头和响应体
}

var NewCodecFuncMap map[Type]NewCodecFunc

func init() {
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
}
