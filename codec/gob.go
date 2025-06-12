package codec

import (
	"bufio"        // 提供缓冲 I/O 操作
	"encoding/gob" // Go 的二进制序列化/反序列化包
	"io"           // 提供基本的 I/O 操作
	"log"          // 提供日志功能
)

type GobCodec struct {
	conn io.ReadWriteCloser // 连接
	buf  *bufio.Writer      // 缓冲区
	dec  *gob.Decoder       // gob解码器
	enc  *gob.Encoder       // gob编码器
}

// 确保 GobCodec 实现了 Codec 接口
var _ Codec = (*GobCodec)(nil)

// 创建一个 GobCodec 实例
func NewGobCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn) // 创建一个缓冲区
	return &GobCodec{
		conn: conn,
		buf:  buf,
		dec:  gob.NewDecoder(conn),
		enc:  gob.NewEncoder(buf),
	}
}

// 读取请求头
func (c *GobCodec) ReadHeader(h *Header) error {
	return c.dec.Decode(h)
}

// 读取请求体
func (c *GobCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body)
}

// 写入响应头和响应体
func (c *GobCodec) Write(h *Header, body interface{}) (err error) {
	defer func() {
		_ = c.buf.Flush()
		if err != nil {
			_ = c.Close()
		}
	}()
	if err := c.enc.Encode(h); err != nil {
		log.Println("rpc: gob error encoding header:", err)
		return err
	}

	if err := c.enc.Encode(body); err != nil {
		log.Println("rpc: gob error encoding body:", err)
		return err
	}
	return nil
}

// 关闭连接
func (c *GobCodec) Close() error {
	return c.conn.Close()
}
