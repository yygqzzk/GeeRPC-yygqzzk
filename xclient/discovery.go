package xclient

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

type SelectMode int

const (
	RandomSelect SelectMode = iota
	RoundRobinSelect
)

// 定义服务发现接口
type Discovery interface {
	Refresh() error
	Update(servers []string) error
	Get(mode SelectMode) (string, error)
	GetAll() ([]string, error)
}

// 服务发现结构体
type MultiServersDiscovery struct {
	r       *rand.Rand   // 生成随机数
	mu      sync.RWMutex // 读写锁
	servers []string
	index   int // 记录轮询算法时的轮询下标
}

func NewMultiServerDiscovery(servers []string) *MultiServersDiscovery {
	d := &MultiServersDiscovery{
		servers: servers,
		r:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	d.index = d.r.Intn(math.MaxInt32 - 1)
	return d
}

var _ Discovery = (*MultiServersDiscovery)(nil)

// TODO
func (d *MultiServersDiscovery) Refresh() error {
	return nil
}

// 动态刷新服务列表
func (d *MultiServersDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	return nil
}

// 根据负载均衡算法获取服务实例
func (d *MultiServersDiscovery) Get(mode SelectMode) (string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	n := len(d.servers)
	if n == 0 {
		return "", errors.New("rpc discovery: no available servers")
	}

	switch mode {
	case RandomSelect:
		return d.servers[d.r.Intn(n)], nil
	case RoundRobinSelect:
		s := d.servers[d.index%n]
		d.index = (d.index + 1) % n
		return s, nil
	default:
		return "", errors.New("rpc discovery: not supported select mode")
	}
}

// 获取所有服务
func (d *MultiServersDiscovery) GetAll() ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	servers := make([]string, 0, len(d.servers))

	copy(servers, d.servers)

	return servers, nil
}
