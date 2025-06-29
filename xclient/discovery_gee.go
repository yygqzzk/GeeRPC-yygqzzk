package xclient

import (
	"log"
	"net/http"
	"strings"
	"time"
)

type GeeRegistryDiscovery struct {
	*MultiServersDiscovery
	registry   string
	timeout    time.Duration
	lastUpdate time.Time
}

const defaultTimeout = 10 * time.Second

func NewGeeRegistryDiscovery(registryAddr string, timeout time.Duration) *GeeRegistryDiscovery {
	if timeout == 0 {
		timeout = defaultTimeout
	}

	d := &GeeRegistryDiscovery{
		MultiServersDiscovery: NewMultiServerDiscovery(make([]string, 0)),
		registry:              registryAddr,
		timeout:               timeout,
	}

	return d
}

func (d *GeeRegistryDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	d.lastUpdate = time.Now()
	return nil
}

func (d *GeeRegistryDiscovery) Refresh() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.lastUpdate.Add(d.timeout).After(time.Now()) {
		return nil
	}

	log.Println("rpc registry: refresh server from registry", d.registry)
	rsp, err := http.Get(d.registry)
	if err != nil {
		log.Println("rpc registry refresh err: ", d.registry)
		return err
	}

	servers := strings.Split(rsp.Header.Get("X-Geerpc-Servers"), ",")
	d.servers = make([]string, 0, len(servers))
	for _, server := range servers {
		server := strings.TrimSpace(server)
		if server != "" {
			d.servers = append(d.servers, server)
		}
	}
	d.lastUpdate = time.Now()
	return nil
}

func (d *GeeRegistryDiscovery) Get(mode SelectMode) (string, error) {
	if err := d.Refresh(); err != nil {
		return "", err
	}
	return d.MultiServersDiscovery.Get(mode)
}

func (d *GeeRegistryDiscovery) GetAll() ([]string, error) {
	if err := d.Refresh(); err != nil {
		return nil, err
	}
	return d.MultiServersDiscovery.GetAll()
}
