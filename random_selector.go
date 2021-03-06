package client

import (
	"math/rand"
	"sync"
	"time"
)

type RandomStrategy struct {
	nodes  []string
	length int
	sync.RWMutex
}

func NewRandomStrategy(nodes []string) *RandomStrategy {
	return &RandomStrategy{
		nodes:  nodes,
		length: len(nodes)}
}

func (self *RandomStrategy) ReHash(nodes []string) {
	self.Lock()
	defer self.Unlock()
	self.nodes = nodes
	self.length = len(nodes)
}

func (self *RandomStrategy) Select(key string) string {
	self.RLock()
	defer self.RUnlock()
	if len(self.nodes) <= 0 {
		return ""
	}
	src := rand.NewSource(time.Now().UnixNano())
	random := rand.New(src)
	return self.nodes[random.Intn(self.length)]
}

func (self *RandomStrategy) Iterator(f func(idx int, node string)) {
	self.RLock()
	defer self.RUnlock()
	for i, n := range self.nodes {
		f(i, n)
	}
}
