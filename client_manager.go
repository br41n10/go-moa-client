package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/blackbeans/log4go"

	"github.com/blackbeans/go-moa"
	"github.com/blackbeans/turbo"
)

type MoaClientManager struct {
	clientsManager *turbo.ClientManager
	uri2Ips        *sync.Map //map[string] /*uri*/ Strategy
	addrToTClient  *sync.Map // address to  tclient
	addrManager    *AddressManager
	op             core.Option
	snappy         bool
	config         *turbo.TConfig
	ctx            context.Context
}

func NewMoaClientManager(ctx context.Context, option core.Option, uris []string) *MoaClientManager {

	cluster := option.Clusters[option.Client.RunMode]
	var reg core.IRegistry
	if strings.HasPrefix(cluster.Registry, core.SCHEMA_ZK) {
		reg = core.NewZkRegistry(strings.TrimPrefix(cluster.Registry, core.SCHEMA_ZK), uris, false)
	}

	reconnect := turbo.NewReconnectManager(true,
		10*time.Second, 10,
		func(ga *turbo.GroupAuth, remoteClient *turbo.TClient) (bool, error) {
			return true, nil
		})

	manager := &MoaClientManager{ctx: ctx}
	//参数
	manager.config =
		turbo.NewTConfig(
			"moa-client",
			cluster.MaxDispatcherSize,
			cluster.ReadBufferSize,
			cluster.ReadBufferSize,
			cluster.WriteChannelSize,
			cluster.ReadChannelSize,
			cluster.IdleTimeout,
			cluster.FutureSize)

	manager.op = option
	manager.clientsManager = turbo.NewClientManager(reconnect)
	manager.uri2Ips = &sync.Map{}
	manager.addrToTClient = &sync.Map{}
	if strings.ToLower(option.Client.Compress) == "snappy" {
		manager.snappy = true
	}
	addrManager := NewAddressManager(reg, uris, manager.OnAddressChange)
	manager.addrManager = addrManager

	go manager.CheckAlive()
	return manager
}

func (self *MoaClientManager) CheckAlive() {
	t := time.Tick(5 * time.Second)
	for {
		<-t
		for hp, c := range self.clientsManager.ClientsClone() {
			ip := hp
			server := c
			//如果当前client是空闲的则需要发送Ping-pong
			if c.Idle() {
				go func() {
					pipo := core.PiPo{Timestamp: time.Now().Unix()}
					p := turbo.NewPacket(core.PING, nil)
					p.PayLoad = pipo
					err := server.Ping(p, self.op.Clusters[self.op.Client.RunMode].ProcessTimeout)
					if nil != err {
						log4go.WarnLog("config_center", "CheckAlive|FAIL|%s|%v", ip, err)
					} else {
						log4go.WarnLog("config_center", "CheckAlive|SUCC|%s...", ip)
					}
				}()
			}
		}

	}
}

func (self *MoaClientManager) OnAddressChange(uri string, hosts []string) {
	log4go.WarnLog("config_center", "OnAddressChange|%s|%s", uri, hosts)
	//新增地址
	addHostport := make([]string, 0, 2)
	//寻找新增连接
	for _, ip := range hosts {
		exist := self.clientsManager.FindTClient(ip)
		if nil == exist {
			addHostport = append(addHostport, ip)
		}
	}

	for _, hp := range addHostport {
		hostport := hp
		newFuture := turbo.NewFutureTask(func(ctx context.Context) (interface{}, error) {
			connection, err := net.DialTimeout("tcp", hostport, self.op.Clusters[self.op.Client.RunMode].ProcessTimeout*5)
			if nil != err {
				log4go.ErrorLog("config_center", "MoaClientManager|Create Client|FAIL|%s|%v", hostport, err)
				return nil, err
			}
			conn := connection.(*net.TCPConn)

			c := turbo.NewTClient(self.ctx, conn, func() turbo.ICodec {
				return core.BinaryCodec{
					MaxFrameLength: turbo.MAX_PACKET_BYTES,
					SnappyCompress: self.snappy}
			}, self.dis, self.config)
			c.Start()
			log4go.InfoLog("config_center", "MoaClientManager|Create Client|SUCC|%s", hostport)
			self.clientsManager.Auth(turbo.NewGroupAuth(hostport, ""), c)
			return c, nil
		})

		//避免重复创建，如果已经存在的不需要做任何事情，如果不存在那么就需要主动执行一次
		_, loaded := self.addrToTClient.LoadOrStore(hostport, newFuture)
		if !loaded {
			newFuture.Run(self.ctx)
		}
	}

	if self.op.Client.SelectorStrategy == core.STRATEGY_KETAMA {
		self.uri2Ips.Store(uri, NewKetamaStrategy(hosts))
	} else if self.op.Client.SelectorStrategy == core.STRATEGY_RANDOM {
		self.uri2Ips.Store(uri, NewRandomStrategy(hosts))
	} else {
		self.uri2Ips.Store(uri, NewRandomStrategy(hosts))
	}

	log4go.InfoLog("config_center", "MoaClientManager|Store Uri Pool|SUCC|%s|%v", uri, hosts)
	//清理掉不再使用client
	usingIps := make(map[string]bool, 5)
	self.uri2Ips.Range(func(key, value interface{}) bool {
		strategy := value.(Strategy)
		strategy.Iterator(func(i int, ip string) {
			usingIps[ip] = true
		})
		return true
	})
	for ip := range self.clientsManager.ClientsClone() {
		_, ok := usingIps[ip]
		if !ok {
			//不再使用了移除
			self.addrToTClient.Delete(ip)
			self.clientsManager.DeleteClients(ip)
			log4go.InfoLog("config_center", "MoaClientManager|RemoveUnUse Client|SUCC|%s", ip)
		}
	}
	log4go.InfoLog("config_center", "MoaClientManager|OnAddressChange|SUCC|%s|%v", uri, hosts)
}

//根据Uri获取连接
func (self *MoaClientManager) SelectClient(uri string, key string) (*turbo.TClient, error) {

	strategy, ok := self.uri2Ips.Load(uri)
	if ok {
		ip := strategy.(Strategy).Select(key)
		if len(ip) > 0 {
			p, loaded := self.addrToTClient.Load(ip)
			if loaded && nil != p {
				future := p.(*turbo.FutureTask)
				tclient, err := future.Get()
				if nil != err {
					return nil, err
				}

				return tclient.(*turbo.TClient), nil
			}
		}
	}
	return nil, errors.New(fmt.Sprintf("NO CLIENT FOR %s", uri))
}

func (self *MoaClientManager) Destroy() {
	self.clientsManager.Shutdown()
}

//设置
func (self *MoaClientManager) dis(ctx *turbo.TContext) error {
	p := ctx.Message
	if p.Header.CmdType == core.PONG {
		pipo := p.PayLoad.(core.PiPo)
		ctx.Client.Attach(p.Header.Opaque, pipo.Timestamp)
	} else {
		ctx.Client.Attach(p.Header.Opaque, p.PayLoad)
	}
	return nil
}
