package worker

import (
	"go.etcd.io/etcd/clientv3"
	"time"
	"net"
	"github.com/betNevS/gocrontab/common"
	"context"
)


// 注册节点到etcd: /cron/worker/IP地址
type Register struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease

	localIP string // 本机ip
}

var (
	G_register *Register
)

// 获取本机网卡IP
func getLocalIP() (ipv4 string, err error) {
	var (
		addrs []net.Addr
		addr net.Addr
		ipNet *net.IPNet
		isIpNet bool
	)
	// 获取所有网卡
	if addrs, err = net.InterfaceAddrs(); err != nil {
		return
	}
	// 取第一个非localhost的网卡
	for _, addr = range addrs {
		if ipNet, isIpNet = addr.(*net.IPNet); isIpNet && !ipNet.IP.IsLoopback() {
			//跳过ipv6
			if ipNet.IP.To4() != nil {
				ipv4 = ipNet.IP.String()
				return
			}
		}
	}

	err = common.ERR_NO_LOCAL_IP_FOUND
	return
}

// 注册到 /cron/workers/IP，并自动续租
func (register *Register) keepOnline() {
	var (
		regKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		err error
		keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
		keepAliveResp *clientv3.LeaseKeepAliveResponse
		cancelCtx context.Context
		cancelFunc context.CancelFunc
	)
	for {
		// 注册路径
		regKey = common.JOB_WORKER_DIR + register.localIP

		cancelFunc = nil

		// 创建租约
		if leaseGrantResp, err = register.lease.Grant(context.TODO(), 5); err != nil{
			goto RETRY
		}

		// 自动续租
		if keepAliveChan, err = register.lease.KeepAlive(context.TODO(), leaseGrantResp.ID); err != nil {
			goto RETRY
		}

		cancelCtx, cancelFunc = context.WithCancel(context.TODO())

		// 注册到etcd
		if _, err = register.kv.Put(cancelCtx, regKey, "", clientv3.WithLease(leaseGrantResp.ID)); err != nil {
			goto RETRY
		}

		// 处理续租应答
		for {
			select {
			case keepAliveResp = <-keepAliveChan:
				if keepAliveResp == nil {
					// 续租失败
					goto RETRY
				}
			}
		}

		RETRY:
			time.Sleep(1 * time.Second)
			if cancelFunc != nil {
				cancelFunc()
			}
	}

}

func InitRegister() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		localIP string

	)

	// 初始化配置
	config = clientv3.Config{
		Endpoints:G_config.EtcdEndpoints, // 集群地址
		DialTimeout:time.Duration(G_config.EtcdDialTimeout) * time.Millisecond, // 超时时间
	}

	// 建立连接
	if client, err = clientv3.New(config); err != nil {
		return
	}

	// 获取本机ip
	if localIP, err = getLocalIP(); err != nil {
		return
	}

	// 得到KV和Lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	G_register = &Register{
		client:client,
		kv:kv,
		lease:lease,
		localIP:localIP,
	}

	go G_register.keepOnline()

	return

}