package master

import (
	"go.etcd.io/etcd/clientv3"
	"time"
	"github.com/betNevS/gocrontab/common"
	"encoding/json"
	"context"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"fmt"
)

type JobMgr struct{
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
	watcher clientv3.Watcher
}

var (
	// 单例
	G_jobMgr *JobMgr
)

// 初始化管理
func InitJobMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease

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

	// 得到KV和Lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	// 复制单例
	G_jobMgr = &JobMgr{
		client:client,
		kv:kv,
		lease:lease,
	}

	return
}

// 保存任务
func (jobMgr *JobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error) {
	var (
		jobKey string
		jobValue []byte
		putResp *clientv3.PutResponse
		oldJobObj common.Job
	)

	// etcd的保存key
	jobKey = common.JOB_SAVE_DIR + job.Name
	// 任务信息json化
	if jobValue, err = json.Marshal(job); err != nil {
		return
	}
	// 保存到etcd
	if putResp, err = jobMgr.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV()); err != nil {
		return
	}
	// 如果是更新，那么将会有旧值返回
	if putResp.PrevKv != nil {
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}
	return
}

// 删除任务
func (jobMgr *JobMgr) DeleteJob(name string) (oldJob *common.Job, err error) {

	var (
		jobKey string
		delResp *clientv3.DeleteResponse
		oldJobObj common.Job
	)

	jobKey = common.JOB_SAVE_DIR + name

	// 从etcd中删除key
	if delResp, err = jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}

	// 返回被删除的任务信息
	if len(delResp.PrevKvs) != 0 {
		if err = json.Unmarshal(delResp.PrevKvs[0].Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}

	return
}

// 列举所有任务
func (jobMgr *JobMgr) ListJob() (jobList []*common.Job, err error) {

	var (
		dirKey string
		getResp *clientv3.GetResponse
		kvPair *mvccpb.KeyValue
		job *common.Job
	)

	dirKey = common.JOB_SAVE_DIR

	// 获取目录下的所有任务
	if getResp, err = jobMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix()); err != nil {
		return
	}

	// 初始化数组空间
	jobList = make([]*common.Job, 0)

	// 遍历所有任务进行反序列化
	for _, kvPair = range getResp.Kvs {
		job = &common.Job{}
		if err = json.Unmarshal(kvPair.Value, job); err != nil {
			fmt.Println(err)
			continue
		}
		jobList = append(jobList, job)
	}
	return
}

// 杀死任务
func (jobMgr *JobMgr) KillJob(name string) (err error) {
	// 原理：更新key = /cron/killer/任务名
	var (
		killerKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId clientv3.LeaseID
	)

	// 通知woker杀死对应的任务
	killerKey = common.JOB_KILLER_DIR + name

	// 让woker监听到一次put操作，创建一个租约让其在稍后自动过期
	if leaseGrantResp, err = jobMgr.lease.Grant(context.TODO(), 1); err != nil {
		return
	}

	// 租约ID
	leaseId = leaseGrantResp.ID

	// 设置killer标记
	if _, err = jobMgr.kv.Put(context.TODO(), killerKey, "1", clientv3.WithLease(leaseId)); err != nil {
		return
	}

	return
}