package worker

import (
	"go.etcd.io/etcd/clientv3"
	"context"
	"github.com/betNevS/gocrontab/common"
)

type JobLock struct {
	kv clientv3.KV
	lease clientv3.Lease
	jobName string
	cancelFunc context.CancelFunc
	leaseId clientv3.LeaseID
	isLocked bool
}

// 初始化一把锁
func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) (jobLock *JobLock) {
	jobLock = &JobLock{
		kv:kv,
		lease:lease,
		jobName:jobName,
	}
	return
}

// 尝试上锁
func (jobLock *JobLock) TryLock() (err error) {
	var (
		leaseGrantResp *clientv3.LeaseGrantResponse
		cancelCtx context.Context
		cancelFunc context.CancelFunc
		leaseId clientv3.LeaseID
		keepRespChan <-chan *clientv3.LeaseKeepAliveResponse
		txn clientv3.Txn
		lockKey string
		txnResp *clientv3.TxnResponse
	)
	// 1、创建租约
	if leaseGrantResp, err = jobLock.lease.Grant(context.TODO(), 5); err != nil {
		return
	}
	// context 用于取消自动续租
	cancelCtx, cancelFunc = context.WithCancel(context.TODO())
	// 2、自动续租
	leaseId = leaseGrantResp.ID
	if keepRespChan, err = jobLock.lease.KeepAlive(cancelCtx, leaseId); err != nil {
		goto FAIL
	}

	// 处理续约应答的协程
	go func() {
		var (
			keepResp *clientv3.LeaseKeepAliveResponse
		)

		for {
			select {
			case keepResp = <-keepRespChan:
				if keepResp == nil {
					goto END
				}
			}
		}
		END:
	}()

	// 3、创建事务txn
	txn = jobLock.kv.Txn(context.TODO())
	// 4、事务抢锁
	lockKey = common.JOB_LOCK_DIR + jobLock.jobName
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "1", clientv3.WithLease(leaseId))).
			Else(clientv3.OpGet(lockKey))
	// 5、成功则放回，失败则释放租约
	if txnResp, err = txn.Commit(); err != nil {
		goto FAIL
	}

	if !txnResp.Succeeded {
		err = common.ERR_LOCK_ALREADY_REQUIRED
		goto FAIL
	}
	// 6、抢锁成功
	jobLock.leaseId = leaseId
	jobLock.cancelFunc = cancelFunc
	jobLock.isLocked = true
	return

	FAIL:
		cancelFunc()
		jobLock.lease.Revoke(context.TODO(),leaseId) // 释放租约
		return
}

// 释放锁
func (jobLock *JobLock) UnLock() {
	if jobLock.isLocked {
		jobLock.cancelFunc() // 取消自动续租的协程
		jobLock.lease.Revoke(context.TODO(), jobLock.leaseId) // 释放租约
	}
}