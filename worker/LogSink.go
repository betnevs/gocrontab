package worker

import (
	"go.mongodb.org/mongo-driver/mongo"
	"github.com/betNevS/gocrontab/common"
	"context"
	"time"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type LogSink struct {
	client *mongo.Client
	logCollection *mongo.Collection
	logChan chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var (
	G_LogSink *LogSink
)

func (logSink *LogSink) saveLogs(batch *common.LogBatch) {
	logSink.logCollection.InsertMany(context.TODO(), batch.Logs)
}

// 日志存储协程
func (logSink *LogSink) writeLoop() {
	var (
		log *common.JobLog
		logBatch *common.LogBatch
		commitTimer *time.Timer
		timeoutBatch *common.LogBatch
	)

	for {
		select {
		case log = <-logSink.logChan:
			// 将多条日志收集一个批次再插入
			// 1、达到一定的数量
			// 2、超过预设置的时间
			if logBatch == nil {
				logBatch = &common.LogBatch{}
				// 超过批次超时时间则自动提交
				commitTimer = time.AfterFunc(time.Duration(G_config.JobLogCommitTimeout) * time.Millisecond,
					func(batch *common.LogBatch) func(){
						return func() {
							logSink.autoCommitChan <- batch
						}
					}(logBatch))
			}
			// 把日志追加到批次中
			logBatch.Logs = append(logBatch.Logs, log)
			// 如果批次满了则立即发送
			if len(logBatch.Logs) >= G_config.JobLogBatchSize {
				logSink.saveLogs(logBatch)
				// 清空logBatch
				logBatch = nil
				commitTimer.Stop()
			}
		case timeoutBatch = <-logSink.autoCommitChan:
			// 判断过期批次是否是当前批次
			if timeoutBatch != logBatch {
				continue
			}
			// 保存日志
			logSink.saveLogs(timeoutBatch)
			logBatch = nil
		}
	}

}

// 发送日志
func (logSink *LogSink) Append(jobLog *common.JobLog) {
	select {
	case logSink.logChan <-jobLog:
	default:
		// 队列满了则丢弃
	}
}

func InitLogSink() (err error) {
	var (
		client *mongo.Client
	)
	// 连接mongodb
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(G_config.MongodbConnectTimeout) * time.Millisecond)

	if client, err = mongo.Connect(ctx, options.Client().ApplyURI(G_config.MongodbUri)); err != nil {
		return
	}
	// 选择db和collection
	G_LogSink = &LogSink{
		client:client,
		logCollection:client.Database("cron").Collection("log"),
		logChan:make(chan *common.JobLog, 1000),
		autoCommitChan:make(chan *common.LogBatch, 1000),
	}
	// 启动一个mongodb处理协程
	go G_LogSink.writeLoop()
	return
}