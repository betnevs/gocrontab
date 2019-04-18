package master

import (
	"go.mongodb.org/mongo-driver/mongo"
	"time"
	"go.mongodb.org/mongo-driver/mongo/options"
	"context"
	"github.com/betNevS/gocrontab/common"
)

type LogMgr struct {
	client *mongo.Client
	logCollection *mongo.Collection
}

var (
	G_logMgr *LogMgr
)

// 查看任务日志
func (logMgr *LogMgr) ListLog(name string, skip int, limit int) (logArr []*common.JobLog, err error){
	var (
		filter *common.JobLogFilter
		logSort *common.SortLogByStartTime
		findOpts *options.FindOptions
		cursor *mongo.Cursor
		jobLog *common.JobLog
	)
	// 初始化
	logArr = make([]*common.JobLog, 0)

	// 过滤条件
	filter = &common.JobLogFilter{
		JobName:name,
	}
	// 按照任务开始时间倒序排列
	logSort = &common.SortLogByStartTime{
		SortOrder:-1,
	}
	findOpts = options.Find()
	findOpts.SetSort(logSort)
	findOpts.SetSkip(int64(skip))
	findOpts.SetLimit(int64(limit))
	if cursor, err = logMgr.logCollection.Find(context.TODO(),filter,findOpts); err != nil {
		return
	}
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		jobLog = &common.JobLog{}
		// 反序列化BSON
		if err = cursor.Decode(jobLog); err != nil {
			continue
		}
		logArr = append(logArr, jobLog)
	}

	return
}

func InitLogMgr() (err error) {
	var (
		client *mongo.Client
	)
	// 连接mongodb
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(G_config.MongodbConnectTimeout) * time.Millisecond)

	if client, err = mongo.Connect(ctx, options.Client().ApplyURI(G_config.MongodbUri)); err != nil {
		return
	}
	// 选择db和collection
	G_logMgr = &LogMgr{
		client:client,
		logCollection:client.Database("cron").Collection("log"),
	}
	return
}