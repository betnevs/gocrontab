package common

import (
	"encoding/json"
	"strings"
	"github.com/gorhill/cronexpr"
	"time"
	"context"
)

// 定时任务
type Job struct {
	Name string `json:"name"` // 任务名
	Command string `json:"command"` // shell命令
	CronExpr string `json:"cronExpr"` // cron表达式
}

// HTTP接口应答
type Response struct {
	Errno int `json:"errno"`
	Msg string `json:"msg"`
	Data interface{} `json:"data"`
}

// 变化事件
type JobEvent struct {
	EventType int // SAVE,DELETE
	Job *Job
}

// 任务调度计划
type JobSchedulePlan struct {
	Job *Job // 任务信息
	Expr *cronexpr.Expression // 解析好的cronExpr表达式
	NextTime time.Time // 下次调度时间
}


// 任务执行状态
type JobExecuteInfo struct {
	Job *Job // 任务信息
	PlanTime time.Time // 理论调度时间
	RealTime time.Time // 实际调度时间
	CancelCtx context.Context // 任务command的context上下文
	CancelFunc context.CancelFunc // 用于取消command执行的cancel函数
}

// 任务执行结果
type JobExecuteResult struct {
	ExecuteInfo *JobExecuteInfo
	Output []byte
	Err error
	StartTime time.Time
	EndTime time.Time
}

// 任务执行日志
type JobLog struct {
	JobName string `json:"jobName" bson:"jobName"` // 任务名
	Command string `json:"command" bson:"command"` // 脚本命令
	Err string `json:"err" bson:"err"` // 错误原因
	Output string `json:"output" bson:"output"` // 脚本输出
	PlanTime int64 `json:"planTime" bson:"planTime"` // 计划开始时间
	ScheduleTime int64 `json:"scheduleTime" bson:"scheduleTime"` // 实际调度时间
	StartTime int64 `json:"startTime" bson:"startTime"` // 任务执行开始时间
	EndTime int64 `json:"endTime" bson:"endTime"` // 任务执行结束时间
}

// 日志批次
type LogBatch struct {
	Logs []interface{} // 多条日志
}

// 任务日志过滤条件
type JobLogFilter struct {
	JobName string `bson:"jobName"`
}

// 任务日志排序规则
type SortLogByStartTime struct {
	SortOrder int `bson:"startTime"`  //{"startTime":-1}
}


// 应答方法
func BuildResponse(errno int, msg string, data interface{}) (resp []byte, err error) {
	var (
		response Response
	)
	response = Response{
		Errno:errno,
		Msg:msg,
		Data:data,
	}
	resp, err = json.Marshal(response)
	return

}

// 反序列化job
func Unpack(value []byte) (ret *Job, err error) {
	var (
		job *Job
	)

	job = &Job{}
	if err = json.Unmarshal(value, job); err != nil {
		return
	}
	ret = job

	return
}

// 从etcd的key中获取任务名
func ExtractJobName(jobKey string) (string) {
	return strings.TrimPrefix(jobKey, JOB_SAVE_DIR)
}

// 从etcd的key中获取任务名
func ExtractKillerName(killerKey string) (string) {
	return strings.TrimPrefix(killerKey, JOB_KILLER_DIR)
}

// 从etcd的key中获取任务名
func ExtractWorkerIP(workerKey string) (string) {
	return strings.TrimPrefix(workerKey, JOB_WORKER_DIR)
}

// 任务变化事件有2种：1）更新任务， 2）删除任务
func BuildJobEvent(eventType int, job *Job) (jobEvent *JobEvent) {
	return &JobEvent{
		EventType:eventType,
		Job:job,
	}
}

// 构造任务执行计划
func BuildJobSchedulePlan(job *Job) (jobSchedulePlan *JobSchedulePlan, err error) {
	var (
		expr *cronexpr.Expression
	)
	// 解析cron表达式
	if expr, err = cronexpr.Parse(job.CronExpr); err != nil {
		return
	}
	// 生成任务调度计划
	jobSchedulePlan = &JobSchedulePlan{
		Job:job,
		Expr:expr,
		NextTime:expr.Next(time.Now()),
	}
	return
}

// 构造执行状态信息
func BuildJobExecuteInfo(jobSchedulePlan *JobSchedulePlan) (jobExecuteInfo *JobExecuteInfo){
	jobExecuteInfo = &JobExecuteInfo{
		Job:jobSchedulePlan.Job,
		PlanTime:jobSchedulePlan.NextTime,
		RealTime:time.Now(),
	}
	jobExecuteInfo.CancelCtx, jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())
	return
}
