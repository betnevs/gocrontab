package worker

import (
	"github.com/betNevS/gocrontab/common"
	"time"
	"fmt"
)

type Scheduler struct {
	jobEventChan chan *common.JobEvent // etcd 任务事件队列
	jobPlanTable map[string]*common.JobSchedulePlan // 任务调度计划表
	jobExecutingTable map[string]*common.JobExecuteInfo // 任务执行表
	jobResultChan chan *common.JobExecuteResult // 任务结果队列
}

var (
	G_scheduler *Scheduler
)

// 处理任务事件
func (scheduler *Scheduler) handleJobEvent(jobEvent *common.JobEvent) {

	var (
		jobSchedulePlan *common.JobSchedulePlan
		jobExisted bool
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting bool
		err error
	)

	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE:
		if jobSchedulePlan, err = common.BuildJobSchedulePlan(jobEvent.Job); err != nil {
			return
		}
		scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan

	case common.JOB_EVENT_DELETE:
		if jobSchedulePlan, jobExisted = scheduler.jobPlanTable[jobEvent.Job.Name]; jobExisted{
			delete(scheduler.jobPlanTable, jobEvent.Job.Name)
		}
	case common.JOB_EVENT_KILL:
		// 取消commond的执行
		if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobEvent.Job.Name]; jobExecuting {
			jobExecuteInfo.CancelFunc() // 触发command杀死shell子进程
		}

	}
}

// 处理任务结果
func (scheduler *Scheduler) handleJobResult(result *common.JobExecuteResult) {
	var (
		jobLog *common.JobLog
	)

	// 删除执行状态
	delete(scheduler.jobExecutingTable, result.ExecuteInfo.Job.Name)

	// 生成执行日志
	if result.Err != common.ERR_LOCK_ALREADY_REQUIRED {
		jobLog = &common.JobLog{
			JobName:result.ExecuteInfo.Job.Name,
			Command:result.ExecuteInfo.Job.Command,
			Output:string(result.Output),
			PlanTime:result.ExecuteInfo.PlanTime.UnixNano()/1000/1000,
			ScheduleTime:result.ExecuteInfo.RealTime.UnixNano()/1000/1000,
			StartTime:result.StartTime.UnixNano()/1000/1000,
			EndTime:result.EndTime.UnixNano()/1000/1000,
		}
		// 处理错误信息的格式
		if result.Err != nil {
			jobLog.Err = result.Err.Error()
		} else {
			jobLog.Err = ""
		}
		G_LogSink.Append(jobLog)
	}
	fmt.Println("任务执行完成,",result.ExecuteInfo.Job.Name, "|", string(result.Output), "出现错误：",result.Err)
}


// 尝试执行任务
func (scheduler *Scheduler) TryStartJob(jobPlan *common.JobSchedulePlan) {
	var (
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting bool
	)

	// 如果任务正在执行，则跳过本次
	if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobPlan.Job.Name]; jobExecuting {
		fmt.Println("任务正在执行,跳过：", jobPlan.Job.Name)
		return
	}

	// 构建执行状态信息
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)

	// 保存执行状态
	scheduler.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo

	// 执行任务
	fmt.Println("开始执行任务:",jobExecuteInfo.Job.Name, jobExecuteInfo.PlanTime, jobExecuteInfo.RealTime)
	G_executor.ExecuteJob(jobExecuteInfo)

}

// 重新计算任务调度状态
func (scheduler *Scheduler) TrySchedule() (scheduleAfter time.Duration) {
	var (
		jobPlan *common.JobSchedulePlan
		now time.Time
		nearTime *time.Time
	)
	// 如果任务表为空，则睡眠1s
	if len(scheduler.jobPlanTable) == 0 {
		scheduleAfter = 1 * time.Second
		return
	}

	// 计算当前时间
	now = time.Now()

	// 1、遍历所有任务
	for _, jobPlan = range scheduler.jobPlanTable {
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			// 尝试执行任务
			scheduler.TryStartJob(jobPlan)
			jobPlan.NextTime = jobPlan.Expr.Next(now) // 更新下次执行时间
		}

		// 统计最近一个过期时间的任务
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime) {
			nearTime = & jobPlan.NextTime
		}

	}

	// 下次调度间隔（最近要执行的任务时间 - 当前时间）
	scheduleAfter = (*nearTime).Sub(now)
	return
}


// 调度协程
func (scheduler *Scheduler) scheduleLoop() {

	var (
		jobEvent *common.JobEvent
		scheduleAfter time.Duration
		scheduleTimer *time.Timer
		jobResult *common.JobExecuteResult
	)

	// 初始化一次(1s)
	scheduleAfter = scheduler.TrySchedule()

	// 调度的延时定时器
	scheduleTimer = time.NewTimer(scheduleAfter)

	// 定时任务
	for {
		select {
		case jobEvent = <-scheduler.jobEventChan: // 监听事件变化
			scheduler.handleJobEvent(jobEvent)
		case <-scheduleTimer.C:
		case jobResult = <-scheduler.jobResultChan: // 监听任务结果
			scheduler.handleJobResult(jobResult)
		}
		// 调度一次任务
		scheduleAfter = scheduler.TrySchedule()
		// 重置调度间隔
		scheduleTimer.Reset(scheduleAfter)
	}


}

// 推送任务变化事件
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}


// 回传任务执行结果
func (scheduler *Scheduler) PushJobResult(jobResult *common.JobExecuteResult) {
	scheduler.jobResultChan <- jobResult
}



// 初始化调度协程
func InitScheduler() (err error){
	G_scheduler = &Scheduler{
		jobEventChan:make(chan *common.JobEvent, 1000),
		jobPlanTable:make(map[string]*common.JobSchedulePlan),
		jobExecutingTable:make(map[string]*common.JobExecuteInfo),
		jobResultChan:make(chan *common.JobExecuteResult, 1000),
	}

	// 启动调度协程
	go G_scheduler.scheduleLoop()

	return
}