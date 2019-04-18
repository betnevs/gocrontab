package main

import (
	"runtime"
	"fmt"
	"flag"
	"time"
	"github.com/betNevS/gocrontab/worker"
)

var (
	confFile string //配置文件路径
)

// 解析命令行参数
func initArgs(){
	// master -config ./master.json
	// master -h
	flag.StringVar(&confFile, "config", "./worker.json", "指定worker.json的路径")
	flag.Parse()
}

// 初始化线程数
func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {

	var (
		err error
	)

	// 初始化命令行参数
	initArgs()

	// 初始化线程
	initEnv()

	// 加载配置
	if err = worker.InitConfig(confFile); err != nil {
		goto ERR
	}

	// 服务注册
	if err = worker.InitRegister(); err != nil {
		goto ERR
	}

	// 启动日志协程
	if err = worker.InitLogSink(); err != nil {
		goto ERR
	}

	// 启动执行器
	if err = worker.InitExecutor(); err != nil {
		goto ERR
	}

	// 启动调度器
	if err = worker.InitScheduler(); err != nil {
		goto ERR
	}

	// 初始化任务管理器
	if err = worker.InitJobMgr(); err != nil {
		goto ERR
	}

	for {
		time.Sleep(1*time.Second)
	}

	return

	ERR:
		fmt.Println(err)

}
