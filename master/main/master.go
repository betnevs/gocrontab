package main

import (
	"runtime"
	"github.com/betNevS/gocrontab/master"
	"fmt"
	"flag"
	"time"
)

var (
	confFile string //配置文件路径
)

// 解析命令行参数
func initArgs(){
	// master -config ./master.json
	// master -h
	flag.StringVar(&confFile, "config", "./master.json", "指定master.json的路径")
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
	if err = master.InitConfig(confFile); err != nil {
		goto ERR
	}

	// 初始化服务发现模块
	if err = master.InitWorkerMgr(); err != nil {
		goto ERR
	}

	// 日志管理器
	if err = master.InitLogMgr(); err != nil {
		goto ERR
	}

	// 任务管理器
	if err = master.InitJobMgr(); err != nil {
		goto ERR
	}

	// 启动Api Http服务
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}

	for {
		time.Sleep(1*time.Second)
	}

	return

	ERR:
		fmt.Println(err)

}
