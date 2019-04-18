package master

import (
	"net/http"
	"net"
	"time"
	"strconv"
	"github.com/betNevS/gocrontab/common"
	"encoding/json"
)

// 任务的http接口
type ApiServer struct {
	httpServer *http.Server
}

var (
	// 单例对象
	G_apiServer *ApiServer
)

// 保存任务接口
// POST job={"name":"job1", "command":"echo hello", "cronExpr":"* * * * *"}
func handleJobSave(resp http.ResponseWriter, req *http.Request) {
	var (
		err error
		postJob string
		job common.Job
		oldJob *common.Job
		bytes []byte
	)
	// 1、解析POST表单
	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	// 2、取表单的job字段
	postJob = req.PostForm.Get("job")

	// 3、反序列化job
	if json.Unmarshal([]byte(postJob),&job); err != nil {
		goto ERR
	}

	// 4、保存到etcd
	if oldJob, err = G_jobMgr.SaveJob(&job); err != nil {
		goto ERR
	}

	// 5、返回正常的应答
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		resp.Write(bytes)
	}
	return

	ERR:
		// 6、返回错误的应答
		if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
			resp.Write(bytes)
		}
}

// 任务删除接口
func handleJobDelete(resp http.ResponseWriter, req *http.Request) {
	var (
		err error
		name string
		oldJob *common.Job
		bytes []byte
	)
	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	name = req.PostForm.Get("name")

	// 删除任务
	if oldJob, err = G_jobMgr.DeleteJob(name); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		resp.Write(bytes)
	}

	return

	ERR:
		if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
			resp.Write(bytes)
		}
}

// 列举所有crontab任务
func handleJobList(resp http.ResponseWriter, req *http.Request) {
	var (
		jobList []*common.Job
		err error
		bytes []byte
	)

	if jobList, err = G_jobMgr.ListJob(); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResponse(0, "success", jobList); err == nil {
		resp.Write(bytes)
	}

	return

	ERR:
		if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
			resp.Write(bytes)
		}
}

// 强制杀死某个任务
func handleJobKill(resp http.ResponseWriter, req *http.Request) {
	var (
		err error
		name string
		bytes []byte
	)

	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	name = req.PostForm.Get("name")

	//杀死任务
	if err = G_jobMgr.KillJob(name); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResponse(0,"success", nil); err == nil {
		resp.Write(bytes)
	}

	return
	ERR:
		if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
			resp.Write(bytes)
		}
}

// 查询任务日志
func handleJobLog(resp http.ResponseWriter, req *http.Request) {
	var (
		err error
		name string
		skipParam string
		limitParam string
		skip int
		limit int
		bytes []byte
		logArr []*common.JobLog
	)

	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	name = req.Form.Get("name")
	skipParam = req.Form.Get("skip")
	limitParam = req.Form.Get("limit")

	if skip, err = strconv.Atoi(skipParam); err != nil {
		skip = 0
	}

	if limit, err = strconv.Atoi(limitParam); err != nil {
		limit = 20
	}

	if logArr, err = G_logMgr.ListLog(name, skip, limit); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResponse(0,"success", logArr); err == nil {
		resp.Write(bytes)
	}

	return

	ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}

}

func handleWorkerList(resp http.ResponseWriter, req *http.Request) {
	var (
		workerArr []string
		err error
		bytes []byte
	)

	if workerArr, err = G_workerMgr.ListWorkers(); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResponse(0,"success", workerArr); err == nil {
		resp.Write(bytes)
	}

	return

	ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}
}

// 初始化服务
func InitApiServer()(err error){
	var (
		mux *http.ServeMux
		listener net.Listener
		httpServer *http.Server
		staticDir http.Dir
		staticHandler http.Handler
	)

	// 配置路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)
	mux.HandleFunc("/job/log", handleJobLog)
	mux.HandleFunc("/worker/list", handleWorkerList)

	// 静态文件目录
	staticDir = http.Dir(G_config.WebRoot)
	staticHandler = http.FileServer(staticDir)
	mux.Handle("/", staticHandler)

	// 启动TCP监听
	if listener, err = net.Listen("tcp", ":" + strconv.Itoa(G_config.ApiPort)); err != nil {
		return
	}

	// 创建一个Http服务
	httpServer = &http.Server{
		ReadTimeout:time.Duration(G_config.ApiReadTimeOut)*time.Millisecond,
		WriteTimeout:time.Duration(G_config.ApiReadTimeOut)*time.Millisecond,
		Handler:mux,
	}
	// 赋值单例
	G_apiServer = &ApiServer{
		httpServer:httpServer,
	}
	// 启动http服务
	go httpServer.Serve(listener)

	return
}

