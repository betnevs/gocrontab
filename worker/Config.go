package worker

import (
	"io/ioutil"
	"encoding/json"
)

var (
	G_config *Config
)

type Config struct {
	EtcdEndpoints []string `json:"etcdEndpoints"`
	EtcdDialTimeout int `json:"etcdDialTimeout"`
	MongodbUri string `json:"mongodbUri"`
	MongodbConnectTimeout int `json:"mongodbConnectTimeout"`
	JobLogBatchSize int `json:"jobLogBatchSize"`
	JobLogCommitTimeout int `json:"jobLogCommitTimeout"`
}

func InitConfig(filename string) (err error) {
	var (
		content []byte
		conf Config
	)

	// 1、读取配置文件
	if content, err = ioutil.ReadFile(filename); err != nil {
		return
	}
	// 2、做Json反序列化
	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}
	// 3、赋值单例
	G_config = &conf
	return
}