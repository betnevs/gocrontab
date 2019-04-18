package master

import (
	"io/ioutil"
	"encoding/json"
)

type Config struct {
	ApiPort int `json:"apiPort"`
	ApiReadTimeOut int `json:"apiReadTimeOut"`
	ApiWriteTimeOut int `json:"apiWriteTimeOut"`
	EtcdEndpoints []string `json:"etcdEndpoints"`
	EtcdDialTimeout int `json:"etcdDialTimeout"`
	WebRoot string `json:"webroot"`
	MongodbUri string `json:"mongodbUri"`
	MongodbConnectTimeout int `json:"mongodbConnectTimeout"`
}

var (
	G_config *Config
)

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