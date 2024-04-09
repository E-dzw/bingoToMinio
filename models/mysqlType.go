package models

import (
	"fmt"

	"github.com/pingcap/errors"
	gorm_mysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type MasterLogsT struct {
	LogName  string `gorm:"column:Log_name"`
	FileSize string `gorm:"column:File_size"`
}

type MyMysqlConf struct {
	Host     string
	Port     string
	Username string
	Password string
}

func (M MyMysqlConf) GetDB() (*gorm.DB, error) {
	if M.Host == "" {
		return nil, errors.New("host is null")
	}
	if M.Port == "" {
		return nil, errors.New("port is null")
	}
	if M.Username == "" {
		return nil, errors.New("username is null")
	}
	if M.Password == "" {
		return nil, errors.New("password is null")
	}
	MysqlUrl := fmt.Sprintf("%s:%s@tcp(%s:%s)/mysql?charset=utf8mb4&parseTime=True&loc=Local", M.Username, M.Password, M.Host, M.Port)
	DB, err := gorm.Open(gorm_mysql.New(gorm_mysql.Config{
		DSN: MysqlUrl,
	}))
	if err != nil {
		return nil, errors.Annotate(err, "fail to  open mysql DB")
	}
	return DB, nil
}
