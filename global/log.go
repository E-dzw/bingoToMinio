package global

import "go.uber.org/zap"

var Slogger *zap.SugaredLogger

func init() {
	logger, _ := zap.NewDevelopment()
	Slogger = logger.Sugar()
}
