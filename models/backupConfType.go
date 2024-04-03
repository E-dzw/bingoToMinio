package models

type BackupConfT struct {
	MyMysqlConf
	MyMinioConf
	ConcurrentNumber int
	TmpPath          string
	MinioPrefix      string
	MinioBucketName  string
	StartBinlog      string
	BackupType       string
}
