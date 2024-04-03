package models

import (
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MyMinioConf struct {
	Endpoint  string
	AccessKey string
	SecretKey string
}

func (M MyMinioConf) NewClient() (*minio.Client, error) {
	return minio.New(M.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(M.AccessKey, M.SecretKey, ""),
		Secure: false,
	})
}

func (M MyMinioConf) NewMinioCore() (*minio.Core, error) {
	return minio.NewCore(
		M.Endpoint,
		&minio.Options{
			Creds:  credentials.NewStaticV4(M.AccessKey, M.SecretKey, ""),
			Secure: false,
		},
	)
}
