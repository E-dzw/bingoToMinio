/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bingoToMinio/global"
	"bingoToMinio/models"
	backup "bingoToMinio/services"
	"strconv"

	"github.com/spf13/cobra"
)

// minioCmd represents the minio command
var minioCmd = &cobra.Command{
	Use:   "minio",
	Short: "backup binlog to minio",
	Long:  `backup binlog to minio`,
	Run: func(cmd *cobra.Command, args []string) {
		conf := getMinioBackConf()
		if err := backup.Backup(conf); err != nil {
			global.Slogger.Error(err)
		}
	},
}

func getMinioBackConf() models.BackupConfT {
	backConf := models.BackupConfT{
		MyMysqlConf: models.MyMysqlConf{
			Host:     mysqlHost,
			Port:     strconv.Itoa(mysqlPort),
			Username: mysqlUser,
			Password: mysqlPassword,
		},
		MyMinioConf: models.MyMinioConf{
			Endpoint:  minioHost + ":" + strconv.Itoa(minioPort),
			AccessKey: minioAccessKey,
			SecretKey: minioSecretKey,
		},
		MinioPrefix:      minioPath,
		MinioBucketName:  minioBucketName,
		TmpPath:          tmpPath,
		ConcurrentNumber: conbackupNumber,
		BackupType:       "minio",
		StartBinlog:      startBinlog,
	}
	return backConf
}

func init() {
	rootCmd.AddCommand(minioCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// minioCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// minioCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	// mysqlHost = minioCmd.Flags().String("mysqlHost", "127.0.0.1", "mysql server address")
	// mysqlPort = minioCmd.Flags().Int("mysqlPort", 3306, "mysql server port")
	// mysqlUser = minioCmd.Flags().String("mysqlUser", "", "mysql account")
	// mysqlPassword = minioCmd.Flags().String("mysqlPassword", "", "mysql password")
	minioCmd.Flags().StringVar(&minioPath, "minioPath", "/", "define a minio path to store binlog")
	minioCmd.Flags().StringVar(&minioBucketName, "minioBucketName", "", "minio bucket name ")
	minioCmd.Flags().StringVar(&minioHost, "minioHost", "127.0.0.1", "minio address")
	minioCmd.Flags().IntVar(&minioPort, "minioPort", 9000, "minio s3 port")
	minioCmd.Flags().StringVar(&minioAccessKey, "minioAccessKey", "", "minio AccessKey")
	minioCmd.Flags().StringVar(&minioSecretKey, "minioSecretKey", "", "minio SecretKey")
	// tmpPath = minioCmd.Flags().String("tmpPath", "./tmpPath", "define  a temporary local file path to store binlog from mysql server")
	// conbackupNumber = minioCmd.Flags().Int("conbackupNumber", 1, "how many backup go routine to backup binlog to minio")
	// startBinlog = minioCmd.Flags().String("startBinlog", "", "where from to start backup  ")
	minioCmd.MarkFlagRequired("minioAccessKey")
	minioCmd.MarkFlagRequired("minioSecretKey")
	minioCmd.MarkFlagRequired("minioBucketName")
	minioCmd.MarkFlagRequired("mysqlUser")
	minioCmd.MarkFlagRequired("mysqlPassword")
}
