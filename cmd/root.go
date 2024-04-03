/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bingoToMinio/global"
	"bingoToMinio/models"
	backup "bingoToMinio/services"
	"os"
	"strconv"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "bingoToMinio",
	Short: "backup binlog to minio or local file",
	Long:  `bingoToMinio is a cli library  to backuo binlog to minio or local file.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		if err := backup.Backup(backConf); err != nil {
			global.Slogger.Error(err)
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

var backConf models.BackupConfT

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.bingoToMinio.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	mysqlHost := rootCmd.Flags().String("mysqlHost", "127.0.0.1", "mysql server address")
	mysqlPort := rootCmd.Flags().Int("mysqlPort", 3306, "mysql server port")
	mysqlUser := rootCmd.Flags().String("mysqlUser", "", "mysql account")
	mysqlPassword := rootCmd.Flags().String("mysqlPassword", "", "mysql password")
	minioPath := rootCmd.Flags().String("minioPath", "/", "define a minio path to store binlog")
	minioBucketName := rootCmd.Flags().String("minioBucketName", "", "minio bucket name ")
	minioHost := rootCmd.Flags().String("minioHost", "127.0.0.1", "minio address")
	minioPort := rootCmd.Flags().Int("minioPort", 9000, "minio s3 port")
	minioAccessKey := rootCmd.Flags().String("minioAccessKey", "", "minio AccessKey")
	minioSecretKey := rootCmd.Flags().String("minioSecretKey", "", "minio SecretKey")
	tmpPath := rootCmd.Flags().String("tmpPath", "./tmpPath", "define  a temporary local file path to store binlog from mysql server")
	conbackupNumber := rootCmd.Flags().Int("conbackupNumber", 1, "how many backup go routine to backup binlog to minio")
	startBinlog := rootCmd.Flags().String("startBinlog", "", "where from to start backup  ")
	backupType := rootCmd.Flags().String("backupType", "local", "the type of backup binglog.local: binlogs are stored in tmpPath dir.minio: binlogs are stored in minio storage")

	rootCmd.MarkFlagsRequiredTogether([]string{"mysqlUser", "mysqlPassword", "minioBucketName", "minioAccessKey", "minioSecretKey"}...)
	backConf = models.BackupConfT{
		MyMysqlConf: models.MyMysqlConf{
			Host:     *mysqlHost,
			Port:     strconv.Itoa(*mysqlPort),
			Username: *mysqlUser,
			Password: *mysqlPassword,
		},
		MyMinioConf: models.MyMinioConf{
			Endpoint:  *minioHost + ":" + strconv.Itoa(*minioPort),
			AccessKey: *minioAccessKey,
			SecretKey: *minioSecretKey,
		},
		MinioPrefix:      *minioPath,
		MinioBucketName:  *minioBucketName,
		TmpPath:          *tmpPath,
		ConcurrentNumber: *conbackupNumber,
		BackupType:       *backupType,
		StartBinlog:      *startBinlog,
	}

}
