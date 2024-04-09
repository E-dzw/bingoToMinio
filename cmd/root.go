/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bingoToMinio/global"
	"os"

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
		global.Slogger.Info("please use subcommand to backup")
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

var mysqlHost, mysqlUser, mysqlPassword, minioPath, minioBucketName, minioHost, minioAccessKey, minioSecretKey, tmpPath, startBinlog string
var mysqlPort, minioPort, conbackupNumber int

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.bingoToMinio.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	// rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	rootCmd.PersistentFlags().StringVar(&mysqlHost, "mysqlHost", "127.0.0.1", "mysql server address")
	rootCmd.PersistentFlags().IntVar(&mysqlPort, "mysqlPort", 3306, "mysql server port")
	rootCmd.PersistentFlags().StringVar(&mysqlUser, "mysqlUser", "", "mysql account")
	rootCmd.PersistentFlags().StringVar(&mysqlPassword, "mysqlPassword", "", "mysql password")
	rootCmd.PersistentFlags().StringVar(&tmpPath, "tmpPath", "./tmpPath", "define  a temporary local file path to store binlog from mysql server")
	rootCmd.PersistentFlags().IntVar(&conbackupNumber, "conbackupNumber", 1, "how many backup go routine to backup binlog to minio")
	rootCmd.PersistentFlags().StringVar(&startBinlog, "startBinlog", "", "where from to start backup  ")

	rootCmd.MarkPersistentFlagRequired("mysqlUser")
	rootCmd.MarkPersistentFlagRequired("mysqlPassword")
}
