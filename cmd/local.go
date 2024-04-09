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

// localCmd represents the local command
var localCmd = &cobra.Command{
	Use:   "local",
	Short: "backup binlog to local file.",
	Long:  `backup binlog to local file.`,
	Run: func(cmd *cobra.Command, args []string) {
		conf := getLocalBackConf()
		if err := backup.Backup(conf); err != nil {
			global.Slogger.Error(err)
		}
	},
}

func getLocalBackConf() models.BackupConfT {
	backConf := models.BackupConfT{
		MyMysqlConf: models.MyMysqlConf{
			Host:     mysqlHost,
			Port:     strconv.Itoa(mysqlPort),
			Username: mysqlUser,
			Password: mysqlPassword,
		},
		TmpPath:          tmpPath,
		ConcurrentNumber: conbackupNumber,
		BackupType:       "local",
		StartBinlog:      startBinlog,
	}
	return backConf
}
func init() {
	rootCmd.AddCommand(localCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// localCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:

}
