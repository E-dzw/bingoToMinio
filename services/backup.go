package backup

import (
	"bingoToMinio/global"
	"bingoToMinio/models"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/minio/minio-go/v7"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var logger *zap.SugaredLogger = global.Slogger

const (
	localProcessionFileName = "backup.record"
)

func Backup(backConf models.BackupConfT) error {
	switch backConf.BackupType {
	case "local":
		return backupToLocalFile(backConf)
	case "minio":
		return backupToMinio(backConf)
	default:
		return errors.New("Unsupport backup type")
	}
}

func backupToMinio(backConf models.BackupConfT) error {
	err := makeTmpDir(backConf.TmpPath)
	if err != nil {
		return errors.Trace(err)
	}
	var p mysql.Position
	minioClient, err := backConf.NewClient()
	if err != nil {
		return errors.Trace(err)
	}
	tx, err := backConf.GetDB()
	if err != nil {
		return errors.Trace(err)
	}
	BinsyncConf, err := newBinlogSyncerConf(backConf.MyMysqlConf)
	if err != nil {
		return errors.New(fmt.Sprintf("failed to init binlogSyncerConf.error: %s", err.Error()))
	}
	BinSyncer := replication.NewBinlogSyncer(BinsyncConf)
	masterLogs := getMasterBinlogs(tx)
	if len(masterLogs) == 0 {
		return errors.New("first binlog  is not found")
	}
	lastBinlogName, err := isfirstlyBackupToMinio(context.Background(), masterLogs, minioClient, backConf.MinioPrefix, backConf.MinioBucketName)
	if err != nil {
		return errors.Trace(err)
	}
	p = mysql.Position{
		Name: lastBinlogName,
		Pos:  uint32(4),
	}
	BinStream, err := BinSyncer.StartSync(p)
	if err != nil {
		return errors.Annotate(err, "failed to start sync.")
	}
	var timeOut time.Duration = 3600 * time.Second
	var fileName string
	var osFile *os.File
	fileNameChan := make(chan string, backConf.ConcurrentNumber)
	var wg *sync.WaitGroup = &sync.WaitGroup{}
	wg.Add(backConf.ConcurrentNumber)
	for i := 0; i < backConf.ConcurrentNumber; i++ {
		go uploadMinio(minioClient, fileNameChan, wg, backConf.TmpPath, backConf.MinioBucketName, backConf.MinioPrefix)
	}
	defer wg.Wait()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), timeOut)
		Bevent, err := BinStream.GetEvent(ctx)
		cancel()
		if err == context.DeadlineExceeded {
			errors.New("timeout to get binlog event")
		}
		if err != nil {
			errors.New("failed to get binlog event")
		}

		offset := Bevent.Header.LogPos
		if Bevent.Header.EventType == replication.ROTATE_EVENT {
			rotateEvent := Bevent.Event.(*replication.RotateEvent)
			fileName = string(rotateEvent.NextLogName)
			if offset == 0 || Bevent.Header.Timestamp == 0 {
				continue
			}
		} else if Bevent.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
			if osFile != nil {
				osFile.Close()
				fileNameChan <- filepath.Base(osFile.Name())
			}
			if len(fileName) == 0 {
				return errors.New("can not get filename from binlog event")
			}
			osFile, err = os.OpenFile(filepath.Join(backConf.TmpPath, fileName), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
			if err != nil {
				return errors.New(fmt.Sprintf("failed to create file. filenpath:%s", filepath.Join(backConf.TmpPath, fileName)))
			}
			_, err = osFile.Write(replication.BinLogFileHeader)
			if err != nil {
				return errors.Trace(err)
			}

		}
		if n, err := osFile.Write(Bevent.RawData); err != nil {
			return errors.Trace(err)
		} else if n != len(Bevent.RawData) {
			return errors.Trace(io.ErrShortWrite)
		}

	}
}

func uploadMinio(minioClient *minio.Client, fileNameChan chan string, wg *sync.WaitGroup, fPath, bucketName, minioPrefix string) {
	var tmpPrefix string
	minioPrefix = strings.Trim(minioPrefix, "/")
	for {
		fileName, ok := <-fileNameChan
		if !ok {
			wg.Done()
		}
		if len(minioPrefix) == 0 {
			tmpPrefix = fileName
		} else {
			tmpPrefix = fmt.Sprintf("%s/%s", minioPrefix, fileName)
		}

		_, err := minioClient.FPutObject(context.Background(), bucketName, tmpPrefix, filepath.Join(fPath, fileName), minio.PutObjectOptions{})
		if err != nil {
			logger.Errorf("failed to upload file. err: %s\n", err.Error())
			return
		}
		if err := os.Remove(filepath.Join(fPath, fileName)); err != nil {
			logger.Errorf("failed to remove file. err: %s\n", err.Error())
		}
	}

}

func makeTmpDir(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0755); err != nil {
			return errors.New(fmt.Sprintf("failed to make tmp dir. err: %s\n", err.Error()))
		}
	}
	return nil
}
func isInMasterLogs(binlogName string, masterBinlogs []models.MasterLogsT) bool {
	for _, v := range masterBinlogs {
		if v.LogName == binlogName {
			return true
		}
	}
	return false
}

func isfirstlyBackupToMinio(ctx context.Context, masterLogs []models.MasterLogsT, minioClient *minio.Client, prefix, bucketName string) (string, error) {
	if bool, err := minioClient.BucketExists(ctx, bucketName); !(bool && err == nil) {
		return "", errors.Trace(err)
	}
	var flagTime time.Time
	var lastBinlogName string
	flagTime, _ = time.Parse("2006-01-02 15:04:05", "2006-01-02 15:04:05")
	prefix = strings.Trim(prefix, "/")
	prefix = prefix + "/"
	minioObjectsChan := minioClient.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false,
	})
	for object := range minioObjectsChan {
		if object.LastModified.After(flagTime) {
			flagTime = object.LastModified
			lastBinlogName = strings.TrimLeft(object.Key, prefix)
		}
	}
	if len(lastBinlogName) == 0 {
		return masterLogs[0].LogName, nil
	}
	for k, v := range masterLogs {
		if v.LogName == lastBinlogName {
			if k == len(masterLogs)-1 {
				return v.LogName, nil
			}
			return masterLogs[k+1].LogName, nil
		}
	}
	return masterLogs[0].LogName, nil
}
func backupToLocalFile(backConf models.BackupConfT) error {
	err := makeTmpDir(backConf.TmpPath)
	if err != nil {
		return errors.Trace(err)
	}
	tx, err := backConf.GetDB()
	if err != nil {
		return errors.Annotate(err, "failed to init db")
	}
	BinsyncConf, err := newBinlogSyncerConf(backConf.MyMysqlConf)
	if err != nil {
		return errors.New(fmt.Sprintf("failed to init binlogSyncerConf.error: %s", err.Error()))
	}
	BinSyncer := replication.NewBinlogSyncer(BinsyncConf)
	masterLogs := getMasterBinlogs(tx)
	if len(masterLogs) == 0 {
		return errors.New("first binlog  is not found")
	}
	startBinlogName, err := isFirstlyBackupToLocalFile(masterLogs, backConf.TmpPath)
	if err != nil {
		return errors.New(fmt.Sprintf("failed to get start  binlog.error: %s", err.Error()))
	}
	p := mysql.Position{
		Name: startBinlogName,
		Pos:  uint32(4),
	}
	BinStream, err := BinSyncer.StartSync(p)
	if err != nil {
		return errors.New(fmt.Sprintf("failed to start sync.error: %s", err.Error()))
	}
	var timeOut time.Duration = 3600 * time.Second
	var fileName string
	var osFile, recordFile *os.File
	recordFile, _ = os.OpenFile(filepath.Join(backConf.TmpPath, localProcessionFileName), os.O_WRONLY, 0644)
	defer func() {
		if osFile != nil {
			osFile.Close()
		}
		if recordFile != nil {
			recordFile.Close()
		}
	}()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), timeOut)
		Bevent, err := BinStream.GetEvent(ctx)
		cancel()
		if err == context.DeadlineExceeded {
			return errors.New("timeout to get binlog event")
		}
		if err != nil {
			return errors.New("failed to get binlog event")
		}

		offset := Bevent.Header.LogPos
		if Bevent.Header.EventType == replication.ROTATE_EVENT {
			rotateEvent := Bevent.Event.(*replication.RotateEvent)
			fileName = string(rotateEvent.NextLogName)
			if offset == 0 || Bevent.Header.Timestamp == 0 {
				continue
			}
		} else if Bevent.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
			if osFile != nil {
				osFile.Close()
			}
			// if fileName == masterLogs[1].LogName {
			// 	return errors.New(fmt.Sprintf("finish sync fist binlog.binlogName: %s", masterLogs[0].LogName))
			// }

			if len(fileName) == 0 {
				return errors.New("can not get filename from binlog event")
			}
			recordFile.Truncate(0)
			recordFile.Seek(0, 0)
			_, err := recordFile.Write([]byte(fileName))
			if err != nil {
				return errors.Annotate(err, "failed to record binlog name.")
			}
			osFile, err = os.OpenFile(filepath.Join(backConf.TmpPath, fileName), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
			if err != nil {
				return errors.New(fmt.Sprintf("failed to create file. filenpath:%s", filepath.Join(backConf.TmpPath, fileName)))
			}
			_, err = osFile.Write(replication.BinLogFileHeader)
			if err != nil {
				return errors.Trace(err)
			}

		}
		if n, err := osFile.Write(Bevent.RawData); err != nil {
			return errors.Trace(err)
		} else if n != len(Bevent.RawData) {
			return errors.Trace(io.ErrShortWrite)
		}

	}
}

func isFirstlyBackupToLocalFile(masterLogs []models.MasterLogsT, path string) (string, error) {

	recordF, err := os.OpenFile(filepath.Join(path, localProcessionFileName), os.O_RDWR, 0644)
	if err != nil && os.IsNotExist(err) {
		f, interErr := os.OpenFile(filepath.Join(path, localProcessionFileName), os.O_CREATE|os.O_WRONLY, 0644)
		if interErr != nil {
			return "", errors.Trace(interErr)
		}
		defer f.Close()
		_, interErr = f.Write([]byte(masterLogs[0].LogName))
		if interErr != nil {
			return "", errors.Trace(interErr)
		}
		return masterLogs[0].LogName, nil
	}
	if err != nil {
		errors.Trace(err)
	}
	defer recordF.Close()
	recordBinlogName, err := io.ReadAll(recordF)
	if err != nil {
		errors.Trace(err)
	}
	recordBinlogName = bytes.Trim(recordBinlogName, "\n")
	recordBinlogName = bytes.TrimSpace(recordBinlogName)
	if len(recordBinlogName) == 0 {
		logger.Warnf("%s is empty", string(recordBinlogName))
		return string(recordBinlogName), nil
	}
	if !isInMasterLogs(string(recordBinlogName), masterLogs) {
		logger.Warnf("%s is not in master logs,start to backup binlog from first in current master logs", string(recordBinlogName))
	}
	return string(recordBinlogName), nil

}

func newBinlogSyncerConf(cfg models.MyMysqlConf) (replication.BinlogSyncerConfig, error) {
	Port16, _ := strconv.ParseUint(cfg.Port, 10, 16)
	return replication.BinlogSyncerConfig{
		ServerID:       65536,
		Host:           cfg.Host,
		Port:           uint16(Port16),
		User:           cfg.Username,
		Password:       cfg.Password,
		RawModeEnabled: true,
	}, nil
}

func getMasterBinlogs(tx *gorm.DB) []models.MasterLogsT {
	var masterLogs []models.MasterLogsT
	tx.Raw("show master logs").Scan(&masterLogs)
	return masterLogs
}
