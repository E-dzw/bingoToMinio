package backup

import (
	"bingoToMinio/global"
	"bingoToMinio/models"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
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
	lastBinlogName, err := isfirstlyBackupToMinio(context.Background(), minioClient, backConf.MinioPrefix, backConf.MinioBucketName)
	if err != nil {
		return errors.Trace(err)
	}
	if isInMasterLogs(lastBinlogName, masterLogs) {
		p = mysql.Position{
			Name: lastBinlogName,
			Pos:  uint32(4),
		}
	} else {
		p = mysql.Position{
			Name: masterLogs[0].LogName,
			Pos:  uint32(4),
		}
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
		go uploadMinio(minioClient, fileNameChan, wg, backConf.TmpPath, backConf.MinioBucketName)
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
			osFile, err = os.OpenFile(filepath.Join(backConf.TmpPath, fileName), os.O_CREATE|os.O_WRONLY, 0644)
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

func uploadMinio(minioClient *minio.Client, fileNameChan chan string, wg *sync.WaitGroup, fPath, bucketName string) {
	for {
		fileName, ok := <-fileNameChan
		if !ok {
			wg.Done()
		}
		_, err := minioClient.FPutObject(context.Background(), bucketName, fileName, filepath.Join(fPath, fileName), minio.PutObjectOptions{})
		if err != nil {
			logger.Errorf("failed to upload file. err: %s\n", err.Error())
			return
		}
		if err := os.Remove(filepath.Join(fPath, fileName)); err != nil {
			logger.Errorf("failed to remove file. err: %s\n", err.Error())
		}
	}

}

func isInMasterLogs(binlogName string, masterBinlogs []models.MasterLogsT) bool {
	for _, v := range masterBinlogs {
		if v.LogName == binlogName {
			return true
		}
	}
	return false
}

func isfirstlyBackupToMinio(ctx context.Context, minioClient *minio.Client, prefix, bucketName string) (string, error) {
	if bool, err := minioClient.BucketExists(ctx, bucketName); !(bool && err == nil) {
		return "", errors.Trace(err)
	}
	var flagTime time.Time
	var lastBinlogName string
	flagTime, _ = time.Parse("2006-01-02 15:04:05", "2006-01-02 15:04:05")
	minioObjectsChan := minioClient.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false,
	})
	for object := range minioObjectsChan {
		if object.LastModified.After(flagTime) {
			flagTime = object.LastModified
			lastBinlogName = object.Key
		}
	}
	if len(lastBinlogName) == 0 {
		return "", nil
	}
	return lastBinlogName, nil
}
func backupToLocalFile(backConf models.BackupConfT) error {
	tx, err := backConf.GetDB()
	if err != nil {
		return errors.New("failed to init db")
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
	p := mysql.Position{
		Name: masterLogs[0].LogName,
		Pos:  uint32(4),
	}
	BinStream, err := BinSyncer.StartSync(p)
	if err != nil {
		return errors.New(fmt.Sprintf("failed to start sync.error: %s", err.Error()))
	}
	var timeOut time.Duration = 3600 * time.Second
	var fileName string
	var osFile *os.File
	defer func() {
		if osFile != nil {
			osFile.Close()
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
			if fileName == masterLogs[1].LogName {
				return errors.New(fmt.Sprintf("finish sync fist binlog.binlogName: %s", masterLogs[0].LogName))
			}

			if len(fileName) == 0 {
				return errors.New("can not get filename from binlog event")
			}
			osFile, err = os.OpenFile(filepath.Join(backConf.TmpPath, fileName), os.O_CREATE|os.O_WRONLY, 0644)
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
