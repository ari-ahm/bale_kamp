package core

import (
	"ConcurrentFileServer/pkg"
	"context"
	"errors"
	"github.com/google/uuid"
	"os"
	"strings"
	"sync"
)

const FilePath string = "/home/arian/codes n stuff/bale_kamp/ConcurrentFileServer/files/"

var fileMimeTypes = make(map[string]string)

type FileHandler interface {
	UploadFile(ctx context.Context, file []byte, mimeType string) (string, error)
	DownloadFile(ctx context.Context, fileID string) ([]byte, string, error)
}

type FileHandlerImpl struct {
	workersCount int
	blockSize    int
}

func NewFileHandlerImpl(workersCount, blockSize int) FileHandler {
	return &FileHandlerImpl{workersCount, blockSize}
}

func FileExists(fileName string) bool {
	_, err := os.Stat(fileName)
	return err == nil
}

func getRandomFileName(file *[]byte, mimeType string) string {
	return strings.ReplaceAll(uuid.NewString(), "-", "")
}

func (f *FileHandlerImpl) UploadFile(ctx context.Context, file []byte, mimeType string) (string, error) {
	var (
		fileName        string
		fileNameRetries = 3
	)
	for fileName = getRandomFileName(&file, mimeType); FileExists(FilePath + fileName); fileName = getRandomFileName(&file, mimeType) {
		if fileNameRetries == 0 {
			return "", errors.New("file name retries exceeded. could not generate uuid for file")
		}
		fileNameRetries--
	}

	writeJob := make(chan error)
	go func() {
		writeJob <- writeFile(f, FilePath+fileName, file)
	}()

	select {
	case err := <-writeJob:
		if err != nil {
			return "", err
		}
		fileMimeTypes[fileName] = mimeType
	case <-ctx.Done():
		return "", errors.New("context canceled")
	}

	return fileName, nil
}

func (f *FileHandlerImpl) DownloadFile(ctx context.Context, fileID string) ([]byte, string, error) {
	if !FileExists(FilePath + fileID) {
		return nil, "", errors.New("file does not exist")
	}

	type readJobRet struct {
		ret []byte
		err error
	}
	readJob := make(chan readJobRet)
	go func() {
		ret, err := readFile(f, FilePath+fileID)
		readJob <- readJobRet{ret, err}
	}()

	select {
	case v := <-readJob:
		if v.err != nil {
			return nil, "", v.err
		}
		return v.ret, fileMimeTypes[fileID], nil
	case <-ctx.Done():
		return nil, "", errors.New("context canceled")
	}
}

func writeFile(f *FileHandlerImpl, fileName string, content []byte) error {
	fout, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer fout.Close()

	err = fout.Truncate(int64(len(content)))
	if err != nil {
		return err
	}

	writeErrors := make([]error, 0)
	pool := pkg.NewWorkerPool(f.workersCount)
	var wg sync.WaitGroup

	for i := 0; i*f.blockSize < len(content); i++ {
		wg.Add(1)
		pool.SubmitJob(func() {
			defer wg.Done()

			_, err = fout.WriteAt(content[i*f.blockSize:min((i+1)*f.blockSize, len(content))], int64(i*f.blockSize))
			if err != nil {
				writeErrors = append(writeErrors, err)
				return
			}
		})
	}

	wg.Wait()
	if len(writeErrors) != 0 {
		return writeErrors[0]
	}
	return nil
}

func readFile(f *FileHandlerImpl, fileName string) ([]byte, error) {
	fin, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer fin.Close()

	fileStat, err := fin.Stat()
	if err != nil {
		return nil, err
	}
	outBuf := make([]byte, fileStat.Size())

	readErrors := make([]error, 0)
	pool := pkg.NewWorkerPool(f.workersCount)
	type workerInput struct {
		bytes    []byte
		startPos int
	}
	workerInputs := make(chan workerInput, 1000)
	var wg sync.WaitGroup

	for i := 0; i*f.blockSize < len(outBuf); i++ {
		workerInputs <- workerInput{outBuf[i*f.blockSize : min((i+1)*f.blockSize, len(outBuf))], i * f.blockSize}
		wg.Add(1)
		pool.SubmitJob(func() {
			defer wg.Done()
			input := <-workerInputs

			_, err = fin.ReadAt(input.bytes, int64(input.startPos))
			if err != nil {
				readErrors = append(readErrors, err)
				return
			}
		})
	}

	wg.Wait()
	if len(readErrors) != 0 {
		return nil, readErrors[0]
	}
	return outBuf, nil
}
