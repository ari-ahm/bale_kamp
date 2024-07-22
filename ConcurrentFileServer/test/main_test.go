package test

import (
	"ConcurrentFileServer/core"
	"ConcurrentFileServer/pkg"
	"ConcurrentFileServer/utils"
	"bytes"
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestUpload(t *testing.T) {
	var (
		data           = []byte("ali")
		mimeType       = "text/plain"
		filesDirectory = "../files"
		handler        = core.NewFileHandlerImpl(1, 1024)
		ctx            = context.Background()
	)

	fileId, err := handler.UploadFile(ctx, data, mimeType)
	assert.Nil(t, err)
	assert.NotEmpty(t, fileId)

	dir, err := os.ReadDir(filesDirectory)
	assert.Nil(t, err)
	assert.Len(t, dir, 1)
	assert.False(t, dir[0].IsDir())
	//expectedFileName := fmt.Sprintf("%s.%s", fileId, utils.GetExtensionByMimeType(mimeType))
	expectedFileName := fileId // edited.
	assert.Equal(t, expectedFileName, dir[0].Name())
	file, err := os.Open(fmt.Sprintf("%s/%s", filesDirectory, dir[0].Name()))
	assert.Nil(t, err)
	buf := make([]byte, len(data))
	read, err := file.Read(buf)
	assert.Nil(t, err)
	assert.Equal(t, len(data), read)
	assert.True(t, bytes.Equal(data, buf))
}

func TestDownload(t *testing.T) {
	var (
		data = []byte("ali")

		//mimeType       = "text/plain"
		filesDirectory = "../files"
		fileId         = "tmp"
		handler        = core.NewFileHandlerImpl(1, 1024)
		ctx            = context.Background()
	)

	//create, err := os.Create(fmt.Sprintf("%s/%s.%s", filesDirectory, fileId, utils.GetExtensionByMimeType(mimeType)))
	create, err := os.Create(fmt.Sprintf("%s/%s", filesDirectory, fileId)) // edited.
	assert.Nil(t, err)

	write, err := create.Write(data)
	assert.Nil(t, err)
	assert.NotEmpty(t, write)

	resultFile, _, err := handler.DownloadFile(ctx, fileId)
	assert.Nil(t, err)
	assert.NotEmpty(t, resultFile)
	//assert.NotEmpty(t, resultMimeType)
	assert.True(t, bytes.Equal(resultFile, data))
}

func TestUploadAndDownloadScenario(t *testing.T) {
	var (
		data     = []byte("ali")
		mimeType = "text/plain"
		handler  = core.NewFileHandlerImpl(1, 1024)
		ctx      = context.Background()
	)

	fileId, err := handler.UploadFile(ctx, data, mimeType)
	assert.Nil(t, err)
	assert.NotEmpty(t, fileId)

	file, downloadMimeType, err := handler.DownloadFile(ctx, fileId)
	assert.Nil(t, err)
	assert.NotEmpty(t, downloadMimeType)
	assert.Equal(t, mimeType, downloadMimeType)
	assert.True(t, bytes.Equal(file, data))
}

func TestUploadAndDownloadConcurrent(t *testing.T) {
	var (
		workerPool = pkg.NewWorkerPool(25)
		handler    = core.NewFileHandlerImpl(1, 1024)
		mimeType   = "text/plain"
	)

	ticker := time.NewTicker(100 * time.Millisecond)
	timer := time.NewTimer(5 * time.Second)

	condition := true
	for condition {
		select {
		case <-timer.C:
			condition = false
		case <-ticker.C:
			workerPool.SubmitJob(func() {
				ctx := context.Background()
				data := []byte(utils.RandStringRunes(1000))
				fileId, err := handler.UploadFile(ctx, data, mimeType)
				assert.Nil(t, err)
				time.Sleep(1 * time.Second)
				file, s, err := handler.DownloadFile(ctx, fileId)
				assert.Nil(t, err)
				assert.NotEmpty(t, s)
				assert.NotEmpty(t, file)
				assert.True(t, bytes.Equal(file, data))
			})
		}
	}
}

func BenchmarkUploadSeq(b *testing.B) {
	handler := core.NewFileHandlerImpl(1, 100000000)
	testBytes := make([]byte, 100000000)
	for i := 0; i < b.N; i++ {
		handler.UploadFile(context.Background(), testBytes, "")
	}
	dir, _ := os.ReadDir("/home/arian/codes n stuff/bale_kamp/ConcurrentFileServer/files/")
	for _, i := range dir {
		os.Remove("/home/arian/codes n stuff/bale_kamp/ConcurrentFileServer/files/" + i.Name())
	}
}

func BenchmarkUploadGorut(b *testing.B) {
	handler := core.NewFileHandlerImpl(40, 2500000)
	testBytes := make([]byte, 100000000)
	for i := 0; i < b.N; i++ {
		handler.UploadFile(context.Background(), testBytes, "")
	}
	dir, _ := os.ReadDir("/home/arian/codes n stuff/bale_kamp/ConcurrentFileServer/files/")
	for _, i := range dir {
		os.Remove("/home/arian/codes n stuff/bale_kamp/ConcurrentFileServer/files/" + i.Name())
	}
}

func BenchmarkDownloadSeq(b *testing.B) {
	handler := core.NewFileHandlerImpl(1, 100000000)
	testBytes := make([]byte, 100000000)
	fileId, _ := handler.UploadFile(context.Background(), testBytes, "")
	for i := 0; i < b.N; i++ {
		handler.DownloadFile(context.Background(), fileId)
	}
	dir, _ := os.ReadDir("/home/arian/codes n stuff/bale_kamp/ConcurrentFileServer/files/")
	for _, i := range dir {
		os.Remove("/home/arian/codes n stuff/bale_kamp/ConcurrentFileServer/files/" + i.Name())
	}
}

func BenchmarkDownloadGorut(b *testing.B) {
	handler := core.NewFileHandlerImpl(40, 2500000)
	testBytes := make([]byte, 100000000)
	fileId, _ := handler.UploadFile(context.Background(), testBytes, "")
	for i := 0; i < b.N; i++ {
		handler.DownloadFile(context.Background(), fileId)
	}
	dir, _ := os.ReadDir("/home/arian/codes n stuff/bale_kamp/ConcurrentFileServer/files/")
	for _, i := range dir {
		os.Remove("/home/arian/codes n stuff/bale_kamp/ConcurrentFileServer/files/" + i.Name())
	}
}
