package utils

import (
	"context"
	fileserver "messenger/grpc"
	"time"
)

var FileClient fileserver.FileServerClient

func FileExists(fileId string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	res, err := FileClient.FileExists(ctx, &fileserver.FileExistsRequest{FileId: fileId})
	if err != nil {
		return false, err
	}

	return res.Exists, nil
}
