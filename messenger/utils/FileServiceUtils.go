package utils

import (
	"context"
	fileserver "messenger/grpc"
	"time"
)

var FileClient fileserver.FileServerClient

func FileExists(par context.Context, fileId string) (bool, error) {
	ctx, cancel := context.WithTimeout(par, 500*time.Millisecond)
	defer cancel()
	res, err := FileClient.FileExists(ctx, &fileserver.FileExistsRequest{FileId: fileId})
	if err != nil {
		return false, err
	}

	return res.Exists, nil
}
