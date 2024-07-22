package main

import (
	"ConcurrentFileServer/core"
	fileserver "ConcurrentFileServer/grpc"
	"context"
	"encoding/json"
	"errors"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
	"net/http"
)

type uploadFileRequest struct {
	File string `json:"file"`
}

type uploadFailureResponse struct {
	Error string `json:"error"`
}

type uploadSuccessResponse struct {
	FileID string `json:"file_id"`
}

type downloadFileRequest struct {
	FileID string `json:"file_id"`
}

type downloadFailureResponse struct {
	Error string `json:"error"`
}

var service = core.NewFileHandlerImpl(10, 2500000)

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	err := r.ParseMultipartForm(64000000)

	var (
		mimeType string
		file     []byte
	)

	retErr := func(err error, status int) {
		w.WriteHeader(status)
		body, _ := json.Marshal(uploadFailureResponse{err.Error()})
		w.Write(body)
	}

	if err == nil {
		if _, ok := r.MultipartForm.File["file"]; !ok {
			retErr(errors.New("'file' is not present in form data"), 400)
			return
		}
		reqFile, err := r.MultipartForm.File["file"][0].Open()
		if err != nil {
			retErr(err, 400)
			return
		}
		file, err = io.ReadAll(reqFile)
		if err != nil {
			retErr(err, 400)
			return
		}
	} else {
		reqBody, err := io.ReadAll(r.Body)
		if err != nil {
			retErr(err, 500)
			return
		}

		var jsonReq uploadFileRequest
		err = json.Unmarshal(reqBody, &jsonReq)
		if err != nil {
			retErr(err, 400)
			return
		}
		fileResp, err := http.Get(jsonReq.File)
		if err != nil {
			retErr(err, 500)
			return
		}
		file, err = io.ReadAll(fileResp.Body)
		if err != nil {
			retErr(err, 500)
			return
		}
	}
	mimeType = http.DetectContentType(file)
	fileId, err := service.UploadFile(r.Context(), file, mimeType)
	if err != nil {
		retErr(err, 400)
		return
	}
	w.WriteHeader(200)
	body, err := json.Marshal(uploadSuccessResponse{fileId})
	if err != nil {
		retErr(err, 500)
		return
	}
	w.Write(body)
}

func downloadHandler(w http.ResponseWriter, r *http.Request) {
	err := r.ParseMultipartForm(10000)

	var fileId string

	retErr := func(err error, status int) {
		w.WriteHeader(status)
		body, _ := json.Marshal(downloadFailureResponse{err.Error()})
		w.Write(body)
	}

	if err == nil {
		fileIdList, ok := r.MultipartForm.Value["file_id"]
		if !ok {
			retErr(errors.New("'file_id' is not present in form data"), 400)
			return
		}
		fileId = fileIdList[0]
	} else {
		reqBody, err := io.ReadAll(r.Body)
		if err != nil {
			retErr(err, 500)
			return
		}

		var jsonReq downloadFileRequest
		err = json.Unmarshal(reqBody, &jsonReq)
		if err != nil {
			retErr(err, 400)
		}
		fileId = jsonReq.FileID
	}
	body, mimeType, err := service.DownloadFile(r.Context(), fileId)
	if err != nil {
		retErr(err, 500)
		return
	}
	w.Header().Add("content-type", mimeType)
	w.WriteHeader(200)
	w.Write(body)
}

type fileserverImpl struct {
	fileserver.UnimplementedFileServerServer
}

func (f *fileserverImpl) FileExists(ctx context.Context, request *fileserver.FileExistsRequest) (*fileserver.FileExistsResponse, error) {
	return &fileserver.FileExistsResponse{Exists: core.FileExists(core.FilePath + request.GetFileId())}, nil
}

func main() {
	go func() {
		lis, err := net.Listen("tcp", ":8090")
		if err != nil {
			log.Fatal(err)
		}
		grpcServer := grpc.NewServer()
		fileserver.RegisterFileServerServer(grpcServer, &fileserverImpl{})
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()

	http.HandleFunc("/upload", uploadHandler)
	http.HandleFunc("/download", downloadHandler)
	http.ListenAndServe("0.0.0.0:8070", nil)
}
