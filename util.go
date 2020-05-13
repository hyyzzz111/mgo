package mgo

import (
	"context"
	"io"
)

type CtxCloser interface {
	Close(ctx context.Context) error
}
func CtxClose(c CtxCloser, ctx context.Context) {
	err := c.Close(ctx)
	if err != nil {
		//logutil.WithError(err).Error("关闭资源文件失败。")
	}
}
func Close(c io.Closer) {
	err := c.Close()
	if err != nil {
		//logutil.WithError(err).Error("关闭资源文件失败。")
	}
}