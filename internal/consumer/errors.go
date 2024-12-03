package consumer

import "errors"

var ErrOffsetNotFound = errors.New("not found offset")
var ErrOffsetUpdate = errors.New("update offset error")
