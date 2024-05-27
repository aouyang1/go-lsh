package lsherrors

import "errors"

var (
	DuplicateDocument = errors.New("document is already indexed")
	DocumentNotStored = errors.New("document id is not stored")
)
