package upload

import (
	"github.com/iotafs/iotafs/internal/object"
)

type PackBlueprint struct {
	Size   uint64         `json:"size"`
	Chunks []object.Chunk `json:"chunks"`
}

type CreateFileResponse struct {
	UploadID   string          `json:"upload_id"`
	Blueprints []PackBlueprint `json:"blueprints"`
}
