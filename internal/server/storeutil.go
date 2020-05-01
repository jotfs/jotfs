package server

import "github.com/iotafs/iotafs/internal/sum"

func PackfileKey(s sum.Sum) string {
	return "packfile/" + s.AsHex()
}

func PackIndexKey(s sum.Sum) string {
	return "packindex/" + s.AsHex()
}

func FileKey(s sum.Sum) string {
	return "file/" + s.AsHex()
}
