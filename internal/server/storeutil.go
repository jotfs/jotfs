package server

import "github.com/iotafs/iotafs/internal/sum"

func PackfileKey(s sum.Sum) string {
	return s.AsHex() + ".pack"
}

func PackIndexKey(s sum.Sum) string {
	return s.AsHex() + ".index"
}

func FileKey(s sum.Sum) string {
	return s.AsHex() + ".file"
}
