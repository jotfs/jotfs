package object

type objType uint8

// Object types
const (
	PackfileObject uint8 = iota + 1
	PackindexObject
	FileObject
)
