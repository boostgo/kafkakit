package kafkakit

import (
	"reflect"
	"unsafe"
)

// convertBytesFromString converts a string to a byte slice without allocating.
// The result MUST stay read-only: it aliases the memory of an immutable string.
func convertBytesFromString(x string) []byte {
	return unsafe.Slice(unsafe.StringData(x), len(x)) //nolint:gosec // zero-copy, the result is read-only
}

// convertStringFromBytes converts a byte slice to a string without allocating.
// The slice must not be modified afterwards: the string shares its memory.
func convertStringFromBytes(x []byte) string {
	return unsafe.String(unsafe.SliceData(x), len(x)) //nolint:gosec // zero-copy, the slice is never modified
}

func isPointer(x any) bool {
	if x == nil {
		return true
	}

	return reflect.TypeOf(x).Kind() == reflect.Pointer
}
