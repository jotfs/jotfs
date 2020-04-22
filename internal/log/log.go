package log

import "log"

// OnError calls the function f, and if it's not nil, logs the error returned.
func OnError(f func() error) {
	if err := f(); err != nil {
		log.Printf("ERROR: %v", err)
	}
}
