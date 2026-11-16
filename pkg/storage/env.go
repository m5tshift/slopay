package storage

import (
	"fmt"
	"os"
	"strconv"
)

func envInt32(name string) (int32, error) {
	v := os.Getenv(name)
	if v == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("storage: %s must be a non-negative integer", name)
	}
	return int32(n), nil
}
