package util

import "hash/fnv"

func CopyMap(original map[string]int) map[string]int {
	copy := make(map[string]int)
	for k, v := range original {
		copy[k] = v
	}
	return copy
}

func Hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
