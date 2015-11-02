package gosmparse

func unpackTags(st [][]byte, pos int, kv []int32) (int, map[string]string) {
	tags := map[string]string{}
	for pos < len(kv) {
		if kv[pos] == 0 {
			pos++
			break
		}
		tags[string(st[kv[pos]])] = string(st[kv[pos+1]])
		pos = pos + 2
	}
	return pos, tags
}
