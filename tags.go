package gosmparse

func unpackTags(st []string, pos int, kv []int32) (int, map[string]string) {
	// Look ahead to know how much space to allocate
	var end int = pos
	for end < len(kv) && kv[end] != 0 {
		end = end + 2
	}

	tags := make(map[string]string, (end-pos)/2)
	for pos < len(kv) {
		if kv[pos] == 0 {
			pos++
			break
		}
		tags[st[kv[pos]]] = st[kv[pos+1]]
		pos = pos + 2
	}
	return pos, tags
}
