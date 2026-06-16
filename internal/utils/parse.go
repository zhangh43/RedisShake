package utils

import (
	"regexp"
	"strconv"
)

var dbKeyspaceRegexp = regexp.MustCompile(`db(\d+):keys=(\d+)`)

func ParseDBs(s string) []int {
	dbsString := regexp.MustCompile(`db(\d+):`).FindAllStringSubmatch(s, -1)
	if dbsString == nil {
		return []int{}
	}
	dbs := make([]int, len(dbsString))
	for i, dbString := range dbsString {
		db, _ := strconv.Atoi(dbString[1])
		dbs[i] = db
	}
	return dbs
}

func ParseDBKeyCounts(s string) map[int]int64 {
	items := dbKeyspaceRegexp.FindAllStringSubmatch(s, -1)
	if items == nil {
		return map[int]int64{}
	}
	counts := make(map[int]int64, len(items))
	for _, item := range items {
		db, err1 := strconv.Atoi(item[1])
		keys, err2 := strconv.ParseInt(item[2], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		counts[db] = keys
	}
	return counts
}
