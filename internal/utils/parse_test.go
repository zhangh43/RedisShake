package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDBKeyCounts(t *testing.T) {
	info := "# Keyspace\n" +
		"db0:keys=12,expires=3,avg_ttl=111\n" +
		"db4:keys=7,expires=0,avg_ttl=0\n"

	got := ParseDBKeyCounts(info)
	require.Equal(t, int64(12), got[0])
	require.Equal(t, int64(7), got[4])
}
