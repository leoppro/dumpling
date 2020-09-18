package export

import (
	"context"
	. "github.com/pingcap/check"
)

var _ = Suite(&testDumpSuite{})

type testDumpSuite struct{}

func (s *testDumpSuite) TestDump(c *C) {
	ctx := context.Background()
	err := Dump(ctx, &Config{
		Host:          "localhost",
		User:          "root",
		Port:          4000,
		Threads:       4,
		NoSchemas:     true,
		LogLevel:      "debug",
		FileType:      "csv",
		Consistency:   "snapshot",
		OutputDirPath: "/Users/leoppro/dumpling",
		SchameName:    "test",
		TableName:     "order_line",
		IndexName:     "",
		CsvSeparator:  ",",
		CsvDelimiter:  "",
		NoHeader:      true,
		RowsLimit:     100000,
	})
	c.Assert(err, IsNil)
}
