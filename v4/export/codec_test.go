package export

import (
	"encoding/hex"
	"fmt"
	. "github.com/pingcap/check"
)

var _ = Suite(&codecSuite{})

type codecSuite struct{}

//
//[2020/09/17 14:25:03.613 +08:00] [DEBUG] [ir_impl.go:326] ["found index info"] [index=PRIMARY] [columnName="[`ol_w_id`,`ol_d_id`,`ol_o_id`,`ol_number`]"] [dataType="[int,int,int,int]"]

func (s *codecSuite) TestCodec(c *C) {
	key := "7480000000000000FF395F698000000000FF0000010380000000FF0000000403800000FF0000000007038000FF0000000004ED0380FF0000000000000A00FE"
	keys, err := DecodeKey(key, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(keys, DeepEquals, []string{"4", "7", "1261", "10"})
}

func (s *codecSuite) TestEncodeDeocdeBytes(c *C) {
	key := "7480000000000000FF395F698000000000FF0000010380000000FF0000000403800000FF0000000007038000FF0000000004ED0380FF0000000000000A00FE"
	keyBytes, err := hex.DecodeString(key)
	c.Assert(err, IsNil)
	_, d, err := DecodeBytes(keyBytes, nil)
	fmt.Println("asd", d)

}
