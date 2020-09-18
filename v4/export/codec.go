package export

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/pingcap/dumpling/v4/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"
	"strconv"
	"strings"
)

var (
	tablePrefix  = []byte{'t'}
	recordPrefix = []byte("_r")
	indexPrefix  = []byte("_i")
	metaPrefix   = []byte("m")

	intLen            = 8
	tablePrefixLen    = len(tablePrefix)
	recordPrefixLen   = len(recordPrefix)
	indexPrefixLen    = len(indexPrefix)
	metaPrefixLen     = len(metaPrefix)
	prefixTableIDLen  = tablePrefixLen + intLen  /*tableID*/
	prefixRecordIDLen = recordPrefixLen + intLen /*recordID*/
	prefixIndexLen    = indexPrefixLen + intLen  /*indexID*/

	encGroupSize = byte(8)
	encMarker    = byte(0xFF)
	encPad       = byte(0x0)
)

func decodeTableID(key []byte) (rest []byte, tableID int64, err error) {
	if len(key) < prefixTableIDLen || !bytes.HasPrefix(key, tablePrefix) {
		return nil, 0, errors.Errorf("invalid record key - %q", key)
	}
	key = key[tablePrefixLen:]
	rest, tableID, err = DecodeInt(key)
	if err != nil {
		return nil, 0, errors.Wrap(err, "invalid record key")
	}
	return
}

// DecodeInt decodes value encoded by EncodeInt before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeInt(b []byte) ([]byte, int64, error) {
	if len(b) < 8 {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	u := binary.BigEndian.Uint64(b[:8])
	v := DecodeCmpUintToInt(u)
	b = b[8:]
	return b, v, nil
}

const signMask uint64 = 0x8000000000000000

// DecodeCmpUintToInt decodes the u that encoded by EncodeIntToCmpUint
func DecodeCmpUintToInt(u uint64) int64 {
	return int64(u ^ signMask)
}

func DecodeKey(key string, dataType []string, unsigned []bool) ([]string, error) {
	keyBytes, err := hex.DecodeString(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	_, keyBytes, err = DecodeBytes(keyBytes, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !bytes.HasPrefix(keyBytes, tablePrefix) {
		return nil, errors.Errorf("")
	}
	keyBytes, _, err = decodeTableID(keyBytes)
	if err != nil {
		return nil, errors.Trace(err)
	}
	switch {
	case bytes.HasPrefix(keyBytes, recordPrefix):
		_, recordID, err := decodeRecordID(keyBytes)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if unsigned[0] {
			return []string{strconv.FormatUint(uint64(recordID), 10)}, nil
		} else {
			return []string{strconv.FormatInt(recordID, 10)}, nil
		}
	case bytes.HasPrefix(keyBytes, indexPrefix):
		_, indexValue, err := decodeIndexKey(keyBytes)
		if err != nil {
			return nil, errors.Trace(err)
		}
		log.Debug("index value", zap.Stringer("value", datums(indexValue)))
		if len(indexValue) > len(dataType) {
			indexValue = indexValue[:len(dataType)]
		}
		keys := make([]string, len(indexValue))
		for i, v := range indexValue {
			keys[i] = datum2String(v, dataType[i], unsigned[i])
		}
		return keys, nil
	default:
		panic("unreachable")
	}
}

type datums []types.Datum

func (ds datums) String() string {
	bs := new(strings.Builder)
	for _, d := range ds {
		bs.WriteString(d.String())
		bs.WriteRune(',')
		bs.WriteRune(' ')
	}
	return bs.String()
}

func decodeRecordID(key []byte) (rest []byte, recordID int64, err error) {
	if len(key) < prefixRecordIDLen || !bytes.HasPrefix(key, recordPrefix) {
		return nil, 0, errors.Errorf("invalid record key - %q", key)
	}
	key = key[recordPrefixLen:]
	rest, recordID, err = codec.DecodeInt(key)
	if err != nil {
		return nil, 0, errors.Wrap(err, "invalid record key")
	}
	return
}

func datum2String(d types.Datum, dataType string, unsigned bool) string {
	switch d.Kind() {
	case types.KindNull:
		return "NULL"
	case types.KindInt64:
		return strconv.FormatInt(d.GetInt64(), 10)
	case types.KindUint64:
		switch dataType {

		}
		return strconv.FormatUint(d.GetUint64(), 10)
	case types.KindFloat32, types.KindFloat64:
		return strconv.FormatFloat(d.GetFloat64(), 'G', -1, 64)
	case types.KindString:
		return d.GetString()
	case types.KindBytes:
		b := d.GetBytes()
		return fmt.Sprintf("x'%s'", hex.EncodeToString(b))
	case types.KindMysqlDecimal:
		v := d.GetMysqlDecimal()
		if v == nil {
			return "NULL"
		}
		return v.String()
	case types.KindMysqlDuration:
		return d.GetMysqlDuration().String()
	case types.KindMysqlEnum:
		return strconv.FormatUint(d.GetMysqlEnum().Value, 10)
	case types.KindBinaryLiteral, types.KindMysqlBit:
		v, _ := d.GetBinaryLiteral().ToInt(nil)
		return strconv.FormatUint(v, 10)
	case types.KindMysqlSet:
		return strconv.FormatUint(d.GetMysqlSet().Value, 10)
	case types.KindMysqlJSON:
		return d.GetMysqlJSON().String()
	case types.KindMysqlTime:
		return d.GetMysqlTime().String()
	default:
		return fmt.Sprintf("%s", d.GetInterface())
	}
}

func decodeBytes(b []byte, buf []byte, reverse bool) ([]byte, []byte, error) {
	if buf == nil {
		buf = make([]byte, 0, len(b))
	}
	buf = buf[:0]
	for {
		if len(b) < int(encGroupSize+1) {
			return nil, nil, errors.New("insufficient bytes to decode value")
		}

		groupBytes := b[:encGroupSize+1]

		group := groupBytes[:encGroupSize]
		marker := groupBytes[encGroupSize]

		var padCount byte
		if reverse {
			padCount = marker
		} else {
			padCount = encMarker - marker
		}
		if padCount > encGroupSize {
			return nil, nil, errors.Errorf("invalid marker byte, group bytes %q", groupBytes)
		}

		realGroupSize := encGroupSize - padCount
		buf = append(buf, group[:realGroupSize]...)
		b = b[encGroupSize+1:]

		if padCount != 0 {
			var padByte = encPad
			if reverse {
				padByte = encMarker
			}
			// Check validity of padding bytes.
			for _, v := range group[realGroupSize:] {
				if v != padByte {
					return nil, nil, errors.Errorf("invalid padding byte, group bytes %q", groupBytes)
				}
			}
			break
		}
	}
	if reverse {
		//reverseBytes(buf)
		panic("unreachable")
	}
	return b, buf, nil
}

// DecodeBytes decodes bytes which is encoded by EncodeBytes before,
// returns the leftover bytes and decoded value if no error.
// `buf` is used to buffer data to avoid the cost of makeslice in decodeBytes when DecodeBytes is called by Decoder.DecodeOne.
func DecodeBytes(b []byte, buf []byte) ([]byte, []byte, error) {
	return decodeBytes(b, buf, false)
}

func decodeIndexKey(key []byte) (indexID int64, indexValue []types.Datum, err error) {
	if len(key) < prefixIndexLen || !bytes.HasPrefix(key, indexPrefix) {
		return 0, nil, errors.Errorf("invalid record key - %q", key)
	}
	key = key[indexPrefixLen:]
	key, indexID, err = codec.DecodeInt(key)
	if err != nil {
		return 0, nil, errors.Wrap(err, "invalid record key")
	}
	indexValue, err = codec.Decode(key, 2)
	if err != nil {
		return 0, nil, errors.Wrap(err, "invalid record key")
	}
	return
}
