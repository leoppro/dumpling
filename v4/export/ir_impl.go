package export

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/pingcap/dumpling/v4/log"
	"go.uber.org/zap"
	"strings"

	"github.com/pkg/errors"
)

const (
	clusterHandle = "clusterHandle="
	tidbRowID     = "_tidb_rowid="
	indexID       = "indexID="
)

// rowIter implements the SQLRowIter interface.
// Note: To create a rowIter, please use `newRowIter()` instead of struct literal.
type rowIter struct {
	rows    *sql.Rows
	hasNext bool
	args    []interface{}
}

func newRowIter(rows *sql.Rows, argLen int) *rowIter {
	r := &rowIter{
		rows:    rows,
		hasNext: false,
		args:    make([]interface{}, argLen),
	}
	r.hasNext = r.rows.Next()
	return r
}

func (iter *rowIter) Close() error {
	return iter.rows.Close()
}

func (iter *rowIter) Decode(row RowReceiver) error {
	return decodeFromRows(iter.rows, iter.args, row)
}

func (iter *rowIter) Error() error {
	return iter.rows.Err()
}

func (iter *rowIter) Next() {
	iter.hasNext = iter.rows.Next()
}

func (iter *rowIter) HasNext() bool {
	return iter.hasNext
}

type stringIter struct {
	idx int
	ss  []string
}

func newStringIter(ss ...string) StringIter {
	return &stringIter{
		idx: 0,
		ss:  ss,
	}
}

func (m *stringIter) Next() string {
	if m.idx >= len(m.ss) {
		return ""
	}
	ret := m.ss[m.idx]
	m.idx += 1
	return ret
}

func (m *stringIter) HasNext() bool {
	return m.idx < len(m.ss)
}

type tableData struct {
	database        string
	table           string
	query           string
	chunkIndex      int
	rows            *sql.Rows
	colTypes        []*sql.ColumnType
	selectedField   string
	specCmts        []string
	escapeBackslash bool
	SQLRowIter
}

func (td *tableData) Start(ctx context.Context, conn *sql.Conn) error {
	rows, err := conn.QueryContext(ctx, td.query)
	if err != nil {
		return err
	}
	td.rows = rows
	return nil
}

func (td *tableData) ColumnTypes() []string {
	colTypes := make([]string, len(td.colTypes))
	for i, ct := range td.colTypes {
		colTypes[i] = ct.DatabaseTypeName()
	}
	return colTypes
}

func (td *tableData) ColumnNames() []string {
	colNames := make([]string, len(td.colTypes))
	for i, ct := range td.colTypes {
		colNames[i] = ct.Name()
	}
	return colNames
}

func (td *tableData) DatabaseName() string {
	return td.database
}

func (td *tableData) TableName() string {
	return td.table
}

func (td *tableData) ChunkIndex() int {
	return td.chunkIndex
}

func (td *tableData) ColumnCount() uint {
	return uint(len(td.colTypes))
}

func (td *tableData) Rows() SQLRowIter {
	if td.SQLRowIter == nil {
		td.SQLRowIter = newRowIter(td.rows, len(td.colTypes))
	}
	return td.SQLRowIter
}

func (td *tableData) SelectedField() string {
	if td.selectedField == "*" {
		return ""
	}
	return fmt.Sprintf("(%s)", td.selectedField)
}

func (td *tableData) SpecialComments() StringIter {
	return newStringIter(td.specCmts...)
}

func (td *tableData) EscapeBackSlash() bool {
	return td.escapeBackslash
}

func splitTableDataIntoChunksByRegion(
	ctx context.Context,
	tableDataIRCh chan TableDataIR,
	errCh chan error,
	linear chan struct{},
	dbName, tableName, index string, db *sql.Conn, conf *Config) {
	if conf.ServerInfo.ServerType != ServerTypeTiDB {
		errCh <- errors.Errorf("can't split chunks by region info for database %s except TiDB", serverTypeString[conf.ServerInfo.ServerType])
		return
	}

	if index == PRIMARY {
		existRowID, err := SelectTiDBRowID(db, dbName, tableName)
		if err != nil {
			errCh <- errors.Wrap(err, "can't selete tidb row id")
			return
		}
		if !existRowID {
			index = ROW_ID_IDX
		}
	}

	startKeys, estimatedCounts, err := getTableRegionInfo(ctx, db, dbName, tableName, index)
	if err != nil {
		errCh <- errors.WithMessage(err, "fail to get TiDB table regions info")
		return
	}
	log.Debug("getTableRegionInfo", zap.Strings("startKeys", startKeys), zap.Uint64s("counts", estimatedCounts))

	whereConditions, err := getWhereConditions(ctx, db, startKeys, estimatedCounts, dbName, tableName, index)
	if err != nil {
		errCh <- errors.WithMessage(err, "fail to generate whereConditions")
		return
	}
	log.Debug("getWhereConditions", zap.Strings("whereConditions", whereConditions))

	if len(whereConditions) <= 1 {
		linear <- struct{}{}
		return
	}

	selectedField, err := buildSelectField(db, dbName, tableName, conf.CompleteInsert)
	if err != nil {
		errCh <- withStack(err)
		return
	}

	colTypes, err := GetColumnTypes(db, selectedField, dbName, tableName)
	if err != nil {
		errCh <- withStack(err)
		return
	}
	orderByClause, err := buildOrderByClause(conf, db, dbName, tableName)
	if err != nil {
		errCh <- withStack(err)
		return
	}

	chunkIndex := 0
LOOP:
	for _, whereCondition := range whereConditions {
		chunkIndex += 1
		query := buildSelectQuery(dbName, tableName, selectedField, buildWhereCondition(conf, whereCondition), orderByClause)

		td := &tableData{
			database:        dbName,
			table:           tableName,
			query:           query,
			chunkIndex:      chunkIndex,
			colTypes:        colTypes,
			selectedField:   selectedField,
			escapeBackslash: conf.EscapeBackslash,
			specCmts: []string{
				"/*!40101 SET NAMES binary*/;",
			},
		}
		select {
		case <-ctx.Done():
			break LOOP
		case tableDataIRCh <- td:
		}
	}
	close(tableDataIRCh)
}

func tryDecodeRowKey(ctx context.Context, db *sql.Conn, key string) ([]string, error) {
	return DecodeKey(key)
}

func getWhereConditions(ctx context.Context, db *sql.Conn, startKeys []string, counts []uint64, dbName, tableName, index string) ([]string, error) {
	whereConditions := make([]string, 0)
	var (
		columnName []string
		dataType   []string
	)
	if index != ROW_ID_IDX {
		rows, err := db.QueryContext(ctx,
			`
SELECT s.COLUMN_NAME, t.DATA_TYPE
FROM INFORMATION_SCHEMA.TIDB_INDEXES s,
     INFORMATION_SCHEMA.COLUMNS t
WHERE s.TABLE_SCHEMA = t.TABLE_SCHEMA
  AND s.TABLE_NAME = t.TABLE_NAME
  AND s.COLUMN_NAME = t.COLUMN_NAME
  AND s.TABLE_SCHEMA = ?
  AND s.TABLE_NAME = ?
  AND s.KEY_NAME = ?
ORDER BY s.SEQ_IN_INDEX;
`, dbName, tableName, index)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var col, dType string
		for rows.Next() {
			err = rows.Scan(&col, &dType)
			if err != nil {
				return nil, err
			}
			columnName = append(columnName, fmt.Sprintf("`%s`", escapeString(col)))
			dataType = append(dataType, dType)
		}
	} else {
		existRowID, err := SelectTiDBRowID(db, dbName, tableName)
		if err != nil {
			return nil, errors.Wrap(err, "can't selete tidb row id")
		}
		if existRowID {
			columnName = []string{"_tidb_rowid"}
			dataType = []string{"int"}
		} else {
			pk, err := GetPrimaryKeyName(db, dbName, tableName)
			if err != nil {
				return nil, errors.Wrap(err, "can't get pk name")
			}
			columnName = []string{pk}
			dataType = []string{"int"}
		}
	}
	if len(columnName) == 0 || len(dataType) == 0 {
		return nil, errors.Errorf("can't found index to split chunk, `%s`.`%s`.`%s`", dbName, tableName, index)
	}
	log.Debug("found index info", zap.String("index", index), zap.Strings("columnName", columnName), zap.Strings("dataType", dataType))

	lastStartKey := ""
	field := strings.Join(columnName, ",")

	generateWhereCondition := func(endKey string) {
		where := ""
		and := ""
		if lastStartKey != "" {
			where += fmt.Sprintf("(%s) >= (%s)", field, lastStartKey)
			and = " AND "
		}
		if endKey != "" {
			where += and
			where += fmt.Sprintf("(%s) < (%s)", field, endKey)
		}
		lastStartKey = endKey
		whereConditions = append(whereConditions, where)
	}
	for i := 1; i < len(startKeys); i++ {
		keys, err := tryDecodeRowKey(ctx, db, startKeys[i])
		if err != nil {
			return nil, err
		}
		log.Debug("decode keys", zap.Strings("columnName", columnName), zap.Strings("keys", keys))
		if len(dataType) != len(keys) {
			continue
		}
		var bf bytes.Buffer
		for j := range keys {
			if _, ok := dataTypeStringMap[strings.ToUpper(dataType[j])]; ok {
				bf.Reset()
				escape([]byte(keys[j]), &bf, nil)
				keys[j] = fmt.Sprintf("'%s'", bf.String())
			}
		}
		generateWhereCondition(strings.Join(keys, ","))
	}
	generateWhereCondition("")
	return whereConditions, nil
}

type metaData struct {
	target   string
	metaSQL  string
	specCmts []string
}

func (m *metaData) SpecialComments() StringIter {
	return newStringIter(m.specCmts...)
}

func (m *metaData) TargetName() string {
	return m.target
}

func (m *metaData) MetaSQL() string {
	return m.metaSQL
}
