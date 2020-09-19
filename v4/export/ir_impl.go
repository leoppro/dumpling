package export

import (
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
	if index == "" {
		index = ROW_ID_IDX
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

	columnNames, _, _, keys, err := decodeStartKey(ctx, db, startKeys, dbName, tableName, index)
	if err != nil {
		errCh <- errors.WithMessage(err, "fail to decodeStartKey")
		return
	}
	if len(keys) < 1 || conf.RegionLimit <= 1 {
		linear <- struct{}{}
		return
	}

	keys = keysLimiter(keys, conf.RegionLimit)

	whereConditions, err := getWhereConditions(columnNames, keys)
	if err != nil {
		errCh <- errors.WithMessage(err, "fail to generate whereConditions")
		return
	}
	for _, whereCondition := range whereConditions {
		log.Debug("getWhereConditions", zap.String("whereCondition", whereCondition))
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

func decodeStartKey(
	ctx context.Context,
	db *sql.Conn,
	startKeys []string,
	dbName, tableName, index string) (
	columnName []string,
	dataType []string,
	dataUnsigned []bool,
	keys [][]string,
	err error) {
	if index != ROW_ID_IDX {
		rows, err := db.QueryContext(ctx,
			`
SELECT s.COLUMN_NAME, t.DATA_TYPE , INSTR(LOWER(COLUMN_TYPE), 'unsigned')>0 as IS_UNSIGNED
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
			return nil, nil, nil, nil, err
		}
		defer rows.Close()
		var col, dType string
		var dUnsigned bool
		for rows.Next() {
			err = rows.Scan(&col, &dType, &dUnsigned)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			columnName = append(columnName, fmt.Sprintf("`%s`", escapeString(col)))
			dataType = append(dataType, dType)
			dataUnsigned = append(dataUnsigned, dUnsigned)
		}
	} else {
		existRowID, err := SelectTiDBRowID(db, dbName, tableName)
		if err != nil {
			return nil, nil, nil, nil, errors.Wrap(err, "can't selete tidb row id")
		}
		if existRowID {
			columnName = []string{"_tidb_rowid"}
			dataType = []string{"int"}
			dataUnsigned = []bool{false}
		} else {
			pk, err := GetPrimaryKeyName(db, dbName, tableName)
			if err != nil {
				return nil, nil, nil, nil, errors.Wrap(err, "can't get pk name")
			}
			columnName = []string{pk}
			dataType = []string{"int"}

			sql := `SELECT INSTR(LOWER(COLUMN_TYPE), 'unsigned')>0 as IS_UNSIGNED
FROM INFORMATION_SCHEMA.COLUMNS t
WHERE t.TABLE_SCHEMA = ?
  AND t.TABLE_NAME = ?
AND t.COLUMN_NAME = ?;
`
			rows, err := db.QueryContext(ctx, sql, dbName, tableName, pk)

			if err != nil {
				return nil, nil, nil, nil, err
			}
			defer rows.Close()

			var dUnsigned bool
			var rowsLen int
			for rows.Next() {
				err = rows.Scan(&dUnsigned)
				if err != nil {
					return nil, nil, nil, nil, err
				}
				rowsLen++
			}
			if rowsLen != 1 {
				return nil, nil, nil, nil, errors.Errorf("error to get type of primary key %s.%s.%s", dbName, tableName, pk)
			}
			dataUnsigned = []bool{dUnsigned}
		}
	}
	if len(columnName) == 0 || len(dataType) == 0 {
		return nil, nil, nil, nil, errors.Errorf("can't found index to split chunk, `%s`.`%s`.`%s`", dbName, tableName, index)
	}
	log.Debug("found index info", zap.String("index", index), zap.Strings("columnName", columnName), zap.Strings("dataType", dataType), zap.Bools("unsigned", dataUnsigned))

	for i := 1; i < len(startKeys); i++ {
		key, err := DecodeKey(startKeys[i], dataType, dataUnsigned)
		if err != nil {
			log.Warn("failed to decode key", zap.Error(err), zap.String("table", tableName), zap.String("index", index))
			continue
		}
		if len(key) < len(dataType) {
			log.Warn("invalid index key", zap.Error(err), zap.String("table", tableName), zap.String("index", index))
			continue
		}
		if len(key) > len(dataType) {
			key = key[:len(dataType)]
		}
		log.Debug("decode keys", zap.Strings("columnName", columnName), zap.Strings("keys", key))
		keys = append(keys, key)
	}
	return
}

func keysLimiter(keys [][]string, fileLimit uint64) [][]string {
	keysLen := uint64(len(keys))
	if fileLimit == UnspecifiedSize || keysLen < fileLimit {
		return keys
	}
	limit := fileLimit - 1
	step := keysLen / limit
	newKeys := make([][]string, limit)
	for i := 0; i < int(limit); i++ {
		newKeys[i] = keys[i*int(step)+(int(step)/2)]
	}
	return newKeys
}

func getWhereConditions(columnName []string, keys [][]string) ([]string, error) {
	whereConditions := make([]string, 0)

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
	for i := 0; i < len(keys); i++ {
		generateWhereCondition(strings.Join(keys[i], ","))
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
