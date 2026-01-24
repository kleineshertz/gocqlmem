package gocqlmem

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableSelectOrderBy(t *testing.T) {
	table := Table{
		ColumnDefs: []*ColumnDef{
			{"col1", PrimaryKeyPartition, CqlDataTypeText, ClusteringOrderAsc},
			{"col2", PrimaryKeyClustering, CqlDataTypeBigint, ClusteringOrderDesc},
			{"col3", PrimaryKeyNone, CqlDataTypeBool, ClusteringOrderNone},
		},
		ColumnValues: [][]any{
			{"a", "a", "b", "c"},
			{3, 2, 1, 1},
		},
		ColumnDefMap: map[string]int{"col1": 0, "col2": 1},
	}

	seq, err := table.getRowSequenceFromColumnDefAndSelectOrderBy([]*OrderByField{
		{"col1", ClusteringOrderAsc},
		{"col2", ClusteringOrderDesc},
	})
	assert.Nil(t, err)
	assert.Equal(t, []int{0, 1, 2, 3}, seq)

	seq, err = table.getRowSequenceFromColumnDefAndSelectOrderBy([]*OrderByField{
		{"col1", ClusteringOrderDesc},
		{"col2", ClusteringOrderAsc},
	})
	assert.Nil(t, err)
	assert.Equal(t, []int{3, 2, 1, 0}, seq)

	seq, err = table.getRowSequenceFromColumnDefAndSelectOrderBy([]*OrderByField{
		{"col1", ClusteringOrderAsc},
		{"col2", ClusteringOrderAsc},
	})
	assert.Nil(t, err)
	assert.Equal(t, []int{1, 0, 2, 3}, seq)

	seq, err = table.getRowSequenceFromColumnDefAndSelectOrderBy([]*OrderByField{
		{"col1", ClusteringOrderDesc},
		{"col2", ClusteringOrderDesc},
	})
	assert.Nil(t, err)
	assert.Equal(t, []int{3, 2, 0, 1}, seq)

	seq, err = table.getRowSequenceFromColumnDefAndSelectOrderBy([]*OrderByField{})
	assert.Nil(t, err)
	assert.Equal(t, []int{0, 1, 2, 3}, seq)

	seq, err = table.getRowSequenceFromColumnDefAndSelectOrderBy([]*OrderByField{
		{"col1", ClusteringOrderDesc},
	})
	assert.Nil(t, err)
	assert.Equal(t, []int{3, 2, 0, 1}, seq)

	seq, err = table.getRowSequenceFromColumnDefAndSelectOrderBy([]*OrderByField{
		{"col2", ClusteringOrderAsc},
	})
	assert.Nil(t, err)
	assert.Equal(t, []int{3, 2, 1, 0}, seq)

	seq, err = table.getRowSequenceFromColumnDefAndSelectOrderBy([]*OrderByField{
		{"col3", ClusteringOrderAsc},
		{"col2", ClusteringOrderAsc},
	})
	assert.Contains(t, err.Error(), "cannot process ORDER BY col3, this field is not a clustering key")
}

func TestRowIndexFromColumnDefAndInsert(t *testing.T) {
	var idx int
	var isExists bool
	var err error

	var columnDefs []*ColumnDef
	var columnValues [][]any

	// ASC ASC

	columnDefs = []*ColumnDef{
		{"col1", PrimaryKeyPartition, CqlDataTypeText, ClusteringOrderAsc},
		{"col2", PrimaryKeyClustering, CqlDataTypeBigint, ClusteringOrderAsc},
		{"col3", PrimaryKeyNone, CqlDataTypeBool, ClusteringOrderNone},
	}
	columnValues = [][]any{
		{"a", "a", "c", "d"},
		{int64(0), int64(1), int64(3), int64(3)},
	}

	idx, isExists, err = getRowIndexFromColumnDefAndInsert(columnValues, columnDefs, map[string]any{
		"col1": "a",
		"col2": int64(10),
		"col3": true})
	assert.Nil(t, err)
	assert.False(t, isExists)
	assert.Equal(t, 2, idx)

	idx, isExists, err = getRowIndexFromColumnDefAndInsert(columnValues, columnDefs, map[string]any{
		"col1": "a",
		"col2": int64(0),
		"col3": true})
	assert.Nil(t, err)
	assert.True(t, isExists)
	assert.Equal(t, 1, idx)

	idx, isExists, err = getRowIndexFromColumnDefAndInsert(columnValues, columnDefs, map[string]any{
		"col1": "e",
		"col2": int64(10000),
		"col3": true})
	assert.Nil(t, err)
	assert.False(t, isExists)
	assert.Equal(t, 4, idx)

	idx, isExists, err = getRowIndexFromColumnDefAndInsert(columnValues, columnDefs, map[string]any{
		"col1": "A",
		"col2": int64(0),
		"col3": true})
	assert.Nil(t, err)
	assert.False(t, isExists)
	assert.Equal(t, 0, idx)

	idx, isExists, err = getRowIndexFromColumnDefAndInsert(columnValues, columnDefs, map[string]any{
		"col1": "b",
		"col2": int64(10000),
		"col3": true})
	assert.Nil(t, err)
	assert.False(t, isExists)
	assert.Equal(t, 2, idx)

	// ASC DESC

	columnDefs = []*ColumnDef{
		{"col1", PrimaryKeyPartition, CqlDataTypeText, ClusteringOrderAsc},
		{"col2", PrimaryKeyClustering, CqlDataTypeBigint, ClusteringOrderDesc},
		{"col3", PrimaryKeyNone, CqlDataTypeBool, ClusteringOrderNone},
	}
	columnValues = [][]any{
		{"a", "a", "c", "d"},
		{int64(3), int64(3), int64(1), int64(0)},
	}

	idx, isExists, err = getRowIndexFromColumnDefAndInsert(columnValues, columnDefs, map[string]any{
		"col1": "a",
		"col2": int64(10),
		"col3": true})
	assert.Nil(t, err)
	assert.False(t, isExists)
	assert.Equal(t, 0, idx)

	idx, isExists, err = getRowIndexFromColumnDefAndInsert(columnValues, columnDefs, map[string]any{
		"col1": "a",
		"col2": int64(3),
		"col3": true})
	assert.Nil(t, err)
	assert.True(t, isExists)
	assert.Equal(t, 2, idx)

	idx, isExists, err = getRowIndexFromColumnDefAndInsert(columnValues, columnDefs, map[string]any{
		"col1": "c",
		"col2": int64(10000),
		"col3": true})
	assert.Nil(t, err)
	assert.False(t, isExists)
	assert.Equal(t, 2, idx)

	idx, isExists, err = getRowIndexFromColumnDefAndInsert(columnValues, columnDefs, map[string]any{
		"col1": "c",
		"col2": int64(1),
		"col3": true})
	assert.Nil(t, err)
	assert.True(t, isExists)
	assert.Equal(t, 3, idx)

	idx, isExists, err = getRowIndexFromColumnDefAndInsert(columnValues, columnDefs, map[string]any{
		"col1": "d",
		"col2": int64(-1),
		"col3": true})
	assert.Nil(t, err)
	assert.False(t, isExists)
	assert.Equal(t, 4, idx)

	// ASC DESC empty

	columnDefs = []*ColumnDef{
		{"col1", PrimaryKeyPartition, CqlDataTypeText, ClusteringOrderAsc},
		{"col2", PrimaryKeyClustering, CqlDataTypeBigint, ClusteringOrderDesc},
		{"col3", PrimaryKeyNone, CqlDataTypeBool, ClusteringOrderNone},
	}
	columnValues = [][]any{
		{},
		{},
	}

	idx, isExists, err = getRowIndexFromColumnDefAndInsert(columnValues, columnDefs, map[string]any{
		"col1": "a",
		"col2": int64(1),
		"col3": true})
	assert.Nil(t, err)
	assert.False(t, isExists)
	assert.Equal(t, 0, idx)
}

func TestTableInsert(t *testing.T) {
	table := Table{
		ColumnDefs: []*ColumnDef{
			{"col1", PrimaryKeyPartition, CqlDataTypeText, ClusteringOrderAsc},
			{"col2", PrimaryKeyClustering, CqlDataTypeBigint, ClusteringOrderDesc},
			{"col3", PrimaryKeyNone, CqlDataTypeBool, ClusteringOrderNone},
		},
		ColumnValues: [][]any{[]any{}, []any{}, []any{}},
		ColumnDefMap: map[string]int{"col1": 0, "col2": 1, "col3": 2},
	}

	var cmd CommandInsert
	var isApplied bool
	var err error

	cmd = CommandInsert{
		ColumnNames:  []string{"col1", "col2", "col3"},
		ColumnValues: []*Lexem{{LexemStringLiteral, "a"}, {LexemNumberLiteral, "1"}, {LexemNull, "NULL"}},
		IfNotExists:  false,
	}

	isApplied, err = table.execInsert(&cmd)
	assert.Nil(t, err)
	assert.True(t, isApplied)
	assert.Equal(t, "a", table.ColumnValues[0][0])
	assert.Equal(t, int64(1), table.ColumnValues[1][0])
	assert.Equal(t, nil, table.ColumnValues[2][0])

	cmd = CommandInsert{
		ColumnNames:  []string{"col1", "col2", "col3"},
		ColumnValues: []*Lexem{{LexemStringLiteral, "a"}, {LexemNumberLiteral, "2"}, {LexemBoolLiteral, "TRUE"}},
		IfNotExists:  false,
	}
	isApplied, err = table.execInsert(&cmd)
	assert.Nil(t, err)
	assert.True(t, isApplied)
	assert.Equal(t, "a", table.ColumnValues[0][0])
	assert.Equal(t, int64(2), table.ColumnValues[1][0])
	assert.Equal(t, true, table.ColumnValues[2][0])
	assert.Equal(t, "a", table.ColumnValues[0][1])
	assert.Equal(t, int64(1), table.ColumnValues[1][1])
	assert.Equal(t, nil, table.ColumnValues[2][1])

	cmd = CommandInsert{
		ColumnNames:  []string{"col1", "col2", "col3"},
		ColumnValues: []*Lexem{{LexemStringLiteral, "b"}, {LexemNumberLiteral, "0"}, {LexemBoolLiteral, "TRUE"}},
		IfNotExists:  false,
	}
	isApplied, err = table.execInsert(&cmd)
	assert.Nil(t, err)
	assert.True(t, isApplied)
	assert.Equal(t, "a", table.ColumnValues[0][0])
	assert.Equal(t, int64(2), table.ColumnValues[1][0])
	assert.Equal(t, true, table.ColumnValues[2][0])
	assert.Equal(t, "a", table.ColumnValues[0][1])
	assert.Equal(t, int64(1), table.ColumnValues[1][1])
	assert.Equal(t, nil, table.ColumnValues[2][1])
	assert.Equal(t, "b", table.ColumnValues[0][2])
	assert.Equal(t, int64(0), table.ColumnValues[1][2])
	assert.Equal(t, true, table.ColumnValues[2][2])

	cmd.IfNotExists = true
	isApplied, err = table.execInsert(&cmd)
	assert.Nil(t, err)
	assert.False(t, isApplied)

	cmd.IfNotExists = false
	isApplied, err = table.execInsert(&cmd)
	assert.Contains(t, err.Error(), "cannot insert duplicate map[col1:b col2:0 col3:true]")
	assert.False(t, isApplied)
}

func TestTableSelect(t *testing.T) {
	table := Table{
		ColumnDefs: []*ColumnDef{
			{"col1", PrimaryKeyPartition, CqlDataTypeText, ClusteringOrderAsc},
			{"col2", PrimaryKeyClustering, CqlDataTypeBigint, ClusteringOrderDesc},
		},
		ColumnValues: [][]any{
			{"a", "a", "c", "d"},
			{int64(0), int64(1), int64(3), int64(3)},
		},
		ColumnDefMap: map[string]int{"col1": 0, "col2": 1, "col3": 2},
	}

	cmds, err := ParseCommands(`SELECT col1+'a' AS c1, col2*5 as c2 FROM ks1.t WHERE col1 = 'a'`)
	assert.Nil(t, err)
	cmd, ok := cmds[0].(*CommandSelect)
	assert.True(t, ok)

	names, values, err := table.execSelect(cmd)
	assert.Nil(t, err)

	assert.Equal(t, "c1", names[0])
	assert.Equal(t, "c2", names[1])

	assert.Equal(t, 2, len(values))

	assert.Equal(t, "aa", values[0][0])
	assert.Equal(t, int64(0), values[0][1])
	assert.Equal(t, "aa", values[1][0])
	assert.Equal(t, int64(5), values[1][1])
}
