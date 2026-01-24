package gocqlmem

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"regexp"
	"strings"
)

type LexemType int

const (
	LexemStringLiteral LexemType = iota
	LexemNumberLiteral
	LexemBoolLiteral
	LexemIdent
	LexemPointedIdent
	LexemKeyword
	LexemComma
	LexemSemicolon
	LexemArithmeticOp
	LexemLogicalOp
	LexemCqlOp
	LexemParenthesis
	LexemNull
	LexemAs
)

type Lexem struct {
	T LexemType
	V string
}

type ClusteringOrderType int

const (
	ClusteringOrderNone ClusteringOrderType = iota
	ClusteringOrderAsc
	ClusteringOrderDesc
)

func stringToClusteringOrderType(s string) ClusteringOrderType {
	switch s {
	case "ASC":
		return ClusteringOrderAsc
	case "DESC":
		return ClusteringOrderDesc
	default:
		return ClusteringOrderNone
	}
}

type OrderByField struct {
	FieldName       string
	ClusteringOrder ClusteringOrderType
}

type Command interface {
	GetCtxKeyspace() string
	SetCtxKeyspace(string)
}

type KeyValuePair struct {
	K string
	V *Lexem
}

type CommandCreateKeyspace struct {
	IfNotExists     bool
	KeyspaceName    string
	WithReplication []*KeyValuePair
}

func (c *CommandCreateKeyspace) GetCtxKeyspace() string {
	return c.KeyspaceName
}
func (c *CommandCreateKeyspace) SetCtxKeyspace(keyspace string) {
}

type CommandUseKeyspace struct {
	KeyspaceName string
}

func (c *CommandUseKeyspace) GetCtxKeyspace() string {
	return c.KeyspaceName
}
func (c *CommandUseKeyspace) SetCtxKeyspace(keyspace string) {
}

type CommandDropKeyspace struct {
	IfExists     bool
	KeyspaceName string
}

func (c *CommandDropKeyspace) GetCtxKeyspace() string {
	return c.KeyspaceName
}
func (c *CommandDropKeyspace) SetCtxKeyspace(keyspace string) {
}

type CreateTableColumnDef struct {
	Name string
	Type string
}

type ColumnSetExp struct {
	Name      string
	ExpLexems []*Lexem
}

type CommandCreateTable struct {
	CtxKeyspace          string
	IfNotExists          bool
	TableName            string
	ColumnDefs           []*CreateTableColumnDef
	PartitionKeyColumns  []string
	ClusteringKeyColumns []string
	ClusteringOrderBy    []*OrderByField
}

func (c *CommandCreateTable) GetCtxKeyspace() string {
	return c.CtxKeyspace
}
func (c *CommandCreateTable) SetCtxKeyspace(keyspace string) {
	c.CtxKeyspace = keyspace
}

type CommandDropTable struct {
	CtxKeyspace string
	IfExists    bool
	TableName   string
}

func (c *CommandDropTable) GetCtxKeyspace() string {
	return c.CtxKeyspace
}
func (c *CommandDropTable) SetCtxKeyspace(keyspace string) {
	c.CtxKeyspace = keyspace
}

// ORDER BY: The partition key must be defined in the WHERE clause and then the ORDER BY clause defines
// one or more clustering columns to use for ordering. The order of the specified columns must match the
// order of the clustering columns in the PRIMARY KEY definition
type CommandSelect struct {
	CtxKeyspace     string
	Distinct        bool
	SelectExpLexems [][]*Lexem
	TableName       string
	WhereExpLexems  []*Lexem
	OrderByFields   []*OrderByField
	Limit           *Lexem
	SelectExpAsts   []ast.Expr
	SelectExpNames  []string
	WhereExpAst     ast.Expr
}

func (c *CommandSelect) GetCtxKeyspace() string {
	return c.CtxKeyspace
}
func (c *CommandSelect) SetCtxKeyspace(keyspace string) {
	c.CtxKeyspace = keyspace
}

type CommandInsert struct {
	CtxKeyspace  string
	TableName    string
	ColumnNames  []string
	ColumnValues []*Lexem
	IfNotExists  bool
}

func (c *CommandInsert) GetCtxKeyspace() string {
	return c.CtxKeyspace
}
func (c *CommandInsert) SetCtxKeyspace(keyspace string) {
	c.CtxKeyspace = keyspace
}

type CommandUpdate struct {
	CtxKeyspace          string
	TableName            string
	ColumnSetExpressions []*ColumnSetExp
	WhereExpLexems       []*Lexem
	IfExists             bool
	ColumnSetExpAsts     []ast.Expr
	WhereExpAst          ast.Expr
}

func (c *CommandUpdate) GetCtxKeyspace() string {
	return c.CtxKeyspace
}
func (c *CommandUpdate) SetCtxKeyspace(keyspace string) {
	c.CtxKeyspace = keyspace
}

type CommandDelete struct {
	CtxKeyspace    string
	TableName      string
	WhereExpLexems []*Lexem
	IfExists       bool
	WhereExpAst    ast.Expr
}

func (c *CommandDelete) GetCtxKeyspace() string {
	return c.CtxKeyspace
}
func (c *CommandDelete) SetCtxKeyspace(keyspace string) {
	c.CtxKeyspace = keyspace
}

func skipBlank(s string) string {
	return strings.TrimLeft(s, " \t\r\n")
}

func getStringLiteral(s string) (*Lexem, string) {
	s = skipBlank(s)
	r := regexp.MustCompile(`^'(''|[^'])*'`)
	litRange := r.FindStringIndex(s)
	if len(litRange) >= 2 {
		return &Lexem{LexemStringLiteral, s[1 : litRange[1]-1]}, s[litRange[1]:]
	}
	return nil, s
}

func getNull(s string) (*Lexem, string) {
	s = skipBlank(s)
	r := regexp.MustCompile(`^(?i)NULL`)
	litRange := r.FindStringIndex(s)
	if len(litRange) >= 2 {
		return &Lexem{LexemNull, s[0:litRange[1]]}, s[litRange[1]:]
	}
	return nil, s
}

func getAs(s string) (*Lexem, string) {
	s = skipBlank(s)
	r := regexp.MustCompile(`^(?i)AS`)
	litRange := r.FindStringIndex(s)
	if len(litRange) >= 2 {
		return &Lexem{LexemAs, s[0:litRange[1]]}, s[litRange[1]:]
	}
	return nil, s
}

func getNumberLiteral(s string) (*Lexem, string) {
	s = skipBlank(s)
	r := regexp.MustCompile(`^[\-\+]?\d*\.?\d+([eE][-+]?\d+)?`)
	litRange := r.FindStringIndex(s)
	if len(litRange) >= 2 {
		return &Lexem{LexemNumberLiteral, s[0:litRange[1]]}, s[litRange[1]:]
	}
	return nil, s
}

func getBoolLiteral(s string) (*Lexem, string) {
	s = skipBlank(s)
	r := regexp.MustCompile(`^(?i)(TRUE|FALSE)`)
	litRange := r.FindStringIndex(s)
	if len(litRange) >= 2 {
		return &Lexem{LexemBoolLiteral, strings.ToUpper(s[0:litRange[1]])}, s[litRange[1]:]
	}
	return nil, s
}

func getKeyword(s string, kwRegex string, isProcess bool) (*Lexem, string) {
	s = skipBlank(s)
	r := regexp.MustCompile(`^` + kwRegex)
	litRange := r.FindStringIndex(s)
	if len(litRange) >= 2 {
		reBlank := regexp.MustCompile(`\s+`)
		result := strings.ToUpper(reBlank.ReplaceAllString(s[0:litRange[1]], " "))
		if isProcess {
			return &Lexem{LexemKeyword, result}, s[litRange[1]:]
		} else {
			return &Lexem{LexemKeyword, result}, s
		}
	}
	return nil, s
}

func getIdent(s string) (*Lexem, string) {
	s = skipBlank(s)
	r := regexp.MustCompile(`^("[a-zA-Z][a-zA-Z0-9_]*"|[a-zA-Z][a-zA-Z0-9_]*)`)
	litRange := r.FindStringIndex(s)
	if len(litRange) >= 2 {
		return &Lexem{LexemIdent, strings.ReplaceAll(s[0:litRange[1]], `"`, ``)}, s[litRange[1]:]
	}
	return nil, s
}

func getPointedIdent(s string) (*Lexem, string) {
	s = skipBlank(s)
	r := regexp.MustCompile(`^("[a-zA-Z][a-zA-Z0-9_]*"|[a-zA-Z][a-zA-Z0-9_]*)\.([a-zA-Z][a-zA-Z0-9_]*"|[a-zA-Z][a-zA-Z0-9_]*)`)
	litRange := r.FindStringIndex(s)
	if len(litRange) >= 2 {
		return &Lexem{LexemPointedIdent, strings.ReplaceAll(s[0:litRange[1]], `"`, ``)}, s[litRange[1]:]
	}
	return nil, s
}

func getIdentOrPointedIdent(s string) (*Lexem, string) {
	var l *Lexem
	if l, s = getPointedIdent(s); l != nil {
		return l, s
	}
	return getIdent(s)
}

func getArithmeticOp(s string) (*Lexem, string) {
	s = skipBlank(s)
	r := regexp.MustCompile(`^[\+\-*/%]`)
	litRange := r.FindStringIndex(s)
	if len(litRange) >= 2 {
		return &Lexem{LexemArithmeticOp, s[0:litRange[1]]}, s[litRange[1]:]
	}
	return nil, s
}

func getLogicalOp(s string) (*Lexem, string) {
	s = skipBlank(s)
	r := regexp.MustCompile(`^(?i)(OR|AND|=|!=|>|<|>=|<=)`)
	litRange := r.FindStringIndex(s)
	if len(litRange) >= 2 {
		return &Lexem{LexemLogicalOp, strings.ToUpper(s[0:litRange[1]])}, s[litRange[1]:]
	}
	return nil, s
}

func getParenthesis(s string) (*Lexem, string) {
	s = skipBlank(s)
	r := regexp.MustCompile(`^[()]`)
	litRange := r.FindStringIndex(s)
	if len(litRange) >= 2 {
		return &Lexem{LexemParenthesis, s[0:litRange[1]]}, s[litRange[1]:]
	}
	return nil, s
}

func getCqlOp(s string) (*Lexem, string) {
	s = skipBlank(s)
	r := regexp.MustCompile(`^(?i)(NOT\s+IN|IN|!~|~)`)
	litRange := r.FindStringIndex(s)
	if len(litRange) >= 2 {
		return &Lexem{LexemCqlOp, strings.ToUpper(s[0:litRange[1]])}, s[litRange[1]:]
	}
	return nil, s
}

func getComma(s string) (*Lexem, string) {
	s = skipBlank(s)
	r := regexp.MustCompile(`^,`)
	litRange := r.FindStringIndex(s)
	if len(litRange) >= 2 {
		return &Lexem{LexemComma, s[0:litRange[1]]}, s[litRange[1]:]
	}
	return nil, s
}
func getSemicolon(s string) (*Lexem, string) {
	s = skipBlank(s)
	r := regexp.MustCompile(`^;`)
	litRange := r.FindStringIndex(s)
	if len(litRange) >= 2 {
		return &Lexem{LexemSemicolon, s[0:litRange[1]]}, s[litRange[1]:]
	}
	return nil, s
}

func getSelectExpression(s string) ([]*Lexem, string) {
	var l *Lexem
	exp := make([]*Lexem, 0)
	for {
		l, s = getKeyword(s, `(?i)FROM`, false)
		if l != nil {
			break
		}
		if l, s = getAs(s); l != nil {
			exp = append(exp, l)
			continue
		}
		if l, s = getBoolLiteral(s); l != nil {
			exp = append(exp, l)
			continue
		}
		if l, s = getNull(s); l != nil {
			exp = append(exp, l)
			continue
		}
		if l, s = getStringLiteral(s); l != nil {
			exp = append(exp, l)
			continue
		}
		if len(exp) == 0 || exp[len(exp)-1].V == "(" || exp[len(exp)-1].T == LexemArithmeticOp || exp[len(exp)-1].T == LexemLogicalOp {
			// No arithmetic op allowed here
			if l, s = getNumberLiteral(s); l != nil {
				exp = append(exp, l)
				continue
			}
		} else {
			// No number literal allowed here
			if l, s = getArithmeticOp(s); l != nil {
				exp = append(exp, l)
				continue
			}
		}
		if l, s = getLogicalOp(s); l != nil {
			exp = append(exp, l)
			continue
		}
		if l, s = getParenthesis(s); l != nil {
			exp = append(exp, l)
			continue
		}
		if l, s = getIdent(s); l != nil {
			exp = append(exp, l)
			continue
		}
		break
	}
	return exp, s
}

func getSelectExpressions(s string) ([][]*Lexem, string) {
	var l *Lexem
	exps := make([][]*Lexem, 0)
	for {
		l, s = getComma(s)
		if l != nil {
			continue
		}
		var exp []*Lexem
		exp, s = getSelectExpression(s)
		if len(exp) == 0 {
			break
		}
		convertLogicalOpLexemsForAstParser(exp)
		exps = append(exps, exp)
	}

	return exps, s
}

// Returns indices of the first found "Ident,IN,(...)"" lexem sequence
func findInNotInLexem(lexems []*Lexem) (int, int) {
	for i := range len(lexems) {
		if lexems[i].T == LexemCqlOp && (lexems[i].V == "IN" || lexems[i].V == "NOT IN") && i > 0 && lexems[i-1].T == LexemIdent && i < len(lexems)-2 && lexems[i+1].V == "(" {
			startIdx := i - 1
			endIdx := i + 2 // Start with the first arg of the IN/NOT IN sequence
			for endIdx < len(lexems) {
				if lexems[endIdx].V == ")" {
					return startIdx, endIdx
				}
				if lexems[endIdx].T != LexemComma && lexems[endIdx].T != LexemNumberLiteral && lexems[endIdx].T != LexemStringLiteral && lexems[endIdx].T != LexemBoolLiteral {
					// Non-literal among IN arguments
					return 0, 0
				}
				endIdx++
			}
			// Closing parenthesis not found, give up
			return 0, 0
		}
	}
	// No candidates found, give up
	return 0, 0
}

// Convert CQL logical ops to Go
func convertLogicalOpLexemsForAstParser(lexems []*Lexem) {
	for i := range len(lexems) {
		if lexems[i].T == LexemLogicalOp && lexems[i].V == "=" {
			lexems[i].V = "=="
		}
		if lexems[i].T == LexemLogicalOp && lexems[i].V == "AND" {
			lexems[i].V = "&&"
		}
		if lexems[i].T == LexemLogicalOp && lexems[i].V == "OR" {
			lexems[i].V = "||"
		}
	}
}

// Convert IN/NOT IN to a series of Go ==/!=
func convertInNotInLexemsForAstParser(lexems []*Lexem) ([]*Lexem, error) {
	for {
		startIdx, endIdx := findInNotInLexem(lexems)
		if endIdx == 0 {
			break
		}
		newLexems := make([]*Lexem, 0)
		newLexems = append(newLexems, lexems[0:startIdx]...)

		curValLexemIdx := startIdx + 3                               // First literal in the IN list of arguments
		newLexems = append(newLexems, &Lexem{LexemParenthesis, "("}) // Wrap our series of OR conditions with ()
		for curValLexemIdx < endIdx {
			if lexems[curValLexemIdx].T != LexemComma {
				if lexems[curValLexemIdx-1].V != "(" {
					newLexems = append(newLexems, &Lexem{LexemLogicalOp, "||"})
				}
				newLexems = append(newLexems, lexems[startIdx])
				switch lexems[startIdx+1].V {
				case "IN":
					newLexems = append(newLexems, &Lexem{LexemLogicalOp, "=="})
				case "NOT IN":
					newLexems = append(newLexems, &Lexem{LexemLogicalOp, "!="})
				default:
					return nil, fmt.Errorf("unexpected IN/NOT IN lexem, dev error: %s", lexems[startIdx+1].V)
				}
				newLexems = append(newLexems, lexems[curValLexemIdx])
			}
			curValLexemIdx++
		}
		newLexems = append(newLexems, &Lexem{LexemParenthesis, ")"}) // Wrap our series of OR conditions with ()
		newLexems = append(newLexems, lexems[endIdx+1:]...)
		lexems = newLexems
	}
	return lexems, nil
}

func getWhereExpression(s string) ([]*Lexem, string, error) {
	var l *Lexem
	exp := make([]*Lexem, 0)
	for {
		// All possible stopwords for SELECT/UPDATE/DELETE
		l, s = getKeyword(s, `(?i)(GROUP\s+BY|ORDER\s+BY|LIMIT|OFFSET|ALLOW\s+FILTERING|IF\s+EXISTS)`, false)
		if l != nil {
			break
		}
		// Comma can be part of IN
		if l, s = getComma(s); l != nil {
			exp = append(exp, l)
			continue
		}
		if l, s = getBoolLiteral(s); l != nil {
			exp = append(exp, l)
			continue
		}
		if l, s = getNull(s); l != nil {
			exp = append(exp, l)
			continue
		}
		if l, s = getStringLiteral(s); l != nil {
			exp = append(exp, l)
			continue
		}
		if len(exp) == 0 || exp[len(exp)-1].V == "(" || exp[len(exp)-1].T == LexemArithmeticOp || exp[len(exp)-1].T == LexemLogicalOp {
			// No arithmetic op allowed here
			if l, s = getNumberLiteral(s); l != nil {
				exp = append(exp, l)
				continue
			}
		} else {
			// No number literal allowed here
			if l, s = getArithmeticOp(s); l != nil {
				exp = append(exp, l)
				continue
			}
		}
		if l, s = getLogicalOp(s); l != nil {
			exp = append(exp, l)
			continue
		}
		if l, s = getParenthesis(s); l != nil {
			exp = append(exp, l)
			continue
		}
		if l, s = getCqlOp(s); l != nil {
			exp = append(exp, l)
			continue
		}
		if l, s = getIdent(s); l != nil {
			exp = append(exp, l)
			continue
		}
		break
	}

	convertLogicalOpLexemsForAstParser(exp)

	var err error
	if exp, err = convertInNotInLexemsForAstParser(exp); err != nil {
		return nil, s, fmt.Errorf("cannot convert IN/NOT IN expression: %s", err.Error())
	}
	return exp, s, nil
}

func getKeyValuePair(s string) (*KeyValuePair, string, error) {
	s = skipBlank(s)
	var l *Lexem
	if l, s = getKeyword(s, `}`, false); l != nil {
		return nil, s, nil
	}
	kvPair := KeyValuePair{}
	l, s = getStringLiteral(s)
	if l != nil {
		kvPair.K = l.V
		l, s = getKeyword(s, `:`, true)
		if l.V == ":" {
			l, s = getStringLiteral(s)
			if l != nil {
				kvPair.V = l
				return &kvPair, s, nil
			}
			l, s = getNumberLiteral(s)
			if l != nil {
				kvPair.V = l
				return &kvPair, s, nil
			}
			return nil, s, fmt.Errorf("cannot parse kv pair value: %s", s)
		}
		return nil, s, fmt.Errorf("cannot parse kv pair colon: %s", s)
	}
	return nil, s, fmt.Errorf("cannot parse kv pair key: %s", s)
}

func getKeyValuePairList(s string) ([]*KeyValuePair, string, error) {
	var l *Lexem
	l, s = getKeyword(s, `{`, true)
	if l == nil {
		return nil, s, fmt.Errorf("cannot parse kv pair list, { expected: %s", s)
	}
	kvPairList := make([]*KeyValuePair, 0)
	for {
		l, s = getComma(s)
		if l != nil {
			continue
		}
		l, s = getKeyword(s, `}`, true)
		if l != nil {
			break
		}
		var kvPair *KeyValuePair
		var err error
		kvPair, s, err = getKeyValuePair(s)
		if err != nil {
			return nil, s, fmt.Errorf("cannot parse kv pair: %s", s)
		}
		if kvPair != nil {
			kvPairList = append(kvPairList, kvPair)
		}
	}
	return kvPairList, s, nil
}

func getColumnDef(s string) (*CreateTableColumnDef, string, error) {
	s = skipBlank(s)
	var l *Lexem
	if l, s = getKeyword(s, `\)`, false); l != nil {
		return nil, s, nil
	}
	def := CreateTableColumnDef{}
	l, s = getIdent(s)
	if l != nil {
		def.Name = l.V
		l, s = getKeyword(s, `(?i)(BIGINT|DECIMAL|DOUBLE|TEXT|BOOLEAN|TIMESTAMP)`, true)
		if l != nil {
			def.Type = l.V
			return &def, s, nil
		}
		return nil, s, fmt.Errorf("cannot parse column def type: %s", s)
	}
	return nil, s, fmt.Errorf("cannot parse column def name: %s", s)
}

func getColumnDefList(s string) ([]*CreateTableColumnDef, string, error) {
	var l *Lexem
	defList := make([]*CreateTableColumnDef, 0)
	for {
		l, s = getComma(s)
		if l != nil {
			continue
		}
		// PRIMARY KEY must be there, Cassandra does not allow tables without it
		l, s = getKeyword(s, `(?i)(PRIMARY\s+KEY)`, false)
		if l != nil {
			break
		}

		var def *CreateTableColumnDef
		var err error
		def, s, err = getColumnDef(s)
		if err != nil {
			return nil, s, fmt.Errorf("cannot parse column def: %s", s)
		}
		if def == nil {
			return nil, s, fmt.Errorf("missing column def or missing PRIMARY KEY: %s", s)
		}
		defList = append(defList, def)
	}
	return defList, s, nil
}

func getPartitionAndClusteringKeys(s string) ([]string, []string, string, error) {
	var l *Lexem
	l, s = getKeyword(s, `\(`, true)
	if l == nil {
		return nil, nil, s, fmt.Errorf("cannot parse keys, ( expected: %s", s)
	}
	partitionKeys := make([]string, 0)
	clusteringKeys := make([]string, 0)

	partitionKeysListComplete := false

	l, s = getKeyword(s, `\(`, true)
	if l != nil {
		for {
			l, s = getComma(s)
			if l != nil {
				continue
			}
			l, s = getKeyword(s, `\)`, true)
			if l != nil {
				break
			}
			l, s = getIdent(s)
			if l == nil {
				return nil, nil, s, fmt.Errorf("cannot parse partition key: %s", s)
			}
			partitionKeys = append(partitionKeys, l.V)
		}
		partitionKeysListComplete = true
	}
	for {
		l, s = getComma(s)
		if l != nil {
			continue
		}
		l, s = getKeyword(s, `\)`, true)
		if l != nil {
			break
		}
		l, s = getIdent(s)
		if l == nil {
			return nil, nil, s, fmt.Errorf("cannot parse partition/clustering key: %s", s)
		}

		if !partitionKeysListComplete {
			partitionKeys = append(partitionKeys, l.V)
			partitionKeysListComplete = true
		} else {
			clusteringKeys = append(clusteringKeys, l.V)
		}
	}
	return partitionKeys, clusteringKeys, s, nil
}

func getColumnSetExpressionLexems(s string) ([]*Lexem, string) {
	var l *Lexem
	lexems := make([]*Lexem, 0)
	for {
		l, s = getKeyword(s, `(?i)WHERE`, false)
		if l != nil {
			break
		}
		if l, s = getBoolLiteral(s); l != nil {
			lexems = append(lexems, l)
			continue
		}
		if l, s = getNull(s); l != nil {
			lexems = append(lexems, l)
			continue
		}
		if l, s = getStringLiteral(s); l != nil {
			lexems = append(lexems, l)
			continue
		}
		if len(lexems) == 0 || lexems[len(lexems)-1].V == "(" || lexems[len(lexems)-1].T == LexemArithmeticOp || lexems[len(lexems)-1].T == LexemLogicalOp {
			// No arithmetic op allowed here
			if l, s = getNumberLiteral(s); l != nil {
				lexems = append(lexems, l)
				continue
			}
		} else {
			// No number literal allowed here
			if l, s = getArithmeticOp(s); l != nil {
				lexems = append(lexems, l)
				continue
			}
		}
		if l, s = getLogicalOp(s); l != nil {
			lexems = append(lexems, l)
			continue
		}
		if l, s = getParenthesis(s); l != nil {
			lexems = append(lexems, l)
			continue
		}
		if l, s = getIdent(s); l != nil {
			lexems = append(lexems, l)
			continue
		}
		break
	}
	return lexems, s
}

func getColumnSetExpressions(s string) ([]*ColumnSetExp, string, error) {
	s = skipBlank(s)
	var l *Lexem
	columnsSetExpList := make([]*ColumnSetExp, 0)
	for {
		l, s = getComma(s)
		if l != nil {
			continue
		}
		l, s = getKeyword(s, `(?i)WHERE`, false)
		if l != nil {
			break
		}
		l, s = getIdent(s)
		if l == nil {
			break
		}

		exp := ColumnSetExp{Name: l.V}
		l, s = getKeyword(s, `=`, true)
		if l == nil {
			return nil, s, errors.New("expected =")
		}
		exp.ExpLexems, s = getColumnSetExpressionLexems(s)
		convertLogicalOpLexemsForAstParser(exp.ExpLexems)
		columnsSetExpList = append(columnsSetExpList, &exp)
	}
	return columnsSetExpList, s, nil
}

func lexemsToString(lexems []*Lexem) (string, string, error) {
	sb := strings.Builder{}
	var as string
	for i, l := range lexems {
		if l.T == LexemAs {
			if len(lexems) != i+2 {
				return "", "", fmt.Errorf("unexpected select exp with AS length, expected %d, got %d", i+2, len(lexems))
			}
			if lexems[i+1].T != LexemIdent {
				return "", "", fmt.Errorf("unexpected select exp with AS last lexem, expected ident, got (%d,%s)", lexems[i+1].T, lexems[i+1].V)
			}
			as = lexems[i+1].V
			break
		}
		if sb.Len() > 0 {
			sb.WriteString(" ")
		}
		switch l.T {
		case LexemArithmeticOp, LexemLogicalOp, LexemIdent, LexemBoolLiteral, LexemNumberLiteral, LexemParenthesis:
			sb.WriteString(fmt.Sprintf("%s", l.V))
		case LexemStringLiteral:
			sb.WriteString(fmt.Sprintf("`%s`", l.V))
		case LexemNull:
			sb.WriteString("nil")
		case LexemComma, LexemPointedIdent, LexemSemicolon, LexemCqlOp, LexemKeyword:
			return "", "", fmt.Errorf("unexpected lexem (%d,%s)", l.T, l.V)
		default:
			return "", "", fmt.Errorf("unknown lexem (%d,%s)", l.T, l.V)
		}
	}
	return sb.String(), as, nil
}

func lexemsToAst(lexems []*Lexem) (ast.Expr, string, error) {
	s, as, err := lexemsToString(lexems)
	if err != nil {
		return nil, "", err
	}
	if as == "" {
		as = s
	}
	exp, err := parser.ParseExpr(s)
	return exp, as, err
}

func parseCreateKeyspace(s string) (*CommandCreateKeyspace, string, error) {
	var l *Lexem
	l, s = getKeyword(s, `(?i)CREATE\s+KEYSPACE`, true)
	if l == nil {
		return nil, s, fmt.Errorf("expected CREATE KEYSPACE: %s", s)
	}
	cmd := CommandCreateKeyspace{}
	l, s = getKeyword(s, `(?i)IF\s+NOT\s+EXISTS`, true)
	if l != nil {
		cmd.IfNotExists = true
	}
	l, s = getIdent(s)
	if l != nil {
		cmd.KeyspaceName = l.V
	}
	l, s = getKeyword(s, `(?i)WITH\s+REPLICATION`, true)
	if l != nil {
		var err error
		cmd.WithReplication, s, err = getKeyValuePairList(s)
		if err != nil {
			return nil, s, fmt.Errorf("cannot parse with replication: %s", err.Error())
		}
	}

	return &cmd, s, nil
}

func parseUseKeyspace(s string) (*CommandUseKeyspace, string, error) {
	var l *Lexem
	l, s = getKeyword(s, `(?i)USE`, true)
	if l == nil {
		return nil, s, fmt.Errorf("expected USE: %s", s)
	}
	cmd := CommandUseKeyspace{}
	l, s = getIdent(s)
	if l != nil {
		cmd.KeyspaceName = l.V
	}
	return &cmd, s, nil
}

func parseDropKeyspace(s string) (*CommandDropKeyspace, string, error) {
	var l *Lexem
	l, s = getKeyword(s, `(?i)DROP\s+KEYSPACE`, true)
	if l == nil {
		return nil, s, fmt.Errorf("expected DROP KEYSPACE: %s", s)
	}
	cmd := CommandDropKeyspace{}
	l, s = getKeyword(s, `(?i)IF\s+EXISTS`, true)
	if l != nil {
		cmd.IfExists = true
	}
	l, s = getIdent(s)
	if l != nil {
		cmd.KeyspaceName = l.V
	}
	return &cmd, s, nil
}

func parseCreateTable(s string) (*CommandCreateTable, string, error) {
	var l *Lexem
	var err error
	l, s = getKeyword(s, `(?i)CREATE\s+TABLE`, true)
	if l == nil {
		return nil, s, fmt.Errorf("expected CREATE TABLE: %s", s)
	}
	cmd := CommandCreateTable{}
	l, s = getKeyword(s, `(?i)IF\s+NOT\s+EXISTS`, true)
	if l != nil {
		cmd.IfNotExists = true
	}
	l, s = getIdentOrPointedIdent(s)
	if l == nil {
		return nil, s, fmt.Errorf("expected table name ident: %s", s)
	}
	if l.T == LexemPointedIdent {
		ksTable := strings.Split(l.V, ".")
		cmd.CtxKeyspace = ksTable[0]
		cmd.TableName = ksTable[1]
	} else {
		cmd.TableName = l.V
	}

	l, s = getKeyword(s, `\(`, true)
	if l == nil {
		return nil, s, fmt.Errorf("cannot parse column def list, ( expected: %s", s)
	}

	cmd.ColumnDefs, s, err = getColumnDefList(s)
	if err != nil {
		return nil, s, err
	}

	l, s = getKeyword(s, `(?i)PRIMARY\s+KEY`, true)
	if l == nil {
		return nil, s, fmt.Errorf("expected PRIMARY KEY: %s", s)
	}

	cmd.PartitionKeyColumns, cmd.ClusteringKeyColumns, s, err = getPartitionAndClusteringKeys(s)
	if err != nil {
		return nil, s, err
	}

	l, s = getKeyword(s, `\)`, true)
	if l == nil {
		return nil, s, fmt.Errorf("cannot parse column def list, ) expected: %s", s)
	}

	l, s = getKeyword(s, `(?i)WITH\s+CLUSTERING\s+ORDER\s+BY`, true)
	if l != nil {
		l, s = getKeyword(s, `\(`, true)
		if l == nil {
			return nil, s, fmt.Errorf("cannot parse WITH CLUSTERING ORDER BY, ( expected: %s", s)
		}
		cmd.ClusteringOrderBy = make([]*OrderByField, 0)
		for {
			l, s = getComma(s)
			if l != nil {
				continue
			}
			l, s = getKeyword(s, `\)`, true)
			if l != nil {
				break
			}
			var lField *Lexem
			lField, s = getIdent(s)
			if lField == nil {
				return nil, s, fmt.Errorf("expected clustering order by ident: %s", s)
			}
			var lAscDesc *Lexem
			lAscDesc, s = getKeyword(s, `(?i)(ASC|DESC)`, true)
			if lAscDesc == nil {
				return nil, s, fmt.Errorf("expected clustering order by asc or desc: %s", s)
			}

			clusteringOrder := stringToClusteringOrderType(lAscDesc.V)
			if clusteringOrder == ClusteringOrderNone {
				return nil, s, fmt.Errorf("expected clustering order by asc or desc, got %s", lAscDesc.V)
			}
			cmd.ClusteringOrderBy = append(cmd.ClusteringOrderBy, &OrderByField{lField.V, clusteringOrder})
		}
		for _, clustOrderBy := range cmd.ClusteringOrderBy {
			var clusteringKeyFieldFound bool
			for _, clusteringKeyColumn := range cmd.ClusteringKeyColumns {
				if clusteringKeyColumn == clustOrderBy.FieldName {
					clusteringKeyFieldFound = true
					break
				}
			}
			if !clusteringKeyFieldFound {
				return nil, s, fmt.Errorf("clustering order field %s specified, but it's not among clustering keys", clustOrderBy.FieldName)
			}
		}
	}

	if len(cmd.ColumnDefs) == 0 {
		return nil, s, errors.New("cannot parse CREATE TABLE with empty columnn def list")
	}

	if len(cmd.PartitionKeyColumns) == 0 {
		return nil, s, errors.New("cannot parse CREATE TABLE with empty partition column list")
	}

	for _, fieldName := range cmd.PartitionKeyColumns {
		var fieldFound bool
		for i := range len(cmd.ColumnDefs) {
			if fieldName == cmd.ColumnDefs[i].Name {
				fieldFound = true
				break
			}
		}
		if !fieldFound {
			return nil, s, fmt.Errorf("partition key %s not found in column definitions", fieldName)
		}
	}

	for _, fieldName := range cmd.ClusteringKeyColumns {
		var fieldFound bool
		for i := range len(cmd.ColumnDefs) {
			if fieldName == cmd.ColumnDefs[i].Name {
				fieldFound = true
				break
			}
		}
		if !fieldFound {
			return nil, s, fmt.Errorf("clustering key %s not found in column definitions", fieldName)
		}
	}

	fieldMap := map[string]struct{}{}
	for _, fieldName := range cmd.PartitionKeyColumns {
		if _, ok := fieldMap[fieldName]; ok {
			return nil, s, fmt.Errorf("partition key %s duplication", fieldName)
		}
		fieldMap[fieldName] = struct{}{}
	}
	for _, fieldName := range cmd.ClusteringKeyColumns {
		if _, ok := fieldMap[fieldName]; ok {
			return nil, s, fmt.Errorf("clustering key %s duplication", fieldName)
		}
		fieldMap[fieldName] = struct{}{}
	}

	return &cmd, s, nil
}

func parseDropTable(s string) (*CommandDropTable, string, error) {
	var l *Lexem
	l, s = getKeyword(s, `(?i)DROP\s+TABLE`, true)
	if l == nil {
		return nil, s, fmt.Errorf("expected DROP TABLE: %s", s)
	}
	cmd := CommandDropTable{}
	l, s = getKeyword(s, `(?i)IF\s+EXISTS`, true)
	if l != nil {
		cmd.IfExists = true
	}
	l, s = getIdentOrPointedIdent(s)
	if l == nil {
		return nil, s, fmt.Errorf("expected table name ident: %s", s)
	}
	if l.T == LexemPointedIdent {
		ksTable := strings.Split(l.V, ".")
		cmd.CtxKeyspace = ksTable[0]
		cmd.TableName = ksTable[1]
	} else {
		cmd.TableName = l.V
	}

	return &cmd, s, nil
}

func parseSelect(s string) (*CommandSelect, string, error) {
	var l *Lexem
	var err error
	l, s = getKeyword(s, `(?i)SELECT`, true)
	if l == nil {
		return nil, s, fmt.Errorf("expected SELECT: %s", s)
	}
	cmd := CommandSelect{}
	l, s = getKeyword(s, `(?i)DISTINCT`, true)
	if l != nil {
		cmd.Distinct = true
	}
	l, s = getKeyword(s, `(?i)JSON`, true)
	if l != nil {
		return nil, s, errors.New("JSON not supported")
	}
	cmd.SelectExpLexems, s = getSelectExpressions(s)
	if len(cmd.SelectExpLexems) == 0 {
		return nil, s, fmt.Errorf("expected select expressions: %s", s)
	}
	l, s = getKeyword(s, `(?i)FROM`, true)
	if l == nil {
		return nil, s, fmt.Errorf("expected FROM: %s", s)
	}
	l, s = getIdentOrPointedIdent(s)
	if l == nil {
		return nil, s, fmt.Errorf("expected from table ident: %s", s)
	}
	if l.T == LexemPointedIdent {
		ksTable := strings.Split(l.V, ".")
		cmd.CtxKeyspace = ksTable[0]
		cmd.TableName = ksTable[1]
	} else {
		cmd.TableName = l.V
	}

	l, s = getKeyword(s, `(?i)WHERE`, true)
	if l != nil {
		cmd.WhereExpLexems, s, err = getWhereExpression(s)
		if err != nil {
			return nil, s, fmt.Errorf("cannot parse where expression: %s", err.Error())
		}
		if cmd.WhereExpLexems == nil {
			return nil, s, fmt.Errorf("expected where expression: %s", s)
		}
	}
	l, s = getKeyword(s, `(?i)GROUP\s+BY`, true)
	if l != nil {
		return nil, s, errors.New("GROUP BY not supported")
	}
	l, s = getKeyword(s, `(?i)ORDER\s+BY`, true)
	if l != nil {
		cmd.OrderByFields = make([]*OrderByField, 0)
		for {
			l, s = getComma(s)
			if l != nil {
				continue
			}
			l, s = getKeyword(s, `(?i)(PER\s+PARTITION\s+LIMIT|LIMIT|ALLOW\sFILTERING|OFFSET)`, false)
			if l != nil {
				break
			}
			var lField *Lexem
			lField, s = getIdent(s)
			if lField == nil {
				return nil, s, fmt.Errorf("expected order by ident: %s", s)
			}
			var lAscDesc *Lexem
			lAscDesc, s = getKeyword(s, `(?i)(ASC|DESC)`, true)
			if lAscDesc == nil {
				return nil, s, fmt.Errorf("expected order by asc or desc: %s", s)
			}
			clusteringOrder := stringToClusteringOrderType(lAscDesc.V)
			if clusteringOrder == ClusteringOrderNone {
				return nil, s, fmt.Errorf("expected order by asc or desc, got %s", lAscDesc.V)
			}
			cmd.OrderByFields = append(cmd.OrderByFields, &OrderByField{lField.V, clusteringOrder})
		}
	}
	l, s = getKeyword(s, `(?i)PER\s+PARTITION\s+LIMIT`, true)
	if l != nil {
		return nil, s, errors.New("PER PARTITION LIMIT not supported")
	}
	l, s = getKeyword(s, `(?i)LIMIT`, true)
	if l != nil {
		cmd.Limit, s = getNumberLiteral(s)
		if cmd.Limit == nil {
			return nil, s, fmt.Errorf("expected limit number: %s", s)
		}
	}
	l, s = getKeyword(s, `(?i)OFFSET`, true)
	if l != nil {
		return nil, s, errors.New("OFFSET not supported")
	}
	l, s = getKeyword(s, `(?i)ALLOW\sFILTERING`, true)
	if l != nil {
		return nil, s, errors.New("ALLOW FILTERING not supported")
	}

	cmd.SelectExpAsts = make([]ast.Expr, len(cmd.SelectExpLexems))
	cmd.SelectExpNames = make([]string, len(cmd.SelectExpLexems))
	for i, selectExp := range cmd.SelectExpLexems {
		cmd.SelectExpAsts[i], cmd.SelectExpNames[i], err = lexemsToAst(selectExp)
		if err != nil {
			return nil, s, fmt.Errorf("cannot build ast from select expression: %s", err.Error())
		}
	}

	if len(cmd.WhereExpLexems) > 0 {
		cmd.WhereExpAst, _, err = lexemsToAst(cmd.WhereExpLexems)
		if err != nil {
			return nil, s, fmt.Errorf("cannot build ast from where expression: %s", err.Error())
		}
	}

	return &cmd, s, nil
}

func parseInsert(s string) (*CommandInsert, string, error) {
	var l *Lexem
	l, s = getKeyword(s, `(?i)INSERT\s+INTO`, true)
	if l == nil {
		return nil, s, fmt.Errorf("expected INSERT INTO: %s", s)
	}
	cmd := CommandInsert{}
	l, s = getIdentOrPointedIdent(s)
	if l == nil {
		return nil, s, fmt.Errorf("expected update table ident: %s", s)
	}
	if l.T == LexemPointedIdent {
		ksTable := strings.Split(l.V, ".")
		cmd.CtxKeyspace = ksTable[0]
		cmd.TableName = ksTable[1]
	} else {
		cmd.TableName = l.V
	}

	l, s = getKeyword(s, `\(`, true)
	if l == nil {
		return nil, s, fmt.Errorf("expected (: %s", s)
	}
	cmd.ColumnNames = make([]string, 0)
	for {
		l, s = getComma(s)
		if l != nil {
			continue
		}
		l, s = getKeyword(s, `\)`, false)
		if l != nil {
			break
		}
		l, s = getIdent(s)
		if l == nil {
			return nil, s, fmt.Errorf("expected column name: %s", s)
		}
		cmd.ColumnNames = append(cmd.ColumnNames, l.V)
	}
	l, s = getKeyword(s, `\)`, true)
	if l == nil {
		return nil, s, fmt.Errorf("expected ): %s", s)
	}

	l, s = getKeyword(s, `(?i)VALUES`, true)
	if l == nil {
		return nil, s, fmt.Errorf("expected VALUES: %s", s)
	}

	l, s = getKeyword(s, `\(`, true)
	if l == nil {
		return nil, s, fmt.Errorf("expected (: %s", s)
	}
	cmd.ColumnValues = make([]*Lexem, 0)
	for {
		l, s = getComma(s)
		if l != nil {
			continue
		}
		l, s = getKeyword(s, `\)`, false)
		if l != nil {
			break
		}
		if l, s = getBoolLiteral(s); l != nil {
			cmd.ColumnValues = append(cmd.ColumnValues, l)
			continue
		}
		if l, s = getNull(s); l != nil {
			cmd.ColumnValues = append(cmd.ColumnValues, l)
			continue
		}
		if l, s = getStringLiteral(s); l != nil {
			cmd.ColumnValues = append(cmd.ColumnValues, l)
			continue
		}
		if l, s = getNumberLiteral(s); l != nil {
			cmd.ColumnValues = append(cmd.ColumnValues, l)
			continue
		}
		return nil, s, fmt.Errorf("expected  a literal: %s", s)
	}
	l, s = getKeyword(s, `\)`, true)
	if l == nil {
		return nil, s, fmt.Errorf("expected ): %s", s)
	}

	l, s = getKeyword(s, `(?i)IF\s+NOT\s+EXISTS`, true)
	if l != nil {
		cmd.IfNotExists = true
	}

	if len(cmd.ColumnNames) == 0 {
		return nil, s, errors.New("column list cannot be empty")
	}

	if len(cmd.ColumnNames) != len(cmd.ColumnValues) {
		return nil, s, fmt.Errorf("value list length (%d) should match column list length (%d)", len(cmd.ColumnValues), len(cmd.ColumnNames))
	}

	for _, colValue := range cmd.ColumnValues {
		if colValue.T != LexemStringLiteral && colValue.T != LexemNumberLiteral && colValue.T != LexemBoolLiteral && colValue.T != LexemNull {
			return nil, s, fmt.Errorf("insert value list can contain only number, string, bool literals, got %s", colValue.V)
		}
	}

	return &cmd, s, nil
}

func parseUpdate(s string) (*CommandUpdate, string, error) {
	var l *Lexem
	l, s = getKeyword(s, `(?i)UPDATE`, true)
	if l == nil {
		return nil, s, fmt.Errorf("expected UPDATE: %s", s)
	}
	cmd := CommandUpdate{}
	l, s = getIdentOrPointedIdent(s)
	if l == nil {
		return nil, s, fmt.Errorf("expected update table ident: %s", s)
	}
	if l.T == LexemPointedIdent {
		ksTable := strings.Split(l.V, ".")
		cmd.CtxKeyspace = ksTable[0]
		cmd.TableName = ksTable[1]
	} else {
		cmd.TableName = l.V
	}

	l, s = getKeyword(s, `(?i)SET`, true)
	if l == nil {
		return nil, s, fmt.Errorf("expected SET: %s", s)
	}

	var err error
	cmd.ColumnSetExpressions, s, err = getColumnSetExpressions(s)
	if err != nil {
		return nil, s, err
	}

	l, s = getKeyword(s, `(?i)WHERE`, true)
	if l != nil {
		cmd.WhereExpLexems, s, err = getWhereExpression(s)
		if err != nil {
			return nil, s, fmt.Errorf("cannot parse where expression: %s", err.Error())
		}
		if cmd.WhereExpLexems == nil {
			return nil, s, fmt.Errorf("expected where expression: %s", s)
		}
	}

	l, s = getKeyword(s, `(?i)IF\s+EXISTS`, true)
	if l != nil {
		cmd.IfExists = true
	}

	l, s = getKeyword(s, `(?i)IF`, true)
	if l != nil {
		return nil, s, fmt.Errorf("IF not upported, it affects performance: %s", s)
	}

	cmd.ColumnSetExpAsts = make([]ast.Expr, len(cmd.ColumnSetExpressions))
	for i, columnSetExp := range cmd.ColumnSetExpressions {
		cmd.ColumnSetExpAsts[i], _, err = lexemsToAst(columnSetExp.ExpLexems)
		if err != nil {
			return nil, s, fmt.Errorf("cannot build ast from column set expression: %s", err.Error())
		}
	}

	if len(cmd.WhereExpLexems) > 0 {
		cmd.WhereExpAst, _, err = lexemsToAst(cmd.WhereExpLexems)
		if err != nil {
			return nil, s, fmt.Errorf("cannot build ast from where expression: %s", err.Error())
		}
	}

	return &cmd, s, nil
}

func parseDelete(s string) (*CommandDelete, string, error) {
	var l *Lexem
	var err error
	l, s = getKeyword(s, `(?i)DELETE`, true)
	if l == nil {
		return nil, s, fmt.Errorf("expected DELETE: %s", s)
	}
	l, s = getKeyword(s, `(?i)FROM`, true)
	if l == nil {
		return nil, s, fmt.Errorf("expected FROM, column list not supported here: %s", s)
	}
	cmd := CommandDelete{}
	l, s = getIdentOrPointedIdent(s)
	if l == nil {
		return nil, s, fmt.Errorf("expected update table ident: %s", s)
	}
	if l.T == LexemPointedIdent {
		ksTable := strings.Split(l.V, ".")
		cmd.CtxKeyspace = ksTable[0]
		cmd.TableName = ksTable[1]
	} else {
		cmd.TableName = l.V
	}

	l, s = getKeyword(s, `(?i)WHERE`, true)
	if l != nil {
		cmd.WhereExpLexems, s, err = getWhereExpression(s)
		if err != nil {
			return nil, s, fmt.Errorf("cannot parse where expression: %s", err.Error())
		}
		if cmd.WhereExpLexems == nil {
			return nil, s, fmt.Errorf("expected where expression: %s", s)
		}
	}

	l, s = getKeyword(s, `(?i)IF\s+EXISTS`, true)
	if l != nil {
		cmd.IfExists = true
	}

	l, s = getKeyword(s, `(?i)IF`, true)
	if l != nil {
		return nil, s, fmt.Errorf("IF not upported, it affects performance: %s", s)
	}

	if len(cmd.WhereExpLexems) > 0 {
		cmd.WhereExpAst, _, err = lexemsToAst(cmd.WhereExpLexems)
		if err != nil {
			return nil, s, fmt.Errorf("cannot build ast from where expression: %s", err.Error())
		}
	}

	return &cmd, s, nil
}

func ParseCommands(s string) ([]Command, error) {
	cmds := make([]Command, 0)
	for {
		var l *Lexem
		l, s = getSemicolon(s)
		if l != nil {
			continue
		}
		if s == "" {
			break
		}
		l, s = getKeyword(s, `(?i)(CREATE\s+KEYSPACE|USE|DROP\s+KEYSPACE|CREATE\s+TABLE|DROP\s+TABLE|SELECT|INSERT\s+INTO|UPDATE|DELETE)`, false)
		if l != nil {
			var cmd Command
			var err error
			switch l.V {
			case "CREATE KEYSPACE":
				cmd, s, err = parseCreateKeyspace(s)
				if err != nil {
					return nil, fmt.Errorf("cannot parse CREATE KEYSPACE: %s", err.Error())
				}
			case "USE":
				cmd, s, err = parseUseKeyspace(s)
				if err != nil {
					return nil, fmt.Errorf("cannot parse USE KEYSPACE: %s", err.Error())
				}
			case "DROP KEYSPACE":
				cmd, s, err = parseDropKeyspace(s)
				if err != nil {
					return nil, fmt.Errorf("cannot parse DROP KEYSPACE: %s", err.Error())
				}
			case "CREATE TABLE":
				cmd, s, err = parseCreateTable(s)
				if err != nil {
					return nil, fmt.Errorf("cannot parse CREATE TABLE: %s", err.Error())
				}
			case "DROP TABLE":
				cmd, s, err = parseDropTable(s)
				if err != nil {
					return nil, fmt.Errorf("cannot parse DROP TABLE: %s", err.Error())
				}
			case "SELECT":
				cmd, s, err = parseSelect(s)
				if err != nil {
					return nil, fmt.Errorf("cannot parse SELECT: %s", err.Error())
				}
			case "INSERT INTO":
				cmd, s, err = parseInsert(s)
				if err != nil {
					return nil, fmt.Errorf("cannot parse INSERT: %s", err.Error())
				}
			case "UPDATE":
				cmd, s, err = parseUpdate(s)
				if err != nil {
					return nil, fmt.Errorf("cannot parse UPDATE: %s", err.Error())
				}
			case "DELETE":
				cmd, s, err = parseDelete(s)
				if err != nil {
					return nil, fmt.Errorf("cannot parse DELETE: %s", err.Error())
				}
			default:
				return nil, fmt.Errorf("unexpected command, dev error: %s", s)
			}
			cmds = append(cmds, cmd)
			continue
		}
		return nil, fmt.Errorf("unexpected command text, a semicolon expected: %s", s)
	}

	// Update keyspace context from "USE <keyspace>"
	curKs := ""
	for i := range len(cmds) {
		cmdUseKeyspace, ok := cmds[i].(*CommandUseKeyspace)
		if ok {
			curKs = cmdUseKeyspace.GetCtxKeyspace()
		} else {
			switch cmds[i].(type) {
			case *CommandCreateTable, *CommandDropTable, *CommandSelect, *CommandInsert, *CommandUpdate, *CommandDelete:
				localCtxKs := cmds[i].GetCtxKeyspace()
				if localCtxKs == "" {
					if curKs == "" {
						return nil, fmt.Errorf("cannot detect keyspace for command %d, are you missing USE <keyspace>?", i)
					}
					cmds[i].SetCtxKeyspace(curKs)
				}
			}
		}
	}
	return cmds, nil
}
