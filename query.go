package serie

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// query.go -- this file handles all of the query building code the lexing and parsing and the query construction from that

const (
	tokSelect = iota
	tokWhere
	tokIdent
	tokInt
	tokFloat
	tokStr
	tokFrom
	tokLt
	tokGt
	tokEq
	tokLParen
	tokRParen
	tokPlus
	tokComma
)

type token struct {
	floatVal float64
	intVal   int64
	content  string
	kind     int
}

type lexer struct {
	idx  int
	data string
}

var keywords = map[string]int{
	"select": tokSelect,
	"where":  tokWhere,
	"from":   tokFrom,
}

var punct = map[byte]int{
	'<': tokLt,
	'>': tokGt,
	'=': tokEq,
	'(': tokLParen,
	')': tokRParen,
	'+': tokPlus,
	',': tokComma,
}

func isPunct(c byte) bool {
	_, ok := punct[c]
	return ok
}

func isLetter(c byte) bool {
	return ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z')
}

func isDigit(c byte) bool {
	return '0' <= c && c <= '9'
}

func (l *lexer) tokenize() ([]token, error) {
	var tokens []token
	for l.idx < len(l.data) {
		c := l.data[l.idx]
		switch {
		case unicode.IsSpace(rune(c)):
			l.idx++
		case isLetter(c):
			startIdx := l.idx
			for l.idx < len(l.data) && (isLetter(l.data[l.idx]) || isDigit(l.data[l.idx])) {
				l.idx++
			}
			identifier := l.data[startIdx:l.idx]
			if keywordType, ok := keywords[strings.ToLower(identifier)]; ok {
				tokens = append(tokens, token{kind: keywordType}) // we can infer the content from the type
			} else {
				tokens = append(tokens, token{kind: tokIdent, content: identifier})
			}
		case isDigit(c):
			startIdx := l.idx
			for l.idx < len(l.data) && isDigit(l.data[l.idx]) {
				l.idx++
			}
			kind := tokInt
			if l.idx < len(l.data) && l.data[l.idx] == '.' {
				l.idx++
				kind = tokFloat
				for l.idx < len(l.data) && isDigit(l.data[l.idx]) {
					l.idx++
				}
			}
			num := l.data[startIdx:l.idx]
			tok := token{kind: kind}
			var err error
			if kind == tokInt {
				tok.intVal, err = strconv.ParseInt(num, 10, 64)
			} else {
				tok.floatVal, err = strconv.ParseFloat(num, 64)
			}
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, tok)
		case c == '"':
			l.idx++
			startIdx := l.idx
			for l.idx < len(l.data) && l.data[l.idx] != '"' {
				l.idx++
			}
			if l.idx == len(l.data) {
				return nil, errors.New("unterminated string literal")
			}
			tokens = append(tokens, token{kind: tokStr, content: l.data[startIdx:l.idx]})
			l.idx++
		case isPunct(c):
			tokens = append(tokens, token{kind: punct[c]})
			l.idx++
		default:
			return nil, fmt.Errorf("unexpected character: %c", c)
		}
	}
	return tokens, nil
}

// Example of query language, it's quite similar to SQL by design.
//   SELECT ts, location, value FROM weather
//     WHERE ts >= '2024-01-10' AND ts <= '2024-01-25' AND location = 'HELSINKI' AND value > -10.0
//
// If we look at what a given datapoint we can relate different parts. 'weather' is the metric
// ts is the built-in name for timestamp, location and note are both tags that the metrics might have.
// and in the condition part the user the user can query based on the timestamp, tags and values.
//
// The query language will also support aggregate functions like AVG, SUM etc.
//   SELECT AVG(value), location FROM weather GROUP BY location
// Here the average temperature is measured for each location in the dataset.

type query struct {
	aggregates []string
	metric     string
	groupBy    map[string]string
	timeStart  int64
	timeEnd    int64
}

type queryNode interface {
	String() string
}

type binopNode struct {
	left  queryNode
	right queryNode

	op token
}

type funcCallNode struct {
	args []queryNode
	name token
}

func (f *funcCallNode) String() string {
	var b strings.Builder
	b.WriteString(f.name.content)
	b.WriteByte('(')
	for idx, arg := range f.args {
		b.WriteString(arg.String())
		if idx < len(f.args)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteByte(')')

	return b.String()
}

func (b *binopNode) String() string {
	return b.left.String() + " " + b.op.content + " " + b.right.String()
}

type selectNode struct {
	columns []queryNode
	from    token
	where   []queryNode
}

func (s *selectNode) String() string {
	var b strings.Builder

	b.WriteString("SELECT\n")
	for i, col := range s.columns {
		b.WriteString("  ")
		b.WriteString(col.String())
		if i < len(s.columns)-1 {
			b.WriteString(",")
		}
		b.WriteRune('\n')
	}

	b.WriteString("FROM\n")
	b.WriteString("  " + s.from.content)
	if len(s.where) > 0 {
		b.WriteString("\nWHERE ")

		for i, cond := range s.where {
			b.WriteString(cond.String())

			if i != len(s.where)-1 {
				b.WriteString(" AND ")
			}
		}
	}

	b.WriteRune('\n')
	return b.String()
}

type literalNode struct {
	lit token
}

func (l *literalNode) String() string {
	return l.lit.content
}

type queryBuilder struct {
	tokens []token
	idx    int
}

func (qb *queryBuilder) expect(kind int) bool {
	if qb.idx >= len(qb.tokens) {
		return false
	}

	return qb.tokens[qb.idx].kind == kind
}

func (qb *queryBuilder) consume(kind int) bool {
	if qb.expect(kind) {
		qb.idx++
		return true
	}

	return false
}

func (qb *queryBuilder) expr() (queryNode, error) {
	var exp queryNode
	canBeFunction := false
	callerToken := token{}
	if qb.expect(tokInt) || qb.expect(tokIdent) || qb.expect(tokStr) {
		if qb.tokens[qb.idx].kind == tokIdent {
			canBeFunction = true
			callerToken = qb.tokens[qb.idx]
		}

		exp = &literalNode{lit: qb.tokens[qb.idx]}
		qb.idx++
	} else {
		return nil, errors.New("no expression")
	}

	if qb.expect(tokLt) || qb.expect(tokEq) || qb.expect(tokPlus) {
		binExp := &binopNode{
			left: exp,
			op:   qb.tokens[qb.idx],
		}

		qb.idx++
		rhs, err := qb.expr()
		if err != nil {
			return nil, err
		}

		binExp.right = rhs
		exp = binExp
	} else if qb.expect(tokLParen) && canBeFunction {
		return qb.parseFuncCall(callerToken)
	}

	return exp, nil
}

func (qb *queryBuilder) parseFuncCall(callerToken token) (queryNode, error) {
	callNode := &funcCallNode{
		name: callerToken,
	}

	if !qb.consume(tokLParen) {
		return nil, errors.New("need parenthesis before call arguments")
	}

	for !qb.expect(tokRParen) {
		if len(callNode.args) > 0 {
			if !qb.consume(tokComma) {
				return nil, errors.New("expected comma")
			}
		}

		colexpr, err := qb.expr()
		if err != nil {
			return nil, err
		}

		callNode.args = append(callNode.args, colexpr)
	}

	if !qb.consume(tokRParen) {
		return nil, errors.New("expected closing call")
	}

	return callNode, nil
}

func (qb *queryBuilder) pselect() (queryNode, error) {
	qb.idx = 0
	if !qb.consume(tokSelect) {
		return nil, errors.New("expected select keyword")
	}

	sn := &selectNode{}
	for !qb.expect(tokFrom) {
		if len(sn.columns) > 0 {
			if !qb.consume(tokComma) {
				return nil, errors.New("expected comma")
			}
		}

		colexpr, err := qb.expr()
		if err != nil {
			return nil, err
		}

		sn.columns = append(sn.columns, colexpr)
	}

	if !qb.consume(tokFrom) {
		return nil, errors.New("expected FROM")
	}

	if !qb.expect(tokIdent) {
		return nil, errors.New("expected FROM")
	}
	sn.from = qb.tokens[qb.idx]
	qb.idx++

	if qb.expect(tokWhere) {
		qb.idx++
		whereexpr, err := qb.expr()
		if err != nil {
			return nil, err
		}

		// TODO: parse multiple where
		sn.where = []queryNode{whereexpr}
	}

	if qb.idx < len(qb.tokens) {
		return nil, errors.New("did not consume whole statement")
	}

	return sn, nil
}

func (qb *queryBuilder) getRootQueryNode() (queryNode, error) {
	if qb.expect(tokSelect) {
		return qb.pselect()
	}

	return nil, errors.New("unrecognized statement")
}

func parseQuery(input string) (*query, error) {
	parts := strings.Fields(input)
	if len(parts) < 4 || parts[0] != "SELECT" {
		return nil, fmt.Errorf("invalid query format")
	}

	q := &query{
		groupBy: make(map[string]string),
	}

	fromIndex := indexOf(parts, "FROM")
	if fromIndex == -1 {
		return nil, fmt.Errorf("missing FROM clause")
	}
	q.aggregates = parseAggregates(parts[1:fromIndex])

	if fromIndex+1 >= len(parts) {
		return nil, fmt.Errorf("missing metric")
	}
	q.metric = parts[fromIndex+1]

	betweenIndex := indexOf(parts, "BETWEEN")
	if betweenIndex != -1 {
		if betweenIndex+1 >= len(parts) {
			return nil, fmt.Errorf("invalid BETWEEN clause")
		}
		timeRange := strings.Split(parts[betweenIndex+1], ":")
		if len(timeRange) != 2 {
			return nil, fmt.Errorf("invalid time range format in BETWEEN clause")
		}

		var err error
		if timeRange[0] != "" {
			q.timeStart, err = strconv.ParseInt(timeRange[0], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid start timestamp: %v", err)
			}
		} else {
			q.timeStart = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
		}

		if timeRange[1] != "" {
			q.timeEnd, err = strconv.ParseInt(timeRange[1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid end timestamp: %v", err)
			}
		} else {
			q.timeEnd = time.Now().Unix()
		}
	}

	groupByIndex := indexOf(parts, "GROUP")
	if groupByIndex != -1 && groupByIndex+1 < len(parts) && parts[groupByIndex+1] == "BY" {
		for _, tag := range parts[groupByIndex+2:] {
			if tag == "BETWEEN" {
				break
			}
			tag = strings.TrimSuffix(tag, ",")
			q.groupBy[tag] = tag
		}
	}

	return q, nil
}

func indexOf(slice []string, item string) int {
	for i, s := range slice {
		if s == item {
			return i
		}
	}
	return -1
}

func parseAggregates(parts []string) []string {
	var aggregates []string
	for _, part := range parts {
		aggregates = append(aggregates, strings.TrimSuffix(part, ","))
	}
	return aggregates
}
