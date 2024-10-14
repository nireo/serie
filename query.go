package serie

import (
	"errors"
	"fmt"
	"strconv"
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
			if keywordType, ok := keywords[identifier]; ok {
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
		default:
			return nil, fmt.Errorf("unexpected character: %c", c)
		}
	}
	return tokens, nil
}
