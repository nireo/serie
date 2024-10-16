package serie

import (
	"reflect"
	"testing"
)

func TestTokenize(t *testing.T) {
	input := "hello select 123 123.456"
	l := lexer{
		idx:  0,
		data: input,
	}

	tokens, err := l.tokenize()
	if err != nil {
		t.Fatalf("failed to tokenize input: %s", err)
	}

	expectedTokens := []token{
		{
			content: "hello",
			kind:    tokIdent,
		},
		{
			content: "",
			kind:    tokSelect,
		},
		{
			kind:   tokInt,
			intVal: 123,
		},
		{
			kind:     tokFloat,
			floatVal: 123.456,
		},
	}

	if !reflect.DeepEqual(expectedTokens, tokens) {
		t.Fatalf("tokens are not equal got: %+v", tokens)
	}
}

func TestQueryBuilder(t *testing.T) {
	tests := []struct {
		name     string
		tokens   []token
		expected queryNode
		wantErr  bool
	}{
		{
			name: "Simple select",
			tokens: []token{
				{kind: tokSelect, content: "SELECT"},
				{kind: tokIdent, content: "id"},
				{kind: tokComma, content: ","},
				{kind: tokIdent, content: "name"},
				{kind: tokFrom, content: "FROM"},
				{kind: tokIdent, content: "users"},
			},
			expected: &selectNode{
				columns: []queryNode{
					&literalNode{lit: token{content: "id"}},
					&literalNode{lit: token{content: "name"}},
				},
				from: token{content: "users"},
			},
			wantErr: false,
		},
		{
			name: "Select with where",
			tokens: []token{
				{kind: tokSelect, content: "SELECT"},
				{kind: tokIdent, content: "id"},
				{kind: tokFrom, content: "FROM"},
				{kind: tokIdent, content: "users"},
				{kind: tokWhere, content: "WHERE"},
				{kind: tokIdent, content: "age"},
				{kind: tokGt, content: ">"},
				{kind: tokInt, content: "18"},
			},
			expected: &selectNode{
				columns: []queryNode{
					&literalNode{lit: token{content: "id"}},
				},
				from: token{content: "users"},
				where: []queryNode{
					&binopNode{
						left:  &literalNode{lit: token{content: "age"}},
						right: &literalNode{lit: token{content: "18"}},
						op:    token{content: ">"},
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qb := &queryBuilder{tokens: tt.tokens}
			got, err := qb.getRootQueryNode()

			if (err != nil) != tt.wantErr {
				t.Errorf("queryBuilder.getRootQueryNode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("queryBuilder.getRootQueryNode() = %v, want %v", got, tt.expected)
			}
		})
	}
}
