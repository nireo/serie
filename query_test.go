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
