package schema

import "testing"

func TestValidate(t *testing.T) {
	s := Schema{
		Name:       "test_table",
		TimeColumn: "ts",
		Columns: []Column{
			{Name: "ts", Type: Int64},
		},
	}

	if err := s.Validate(); err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}
