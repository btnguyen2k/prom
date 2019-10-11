package prom

import (
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"strconv"
	"testing"
)

func TestAwsDynamodbToAttributeValue_Bytes(t *testing.T) {
	name := "TestAwsDynamodbToAttributeValue_Bytes"
	{
		input := []byte{1, 2, 3}
		v := AwsDynamodbToAttributeValue(input)
		if v == nil || v.B == nil {
			t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.B", name, input)
		}
	}
}

func TestAwsDynamodbToAttributeValue_Bool(t *testing.T) {
	name := "TestAwsDynamodbToAttributeValue_Bool"
	{
		input := true
		v := AwsDynamodbToAttributeValue(input)
		if v == nil || v.BOOL == nil || *v.BOOL != input {
			t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.BOOL", name, input)
		}
	}
}

func TestAwsDynamodbToAttributeValue_String(t *testing.T) {
	name := "TestAwsDynamodbToAttributeValue_String"
	{
		input := "a string"
		v := AwsDynamodbToAttributeValue(input)
		if v == nil || v.S == nil || *v.S != input {
			t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.BOOL", name, input)
		}
	}
}

func TestAwsDynamodbToAttributeValue_List(t *testing.T) {
	name := "TestAwsDynamodbToAttributeValue_List"
	{
		input := []interface{}{true, 0, 1.2, "3"}
		v := AwsDynamodbToAttributeValue(input)
		if v == nil || v.L == nil || len(v.L) != len(input) {
			t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.L", name, input)
		}
	}
}

func TestAwsDynamodbToAttributeValue_Map(t *testing.T) {
	name := "TestAwsDynamodbToAttributeValue_Map"
	{
		a := []interface{}{true, 0, 1.2, "3"}
		m := map[string]interface{}{"b": false, "n": 0, "s": "a string"}
		input := map[string]interface{}{"b": true, "n1": 0, "n2": 1.2, "s": "3", "nested_a": a, "nested_m": m}
		v := AwsDynamodbToAttributeValue(input)
		if v == nil || v.M == nil || len(v.M) != len(input) {
			t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.M", name, input)
		}
	}
}

func TestAwsDynamodbToAttributeValue_Struct(t *testing.T) {
	type MyStruct struct {
		B       bool
		N1      int
		N2      float64
		S       string
		NestedA []interface{}
		NestedM map[string]interface{}
		privB   bool
		privN   uint
		privS   string
	}
	name := "TestAwsDynamodbToAttributeValue_Struct"
	{
		a := []interface{}{true, 0, 1.2, "3"}
		m := map[string]interface{}{"b": false, "n": 0, "s": "a string"}
		input := MyStruct{
			B:       true,
			N1:      0,
			N2:      1.2,
			S:       "3",
			NestedA: a,
			NestedM: m,
			privB:   false,
			privN:   100,
			privS:   "a string",
		}
		v := AwsDynamodbToAttributeValue(input)
		if v == nil || v.M == nil || len(v.M) != 6 {
			t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.M", name, input)
		}
	}
}

func TestAwsDynamodbToAttributeValue_Number(t *testing.T) {
	name := "TestAwsDynamodbToAttributeValue_Number"
	{
		// convert int
		input := int(1)
		v := AwsDynamodbToAttributeValue(input)
		if v == nil || v.N == nil || *v.N != strconv.FormatInt(int64(input), 10) {
			t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.N", name, input)
		}
	}
	{
		// convert uint
		input := uint(1)
		v := AwsDynamodbToAttributeValue(input)
		if v == nil || v.N == nil || *v.N != strconv.FormatUint(uint64(input), 10) {
			t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.N", name, input)
		}
	}
	{
		// convert float32
		input := float32(1.0)
		v := AwsDynamodbToAttributeValue(input)
		if v == nil || v.N == nil {
			t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.N", name, input)
		}
	}
	{
		// convert float64
		input := float64(1.0)
		v := AwsDynamodbToAttributeValue(input)
		if v == nil || v.N == nil {
			t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.N", name, input)
		}
	}
}

func TestAwsDynamodbToAttributeSet(t *testing.T) {
	name := "TestAwsDynamodbToAttributeSet"
	{
		// single number
		input := int(1)
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.NS == nil || len(v.NS) != 1 {
			if v == nil {
				t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS", name, input)
			} else {
				t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS (result :%#v)", name, input, v.N)
			}
		}
	}
	{
		// single number
		input := uint(2)
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.NS == nil || len(v.NS) != 1 {
			if v == nil {
				t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS", name, input)
			} else {
				t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS (result :%#v)", name, input, v.N)
			}
		}
	}
	{
		// single number
		input := float32(3.4)
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.NS == nil || len(v.NS) != 1 {
			if v == nil {
				t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS", name, input)
			} else {
				t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS (result :%#v)", name, input, v.N)
			}
		}
	}
	{
		// single string
		input := "a string"
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.SS == nil || len(v.SS) != 1 {
			if v == nil {
				t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.SS", name, input)
			} else {
				t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.SS (result :%#v)", name, input, v.N)
			}
		}
	}
	{
		// bytes
		input := []byte("a string")
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.BS == nil || len(v.BS) != 1 {
			if v == nil {
				t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.BS", name, input)
			} else {
				t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.BS (result :%#v)", name, input, v.N)
			}
		}
	}
	{
		// slice of bytes
		input := [][]byte{[]byte("string1"), []byte("string2"), []byte("string3")}
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.BS == nil || len(v.BS) != len(input) {
			if v == nil {
				t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.BS", name, input)
			} else {
				t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.BS (result :%#v)", name, input, v.N)
			}
		}
	}
	{
		// slice of int
		input := []int{1, 2, 3, 4}
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.NS == nil || len(v.NS) != len(input) {
			if v == nil {
				t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS", name, input)
			} else {
				t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS (result :%#v)", name, input, v.N)
			}
		}
	}
	{
		// slice of uint
		input := []uint{1, 2, 3, 4}
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.NS == nil || len(v.NS) != len(input) {
			if v == nil {
				t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS", name, input)
			} else {
				t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS (result :%#v)", name, input, v.N)
			}
		}
	}
	{
		// slice of floats
		input := []float64{1.2, 3.4, 5.6}
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.NS == nil || len(v.NS) != len(input) {
			if v == nil {
				t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS", name, input)
			} else {
				t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS (result :%#v)", name, input, v.N)
			}
		}
	}
	{
		// slice of strings
		input := []string{"1", "2.3"}
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.SS == nil || len(v.SS) != len(input) {
			if v == nil {
				t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.SS", name, input)
			} else {
				t.Errorf("%s failed: cannot convert %#v to dynamodb.AttributeValue.SS (result :%#v)", name, input, v.N)
			}
		}
	}
}

func TestAwsDynamodbExistsAllBuilder(t *testing.T) {
	name := "TestAwsDynamodbExistsAllBuilder"
	input := []string{"a", "b", "c"}
	conditionBuilder := AwsDynamodbExistsAllBuilder(input)
	builder, err := expression.NewBuilder().WithCondition(*conditionBuilder).Build()
	if err != nil {
		t.Errorf("%s failed: %e", name, err)
	}
	condition := builder.Condition()
	expected := "((attribute_exists (#0)) AND (attribute_exists (#1))) AND (attribute_exists (#2))"
	if condition == nil || *condition != expected {
		t.Errorf("%s failed: expected [%s] but received [%s]", name, expected, *condition)
	}
}

func TestAwsDynamodbNotExistsAllBuilder(t *testing.T) {
	name := "TestAwsDynamodbNotExistsAllBuilder"
	input := []string{"a", "b", "c"}
	conditionBuilder := AwsDynamodbNotExistsAllBuilder(input)
	builder, err := expression.NewBuilder().WithCondition(*conditionBuilder).Build()
	if err != nil {
		t.Errorf("%s failed: %e", name, err)
	}
	condition := builder.Condition()
	expected := "((attribute_not_exists (#0)) AND (attribute_not_exists (#1))) AND (attribute_not_exists (#2))"
	if condition == nil || *condition != expected {
		t.Errorf("%s failed: expected [%s] but received [%s]", name, expected, *condition)
	}
}

func TestAwsDynamodbEqualsBuilder(t *testing.T) {
	name := "TestAwsDynamodbEqualsBuilder"
	input := map[string]interface{}{"s": "a string", "n": 0, "b": true}
	conditionBuilder := AwsDynamodbEqualsBuilder(input)
	builder, err := expression.NewBuilder().WithCondition(*conditionBuilder).Build()
	if err != nil {
		t.Errorf("%s failed: %e", name, err)
	}
	condition := builder.Condition()
	expected := "((#0 = :0) AND (#1 = :1)) AND (#2 = :2)"
	if condition == nil || *condition != expected {
		t.Errorf("%s failed: expected [%s] but received [%s]", name, expected, *condition)
	}
}
