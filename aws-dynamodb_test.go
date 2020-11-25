package prom

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

func TestIsAwsError(t *testing.T) {
	name := "TestIsAwsError"
	if IsAwsError(nil, "0") {
		t.Fatalf("%s failed: %#v should not be an awserr.Error", name, nil)
	}
	{
		e := errors.New("dummy")
		if IsAwsError(e, "0") {
			t.Fatalf("%s failed: %#v should not be an awserr.Error", name, e)
		}
	}
	{
		e := awserr.New("123", "dummy", errors.New("dummy"))
		if !IsAwsError(e, "123") {
			t.Fatalf("%s failed: %#v", name, e)
		}
	}
	{
		e := awserr.New("123", "dummy", errors.New("dummy"))
		if IsAwsError(e, "456") {
			t.Fatalf("%s failed: %#v", name, e)
		}
	}
}

func TestAwsIgnoreErrorIfMatched(t *testing.T) {
	name := "TestAwsIgnoreErrorIfMatched"
	{
		var e error = nil
		if AwsIgnoreErrorIfMatched(e, "0") != nil {
			t.Fatalf("%s failed: %#v", name, e)
		}
	}
	{
		e := errors.New("dummy")
		if AwsIgnoreErrorIfMatched(e, "0") != e {
			t.Fatalf("%s failed: %#v", name, e)
		}
	}
	{
		e := awserr.New("123", "dummy", errors.New("dummy"))
		if AwsIgnoreErrorIfMatched(e, "123") != nil {
			t.Fatalf("%s failed: %#v", name, e)
		}
	}
	{
		e := awserr.New("123", "dummy", errors.New("dummy"))
		if AwsIgnoreErrorIfMatched(e, "456") != e {
			t.Fatalf("%s failed: %#v", name, e)
		}
	}
}

func TestAwsDynamodbToAttributeValue_Bytes(t *testing.T) {
	name := "TestAwsDynamodbToAttributeValue_Bytes"
	{
		input := []byte{1, 2, 3}
		v := AwsDynamodbToAttributeValue(input)
		if v == nil || v.B == nil {
			t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.B", name, input)
		}
	}
}

func TestAwsDynamodbToAttributeValue_Bool(t *testing.T) {
	name := "TestAwsDynamodbToAttributeValue_Bool"
	{
		input := true
		v := AwsDynamodbToAttributeValue(input)
		if v == nil || v.BOOL == nil || *v.BOOL != input {
			t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.BOOL", name, input)
		}
	}
}

func TestAwsDynamodbToAttributeValue_String(t *testing.T) {
	name := "TestAwsDynamodbToAttributeValue_String"
	{
		input := "a string"
		v := AwsDynamodbToAttributeValue(input)
		if v == nil || v.S == nil || *v.S != input {
			t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.BOOL", name, input)
		}
	}
}

func TestAwsDynamodbToAttributeValue_List(t *testing.T) {
	name := "TestAwsDynamodbToAttributeValue_List"
	{
		input := []interface{}{true, 0, 1.2, "3"}
		v := AwsDynamodbToAttributeValue(input)
		if v == nil || v.L == nil || len(v.L) != len(input) {
			t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.L", name, input)
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
			t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.M", name, input)
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
			t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.M", name, input)
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
			t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.N", name, input)
		}
	}
	{
		// convert uint
		input := uint(1)
		v := AwsDynamodbToAttributeValue(input)
		if v == nil || v.N == nil || *v.N != strconv.FormatUint(uint64(input), 10) {
			t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.N", name, input)
		}
	}
	{
		// convert float32
		input := float32(1.0)
		v := AwsDynamodbToAttributeValue(input)
		if v == nil || v.N == nil {
			t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.N", name, input)
		}
	}
	{
		// convert float64
		input := float64(1.0)
		v := AwsDynamodbToAttributeValue(input)
		if v == nil || v.N == nil {
			t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.N", name, input)
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
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS", name, input)
			} else {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS (result :%#v)", name, input, v.N)
			}
		}
	}
	{
		// single number
		input := uint(2)
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.NS == nil || len(v.NS) != 1 {
			if v == nil {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS", name, input)
			} else {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS (result :%#v)", name, input, v.N)
			}
		}
	}
	{
		// single number
		input := float32(3.4)
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.NS == nil || len(v.NS) != 1 {
			if v == nil {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS", name, input)
			} else {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS (result :%#v)", name, input, v.N)
			}
		}
	}
	{
		// single string
		input := "a string"
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.SS == nil || len(v.SS) != 1 {
			if v == nil {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.SS", name, input)
			} else {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.SS (result :%#v)", name, input, v.N)
			}
		}
	}
	{
		// bytes
		input := []byte("a string")
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.BS == nil || len(v.BS) != 1 {
			if v == nil {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.BS", name, input)
			} else {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.BS (result :%#v)", name, input, v.N)
			}
		}
	}
	{
		// slice of bytes
		input := [][]byte{[]byte("string1"), []byte("string2"), []byte("string3")}
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.BS == nil || len(v.BS) != len(input) {
			if v == nil {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.BS", name, input)
			} else {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.BS (result :%#v)", name, input, v.N)
			}
		}
	}
	{
		// slice of int
		input := []int{1, 2, 3, 4}
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.NS == nil || len(v.NS) != len(input) {
			if v == nil {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS", name, input)
			} else {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS (result :%#v)", name, input, v.N)
			}
		}
	}
	{
		// slice of uint
		input := []uint{1, 2, 3, 4}
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.NS == nil || len(v.NS) != len(input) {
			if v == nil {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS", name, input)
			} else {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS (result :%#v)", name, input, v.N)
			}
		}
	}
	{
		// slice of floats
		input := []float64{1.2, 3.4, 5.6}
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.NS == nil || len(v.NS) != len(input) {
			if v == nil {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS", name, input)
			} else {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS (result :%#v)", name, input, v.N)
			}
		}
	}
	{
		// slice of strings
		input := []string{"1", "2.3"}
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.SS == nil || len(v.SS) != len(input) {
			if v == nil {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.SS", name, input)
			} else {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.SS (result :%#v)", name, input, v.N)
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
		t.Fatalf("%s failed: %e", name, err)
	}
	condition := builder.Condition()
	expected := "((attribute_exists (#0)) AND (attribute_exists (#1))) AND (attribute_exists (#2))"
	if condition == nil || *condition != expected {
		t.Fatalf("%s failed: expected [%s] but received [%s]", name, expected, *condition)
	}
}

func TestAwsDynamodbNotExistsAllBuilder(t *testing.T) {
	name := "TestAwsDynamodbNotExistsAllBuilder"
	input := []string{"a", "b", "c"}
	conditionBuilder := AwsDynamodbNotExistsAllBuilder(input)
	builder, err := expression.NewBuilder().WithCondition(*conditionBuilder).Build()
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	condition := builder.Condition()
	expected := "((attribute_not_exists (#0)) AND (attribute_not_exists (#1))) AND (attribute_not_exists (#2))"
	if condition == nil || *condition != expected {
		t.Fatalf("%s failed: expected [%s] but received [%s]", name, expected, *condition)
	}
}

func TestAwsDynamodbEqualsBuilder(t *testing.T) {
	name := "TestAwsDynamodbEqualsBuilder"
	input := map[string]interface{}{"s": "a string", "n": 0, "b": true}
	conditionBuilder := AwsDynamodbEqualsBuilder(input)
	builder, err := expression.NewBuilder().WithCondition(*conditionBuilder).Build()
	if err != nil {
		t.Fatalf("%s failed: %e", name, err)
	}
	condition := builder.Condition()
	expected := "((#0 = :0) AND (#1 = :1)) AND (#2 = :2)"
	if condition == nil || *condition != expected {
		t.Fatalf("%s failed: expected [%s] but received [%s]", name, expected, *condition)
	}
}

func TestAwsDynamodbFastFailed(t *testing.T) {
	name := "TestAwsDynamodbFastFailed"
	cfg := &aws.Config{
		Region:      aws.String("dummy"),
		Credentials: credentials.NewStaticCredentials("id", "secret", "token"),
		DisableSSL:  aws.Bool(true),
		Endpoint:    aws.String("http://localhost:1234"),
	}
	timeoutMs := 100
	adc, err := NewAwsDynamodbConnect(cfg, nil, nil, timeoutMs)
	if err != nil {
		t.Fatalf("%s/%s failed: %s", name, "NewAwsDynamodbConnect", err)
	}
	tstart := time.Now()
	_, err = adc.HasTable(nil, "mytable")
	if err == nil {
		t.Fatalf("%s failed: the operation should not success", name)
	}
	d := time.Duration(time.Now().UnixNano() - tstart.UnixNano())
	dmax := time.Duration(float64(time.Duration(timeoutMs)*time.Millisecond) * 1.5)
	if d > dmax {
		t.Fatalf("%s failed: operation is expected to fail within %#v ms but in fact %#v ms", name, dmax/1e6, d/1e6)
	}
}

func _createAwsDynamodbConnect(t *testing.T, testName string) *AwsDynamodbConnect {
	awsRegion := strings.ReplaceAll(os.Getenv("AWS_REGION"), `"`, "")
	awsAccessKeyId := strings.ReplaceAll(os.Getenv("AWS_ACCESS_KEY_ID"), `"`, "")
	awsSecretAccessKey := strings.ReplaceAll(os.Getenv("AWS_SECRET_ACCESS_KEY"), `"`, "")
	if awsRegion == "" || awsAccessKeyId == "" || awsSecretAccessKey == "" {
		t.Skipf("%s skipped", testName)
		return nil
	}
	cfg := &aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: credentials.NewEnvCredentials(),
	}
	if awsDynamodbEndpoint := strings.ReplaceAll(os.Getenv("AWS_DYNAMODB_ENDPOINT"), `"`, ""); awsDynamodbEndpoint != "" {
		cfg.Endpoint = aws.String(awsDynamodbEndpoint)
		if strings.HasPrefix(awsDynamodbEndpoint, "http://") {
			cfg.DisableSSL = aws.Bool(true)
		}
	}
	adc, err := NewAwsDynamodbConnect(cfg, nil, nil, 10000)
	if err != nil {
		t.Fatalf("%s/%s failed: %s", testName, "NewAwsDynamodbConnect", err)
	}
	return adc
}

func TestNewAwsDynamodbConnect(t *testing.T) {
	name := "TestNewAwsDynamodbConnect"
	adc := _createAwsDynamodbConnect(t, name)
	if adc == nil {
		t.Fatalf("%s failed: nil", name)
	}
	defer adc.Close()
}

func TestNewAwsDynamodbConnect_timeout(t *testing.T) {
	name := "TestNewAwsDynamodbConnect_timeout"
	region := "ap-southeast-1"
	cfg := &aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewEnvCredentials(),
	}
	adc, err := NewAwsDynamodbConnect(cfg, nil, nil, -1)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}
	if adc == nil {
		t.Fatalf("%s failed: nil", name)
	}
	defer adc.Close()
	if adc.timeoutMs < 0 {
		t.Fatalf("%s failed: invalid timeout value #%v", name, adc.timeoutMs)
	}
}

func TestNewAwsDynamodbConnect_nil(t *testing.T) {
	name := "TestNewAwsDynamodbConnect_nil"
	adc, err := NewAwsDynamodbConnect(nil, nil, nil, -1)
	if err == nil || adc != nil {
		t.Fatalf("%s failed: AwsDynamodbConnect should not be created", name)
	}
}

var testRegion = "ap-southeast-1"

func TestAwsDynamodbConnect_Close(t *testing.T) {
	name := "TestAwsDynamodbConnect_Close"
	adc := _createAwsDynamodbConnect(t, name)
	err := adc.Close()
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}
}

func TestAwsDynamodbConnect_GetDb(t *testing.T) {
	name := "TestAwsDynamodbConnect_GetDb"
	adc := _createAwsDynamodbConnect(t, name)
	if adc.GetDb() == nil {
		t.Fatalf("%s failed: nil", name)
	}
}

func inSlide(item string, slide []string) bool {
	for _, s := range slide {
		if item == s {
			return true
		}
	}
	return false
}

func waitForGsi(adc *AwsDynamodbConnect, table, index string, statusList []string, delay int) {
	for status, err := adc.GetGlobalSecondaryIndexStatus(nil, table, index); !inSlide(status, statusList) && err == nil; {
		fmt.Printf("\tGSI [%s] on table [%s] status: %v - %e\n", index, table, status, err)
		if delay > 0 {
			time.Sleep(time.Duration(delay) * time.Second)
		}
		status, err = adc.GetGlobalSecondaryIndexStatus(nil, table, index)
	}
}

func waitForTable(adc *AwsDynamodbConnect, table string, statusList []string, delay int) {
	for status, err := adc.GetTableStatus(nil, table); !inSlide(status, statusList) && err == nil; {
		fmt.Printf("\tTable [%s] status: %v - %e\n", table, status, err)
		if delay > 0 {
			time.Sleep(time.Duration(delay) * time.Second)
		}
		status, err = adc.GetTableStatus(nil, table)
	}
}

func prepareAwsDynamodbTable(adc *AwsDynamodbConnect, table string) error {
	err := adc.DeleteTable(nil, table)
	if AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException) != nil {
		return err
	}
	fmt.Printf("\tDeleted table [%s]\n", table)
	waitForTable(adc, table, []string{""}, 1)

	err = adc.CreateTable(nil, table, 2, 2,
		[]AwsDynamodbNameAndType{{"username", AwsAttrTypeString}, {"email", AwsAttrTypeString}},
		[]AwsDynamodbNameAndType{{"username", AwsKeyTypePartition}, {"email", AwsKeyTypeSort}})
	if AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException) != nil {
		return err
	}
	waitForTable(adc, table, []string{"ACTIVE"}, 1)
	return nil
}

const (
	dynamodbTestTableName = "DYNAMODB_TEST_TABLE_NAME"
	dynamodbTestGsiName   = "DYNAMODB_TEST_GSI_NAME"
)

var (
	testDynamodbTableName = "test_prom"
	testDynamodbGsiName   = "test_prom_gsi_email"
)

func TestAwsDynamodbConnect_PutItem(t *testing.T) {
	name := "TestAwsDynamodbConnect_PutItem"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/prepareAwsDynamodbTable", err)
	}

	item := map[string]interface{}{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  time.Now().Unix(),
		"actived":  true,
	}
	_, err = adc.PutItem(nil, testDynamodbTableName, item, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}
}

func TestAwsDynamodbConnect_ListTables(t *testing.T) {
	name := "TestAwsDynamodbConnect_ListTables"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	tables, err := adc.ListTables(nil)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}
	if tables == nil {
		t.Fatalf("%s failed: nil", name)
	}
}

func TestAwsDynamodbConnect_TableAndIndex(t *testing.T) {
	name := "TestAwsDynamodbConnect_TableAndIndex"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := adc.CreateTable(nil, testDynamodbTableName, 2, 2,
		[]AwsDynamodbNameAndType{{"username", AwsAttrTypeString}},
		[]AwsDynamodbNameAndType{{"username", AwsKeyTypePartition}})
	if AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException) != nil {
		t.Fatalf("%s failed: error [%e]", name+"/CreateTable", err)
	}
	ok, err := adc.HasTable(nil, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/HasTable", err)
	}
	if !ok {
		t.Fatalf("%s failed: table [%s] not found", name+"/HasTable", testDynamodbTableName)
	}
	fmt.Printf("\tCreated table [%s]\n", testDynamodbTableName)
	waitForTable(adc, testDynamodbTableName, []string{"ACTIVE"}, 1)

	if os.Getenv(dynamodbTestGsiName) != "" {
		testDynamodbGsiName = os.Getenv(dynamodbTestGsiName)
	}
	{
		err = adc.DeleteGlobalSecondaryIndex(nil, testDynamodbTableName, testDynamodbGsiName)
		if AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException) != nil {
			t.Fatalf("%s failed: error [%e]", name+"/DeleteGlobalSecondaryIndex", err)
		}
		fmt.Printf("\tDeleted GSI [%s] on table [%s]\n", testDynamodbGsiName, testDynamodbTableName)
		waitForGsi(adc, testDynamodbTableName, testDynamodbGsiName, []string{""}, 1)
	}

	err = adc.CreateGlobalSecondaryIndex(nil, testDynamodbTableName, testDynamodbGsiName, 1, 1,
		[]AwsDynamodbNameAndType{{"email", AwsAttrTypeString}},
		[]AwsDynamodbNameAndType{{"email", AwsKeyTypePartition}})
	if AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException) != nil {
		t.Fatalf("%s failed: error [%e]", name+"/CreateGlobalSecondaryIndex", err)
	}
	fmt.Printf("\tCreated GSI [%s] on table [%s]\n", testDynamodbGsiName, testDynamodbTableName)
	waitForGsi(adc, testDynamodbTableName, testDynamodbGsiName, []string{"ACTIVE", "CREATING"}, 5)

	time.Sleep(10 * time.Second)

	err = adc.DeleteGlobalSecondaryIndex(nil, testDynamodbTableName, testDynamodbGsiName)
	if AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException) != nil {
		t.Fatalf("%s failed: error [%e]", name+"/DeleteGlobalSecondaryIndex", err)
	}
	fmt.Printf("\tDeleted GSI [%s] on table [%s]\n", testDynamodbGsiName, testDynamodbTableName)
	waitForGsi(adc, testDynamodbTableName, testDynamodbGsiName, []string{""}, 1)

	err = adc.DeleteTable(nil, testDynamodbTableName)
	if AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException) != nil {
		t.Fatalf("%s failed: error [%e]", name+"/DeleteTable", err)
	}
	fmt.Printf("\tDeleted table [%s]\n", testDynamodbTableName)
	waitForTable(adc, testDynamodbTableName, []string{""}, 1)
	ok, err = adc.HasTable(nil, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/HasTable", err)
	}
	if ok {
		t.Fatalf("%s failed: table [%s] not deleted", name+"/HasTable", testDynamodbTableName)
	}
}

func TestAwsDynamodbConnect_GetPutItem(t *testing.T) {
	name := "TestAwsDynamodbConnect_GetPutItem"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/prepareAwsDynamodbTable", err)
	}

	keyFilter := map[string]interface{}{"username": "btnguyen2k", "email": "me@domain.com"}
	item := map[string]interface{}{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  float64(time.Now().Unix()),
		"actived":  true,
	}

	// GetItem: must be "not found"
	fetchedItem, err := adc.GetItem(nil, testDynamodbTableName, keyFilter)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/GetItem", err)
	}
	if fetchedItem != nil {
		t.Fatalf("%s failed: item should not exist", name+"/GetItem")
	}

	// PutItem: must be successful
	_, err = adc.PutItem(nil, testDynamodbTableName, item, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/PutItem", err)
	}

	// GetItem: must match the original one
	fetchedItem, err = adc.GetItem(nil, testDynamodbTableName, keyFilter)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/GetItem", err)
	}
	if fetchedItem == nil {
		t.Fatalf("%s failed: item not exist", name+"/GetItem")
	} else {
		var m map[string]interface{} = fetchedItem
		if !reflect.DeepEqual(m, item) {
			t.Fatalf("%s failed: fetched [%#v] vs original [%#v]", name+"/GetItem", m, item)
		}
	}

	item["version"] = nil
	item["actived"] = false
	item["name"] = "Thanh Nguyen"
	// PutItem: must be successful
	_, err = adc.PutItem(nil, testDynamodbTableName, item, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/PutItem", err)
	}

	// GetItem: must match the original one
	fetchedItem, err = adc.GetItem(nil, testDynamodbTableName, keyFilter)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/GetItem", err)
	}
	if fetchedItem == nil {
		t.Fatalf("%s failed: item not exist", name+"/GetItem")
	} else {
		var m map[string]interface{} = fetchedItem
		if !reflect.DeepEqual(m, item) {
			t.Fatalf("%s failed: fetched [%#v] vs original [%#v]", name+"/GetItem", m, item)
		}
	}

	item["version"] = float64(123)
	item["name"] = "Thanh Nguyen"
	delete(item, "actived")
	// PutItem: must be successful
	_, err = adc.PutItem(nil, testDynamodbTableName, item, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/PutItem", err)
	}

	// GetItem: must match the original one
	fetchedItem, err = adc.GetItem(nil, testDynamodbTableName, keyFilter)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/GetItem", err)
	}
	if fetchedItem == nil {
		t.Fatalf("%s failed: item not exist", name+"/GetItem")
	} else {
		var m map[string]interface{} = fetchedItem
		if !reflect.DeepEqual(m, item) {
			t.Fatalf("%s failed: fetched [%#v] vs original [%#v]", name+"/GetItem", m, item)
		}
	}
}

func TestAwsDynamodbConnect_PutItemIfNotExist(t *testing.T) {
	name := "TestAwsDynamodbConnect_PutItemIfNotExist"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/prepareAwsDynamodbTable", err)
	}

	item := map[string]interface{}{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  float64(time.Now().Unix()),
		"actived":  true,
	}
	// PutItem: must be successful
	_, err = adc.PutItem(nil, testDynamodbTableName, item, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/PutItem", err)
	}

	newItem := map[string]interface{}{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  float64(time.Now().Unix()),
		"actived":  false,
		"name":     "Thanh Nguyen",
	}
	// PutItemIfNotExist: must be successful
	putItem, err := adc.PutItemIfNotExist(nil, testDynamodbTableName, newItem, []string{"username"})
	if err != nil {
		t.Fatalf("%s failed: error [%#v]", name+"/PutItemIfNotExist", err)
	}
	if putItem != nil {
		t.Fatalf("%s failed: expected nil result but received [%#v]", name+"/PutItemIfNotExist", putItem)
	}

	keyFilter := map[string]interface{}{"username": "btnguyen2k", "email": "me@domain.com"}
	// GetItem: must match the original one
	fetchedItem, err := adc.GetItem(nil, testDynamodbTableName, keyFilter)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/GetItem", err)
	}
	if fetchedItem == nil {
		t.Fatalf("%s failed: item not exist", name+"/GetItem")
	} else {
		var m map[string]interface{} = fetchedItem
		if !reflect.DeepEqual(m, item) {
			t.Fatalf("%s failed: fetched [%#v] vs original [%#v]", name+"/GetItem", m, item)
		}
	}

	item = map[string]interface{}{
		"username": "thanhn",
		"email":    "me@domain.com",
		"version":  nil,
		"actived":  false,
		"name":     "Thanh Nguyen",
	}
	// PutItemIfNotExist: must be successful
	putItem, err = adc.PutItemIfNotExist(nil, testDynamodbTableName, item, []string{"username"})
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/PutItemIfNotExist", err)
	}
	if putItem == nil {
		t.Fatalf("%s failed: nil", name+"/PutItemIfNotExist")
	}

	keyFilter = map[string]interface{}{"username": "thanhn", "email": "me@domain.com"}
	// GetItem: must match the original one
	fetchedItem, err = adc.GetItem(nil, testDynamodbTableName, keyFilter)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/GetItem", err)
	}
	if fetchedItem == nil {
		t.Fatalf("%s failed: item not exist", name+"/GetItem")
	} else {
		var m map[string]interface{} = fetchedItem
		if !reflect.DeepEqual(m, item) {
			t.Fatalf("%s failed: fetched [%#v] vs original [%#v]", name+"/GetItem", m, item)
		}
	}
}

func TestAwsDynamodbConnect_DeleteItem(t *testing.T) {
	name := "TestAwsDynamodbConnect_DeleteItem"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/prepareAwsDynamodbTable", err)
	}

	item := map[string]interface{}{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  float64(time.Now().Unix()),
		"actived":  true,
	}
	// PutItem: must be successful
	_, err = adc.PutItem(nil, testDynamodbTableName, item, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/PutItem", err)
	}

	keyFilter := map[string]interface{}{"username": "btnguyen2k", "email": "me@domain.com"}
	condition := AwsDynamodbExistsAllBuilder([]string{"version"})
	// DeleteItem: must be successful
	_, err = adc.DeleteItem(nil, testDynamodbTableName, keyFilter, condition)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/PutItem", err)
	}
	fetchedItem, err := adc.GetItem(nil, testDynamodbTableName, keyFilter)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/GetItem", err)
	}
	if fetchedItem != nil {
		t.Fatalf("%s failed: item has not been deleted", name+"/GetItem")
	}
}

func TestAwsDynamodbConnect_RemoveAttributes(t *testing.T) {
	name := "TestAwsDynamodbConnect_RemoveAttributes"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/prepareAwsDynamodbTable", err)
	}

	item := map[string]interface{}{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  float64(time.Now().Unix()),
		"actived":  true,
	}
	_, err = adc.PutItem(nil, testDynamodbTableName, item, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/PutItem", err)
	}

	fieldsToRemove := []string{"version", "actived"}
	keyFilter := map[string]interface{}{"username": "btnguyen2k", "email": "me@domain.com"}
	_, err = adc.RemoveAttributes(nil, testDynamodbTableName, keyFilter, nil, fieldsToRemove)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/RemoveAttributes", err)
	}

	for _, f := range fieldsToRemove {
		delete(item, f)
	}

	fetchedItem, err := adc.GetItem(nil, testDynamodbTableName, keyFilter)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/GetItem", err)
	}
	if fetchedItem == nil {
		t.Fatalf("%s failed: item not exist", name+"/GetItem")
	} else {
		var m map[string]interface{} = fetchedItem
		if !reflect.DeepEqual(m, item) {
			t.Fatalf("%s failed: fetched [%#v] vs original [%#v]", name+"/GetItem", m, item)
		}
	}
}

func TestAwsDynamodbConnect_SetAttributes(t *testing.T) {
	name := "TestAwsDynamodbConnect_SetAttributes"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/prepareAwsDynamodbTable", err)
	}

	item := map[string]interface{}{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  float64(time.Now().Unix()),
		"actived":  true,
	}
	_, err = adc.PutItem(nil, testDynamodbTableName, item, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/PutItem", err)
	}

	newFieldsAndValues := map[string]interface{}{
		"name":    "Thanh Nguyen",
		"actived": false,
		"version": nil,
	}
	keyFilter := map[string]interface{}{"username": "btnguyen2k", "email": "me@domain.com"}
	_, err = adc.SetAttributes(nil, testDynamodbTableName, keyFilter, nil, newFieldsAndValues)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/SetAttributes", err)
	}

	for k, v := range newFieldsAndValues {
		item[k] = v
	}

	fetchedItem, err := adc.GetItem(nil, testDynamodbTableName, keyFilter)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/GetItem", err)
	}
	if fetchedItem == nil {
		t.Fatalf("%s failed: item not exist", name+"/GetItem")
	} else {
		var m map[string]interface{} = fetchedItem
		if !reflect.DeepEqual(m, item) {
			t.Fatalf("%s failed: fetched [%#v] vs original [%#v]", name+"/GetItem", m, item)
		}
	}
}

func TestAwsDynamodbConnect_AddValuesToAttributes(t *testing.T) {
	name := "TestAwsDynamodbConnect_AddValuesToAttributes"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/prepareAwsDynamodbTable", err)
	}

	rand.Seed(time.Now().UnixNano())
	a := []interface{}{rand.Int()%2 == 0, 1.0, "a string"}
	m := map[string]interface{}{"b": rand.Int()%2 == 0, "n": 2.0, "s": "a string"}
	item := map[string]interface{}{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"b":        true,
		"s":        "a string",
		"n":        1.0,
		"m":        m,
		"a":        a,
		"an":       []interface{}{1.0, 2.0, 3.0},
		"as":       []interface{}{"1", "2", "3"},
	}
	_, err = adc.PutItem(nil, testDynamodbTableName, item, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/PutItem", err)
	}

	keyFilter := map[string]interface{}{"username": "btnguyen2k", "email": "me@domain.com"}
	attrsAndValuesToAdd := map[string]interface{}{
		"a[1]":  1.1,   // a[1]'s value is added by 1.1 --> new value 2.1
		"a[10]": 12.34, // new value 12.34 is appended to array a
		"m.n":   1.2,   // m.n's value is added by 1.2 --> new value 3.2
		"m.new": 3.0,   // m.new does not exist, its value is assumed zero, hence new key m.new is created with value 3.0
		"n0":    1.2,   // n0 does not exist, its value is assumed zero, hence new attribute n0 is created with value 1.2
		"n":     2.3,   // n's value is added by 2.3 --> new value 3.3
	}
	_, err = adc.AddValuesToAttributes(nil, testDynamodbTableName, keyFilter, nil, attrsAndValuesToAdd)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/AddValuesToAttributes", err)
	}

	a[1] = a[1].(float64) + 1.1
	item["a"] = append(a, 12.34)
	m["n"] = m["n"].(float64) + 1.2
	m["new"] = 3.0
	item["n0"] = 1.2
	item["n"] = item["n"].(float64) + 2.3
	fetchedItem, err := adc.GetItem(nil, testDynamodbTableName, keyFilter)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/GetItem", err)
	}
	if fetchedItem == nil {
		t.Fatalf("%s failed: item not exist", name+"/GetItem")
	} else {
		var m map[string]interface{} = fetchedItem
		if !reflect.DeepEqual(m, item) {
			t.Fatalf("%s failed: fetched [%#v] vs original [%#v]", name+"/GetItem", m, item)
		}
	}
}

func TestAwsDynamodbConnect_AddValuesToSet(t *testing.T) {
	name := "TestAwsDynamodbConnect_AddValuesToSet"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/prepareAwsDynamodbTable", err)
	}

	item := map[string]*dynamodb.AttributeValue{
		"username": {S: aws.String("btnguyen2k")},
		"an": {
			NS: []*string{aws.String("1"), aws.String("2"), aws.String("3")},
		},
		"as": {
			SS: []*string{aws.String("1"), aws.String("2"), aws.String("3")},
		},
		"a":     {L: []*dynamodb.AttributeValue{}},
		"email": {S: aws.String("me@domain.com")},
		"m":     {M: map[string]*dynamodb.AttributeValue{}},
	}
	_, err = adc.PutItemRaw(nil, testDynamodbTableName, item, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/PutItemRaw", err)
	}

	attrsAndValues := map[string]interface{}{"an": 8, "as": []string{"9", "10"}}
	keyFilter := map[string]interface{}{"username": "btnguyen2k", "email": "me@domain.com"}
	_, err = adc.AddValuesToSet(nil, testDynamodbTableName, keyFilter, nil, attrsAndValues)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/AddValuesToSet", err)
	}

	item0 := map[string]interface{}{
		"username": "btnguyen2k",
		"an":       []float64{8.0, 3.0, 2.0, 1.0},
		"as":       []string{"1", "10", "2", "3", "9"},
		"a":        []interface{}{},
		"email":    "me@domain.com",
		"m":        map[string]interface{}{},
	}
	sort.Float64s(item0["an"].([]float64))
	sort.Strings(item0["as"].([]string))
	// a[1] = a[1].(float64) + 1.1
	fetchedItem, err := adc.GetItem(nil, testDynamodbTableName, keyFilter)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/GetItem", err)
	}
	if fetchedItem == nil {
		t.Fatalf("%s failed: item not exist", name+"/GetItem")
	} else {
		var m map[string]interface{} = fetchedItem
		if !reflect.DeepEqual(m, item0) {
			t.Fatalf("%s failed: fetched\n%#v\noriginal\n%#v", name+"/GetItem", m, item0)
		}
	}
}

func TestAwsDynamodbConnect_DeleteValuesFromSet(t *testing.T) {
	name := "TestAwsDynamodbConnect_DeleteValuesFromSet"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/prepareAwsDynamodbTable", err)
	}

	item := map[string]*dynamodb.AttributeValue{
		"username": {S: aws.String("btnguyen2k")},
		"an": {
			NS: []*string{aws.String("1"), aws.String("2"), aws.String("3")},
		},
		"as": {
			SS: []*string{aws.String("1"), aws.String("2"), aws.String("3")},
		},
		"a":     {L: []*dynamodb.AttributeValue{}},
		"email": {S: aws.String("me@domain.com")},
		"m":     {M: map[string]*dynamodb.AttributeValue{}},
	}
	_, err = adc.PutItemRaw(nil, testDynamodbTableName, item, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/PutItemRaw", err)
	}

	attrsAndValues := map[string]interface{}{"an": 1, "as": []string{"1", "3", "5"}}
	keyFilter := map[string]interface{}{"username": "btnguyen2k", "email": "me@domain.com"}
	_, err = adc.DeleteValuesFromSet(nil, testDynamodbTableName, keyFilter, nil, attrsAndValues)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/AddValuesToSet", err)
	}

	item0 := map[string]interface{}{
		"username": "btnguyen2k",
		"an":       []float64{3.0, 2.0},
		"as":       []string{"2"},
		"a":        []interface{}{},
		"email":    "me@domain.com",
		"m":        map[string]interface{}{},
	}
	sort.Float64s(item0["an"].([]float64))
	// a[1] = a[1].(float64) + 1.1
	fetchedItem, err := adc.GetItem(nil, testDynamodbTableName, keyFilter)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/GetItem", err)
	}
	if fetchedItem == nil {
		t.Fatalf("%s failed: item not exist", name+"/GetItem")
	} else {
		var m map[string]interface{} = fetchedItem
		if !reflect.DeepEqual(m, item0) {
			t.Fatalf("%s failed: fetched\n%#v\noriginal\n%#v", name+"/GetItem", m, item0)
		}
	}
}

func TestAwsDynamodbConnect_ScanItems(t *testing.T) {
	name := "TestAwsDynamodbConnect_ScanItems"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/prepareAwsDynamodbTable", err)
	}

	itemsMap := make(map[string]map[string]interface{})
	for i := 0; i < 10; i++ {
		id := strconv.Itoa(i)
		item := map[string]interface{}{
			"username": id,
			"email":    id + "@domain.com",
			"name":     "Thanh " + id,
		}
		_, err = adc.PutItem(nil, testDynamodbTableName, item, nil)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", name+"/PutItem", err)
		}
		itemsMap[id] = item
	}

	filter := expression.Or(expression.Name("username").GreaterThan(expression.Value("7@domain.com")),
		expression.Name("email").LessThanEqual(expression.Value("2@domain.com")))
	scannedItems, err := adc.ScanItems(nil, testDynamodbTableName, &filter, "")
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/ScanItems", err)
	}
	for _, si := range scannedItems {
		id := si["username"].(string)
		item := itemsMap[id]
		var m map[string]interface{} = si
		if !reflect.DeepEqual(m, item) {
			t.Fatalf("%s failed: fetched\n%#v\noriginal\n%#v", name+"/ScanItems", m, item)
		}
		delete(itemsMap, id)
	}
	if len(itemsMap) != 5 {
		t.Fatalf("%s failed: remaining item(s) %d", name+"/ScanItems", len(itemsMap))
	}
}

func TestAwsDynamodbConnect_ScanItemsWithCallback(t *testing.T) {
	name := "TestAwsDynamodbConnect_ScanItemsWithCallback"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/prepareAwsDynamodbTable", err)
	}

	itemsMap := make(map[string]map[string]interface{})
	for i := 0; i < 10; i++ {
		id := strconv.Itoa(i)
		item := map[string]interface{}{
			"username": id,
			"email":    id + "@domain.com",
			"name":     "Thanh " + id,
		}
		_, err = adc.PutItem(nil, testDynamodbTableName, item, nil)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", name+"/PutItem", err)
		}
		itemsMap[id] = item
	}

	filter := expression.Or(expression.Name("username").GreaterThan(expression.Value("7@domain.com")),
		expression.Name("email").LessThanEqual(expression.Value("2@domain.com")))
	err = adc.ScanItemsWithCallback(nil, testDynamodbTableName, &filter, "", nil, func(si AwsDynamodbItem, lastEvaluatedKey map[string]*dynamodb.AttributeValue) (bool, error) {
		id := si["username"].(string)
		item := itemsMap[id]
		var m map[string]interface{} = si
		if !reflect.DeepEqual(m, item) {
			t.Fatalf("%s failed: fetched\n%#v\noriginal\n%#v", name+"/ScanItems", m, item)
		}
		delete(itemsMap, id)
		return true, nil
	})
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/ScanItemsWithCallback", err)
	}
	if len(itemsMap) != 5 {
		t.Fatalf("%s failed: remaining item(s) %d", name+"/ScanItems", len(itemsMap))
	}
}

func TestAwsDynamodbConnect_QueryItems(t *testing.T) {
	name := "TestAwsDynamodbConnect_QueryItems"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/prepareAwsDynamodbTable", err)
	}

	itemsMap := make(map[string]map[string]interface{})
	for i := 0; i < 10; i++ {
		id := strconv.Itoa(i)
		item := map[string]interface{}{
			"username": "btnguyen2k",
			"email":    id + "@domain.com",
			"name":     "Thanh " + id,
		}
		_, err = adc.PutItem(nil, testDynamodbTableName, item, nil)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", name+"/PutItem", err)
		}
		itemsMap[id+"@domain.com"] = item
	}

	keyFilter := expression.And(expression.Name("username").Equal(expression.Value("btnguyen2k")),
		expression.Name("email").LessThan(expression.Value("8@domain.com")))
	queriesItems, err := adc.QueryItems(nil, testDynamodbTableName, &keyFilter, nil, "")
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/QueryItems", err)
	}
	for _, qi := range queriesItems {
		fmt.Printf("Item: %#v\n", qi)
		id := qi["email"].(string)
		item := itemsMap[id]
		var m map[string]interface{} = qi
		if !reflect.DeepEqual(m, item) {
			t.Fatalf("%s failed: fetched\n%#v\noriginal\n%#v", name+"/QueryItems", m, item)
		}
		delete(itemsMap, id)
	}
	if len(itemsMap) != 2 {
		t.Fatalf("%s failed: remaining item(s) %d", name+"/QueryItems", len(itemsMap))
	}
}

func TestAwsDynamodbConnect_QueryItemsWithCallback(t *testing.T) {
	name := "TestAwsDynamodbConnect_QueryItemsWithCallback"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/prepareAwsDynamodbTable", err)
	}

	itemsMap := make(map[string]map[string]interface{})
	for i := 0; i < 10; i++ {
		id := strconv.Itoa(i)
		item := map[string]interface{}{
			"username": "btnguyen2k",
			"email":    id + "@domain.com",
			"name":     "Thanh " + id,
		}
		_, err = adc.PutItem(nil, testDynamodbTableName, item, nil)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", name+"/PutItem", err)
		}
		itemsMap[id+"@domain.com"] = item
	}

	keyFilter := expression.And(expression.Name("username").Equal(expression.Value("btnguyen2k")),
		expression.Name("email").LessThan(expression.Value("8@domain.com")))
	err = adc.QueryItemsWithCallback(nil, testDynamodbTableName, &keyFilter, nil, "", nil, func(si AwsDynamodbItem, lastEvaluatedKey map[string]*dynamodb.AttributeValue) (bool, error) {
		id := si["email"].(string)
		item := itemsMap[id]
		var m map[string]interface{} = si
		if !reflect.DeepEqual(m, item) {
			t.Fatalf("%s failed: fetched\n%#v\noriginal\n%#v", name+"/QueryItemsWithCallback", m, item)
		}
		delete(itemsMap, id)
		return true, nil
	})
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/QueryItemsWithCallback", err)
	}
	if len(itemsMap) != 2 {
		t.Fatalf("%s failed: remaining item(s) %d", name+"/QueryItemsWithCallback", len(itemsMap))
	}
}

func TestAwsDynamodbConnect_BuildTxPut(t *testing.T) {
	name := "TestAwsDynamodbConnect_BuildTxPut"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	item := map[string]interface{}{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"name":     "Thanh Nguyen",
	}
	tx, err := adc.BuildTxPut(testDynamodbTableName, item, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%#e]", name, err)
	}
	if tx == nil {
		t.Fatalf("%s failed: nill", name)
	}
}

func TestAwsDynamodbConnect_BuildTxPutIfNotExist(t *testing.T) {
	name := "TestAwsDynamodbConnect_BuildTxPutIfNotExist"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	item := map[string]interface{}{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"name":     "Thanh Nguyen",
	}
	tx, err := adc.BuildTxPutIfNotExist(testDynamodbTableName, item, []string{"username"})
	if err != nil {
		t.Fatalf("%s failed: error [%#e]", name, err)
	}
	if tx == nil {
		t.Fatalf("%s failed: nill", name)
	}
}

func TestAwsDynamodbConnect_BuildTxDelete(t *testing.T) {
	name := "TestAwsDynamodbConnect_BuildTxDelete"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	keyFilter := map[string]interface{}{"username": "btnguyen2k"}
	{
		tx, err := adc.BuildTxDelete(testDynamodbTableName, keyFilter, nil)
		if err != nil {
			t.Fatalf("%s failed: error [%#e]", name, err)
		}
		if tx == nil {
			t.Fatalf("%s failed: nill", name)
		}
		if tx.Delete.ConditionExpression != nil {
			t.Fatalf("%s failed: not nill", name)
		}
	}

	{
		condition := AwsDynamodbExistsAllBuilder([]string{"version"})
		tx, err := adc.BuildTxDelete(testDynamodbTableName, keyFilter, condition)
		if err != nil {
			t.Fatalf("%s failed: error [%#e]", name, err)
		}
		if tx == nil {
			t.Fatalf("%s failed: nill", name)
		}
		if tx.Delete.ConditionExpression == nil {
			t.Fatalf("%s failed: nill", name)
		}
	}
}

func TestAwsDynamodbConnect_BuildTxGet(t *testing.T) {
	name := "TestAwsDynamodbConnect_BuildTxGet"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	keyFilter := map[string]interface{}{"username": "btnguyen2k"}
	tx, err := adc.BuildTxGet(testDynamodbTableName, keyFilter)
	if err != nil {
		t.Fatalf("%s failed: error [%#e]", name, err)
	}
	if tx == nil {
		t.Fatalf("%s failed: nill", name)
	}
}

func TestAwsDynamodbConnect_BuildTxRemoveAttributes(t *testing.T) {
	name := "TestAwsDynamodbConnect_BuildTxRemoveAttributes"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	keyFilter := map[string]interface{}{"username": "btnguyen2k"}
	tx, err := adc.BuildTxRemoveAttributes(testDynamodbTableName, keyFilter, nil, []string{"version"})
	if err != nil {
		t.Fatalf("%s failed: error [%#e]", name, err)
	}
	if tx == nil {
		t.Fatalf("%s failed: nill", name)
	}
}

func TestAwsDynamodbConnect_BuildTxSetAttributes(t *testing.T) {
	name := "TestAwsDynamodbConnect_BuildTxSetAttributes"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	keyFilter := map[string]interface{}{"username": "btnguyen2k"}
	attrsAndValues := map[string]interface{}{"version": "new version", "new_field": "a value"}
	tx, err := adc.BuildTxSetAttributes(testDynamodbTableName, keyFilter, nil, attrsAndValues)
	if err != nil {
		t.Fatalf("%s failed: error [%#e]", name, err)
	}
	if tx == nil {
		t.Fatalf("%s failed: nill", name)
	}
}

func TestAwsDynamodbConnect_BuildTxAddValuesToAttributes(t *testing.T) {
	name := "TestAwsDynamodbConnect_BuildTxAddValuesToAttributes"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	keyFilter := map[string]interface{}{"username": "btnguyen2k"}
	attrsAndValues := map[string]interface{}{"version": 1, "new_field": 2}
	tx, err := adc.BuildTxAddValuesToAttributes(testDynamodbTableName, keyFilter, nil, attrsAndValues)
	if err != nil {
		t.Fatalf("%s failed: error [%#e]", name, err)
	}
	if tx == nil {
		t.Fatalf("%s failed: nill", name)
	}
}

func TestAwsDynamodbConnect_BuildTxAddValuesToSet(t *testing.T) {
	name := "TestAwsDynamodbConnect_BuildTxAddValuesToSet"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	keyFilter := map[string]interface{}{"username": "btnguyen2k"}
	attrsAndValues := map[string]interface{}{"version": 1}
	tx, err := adc.BuildTxAddValuesToSet(testDynamodbTableName, keyFilter, nil, attrsAndValues)
	if err != nil {
		t.Fatalf("%s failed: error [%#e]", name, err)
	}
	if tx == nil {
		t.Fatalf("%s failed: nill", name)
	}
}

func TestAwsDynamodbConnect_BuildTxDeleteValuesFromSet(t *testing.T) {
	name := "TestAwsDynamodbConnect_BuildTxDeleteValuesFromSet"
	adc := _createAwsDynamodbConnect(t, name)
	defer adc.Close()

	keyFilter := map[string]interface{}{"username": "btnguyen2k"}
	attrsAndValues := map[string]interface{}{"version": 1}
	tx, err := adc.BuildTxDeleteValuesFromSet(testDynamodbTableName, keyFilter, nil, attrsAndValues)
	if err != nil {
		t.Fatalf("%s failed: error [%#e]", name, err)
	}
	if tx == nil {
		t.Fatalf("%s failed: nill", name)
	}
}
