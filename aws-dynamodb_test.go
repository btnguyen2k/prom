package prom

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"go.mongodb.org/mongo-driver/bson"
)

func TestIsAwsError(t *testing.T) {
	testName := "TestIsAwsError"
	if IsAwsError(nil, "0") {
		t.Fatalf("%s failed: %#v should not be an awserr.Error", testName, nil)
	}
	{
		e := errors.New("dummy")
		if IsAwsError(e, "0") {
			t.Fatalf("%s failed: %#v should not be an awserr.Error", testName, e)
		}
	}
	{
		e := awserr.New("123", "dummy", errors.New("dummy"))
		if !IsAwsError(e, "123") {
			t.Fatalf("%s failed: %#v", testName, e)
		}
	}
	{
		e := awserr.New("123", "dummy", errors.New("dummy"))
		if IsAwsError(e, "456") {
			t.Fatalf("%s failed: %#v", testName, e)
		}
	}
}

func TestAwsIgnoreErrorIfMatched(t *testing.T) {
	testName := "TestAwsIgnoreErrorIfMatched"
	{
		var e error = nil
		if AwsIgnoreErrorIfMatched(e, "0") != nil {
			t.Fatalf("%s failed: %#v", testName, e)
		}
	}
	{
		e := errors.New("dummy")
		if AwsIgnoreErrorIfMatched(e, "0") != e {
			t.Fatalf("%s failed: %#v", testName, e)
		}
	}
	{
		e := awserr.New("123", "dummy", errors.New("dummy"))
		if AwsIgnoreErrorIfMatched(e, "123") != nil {
			t.Fatalf("%s failed: %#v", testName, e)
		}
	}
	{
		e := awserr.New("123", "dummy", errors.New("dummy"))
		if AwsIgnoreErrorIfMatched(e, "456") != e {
			t.Fatalf("%s failed: %#v", testName, e)
		}
	}
}

func TestAwsDynamodbToAttributeValue_Bytes(t *testing.T) {
	testName := "TestAwsDynamodbToAttributeValue_Bytes"
	input := []byte{1, 2, 3}
	v := AwsDynamodbToAttributeValue(input)
	if v == nil || v.B == nil {
		t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.B", testName, input)
	}
}

func TestAwsDynamodbToAttributeValue_Bool(t *testing.T) {
	testName := "TestAwsDynamodbToAttributeValue_Bool"
	input := true
	v := AwsDynamodbToAttributeValue(input)
	if v == nil || v.BOOL == nil || *v.BOOL != input {
		t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.BOOL", testName, input)
	}
}

func TestAwsDynamodbToAttributeValue_String(t *testing.T) {
	testName := "TestAwsDynamodbToAttributeValue_String"
	input := "a string"
	v := AwsDynamodbToAttributeValue(input)
	if v == nil || v.S == nil || *v.S != input {
		t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.BOOL", testName, input)
	}
}

func TestAwsDynamodbToAttributeValue_List(t *testing.T) {
	testName := "TestAwsDynamodbToAttributeValue_List"
	input := []interface{}{true, 0, 1.2, "3"}
	v := AwsDynamodbToAttributeValue(input)
	if v == nil || v.L == nil || len(v.L) != len(input) {
		t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.L", testName, input)
	}
}

func TestAwsDynamodbToAttributeValue_Map(t *testing.T) {
	testName := "TestAwsDynamodbToAttributeValue_Map"
	a := []interface{}{true, 0, 1.2, "3"}
	m := map[string]interface{}{"b": false, "n": 0, "s": "a string"}
	input := map[string]interface{}{"b": true, "n1": 0, "n2": 1.2, "s": "3", "nested_a": a, "nested_m": m}
	v := AwsDynamodbToAttributeValue(input)
	if v == nil || v.M == nil || len(v.M) != len(input) {
		t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.M", testName, input)
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
	testName := "TestAwsDynamodbToAttributeValue_Struct"
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
		t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.M", testName, input)
	}
}

func TestAwsDynamodbToAttributeValue_Number(t *testing.T) {
	testName := "TestAwsDynamodbToAttributeValue_Number"
	{
		// convert int
		input := int(1)
		v := AwsDynamodbToAttributeValue(input)
		if v == nil || v.N == nil || *v.N != strconv.FormatInt(int64(input), 10) {
			t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.N", testName, input)
		}
	}
	{
		// convert uint
		input := uint(1)
		v := AwsDynamodbToAttributeValue(input)
		if v == nil || v.N == nil || *v.N != strconv.FormatUint(uint64(input), 10) {
			t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.N", testName, input)
		}
	}
	{
		// convert float32
		input := float32(1.0)
		v := AwsDynamodbToAttributeValue(input)
		if v == nil || v.N == nil {
			t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.N", testName, input)
		}
	}
	{
		// convert float64
		input := float64(1.0)
		v := AwsDynamodbToAttributeValue(input)
		if v == nil || v.N == nil {
			t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.N", testName, input)
		}
	}
}

func TestAwsDynamodbToAttributeSet(t *testing.T) {
	testName := "TestAwsDynamodbToAttributeSet"
	{
		// single number
		input := int(1)
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.NS == nil || len(v.NS) != 1 {
			if v == nil {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS", testName, input)
			} else {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS (result :%#v)", testName, input, v.N)
			}
		}
	}
	{
		// single number
		input := uint(2)
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.NS == nil || len(v.NS) != 1 {
			if v == nil {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS", testName, input)
			} else {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS (result :%#v)", testName, input, v.N)
			}
		}
	}
	{
		// single number
		input := float32(3.4)
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.NS == nil || len(v.NS) != 1 {
			if v == nil {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS", testName, input)
			} else {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS (result :%#v)", testName, input, v.N)
			}
		}
	}
	{
		// single string
		input := "a string"
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.SS == nil || len(v.SS) != 1 {
			if v == nil {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.SS", testName, input)
			} else {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.SS (result :%#v)", testName, input, v.N)
			}
		}
	}
	{
		// bytes
		input := []byte("a string")
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.BS == nil || len(v.BS) != 1 {
			if v == nil {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.BS", testName, input)
			} else {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.BS (result :%#v)", testName, input, v.N)
			}
		}
	}
	{
		// slice of bytes
		input := [][]byte{[]byte("string1"), []byte("string2"), []byte("string3")}
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.BS == nil || len(v.BS) != len(input) {
			if v == nil {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.BS", testName, input)
			} else {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.BS (result :%#v)", testName, input, v.N)
			}
		}
	}
	{
		// slice of int
		input := []int{1, 2, 3, 4}
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.NS == nil || len(v.NS) != len(input) {
			if v == nil {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS", testName, input)
			} else {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS (result :%#v)", testName, input, v.N)
			}
		}
	}
	{
		// slice of uint
		input := []uint{1, 2, 3, 4}
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.NS == nil || len(v.NS) != len(input) {
			if v == nil {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS", testName, input)
			} else {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS (result :%#v)", testName, input, v.N)
			}
		}
	}
	{
		// slice of floats
		input := []float64{1.2, 3.4, 5.6}
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.NS == nil || len(v.NS) != len(input) {
			if v == nil {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS", testName, input)
			} else {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.NS (result :%#v)", testName, input, v.N)
			}
		}
	}
	{
		// slice of strings
		input := []string{"1", "2.3"}
		v := AwsDynamodbToAttributeSet(input)
		if v == nil || v.SS == nil || len(v.SS) != len(input) {
			if v == nil {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.SS", testName, input)
			} else {
				t.Fatalf("%s failed: cannot convert %#v to dynamodb.AttributeValue.SS (result :%#v)", testName, input, v.N)
			}
		}
	}
}

func TestAwsDynamodbExistsAllBuilder(t *testing.T) {
	testName := "TestAwsDynamodbExistsAllBuilder"
	input := []string{"a", "b", "c"}
	conditionBuilder := AwsDynamodbExistsAllBuilder(input)
	builder, err := expression.NewBuilder().WithCondition(*conditionBuilder).Build()
	if err != nil {
		t.Fatalf("%s failed: %e", testName, err)
	}
	condition := builder.Condition()
	expected := "((attribute_exists (#0)) AND (attribute_exists (#1))) AND (attribute_exists (#2))"
	if condition == nil || *condition != expected {
		t.Fatalf("%s failed: expected [%s] but received [%s]", testName, expected, *condition)
	}
}

func TestAwsDynamodbNotExistsAllBuilder(t *testing.T) {
	testName := "TestAwsDynamodbNotExistsAllBuilder"
	input := []string{"a", "b", "c"}
	conditionBuilder := AwsDynamodbNotExistsAllBuilder(input)
	builder, err := expression.NewBuilder().WithCondition(*conditionBuilder).Build()
	if err != nil {
		t.Fatalf("%s failed: %e", testName, err)
	}
	condition := builder.Condition()
	expected := "((attribute_not_exists (#0)) AND (attribute_not_exists (#1))) AND (attribute_not_exists (#2))"
	if condition == nil || *condition != expected {
		t.Fatalf("%s failed: expected [%s] but received [%s]", testName, expected, *condition)
	}
}

func TestAwsDynamodbEqualsBuilder(t *testing.T) {
	testName := "TestAwsDynamodbEqualsBuilder"
	input := map[string]interface{}{"s": "a string", "n": 0, "b": true}
	conditionBuilder := AwsDynamodbEqualsBuilder(input)
	builder, err := expression.NewBuilder().WithCondition(*conditionBuilder).Build()
	if err != nil {
		t.Fatalf("%s failed: %e", testName, err)
	}
	condition := builder.Condition()
	expected := "((#0 = :0) AND (#1 = :1)) AND (#2 = :2)"
	if condition == nil || *condition != expected {
		t.Fatalf("%s failed: expected [%s] but received [%s]", testName, expected, *condition)
	}
}

func TestAwsDynamodbFastFailed(t *testing.T) {
	testName := "TestAwsDynamodbFastFailed"
	cfg := &aws.Config{
		Region:      aws.String("dummy"),
		Credentials: credentials.NewStaticCredentials("id", "secret", "token"),
		DisableSSL:  aws.Bool(true),
		Endpoint:    aws.String("http://localhost:1234"),
	}
	timeoutMs := 100
	adc, err := NewAwsDynamodbConnect(cfg, nil, nil, timeoutMs)
	if err != nil {
		t.Fatalf("%s/%s failed: %s", testName, "NewAwsDynamodbConnect", err)
	}
	tstart := time.Now()
	_, err = adc.HasTable(nil, "mytable")
	if err == nil {
		t.Fatalf("%s failed: the operation should not success", testName)
	}
	d := time.Duration(time.Now().UnixNano() - tstart.UnixNano())
	dmax := time.Duration(float64(time.Duration(timeoutMs)*time.Millisecond) * 1.5)
	if d > dmax {
		t.Fatalf("%s failed: operation is expected to fail within %#v ms but in fact %#v ms", testName, dmax/1e6, d/1e6)
	}
}

func _createAwsDynamodbConnect(t *testing.T, testName string) *AwsDynamodbConnect {
	creProvider := &credentials.EnvProvider{}
	_, err := creProvider.Retrieve()
	if err == credentials.ErrAccessKeyIDNotFound || err == credentials.ErrSecretAccessKeyNotFound {
		t.Skipf("%s skipped", testName)
		return nil
	}
	awsRegion := strings.ReplaceAll(os.Getenv("AWS_REGION"), `"`, "")
	cfg := &aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: credentials.NewCredentials(creProvider),
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
	testName := "TestNewAwsDynamodbConnect"
	adc := _createAwsDynamodbConnect(t, testName)
	if adc == nil {
		t.Fatalf("%s failed: nil", testName)
	}
	defer adc.Close()
	if err := adc.Init(); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
}

func TestNewAwsDynamodbConnect_timeout(t *testing.T) {
	testName := "TestNewAwsDynamodbConnect_timeout"
	region := "ap-southeast-1"
	cfg := &aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewEnvCredentials(),
	}
	adc, err := NewAwsDynamodbConnect(cfg, nil, nil, -1)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName, err)
	}
	if adc == nil {
		t.Fatalf("%s failed: nil", testName)
	}
	defer adc.Close()
	if adc.timeoutMs < 0 {
		t.Fatalf("%s failed: invalid timeout value #%v", testName, adc.timeoutMs)
	}
}

func TestNewAwsDynamodbConnect_MetricsLogger(t *testing.T) {
	testName := "TestNewAwsDynamodbConnect_MetricsLogger"
	cfg := &aws.Config{
		Region:      aws.String(testRegion),
		Credentials: credentials.NewEnvCredentials(),
	}
	adc, err := NewAwsDynamodbConnect(cfg, nil, nil, -1)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName, err)
	}
	if adc == nil {
		t.Fatalf("%s failed: nil", testName)
	}
	defer adc.Close()
	if adc.MetricsLogger() == nil {
		t.Fatalf("%s failed: nil", testName)
	}

	ml := &MemoryStoreMetricsLogger{capacity: 1028}
	adc.RegisterMetricsLogger(ml)
	if adc.MetricsLogger() != ml {
		t.Fatalf("%s failed.", testName)
	}
}

func TestNewAwsDynamodbConnect_nil(t *testing.T) {
	testName := "TestNewAwsDynamodbConnect_nil"
	adc, err := NewAwsDynamodbConnect(nil, nil, nil, -1)
	if err == nil || adc != nil {
		t.Fatalf("%s failed: AwsDynamodbConnect should not be created", testName)
	}
}

var testRegion = "ap-southeast-1"

func TestAwsDynamodbConnect_Close(t *testing.T) {
	testName := "TestAwsDynamodbConnect_Close"
	adc := _createAwsDynamodbConnect(t, testName)
	err := adc.Close()
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName, err)
	}
}

func TestAwsDynamodbConnect_GetDb(t *testing.T) {
	testName := "TestAwsDynamodbConnect_GetDb"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	if adc.GetDb() == nil {
		t.Fatalf("%s failed: nil", testName)
	}
}

func TestAwsDynamodbConnect_GetDbProxy(t *testing.T) {
	testName := "TestAwsDynamodbConnect_GetDbProxy"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	if adc.GetDbProxy() == nil {
		t.Fatalf("%s failed: nil", testName)
	}
	adc.dbProxy = nil
	if adc.GetDbProxy() == nil {
		t.Fatalf("%s failed: nil", testName)
	}
}

func TestAwsDynamodbConnect_NewContext(t *testing.T) {
	testName := "TestAwsDynamodbConnect_NewContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	if ctx, _ := adc.NewContext(); ctx == nil {
		t.Fatalf("%s failed: nil", testName)
	}
	if ctx, _ := adc.NewContext(1234); ctx == nil {
		t.Fatalf("%s failed: nil", testName)
	}
}

func prepareAwsDynamodbTable(adc *AwsDynamodbConnect, table string) error {
	err := adc.DeleteTable(nil, table)
	if AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException) != nil {
		return err
	}
	fmt.Printf("\tDeleted table [%s]\n", table)
	AwsDynamodbWaitForTableStatus(adc, table, []string{""}, 5*time.Second, 60*time.Second)

	err = adc.CreateTable(nil, table, 2, 2,
		[]AwsDynamodbNameAndType{{"username", AwsAttrTypeString}, {"email", AwsAttrTypeString}},
		[]AwsDynamodbNameAndType{{"username", AwsKeyTypePartition}, {"email", AwsKeyTypeSort}})
	if AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException) != nil {
		return err
	}
	AwsDynamodbWaitForTableStatus(adc, table, []string{"ACTIVE"}, 5*time.Second, 60*time.Second)
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

func _adcVerifyLastCommand(f _testFailedWithMsgFunc, testName string, adc *AwsDynamodbConnect, cmdName string, ignoreErrorCodes []string, cmdCats ...string) {
	for _, cat := range cmdCats {
		m, err := adc.Metrics(cat, MetricsOpts{ReturnLatestCommands: 1})
		if err != nil {
			f(fmt.Sprintf("%s failed: error [%e]", testName+"/Metrics("+cat+")", err))
		}
		if m == nil {
			f(fmt.Sprintf("%s failed: cannot obtain metrics info", testName+"/Metrics("+cat+")"))
		}
		if e, v := 1, len(m.LastNCmds); e != v {
			f(fmt.Sprintf("%s failed: expected %v last command returned but received %v", testName+"/Metrics("+cat+")", e, v))
		}
		cmd := m.LastNCmds[0]
		cmd.CmdRequest, cmd.CmdResponse, cmd.CmdMeta = nil, nil, nil
		if cmd.CmdName == cmdName && cmd.Error != nil {
			for _, errCode := range ignoreErrorCodes {
				if AwsIgnoreErrorIfMatched(cmd.Error, errCode) == nil {
					return
				}
			}
		}
		if cmd.CmdName != cmdName || cmd.Result != CmdResultOk || cmd.Error != nil || cmd.Cost <= 0 {
			f(fmt.Sprintf("%s failed: invalid last command metrics.\nExpected: [Name=%v / Result=%v / Error = %e / Cost = %v]\nReceived: [Name=%v / Result=%v / Error = %s / Cost = %v]",
				testName+"/Metrics("+cat+")",
				cmdName, CmdResultOk, error(nil), "> 0",
				cmd.CmdName, cmd.Result, cmd.Error, cmd.Cost))
		}
	}
}

func TestAwsDynamodbConnect_ListTables(t *testing.T) {
	testName := "TestAwsDynamodbConnect_ListTables"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	tables, err := adc.ListTables(nil)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName, err)
	}
	if tables == nil {
		t.Fatalf("%s failed: nil", testName)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbListTables, nil, MetricsCatAll, MetricsCatOther)
}

func TestAwsDynamodbConnect_TableAndIndex(t *testing.T) {
	testName := "TestAwsDynamodbConnect_TableAndIndex"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}

	err := adc.CreateTable(nil, testDynamodbTableName, 2, 2,
		[]AwsDynamodbNameAndType{{"username", AwsAttrTypeString}},
		[]AwsDynamodbNameAndType{{"username", AwsKeyTypePartition}})
	if err = AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException); err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/CreateTable", err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbCreateTable, []string{dynamodb.ErrCodeResourceInUseException}, MetricsCatAll, MetricsCatDDL)

	ok, err := adc.HasTable(nil, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/HasTable", err)
	}
	if !ok {
		t.Fatalf("%s failed: table [%s] not found", testName+"/HasTable", testDynamodbTableName)
	}
	fmt.Printf("\tCreated table [%s]\n", testDynamodbTableName)
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbListTables, nil, MetricsCatAll, MetricsCatOther)

	AwsDynamodbWaitForTableStatus(adc, testDynamodbTableName, []string{"ACTIVE"}, 5*time.Second, 60*time.Second)
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescTable, nil, MetricsCatAll, MetricsCatOther)

	if os.Getenv(dynamodbTestGsiName) != "" {
		testDynamodbGsiName = os.Getenv(dynamodbTestGsiName)
	}

	{
		err = adc.DeleteGlobalSecondaryIndex(nil, testDynamodbTableName, testDynamodbGsiName)
		if AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException) != nil {
			t.Fatalf("%s failed: error [%e]", testName+"/DeleteGlobalSecondaryIndex", err)
		}
		fmt.Printf("\tDeleted GSI [%s] on table [%s]\n", testDynamodbGsiName, testDynamodbTableName)
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateTable, []string{dynamodb.ErrCodeResourceNotFoundException}, MetricsCatAll, MetricsCatDDL)
		AwsDynamodbWaitForGsiStatus(adc, testDynamodbTableName, testDynamodbGsiName, []string{""}, 5*time.Second, 60*time.Second)
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescTable, nil, MetricsCatAll, MetricsCatOther)
	}

	err = adc.CreateGlobalSecondaryIndex(nil, testDynamodbTableName, testDynamodbGsiName, 1, 1,
		[]AwsDynamodbNameAndType{{"email", AwsAttrTypeString}},
		[]AwsDynamodbNameAndType{{"email", AwsKeyTypePartition}})
	if AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException) != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/CreateGlobalSecondaryIndex", err)
	}
	fmt.Printf("\tCreated GSI [%s] on table [%s]\n", testDynamodbGsiName, testDynamodbTableName)
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateTable, []string{dynamodb.ErrCodeResourceInUseException}, MetricsCatAll, MetricsCatDDL)

	AwsDynamodbWaitForGsiStatus(adc, testDynamodbTableName, testDynamodbGsiName, []string{"ACTIVE", "CREATING"}, 5*time.Second, 60*time.Second)
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescTable, nil, MetricsCatAll, MetricsCatOther)

	time.Sleep(10 * time.Second)

	err = adc.DeleteGlobalSecondaryIndex(nil, testDynamodbTableName, testDynamodbGsiName)
	if AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException) != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/DeleteGlobalSecondaryIndex", err)
	}
	fmt.Printf("\tDeleted GSI [%s] on table [%s]\n", testDynamodbGsiName, testDynamodbTableName)
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateTable, []string{dynamodb.ErrCodeResourceInUseException}, MetricsCatAll, MetricsCatDDL)

	AwsDynamodbWaitForGsiStatus(adc, testDynamodbTableName, testDynamodbGsiName, []string{""}, 5*time.Second, 60*time.Second)
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescTable, nil, MetricsCatAll, MetricsCatOther)

	err = adc.DeleteTable(nil, testDynamodbTableName)
	if AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException) != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/DeleteTable", err)
	}
	fmt.Printf("\tDeleted table [%s]\n", testDynamodbTableName)
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDeleteTable, nil, MetricsCatAll, MetricsCatDDL)

	AwsDynamodbWaitForTableStatus(adc, testDynamodbTableName, []string{""}, 5*time.Second, 60*time.Second)
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescTable, []string{dynamodb.ErrCodeResourceNotFoundException}, MetricsCatAll, MetricsCatOther)

	ok, err = adc.HasTable(nil, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/HasTable", err)
	}
	if ok {
		t.Fatalf("%s failed: table [%s] not deleted", testName+"/HasTable", testDynamodbTableName)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbListTables, nil, MetricsCatAll, MetricsCatOther)
}

func TestAwsDynamodbConnect_PutItem(t *testing.T) {
	testName := "TestAwsDynamodbConnect_PutItem"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	item := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  time.Now().Unix(),
		"active":   true,
	}
	if _, err = adc.PutItem(nil, testDynamodbTableName, item, nil); err != nil {
		t.Fatalf("%s failed: error [%e]", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, nil, MetricsCatAll, MetricsCatDML)

	if _, err = adc.PutItem(nil, testDynamodbTableName, item, AwsDynamodbExistsAllBuilder([]string{"notexists"})); AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeConditionalCheckFailedException) != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, []string{dynamodb.ErrCodeConditionalCheckFailedException}, MetricsCatAll, MetricsCatDML)
}

func TestAwsDynamodbConnect_GetPutItem(t *testing.T) {
	testName := "TestAwsDynamodbConnect_GetPutItem"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	item1 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  float64(time.Now().Unix()),
		"active":   true,
	}
	item2 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  nil,
		"active":   false,
		"name":     "Thanh Nguyen",
	}
	item3 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  float64(123),
		"name":     "Tom",
	}
	testData := []struct {
		item         bson.M
		condition    *expression.ConditionBuilder
		shouldWrite  bool
		expectedItem bson.M
	}{
		{item1, nil, true, item1},
		{item2, AwsDynamodbExistsAllBuilder([]string{"notexists"}), false, item1},
		{item3, nil, true, item3},
	}

	var fetchedItem AwsDynamodbItem
	keyFilter := bson.M{"username": "btnguyen2k", "email": "me@domain.com"}

	// GetItem: must be "not found"
	if fetchedItem, err = adc.GetItem(nil, testDynamodbTableName, keyFilter); err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/GetItem", err)
	} else if fetchedItem != nil {
		t.Fatalf("%s failed: item should not exist", testName+"/GetItem")
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbGetItem, nil, MetricsCatAll, MetricsCatDQL)

	for i, data := range testData {
		// PutItem: must be successful (new item or overriding existing one)
		_, err = adc.PutItem(nil, testDynamodbTableName, data.item, data.condition)
		if AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeConditionalCheckFailedException) != nil {
			t.Fatalf("%s failed: error [%s]", testName+"/PutItem/"+strconv.Itoa(i), err)
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, []string{dynamodb.ErrCodeConditionalCheckFailedException}, MetricsCatAll, MetricsCatDML)

		// GetItem: must match the original one
		if fetchedItem, err = adc.GetItem(nil, testDynamodbTableName, keyFilter); fetchedItem == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/GetItem/"+strconv.Itoa(i), err)
		} else {
			var m = bson.M(fetchedItem)
			if !reflect.DeepEqual(m, data.expectedItem) {
				t.Fatalf("%s failed: fetched [%#v] vs original [%#v]", testName+"/GetItem/"+strconv.Itoa(i), m, data.expectedItem)
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbGetItem, nil, MetricsCatAll, MetricsCatDQL)
	}
}

func TestAwsDynamodbConnect_PutItemIfNotExist(t *testing.T) {
	testName := "TestAwsDynamodbConnect_PutItemIfNotExist"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	item1 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"name":     "Thanh Nguyen",
	}
	item2 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  float64(time.Now().Unix()),
		"active":   false,
		"name":     "Thanh Nguyen",
	}
	item3 := bson.M{
		"username": "thanhn",
		"email":    "me@domain.com",
		"version":  nil,
		"name":     "Tom",
	}
	testData := []struct {
		item, expected, filter bson.M
		shouldWrite            bool
	}{
		{item1, item1, bson.M{"username": "btnguyen2k", "email": "me@domain.com"}, true},
		{item2, item1, bson.M{"username": "btnguyen2k", "email": "me@domain.com"}, false},
		{item3, item3, bson.M{"username": "thanhn", "email": "me@domain.com"}, true},
	}
	pkAttrs := []string{"username"}
	var fetchedItem AwsDynamodbItem
	for i, data := range testData {
		// PutItem: must be successful for new item
		if _, err = adc.PutItemIfNotExist(nil, testDynamodbTableName, data.item, pkAttrs); err != nil {
			t.Fatalf("%s failed: error [%s]", testName+"/PutItem/"+strconv.Itoa(i), err)
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, []string{dynamodb.ErrCodeConditionalCheckFailedException}, MetricsCatAll, MetricsCatDML)

		// GetItem: must match the expected one
		if fetchedItem, err = adc.GetItem(nil, testDynamodbTableName, data.filter); fetchedItem == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/GetItem/"+strconv.Itoa(i), err)
		} else {
			var m = bson.M(fetchedItem)
			if !reflect.DeepEqual(m, data.expected) {
				t.Fatalf("%s failed: fetched\n%#v\nvs original\n%#v", testName+"/GetItem/"+strconv.Itoa(i), m, data.expected)
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbGetItem, nil, MetricsCatAll, MetricsCatDQL)
	}
}

func TestAwsDynamodbConnect_DeleteItem(t *testing.T) {
	testName := "TestAwsDynamodbConnect_DeleteItem"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	item1 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  float64(time.Now().Unix()),
		"active":   true,
	}
	item2 := bson.M{
		"username": "thanhn",
		"email":    "me@domain.com",
		"version":  nil,
		"active":   false,
		"name":     "Thanh Nguyen",
	}
	testData := []struct {
		item, keyFilter bson.M
		condition       *expression.ConditionBuilder
		shouldDelete    bool
	}{
		{item1, bson.M{"username": "btnguyen2k", "email": "me@domain.com"}, nil, true},
		{item2, bson.M{"username": "thanhn", "email": "me@domain.com"}, AwsDynamodbExistsAllBuilder([]string{"notexists"}), false},
	}

	var fetchedItem AwsDynamodbItem
	for i, data := range testData {
		// firstly: put item to the table
		if _, err = adc.PutItem(nil, testDynamodbTableName, data.item, nil); err != nil {
			t.Fatalf("%s failed: error [%s]", testName+"/PutItem/"+strconv.Itoa(i), err)
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, nil, MetricsCatAll, MetricsCatDML)

		// secondly: delete it
		if _, err = adc.DeleteItem(nil, testDynamodbTableName, data.keyFilter, data.condition); err != nil {
			if data.shouldDelete || AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeConditionalCheckFailedException) != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/DeleteItem/"+strconv.Itoa(i), err)
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDeleteItem, []string{dynamodb.ErrCodeConditionalCheckFailedException}, MetricsCatAll, MetricsCatDML)

		// lastly: fetch it back
		if fetchedItem, err = adc.GetItem(nil, testDynamodbTableName, data.keyFilter); err != nil {
			t.Fatalf("%s failed: error [%s]", testName+"/GetItem/"+strconv.Itoa(i), err)
		}
		if data.shouldDelete {
			if fetchedItem != nil {
				// item must be deleted
				t.Fatalf("%s failed: item must be deleted", testName+"/GetItem/"+strconv.Itoa(i))
			}
		} else {
			if fetchedItem == nil {
				t.Fatalf("%s failed: nil", testName+"/GetItem/"+strconv.Itoa(i))
			} else {
				var m = bson.M(fetchedItem)
				if !reflect.DeepEqual(m, data.item) {
					t.Fatalf("%s failed: fetched\n%#v\nvs original\n%#v", testName+"/WrapTxGetItems/"+strconv.Itoa(i), m, data.item)
				}
			}
		}
	}
}

func TestAwsDynamodbConnect_RemoveAttributes(t *testing.T) {
	testName := "TestAwsDynamodbConnect_RemoveAttributes"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	item0 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  float64(time.Now().Unix()),
		"active":   true,
	}
	item1 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
	}
	keyFilter := bson.M{"username": "btnguyen2k", "email": "me@domain.com"}
	testData := []struct {
		orgItem, newItem bson.M
		fieldsToRemove   []string
		condition        *expression.ConditionBuilder
		shouldUpdate     bool
	}{
		{item0, item1, []string{"version", "active"}, nil, true},
		{item0, item0, []string{"version"}, AwsDynamodbExistsAllBuilder([]string{"notexists"}), false},
	}

	var fetchedItem AwsDynamodbItem
	for i, data := range testData {
		// firstly: put item to the table
		if _, err = adc.PutItem(nil, testDynamodbTableName, data.orgItem, nil); err != nil {
			t.Fatalf("%s failed: error [%s]", testName+"/PutItem/"+strconv.Itoa(i), err)
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, nil, MetricsCatAll, MetricsCatDML)

		// secondly: update it
		if _, err = adc.RemoveAttributes(nil, testDynamodbTableName, keyFilter, data.condition, data.fieldsToRemove); err != nil {
			if data.shouldUpdate || AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeConditionalCheckFailedException) != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/RemoveAttributes/"+strconv.Itoa(i), err)
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateItem, []string{dynamodb.ErrCodeConditionalCheckFailedException}, MetricsCatAll, MetricsCatDML)

		// lastly: fetch it back
		if fetchedItem, err = adc.GetItem(nil, testDynamodbTableName, keyFilter); fetchedItem == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/GetItem/"+strconv.Itoa(i), err)
		} else {
			var m = bson.M(fetchedItem)
			if !reflect.DeepEqual(m, data.newItem) {
				t.Fatalf("%s failed: fetched\n%#v\nvs original\n%#v", testName+"/GetItem/"+strconv.Itoa(i), m, data.newItem)
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbGetItem, nil, MetricsCatAll, MetricsCatDQL)
	}
}

func TestAwsDynamodbConnect_SetAttributes(t *testing.T) {
	testName := "TestAwsDynamodbConnect_SetAttributes"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	item0 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  float64(time.Now().Unix()),
		"active":   true,
	}
	newFieldsAndValues := bson.M{
		"name":    "Thanh Nguyen",
		"active":  false,
		"version": nil,
	}
	item1 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"active":   false,
		"version":  nil,
		"name":     "Thanh Nguyen",
	}
	keyFilter := map[string]interface{}{"username": "btnguyen2k", "email": "me@domain.com"}
	testData := []struct {
		orgItem, newItem, newFieldsAndValues bson.M
		condition                            *expression.ConditionBuilder
		shouldUpdate                         bool
	}{
		{item0, item1, newFieldsAndValues, nil, true},
		{item0, item0, newFieldsAndValues, AwsDynamodbExistsAllBuilder([]string{"notexists"}), false},
	}

	var fetchedItem AwsDynamodbItem
	for i, data := range testData {
		// firstly: put item to the table
		if _, err = adc.PutItem(nil, testDynamodbTableName, data.orgItem, nil); err != nil {
			t.Fatalf("%s failed: error [%s]", testName+"/PutItem/"+strconv.Itoa(i), err)
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, nil, MetricsCatAll, MetricsCatDML)

		// secondly: update it
		if _, err = adc.SetAttributes(nil, testDynamodbTableName, keyFilter, data.condition, data.newFieldsAndValues); err != nil {
			if data.shouldUpdate || AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeConditionalCheckFailedException) != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/SetAttributes/"+strconv.Itoa(i), err)
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateItem, []string{dynamodb.ErrCodeConditionalCheckFailedException}, MetricsCatAll, MetricsCatDML)

		// lastly: fetch it back
		if fetchedItem, err = adc.GetItem(nil, testDynamodbTableName, keyFilter); fetchedItem == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/GetItem/"+strconv.Itoa(i), err)
		} else {
			var m = bson.M(fetchedItem)
			if !reflect.DeepEqual(m, data.newItem) {
				t.Fatalf("%s failed: fetched\n%#v\nvs original\n%#v", testName+"/GetItem/"+strconv.Itoa(i), m, data.newItem)
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbGetItem, nil, MetricsCatAll, MetricsCatDQL)
	}
}

func TestAwsDynamodbConnect_AddValuesToAttributes(t *testing.T) {
	testName := "TestAwsDynamodbConnect_AddValuesToAttributes"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	item0 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"b":        true,
		"s":        "a string",
		"n":        1.0,
		"m":        map[string]interface{}{"b": 0.0, "n": 2.0, "s": "a string"},
		"a":        []interface{}{0.0, 1.0, "a string"},
		"an":       []interface{}{1.0, 2.0, 3.0},
		"as":       []interface{}{"1", "2", "3"},
	}
	attrsAndValuesToAdd := bson.M{
		"a[1]":  1.1,   // a[1]'s value is added by 1.1 --> new value 2.1
		"a[10]": 12.34, // new value 12.34 is appended to array a
		"m.n":   1.2,   // m.n's value is added by 1.2 --> new value 3.2
		"m.new": 3.0,   // m.new does not exist, its value is assumed zero, hence new key m.new is created with value 3.0
		"n0":    1.2,   // n0 does not exist, its value is assumed zero, hence new attribute n0 is created with value 1.2
		"n":     2.3,   // n's value is added by 2.3 --> new value 3.3
	}
	item1 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"b":        true,
		"s":        "a string",
		"n":        1.0 + 2.3,
		"m":        map[string]interface{}{"b": 0.0, "n": 2.0 + 1.2, "s": "a string", "new": 3.0},
		"a":        []interface{}{0.0, 1.0 + 1.1, "a string", 12.34},
		"an":       []interface{}{1.0, 2.0, 3.0},
		"as":       []interface{}{"1", "2", "3"},
		"n0":       1.2,
	}
	keyFilter := bson.M{"username": "btnguyen2k", "email": "me@domain.com"}
	testData := []struct {
		orgItem, newItem, attrsAndValuesToAdd bson.M
		condition                             *expression.ConditionBuilder
		shouldUpdate                          bool
	}{
		{item0, item1, attrsAndValuesToAdd, nil, true},
		{item0, item0, attrsAndValuesToAdd, AwsDynamodbExistsAllBuilder([]string{"notexists"}), false},
	}

	var fetchedItem AwsDynamodbItem
	for i, data := range testData {
		// firstly: put item to the table
		if _, err = adc.PutItem(nil, testDynamodbTableName, data.orgItem, nil); err != nil {
			t.Fatalf("%s failed: error [%s]", testName+"/PutItem/"+strconv.Itoa(i), err)
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, nil, MetricsCatAll, MetricsCatDML)

		// secondly: update it
		if _, err = adc.AddValuesToAttributes(nil, testDynamodbTableName, keyFilter, data.condition, data.attrsAndValuesToAdd); err != nil {
			if data.shouldUpdate || AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeConditionalCheckFailedException) != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/AddValuesToAttributes/"+strconv.Itoa(i), err)
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateItem, []string{dynamodb.ErrCodeConditionalCheckFailedException}, MetricsCatAll, MetricsCatDML)

		// lastly: fetch it back
		if fetchedItem, err = adc.GetItem(nil, testDynamodbTableName, keyFilter); fetchedItem == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/GetItem/"+strconv.Itoa(i), err)
		} else {
			var m = bson.M(fetchedItem)
			if !reflect.DeepEqual(m, data.newItem) {
				t.Fatalf("%s failed: fetched\n%#v\nvs original\n%#v", testName+"/GetItem/"+strconv.Itoa(i), m, data.newItem)
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbGetItem, nil, MetricsCatAll, MetricsCatDQL)
	}
}

func TestAwsDynamodbConnect_AddValuesToSet(t *testing.T) {
	testName := "TestAwsDynamodbConnect_AddValuesToSet"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	item0Raw := map[string]*dynamodb.AttributeValue{
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
	item0 := bson.M{
		"username": "btnguyen2k",
		"an":       []float64{1.0, 2.0, 3.0},
		"as":       []string{"1", "2", "3"},
		"a":        []interface{}{},
		"email":    "me@domain.com",
		"m":        map[string]interface{}{},
	}
	attrsAndValuesToAdd1 := bson.M{"an": 8, "as": []string{"9", "10"}}
	item1 := bson.M{
		"username": "btnguyen2k",
		"an":       []float64{1.0, 2.0, 3.0, 8.0},
		"as":       []string{"1", "10", "2", "3", "9"},
		"a":        []interface{}{},
		"email":    "me@domain.com",
		"m":        map[string]interface{}{},
	}
	attrsAndValuesToAdd2 := bson.M{
		"an": &dynamodb.AttributeValue{NS: []*string{aws.String("7.0")}},
		"as": &dynamodb.AttributeValue{SS: []*string{aws.String("7"), aws.String("8")}},
	}
	item2 := bson.M{
		"username": "btnguyen2k",
		"an":       []float64{1.0, 2.0, 3.0, 7.0},
		"as":       []string{"1", "2", "3", "7", "8"},
		"a":        []interface{}{},
		"email":    "me@domain.com",
		"m":        map[string]interface{}{},
	}
	testData := []struct {
		orgItem                      map[string]*dynamodb.AttributeValue
		attrsAndValuesToAdd, newItem bson.M
		condition                    *expression.ConditionBuilder
		shouldUpdate                 bool
	}{
		{item0Raw, attrsAndValuesToAdd1, item1, nil, true},
		{item0Raw, attrsAndValuesToAdd2, item2, nil, true},
		{item0Raw, attrsAndValuesToAdd1, item0, AwsDynamodbExistsAllBuilder([]string{"notexists"}), false},
	}
	keyFilter := bson.M{"username": "btnguyen2k", "email": "me@domain.com"}

	var fetchedItem AwsDynamodbItem
	for i, data := range testData {
		// firstly: put item to the table
		if _, err = adc.PutItemRaw(nil, testDynamodbTableName, data.orgItem, nil); err != nil {
			t.Fatalf("%s failed: error [%s]", testName+"/PutItem/"+strconv.Itoa(i), err)
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, nil, MetricsCatAll, MetricsCatDML)

		// secondly: update it
		if _, err = adc.AddValuesToSet(nil, testDynamodbTableName, keyFilter, data.condition, data.attrsAndValuesToAdd); err != nil {
			if data.shouldUpdate || AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeConditionalCheckFailedException) != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/AddValuesToSet/"+strconv.Itoa(i), err)
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateItem, []string{dynamodb.ErrCodeConditionalCheckFailedException}, MetricsCatAll, MetricsCatDML)

		// lastly: fetch it back
		if fetchedItem, err = adc.GetItem(nil, testDynamodbTableName, keyFilter); fetchedItem == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/GetItem/"+strconv.Itoa(i), err)
		} else {
			var m = bson.M(fetchedItem)
			if !reflect.DeepEqual(m, data.newItem) {
				t.Fatalf("%s failed: fetched\n%#v\nvs original\n%#v", testName+"/GetItem/"+strconv.Itoa(i), m, data.newItem)
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbGetItem, nil, MetricsCatAll, MetricsCatDQL)
	}
}

func TestAwsDynamodbConnect_DeleteValuesFromSet(t *testing.T) {
	testName := "TestAwsDynamodbConnect_DeleteValuesFromSet"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	item0Raw := map[string]*dynamodb.AttributeValue{
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
	item0 := bson.M{
		"username": "btnguyen2k",
		"an":       []float64{1.0, 2.0, 3.0},
		"as":       []string{"1", "2", "3"},
		"a":        []interface{}{},
		"email":    "me@domain.com",
		"m":        map[string]interface{}{},
	}
	attrsAndValuesToDelete1 := bson.M{"an": 1, "as": []string{"1", "3", "5"}}
	item1 := bson.M{
		"username": "btnguyen2k",
		"an":       []float64{2.0, 3.0},
		"as":       []string{"2"},
		"a":        []interface{}{},
		"email":    "me@domain.com",
		"m":        map[string]interface{}{},
	}
	attrsAndValuesToDelete2 := bson.M{
		"an": &dynamodb.AttributeValue{NS: []*string{aws.String("3.0")}},
		"as": &dynamodb.AttributeValue{SS: []*string{aws.String("2"), aws.String("3")}},
	}
	item2 := bson.M{
		"username": "btnguyen2k",
		"an":       []float64{1.0, 2.0},
		"as":       []string{"1"},
		"a":        []interface{}{},
		"email":    "me@domain.com",
		"m":        map[string]interface{}{},
	}
	testData := []struct {
		orgItem                         map[string]*dynamodb.AttributeValue
		attrsAndValuesToDelete, newItem bson.M
		condition                       *expression.ConditionBuilder
		shouldUpdate                    bool
	}{
		{item0Raw, attrsAndValuesToDelete1, item1, nil, true},
		{item0Raw, attrsAndValuesToDelete2, item2, nil, true},
		{item0Raw, attrsAndValuesToDelete1, item0, AwsDynamodbExistsAllBuilder([]string{"notexists"}), false},
	}
	keyFilter := bson.M{"username": "btnguyen2k", "email": "me@domain.com"}

	var fetchedItem AwsDynamodbItem
	for i, data := range testData {
		// firstly: put item to the table
		if _, err = adc.PutItemRaw(nil, testDynamodbTableName, data.orgItem, nil); err != nil {
			t.Fatalf("%s failed: error [%s]", testName+"/PutItem/"+strconv.Itoa(i), err)
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, nil, MetricsCatAll, MetricsCatDML)

		// secondly: update it
		if _, err = adc.DeleteValuesFromSet(nil, testDynamodbTableName, keyFilter, data.condition, data.attrsAndValuesToDelete); err != nil {
			if data.shouldUpdate || AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeConditionalCheckFailedException) != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/DeleteValuesFromSet/"+strconv.Itoa(i), err)
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateItem, []string{dynamodb.ErrCodeConditionalCheckFailedException}, MetricsCatAll, MetricsCatDML)

		// lastly: fetch it back
		if fetchedItem, err = adc.GetItem(nil, testDynamodbTableName, keyFilter); fetchedItem == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/GetItem/"+strconv.Itoa(i), err)
		} else {
			var m = bson.M(fetchedItem)
			if !reflect.DeepEqual(m, data.newItem) {
				t.Fatalf("%s failed: fetched\n%#v\nvs original\n%#v", testName+"/GetItem/"+strconv.Itoa(i), m, data.newItem)
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbGetItem, nil, MetricsCatAll, MetricsCatDQL)
	}
}

func TestAwsDynamodbConnect_ScanItems(t *testing.T) {
	testName := "TestAwsDynamodbConnect_ScanItems"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	numItems := 1024
	itemsMap := make(map[string]map[string]interface{})
	for i := 0; i < numItems; i++ {
		id := strconv.Itoa(i)
		item := map[string]interface{}{
			"username": id,
			"email":    id + "@domain.com",
			"testName": "Thanh " + id,
		}
		_, err = adc.PutItem(nil, testDynamodbTableName, item, nil)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", testName+"/PutItem", err)
		}
		itemsMap[id] = item
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, nil, MetricsCatAll, MetricsCatDML)
	}

	rand.Seed(time.Now().UnixNano())
	filter1 := rand.Intn(numItems / 5)
	filter1Str := strconv.Itoa(filter1)
	filter2 := numItems*4/5 + rand.Intn(numItems/5)
	filter2Str := strconv.Itoa(filter2) + "@domain.com"
	filter := expression.Or(expression.Name("username").GreaterThan(expression.Value(filter1Str)),
		expression.Name("email").LessThanEqual(expression.Value(filter2Str)))
	scannedItems, err := adc.ScanItems(nil, testDynamodbTableName, &filter, "")
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/ScanItems", err)
	}
	for _, si := range scannedItems {
		username := si["username"].(string)
		var m map[string]interface{} = si
		if !reflect.DeepEqual(m, itemsMap[username]) {
			t.Fatalf("%s failed: fetched\n%#v\noriginal\n%#v", testName+"/ScanItems", m, itemsMap[username])
		}
		email := si["email"].(string)

		if !(username > filter1Str || email <= filter2Str) {
			t.Fatalf("%s failed: [%s>%s] or [%s<=%s] is not true", testName+"/ScanItems", username, filter1Str, email, filter2Str)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbScanItems, nil, MetricsCatAll, MetricsCatDQL)
}

func TestAwsDynamodbConnect_ScanItemsWithCallback(t *testing.T) {
	testName := "TestAwsDynamodbConnect_ScanItemsWithCallback"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	numItems := 1024
	itemsMap := make(map[string]map[string]interface{})
	for i := 0; i < numItems; i++ {
		id := strconv.Itoa(i)
		item := map[string]interface{}{
			"username": id,
			"email":    id + "@domain.com",
			"testName": "Thanh " + id,
		}
		_, err = adc.PutItem(nil, testDynamodbTableName, item, nil)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", testName+"/PutItem", err)
		}
		itemsMap[id] = item
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, nil, MetricsCatAll, MetricsCatDML)
	}

	rand.Seed(time.Now().UnixNano())
	filter1 := rand.Intn(numItems / 5)
	filter1Str := strconv.Itoa(filter1)
	filter2 := numItems*4/5 + rand.Intn(numItems/5)
	filter2Str := strconv.Itoa(filter2) + "@domain.com"
	filter := expression.Or(expression.Name("username").GreaterThan(expression.Value(filter1Str)),
		expression.Name("email").LessThanEqual(expression.Value(filter2Str)))
	err = adc.ScanItemsWithCallback(nil, testDynamodbTableName, &filter, "", nil, func(si AwsDynamodbItem, lastEvaluatedKey map[string]*dynamodb.AttributeValue) (bool, error) {
		username := si["username"].(string)
		var m map[string]interface{} = si
		if !reflect.DeepEqual(m, itemsMap[username]) {
			t.Fatalf("%s failed: fetched\n%#v\noriginal\n%#v", testName+"/ScanItems", m, itemsMap[username])
		}
		email := si["email"].(string)

		if !(username > filter1Str || email <= filter2Str) {
			t.Fatalf("%s failed: [%s>%s] or [%s<=%s] is not true", testName+"/ScanItems", username, filter1Str, email, filter2Str)
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/ScanItemsWithCallback", err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbScanItems, nil, MetricsCatAll, MetricsCatDQL)
}

func TestAwsDynamodbConnect_QueryItems(t *testing.T) {
	testName := "TestAwsDynamodbConnect_QueryItems"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	numItems := 1024
	itemsMap := make(map[string]map[string]interface{})
	for i := 0; i < numItems; i++ {
		id := strconv.Itoa(i)
		email := id + "@domain.com"
		item := map[string]interface{}{
			"username": "btnguyen2k-" + strconv.Itoa(i%2),
			"email":    email,
			"testName": "Thanh " + id,
		}
		_, err = adc.PutItem(nil, testDynamodbTableName, item, nil)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", testName+"/PutItem", err)
		}
		itemsMap[email] = item
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, nil, MetricsCatAll, MetricsCatDML)
	}

	rand.Seed(time.Now().UnixNano())
	filter := numItems*4/5 + rand.Intn(numItems/5)
	filterStr := strconv.Itoa(filter) + "@domain.com"
	keyFilter := expression.And(expression.Name("username").Equal(expression.Value("btnguyen2k-0")),
		expression.Name("email").LessThan(expression.Value(filterStr)))
	queriesItems, err := adc.QueryItems(nil, testDynamodbTableName, &keyFilter, nil, "")
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/QueryItems", err)
	}
	for _, qi := range queriesItems {
		email := qi["email"].(string)
		var m map[string]interface{} = qi
		if !reflect.DeepEqual(m, itemsMap[email]) {
			t.Fatalf("%s failed: fetched\n%#v\noriginal\n%#v", testName+"/QueryItems", m, itemsMap[email])
		}
		username := qi["username"].(string)
		if !(username == "btnguyen2k-0" || email < filterStr) {
			t.Fatalf("%s failed: [%s==%s] or [%s<%s] is not true", testName+"/QueryItems", username, "btnguyen2k-0", email, filterStr)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbQueryItems, nil, MetricsCatAll, MetricsCatDQL)
}

func TestAwsDynamodbConnect_QueryItemsWithCallback(t *testing.T) {
	testName := "TestAwsDynamodbConnect_QueryItemsWithCallback"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	numItems := 1024
	itemsMap := make(map[string]map[string]interface{})
	for i := 0; i < numItems; i++ {
		id := strconv.Itoa(i)
		email := id + "@domain.com"
		item := map[string]interface{}{
			"username": "btnguyen2k-" + strconv.Itoa(i%2),
			"email":    email,
			"testName": "Thanh " + id,
		}
		_, err = adc.PutItem(nil, testDynamodbTableName, item, nil)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", testName+"/PutItem", err)
		}
		itemsMap[email] = item
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, nil, MetricsCatAll, MetricsCatDML)
	}

	rand.Seed(time.Now().UnixNano())
	filter := numItems*4/5 + rand.Intn(numItems/5)
	filterStr := strconv.Itoa(filter) + "@domain.com"
	keyFilter := expression.And(expression.Name("username").Equal(expression.Value("btnguyen2k-0")),
		expression.Name("email").LessThan(expression.Value(filterStr)))
	err = adc.QueryItemsWithCallback(nil, testDynamodbTableName, &keyFilter, nil, "", nil, func(qi AwsDynamodbItem, lastEvaluatedKey map[string]*dynamodb.AttributeValue) (bool, error) {
		email := qi["email"].(string)
		var m map[string]interface{} = qi
		if !reflect.DeepEqual(m, itemsMap[email]) {
			t.Fatalf("%s failed: fetched\n%#v\noriginal\n%#v", testName+"/ScanItems", m, itemsMap[email])
		}
		username := qi["username"].(string)
		if !(username == "btnguyen2k-0" || email < filterStr) {
			t.Fatalf("%s failed: [%s==%s] or [%s<%s] is not true", testName+"/QueryItemsWithCallback", username, "btnguyen2k-0", email, filterStr)
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/QueryItemsWithCallback", err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbQueryItems, nil, MetricsCatAll, MetricsCatDQL)
}

func TestAwsDynamodbConnect_QueryItems_Backward(t *testing.T) {
	testName := "TestAwsDynamodbConnect_QueryItems_Backward"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	numItems := 1024
	itemsMap := make(map[string]map[string]interface{})
	for i := 0; i < numItems; i++ {
		id := strconv.Itoa(i)
		email := id + "@domain.com"
		item := map[string]interface{}{
			"username": "btnguyen2k-" + strconv.Itoa(i%2),
			"email":    email,
			"testName": "Thanh " + id,
		}
		_, err = adc.PutItem(nil, testDynamodbTableName, item, nil)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", testName+"/PutItem", err)
		}
		itemsMap[email] = item
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, nil, MetricsCatAll, MetricsCatDML)
	}

	rand.Seed(time.Now().UnixNano())
	filter := numItems*4/5 + rand.Intn(numItems/5)
	filterStr := strconv.Itoa(filter) + "@domain.com"
	keyFilter := expression.And(expression.Name("username").Equal(expression.Value("btnguyen2k-0")),
		expression.Name("email").LessThan(expression.Value(filterStr)))
	queriesItems, err := adc.QueryItems(nil, testDynamodbTableName, &keyFilter, nil, "", AwsQueryOpt{ScanIndexBackward: aws.Bool(true)})
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/QueryItems", err)
	}
	for i, qi := range queriesItems {
		email := qi["email"].(string)
		var m map[string]interface{} = qi
		if !reflect.DeepEqual(m, itemsMap[email]) {
			t.Fatalf("%s failed: fetched\n%#v\noriginal\n%#v", testName+"/QueryItems", m, itemsMap[email])
		}
		username := qi["username"].(string)
		if !(username == "btnguyen2k-0" || email < filterStr) {
			t.Fatalf("%s failed: [%s==%s] or [%s<%s] is not true", testName+"/QueryItems", username, "btnguyen2k-0", email, filterStr)
		}

		if i > 0 {
			prev := queriesItems[i-1]
			pemail := prev["email"].(string)
			if !(pemail > email) {
				t.Fatalf("%s failed: out of order [%s vs %s]", testName+"/QueryItems", pemail, email)
			}
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbQueryItems, nil, MetricsCatAll, MetricsCatDQL)
}

func TestAwsDynamodbConnect_QueryItemsWithCallback_Backward(t *testing.T) {
	testName := "TestAwsDynamodbConnect_QueryItemsWithCallback_Backward"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	numItems := 1024
	itemsMap := make(map[string]map[string]interface{})
	for i := 0; i < numItems; i++ {
		id := strconv.Itoa(i)
		email := id + "@domain.com"
		item := map[string]interface{}{
			"username": "btnguyen2k-" + strconv.Itoa(i%2),
			"email":    email,
			"testName": "Thanh " + id,
		}
		_, err = adc.PutItem(nil, testDynamodbTableName, item, nil)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", testName+"/PutItem", err)
		}
		itemsMap[email] = item
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, nil, MetricsCatAll, MetricsCatDML)
	}

	rand.Seed(time.Now().UnixNano())
	filter := numItems*4/5 + rand.Intn(numItems/5)
	filterStr := strconv.Itoa(filter) + "@domain.com"
	keyFilter := expression.And(expression.Name("username").Equal(expression.Value("btnguyen2k-0")),
		expression.Name("email").LessThan(expression.Value(filterStr)))
	var prev AwsDynamodbItem = nil
	err = adc.QueryItemsWithCallback(nil, testDynamodbTableName, &keyFilter, nil, "", nil, func(qi AwsDynamodbItem, lastEvaluatedKey map[string]*dynamodb.AttributeValue) (bool, error) {
		email := qi["email"].(string)
		var m map[string]interface{} = qi
		if !reflect.DeepEqual(m, itemsMap[email]) {
			t.Fatalf("%s failed: fetched\n%#v\noriginal\n%#v", testName+"/QueryItems", m, itemsMap[email])
		}
		username := qi["username"].(string)
		if !(username == "btnguyen2k-0" || email < filterStr) {
			t.Fatalf("%s failed: [%s==%s] or [%s<%s] is not true", testName+"/QueryItems", username, "btnguyen2k-0", email, filterStr)
		}

		if prev != nil {
			pemail := prev["email"].(string)
			if !(pemail > email) {
				t.Fatalf("%s failed: out of order [%s vs %s]", testName+"/QueryItems", pemail, email)
			}
		}
		prev = qi
		return true, nil
	}, AwsQueryOpt{ScanIndexBackward: aws.Bool(true)})
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/QueryItemsWithCallback", err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbQueryItems, nil, MetricsCatAll, MetricsCatDQL)
}

func TestAwsDynamodbConnect_TxPut(t *testing.T) {
	testName := "TestAwsDynamodbConnect_TxPut"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	item := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"name":     "Thanh Nguyen",
	}
	tx, err := adc.BuildTxPut(testDynamodbTableName, item, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%#e]", testName, err)
	}
	if tx == nil {
		t.Fatalf("%s failed: nill", testName)
	}
	_, err = adc.WrapTxWriteItems(nil, "", tx)
	if err != nil {
		t.Fatalf("%s failed: error [%#e]", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactWriteItems, nil, MetricsCatAll, MetricsCatDML)
}

func TestAwsDynamodbConnect_TxGetPut(t *testing.T) {
	testName := "TestAwsDynamodbConnect_TxGetPut"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	item1 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  float64(time.Now().Unix()),
		"active":   true,
	}
	item2 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  nil,
		"active":   false,
		"name":     "Thanh Nguyen",
	}
	item3 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  float64(123),
		"name":     "Tom",
	}
	items := []bson.M{item1, item2, item3}

	var txGet *dynamodb.TransactGetItem
	var txPut *dynamodb.TransactWriteItem
	var fetchedItems []AwsDynamodbItem
	keyFilter := bson.M{"username": "btnguyen2k", "email": "me@domain.com"}

	// GetItem: must be "not found"
	if txGet, err = adc.BuildTxGet(testDynamodbTableName, keyFilter); txGet == nil || err != nil {
		t.Fatalf("%s failed: nil result or error [%s]", testName+"/BuildTxGet", err)
	}
	if fetchedItems, err = adc.WrapTxGetItems(nil, txGet); fetchedItems == nil || err != nil {
		t.Fatalf("%s failed: nil result or error [%s]", testName+"/WrapTxGetItems", err)
	} else if len(fetchedItems) != 0 {
		t.Fatalf("%s failed: item should not exist", testName+"/WrapTxGetItems")
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactGetItems, nil, MetricsCatAll, MetricsCatDQL)

	for i, item := range items {
		// PutItem: must be successful (new item or overriding existing one)
		if txPut, err = adc.BuildTxPut(testDynamodbTableName, item, nil); txPut == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/BuildTxPut/"+strconv.Itoa(i), err)
		}
		if _, err = adc.WrapTxWriteItems(nil, "", txPut); err != nil {
			t.Fatalf("%s failed: [%s]", testName+"/WrapTxWriteItems/"+strconv.Itoa(i), err)
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactWriteItems, nil, MetricsCatAll, MetricsCatDML)

		// GetItem: must match the original one
		if fetchedItems, err = adc.WrapTxGetItems(nil, txGet); len(fetchedItems) == 0 || err != nil {
			t.Fatalf("%s failed: empty result or error [%s]", testName+"/WrapTxGetItems/"+strconv.Itoa(i), err)
		} else {
			var m = bson.M(fetchedItems[0])
			if !reflect.DeepEqual(m, item) {
				t.Fatalf("%s failed: fetched\n%#v\nvs original\n%#v", testName+"/WrapTxGetItems/"+strconv.Itoa(i), m, item)
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactGetItems, nil, MetricsCatAll, MetricsCatDQL)
	}
}

func TestAwsDynamodbConnect_TxPutIfNotExist(t *testing.T) {
	testName := "TestAwsDynamodbConnect_TxPutIfNotExist"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/prepareAwsDynamodbTable", err)
	}

	item1 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"name":     "Thanh Nguyen",
	}
	item2 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  float64(time.Now().Unix()),
		"active":   false,
		"name":     "Thanh Nguyen",
	}
	item3 := bson.M{
		"username": "thanhn",
		"email":    "me@domain.com",
		"version":  nil,
		"name":     "Tom",
	}
	testData := []struct {
		item, expected, filter bson.M
		shouldWrite            bool
	}{
		{item1, item1, bson.M{"username": "btnguyen2k", "email": "me@domain.com"}, true},
		{item2, item1, bson.M{"username": "btnguyen2k", "email": "me@domain.com"}, false},
		{item3, item3, bson.M{"username": "thanhn", "email": "me@domain.com"}, true},
	}

	var txGet *dynamodb.TransactGetItem
	var txPut *dynamodb.TransactWriteItem
	var fetchedItems []AwsDynamodbItem
	pkAttrs := []string{"username"}
	for i, data := range testData {
		item := data.item
		// PutItem: must be successful for new item
		if txPut, err = adc.BuildTxPutIfNotExist(testDynamodbTableName, item, pkAttrs); txPut == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/BuildTxPutIfNotExist/"+strconv.Itoa(i), err)
		}
		if _, err = adc.WrapTxWriteItems(nil, "", txPut); err != nil {
			if data.shouldWrite || AwsIgnoreTransactErrorIfMatched(err, "ConditionalCheckFailed") != nil {
				t.Fatalf("%s failed: [%s]", testName+"/WrapTxWriteItems/"+strconv.Itoa(i), err)
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactWriteItems, []string{dynamodb.ErrCodeTransactionCanceledException}, MetricsCatAll, MetricsCatDML)

		expected := data.expected
		filter := data.filter
		// GetItem: must match the expected one
		if txGet, err = adc.BuildTxGet(testDynamodbTableName, filter); txGet == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/BuildTxGet", err)
		}
		if fetchedItems, err = adc.WrapTxGetItems(nil, txGet); len(fetchedItems) == 0 || err != nil {
			t.Fatalf("%s failed: empty result or error [%s]", testName+"/WrapTxGetItems/"+strconv.Itoa(i), err)
		} else {
			var m = bson.M(fetchedItems[0])
			if !reflect.DeepEqual(m, expected) {
				t.Fatalf("%s failed: fetched\n%#v\nvs original\n%#v", testName+"/WrapTxGetItems/"+strconv.Itoa(i), m, item)
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactGetItems, nil, MetricsCatAll, MetricsCatDQL)
	}
}

func TestAwsDynamodbConnect_TxDelete(t *testing.T) {
	testName := "TestAwsDynamodbConnect_TxDelete"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	item1 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  float64(time.Now().Unix()),
		"active":   true,
	}
	item2 := bson.M{
		"username": "thanhn",
		"email":    "me@domain.com",
		"version":  nil,
		"active":   false,
		"name":     "Thanh Nguyen",
	}
	testData := []struct {
		item, keyFilter bson.M
		condition       *expression.ConditionBuilder
		shouldDelete    bool
	}{
		{item1, bson.M{"username": "btnguyen2k", "email": "me@domain.com"}, nil, true},
		{item2, bson.M{"username": "thanhn", "email": "me@domain.com"}, AwsDynamodbExistsAllBuilder([]string{"notexists"}), false},
	}
	var txDelete *dynamodb.TransactWriteItem
	var txGet *dynamodb.TransactGetItem
	var fetchedItems []AwsDynamodbItem
	for i, data := range testData {
		// firstly: put item to the table
		_, err = adc.PutItem(nil, testDynamodbTableName, data.item, nil)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", testName+"/PutItem/"+strconv.Itoa(i), err)
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, nil, MetricsCatAll, MetricsCatDML)

		// secondly: delete it
		keyFilter := data.keyFilter
		condition := data.condition
		if txDelete, err = adc.BuildTxDelete(testDynamodbTableName, keyFilter, condition); txDelete == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/BuildTxDelete/"+strconv.Itoa(i), err)
		}
		if condition == nil && txDelete.Delete.ConditionExpression != nil {
			t.Fatalf("%s failed: transaction's ConditionExpression must be nil", testName+"/BuildTxDelete/"+strconv.Itoa(i))
		}
		if _, err = adc.WrapTxWriteItems(nil, "", txDelete); err != nil {
			if data.shouldDelete || AwsIgnoreTransactErrorIfMatched(err, "ConditionalCheckFailed") != nil {
				t.Fatalf("%s failed: [%s]", testName+"/WrapTxWriteItems/"+strconv.Itoa(i), err)
			}
		}
		var ignoreErrorCodes []string
		if !data.shouldDelete {
			ignoreErrorCodes = []string{dynamodb.ErrCodeTransactionCanceledException}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactWriteItems, ignoreErrorCodes, MetricsCatAll, MetricsCatDML)

		// lastly: fetch it back
		if txGet, err = adc.BuildTxGet(testDynamodbTableName, keyFilter); txGet == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/BuildTxGet/"+strconv.Itoa(i), err)
		}
		if fetchedItems, err = adc.WrapTxGetItems(nil, txGet); err != nil {
			t.Fatalf("%s failed: error [%s]", testName+"/WrapTxGetItems/"+strconv.Itoa(i), err)
		}
		if data.shouldDelete {
			if len(fetchedItems) != 0 {
				// item must be deleted
				t.Fatalf("%s failed: item must be deleted", testName+"/WrapTxGetItems/"+strconv.Itoa(i))
			}
		} else {
			if len(fetchedItems) != 1 {
				t.Fatalf("%s failed: {shouldDelete: %#v / num items: %#v}", testName+"/WrapTxGetItems/"+strconv.Itoa(i), data.shouldDelete, len(fetchedItems))
			} else {
				var m = bson.M(fetchedItems[0])
				if !reflect.DeepEqual(m, data.item) {
					t.Fatalf("%s failed: fetched\n%#v\nvs original\n%#v", testName+"/WrapTxGetItems/"+strconv.Itoa(i), m, data.item)
				}
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactGetItems, nil, MetricsCatAll, MetricsCatDQL)
	}
}

func TestAwsDynamodbConnect_TxRemoveAttributes(t *testing.T) {
	testName := "TestAwsDynamodbConnect_TxRemoveAttributes"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	item0 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  float64(time.Now().Unix()),
		"active":   true,
	}
	item1 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
	}
	keyFilter := bson.M{"username": "btnguyen2k", "email": "me@domain.com"}
	testData := []struct {
		orgItem, newItem bson.M
		fieldsToRemove   []string
		condition        *expression.ConditionBuilder
		shouldUpdate     bool
	}{
		{item0, item1, []string{"version", "active"}, nil, true},
		{item0, item0, []string{"version"}, AwsDynamodbExistsAllBuilder([]string{"notexists"}), false},
	}
	var txRemoveAttrs *dynamodb.TransactWriteItem
	var txGet *dynamodb.TransactGetItem
	var fetchedItems []AwsDynamodbItem
	for i, data := range testData {
		// firstly: put item to the table
		_, err = adc.PutItem(nil, testDynamodbTableName, data.orgItem, nil)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", testName+"/PutItem/"+strconv.Itoa(i), err)
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, nil, MetricsCatAll, MetricsCatDML)

		// secondly: update it
		condition := data.condition
		if txRemoveAttrs, err = adc.BuildTxRemoveAttributes(testDynamodbTableName, keyFilter, data.condition, data.fieldsToRemove); txRemoveAttrs == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/BuildTxRemoveAttributes/"+strconv.Itoa(i), err)
		}
		if condition == nil && txRemoveAttrs.Update.ConditionExpression != nil {
			t.Fatalf("%s failed: transaction's ConditionExpression must be nil", testName+"/BuildTxRemoveAttributes/"+strconv.Itoa(i))
		}
		if _, err = adc.WrapTxWriteItems(nil, "", txRemoveAttrs); err != nil {
			if data.shouldUpdate || AwsIgnoreTransactErrorIfMatched(err, "ConditionalCheckFailed") != nil {
				t.Fatalf("%s failed: [%s]", testName+"/WrapTxWriteItems/"+strconv.Itoa(i), err)
			}
		}
		var ignoreErrorCodes []string
		if !data.shouldUpdate {
			ignoreErrorCodes = []string{dynamodb.ErrCodeTransactionCanceledException}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactWriteItems, ignoreErrorCodes, MetricsCatAll, MetricsCatDML)

		// lastly: fetch it back
		if txGet, err = adc.BuildTxGet(testDynamodbTableName, keyFilter); txGet == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/BuildTxGet/"+strconv.Itoa(i), err)
		}
		if fetchedItems, err = adc.WrapTxGetItems(nil, txGet); err != nil {
			t.Fatalf("%s failed: error [%s]", testName+"/WrapTxGetItems/"+strconv.Itoa(i), err)
		}
		if len(fetchedItems) != 1 {
			t.Fatalf("%s failed: {num items: %#v}", testName+"/WrapTxGetItems/"+strconv.Itoa(i), len(fetchedItems))
		} else {
			var m = bson.M(fetchedItems[0])
			if !reflect.DeepEqual(m, data.newItem) {
				t.Fatalf("%s failed: fetched\n%#v\nvs original\n%#v", testName+"/WrapTxGetItems/"+strconv.Itoa(i), m, data.newItem)
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactGetItems, nil, MetricsCatAll, MetricsCatDQL)
	}
}

func TestAwsDynamodbConnect_TxSetAttributes(t *testing.T) {
	testName := "TestAwsDynamodbConnect_TxSetAttributes"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	item0 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  float64(time.Now().Unix()),
		"active":   true,
	}
	newFieldsAndValues := bson.M{
		"name":    "Thanh Nguyen",
		"active":  false,
		"version": nil,
	}
	item1 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"active":   false,
		"version":  nil,
		"name":     "Thanh Nguyen",
	}
	keyFilter := map[string]interface{}{"username": "btnguyen2k", "email": "me@domain.com"}
	testData := []struct {
		orgItem, newItem, newFieldsAndValues bson.M
		condition                            *expression.ConditionBuilder
		shouldUpdate                         bool
	}{
		{item0, item1, newFieldsAndValues, nil, true},
		{item0, item0, newFieldsAndValues, AwsDynamodbExistsAllBuilder([]string{"notexists"}), false},
	}
	var txSetAttrs *dynamodb.TransactWriteItem
	var txGet *dynamodb.TransactGetItem
	var fetchedItems []AwsDynamodbItem
	for i, data := range testData {
		// firstly: put item to the table
		_, err = adc.PutItem(nil, testDynamodbTableName, data.orgItem, nil)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", testName+"/PutItem/"+strconv.Itoa(i), err)
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, nil, MetricsCatAll, MetricsCatDML)

		// secondly: update it
		condition := data.condition
		if txSetAttrs, err = adc.BuildTxSetAttributes(testDynamodbTableName, keyFilter, data.condition, data.newFieldsAndValues); txSetAttrs == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/BuildTxSetAttributes/"+strconv.Itoa(i), err)
		}
		if condition == nil && txSetAttrs.Update.ConditionExpression != nil {
			t.Fatalf("%s failed: transaction's ConditionExpression must be nil", testName+"/BuildTxSetAttributes/"+strconv.Itoa(i))
		}
		if _, err = adc.WrapTxWriteItems(nil, "", txSetAttrs); err != nil {
			if data.shouldUpdate || AwsIgnoreTransactErrorIfMatched(err, "ConditionalCheckFailed") != nil {
				t.Fatalf("%s failed: [%s]", testName+"/WrapTxWriteItems/"+strconv.Itoa(i), err)
			}
		}
		var ignoreErrorCodes []string
		if !data.shouldUpdate {
			ignoreErrorCodes = []string{dynamodb.ErrCodeTransactionCanceledException}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactWriteItems, ignoreErrorCodes, MetricsCatAll, MetricsCatDML)

		// lastly: fetch it back
		if txGet, err = adc.BuildTxGet(testDynamodbTableName, keyFilter); txGet == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/BuildTxGet/"+strconv.Itoa(i), err)
		}
		if fetchedItems, err = adc.WrapTxGetItems(nil, txGet); err != nil {
			t.Fatalf("%s failed: error [%s]", testName+"/WrapTxGetItems/"+strconv.Itoa(i), err)
		}
		if len(fetchedItems) != 1 {
			t.Fatalf("%s failed: {num items: %#v}", testName+"/WrapTxGetItems/"+strconv.Itoa(i), len(fetchedItems))
		} else {
			var m = bson.M(fetchedItems[0])
			if !reflect.DeepEqual(m, data.newItem) {
				t.Fatalf("%s failed: fetched\n%#v\nvs original\n%#v", testName+"/WrapTxGetItems/"+strconv.Itoa(i), m, data.newItem)
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactGetItems, nil, MetricsCatAll, MetricsCatDQL)
	}
}

func TestAwsDynamodbConnect_TxAddValuesToAttributes(t *testing.T) {
	testName := "TestAwsDynamodbConnect_TxAddValuesToAttributes"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	item0 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"b":        true,
		"s":        "a string",
		"n":        1.0,
		"m":        map[string]interface{}{"b": 0.0, "n": 2.0, "s": "a string"},
		"a":        []interface{}{0.0, 1.0, "a string"},
		"an":       []interface{}{1.0, 2.0, 3.0},
		"as":       []interface{}{"1", "2", "3"},
	}
	attrsAndValuesToAdd := bson.M{
		"a[1]":  1.1,   // a[1]'s value is added by 1.1 --> new value 2.1
		"a[10]": 12.34, // new value 12.34 is appended to array a
		"m.n":   1.2,   // m.n's value is added by 1.2 --> new value 3.2
		"m.new": 3.0,   // m.new does not exist, its value is assumed zero, hence new key m.new is created with value 3.0
		"n0":    1.2,   // n0 does not exist, its value is assumed zero, hence new attribute n0 is created with value 1.2
		"n":     2.3,   // n's value is added by 2.3 --> new value 3.3
	}
	item1 := bson.M{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"b":        true,
		"s":        "a string",
		"n":        1.0 + 2.3,
		"m":        map[string]interface{}{"b": 0.0, "n": 2.0 + 1.2, "s": "a string", "new": 3.0},
		"a":        []interface{}{0.0, 1.0 + 1.1, "a string", 12.34},
		"an":       []interface{}{1.0, 2.0, 3.0},
		"as":       []interface{}{"1", "2", "3"},
		"n0":       1.2,
	}
	keyFilter := bson.M{"username": "btnguyen2k", "email": "me@domain.com"}
	testData := []struct {
		orgItem, newItem, attrsAndValuesToAdd bson.M
		condition                             *expression.ConditionBuilder
		shouldUpdate                          bool
	}{
		{item0, item1, attrsAndValuesToAdd, nil, true},
		{item0, item0, attrsAndValuesToAdd, AwsDynamodbExistsAllBuilder([]string{"notexists"}), false},
	}
	var txAddValuesToAttrs *dynamodb.TransactWriteItem
	var txGet *dynamodb.TransactGetItem
	var fetchedItems []AwsDynamodbItem
	for i, data := range testData {
		// firstly: put item to the table
		_, err = adc.PutItem(nil, testDynamodbTableName, data.orgItem, nil)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", testName+"/PutItem/"+strconv.Itoa(i), err)
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, nil, MetricsCatAll, MetricsCatDML)

		// secondly: update it
		condition := data.condition
		if txAddValuesToAttrs, err = adc.BuildTxAddValuesToAttributes(testDynamodbTableName, keyFilter, data.condition, data.attrsAndValuesToAdd); txAddValuesToAttrs == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/BuildTxAddValuesToAttributes/"+strconv.Itoa(i), err)
		}
		if condition == nil && txAddValuesToAttrs.Update.ConditionExpression != nil {
			t.Fatalf("%s failed: transaction's ConditionExpression must be nil", testName+"/BuildTxAddValuesToAttributes/"+strconv.Itoa(i))
		}
		if _, err = adc.WrapTxWriteItems(nil, "", txAddValuesToAttrs); err != nil {
			if data.shouldUpdate || AwsIgnoreTransactErrorIfMatched(err, "ConditionalCheckFailed") != nil {
				t.Fatalf("%s failed: [%s]", testName+"/WrapTxWriteItems/"+strconv.Itoa(i), err)
			}
		}
		var ignoreErrorCodes []string
		if !data.shouldUpdate {
			ignoreErrorCodes = []string{dynamodb.ErrCodeTransactionCanceledException}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactWriteItems, ignoreErrorCodes, MetricsCatAll, MetricsCatDML)

		// lastly: fetch it back
		if txGet, err = adc.BuildTxGet(testDynamodbTableName, keyFilter); txGet == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/BuildTxGet/"+strconv.Itoa(i), err)
		}
		if fetchedItems, err = adc.WrapTxGetItems(nil, txGet); err != nil {
			t.Fatalf("%s failed: error [%s]", testName+"/WrapTxGetItems/"+strconv.Itoa(i), err)
		}
		if len(fetchedItems) != 1 {
			t.Fatalf("%s failed: {num items: %#v}", testName+"/WrapTxGetItems/"+strconv.Itoa(i), len(fetchedItems))
		} else {
			var m = bson.M(fetchedItems[0])
			if !reflect.DeepEqual(m, data.newItem) {
				t.Fatalf("%s failed: fetched\n%#v\nvs original\n%#v", testName+"/WrapTxGetItems/"+strconv.Itoa(i), m, data.newItem)
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactGetItems, nil, MetricsCatAll, MetricsCatDQL)
	}
}

func TestAwsDynamodbConnect_TxAddValuesToSet(t *testing.T) {
	testName := "TestAwsDynamodbConnect_TxAddValuesToSet"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	item0Raw := map[string]*dynamodb.AttributeValue{
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
	item0 := bson.M{
		"username": "btnguyen2k",
		"an":       []float64{1.0, 2.0, 3.0},
		"as":       []string{"1", "2", "3"},
		"a":        []interface{}{},
		"email":    "me@domain.com",
		"m":        map[string]interface{}{},
	}
	attrsAndValuesToAdd1 := bson.M{"an": 8, "as": []string{"9", "10"}}
	item1 := bson.M{
		"username": "btnguyen2k",
		"an":       []float64{1.0, 2.0, 3.0, 8.0},
		"as":       []string{"1", "10", "2", "3", "9"},
		"a":        []interface{}{},
		"email":    "me@domain.com",
		"m":        map[string]interface{}{},
	}
	attrsAndValuesToAdd2 := bson.M{
		"an": &dynamodb.AttributeValue{NS: []*string{aws.String("7.0")}},
		"as": &dynamodb.AttributeValue{SS: []*string{aws.String("7"), aws.String("8")}},
	}
	item2 := bson.M{
		"username": "btnguyen2k",
		"an":       []float64{1.0, 2.0, 3.0, 7.0},
		"as":       []string{"1", "2", "3", "7", "8"},
		"a":        []interface{}{},
		"email":    "me@domain.com",
		"m":        map[string]interface{}{},
	}
	testData := []struct {
		orgItem                      map[string]*dynamodb.AttributeValue
		attrsAndValuesToAdd, newItem bson.M
		condition                    *expression.ConditionBuilder
		shouldUpdate                 bool
	}{
		{item0Raw, attrsAndValuesToAdd1, item1, nil, true},
		{item0Raw, attrsAndValuesToAdd2, item2, nil, true},
		{item0Raw, attrsAndValuesToAdd1, item0, AwsDynamodbExistsAllBuilder([]string{"notexists"}), false},
	}
	keyFilter := bson.M{"username": "btnguyen2k", "email": "me@domain.com"}
	var txAddValuesToAdd *dynamodb.TransactWriteItem
	var txGet *dynamodb.TransactGetItem
	var fetchedItems []AwsDynamodbItem
	for i, data := range testData {
		// firstly: put item to the table
		_, err = adc.PutItemRaw(nil, testDynamodbTableName, data.orgItem, nil)
		if err != nil {
			t.Fatalf("%s failed: error %s", testName+"/PutItem/"+strconv.Itoa(i), err)
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, nil, MetricsCatAll, MetricsCatDML)

		// secondly: update it
		condition := data.condition
		if txAddValuesToAdd, err = adc.BuildTxAddValuesToSet(testDynamodbTableName, keyFilter, data.condition, data.attrsAndValuesToAdd); txAddValuesToAdd == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/BuildTxAddValuesToSet/"+strconv.Itoa(i), err)
		}
		if condition == nil && txAddValuesToAdd.Update.ConditionExpression != nil {
			t.Fatalf("%s failed: transaction's ConditionExpression must be nil", testName+"/BuildTxAddValuesToSet/"+strconv.Itoa(i))
		}
		if _, err = adc.WrapTxWriteItems(nil, "", txAddValuesToAdd); err != nil {
			if data.shouldUpdate || AwsIgnoreTransactErrorIfMatched(err, "ConditionalCheckFailed") != nil {
				t.Fatalf("%s failed: [%s]", testName+"/WrapTxWriteItems/"+strconv.Itoa(i), err)
			}
		}
		var ignoreErrorCodes []string
		if !data.shouldUpdate {
			ignoreErrorCodes = []string{dynamodb.ErrCodeTransactionCanceledException}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactWriteItems, ignoreErrorCodes, MetricsCatAll, MetricsCatDML)

		// lastly: fetch it back
		if txGet, err = adc.BuildTxGet(testDynamodbTableName, keyFilter); txGet == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/BuildTxGet/"+strconv.Itoa(i), err)
		}
		if fetchedItems, err = adc.WrapTxGetItems(nil, txGet); err != nil {
			t.Fatalf("%s failed: error [%s]", testName+"/WrapTxGetItems/"+strconv.Itoa(i), err)
		}
		if len(fetchedItems) != 1 {
			t.Fatalf("%s failed: {num items: %#v}", testName+"/WrapTxGetItems/"+strconv.Itoa(i), len(fetchedItems))
		} else {
			var m = bson.M(fetchedItems[0])
			if !reflect.DeepEqual(m, data.newItem) {
				t.Fatalf("%s failed: fetched\n%#v\nvs original\n%#v", testName+"/WrapTxGetItems/"+strconv.Itoa(i), m, data.newItem)
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactGetItems, nil, MetricsCatAll, MetricsCatDQL)
	}
}

func TestAwsDynamodbConnect_TxDeleteValuesFromSet(t *testing.T) {
	testName := "TestAwsDynamodbConnect_TxDeleteValuesFromSet"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()

	if os.Getenv(dynamodbTestTableName) != "" {
		testDynamodbTableName = os.Getenv(dynamodbTestTableName)
	}
	err := prepareAwsDynamodbTable(adc, testDynamodbTableName)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName+"/prepareAwsDynamodbTable", err)
	}

	item0Raw := map[string]*dynamodb.AttributeValue{
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
	item0 := bson.M{
		"username": "btnguyen2k",
		"an":       []float64{1.0, 2.0, 3.0},
		"as":       []string{"1", "2", "3"},
		"a":        []interface{}{},
		"email":    "me@domain.com",
		"m":        map[string]interface{}{},
	}
	attrsAndValuesToDelete1 := bson.M{"an": 1, "as": []string{"1", "3", "5"}}
	item1 := bson.M{
		"username": "btnguyen2k",
		"an":       []float64{2.0, 3.0},
		"as":       []string{"2"},
		"a":        []interface{}{},
		"email":    "me@domain.com",
		"m":        map[string]interface{}{},
	}
	attrsAndValuesToDelete2 := bson.M{
		"an": &dynamodb.AttributeValue{NS: []*string{aws.String("3.0")}},
		"as": &dynamodb.AttributeValue{SS: []*string{aws.String("2"), aws.String("3")}},
	}
	item2 := bson.M{
		"username": "btnguyen2k",
		"an":       []float64{1.0, 2.0},
		"as":       []string{"1"},
		"a":        []interface{}{},
		"email":    "me@domain.com",
		"m":        map[string]interface{}{},
	}
	testData := []struct {
		orgItem                         map[string]*dynamodb.AttributeValue
		attrsAndValuesToDelete, newItem bson.M
		condition                       *expression.ConditionBuilder
		shouldUpdate                    bool
	}{
		{item0Raw, attrsAndValuesToDelete1, item1, nil, true},
		{item0Raw, attrsAndValuesToDelete2, item2, nil, true},
		{item0Raw, attrsAndValuesToDelete1, item0, AwsDynamodbExistsAllBuilder([]string{"notexists"}), false},
	}
	keyFilter := bson.M{"username": "btnguyen2k", "email": "me@domain.com"}
	var txAddValuesToDelete *dynamodb.TransactWriteItem
	var txGet *dynamodb.TransactGetItem
	var fetchedItems []AwsDynamodbItem
	for i, data := range testData {
		// firstly: put item to the table
		_, err = adc.PutItemRaw(nil, testDynamodbTableName, data.orgItem, nil)
		if err != nil {
			t.Fatalf("%s failed: error %s", testName+"/PutItem/"+strconv.Itoa(i), err)
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, nil, MetricsCatAll, MetricsCatDML)

		// secondly: update it
		condition := data.condition
		if txAddValuesToDelete, err = adc.BuildTxDeleteValuesFromSet(testDynamodbTableName, keyFilter, data.condition, data.attrsAndValuesToDelete); txAddValuesToDelete == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/BuildTxDeleteValuesFromSet/"+strconv.Itoa(i), err)
		}
		if condition == nil && txAddValuesToDelete.Update.ConditionExpression != nil {
			t.Fatalf("%s failed: transaction's ConditionExpression must be nil", testName+"/BuildTxDeleteValuesFromSet/"+strconv.Itoa(i))
		}
		if _, err = adc.WrapTxWriteItems(nil, "", txAddValuesToDelete); err != nil {
			if data.shouldUpdate || AwsIgnoreTransactErrorIfMatched(err, "ConditionalCheckFailed") != nil {
				t.Fatalf("%s failed: [%s]", testName+"/WrapTxWriteItems/"+strconv.Itoa(i), err)
			}
		}
		var ignoreErrorCodes []string
		if !data.shouldUpdate {
			ignoreErrorCodes = []string{dynamodb.ErrCodeTransactionCanceledException}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactWriteItems, ignoreErrorCodes, MetricsCatAll, MetricsCatDML)

		// lastly: fetch it back
		if txGet, err = adc.BuildTxGet(testDynamodbTableName, keyFilter); txGet == nil || err != nil {
			t.Fatalf("%s failed: nil result or error [%s]", testName+"/BuildTxGet/"+strconv.Itoa(i), err)
		}
		if fetchedItems, err = adc.WrapTxGetItems(nil, txGet); err != nil {
			t.Fatalf("%s failed: error [%s]", testName+"/WrapTxGetItems/"+strconv.Itoa(i), err)
		}
		if len(fetchedItems) != 1 {
			t.Fatalf("%s failed: {num items: %#v}", testName+"/WrapTxGetItems/"+strconv.Itoa(i), len(fetchedItems))
		} else {
			var m = bson.M(fetchedItems[0])
			if !reflect.DeepEqual(m, data.newItem) {
				t.Fatalf("%s failed: fetched\n%#v\nvs original\n%#v", testName+"/WrapTxGetItems/"+strconv.Itoa(i), m, data.newItem)
			}
		}
		_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactGetItems, nil, MetricsCatAll, MetricsCatDQL)
	}
}
