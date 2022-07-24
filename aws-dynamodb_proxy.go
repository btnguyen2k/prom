package prom

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	cmdDynamodbBatchExecStm       = "BatchExecuteStatement" // (internal) command name for "batch execute statement" operation
	cmdDynamodbBatchGetItem       = "BatchGetItem"          // (internal) command name for "batch get item" operation
	cmdDynamodbBatchWriteItem     = "BatchWriteItem"        // (internal) command name for "batch write item" operation
	cmdDynamodbExecuteStatement   = "ExecuteStatement"      // (internal) command for "execute statement" operation
	cmdDynamodbExecuteTransaction = "ExecuteTransaction"    //  (internal) command for "execute transaction" operation

	cmdDynamodbCreateBackup            = "CreateBackup"              // (internal) command name for "create backup" operation
	cmdDynamodbDescBackup              = "DescribeBackup"            // (internal) command name for "describe backup" operation
	cmdDynamodbDescContinuousBackups   = "DescribeContinuousBackups" // (internal) command name for "describe continuous backups" operation
	cmdDynamodbUpdateContinuousBackups = "UpdateContinuousBackups"   // (internal) command name for "update continuous backups" operation
	cmdDynamodbListBackups             = "ListBackups"               // (internal) command name for "list backups" operation
	cmdDynamodbDeleteBackup            = "DeleteBackup"              // (internal) command name for "delete backup" operation
	cmdDynamodbRestoreTableFromBackup  = "RestoreTableFromBackup"    // (internal) command name for "restore table from backup" operation
	cmdDynamodbRestoreTableToPIT       = "RestoreTableToPointInTime" // (internal) command name for "restore table to point in time" operation

	cmdDynamodbCreateGlobalTable         = "CreateGlobalTable"           // (internal) command name for "create global table" operation
	cmdDynamodbDescGlobalTable           = "DescribeGlobalTable"         // (internal) command name for "describe global table" operation
	cmdDynamodbDescGlobalTableSettings   = "DescribeGlobalTableSettings" // (internal) command name for "describe global table settings" operation
	cmdDynamodbListGlobalTables          = "ListGlobalTables"            // (internal) command name for "list global tables" operation
	cmdDynamodbUpdateGlobalTable         = "UpdateGlobalTable"           // (internal) command name for "update global table" operation
	cmdDynamodbUpdateGlobalTableSettings = "UpdateGlobalTableSettings"   // (internal) command name for "update global table settings" operation

	cmdDynamodbCreateTable                   = "CreateTable"                     // (internal) command name for "create table" operation
	cmdDynamodbDescTable                     = "DescribeTable"                   // (internal) command name for "describe table" operation
	cmdDynamodbDescTableReplicaAutoScaling   = "DescribeTableReplicaAutoScaling" // (internal) command name for "describe table replica auto scaling" operation
	cmdDynamodbUpdateTableReplicaAutoScaling = "UpdateTableReplicaAutoScaling"   // (internal) command name for "update table replica auto scaling" operation
	cmdDynamodbListTables                    = "ListTables"                      // (internal) command name for "list tables" operation
	cmdDynamodbUpdateTable                   = "UpdateTable"                     // (internal) command name for "update table" operation
	cmdDynamodbDeleteTable                   = "DeleteTable"                     // (internal) command name for "delete table" operation
	cmdDynamodbExportTableToPIT              = "ExportTableToPointInTime"        // (internal) command name for "export table to point in time" operation

	cmdDynamodbPutItem            = "PutItem"            // (internal) command name for "put item" operation
	cmdDynamodbGetItem            = "GetItem"            // (internal) command name for "get item" operation
	cmdDynamodbQueryItems         = "Query"              // (internal) command name for "query items" operation
	cmdDynamodbScanItems          = "Scan"               // (internal) command name for "scan items" operation
	cmdDynamodbUpdateItem         = "UpdateItem"         // (internal) command name for "update item" operation
	cmdDynamodbDeleteItem         = "DeleteItem"         // (internal) command name for "delete item" operation
	cmdDynamodbTransactGetItems   = "TransactGetItems"   // (internal) command name for "transact get items" operation
	cmdDynamodbTransactWriteItems = "TransactWriteItems" // (internal) command name for "transact write items" operation

	cmdDynamodbDescKinesisStreamingDestination    = "DescribeKinesisStreamingDestination" // (internal) command name for "describe kinesis streaming destination" operation
	cmdDynamodbEnableKinesisStreamingDestination  = "EnableKinesisStreamingDestination"   // (internal) command for "enable kinesis streaming destination" operation
	cmdDynamodbDisableKinesisStreamingDestination = "DisableKinesisStreamingDestination"  // (internal) command name for "disable kinesis streaming destination" operation

	cmdDynamodbDescLimits = "DescribeLimits"     // (internal) command name for "describe limits" operation
	cmdDynamodbDescTTL    = "DescribeTimeToLive" // (internal) command name for "describe time to live" operation
	cmdDynamodbUpdateTTL  = "UpdateTimeToLive"   // (internal) command name for "update time to live" operation

	cmdDynamodbDescContributorInsights   = "DescribeContributorInsights" // (internal) command name for "describe contributor insights" operation
	cmdDynamodbListContributorInsights   = "ListContributorInsights"     // (internal) command name for "list contributor insights" operation
	cmdDynamodbUpdateContributorInsights = "UpdateContributorInsights"   // (internal) command name for "update contributor insights" operation

	cmdDynamodbTagResource   = "TagResource"   // (internal) command name for "tag resource" operation
	cmdDynamodbUntagResource = "UntagResource" // (internal) command name for "untag resource" operation

	cmdDynamodbDescEndpoints = "DescribeEndpoints" // (internal) command name for "describe endpoints" operation

	cmdDynamodbDescExport         = "DescribeExport"     // (internal) command name for "describe export" operation
	cmdDynamodbListExports        = "ListExports"        // (internal) command name for "list exports" operation
	cmdDynamodbListTagsOfResource = "ListTagsOfResource" // (internal) command for "list tags of resource" operation
)

func dynamodbCombineConsumedCapacity(input []*dynamodb.ConsumedCapacity) float64 {
	result := 0.0
	if input != nil {
		for _, c := range input {
			if c != nil && c.CapacityUnits != nil {
				result += *c.CapacityUnits
			}
		}
	}
	return result
}

// DynamoDbProxy is a proxy that can be used as replacement for dynamodb.DynamoDB.
//
// This proxy overrides some functions from dynamodb.DynamoDB and automatically logs the execution metrics.
//
// Available since v0.3.0
type DynamoDbProxy struct {
	*dynamodb.DynamoDB
	adc *AwsDynamodbConnect
}

// BatchExecuteStatement overrides dynamodb.DynamoDB/BatchExecuteStatement to log execution metrics.
func (dp *DynamoDbProxy) BatchExecuteStatement(input *dynamodb.BatchExecuteStatementInput) (*dynamodb.BatchExecuteStatementOutput, error) {
	return dp.metricsBatchExecuteStatement(true, nil, input)
}

// BatchExecuteStatementWithContext overrides dynamodb.DynamoDB/BatchExecuteStatementWithContext to log execution metrics.
func (dp *DynamoDbProxy) BatchExecuteStatementWithContext(ctx aws.Context, input *dynamodb.BatchExecuteStatementInput, opts ...request.Option) (*dynamodb.BatchExecuteStatementOutput, error) {
	return dp.metricsBatchExecuteStatement(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsBatchExecuteStatement(withoutContext bool, ctx aws.Context, input *dynamodb.BatchExecuteStatementInput, opts ...request.Option) (output *dynamodb.BatchExecuteStatementOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbBatchExecStm, input
	if withoutContext {
		output, err = dp.DynamoDB.BatchExecuteStatement(input)
	} else {
		output, err = dp.DynamoDB.BatchExecuteStatementWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	if output != nil {
		cmd.EndWithCost(dynamodbCombineConsumedCapacity(output.ConsumedCapacity), CmdResultOk, CmdResultError, err)
	} else {
		cmd.EndWithCost(0.0, CmdResultOk, CmdResultError, err)
	}
	return output, err
}

// BatchGetItem overrides dynamodb.DynamoDB/BatchGetItem to log execution metrics.
func (dp *DynamoDbProxy) BatchGetItem(input *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error) {
	return dp.metricsBatchGetItem(true, nil, input)
}

// BatchGetItemWithContext overrides dynamodb.DynamoDB/BatchGetItemWithContext to log execution metrics.
func (dp *DynamoDbProxy) BatchGetItemWithContext(ctx aws.Context, input *dynamodb.BatchGetItemInput, opts ...request.Option) (*dynamodb.BatchGetItemOutput, error) {
	return dp.metricsBatchGetItem(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsBatchGetItem(withoutContext bool, ctx aws.Context, input *dynamodb.BatchGetItemInput, opts ...request.Option) (output *dynamodb.BatchGetItemOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbBatchGetItem, input
	if withoutContext {
		output, err = dp.DynamoDB.BatchGetItem(input)
	} else {
		output, err = dp.DynamoDB.BatchGetItemWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	if output != nil {
		cmd.EndWithCost(dynamodbCombineConsumedCapacity(output.ConsumedCapacity), CmdResultOk, CmdResultError, err)
	} else {
		cmd.EndWithCost(0.0, CmdResultOk, CmdResultError, err)
	}
	return output, err
}

// BatchGetItemPages overrides dynamodb.DynamoDB/BatchGetItemPages to log execution metrics.
func (dp *DynamoDbProxy) BatchGetItemPages(input *dynamodb.BatchGetItemInput, fn func(*dynamodb.BatchGetItemOutput, bool) bool) error {
	return dp.metricsBatchGetItemPages(true, nil, input, fn)
}

// BatchGetItemPagesWithContext overrides dynamodb.DynamoDB/BatchGetItemPagesWithContext to log execution metrics.
func (dp *DynamoDbProxy) BatchGetItemPagesWithContext(ctx aws.Context, input *dynamodb.BatchGetItemInput, fn func(*dynamodb.BatchGetItemOutput, bool) bool, opts ...request.Option) error {
	return dp.metricsBatchGetItemPages(false, nil, input, fn, opts...)
}

func (dp *DynamoDbProxy) metricsBatchGetItemPages(withoutContext bool, ctx aws.Context, input *dynamodb.BatchGetItemInput, fn func(*dynamodb.BatchGetItemOutput, bool) bool, opts ...request.Option) (err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbBatchGetItem, input
	cost := 0.0
	if withoutContext {
		err = dp.DynamoDB.BatchGetItemPages(input, func(output *dynamodb.BatchGetItemOutput, b bool) bool {
			if output != nil {
				cost += dynamodbCombineConsumedCapacity(output.ConsumedCapacity)
			}
			return fn(output, b)
		})
	} else {
		err = dp.DynamoDB.BatchGetItemPagesWithContext(ctx, input, func(output *dynamodb.BatchGetItemOutput, b bool) bool {
			if output != nil {
				cost += dynamodbCombineConsumedCapacity(output.ConsumedCapacity)
			}
			return fn(output, b)
		}, opts...)
	}
	cmd.EndWithCost(cost, CmdResultOk, CmdResultError, err)
	return err
}

// BatchWriteItem overrides dynamodb.DynamoDB/BatchWriteItem to log execution metrics.
func (dp *DynamoDbProxy) BatchWriteItem(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
	return dp.metricsBatchWriteItem(true, nil, input)
}

// BatchWriteItemWithContext overrides dynamodb.DynamoDB/BatchWriteItemWithContext to log execution metrics.
func (dp *DynamoDbProxy) BatchWriteItemWithContext(ctx aws.Context, input *dynamodb.BatchWriteItemInput, opts ...request.Option) (*dynamodb.BatchWriteItemOutput, error) {
	return dp.metricsBatchWriteItem(false, nil, input, opts...)
}

func (dp *DynamoDbProxy) metricsBatchWriteItem(withoutContext bool, ctx aws.Context, input *dynamodb.BatchWriteItemInput, opts ...request.Option) (output *dynamodb.BatchWriteItemOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbBatchWriteItem, input
	if withoutContext {
		output, err = dp.DynamoDB.BatchWriteItem(input)
	} else {
		output, err = dp.DynamoDB.BatchWriteItemWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	if output != nil {
		cmd.EndWithCost(dynamodbCombineConsumedCapacity(output.ConsumedCapacity), CmdResultOk, CmdResultError, err)
	} else {
		cmd.EndWithCost(0.0, CmdResultOk, CmdResultError, err)
	}
	return output, err
}

// CreateBackup overrides dynamodb.DynamoDB/CreateBackup to log execution metrics.
func (dp *DynamoDbProxy) CreateBackup(input *dynamodb.CreateBackupInput) (*dynamodb.CreateBackupOutput, error) {
	return dp.metricsCreateBackup(true, nil, input)
}

// CreateBackupWithContext overrides dynamodb.DynamoDB/CreateBackupWithContext to log execution metrics.
func (dp *DynamoDbProxy) CreateBackupWithContext(ctx aws.Context, input *dynamodb.CreateBackupInput, opts ...request.Option) (*dynamodb.CreateBackupOutput, error) {
	return dp.metricsCreateBackup(false, nil, input, opts...)
}

func (dp *DynamoDbProxy) metricsCreateBackup(withoutContext bool, ctx aws.Context, input *dynamodb.CreateBackupInput, opts ...request.Option) (output *dynamodb.CreateBackupOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbCreateBackup, input
	if withoutContext {
		output, err = dp.DynamoDB.CreateBackup(input)
	} else {
		output, err = dp.DynamoDB.CreateBackupWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// CreateGlobalTable overrides dynamodb.DynamoDB/CreateGlobalTable to log execution metrics.
func (dp *DynamoDbProxy) CreateGlobalTable(input *dynamodb.CreateGlobalTableInput) (*dynamodb.CreateGlobalTableOutput, error) {
	return dp.metricsCreateGlobalTable(true, nil, input)
}

// CreateGlobalTableWithContext overrides dynamodb.DynamoDB/CreateGlobalTableWithContext to log execution metrics.
func (dp *DynamoDbProxy) CreateGlobalTableWithContext(ctx aws.Context, input *dynamodb.CreateGlobalTableInput, opts ...request.Option) (*dynamodb.CreateGlobalTableOutput, error) {
	return dp.metricsCreateGlobalTable(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsCreateGlobalTable(withoutContext bool, ctx aws.Context, input *dynamodb.CreateGlobalTableInput, opts ...request.Option) (output *dynamodb.CreateGlobalTableOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbCreateGlobalTable, input
	if withoutContext {
		output, err = dp.DynamoDB.CreateGlobalTable(input)
	} else {
		output, err = dp.DynamoDB.CreateGlobalTableWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// CreateTable overrides dynamodb.DynamoDB/CreateTable to log execution metrics.
func (dp *DynamoDbProxy) CreateTable(input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	return dp.metricsCreateTable(true, nil, input)
}

// CreateTableWithContext overrides dynamodb.DynamoDB/CreateTableWithContext to log execution metrics.
func (dp *DynamoDbProxy) CreateTableWithContext(ctx aws.Context, input *dynamodb.CreateTableInput, opts ...request.Option) (*dynamodb.CreateTableOutput, error) {
	return dp.metricsCreateTable(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsCreateTable(withoutContext bool, ctx aws.Context, input *dynamodb.CreateTableInput, opts ...request.Option) (output *dynamodb.CreateTableOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbCreateTable, input
	if withoutContext {
		output, err = dp.DynamoDB.CreateTable(input)
	} else {
		output, err = dp.DynamoDB.CreateTableWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// DeleteBackup overrides dynamodb.DynamoDB/DeleteBackup to log execution metrics.
func (dp *DynamoDbProxy) DeleteBackup(input *dynamodb.DeleteBackupInput) (*dynamodb.DeleteBackupOutput, error) {
	return dp.metricsDeleteBackup(true, nil, input)
}

// DeleteBackupWithContext overrides dynamodb.DynamoDB/DeleteBackupWithContext to log execution metrics.
func (dp *DynamoDbProxy) DeleteBackupWithContext(ctx aws.Context, input *dynamodb.DeleteBackupInput, opts ...request.Option) (*dynamodb.DeleteBackupOutput, error) {
	return dp.metricsDeleteBackup(false, nil, input, opts...)
}

func (dp *DynamoDbProxy) metricsDeleteBackup(withoutContext bool, ctx aws.Context, input *dynamodb.DeleteBackupInput, opts ...request.Option) (output *dynamodb.DeleteBackupOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbDeleteBackup, input
	if withoutContext {
		output, err = dp.DynamoDB.DeleteBackup(input)
	} else {
		output, err = dp.DynamoDB.DeleteBackupWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// DeleteItem overrides dynamodb.DynamoDB/DeleteItem to log execution metrics.
func (dp *DynamoDbProxy) DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	return dp.metricsDeleteItem(true, nil, input)
}

// DeleteItemWithContext overrides dynamodb.DynamoDB/DeleteItemWithContext to log execution metrics.
func (dp *DynamoDbProxy) DeleteItemWithContext(ctx aws.Context, input *dynamodb.DeleteItemInput, opts ...request.Option) (*dynamodb.DeleteItemOutput, error) {
	return dp.metricsDeleteItem(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsDeleteItem(withoutContext bool, ctx aws.Context, input *dynamodb.DeleteItemInput, opts ...request.Option) (output *dynamodb.DeleteItemOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbDeleteItem, input
	if withoutContext {
		output, err = dp.DynamoDB.DeleteItem(input)
	} else {
		output, err = dp.DynamoDB.DeleteItemWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	if output != nil {
		cmd.EndWithCost(dynamodbCombineConsumedCapacity([]*dynamodb.ConsumedCapacity{output.ConsumedCapacity}), CmdResultOk, CmdResultError, err)
	} else {
		cmd.EndWithCost(0.0, CmdResultOk, CmdResultError, err)
	}
	return output, err
}

// DeleteTable overrides dynamodb.DynamoDB/DeleteTable to log execution metrics.
func (dp *DynamoDbProxy) DeleteTable(input *dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error) {
	return dp.metricsDeleteTable(true, nil, input)
}

// DeleteTableWithContext overrides dynamodb.DynamoDB/DeleteTableWithContext to log execution metrics.
func (dp *DynamoDbProxy) DeleteTableWithContext(ctx aws.Context, input *dynamodb.DeleteTableInput, opts ...request.Option) (*dynamodb.DeleteTableOutput, error) {
	return dp.metricsDeleteTable(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsDeleteTable(withoutContext bool, ctx aws.Context, input *dynamodb.DeleteTableInput, opts ...request.Option) (output *dynamodb.DeleteTableOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbDeleteTable, input
	if withoutContext {
		output, err = dp.DynamoDB.DeleteTable(input)
	} else {
		output, err = dp.DynamoDB.DeleteTableWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// DescribeBackup overrides dynamodb.DynamoDB/DescribeBackup to log execution metrics.
func (dp *DynamoDbProxy) DescribeBackup(input *dynamodb.DescribeBackupInput) (*dynamodb.DescribeBackupOutput, error) {
	return dp.metricsDescribeBackup(true, nil, input)
}

// DescribeBackupWithContext overrides dynamodb.DynamoDB/DescribeBackupWithContext to log execution metrics.
func (dp *DynamoDbProxy) DescribeBackupWithContext(ctx aws.Context, input *dynamodb.DescribeBackupInput, opts ...request.Option) (*dynamodb.DescribeBackupOutput, error) {
	return dp.metricsDescribeBackup(false, nil, input, opts...)
}

func (dp *DynamoDbProxy) metricsDescribeBackup(withoutContext bool, ctx aws.Context, input *dynamodb.DescribeBackupInput, opts ...request.Option) (output *dynamodb.DescribeBackupOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbDescBackup, input
	if withoutContext {
		output, err = dp.DynamoDB.DescribeBackup(input)
	} else {
		output, err = dp.DynamoDB.DescribeBackupWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// DescribeContinuousBackups overrides dynamodb.DynamoDB/DescribeContinuousBackups to log execution metrics.
func (dp *DynamoDbProxy) DescribeContinuousBackups(input *dynamodb.DescribeContinuousBackupsInput) (*dynamodb.DescribeContinuousBackupsOutput, error) {
	return dp.metricsDescribeContinuousBackups(true, nil, input)
}

// DescribeContinuousBackupsWithContext overrides dynamodb.DynamoDB/DescribeContinuousBackupsWithContext to log execution metrics.
func (dp *DynamoDbProxy) DescribeContinuousBackupsWithContext(ctx aws.Context, input *dynamodb.DescribeContinuousBackupsInput, opts ...request.Option) (*dynamodb.DescribeContinuousBackupsOutput, error) {
	return dp.metricsDescribeContinuousBackups(false, nil, input, opts...)
}

func (dp *DynamoDbProxy) metricsDescribeContinuousBackups(withoutContext bool, ctx aws.Context, input *dynamodb.DescribeContinuousBackupsInput, opts ...request.Option) (output *dynamodb.DescribeContinuousBackupsOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbDescContinuousBackups, input
	if withoutContext {
		output, err = dp.DynamoDB.DescribeContinuousBackups(input)
	} else {
		output, err = dp.DynamoDB.DescribeContinuousBackupsWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// DescribeContributorInsights overrides dynamodb.DynamoDB/DescribeContributorInsights to log execution metrics.
func (dp *DynamoDbProxy) DescribeContributorInsights(input *dynamodb.DescribeContributorInsightsInput) (*dynamodb.DescribeContributorInsightsOutput, error) {
	return dp.metricsDescribeContributorInsights(true, nil, input)
}

// DescribeContributorInsightsWithContext overrides dynamodb.DynamoDB/DescribeContributorInsightsWithContext to log execution metrics.
func (dp *DynamoDbProxy) DescribeContributorInsightsWithContext(ctx aws.Context, input *dynamodb.DescribeContributorInsightsInput, opts ...request.Option) (*dynamodb.DescribeContributorInsightsOutput, error) {
	return dp.metricsDescribeContributorInsights(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsDescribeContributorInsights(withoutContext bool, ctx aws.Context, input *dynamodb.DescribeContributorInsightsInput, opts ...request.Option) (output *dynamodb.DescribeContributorInsightsOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbDescContributorInsights, input
	if withoutContext {
		output, err = dp.DynamoDB.DescribeContributorInsights(input)
	} else {
		output, err = dp.DynamoDB.DescribeContributorInsightsWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// DescribeEndpoints overrides dynamodb.DynamoDB/DescribeEndpoints to log execution metrics.
func (dp *DynamoDbProxy) DescribeEndpoints(input *dynamodb.DescribeEndpointsInput) (*dynamodb.DescribeEndpointsOutput, error) {
	return dp.metricsDescribeEndpoints(true, nil, input)
}

// DescribeEndpointsWithContext overrides dynamodb.DynamoDB/DescribeEndpointsWithContext to log execution metrics.
func (dp *DynamoDbProxy) DescribeEndpointsWithContext(ctx aws.Context, input *dynamodb.DescribeEndpointsInput, opts ...request.Option) (*dynamodb.DescribeEndpointsOutput, error) {
	return dp.metricsDescribeEndpoints(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsDescribeEndpoints(withoutContext bool, ctx aws.Context, input *dynamodb.DescribeEndpointsInput, opts ...request.Option) (output *dynamodb.DescribeEndpointsOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbDescEndpoints, input
	if withoutContext {
		output, err = dp.DynamoDB.DescribeEndpoints(input)
	} else {
		output, err = dp.DynamoDB.DescribeEndpointsWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// DescribeExport overrides dynamodb.DynamoDB/DescribeExport to log execution metrics.
func (dp *DynamoDbProxy) DescribeExport(input *dynamodb.DescribeExportInput) (*dynamodb.DescribeExportOutput, error) {
	return dp.metricsDescribeExport(true, nil, input)
}

// DescribeExportWithContext overrides dynamodb.DynamoDB/DescribeExportWithContext to log execution metrics.
func (dp *DynamoDbProxy) DescribeExportWithContext(ctx aws.Context, input *dynamodb.DescribeExportInput, opts ...request.Option) (*dynamodb.DescribeExportOutput, error) {
	return dp.metricsDescribeExport(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsDescribeExport(withoutContext bool, ctx aws.Context, input *dynamodb.DescribeExportInput, opts ...request.Option) (output *dynamodb.DescribeExportOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbDescExport, input
	if withoutContext {
		output, err = dp.DynamoDB.DescribeExport(input)
	} else {
		output, err = dp.DynamoDB.DescribeExportWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// DescribeGlobalTable overrides dynamodb.DynamoDB/DescribeGlobalTable to log execution metrics.
func (dp *DynamoDbProxy) DescribeGlobalTable(input *dynamodb.DescribeGlobalTableInput) (*dynamodb.DescribeGlobalTableOutput, error) {
	return dp.metricsDescribeGlobalTable(true, nil, input)
}

// DescribeGlobalTableWithContext overrides dynamodb.DynamoDB/DescribeGlobalTableWithContext to log execution metrics.
func (dp *DynamoDbProxy) DescribeGlobalTableWithContext(ctx aws.Context, input *dynamodb.DescribeGlobalTableInput, opts ...request.Option) (*dynamodb.DescribeGlobalTableOutput, error) {
	return dp.metricsDescribeGlobalTable(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsDescribeGlobalTable(withoutContext bool, ctx aws.Context, input *dynamodb.DescribeGlobalTableInput, opts ...request.Option) (output *dynamodb.DescribeGlobalTableOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbDescGlobalTable, input
	if withoutContext {
		output, err = dp.DynamoDB.DescribeGlobalTable(input)
	} else {
		output, err = dp.DynamoDB.DescribeGlobalTableWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// DescribeGlobalTableSettings overrides dynamodb.DynamoDB/DescribeGlobalTableSettings to log execution metrics.
func (dp *DynamoDbProxy) DescribeGlobalTableSettings(input *dynamodb.DescribeGlobalTableSettingsInput) (*dynamodb.DescribeGlobalTableSettingsOutput, error) {
	return dp.metricsDescribeGlobalTableSettings(true, nil, input)
}

// DescribeGlobalTableSettingsWithContext overrides dynamodb.DynamoDB/DescribeGlobalTableSettingsWithContext to log execution metrics.
func (dp *DynamoDbProxy) DescribeGlobalTableSettingsWithContext(ctx aws.Context, input *dynamodb.DescribeGlobalTableSettingsInput, opts ...request.Option) (*dynamodb.DescribeGlobalTableSettingsOutput, error) {
	return dp.metricsDescribeGlobalTableSettings(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsDescribeGlobalTableSettings(withoutContext bool, ctx aws.Context, input *dynamodb.DescribeGlobalTableSettingsInput, opts ...request.Option) (output *dynamodb.DescribeGlobalTableSettingsOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbDescGlobalTableSettings, input
	if withoutContext {
		output, err = dp.DynamoDB.DescribeGlobalTableSettings(input)
	} else {
		output, err = dp.DynamoDB.DescribeGlobalTableSettingsWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// DescribeKinesisStreamingDestination overrides dynamodb.DynamoDB/DescribeKinesisStreamingDestination to log execution metrics.
func (dp *DynamoDbProxy) DescribeKinesisStreamingDestination(input *dynamodb.DescribeKinesisStreamingDestinationInput) (*dynamodb.DescribeKinesisStreamingDestinationOutput, error) {
	return dp.metricsDescribeKinesisStreamingDestination(true, nil, input)
}

// DescribeKinesisStreamingDestinationWithContext overrides dynamodb.DynamoDB/DescribeKinesisStreamingDestinationWithContext to log execution metrics.
func (dp *DynamoDbProxy) DescribeKinesisStreamingDestinationWithContext(ctx aws.Context, input *dynamodb.DescribeKinesisStreamingDestinationInput, opts ...request.Option) (*dynamodb.DescribeKinesisStreamingDestinationOutput, error) {
	return dp.metricsDescribeKinesisStreamingDestination(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsDescribeKinesisStreamingDestination(withoutContext bool, ctx aws.Context, input *dynamodb.DescribeKinesisStreamingDestinationInput, opts ...request.Option) (output *dynamodb.DescribeKinesisStreamingDestinationOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbDescKinesisStreamingDestination, input
	if withoutContext {
		output, err = dp.DynamoDB.DescribeKinesisStreamingDestination(input)
	} else {
		output, err = dp.DynamoDB.DescribeKinesisStreamingDestinationWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// DescribeLimits overrides dynamodb.DynamoDB/DescribeLimits to log execution metrics.
func (dp *DynamoDbProxy) DescribeLimits(input *dynamodb.DescribeLimitsInput) (*dynamodb.DescribeLimitsOutput, error) {
	return dp.metricsDescribeLimits(true, nil, input)
}

// DescribeLimitsWithContext overrides dynamodb.DynamoDB/DescribeLimitsWithContext to log execution metrics.
func (dp *DynamoDbProxy) DescribeLimitsWithContext(ctx aws.Context, input *dynamodb.DescribeLimitsInput, opts ...request.Option) (*dynamodb.DescribeLimitsOutput, error) {
	return dp.metricsDescribeLimits(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsDescribeLimits(withoutContext bool, ctx aws.Context, input *dynamodb.DescribeLimitsInput, opts ...request.Option) (output *dynamodb.DescribeLimitsOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbDescLimits, input
	if withoutContext {
		output, err = dp.DynamoDB.DescribeLimits(input)
	} else {
		output, err = dp.DynamoDB.DescribeLimitsWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// DescribeTable overrides dynamodb.DynamoDB/DescribeTable to log execution metrics.
func (dp *DynamoDbProxy) DescribeTable(input *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
	return dp.metricsDescribeTable(true, nil, input)
}

// DescribeTableWithContext overrides dynamodb.DynamoDB/DescribeTableWithContext to log execution metrics.
func (dp *DynamoDbProxy) DescribeTableWithContext(ctx aws.Context, input *dynamodb.DescribeTableInput, opts ...request.Option) (*dynamodb.DescribeTableOutput, error) {
	return dp.metricsDescribeTable(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsDescribeTable(withoutContext bool, ctx aws.Context, input *dynamodb.DescribeTableInput, opts ...request.Option) (output *dynamodb.DescribeTableOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbDescTable, input
	if withoutContext {
		output, err = dp.DynamoDB.DescribeTable(input)
	} else {
		output, err = dp.DynamoDB.DescribeTableWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// DescribeTableReplicaAutoScaling overrides dynamodb.DynamoDB/DescribeTableReplicaAutoScaling to log execution metrics.
func (dp *DynamoDbProxy) DescribeTableReplicaAutoScaling(input *dynamodb.DescribeTableReplicaAutoScalingInput) (*dynamodb.DescribeTableReplicaAutoScalingOutput, error) {
	return dp.metricsDescribeTableReplicaAutoScaling(true, nil, input)
}

// DescribeTableReplicaAutoScalingWithContext overrides dynamodb.DynamoDB/DescribeTableReplicaAutoScalingWithContext to log execution metrics.
func (dp *DynamoDbProxy) DescribeTableReplicaAutoScalingWithContext(ctx aws.Context, input *dynamodb.DescribeTableReplicaAutoScalingInput, opts ...request.Option) (*dynamodb.DescribeTableReplicaAutoScalingOutput, error) {
	return dp.metricsDescribeTableReplicaAutoScaling(false, nil, input, opts...)
}

func (dp *DynamoDbProxy) metricsDescribeTableReplicaAutoScaling(withoutContext bool, ctx aws.Context, input *dynamodb.DescribeTableReplicaAutoScalingInput, opts ...request.Option) (output *dynamodb.DescribeTableReplicaAutoScalingOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbDescTableReplicaAutoScaling, input

	if withoutContext {
		output, err = dp.DynamoDB.DescribeTableReplicaAutoScaling(input)
	} else {
		output, err = dp.DynamoDB.DescribeTableReplicaAutoScalingWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// DescribeTimeToLive overrides dynamodb.DynamoDB/DescribeTimeToLive to log execution metrics.
func (dp *DynamoDbProxy) DescribeTimeToLive(input *dynamodb.DescribeTimeToLiveInput) (*dynamodb.DescribeTimeToLiveOutput, error) {
	return dp.metricsDescribeTimeToLive(true, nil, input)
}

// DescribeTimeToLiveWithContext overrides dynamodb.DynamoDB/DescribeTimeToLiveWithContext to log execution metrics.
func (dp *DynamoDbProxy) DescribeTimeToLiveWithContext(ctx aws.Context, input *dynamodb.DescribeTimeToLiveInput, opts ...request.Option) (*dynamodb.DescribeTimeToLiveOutput, error) {
	return dp.metricsDescribeTimeToLive(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsDescribeTimeToLive(withoutContext bool, ctx aws.Context, input *dynamodb.DescribeTimeToLiveInput, opts ...request.Option) (output *dynamodb.DescribeTimeToLiveOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbDescTTL, input
	if withoutContext {
		output, err = dp.DynamoDB.DescribeTimeToLive(input)
	} else {
		output, err = dp.DynamoDB.DescribeTimeToLiveWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// DisableKinesisStreamingDestination overrides dynamodb.DynamoDB/DisableKinesisStreamingDestination to log execution metrics.
func (dp *DynamoDbProxy) DisableKinesisStreamingDestination(input *dynamodb.DisableKinesisStreamingDestinationInput) (*dynamodb.DisableKinesisStreamingDestinationOutput, error) {
	return dp.metricsDisableKinesisStreamingDestination(true, nil, input)
}

// DisableKinesisStreamingDestinationWithContext overrides dynamodb.DynamoDB/DisableKinesisStreamingDestinationWithContext to log execution metrics.
func (dp *DynamoDbProxy) DisableKinesisStreamingDestinationWithContext(ctx aws.Context, input *dynamodb.DisableKinesisStreamingDestinationInput, opts ...request.Option) (*dynamodb.DisableKinesisStreamingDestinationOutput, error) {
	return dp.metricsDisableKinesisStreamingDestination(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsDisableKinesisStreamingDestination(withoutContext bool, ctx aws.Context, input *dynamodb.DisableKinesisStreamingDestinationInput, opts ...request.Option) (output *dynamodb.DisableKinesisStreamingDestinationOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbDisableKinesisStreamingDestination, input
	if withoutContext {
		output, err = dp.DynamoDB.DisableKinesisStreamingDestination(input)
	} else {
		output, err = dp.DynamoDB.DisableKinesisStreamingDestinationWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// EnableKinesisStreamingDestination overrides dynamodb.DynamoDB/EnableKinesisStreamingDestination to log execution metrics.
func (dp *DynamoDbProxy) EnableKinesisStreamingDestination(input *dynamodb.EnableKinesisStreamingDestinationInput) (*dynamodb.EnableKinesisStreamingDestinationOutput, error) {
	return dp.metricsEnableKinesisStreamingDestination(true, nil, input)
}

// EnableKinesisStreamingDestinationWithContext overrides dynamodb.DynamoDB/EnableKinesisStreamingDestinationWithContext to log execution metrics.
func (dp *DynamoDbProxy) EnableKinesisStreamingDestinationWithContext(ctx aws.Context, input *dynamodb.EnableKinesisStreamingDestinationInput, opts ...request.Option) (*dynamodb.EnableKinesisStreamingDestinationOutput, error) {
	return dp.metricsEnableKinesisStreamingDestination(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsEnableKinesisStreamingDestination(withoutContext bool, ctx aws.Context, input *dynamodb.EnableKinesisStreamingDestinationInput, opts ...request.Option) (output *dynamodb.EnableKinesisStreamingDestinationOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbEnableKinesisStreamingDestination, input
	if withoutContext {
		output, err = dp.DynamoDB.EnableKinesisStreamingDestination(input)
	} else {
		output, err = dp.DynamoDB.EnableKinesisStreamingDestinationWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// ExecuteStatement overrides dynamodb.DynamoDB/ExecuteStatement to log execution metrics.
func (dp *DynamoDbProxy) ExecuteStatement(input *dynamodb.ExecuteStatementInput) (*dynamodb.ExecuteStatementOutput, error) {
	return dp.metricsExecuteStatement(true, nil, input)
}

// ExecuteStatementWithContext overrides dynamodb.DynamoDB/ExecuteStatementWithContext to log execution metrics.
func (dp *DynamoDbProxy) ExecuteStatementWithContext(ctx aws.Context, input *dynamodb.ExecuteStatementInput, opts ...request.Option) (*dynamodb.ExecuteStatementOutput, error) {
	return dp.metricsExecuteStatement(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsExecuteStatement(withoutContext bool, ctx aws.Context, input *dynamodb.ExecuteStatementInput, opts ...request.Option) (output *dynamodb.ExecuteStatementOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbExecuteStatement, input
	if withoutContext {
		output, err = dp.DynamoDB.ExecuteStatement(input)
	} else {
		output, err = dp.DynamoDB.ExecuteStatementWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCost(dynamodbCombineConsumedCapacity([]*dynamodb.ConsumedCapacity{output.ConsumedCapacity}), CmdResultOk, CmdResultError, err)
	return output, err
}

// ExecuteTransaction overrides dynamodb.DynamoDB/ExecuteTransaction to log execution metrics.
func (dp *DynamoDbProxy) ExecuteTransaction(input *dynamodb.ExecuteTransactionInput) (*dynamodb.ExecuteTransactionOutput, error) {
	return dp.metricsExecuteTransaction(true, nil, input)
}

// ExecuteTransactionWithContext overrides dynamodb.DynamoDB/ExecuteTransactionWithContext to log execution metrics.
func (dp *DynamoDbProxy) ExecuteTransactionWithContext(ctx aws.Context, input *dynamodb.ExecuteTransactionInput, opts ...request.Option) (*dynamodb.ExecuteTransactionOutput, error) {
	return dp.metricsExecuteTransaction(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsExecuteTransaction(withoutContext bool, ctx aws.Context, input *dynamodb.ExecuteTransactionInput, opts ...request.Option) (output *dynamodb.ExecuteTransactionOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbExecuteTransaction, input
	if withoutContext {
		output, err = dp.DynamoDB.ExecuteTransaction(input)
	} else {
		output, err = dp.DynamoDB.ExecuteTransactionWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCost(dynamodbCombineConsumedCapacity(output.ConsumedCapacity), CmdResultOk, CmdResultError, err)
	return output, err
}

// ExportTableToPointInTime overrides dynamodb.DynamoDB/ExportTableToPointInTime to log execution metrics.
func (dp *DynamoDbProxy) ExportTableToPointInTime(input *dynamodb.ExportTableToPointInTimeInput) (*dynamodb.ExportTableToPointInTimeOutput, error) {
	return dp.metricsExportTableToPointInTime(true, nil, input)
}

// ExportTableToPointInTimeWithContext overrides dynamodb.DynamoDB/ExportTableToPointInTimeWithContext to log execution metrics.
func (dp *DynamoDbProxy) ExportTableToPointInTimeWithContext(ctx aws.Context, input *dynamodb.ExportTableToPointInTimeInput, opts ...request.Option) (*dynamodb.ExportTableToPointInTimeOutput, error) {
	return dp.metricsExportTableToPointInTime(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsExportTableToPointInTime(withoutContext bool, ctx aws.Context, input *dynamodb.ExportTableToPointInTimeInput, opts ...request.Option) (output *dynamodb.ExportTableToPointInTimeOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbExportTableToPIT, input
	if withoutContext {
		output, err = dp.DynamoDB.ExportTableToPointInTime(input)
	} else {
		output, err = dp.DynamoDB.ExportTableToPointInTimeWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// GetItem overrides dynamodb.DynamoDB/GetItem to log execution metrics.
func (dp *DynamoDbProxy) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	return dp.metricsGetItem(true, nil, input)
}

// GetItemWithContext overrides dynamodb.DynamoDB/GetItemWithContext to log execution metrics.
func (dp *DynamoDbProxy) GetItemWithContext(ctx aws.Context, input *dynamodb.GetItemInput, opts ...request.Option) (*dynamodb.GetItemOutput, error) {
	return dp.metricsGetItem(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsGetItem(withoutContext bool, ctx aws.Context, input *dynamodb.GetItemInput, opts ...request.Option) (output *dynamodb.GetItemOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbGetItem, input
	if withoutContext {
		output, err = dp.DynamoDB.GetItem(input)
	} else {
		output, err = dp.DynamoDB.GetItemWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCost(dynamodbCombineConsumedCapacity([]*dynamodb.ConsumedCapacity{output.ConsumedCapacity}), CmdResultOk, CmdResultError, err)
	return output, err
}

// ListBackups overrides dynamodb.DynamoDB/ListBackups to log execution metrics.
func (dp *DynamoDbProxy) ListBackups(input *dynamodb.ListBackupsInput) (*dynamodb.ListBackupsOutput, error) {
	return dp.metricsListBackups(true, nil, input)
}

// ListBackupsWithContext overrides dynamodb.DynamoDB/ListBackupsWithContext to log execution metrics.
func (dp *DynamoDbProxy) ListBackupsWithContext(ctx aws.Context, input *dynamodb.ListBackupsInput, opts ...request.Option) (*dynamodb.ListBackupsOutput, error) {
	return dp.metricsListBackups(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsListBackups(withoutContext bool, ctx aws.Context, input *dynamodb.ListBackupsInput, opts ...request.Option) (output *dynamodb.ListBackupsOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbListBackups, input
	if withoutContext {
		output, err = dp.DynamoDB.ListBackups(input)
	} else {
		output, err = dp.DynamoDB.ListBackupsWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// ListContributorInsights overrides dynamodb.DynamoDB/ListContributorInsights to log execution metrics.
func (dp *DynamoDbProxy) ListContributorInsights(input *dynamodb.ListContributorInsightsInput) (*dynamodb.ListContributorInsightsOutput, error) {
	return dp.metricsListContributorInsights(true, nil, input)
}

// ListContributorInsightsWithContext overrides dynamodb.DynamoDB/ListContributorInsightsWithContext to log execution metrics.
func (dp *DynamoDbProxy) ListContributorInsightsWithContext(ctx aws.Context, input *dynamodb.ListContributorInsightsInput, opts ...request.Option) (*dynamodb.ListContributorInsightsOutput, error) {
	return dp.metricsListContributorInsights(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsListContributorInsights(withoutContext bool, ctx aws.Context, input *dynamodb.ListContributorInsightsInput, opts ...request.Option) (output *dynamodb.ListContributorInsightsOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbListContributorInsights, input
	if withoutContext {
		output, err = dp.DynamoDB.ListContributorInsights(input)
	} else {
		output, err = dp.DynamoDB.ListContributorInsightsWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// ListContributorInsightsPages overrides dynamodb.DynamoDB/ListContributorInsightsPages to log execution metrics.
func (dp *DynamoDbProxy) ListContributorInsightsPages(input *dynamodb.ListContributorInsightsInput, fn func(*dynamodb.ListContributorInsightsOutput, bool) bool) error {
	return dp.metricsListContributorInsightsPages(true, nil, input, fn)
}

// ListContributorInsightsPagesWithContext overrides dynamodb.DynamoDB/ListContributorInsightsPagesWithContext to log execution metrics.
func (dp *DynamoDbProxy) ListContributorInsightsPagesWithContext(ctx aws.Context, input *dynamodb.ListContributorInsightsInput, fn func(*dynamodb.ListContributorInsightsOutput, bool) bool, opts ...request.Option) error {
	return dp.metricsListContributorInsightsPages(false, ctx, input, fn, opts...)
}

func (dp *DynamoDbProxy) metricsListContributorInsightsPages(withoutContext bool, ctx aws.Context, input *dynamodb.ListContributorInsightsInput, fn func(*dynamodb.ListContributorInsightsOutput, bool) bool, opts ...request.Option) (err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbListContributorInsights, input
	if withoutContext {
		err = dp.DynamoDB.ListContributorInsightsPages(input, fn)
	} else {
		err = dp.DynamoDB.ListContributorInsightsPagesWithContext(ctx, input, fn, opts...)
	}
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return err
}

// ListExports overrides dynamodb.DynamoDB/ListExports to log execution metrics.
func (dp *DynamoDbProxy) ListExports(input *dynamodb.ListExportsInput) (*dynamodb.ListExportsOutput, error) {
	return dp.metricsListExports(true, nil, input)
}

// ListExportsWithContext overrides dynamodb.DynamoDB/ListExportsWithContext to log execution metrics.
func (dp *DynamoDbProxy) ListExportsWithContext(ctx aws.Context, input *dynamodb.ListExportsInput, opts ...request.Option) (*dynamodb.ListExportsOutput, error) {
	return dp.metricsListExports(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsListExports(withoutContext bool, ctx aws.Context, input *dynamodb.ListExportsInput, opts ...request.Option) (output *dynamodb.ListExportsOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbListExports, input
	if withoutContext {
		output, err = dp.DynamoDB.ListExports(input)
	} else {
		output, err = dp.DynamoDB.ListExportsWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// ListExportsPages overrides dynamodb.DynamoDB/ListExportsPages to log execution metrics.
func (dp *DynamoDbProxy) ListExportsPages(input *dynamodb.ListExportsInput, fn func(*dynamodb.ListExportsOutput, bool) bool) error {
	return dp.metricsListExportsPages(true, nil, input, fn)
}

// ListExportsPagesWithContext overrides dynamodb.DynamoDB/ListExportsPagesWithContext to log execution metrics.
func (dp *DynamoDbProxy) ListExportsPagesWithContext(ctx aws.Context, input *dynamodb.ListExportsInput, fn func(*dynamodb.ListExportsOutput, bool) bool, opts ...request.Option) error {
	return dp.metricsListExportsPages(false, ctx, input, fn, opts...)
}

func (dp *DynamoDbProxy) metricsListExportsPages(withoutContext bool, ctx aws.Context, input *dynamodb.ListExportsInput, fn func(*dynamodb.ListExportsOutput, bool) bool, opts ...request.Option) (err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbListExports, input
	if withoutContext {
		err = dp.DynamoDB.ListExportsPages(input, fn)
	} else {
		err = dp.DynamoDB.ListExportsPagesWithContext(ctx, input, fn, opts...)
	}
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return err
}

// ListGlobalTables overrides dynamodb.DynamoDB/ListGlobalTables to log execution metrics.
func (dp *DynamoDbProxy) ListGlobalTables(input *dynamodb.ListGlobalTablesInput) (*dynamodb.ListGlobalTablesOutput, error) {
	return dp.metricsListGlobalTables(true, nil, input)
}

// ListGlobalTablesWithContext overrides dynamodb.DynamoDB/ListGlobalTablesWithContext to log execution metrics.
func (dp *DynamoDbProxy) ListGlobalTablesWithContext(ctx aws.Context, input *dynamodb.ListGlobalTablesInput, opts ...request.Option) (*dynamodb.ListGlobalTablesOutput, error) {
	return dp.metricsListGlobalTables(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsListGlobalTables(withoutContext bool, ctx aws.Context, input *dynamodb.ListGlobalTablesInput, opts ...request.Option) (output *dynamodb.ListGlobalTablesOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbListGlobalTables, input
	if withoutContext {
		output, err = dp.DynamoDB.ListGlobalTables(input)
	} else {
		output, err = dp.DynamoDB.ListGlobalTablesWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// ListTables overrides dynamodb.DynamoDB/ListTables to log execution metrics.
func (dp *DynamoDbProxy) ListTables(input *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error) {
	return dp.metricsListTables(true, nil, input)
}

// ListTablesWithContext overrides dynamodb.DynamoDB/ListTablesWithContext to log execution metrics.
func (dp *DynamoDbProxy) ListTablesWithContext(ctx aws.Context, input *dynamodb.ListTablesInput, opts ...request.Option) (*dynamodb.ListTablesOutput, error) {
	return dp.metricsListTables(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsListTables(withoutContext bool, ctx aws.Context, input *dynamodb.ListTablesInput, opts ...request.Option) (output *dynamodb.ListTablesOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbListTables, input
	if withoutContext {
		output, err = dp.DynamoDB.ListTables(input)
	} else {
		output, err = dp.DynamoDB.ListTablesWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// ListTablesPages overrides dynamodb.DynamoDB/ListTablesPages to log execution metrics.
func (dp *DynamoDbProxy) ListTablesPages(input *dynamodb.ListTablesInput, fn func(*dynamodb.ListTablesOutput, bool) bool) error {
	return dp.metricsListTablesPages(true, nil, input, fn)
}

// ListTablesPagesWithContext overrides dynamodb.DynamoDB/ListTablesPagesWithContext to log execution metrics.
func (dp *DynamoDbProxy) ListTablesPagesWithContext(ctx aws.Context, input *dynamodb.ListTablesInput, fn func(*dynamodb.ListTablesOutput, bool) bool, opts ...request.Option) error {
	return dp.metricsListTablesPages(false, ctx, input, fn, opts...)
}

func (dp *DynamoDbProxy) metricsListTablesPages(withoutContext bool, ctx aws.Context, input *dynamodb.ListTablesInput, fn func(*dynamodb.ListTablesOutput, bool) bool, opts ...request.Option) (err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbListTables, input
	if withoutContext {
		err = dp.DynamoDB.ListTablesPages(input, fn)
	} else {
		err = dp.DynamoDB.ListTablesPagesWithContext(ctx, input, fn, opts...)
	}
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return err
}

// ListTagsOfResource overrides dynamodb.DynamoDB/ListTagsOfResource to log execution metrics.
func (dp *DynamoDbProxy) ListTagsOfResource(input *dynamodb.ListTagsOfResourceInput) (*dynamodb.ListTagsOfResourceOutput, error) {
	return dp.metricsListTagsOfResource(true, nil, input)
}

// ListTagsOfResourceWithContext overrides dynamodb.DynamoDB/ListTagsOfResourceWithContext to log execution metrics.
func (dp *DynamoDbProxy) ListTagsOfResourceWithContext(ctx aws.Context, input *dynamodb.ListTagsOfResourceInput, opts ...request.Option) (*dynamodb.ListTagsOfResourceOutput, error) {
	return dp.metricsListTagsOfResource(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsListTagsOfResource(withoutContext bool, ctx aws.Context, input *dynamodb.ListTagsOfResourceInput, opts ...request.Option) (output *dynamodb.ListTagsOfResourceOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbListTagsOfResource, input
	if withoutContext {
		output, err = dp.DynamoDB.ListTagsOfResource(input)
	} else {
		output, err = dp.DynamoDB.ListTagsOfResourceWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// PutItem overrides dynamodb.DynamoDB/PutItem to log execution metrics.
func (dp *DynamoDbProxy) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return dp.metricsPutItem(true, nil, input)
}

// PutItemWithContext overrides dynamodb.DynamoDB/PutItemWithContext to log execution metrics.
func (dp *DynamoDbProxy) PutItemWithContext(ctx aws.Context, input *dynamodb.PutItemInput, opts ...request.Option) (*dynamodb.PutItemOutput, error) {
	return dp.metricsPutItem(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsPutItem(withoutContext bool, ctx aws.Context, input *dynamodb.PutItemInput, opts ...request.Option) (output *dynamodb.PutItemOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbPutItem, input
	if withoutContext {
		output, err = dp.DynamoDB.PutItem(input)
	} else {
		output, err = dp.DynamoDB.PutItemWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cost := 0.0
	if output != nil {
		cost = dynamodbCombineConsumedCapacity([]*dynamodb.ConsumedCapacity{output.ConsumedCapacity})
	}
	cmd.EndWithCost(cost, CmdResultOk, CmdResultError, err)
	return output, err
}

// Query overrides dynamodb.DynamoDB/Query to log execution metrics.
func (dp *DynamoDbProxy) Query(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	return dp.metricsQuery(true, nil, input)
}

// QueryWithContext overrides dynamodb.DynamoDB/QueryWithContext to log execution metrics.
func (dp *DynamoDbProxy) QueryWithContext(ctx aws.Context, input *dynamodb.QueryInput, opts ...request.Option) (*dynamodb.QueryOutput, error) {
	return dp.metricsQuery(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsQuery(withoutContext bool, ctx aws.Context, input *dynamodb.QueryInput, opts ...request.Option) (output *dynamodb.QueryOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbQueryItems, input
	if withoutContext {
		output, err = dp.DynamoDB.Query(input)
	} else {
		output, err = dp.DynamoDB.QueryWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cost := 0.0
	if output != nil {
		cost = dynamodbCombineConsumedCapacity([]*dynamodb.ConsumedCapacity{output.ConsumedCapacity})
	}
	cmd.EndWithCost(cost, CmdResultOk, CmdResultError, err)
	return output, err
}

// QueryPages overrides dynamodb.DynamoDB/QueryPages to log execution metrics.
func (dp *DynamoDbProxy) QueryPages(input *dynamodb.QueryInput, fn func(*dynamodb.QueryOutput, bool) bool) error {
	return dp.metricsQueryPages(true, nil, input, fn)
}

// QueryPagesWithContext overrides dynamodb.DynamoDB/QueryPagesWithContext to log execution metrics.
func (dp *DynamoDbProxy) QueryPagesWithContext(ctx aws.Context, input *dynamodb.QueryInput, fn func(*dynamodb.QueryOutput, bool) bool, opts ...request.Option) error {
	return dp.metricsQueryPages(false, ctx, input, fn, opts...)
}

func (dp *DynamoDbProxy) metricsQueryPages(withoutContext bool, ctx aws.Context, input *dynamodb.QueryInput, fn func(*dynamodb.QueryOutput, bool) bool, opts ...request.Option) (err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbQueryItems, input
	cost := 0.0
	if withoutContext {
		err = dp.DynamoDB.QueryPages(input, func(output *dynamodb.QueryOutput, b bool) bool {
			if output != nil {
				cost += dynamodbCombineConsumedCapacity([]*dynamodb.ConsumedCapacity{output.ConsumedCapacity})
			}
			return fn(output, b)
		})
	} else {
		err = dp.DynamoDB.QueryPagesWithContext(ctx, input, func(output *dynamodb.QueryOutput, b bool) bool {
			if output != nil {
				cost += dynamodbCombineConsumedCapacity([]*dynamodb.ConsumedCapacity{output.ConsumedCapacity})
			}
			return fn(output, b)
		}, opts...)
	}
	cmd.EndWithCost(cost, CmdResultOk, CmdResultError, err)
	return err
}

// RestoreTableFromBackup overrides dynamodb.DynamoDB/RestoreTableFromBackup to log execution metrics.
func (dp *DynamoDbProxy) RestoreTableFromBackup(input *dynamodb.RestoreTableFromBackupInput) (*dynamodb.RestoreTableFromBackupOutput, error) {
	return dp.metricsRestoreTableFromBackup(true, nil, input)
}

// RestoreTableFromBackupWithContext overrides dynamodb.DynamoDB/RestoreTableFromBackupWithContext to log execution metrics.
func (dp *DynamoDbProxy) RestoreTableFromBackupWithContext(ctx aws.Context, input *dynamodb.RestoreTableFromBackupInput, opts ...request.Option) (*dynamodb.RestoreTableFromBackupOutput, error) {
	return dp.metricsRestoreTableFromBackup(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsRestoreTableFromBackup(withoutContext bool, ctx aws.Context, input *dynamodb.RestoreTableFromBackupInput, opts ...request.Option) (output *dynamodb.RestoreTableFromBackupOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbRestoreTableFromBackup, input
	if withoutContext {
		output, err = dp.DynamoDB.RestoreTableFromBackup(input)
	} else {
		output, err = dp.DynamoDB.RestoreTableFromBackupWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// RestoreTableToPointInTime overrides dynamodb.DynamoDB/RestoreTableToPointInTime to log execution metrics.
func (dp *DynamoDbProxy) RestoreTableToPointInTime(input *dynamodb.RestoreTableToPointInTimeInput) (*dynamodb.RestoreTableToPointInTimeOutput, error) {
	return dp.metricsRestoreTableToPointInTime(true, nil, input)
}

// RestoreTableToPointInTimeWithContext overrides dynamodb.DynamoDB/RestoreTableToPointInTimeWithContext to log execution metrics.
func (dp *DynamoDbProxy) RestoreTableToPointInTimeWithContext(ctx aws.Context, input *dynamodb.RestoreTableToPointInTimeInput, opts ...request.Option) (*dynamodb.RestoreTableToPointInTimeOutput, error) {
	return dp.metricsRestoreTableToPointInTime(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsRestoreTableToPointInTime(withoutContext bool, ctx aws.Context, input *dynamodb.RestoreTableToPointInTimeInput, opts ...request.Option) (output *dynamodb.RestoreTableToPointInTimeOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbRestoreTableToPIT, input
	if withoutContext {
		output, err = dp.DynamoDB.RestoreTableToPointInTime(input)
	} else {
		output, err = dp.DynamoDB.RestoreTableToPointInTimeWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// Scan overrides dynamodb.DynamoDB/Scan to log execution metrics.
func (dp *DynamoDbProxy) Scan(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	return dp.metricsScan(true, nil, input)
}

// ScanWithContext overrides dynamodb.DynamoDB/ScanWithContext to log execution metrics.
func (dp *DynamoDbProxy) ScanWithContext(ctx aws.Context, input *dynamodb.ScanInput, opts ...request.Option) (*dynamodb.ScanOutput, error) {
	return dp.metricsScan(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsScan(withoutContext bool, ctx aws.Context, input *dynamodb.ScanInput, opts ...request.Option) (output *dynamodb.ScanOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbScanItems, input
	if withoutContext {
		output, err = dp.DynamoDB.Scan(input)
	} else {
		output, err = dp.DynamoDB.ScanWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cost := 0.0
	if output != nil {
		cost = dynamodbCombineConsumedCapacity([]*dynamodb.ConsumedCapacity{output.ConsumedCapacity})
	}
	cmd.EndWithCost(cost, CmdResultOk, CmdResultError, err)
	return output, err
}

// ScanPages overrides dynamodb.DynamoDB/ScanPages to log execution metrics.
func (dp *DynamoDbProxy) ScanPages(input *dynamodb.ScanInput, fn func(*dynamodb.ScanOutput, bool) bool) error {
	return dp.metricsScanPages(true, nil, input, fn)
}

// ScanPagesWithContext overrides dynamodb.DynamoDB/ScanPagesWithContext to log execution metrics.
func (dp *DynamoDbProxy) ScanPagesWithContext(ctx aws.Context, input *dynamodb.ScanInput, fn func(*dynamodb.ScanOutput, bool) bool, opts ...request.Option) error {
	return dp.metricsScanPages(false, ctx, input, fn, opts...)
}

func (dp *DynamoDbProxy) metricsScanPages(withoutContext bool, ctx aws.Context, input *dynamodb.ScanInput, fn func(*dynamodb.ScanOutput, bool) bool, opts ...request.Option) (err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbScanItems, input
	cost := 0.0
	if withoutContext {
		err = dp.DynamoDB.ScanPages(input, func(output *dynamodb.ScanOutput, b bool) bool {
			if output != nil {
				cost += dynamodbCombineConsumedCapacity([]*dynamodb.ConsumedCapacity{output.ConsumedCapacity})
			}
			return fn(output, b)
		})
	} else {
		err = dp.DynamoDB.ScanPagesWithContext(ctx, input, func(output *dynamodb.ScanOutput, b bool) bool {
			if output != nil {
				cost += dynamodbCombineConsumedCapacity([]*dynamodb.ConsumedCapacity{output.ConsumedCapacity})
			}
			return fn(output, b)
		}, opts...)
	}
	cmd.EndWithCost(cost, CmdResultOk, CmdResultError, err)
	return err
}

// TagResource overrides dynamodb.DynamoDB/TagResource to log execution metrics.
func (dp *DynamoDbProxy) TagResource(input *dynamodb.TagResourceInput) (*dynamodb.TagResourceOutput, error) {
	return dp.metricsTagResource(true, nil, input)
}

// TagResourceWithContext overrides dynamodb.DynamoDB/TagResourceWithContext to log execution metrics.
func (dp *DynamoDbProxy) TagResourceWithContext(ctx aws.Context, input *dynamodb.TagResourceInput, opts ...request.Option) (*dynamodb.TagResourceOutput, error) {
	return dp.metricsTagResource(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsTagResource(withoutContext bool, ctx aws.Context, input *dynamodb.TagResourceInput, opts ...request.Option) (output *dynamodb.TagResourceOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbTagResource, input
	if withoutContext {
		output, err = dp.DynamoDB.TagResource(input)
	} else {
		output, err = dp.DynamoDB.TagResourceWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// TransactGetItems overrides dynamodb.DynamoDB/TransactGetItems to log execution metrics.
func (dp *DynamoDbProxy) TransactGetItems(input *dynamodb.TransactGetItemsInput) (*dynamodb.TransactGetItemsOutput, error) {
	return dp.metricsTransactGetItems(true, nil, input)
}

// TransactGetItemsWithContext overrides dynamodb.DynamoDB/TransactGetItemsWithContext to log execution metrics.
func (dp *DynamoDbProxy) TransactGetItemsWithContext(ctx aws.Context, input *dynamodb.TransactGetItemsInput, opts ...request.Option) (*dynamodb.TransactGetItemsOutput, error) {
	return dp.metricsTransactGetItems(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsTransactGetItems(withoutContext bool, ctx aws.Context, input *dynamodb.TransactGetItemsInput, opts ...request.Option) (output *dynamodb.TransactGetItemsOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbTransactGetItems, input
	if withoutContext {
		output, err = dp.DynamoDB.TransactGetItems(input)
	} else {
		output, err = dp.DynamoDB.TransactGetItemsWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cost := 0.0
	if output != nil {
		cost = dynamodbCombineConsumedCapacity(output.ConsumedCapacity)
	}
	cmd.EndWithCost(cost, CmdResultOk, CmdResultError, err)
	return output, err
}

// TransactWriteItems overrides dynamodb.DynamoDB/TransactWriteItems to log execution metrics.
func (dp *DynamoDbProxy) TransactWriteItems(input *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
	return dp.metricsTransactWriteItems(true, nil, input)
}

// TransactWriteItemsWithContext overrides dynamodb.DynamoDB/TransactWriteItemsWithContext to log execution metrics.
func (dp *DynamoDbProxy) TransactWriteItemsWithContext(ctx aws.Context, input *dynamodb.TransactWriteItemsInput, opts ...request.Option) (*dynamodb.TransactWriteItemsOutput, error) {
	return dp.metricsTransactWriteItems(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsTransactWriteItems(withoutContext bool, ctx aws.Context, input *dynamodb.TransactWriteItemsInput, opts ...request.Option) (output *dynamodb.TransactWriteItemsOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbTransactWriteItems, input
	if withoutContext {
		output, err = dp.DynamoDB.TransactWriteItems(input)
	} else {
		output, err = dp.DynamoDB.TransactWriteItemsWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cost := 0.0
	if output != nil {
		cost = dynamodbCombineConsumedCapacity(output.ConsumedCapacity)
	}
	cmd.EndWithCost(cost, CmdResultOk, CmdResultError, err)
	return output, err
}

// UntagResource overrides dynamodb.DynamoDB/UntagResource to log execution metrics.
func (dp *DynamoDbProxy) UntagResource(input *dynamodb.UntagResourceInput) (*dynamodb.UntagResourceOutput, error) {
	return dp.metricsUntagResource(true, nil, input)
}

// UntagResourceWithContext overrides dynamodb.DynamoDB/UntagResourceWithContext to log execution metrics.
func (dp *DynamoDbProxy) UntagResourceWithContext(ctx aws.Context, input *dynamodb.UntagResourceInput, opts ...request.Option) (*dynamodb.UntagResourceOutput, error) {
	return dp.metricsUntagResource(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsUntagResource(withoutContext bool, ctx aws.Context, input *dynamodb.UntagResourceInput, opts ...request.Option) (output *dynamodb.UntagResourceOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbUntagResource, input
	if withoutContext {
		output, err = dp.DynamoDB.UntagResource(input)
	} else {
		output, err = dp.DynamoDB.UntagResourceWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// UpdateContinuousBackups overrides dynamodb.DynamoDB/UpdateContinuousBackups to log execution metrics.
func (dp *DynamoDbProxy) UpdateContinuousBackups(input *dynamodb.UpdateContinuousBackupsInput) (*dynamodb.UpdateContinuousBackupsOutput, error) {
	return dp.metricsUpdateContinuousBackups(true, nil, input)
}

// UpdateContinuousBackupsWithContext overrides dynamodb.DynamoDB/UpdateContinuousBackupsWithContext to log execution metrics.
func (dp *DynamoDbProxy) UpdateContinuousBackupsWithContext(ctx aws.Context, input *dynamodb.UpdateContinuousBackupsInput, opts ...request.Option) (*dynamodb.UpdateContinuousBackupsOutput, error) {
	return dp.metricsUpdateContinuousBackups(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsUpdateContinuousBackups(withoutContext bool, ctx aws.Context, input *dynamodb.UpdateContinuousBackupsInput, opts ...request.Option) (output *dynamodb.UpdateContinuousBackupsOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbUpdateContinuousBackups, input
	if withoutContext {
		output, err = dp.DynamoDB.UpdateContinuousBackups(input)
	} else {
		output, err = dp.DynamoDB.UpdateContinuousBackupsWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// UpdateContributorInsights overrides dynamodb.DynamoDB/UpdateContributorInsights to log execution metrics.
func (dp *DynamoDbProxy) UpdateContributorInsights(input *dynamodb.UpdateContributorInsightsInput) (*dynamodb.UpdateContributorInsightsOutput, error) {
	return dp.metricsUpdateContributorInsights(true, nil, input)
}

// UpdateContributorInsightsWithContext overrides dynamodb.DynamoDB/UpdateContributorInsightsWithContext to log execution metrics.
func (dp *DynamoDbProxy) UpdateContributorInsightsWithContext(ctx aws.Context, input *dynamodb.UpdateContributorInsightsInput, opts ...request.Option) (*dynamodb.UpdateContributorInsightsOutput, error) {
	return dp.metricsUpdateContributorInsights(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsUpdateContributorInsights(withoutContext bool, ctx aws.Context, input *dynamodb.UpdateContributorInsightsInput, opts ...request.Option) (output *dynamodb.UpdateContributorInsightsOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbUpdateContributorInsights, input
	if withoutContext {
		output, err = dp.DynamoDB.UpdateContributorInsights(input)
	} else {
		output, err = dp.DynamoDB.UpdateContributorInsightsWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// UpdateGlobalTable overrides dynamodb.DynamoDB/UpdateGlobalTable to log execution metrics.
func (dp *DynamoDbProxy) UpdateGlobalTable(input *dynamodb.UpdateGlobalTableInput) (*dynamodb.UpdateGlobalTableOutput, error) {
	return dp.metricsUpdateGlobalTable(true, nil, input)
}

// UpdateGlobalTableWithContext overrides dynamodb.DynamoDB/UpdateGlobalTableWithContext to log execution metrics.
func (dp *DynamoDbProxy) UpdateGlobalTableWithContext(ctx aws.Context, input *dynamodb.UpdateGlobalTableInput, opts ...request.Option) (*dynamodb.UpdateGlobalTableOutput, error) {
	return dp.metricsUpdateGlobalTable(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsUpdateGlobalTable(withoutContext bool, ctx aws.Context, input *dynamodb.UpdateGlobalTableInput, opts ...request.Option) (output *dynamodb.UpdateGlobalTableOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbUpdateGlobalTable, input
	if withoutContext {
		output, err = dp.DynamoDB.UpdateGlobalTable(input)
	} else {
		output, err = dp.DynamoDB.UpdateGlobalTableWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// UpdateGlobalTableSettings overrides dynamodb.DynamoDB/UpdateGlobalTableSettings to log execution metrics.
func (dp *DynamoDbProxy) UpdateGlobalTableSettings(input *dynamodb.UpdateGlobalTableSettingsInput) (*dynamodb.UpdateGlobalTableSettingsOutput, error) {
	return dp.metricsUpdateGlobalTableSettings(true, nil, input)
}

// UpdateGlobalTableSettingsWithContext overrides dynamodb.DynamoDB/UpdateGlobalTableSettingsWithContext to log execution metrics.
func (dp *DynamoDbProxy) UpdateGlobalTableSettingsWithContext(ctx aws.Context, input *dynamodb.UpdateGlobalTableSettingsInput, opts ...request.Option) (*dynamodb.UpdateGlobalTableSettingsOutput, error) {
	return dp.metricsUpdateGlobalTableSettings(false, nil, input, opts...)
}

func (dp *DynamoDbProxy) metricsUpdateGlobalTableSettings(withoutContext bool, ctx aws.Context, input *dynamodb.UpdateGlobalTableSettingsInput, opts ...request.Option) (output *dynamodb.UpdateGlobalTableSettingsOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbUpdateGlobalTableSettings, input
	if withoutContext {
		output, err = dp.DynamoDB.UpdateGlobalTableSettings(input)
	} else {
		output, err = dp.DynamoDB.UpdateGlobalTableSettingsWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// UpdateItem overrides dynamodb.DynamoDB/UpdateItem to log execution metrics.
func (dp *DynamoDbProxy) UpdateItem(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	return dp.metricsUpdateItem(true, nil, input)
}

// UpdateItemWithContext overrides dynamodb.DynamoDB/UpdateItemWithContext to log execution metrics.
func (dp *DynamoDbProxy) UpdateItemWithContext(ctx aws.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	return dp.metricsUpdateItem(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsUpdateItem(withoutContext bool, ctx aws.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (output *dynamodb.UpdateItemOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbUpdateItem, input
	if withoutContext {
		output, err = dp.DynamoDB.UpdateItem(input)
	} else {
		output, err = dp.DynamoDB.UpdateItemWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cost := 0.0
	if output != nil {
		cost = dynamodbCombineConsumedCapacity([]*dynamodb.ConsumedCapacity{output.ConsumedCapacity})
	}
	cmd.EndWithCost(cost, CmdResultOk, CmdResultError, err)
	return output, err
}

// UpdateTable overrides dynamodb.DynamoDB/UpdateTable to log execution metrics.
func (dp *DynamoDbProxy) UpdateTable(input *dynamodb.UpdateTableInput) (*dynamodb.UpdateTableOutput, error) {
	return dp.metricsUpdateTable(true, nil, input)
}

// UpdateTableWithContext overrides dynamodb.DynamoDB/UpdateTableWithContext to log execution metrics.
func (dp *DynamoDbProxy) UpdateTableWithContext(ctx aws.Context, input *dynamodb.UpdateTableInput, opts ...request.Option) (*dynamodb.UpdateTableOutput, error) {
	return dp.metricsUpdateTable(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsUpdateTable(withoutContext bool, ctx aws.Context, input *dynamodb.UpdateTableInput, opts ...request.Option) (output *dynamodb.UpdateTableOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbUpdateTable, input
	if withoutContext {
		output, err = dp.DynamoDB.UpdateTable(input)
	} else {
		output, err = dp.DynamoDB.UpdateTableWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// UpdateTableReplicaAutoScaling overrides dynamodb.DynamoDB/UpdateTableReplicaAutoScaling to log execution metrics.
func (dp *DynamoDbProxy) UpdateTableReplicaAutoScaling(input *dynamodb.UpdateTableReplicaAutoScalingInput) (*dynamodb.UpdateTableReplicaAutoScalingOutput, error) {
	return dp.metricsUpdateTableReplicaAutoScaling(true, nil, input)
}

// UpdateTableReplicaAutoScalingWithContext overrides dynamodb.DynamoDB/UpdateTableReplicaAutoScalingWithContext to log execution metrics.
func (dp *DynamoDbProxy) UpdateTableReplicaAutoScalingWithContext(ctx aws.Context, input *dynamodb.UpdateTableReplicaAutoScalingInput, opts ...request.Option) (*dynamodb.UpdateTableReplicaAutoScalingOutput, error) {
	return dp.metricsUpdateTableReplicaAutoScaling(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsUpdateTableReplicaAutoScaling(withoutContext bool, ctx aws.Context, input *dynamodb.UpdateTableReplicaAutoScalingInput, opts ...request.Option) (output *dynamodb.UpdateTableReplicaAutoScalingOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbUpdateTableReplicaAutoScaling, input
	if withoutContext {
		output, err = dp.DynamoDB.UpdateTableReplicaAutoScaling(input)
	} else {
		output, err = dp.DynamoDB.UpdateTableReplicaAutoScalingWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}

// UpdateTimeToLive overrides dynamodb.DynamoDB/UpdateTimeToLive to log execution metrics.
func (dp *DynamoDbProxy) UpdateTimeToLive(input *dynamodb.UpdateTimeToLiveInput) (*dynamodb.UpdateTimeToLiveOutput, error) {
	return dp.metricsUpdateTimeToLive(true, nil, input)
}

// UpdateTimeToLiveWithContext overrides dynamodb.DynamoDB/UpdateTimeToLiveWithContext to log execution metrics.
func (dp *DynamoDbProxy) UpdateTimeToLiveWithContext(ctx aws.Context, input *dynamodb.UpdateTimeToLiveInput, opts ...request.Option) (*dynamodb.UpdateTimeToLiveOutput, error) {
	return dp.metricsUpdateTimeToLive(false, ctx, input, opts...)
}

func (dp *DynamoDbProxy) metricsUpdateTimeToLive(withoutContext bool, ctx aws.Context, input *dynamodb.UpdateTimeToLiveInput, opts ...request.Option) (output *dynamodb.UpdateTimeToLiveOutput, err error) {
	cmd := dp.adc.NewCmdExecInfo()
	defer func() {
		defer dp.adc.LogMetrics(MetricsCatAll, cmd)
		defer dp.adc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = cmdDynamodbUpdateTTL, input
	if withoutContext {
		output, err = dp.DynamoDB.UpdateTimeToLive(input)
	} else {
		output, err = dp.DynamoDB.UpdateTimeToLiveWithContext(ctx, input, opts...)
	}
	cmd.CmdResponse = output
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return output, err
}
