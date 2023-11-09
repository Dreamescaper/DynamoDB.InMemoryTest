using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Amazon.Runtime.Internal;
using DynamoDB.InMemoryTest.Extensions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace DynamoDB.InMemoryTest;

internal class InMemoryPipelineHandler : PipelineHandler
{
    private readonly ConcurrentDictionary<string, InMemoryTable> _tables = new();

    public override Task<T> InvokeAsync<T>(IExecutionContext executionContext)
    {
        InvokeSync(executionContext);
        return Task.FromResult((T)executionContext.ResponseContext.Response);
    }

    public override void InvokeSync(IExecutionContext executionContext)
    {
        executionContext.ResponseContext.Response = executionContext.RequestContext.OriginalRequest switch
        {
            CreateTableRequest createTableRequest => CreateTable(createTableRequest),
            DescribeTableRequest describeTableRequest => DescribeTable(describeTableRequest),
            GetItemRequest getItemRequest => GetItem(getItemRequest),
            PutItemRequest putItemRequest => PutItem(putItemRequest),
            UpdateItemRequest updateItemRequest => UpdateItem(updateItemRequest),
            DeleteItemRequest deleteItemRequest => DeleteItem(deleteItemRequest),
            BatchWriteItemRequest batchWriteItemRequest => BatchWriteItem(batchWriteItemRequest),
            BatchGetItemRequest batchGetItemRequest => BatchGetItem(batchGetItemRequest),
            QueryRequest queryRequest => Query(queryRequest),
            ScanRequest scanRequest => Scan(scanRequest),

            _ => throw new NotImplementedException($"Request {executionContext.RequestContext.RequestName} is not supported by InMemory DynamoDB.")
        };
    }

    private ScanResponse Scan(ScanRequest scanRequest)
    {
        var table = _tables[scanRequest.TableName];
        var items = table.Items;

        return new ScanResponse
        {
            HttpStatusCode = HttpStatusCode.OK,
            Count = items.Count,
            ScannedCount = items.Count,
            Items = items
        };
    }

    private BatchGetItemResponse BatchGetItem(BatchGetItemRequest batchGetItemRequest)
    {
        var items = batchGetItemRequest.RequestItems.Select(i =>
        {
            var table = _tables[i.Key];
            var items = i.Value.Keys.Select(table.GetItem).ToList();
            items = ProjectAttributes(items, i.Value.AttributesToGet);
            return KeyValuePair.Create(i.Key, items);
        });

        return new BatchGetItemResponse
        {
            HttpStatusCode = HttpStatusCode.OK,
            Responses = items.ToDictionary()
        };
    }

    private QueryResponse Query(QueryRequest request)
    {
        var table = _tables[request.TableName];

        var items = table.QueryByKey(request.KeyConditions, request.IndexName);
        items = ProjectAttributes(items, request.AttributesToGet);

        return new QueryResponse
        {
            HttpStatusCode = HttpStatusCode.OK,
            Items = items,
        };
    }

    private BatchWriteItemResponse BatchWriteItem(BatchWriteItemRequest request)
    {
        foreach (var (tableName, writeRequests) in request.RequestItems)
        {
            var table = _tables[tableName];
            foreach (var write in writeRequests)
            {
                if (write.PutRequest != null)
                {
                    table.PutItem(write.PutRequest.Item);
                }
                else if (write.DeleteRequest != null)
                {
                    table.DeleteItem(write.DeleteRequest.Key);
                }
            }
        }

        return new BatchWriteItemResponse { HttpStatusCode = HttpStatusCode.OK };
    }

    private DeleteItemResponse DeleteItem(DeleteItemRequest request)
    {
        var table = _tables[request.TableName];
        table.DeleteItem(request.Key);

        return new DeleteItemResponse { HttpStatusCode = HttpStatusCode.OK };
    }

    private UpdateItemResponse UpdateItem(UpdateItemRequest request)
    {
        var table = _tables[request.TableName];
        var item = table.GetItem(request.Key);
        if (item == null)
        {
            item = request.Key;
            foreach (var (key, valueUpdate) in request.AttributeUpdates ?? [])
            {
                if (valueUpdate.Action != AttributeAction.DELETE)
                {
                    item[key] = valueUpdate.Value;
                }
            }

            table.Items.Add(item);
        }
        else
        {
            foreach (var (key, valueUpdate) in request.AttributeUpdates ?? [])
            {
                if (valueUpdate.Action == AttributeAction.DELETE)
                {
                    item.Remove(key);
                }
                else
                {
                    item[key] = valueUpdate.Value;
                }
            }
        }

        return new UpdateItemResponse { HttpStatusCode = HttpStatusCode.OK };
    }

    private GetItemResponse GetItem(GetItemRequest request)
    {
        var table = _tables[request.TableName];
        var item = table.GetItem(request.Key);
        item = ProjectAttributes(item, request.AttributesToGet);

        if (item != null && request.AttributesToGet?.Any() == true)
        {
            item = item.Where(i => request.AttributesToGet.Contains(i.Key)).ToDictionary();
        }

        return new GetItemResponse
        {
            HttpStatusCode = HttpStatusCode.OK,
            IsItemSet = item != null,
            Item = item
        };
    }

    private PutItemResponse PutItem(PutItemRequest request)
    {
        var table = _tables[request.TableName];
        table.PutItem(request.Item);
        return new PutItemResponse { HttpStatusCode = HttpStatusCode.OK };
    }

    private CreateTableResponse CreateTable(CreateTableRequest request)
    {
        var table = request.ConvertTo<TableDescription>();
        _tables[table.TableName] = new InMemoryTable { TableDescription = table };

        return new CreateTableResponse
        {
            HttpStatusCode = HttpStatusCode.Created,
            TableDescription = table
        };
    }

    private DescribeTableResponse DescribeTable(DescribeTableRequest request)
    {
        return _tables.TryGetValue(request.TableName, out var table)
            ? new DescribeTableResponse { HttpStatusCode = HttpStatusCode.OK, Table = table.TableDescription }
            : new DescribeTableResponse { HttpStatusCode = HttpStatusCode.NotFound };
    }

    private static Dictionary<string, AttributeValue> ProjectAttributes(Dictionary<string, AttributeValue> item, List<string> attributesToGet)
    {
        if (item != null && attributesToGet?.Any() == true)
        {
            item = item.Where(i => attributesToGet.Contains(i.Key)).ToDictionary();
        }
        return item;
    }

    private static List<Dictionary<string, AttributeValue>> ProjectAttributes(List<Dictionary<string, AttributeValue>> items, List<string> attributesToGet)
    {
        if (attributesToGet?.Any() == true && items.Any())
        {
            items = items.Select(i => ProjectAttributes(i, attributesToGet)).ToList();
        }
        return items;
    }
}
