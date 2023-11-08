using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Amazon.Runtime.Internal;
using System;
using System.Collections.Concurrent;
using System.Net;
using System.Threading.Tasks;

namespace DynamoDB.InMemoryTest
{
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

                _ => throw new NotImplementedException()
            };
        }

        private DeleteItemResponse DeleteItem(DeleteItemRequest request)
        {
            var table = _tables[request.TableName];
            var item = table.GetItem(request.Key);
            if (item != null)
            {
                table.Items.Remove(item);
            }

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
            table.Items.Add(request.Item);
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
    }
}
