using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Amazon.Runtime.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Threading.Tasks;

namespace DynamoDB.InMemoryTest
{
    public class InMemoryDynamoDb : AmazonDynamoDBClient
    {
        public InMemoryDynamoDb() : base("test-access-key", "test-aws-secret", new AmazonDynamoDBConfig
        {
            RegionEndpoint = RegionEndpoint.EUCentral1
        })
        {

        }

        protected override void CustomizeRuntimePipeline(RuntimePipeline pipeline)
        {
            pipeline.AddHandler(new InMemoryHandler());
        }


        class InMemoryHandler : PipelineHandler
        {
            private List<InMemoryTable> _tables = new();

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

                    _ => throw new NotImplementedException()
                };
            }

            private UpdateItemResponse UpdateItem(UpdateItemRequest updateItemRequest)
            {
                var table = _tables.First(t => t.TableDescription.TableName == updateItemRequest.TableName);
                table.Items.Add(updateItemRequest.Key);
                return new UpdateItemResponse { HttpStatusCode = HttpStatusCode.OK };
            }

            private GetItemResponse GetItem(GetItemRequest getItemRequest)
            {
                var table = _tables.First(t => t.TableDescription.TableName == getItemRequest.TableName);

                return new GetItemResponse
                {
                    HttpStatusCode = HttpStatusCode.OK,
                    IsItemSet = true,
                    Item = table.Items.First(i => getItemRequest.Key.All(k => JsonSerializer.Serialize(i[k.Key]) == JsonSerializer.Serialize(k.Value)))
                };
            }

            private PutItemResponse PutItem(PutItemRequest putItemRequest)
            {
                var table = _tables.First(t => t.TableDescription.TableName == putItemRequest.TableName);
                table.Items.Add(putItemRequest.Item);
                return new PutItemResponse { HttpStatusCode = HttpStatusCode.OK };
            }

            private CreateTableResponse CreateTable(CreateTableRequest createTableRequest)
            {
                var table = createTableRequest.ConvertTo<TableDescription>();
                _tables.Add(new InMemoryTable { TableDescription = table });

                return new CreateTableResponse
                {
                    HttpStatusCode = HttpStatusCode.Created,
                    TableDescription = table
                };
            }

            private DescribeTableResponse DescribeTable(DescribeTableRequest describeTableRequest)
            {
                var table = _tables.FirstOrDefault(t => t.TableDescription.TableName == describeTableRequest.TableName);
                return table is null
                    ? new DescribeTableResponse { HttpStatusCode = HttpStatusCode.NotFound }
                    : new DescribeTableResponse { HttpStatusCode = HttpStatusCode.OK, Table = table.TableDescription };
            }
        }
    }
}
