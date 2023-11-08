// See https://aka.ms/new-console-template for more information
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using DynamoDB.InMemoryTest;

Console.WriteLine("Hello, World!");


var dynamoDb = new InMemoryDynamoDb();
var dynamoDbContext = new DynamoDBContext(dynamoDb);

await dynamoDb.CreateTableAsync(new CreateTableRequest
{
    TableName = "test-table",
    KeySchema = new() { new KeySchemaElement("Key", KeyType.HASH), new KeySchemaElement("Range", KeyType.RANGE) },
    AttributeDefinitions = new() { new AttributeDefinition("Key", ScalarAttributeType.S), new AttributeDefinition("Range", ScalarAttributeType.S) }
});

await dynamoDbContext.SaveAsync(new Whatever("id", "range"));

var w = await dynamoDbContext.LoadAsync<Whatever>("id", "range");

await dynamoDbContext.DeleteAsync(w);

w = await dynamoDbContext.LoadAsync<Whatever>("id", "range");


var batchWrite = dynamoDbContext.CreateBatchWrite<Whatever>();
batchWrite.AddPutItems(new Whatever[] { new("1", "1"), new("1", "2"), new("2", "2") });
await batchWrite.ExecuteAsync();

var items = await dynamoDbContext.QueryAsync<Whatever>("1").GetRemainingAsync();

int i = 0;

[DynamoDBTable("test-table")]
class Whatever
{
    public Whatever() { }

    public Whatever(string key, string range)
    {
        Key = key;
        Range = range;
    }

    [DynamoDBHashKey] public string Key { get; set; }
    [DynamoDBRangeKey] public string Range { get; set; }
}