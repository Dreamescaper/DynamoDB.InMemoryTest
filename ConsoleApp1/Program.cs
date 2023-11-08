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
    KeySchema = new() { new KeySchemaElement("Key", KeyType.HASH) },
    AttributeDefinitions = new() { new AttributeDefinition("Key", ScalarAttributeType.S) }
});

await dynamoDbContext.SaveAsync(new Whatever { Key = "id" });

var w = await dynamoDbContext.LoadAsync<Whatever>("id");

[DynamoDBTable("test-table")]
class Whatever
{
    [DynamoDBHashKey]
    public string Key { get; set; }
}