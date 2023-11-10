using Amazon.DynamoDBv2.DataModel;

namespace DynamoDB.InMemoryTest.Tests;

public static class DynamoDBContextExtensions
{
    public static async Task SaveItemsAsync<T>(this DynamoDBContext context, params T[] items)
    {
        var batchWrite = context.CreateBatchWrite<T>();
        batchWrite.AddPutItems(items);
        await batchWrite.ExecuteAsync();
    }
}
