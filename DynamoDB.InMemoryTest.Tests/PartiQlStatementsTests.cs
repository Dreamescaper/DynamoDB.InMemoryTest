using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using NUnit.Framework;

namespace DynamoDB.InMemoryTest.Tests;

public class PartiQlStatementsTests
{
    readonly InMemoryDynamoDb _client = new();
    readonly DynamoDBContext _context;

    public PartiQlStatementsTests()
    {
        _context = new(_client);

        _client.CreateTableFromType<ModelWithHashKey>();
    }

    [Test, Ignore("not implemented")]
    public async Task SelectAllWithInCondition_HashKey()
    {
        var item1 = new ModelWithHashKey { Id = 4, Data = "test-data-1" };
        var item2 = new ModelWithHashKey { Id = 5, Data = "test-data-2" };
        var item3 = new ModelWithHashKey { Id = 6, Data = "test-data-3" };
        await _context.SaveItemsAsync(item1, item2, item3);

        var result = await _client.ExecuteStatementAsync(new ExecuteStatementRequest
        {
            Statement = """
                SELECT * FROM "table-with-hash"
                WHERE "Id" IN ['5','6']
                """
        });

        // TODO
    }
}
