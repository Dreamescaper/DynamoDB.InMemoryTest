using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
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

    [Test]
    public async Task SelectAllWithNoConditions()
    {
        var item1 = new ModelWithHashKey { Id = 4, Data = "test-data-1" };
        var item2 = new ModelWithHashKey { Id = 5, Data = "test-data-2" };
        var item3 = new ModelWithHashKey { Id = 6, Data = "test-data-3" };
        await _context.SaveItemsAsync(item1, item2, item3);

        var result = await _client.ExecuteStatementAsync(new ExecuteStatementRequest
        {
            Statement = "SELECT * FROM \"table-with-hash\""
        });

        var items = result.Items.Select(Document.FromAttributeMap).Select(_context.FromDocument<ModelWithHashKey>).ToList();
        Assert.That(items, Is.EquivalentTo(new[] { item1, item2, item3 }));
    }

    [Test]
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
                WHERE Id IN [5, 6]
                """
        });

        var items = result.Items.Select(Document.FromAttributeMap).Select(_context.FromDocument<ModelWithHashKey>).ToList();
        Assert.That(items, Is.EquivalentTo(new[] { item2, item3 }));
    }

    [Test]
    public async Task SelectWithAndCondition()
    {
        var item1 = new ModelWithHashKey { Id = 4, Data = "test-data-2" };
        var item2 = new ModelWithHashKey { Id = 5, Data = "test-data-2" };
        var item3 = new ModelWithHashKey { Id = 6, Data = "test-data-3" };
        await _context.SaveItemsAsync(item1, item2, item3);

        var result = await _client.ExecuteStatementAsync(new ExecuteStatementRequest
        {
            Statement = """
                SELECT Data FROM "table-with-hash"
                WHERE Id IN [5, 6] AND Data = 'test-data-2'
                """
        });

        var itemAttribute = result.Items.Single().Single();
        Assert.That(itemAttribute.Key, Is.EqualTo("Data"));
        Assert.That(itemAttribute.Value.S, Is.EqualTo("test-data-2"));
    }

    [Test]
    public async Task SelectWithOrCondition()
    {
        var item1 = new ModelWithHashKey { Id = 4, Data = "test-data-1" };
        var item2 = new ModelWithHashKey { Id = 5, Data = "test-data-2" };
        var item3 = new ModelWithHashKey { Id = 6, Data = "test-data-3" };
        await _context.SaveItemsAsync(item1, item2, item3);

        var result = await _client.ExecuteStatementAsync(new ExecuteStatementRequest
        {
            Statement = """
                SELECT Id FROM "table-with-hash"
                WHERE Id=5 OR Data='test-data-1'
                """
        });

        var ids = result.Items.Select(s => s["Id"].N);
        Assert.That(ids, Is.EqualTo(new[] { "4", "5" }));
    }
}
