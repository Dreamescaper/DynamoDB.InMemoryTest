using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using NUnit.Framework;

namespace DynamoDB.InMemoryTest.Tests;

public class DynamoDBContextTests
{
    readonly InMemoryDynamoDb _client = new();
    readonly DynamoDBContext _context;

    public DynamoDBContextTests()
    {
        _context = new(_client);

        _client.CreateTableFromType<ModelWithHashKey>();
        _client.CreateTableFromType<ModelWithHashKeyAndRangeKey>();
        _client.CreateTableFromType<ModelWithIndex>();
    }

    [Test]
    public async Task SaveAsync_UpdateItem()
    {
        var item = new ModelWithHashKey { Id = 5, Data = "test-data-1" };
        await _context.SaveAsync(item);

        item.Data = "updated-data";
        await _context.SaveAsync(item);

        var retrievedItem = await _context.LoadAsync<ModelWithHashKey>(item.Id);
        Assert.That(retrievedItem, Is.EqualTo(item));
    }

    [Test]
    public async Task LoadAsync_ByHashKey()
    {
        var item = new ModelWithHashKey { Id = 5, Data = "test-data-1" };
        var otherItem = new ModelWithHashKey { Id = 6, Data = "test-data-2" };
        await _context.SaveItemsAsync(item, otherItem);

        var retrievedItem = await _context.LoadAsync<ModelWithHashKey>(item.Id);
        Assert.That(retrievedItem, Is.EqualTo(item));
    }

    [Test]
    public async Task LoadAsync_ByHashKeyAndRangeKey()
    {
        var item = new ModelWithHashKeyAndRangeKey { Id = 5, Range = "a", Data = "test-data-1" };
        var otherItem = new ModelWithHashKeyAndRangeKey { Id = 6, Range = "b", Data = "test-data-2" };
        await _context.SaveItemsAsync(item, otherItem);

        var retrievedItem = await _context.LoadAsync<ModelWithHashKeyAndRangeKey>(item.Id, item.Range);

        Assert.That(retrievedItem, Is.EqualTo(item));
    }

    [Test]
    public async Task DeleteAsync_ByHashKey()
    {
        var item = new ModelWithHashKey { Id = 5, Data = "test-data-1" };
        await _context.SaveAsync(item);

        await _context.DeleteAsync(item);

        var result = await _context.LoadAsync<ModelWithHashKey>(item.Id);
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task DeleteAsync_ByHashKeyAndRangeKey()
    {
        var item = new ModelWithHashKeyAndRangeKey { Id = 5, Range = "a", Data = "test-data-1" };
        await _context.SaveAsync(item);

        await _context.DeleteAsync(item);

        var result = await _context.LoadAsync<ModelWithHashKeyAndRangeKey>(item.Id, item.Range);
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task QueryAsync_ByHashKey()
    {
        var item1 = new ModelWithHashKeyAndRangeKey { Id = 5, Range = "a", Data = "test-data-1" };
        var item2 = new ModelWithHashKeyAndRangeKey { Id = 5, Range = "b", Data = "test-data-2" };
        var otherItem = new ModelWithHashKeyAndRangeKey { Id = 6, Range = "b", Data = "test-data-3" };
        await _context.SaveItemsAsync(item1, item2, otherItem);

        var retrievedItems = await _context.QueryAsync<ModelWithHashKeyAndRangeKey>(5L).GetRemainingAsync();

        Assert.That(retrievedItems, Is.EquivalentTo(new[] { item1, item2 }));
    }

    [Test]
    public async Task QueryAsync_WithKeyConditions_StartsWith()
    {
        var item1 = new ModelWithHashKeyAndRangeKey { Id = 4, Range = "abc", Data = "test-data-1" };
        var item2 = new ModelWithHashKeyAndRangeKey { Id = 5, Range = "bcd", Data = "test-data-2" };
        var item3 = new ModelWithHashKeyAndRangeKey { Id = 5, Range = "ab", Data = "test-data-3" };
        var item4 = new ModelWithHashKeyAndRangeKey { Id = 5, Range = "abcd", Data = "test-data-4" };
        await _context.SaveItemsAsync(item1, item2, item3, item4);

        var retrievedItems = await _context
            .QueryAsync<ModelWithHashKeyAndRangeKey>(5L, QueryOperator.BeginsWith, ["a"])
            .GetRemainingAsync();

        Assert.That(retrievedItems, Is.EquivalentTo(new[] { item3, item4 }));
    }

    [Test]
    public async Task QueryAsync_BySecondaryIndex()
    {
        var item1 = new ModelWithIndex { Id = "4", Index = "a", Data = "test-data-1" };
        var item2 = new ModelWithIndex { Id = "5", Index = "b", Data = "test-data-2" };
        var item3 = new ModelWithIndex { Id = "6", Index = "b", Data = "test-data-3" };
        await _context.SaveItemsAsync(item1, item2, item3);

        var retrievedItems = await _context
            .QueryAsync<ModelWithIndex>("b", new DynamoDBOperationConfig
            {
                IndexName = "GSI"
            })
            .GetRemainingAsync();

        Assert.That(retrievedItems, Is.EquivalentTo(new[] { item2, item3 }));
    }

    [Test]
    public async Task ScanAsync_WithoutFilters()
    {
        var item1 = new ModelWithIndex { Id = "4", Index = "a", Data = "test-data-1" };
        var item2 = new ModelWithIndex { Id = "5", Index = "b", Data = "test-data-2" };
        var item3 = new ModelWithIndex { Id = "6", Index = "b", Data = "test-data-3" };
        await _context.SaveItemsAsync(item1, item2, item3);

        var retrievedItems = await _context.ScanAsync<ModelWithIndex>([]).GetRemainingAsync();

        Assert.That(retrievedItems, Is.EquivalentTo(new[] { item1, item2, item3 }));
    }

    [Test]
    public async Task BatchGet_ByHashKey()
    {
        var item1 = new ModelWithHashKey { Id = 4, Data = "test-data-1" };
        var item2 = new ModelWithHashKey { Id = 5, Data = "test-data-2" };
        var item3 = new ModelWithHashKey { Id = 6, Data = "test-data-3" };
        await _context.SaveItemsAsync(item1, item2, item3);

        var batchRead = _context.CreateBatchGet<ModelWithHashKey>();
        batchRead.AddKey(5L);
        batchRead.AddKey(6L);
        await batchRead.ExecuteAsync();

        Assert.That(batchRead.Results, Is.EquivalentTo(new[] { item2, item3 }));
    }

    [Test]
    public async Task BatchGet_ByHashKeyAndRangeKey()
    {
        var item1 = new ModelWithHashKeyAndRangeKey { Id = 5, Range = "a", Data = "test-data-1" };
        var item2 = new ModelWithHashKeyAndRangeKey { Id = 5, Range = "b", Data = "test-data-2" };
        var item3 = new ModelWithHashKeyAndRangeKey { Id = 6, Range = "b", Data = "test-data-3" };
        await _context.SaveItemsAsync(item1, item2, item3);

        var batchRead = _context.CreateBatchGet<ModelWithHashKeyAndRangeKey>();
        batchRead.AddKey(5L, "a");
        batchRead.AddKey(5L, "b");
        await batchRead.ExecuteAsync();

        Assert.That(batchRead.Results, Is.EquivalentTo(new[] { item1, item2 }));
    }

    [Test]
    public async Task BatchWrite_ByHashKeyAndRangeKey()
    {
        var item1 = new ModelWithHashKeyAndRangeKey { Id = 5, Range = "a", Data = "test-data-1" };
        var item2 = new ModelWithHashKeyAndRangeKey { Id = 5, Range = "b", Data = "test-data-2" };
        var existingItem = new ModelWithHashKeyAndRangeKey { Id = 6, Range = "b", Data = "test-data-3" };
        await _context.SaveAsync(existingItem);

        var batchWrite = _context.CreateBatchWrite<ModelWithHashKeyAndRangeKey>();
        batchWrite.AddPutItems([item1, item2]);
        batchWrite.AddDeleteItem(existingItem);
        await batchWrite.ExecuteAsync();

        var retrievedItems = await _context.ScanAsync<ModelWithHashKeyAndRangeKey>([]).GetRemainingAsync();
        Assert.That(retrievedItems, Is.EquivalentTo(new[] { item1, item2 }));
    }
}
