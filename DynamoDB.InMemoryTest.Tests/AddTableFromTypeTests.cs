using Amazon.DynamoDBv2;
using NUnit.Framework;

namespace DynamoDB.InMemoryTest.Tests;

public class AddTableFromTypeTests
{
    readonly InMemoryDynamoDb _db = new();

    [Test]
    public async Task AddTableFromTypeWithStringIds()
    {
        _db.CreateTableFromType<TestTypeWithStringId>();

        var tableDescription = (await _db.DescribeTableAsync("table-with-string-id")).Table;

        var hashAttribute = tableDescription.KeySchema.Single(k => k.KeyType == KeyType.HASH).AttributeName;
        var hashType = tableDescription.AttributeDefinitions.Single(a => a.AttributeName == hashAttribute).AttributeType;
        Assert.That(hashAttribute, Is.EqualTo("HashId"));
        Assert.That(hashType, Is.EqualTo(ScalarAttributeType.S));

        var rangeAttribute = tableDescription.KeySchema.Single(k => k.KeyType == KeyType.RANGE).AttributeName;
        var rangeType = tableDescription.AttributeDefinitions.Single(a => a.AttributeName == rangeAttribute).AttributeType;
        Assert.That(rangeAttribute, Is.EqualTo("_rangeId"));
        Assert.That(rangeType, Is.EqualTo(ScalarAttributeType.S));

        var index = tableDescription.GlobalSecondaryIndexes.Single();
        Assert.That(index.IndexName, Is.EqualTo("GSI-Test"));

        var indexHashAttribute = index.KeySchema.Single(k => k.KeyType == KeyType.HASH).AttributeName;
        var indexHashType = tableDescription.AttributeDefinitions.Single(a => a.AttributeName == indexHashAttribute).AttributeType;
        Assert.That(indexHashAttribute, Is.EqualTo("IndexHashId"));
        Assert.That(indexHashType, Is.EqualTo(ScalarAttributeType.S));

        var indexRangeAttribute = index.KeySchema.Single(k => k.KeyType == KeyType.RANGE).AttributeName;
        var indexRangeType = tableDescription.AttributeDefinitions.Single(a => a.AttributeName == indexRangeAttribute).AttributeType;
        Assert.That(indexRangeAttribute, Is.EqualTo("IndexRangeId"));
        Assert.That(indexRangeType, Is.EqualTo(ScalarAttributeType.S));
    }

    [Test]
    public async Task AddTableFromTypeWithNumericIds()
    {
        _db.CreateTableFromType<TestTypeWithNumericIds>();

        var tableDescription = (await _db.DescribeTableAsync("table-with-n-ids")).Table;

        var hashAttribute = tableDescription.KeySchema.Single(k => k.KeyType == KeyType.HASH).AttributeName;
        var hashType = tableDescription.AttributeDefinitions.Single(a => a.AttributeName == hashAttribute).AttributeType;
        Assert.That(hashAttribute, Is.EqualTo("Id"));
        Assert.That(hashType, Is.EqualTo(ScalarAttributeType.N));

        var index = tableDescription.GlobalSecondaryIndexes.Single();
        Assert.That(index.IndexName, Is.EqualTo("GSI-Int"));

        var indexHashAttribute = index.KeySchema.Single(k => k.KeyType == KeyType.HASH).AttributeName;
        var indexHashType = tableDescription.AttributeDefinitions.Single(a => a.AttributeName == indexHashAttribute).AttributeType;
        Assert.That(indexHashAttribute, Is.EqualTo("IndexHashId"));
        Assert.That(indexHashType, Is.EqualTo(ScalarAttributeType.N));
    }
}
