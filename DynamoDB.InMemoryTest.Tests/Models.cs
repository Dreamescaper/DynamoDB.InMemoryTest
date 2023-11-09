using Amazon.DynamoDBv2.DataModel;

namespace DynamoDB.InMemoryTest.Tests;

[DynamoDBTable("table-with-string-id")]
public class TestTypeWithStringId
{
    [DynamoDBHashKey]
    public string HashId { get; set; }

    [DynamoDBRangeKey("_rangeId")]
    public string RangeId { get; set; }

    [DynamoDBGlobalSecondaryIndexHashKey("GSI-Test")]
    public string IndexHashId { get; set; }

    [DynamoDBGlobalSecondaryIndexRangeKey("GSI-Test")]
    public string IndexRangeId { get; set; }
}

[DynamoDBTable("table-with-n-ids")]
public class TestTypeWithNumericIds
{
    [DynamoDBHashKey]
    public long Id { get; set; }

    [DynamoDBGlobalSecondaryIndexHashKey("GSI-Int")]
    public int IndexHashId { get; set; }
}


[DynamoDBTable("table-with-hash")]
public class ModelWithHashKey
{
    [DynamoDBHashKey]
    public long Id { get; set; }
    public string Data { get; set; }
}

[DynamoDBTable("table-with-hash-and-range")]
public class ModelWithHashKeyAndRangeKey
{
    [DynamoDBHashKey]
    public long Id { get; set; }

    [DynamoDBRangeKey]
    public string Range { get; set; }

    public string Data { get; set; }
}

[DynamoDBTable("table-with-index")]
public class ModelWithIndex
{
    [DynamoDBHashKey]
    public string Id { get; set; }

    [DynamoDBGlobalSecondaryIndexHashKey("GSI")]
    public string Index { get; set; }

    public string Data { get; set; }
}
