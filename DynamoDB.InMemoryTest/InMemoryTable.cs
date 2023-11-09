using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDB.InMemoryTest.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DynamoDB.InMemoryTest;

internal class InMemoryTable
{
    public TableDescription TableDescription { get; set; }
    public List<Dictionary<string, AttributeValue>> Items { get; } = [];

    public Dictionary<string, AttributeValue>? GetItem(Dictionary<string, AttributeValue> key)
    {
        return Items.FirstOrDefault(i => Equals(GetKey(i), key));
    }

    public void PutItem(Dictionary<string, AttributeValue> item)
    {
        var key = GetKey(item);
        DeleteItem(key);
        Items.Add(item);
    }

    public void DeleteItem(Dictionary<string, AttributeValue> key)
    {
        var existingItem = GetItem(key);
        if (existingItem != null)
            Items.Remove(existingItem);
    }

    public List<Dictionary<string, AttributeValue>> QueryByKey(Dictionary<string, Condition> keyConditions, string indexName)
    {
        return Items.Where(i =>
        {
            var key = GetKey(i, indexName);
            return keyConditions.All(c =>
            {
                if (!key.TryGetValue(c.Key, out var value))
                    return false;

                if (c.Value.ComparisonOperator == ComparisonOperator.EQ)
                {
                    return Equals(c.Value.AttributeValueList[0], value);
                }
                else
                {
                    throw new NotImplementedException();
                }
            });
        }).ToList();
    }

    private Dictionary<string, AttributeValue> GetKey(Dictionary<string, AttributeValue> item, string indexName = null)
    {
        var keySchema = indexName is null
            ? TableDescription.KeySchema
            : TableDescription.GlobalSecondaryIndexes.First(i => i.IndexName == indexName).KeySchema;

        var keyNames = keySchema.Select(k => k.AttributeName);
        return item.Where(i => keyNames.Contains(i.Key)).ToDictionary();
    }

    private static bool Equals(Dictionary<string, AttributeValue> key1, Dictionary<string, AttributeValue> key2)
    {
        return key1.All(kv => key2.TryGetValue(kv.Key, out var value2) && Equals(kv.Value, value2));
    }

    private static bool Equals(AttributeValue v1, AttributeValue v2)
    {
        return Equals(v1.GetValue(), v2.GetValue());
    }
}
