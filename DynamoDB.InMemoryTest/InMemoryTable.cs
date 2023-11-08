using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

namespace DynamoDB.InMemoryTest
{
    internal class InMemoryTable
    {
        public TableDescription TableDescription { get; set; }
        public List<Dictionary<string, AttributeValue>> Items { get; } = new();

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

        public List<Dictionary<string, AttributeValue>> QueryByKey(Dictionary<string, Condition> keyConditions)
        {
            return Items.Where(i =>
            {
                var key = GetKey(i);
                return keyConditions.All(c =>
                {
                    var value = key[c.Key];
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

        private Dictionary<string, AttributeValue> GetKey(Dictionary<string, AttributeValue> item)
        {
            var keyNames = TableDescription.KeySchema.Select(k => k.AttributeName);
            return item.Where(i => keyNames.Contains(i.Key)).ToDictionary();
        }

        private static bool Equals(Dictionary<string, AttributeValue> key1, Dictionary<string, AttributeValue> key2)
        {
            return JsonSerializer.Serialize(key1) == JsonSerializer.Serialize(key2);
        }

        private static bool Equals(AttributeValue v1, AttributeValue v2)
        {
            return JsonSerializer.Serialize(v1) == JsonSerializer.Serialize(v2);
        }
    }
}
