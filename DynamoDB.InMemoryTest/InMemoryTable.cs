using Amazon.DynamoDBv2.Model;
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
            return Items.FirstOrDefault(i => key.All(k => Equals(i[k.Key], k.Value)));
        }

        private static bool Equals(AttributeValue v1, AttributeValue v2)
        {
            return JsonSerializer.Serialize(v1) == JsonSerializer.Serialize(v2);
        }
    }
}
