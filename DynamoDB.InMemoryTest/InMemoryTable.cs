using Amazon.DynamoDBv2.Model;
using System.Collections.Generic;

namespace DynamoDB.InMemoryTest
{
    internal class InMemoryTable
    {
        public TableDescription TableDescription { get; set; }
        public List<Dictionary<string, AttributeValue>> Items { get; } = new();
    }
}
