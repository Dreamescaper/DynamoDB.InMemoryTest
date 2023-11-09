using Amazon.DynamoDBv2.Model;
using System;
using System.Linq;

namespace DynamoDB.InMemoryTest.Extensions;

internal static class AttributeValueExtensions
{
    public static object GetValue(this AttributeValue attributeValue)
    {
        return attributeValue switch
        {
            { NULL: true } => null,
            { IsBOOLSet: true } => attributeValue.BOOL,
            { IsLSet: true } => attributeValue.L.Select(GetValue).ToArray(),
            { IsMSet: true } => attributeValue.M.ToDictionary(kv => kv.Key, kv => GetValue(kv.Value)),

            { S: not null } => attributeValue.S,
            { N: not null } => long.TryParse(attributeValue.N, out var l) ? l : double.Parse(attributeValue.N),

            _ => throw new NotImplementedException()
        };
    }
}
