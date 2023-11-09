using System.Collections.Generic;

namespace DynamoDB.InMemoryTest.Extensions;

internal static class CollectionExtensions
{
    public static Dictionary<TKey, TValue> ToDictionary<TKey, TValue>(this IEnumerable<KeyValuePair<TKey, TValue>> keyValuePairs)
    {
        return new Dictionary<TKey, TValue>(keyValuePairs);
    }
}
