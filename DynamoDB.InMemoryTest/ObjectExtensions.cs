using System.Text.Json;

namespace DynamoDB.InMemoryTest
{
    internal static class ObjectExtensions
    {
        public static T ConvertTo<T>(this object o)
        {
            return JsonSerializer.Deserialize<T>(JsonSerializer.Serialize(o));
        }
    }
}
