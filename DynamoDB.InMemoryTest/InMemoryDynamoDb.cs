using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime.Internal;
using System.Linq;
using System.Reflection;

namespace DynamoDB.InMemoryTest
{
    public partial class InMemoryDynamoDb : AmazonDynamoDBClient
    {
        public InMemoryDynamoDb() : base("test-access-key", "test-aws-secret", new AmazonDynamoDBConfig
        {
            RegionEndpoint = RegionEndpoint.EUCentral1
        })
        {

        }

        protected override void CustomizeRuntimePipeline(RuntimePipeline pipeline)
        {
            pipeline.AddHandler(new InMemoryPipelineHandler());
        }

        public InMemoryDynamoDb AddTableFromType<T>(string tableNamePrefix = "")
        {
            var request = new CreateTableRequest();

            var tableAttribute = typeof(T).GetCustomAttribute<DynamoDBTableAttribute>(true);
            var tableName = tableNamePrefix + tableAttribute?.TableName ?? typeof(T).Name;
            request.TableName = tableName;

            var lowerCase = tableAttribute?.LowerCamelCaseProperties ?? false;

            var props = typeof(T).GetProperties();
            var (hashProp, hashAttribute) = props.Select(p => (p, a: p.GetCustomAttribute<DynamoDBHashKeyAttribute>())).FirstOrDefault(k => k.a != null);
            if (hashProp != null)
            {
                var name = hashAttribute.AttributeName ?? hashProp.Name;
                if (lowerCase)
                    name = name.ToLower();

                var type = hashProp.PropertyType == typeof(int) || hashProp.PropertyType == typeof(long)
                    ? ScalarAttributeType.N
                    : ScalarAttributeType.S;

                request.KeySchema.Add(new KeySchemaElement { AttributeName = name, KeyType = KeyType.HASH });
                request.AttributeDefinitions.Add(new AttributeDefinition { AttributeName = name, AttributeType = type });
            }

            var (rangeProp, rangeAttribute) = props.Select(p => (p, a: p.GetCustomAttribute<DynamoDBRangeKeyAttribute>())).FirstOrDefault(k => k.a != null);
            if (rangeProp != null)
            {
                var name = rangeAttribute.AttributeName ?? rangeProp.Name;
                var type = rangeProp.PropertyType == typeof(int) || rangeProp.PropertyType == typeof(long)
                    ? ScalarAttributeType.N
                    : ScalarAttributeType.S;

                request.KeySchema.Add(new KeySchemaElement { AttributeName = name, KeyType = KeyType.RANGE });
                request.AttributeDefinitions.Add(new AttributeDefinition { AttributeName = name, AttributeType = type });
            }

            CreateTableAsync(request).Wait();

            return this;
        }
    }
}
