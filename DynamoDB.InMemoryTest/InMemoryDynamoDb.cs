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
                if (lowerCase)
                    name = name.ToLower();

                var type = rangeProp.PropertyType == typeof(int) || rangeProp.PropertyType == typeof(long)
                    ? ScalarAttributeType.N
                    : ScalarAttributeType.S;

                request.KeySchema.Add(new KeySchemaElement { AttributeName = name, KeyType = KeyType.RANGE });
                request.AttributeDefinitions.Add(new AttributeDefinition { AttributeName = name, AttributeType = type });
            }

            var indexHashProps = props.Select(p => (p, a: p.GetCustomAttribute<DynamoDBGlobalSecondaryIndexHashKeyAttribute>())).Where(k => k.a != null);
            var indexRangeProps = props.Select(p => (p, a: p.GetCustomAttribute<DynamoDBGlobalSecondaryIndexRangeKeyAttribute>())).Where(k => k.a != null);

            foreach (var (indexHashProp, indexHashAttribute) in indexHashProps)
            {
                var name = indexHashAttribute.AttributeName ?? indexHashProp.Name;
                if (lowerCase)
                    name = name.ToLower();

                var type = indexHashProp.PropertyType == typeof(int) || indexHashProp.PropertyType == typeof(long)
                    ? ScalarAttributeType.N
                    : ScalarAttributeType.S;

                request.GlobalSecondaryIndexes.Add(new GlobalSecondaryIndex
                {
                    IndexName = indexHashAttribute.IndexNames[0],
                    KeySchema = { new KeySchemaElement { AttributeName = name, KeyType = KeyType.HASH } }
                });
            }

            foreach (var (indexRangeProp, indexRangeAttribute) in indexRangeProps)
            {
                var name = indexRangeAttribute.AttributeName ?? indexRangeProp.Name;
                if (lowerCase)
                    name = name.ToLower();

                var type = indexRangeProp.PropertyType == typeof(int) || indexRangeProp.PropertyType == typeof(long)
                    ? ScalarAttributeType.N
                    : ScalarAttributeType.S;

                var index = request.GlobalSecondaryIndexes.First(i => i.IndexName == indexRangeAttribute.IndexNames[0]);
                index.KeySchema.Add(new KeySchemaElement { AttributeName = name, KeyType = KeyType.RANGE });
            }

            CreateTableAsync(request).Wait();

            return this;
        }
    }
}
