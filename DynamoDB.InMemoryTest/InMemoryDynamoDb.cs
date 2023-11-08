using Amazon;
using Amazon.DynamoDBv2;
using Amazon.Runtime.Internal;

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
    }
}
