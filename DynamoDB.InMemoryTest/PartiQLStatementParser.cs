using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDB.InMemoryTest.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace DynamoDB.InMemoryTest;

internal static class PartiQLStatementParser
{
    private static readonly Regex SelectRegex = new(
        "select (?<expr>.*) from \"(?<table>[^\"]+)\"(.\"(?<index>[^\"]+)\")?( where (?<condition>.+))?",
        RegexOptions.IgnoreCase);

    private static readonly Regex ConditionRegex = new(@"^(?<key>\S+)(?<op>(=| \S+ ))(?<value>.+)");


    public static (string TableName, string? IndexName, List<string> AttributesToGet, ConditionalOperator ConditionalOperator, Dictionary<string, Condition> Conditions)
        Parse(string statement)
    {
        statement = Regex.Replace(statement.Trim(), "\\s+", " ");
        var match = SelectRegex.Match(statement);

        if (!match.Success)
            throw new NotImplementedException($"Statement '{statement}' is not supported by InMemory DynamoDB.");

        var selectExpression = match.Groups["expr"].Value;
        var attributesToGet = selectExpression == "*"
            ? null
            : selectExpression.Split(',').Select(a => a.Trim()).ToList();

        var tableName = match.Groups["table"].Value;
        var indexName = match.Groups["index"].Value;
        var condition = match.Groups["condition"].Value;

        if (string.IsNullOrEmpty(indexName))
            indexName = null;

        var (conditionalOperator, conditions) = ParseConditions(condition);

        return (tableName, indexName, attributesToGet, conditionalOperator, conditions);
    }

    private static (ConditionalOperator ConditionalOperator, Dictionary<string, Condition> Conditions) ParseConditions(string condition)
    {
        if (string.IsNullOrEmpty(condition))
            return (ConditionalOperator.AND, []);

        var op = condition.Contains(" OR ", StringComparison.OrdinalIgnoreCase) ? ConditionalOperator.OR : ConditionalOperator.AND;

        var conditions = condition.Split([" AND ", " OR "], StringSplitOptions.TrimEntries)
            .Select(s =>
            {
                var match = ConditionRegex.Match(s);

                if (!match.Success)
                    throw new NotImplementedException($"Condition '{s}' is not supported by InMemory DynamoDb.");

                var key = match.Groups["key"].Value.Trim('"');
                var op = match.Groups["op"].Value.Trim();
                var value = match.Groups["value"].Value;

                var values = value.StartsWith('[')
                    ? value.Trim('[', ']').Split(',', StringSplitOptions.TrimEntries)
                    : [value];

                var attributeValues = values
                    .Select(v => v.StartsWith('\'')
                        ? new AttributeValue { S = v.Trim('\'') }
                        : new AttributeValue { N = v })
                    .ToList();

                var condition = new Condition { ComparisonOperator = ParseComparisonOperator(op), AttributeValueList = attributeValues };

                return KeyValuePair.Create(key, condition);
            });

        return (op, conditions.ToDictionary());
    }

    private static ComparisonOperator ParseComparisonOperator(string condOp)
    {
        return condOp switch
        {
            "=" => ComparisonOperator.EQ,
            "<>" or "!=" => ComparisonOperator.NE,
            ">" => ComparisonOperator.GT,
            ">=" => ComparisonOperator.GE,
            "<=" => ComparisonOperator.LE,
            "IN" => ComparisonOperator.IN,

            _ => throw new NotImplementedException($"Operator '{condOp}' is not supported by InMemory DynamoDB.")
        };
    }
}
