using Amazon.DynamoDBv2;
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

    public static bool ApplyCondition(this AttributeValue attributeValue, Condition condition)
    {
        var conditionValues = condition.AttributeValueList;
        var comparison = condition.ComparisonOperator;
        var value = attributeValue.GetValue();
        var comparableValue = value as IComparable;

        if (comparison == ComparisonOperator.EQ)
            return Equals(value, conditionValues[0].GetValue());

        if (comparison == ComparisonOperator.NE)
            return !Equals(value, conditionValues[0].GetValue());

        if (comparison == ComparisonOperator.IN)
            return conditionValues.Any(cv => Equals(value, cv.GetValue()));

        if (comparison == ComparisonOperator.NULL)
            return value is null;

        if (comparison == ComparisonOperator.NOT_NULL)
            return value is not null;

        if (comparison == ComparisonOperator.GT)
            return comparableValue?.CompareTo(conditionValues[0].GetValue()) > 0;

        if (comparison == ComparisonOperator.LT)
            return comparableValue?.CompareTo(conditionValues[0].GetValue()) < 0;

        if (comparison == ComparisonOperator.LE)
            return comparableValue?.CompareTo(conditionValues[0].GetValue()) <= 0;

        if (comparison == ComparisonOperator.GE)
            return comparableValue?.CompareTo(conditionValues[0].GetValue()) >= 0;

        if (comparison == ComparisonOperator.BETWEEN)
        {
            var fromValue = conditionValues[0].GetValue();
            var toValue = conditionValues[1].GetValue();
            return comparableValue?.CompareTo(fromValue) >= 0 && comparableValue?.CompareTo(toValue) <= 0;
        }

        if (comparison == ComparisonOperator.BEGINS_WITH)
        {
            return value is string strValue
                && conditionValues[0].GetValue() is string condValue
                && strValue.StartsWith(condValue, StringComparison.InvariantCulture);
        }

        if (comparison == ComparisonOperator.CONTAINS)
        {
            return value is string strValue
                && conditionValues[0].GetValue() is string condValue
                && strValue.Contains(condValue, StringComparison.InvariantCulture);
        }

        if (comparison == ComparisonOperator.NOT_CONTAINS)
        {
            return value is string strValue
                && conditionValues[0].GetValue() is string condValue
                && !strValue.Contains(condValue, StringComparison.InvariantCulture);
        }

        throw new NotImplementedException($"Comparison operator {comparison.Value} is not supported.");
    }
}
