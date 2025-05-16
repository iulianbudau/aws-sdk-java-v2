/*
 * Copyright 2015-2025 Amazon Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    http://aws.amazon.com/apache2.0
 *
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
 * OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.awssdk.enhanced.dynamodb.xspec;

import java.util.Collections;
import java.util.Map;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.internal.converter.AttributeValueMapConverter;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Expression specification for making UpdateItem request to Amazon DynamoDB.
 * <p>
 * Even though the internal name map and value map are unmodifiable, this object is only as unmodifiable as the underlying values
 * in its value map. In turn, the values in the value maps are essentially provided by the users of this library. In other words,
 * this object is thread-safe as long as the underlying values provided by the users are thread-safe.
 */
public class UpdateItemExpressionSpec {
    private final Expression updateExpression;
    private final Expression conditionExpression;
    private final Map<String, String> expressionNames;
    private final Map<String, AttributeValue> expressionValues;

    UpdateItemExpressionSpec(ExpressionSpecBuilder builder) {
        SubstitutionContext context = new SubstitutionContext();
        this.updateExpression = builder.buildUpdateExpression(context);
        this.conditionExpression = builder.buildConditionExpression(context);
        this.expressionNames = context.getNameMap() == null ? null : Collections.unmodifiableMap(context.getNameMap());
        this.expressionValues = context.getValueMap() == null ? null :
                                new AttributeValueMapConverter().convert(context.getValueMap());
    }

    /**
     * Returns the update expression; or null if there is none.
     */
    public Expression getUpdateExpression() {
        return updateExpression;
    }

    /**
     * Returns the condition expression; or null if there is none.
     */
    public Expression getConditionExpression() {
        return conditionExpression;
    }

    /**
     * Returns the expression names map which is unmodifiable; or null if there is none.
     */
    public Map<String, String> getExpressionNames() {
        return Collections.unmodifiableMap(expressionNames);
    }

    /**
     * Returns the expression values map which is unmodifiable; or null if there is none.
     */
    public Map<String, AttributeValue> getExpressionValues() {
        return Collections.unmodifiableMap(expressionValues);
    }
}
