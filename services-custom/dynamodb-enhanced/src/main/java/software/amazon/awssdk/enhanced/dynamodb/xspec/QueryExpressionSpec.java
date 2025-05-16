/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package software.amazon.awssdk.enhanced.dynamodb.xspec;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.NestedAttributeName;
import software.amazon.awssdk.enhanced.dynamodb.internal.converter.AttributeValueMapConverter;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Expression specification for making query request to Amazon DynamoDB.
 * <p>
 * Even though the internal name map and value map are unmodifiable, this object is only as unmodifiable as the underlying values
 * in its value map. In turn, the values in the value maps are essentially provided by the users of this library. In other words,
 * this object is thread-safe as long as the underlying values provided by the users are thread-safe.
 */
public final class QueryExpressionSpec {

    private final Expression keyConditionExpression;
    private final Expression filterExpression;
    private final Expression projectionExpression;
    private final List<String> attributesToProject;
    private final List<NestedAttributeName> nestedAttributesToProject;
    private final Map<String, String> expressionNames;
    private final Map<String, AttributeValue> expressionValues;

    QueryExpressionSpec(ExpressionSpecBuilder builder) {
        SubstitutionContext context = new SubstitutionContext();
        this.keyConditionExpression = builder.buildKeyConditionExpression(context);
        this.filterExpression = builder.buildConditionExpression(context);
        this.projectionExpression = builder.buildProjectionExpression(context);
        this.attributesToProject = builder.buildAttributesToProject();
        this.nestedAttributesToProject = builder.buildNestedAttributesToProject();
        this.expressionNames = context.getNameMap() == null ? null : Collections.unmodifiableMap(context.getNameMap());
        this.expressionValues = context.getValueMap() == null ? null :
                                new AttributeValueMapConverter().convert(context.getValueMap());
    }

    public Expression getKeyConditionExpression() {
        return keyConditionExpression;
    }

    public Expression getFilterExpression() {
        return filterExpression;
    }

    public Expression getProjectionExpression() {
        return projectionExpression;
    }

    public List<String> getAttributesToProject() {
        return Collections.unmodifiableList(attributesToProject);
    }

    public List<NestedAttributeName> getNestedAttributesToProject() {
        return Collections.unmodifiableList(nestedAttributesToProject);
    }

    public Map<String, String> getExpressionNames() {
        return Collections.unmodifiableMap(expressionNames);
    }

    public Map<String, AttributeValue> getExpressionValues() {
        return Collections.unmodifiableMap(expressionValues);
    }
}
