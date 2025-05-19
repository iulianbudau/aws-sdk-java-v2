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
import java.util.Map;
import software.amazon.awssdk.enhanced.dynamodb.internal.converter.AttributeValueMapConverter;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Expression specification for making scan request to Amazon DynamoDB.
 * <p>
 * Even though the internal name map and value map are unmodifiable, this object is only as unmodifiable as the underlying values
 * in its value map. In turn, the values in the value maps are essentially provided by the users of this library. In other words,
 * this object is thread-safe as long as the underlying values provided by the users are thread-safe.
 */
public final class ScanExpressionSpec {
    private final String projectionExpression;
    private final String filterExpression;
    private final Map<String, String> expressionNames;
    private final Map<String, AttributeValue> expressionValues;

    ScanExpressionSpec(ExpressionSpecBuilder builder) {
        SubstitutionContext context = new SubstitutionContext();
        this.filterExpression = builder.buildConditionExpression(context);
        this.projectionExpression = builder.buildProjectionExpression(context);

        this.expressionNames = context.getNameMap() == null ? null : Collections.unmodifiableMap(context.getNameMap());
        this.expressionValues = context.getValueMap() == null ? null :
                                new AttributeValueMapConverter().convert(context.getValueMap());
    }

    /**
     * Returns the projection expression; or null if there is none.
     */
    public String getProjectionExpression() {
        return projectionExpression;
    }

    /**
     * Returns the condition expression; or null if there is none.
     */
    public String getFilterExpression() {
        return filterExpression;
    }

    /**
     * Returns the name map which is unmodifiable; or null if there is none.
     */
    public Map<String, String> getExpressionNames() {
        return Collections.unmodifiableMap(expressionNames);
    }

    /**
     * Returns the value map which is unmodifiable; or null if there is none.
     */
    public Map<String, AttributeValue> getExpressionValues() {
        return Collections.unmodifiableMap(expressionValues);
    }
}
