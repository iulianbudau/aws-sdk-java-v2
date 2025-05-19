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
import software.amazon.awssdk.annotations.Immutable;

/**
 * Expression specification for making GetItem request to Amazon DynamoDB.
 */
@Immutable
public final class GetItemExpressionSpec {
    private final String projectionExpression;
    private final Map<String, String> expressionNames;

    GetItemExpressionSpec(ExpressionSpecBuilder builder) {
        SubstitutionContext context = new SubstitutionContext();
        this.projectionExpression = builder.buildProjectionExpression(context);
        this.expressionNames = context.getNameMap() == null ? null : Collections.unmodifiableMap(context.getNameMap());
    }

    /**
     * Returns the projection expression; or null if there is none.
     */
    public String getProjectionExpression() {
        return projectionExpression;
    }

    /**
     * Returns the name map which is unmodifiable; or null if there is none.
     */
    public Map<String, String> getExpressionNames() {
        return Collections.unmodifiableMap(expressionNames);
    }
}

