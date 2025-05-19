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

import software.amazon.awssdk.enhanced.dynamodb.Expression;

/**
 * Expression specification for making UpdateItem request to Amazon DynamoDB.
 * <p>
 * Even though the internal name map and value map are unmodifiable, this object is only as unmodifiable as the underlying values
 * in its value map. In turn, the values in the value maps are essentially provided by the users of this library. In other words,
 * this object is thread-safe as long as the underlying values provided by the users are thread-safe.
 */
public class UpdateItemEnhancedExpressionSpec {
    private final Expression conditionExpression;

    UpdateItemEnhancedExpressionSpec(ExpressionSpecBuilder builder) {
        this.conditionExpression = builder.buildConditionExpression();
    }

    /**
     * Returns the condition expression; or null if there is none.
     */
    public Expression getConditionExpression() {
        return conditionExpression;
    }

}
