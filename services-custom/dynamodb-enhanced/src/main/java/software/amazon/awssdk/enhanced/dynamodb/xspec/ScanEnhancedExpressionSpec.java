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
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.NestedAttributeName;

/**
 * Expression specification for making scan request to Amazon DynamoDB.
 * <p>
 * Even though the internal name map and value map are unmodifiable, this object is only as unmodifiable as the underlying values
 * in its value map. In turn, the values in the value maps are essentially provided by the users of this library. In other words,
 * this object is thread-safe as long as the underlying values provided by the users are thread-safe.
 */
public final class ScanEnhancedExpressionSpec {

    private final Expression filterExpression;
    private final List<String> attributesToProject;
    private final List<NestedAttributeName> nestedAttributesToProject;

    ScanEnhancedExpressionSpec(ExpressionSpecBuilder builder) {
        this.filterExpression = builder.buildConditionExpression();
        this.attributesToProject = builder.buildAttributesToProject();
        this.nestedAttributesToProject = builder.buildNestedAttributesToProject();
    }

    /**
     * Returns the condition expression; or null if there is none.
     */
    public Expression getFilterExpression() {
        return filterExpression;
    }

    public List<String> getAttributesToProject() {
        return Collections.unmodifiableList(attributesToProject);
    }

    public List<NestedAttributeName> getNestedAttributesToProject() {
        return Collections.unmodifiableList(nestedAttributesToProject);
    }
}
