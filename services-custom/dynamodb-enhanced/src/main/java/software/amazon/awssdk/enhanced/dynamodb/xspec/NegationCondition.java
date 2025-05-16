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

import static software.amazon.awssdk.enhanced.dynamodb.xspec.ExpressionSpecBuilder.wrapCondition;

/**
 * Represents a <a href=
 * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference">negation</a>
 * condition in building condition expressions.
 * <p>
 * Underlying grammar:
 *
 * <pre>
 *    NOT condition
 * </pre>
 * <p>
 * This object is as immutable (or unmodifiable) as the underlying condition.
 */
public final class NegationCondition extends Condition {
    private final Condition condition;

    NegationCondition(Condition condition) {
        this.condition = condition;
    }

    @Override
    String asSubstituted(SubstitutionContext context) {
        if (this.precedence() > condition.precedence()) {
            return "NOT " + wrapCondition(condition).asSubstituted(context);
        }
        return "NOT " + condition.asSubstituted(context);
    }

    @Override
    boolean atomic() {
        return true;
    }

    @Override
    int precedence() {
        return Precedence.NOT.value();
    }
}
