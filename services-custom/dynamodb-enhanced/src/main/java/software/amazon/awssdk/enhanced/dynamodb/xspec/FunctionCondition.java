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

/**
 * Represents a <a href=
 * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference"
 * >Function</a> condition in building condition expression.
 * <p>
 * Underlying grammar:
 *
 * <pre>
 * function ::=
 *    attribute_exists (path)
 *  | attribute_not_exists (path)
 *  | begins_with (path, operand)
 *  | contains (path, operand)
 * </pre>
 * <p>
 * This object is as immutable (or unmodifiable) as the underlying operand.
 */
public final class FunctionCondition extends Condition {
    private final String functionId;
    private final PathOperand pathOperand;
    private final Operand operand;

    FunctionCondition(String functionId, PathOperand attribute) {
        this.functionId = functionId;
        this.pathOperand = attribute;
        this.operand = null;
    }

    FunctionCondition(String functionId, PathOperand attribute, Operand operand) {
        this.functionId = functionId;
        this.pathOperand = attribute;
        this.operand = operand;
    }

    @Override
    String asSubstituted(SubstitutionContext context) {
        StringBuilder sb = new StringBuilder(functionId).append("(").append(
            pathOperand.asSubstituted(context));
        if (operand != null) {
            sb.append(", ").append(operand.asSubstituted(context));
        }
        return sb.append(")").toString();
    }

    String getFunctionId() {
        return functionId;
    }

    PathOperand getPathOperand() {
        return pathOperand;
    }

    Operand getOperand() {
        return operand;
    }

    @Override
    boolean atomic() {
        return true;
    }

    @Override
    int precedence() {
        return Precedence.Function.value();
    }
}
