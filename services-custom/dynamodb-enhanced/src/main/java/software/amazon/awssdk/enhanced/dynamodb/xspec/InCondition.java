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

import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a <a href=
 * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference"
 * >IN</a> condition in building condition expression.
 * <p>
 * Underlying grammar:
 *
 * <pre>
 *    operand IN ( operand (',' operand (, ...) ))
 * </pre>
 * <p>
 * This object is as immutable (or unmodifiable) as the underlying set of operands.
 */
public final class InCondition extends Condition {
    private final PathOperand attribute;
    private final List<? extends Operand> operands;

    /**
     * @param operands assumed to be allocated on the stack so it will remain externally unmodifiable
     */
    InCondition(PathOperand attribute, List<? extends Operand> operands) {
        this.attribute = attribute;
        this.operands = operands;
    }

    @Override
    String asSubstituted(SubstitutionContext context) {
        return operands.stream().map(operand -> operand.asSubstituted(context)).collect(Collectors.joining(", ", attribute.asSubstituted(context) + " IN (", ")"));
    }

    @Override
    boolean atomic() {
        return true;
    }

    @Override
    int precedence() {
        return Precedence.IN.value();
    }
}
