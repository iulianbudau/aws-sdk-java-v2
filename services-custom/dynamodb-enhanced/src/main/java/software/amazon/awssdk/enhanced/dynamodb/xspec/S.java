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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import software.amazon.awssdk.annotations.Immutable;

/**
 * A path operand that refers to a <a href=
 * "http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html" >string</a> attribute in DynamoDB; used
 * for building expressions.
 * <p>
 * Use {@link ExpressionSpecBuilder#S(String)} to instantiate this class.
 */
@Immutable
public final class S extends PathOperand {
    S(String path) {
        super(path);
    }

    /**
     * Returns a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Functions"
     * >function condition</a> (that evaluates to true if the value of the current attribute begins with the specified operand)
     * for building condition expression.
     */
    public FunctionCondition beginsWith(String value) {
        return new FunctionCondition("begins_with", this, new LiteralOperand(value));
    }

    /**
     * Returns a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Functions"
     * >function condition</a> (that evaluates to true if the value of the current attribute contains the specified substring) for
     * building condition expression.
     */
    public FunctionCondition contains(String substring) {
        return new FunctionCondition("contains", this, new LiteralOperand(substring));
    }

    /**
     * Returns a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Comparators"
     * >comparator condition</a> (that evaluates to true if the value of the current attribute is equal to that of the specified
     * attribute) for building condition expression.
     */
    public ComparatorCondition eq(S that) {
        return new ComparatorCondition("=", this, that);
    }

    /**
     * Returns a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Comparators"
     * >comparator condition</a> (that evaluates to true if the value of the current attribute is not equal to that of the
     * specified attribute) for building condition expression.
     */
    public ComparatorCondition ne(S that) {
        return new ComparatorCondition("<>", this, that);
    }

    /**
     * Returns a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Comparators"
     * >comparator condition</a> (that evaluates to true if the value of the current attribute is less than or equal to the
     * specified value) for building condition expression.
     */
    public ComparatorCondition le(String value) {
        return new ComparatorCondition("<=", this, new LiteralOperand(value));
    }

    /**
     * Returns a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Comparators"
     * >comparator condition</a> (that evaluates to true if the value of the current attribute is less than or equal to that of
     * the specified attribute) for building condition expression.
     */
    public ComparatorCondition le(S that) {
        return new ComparatorCondition("<=", this, that);
    }

    /**
     * Returns a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Comparators"
     * >comparator condition</a> (that evaluates to true if the value of the current attribute is less than the specified value)
     * for building condition expression.
     */
    public ComparatorCondition lt(String value) {
        return new ComparatorCondition("<", this, new LiteralOperand(value));
    }

    /**
     * Returns a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Comparators"
     * >comparator condition</a> (that evaluates to true if the value of the current attribute is less than that of the specified
     * attribute) for building condition expression.
     */
    public ComparatorCondition lt(S that) {
        return new ComparatorCondition("<", this, that);
    }

    /**
     * Returns a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Comparators"
     * >comparator condition</a> (that evaluates to true if the value of the current attribute is greater than or equal to the
     * specified value) for building condition expression.
     */
    public ComparatorCondition ge(String value) {
        return new ComparatorCondition(">=", this, new LiteralOperand(value));
    }

    /**
     * Returns a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Comparators"
     * >comparator condition</a> (that evaluates to true if the value of the current attribute is greater than or equal to that of
     * the specified attribute) for building condition expression.
     */
    public ComparatorCondition ge(S that) {
        return new ComparatorCondition(">=", this, that);
    }

    /**
     * Returns a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Comparators"
     * >comparator condition</a> (that evaluates to true if the value of the current attribute is greater than the specified
     * value) for building condition expression.
     */
    public ComparatorCondition gt(String value) {
        return new ComparatorCondition(">", this, new LiteralOperand(value));
    }

    /**
     * Returns a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Comparators"
     * >comparator condition</a> (that evaluates to true if the value of the current attribute is greater than that of the
     * specified attribute) for building condition expression.
     */
    public ComparatorCondition gt(S that) {
        return new ComparatorCondition(">", this, that);
    }

    /**
     * Returns a {@code BetweenCondition} that represents a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Comparators"
     * >BETWEEN comparison</a> (that evaluates to true if the value of the current attribute is greater than or equal to the given
     * low value, and less than or equal to the given high value) for building condition expression.
     */
    public BetweenCondition between(String low, String high) {
        return new BetweenCondition(this,
                                    new LiteralOperand(low),
                                    new LiteralOperand(high));
    }

    /**
     * Returns an {@code IfNotExists} object which represents an <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html" >if_not_exists(path,
     * operand)</a> function call where path refers to that of the current attribute; used for building expressions.
     *
     * <pre>
     * "if_not_exists (path, operand) – If the item does not contain an attribute
     * at the specified path, then if_not_exists evaluates to operand; otherwise,
     * it evaluates to path. You can use this function to avoid overwriting an
     * attribute already present in the item."
     * </pre>
     *
     * @param defaultValue the default value that will be used as the operand to if_not_exists function call.
     */
    public IfNotExistsFunction<S> ifNotExists(S defaultValue) {
        return ExpressionSpecBuilder.if_not_exists(this, defaultValue);
    }

    /**
     * Returns an <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Functions"
     * >InCondition</a> (that evaluates to true if the value of the current attribute is equal to any of the specified values) for
     * building condition expression.
     *
     * @param values specified values. The number of values must be at least one and at most 100.
     */
    public InCondition in(String... values) {
        List<LiteralOperand> list = Arrays.stream(values).map(LiteralOperand::new).collect(Collectors.toCollection(LinkedList::new));
        return new InCondition(this, list);
    }

    /**
     * Returns an <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Functions"
     * >InCondition</a> (that evaluates to true if the value of the current attribute is equal to any of the values in the
     * specified list) for building condition expression.
     *
     * @param values specified list of values. The number of values must be at least one and at most 100.
     */
    public InCondition in(List<String> values) {
        List<LiteralOperand> list = values.stream().map(LiteralOperand::new).collect(Collectors.toCollection(LinkedList::new));
        return new InCondition(this, list);
    }

    /**
     * Returns a {@code SetAction} object used for building update expression. If the attribute referred to by this path
     * operand doesn't exist, the returned object represents adding the attribute value of the specified source path operand to an
     * item. If the current attribute already exists, the returned object represents the value replacement of the current
     * attribute by the attribute value of the specified source path operand.
     */
    public SetAction set(S source) {
        return new SetAction(this, source);
    }

    /**
     * Returns a {@code SetAction} object used for building update expression. If the attribute referred to by this path
     * operand doesn't exist, the returned object represents adding the specified value as an attribute to an item. If the
     * attribute referred to by this path operand already exists, the returned object represents the value replacement of the
     * current attribute by the specified value.
     */
    public SetAction set(String value) {
        return new SetAction(this, new LiteralOperand(value));
    }

    /**
     * Returns a {@code SetAction} object used for building update expression. If the attribute referred to by this path
     * operand doesn't exist, the returned object represents adding the value of evaluating the specified {@code IfNotExists}
     * function as an attribute to an item. If the attribute referred to by this path operand already exists, the returned object
     * represents the value replacement of the current attribute by the value of evaluating the specified {@code IfNotExists}
     * function.
     */
    public SetAction set(IfNotExistsFunction<S> ifNotExistsFunction) {
        return new SetAction(this, ifNotExistsFunction);
    }

    /**
     * Returns a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Functions"
     * >comparator condition</a> (that evaluates to true if the attribute value referred to by this path operand is equal to the
     * specified value) for building condition expression.
     */
    public ComparatorCondition eq(String value) {
        return new ComparatorCondition("=", this, new LiteralOperand(value));
    }

    /**
     * Returns a <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference.Functions"
     * >comparator condition</a> (that evaluates to true if the attribute value referred to by this path operand is not equal to
     * that of the specified path operand) for building condition expression.
     */
    public ComparatorCondition ne(String value) {
        return new ComparatorCondition("<>", this, new LiteralOperand(value));
    }

    /**
     * Returns an {@code IfNotExists} object which represents an <a href=
     * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html" >if_not_exists(path,
     * operand)</a> function call where path refers to that of the current path operand; used for building expressions.
     *
     * <pre>
     * "if_not_exists (path, operand) – If the item does not contain an attribute
     * at the specified path, then if_not_exists evaluates to operand; otherwise,
     * it evaluates to path. You can use this function to avoid overwriting an
     * attribute already present in the item."
     * </pre>
     *
     * @param defaultValue the default value that will be used as the operand to if_not_exists function call.
     */
    public IfNotExistsFunction<S> ifNotExists(String defaultValue) {
        return new IfNotExistsFunction<>(this, new LiteralOperand(defaultValue));
    }
}
