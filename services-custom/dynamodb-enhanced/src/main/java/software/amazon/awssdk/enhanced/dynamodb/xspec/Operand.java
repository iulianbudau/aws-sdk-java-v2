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
 * Represents an operand for building expressions.
 * <p>
 * Underlying grammar:
 * <pre>
 * operand
 *      : path          # PathOperand
 *      | literal       # LiteralOperand
 *      | function      # FunctionCall
 *      | '(' operand ')'
 *      ;
 * </pre>
 *
 * @see PathOperand
 * @see FunctionOperand
 * @see LiteralOperand
 */
public abstract class Operand extends UnitOfExpression {
}
