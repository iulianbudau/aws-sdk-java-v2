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

/**
 * Represents a function call in building expression.
 * <p>
 * Underlying grammar:
 *
 * <pre>
 * function
 *      : ID '(' operand (',' operand)* ')'    # FunctionCall
 *      ;
 * </pre>
 */
public abstract class FunctionOperand extends Operand {
}
