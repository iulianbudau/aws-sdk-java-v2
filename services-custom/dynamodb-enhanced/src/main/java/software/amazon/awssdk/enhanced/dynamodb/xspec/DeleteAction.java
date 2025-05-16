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
 * Represents a <a href= "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html" >DELETE</a>
 * action in the DELETE section of an update expression.
 * <p>
 * A DELETE action is used to delete an element from a set. The attribute involved must be a set data type.
 * <p>
 * This object is as immutable (or unmodifiable) as the underlying value (of type {@code ValueOperand}) given during
 * construction.
 */
public final class DeleteAction extends UpdateAction {
    DeleteAction(PathOperand attr, LiteralOperand value) {
        super("DELETE", attr, value);
    }
}
