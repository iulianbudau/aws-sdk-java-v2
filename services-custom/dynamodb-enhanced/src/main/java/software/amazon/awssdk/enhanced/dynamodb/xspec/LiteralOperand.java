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

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents a literal value in building expressions.
 * <p>
 * This object is as immutable (or unmodifiable) as the underlying value given during construction.
 */
class LiteralOperand extends Operand {
    private final Object value;

    /**
     * Represents a null value.
     */
    LiteralOperand() {
        this.value = null;
    }

    /**
     * @param value must be a supported type by the DynamoDB Document API.
     */
    LiteralOperand(Object value) {
        this.value = value;
    }

    /**
     * Must be a set of String, Number, or byte[].
     */
    LiteralOperand(Set<?> value) {
        this.value = value;
    }

    /**
     * Must be a list of supported types by the DynamoDB Document API.
     */
    LiteralOperand(List<?> value) {
        this.value = value;
    }

    /**
     * Each value of the map must be a supported type by the DynamoDB Document API.
     */
    LiteralOperand(Map<String, ?> value) {
        this.value = value;
    }

    LiteralOperand(boolean value) {
        this.value = value;
    }

    @Override
    final String asSubstituted(SubstitutionContext context) {
        return context.valueTokenFor(value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
