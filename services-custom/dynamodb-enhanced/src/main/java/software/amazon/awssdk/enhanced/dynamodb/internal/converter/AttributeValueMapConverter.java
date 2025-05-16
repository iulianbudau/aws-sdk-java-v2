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

package software.amazon.awssdk.enhanced.dynamodb.internal.converter;

import software.amazon.awssdk.enhanced.dynamodb.AttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.AttributeConverterProvider;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Converts a {@code Map<String, Object>} into a {@code Map<String, AttributeValue>} using the given
 * {@link AttributeConverterProvider}. This is typically used to prepare expression attribute values
 * for use in DynamoDB operations.
 */
public final class AttributeValueMapConverter {

    private final AttributeConverterProvider provider;

    public AttributeValueMapConverter() {
        this(AttributeConverterProvider.defaultProvider());
    }

    public AttributeValueMapConverter(AttributeConverterProvider provider) {
        this.provider = Objects.requireNonNull(provider, "AttributeConverterProvider must not be null");
    }

    /**
     * Converts the given {@code Map<String, Object>} into a {@code Map<String, AttributeValue>} using the
     * configured {@link AttributeConverterProvider}.
     *
     * @param rawValues The raw name-value pairs to convert.
     * @return An immutable map of expression attribute values.
     * @throws IllegalArgumentException if no converter is found for a given value.
     */
    public Map<String, AttributeValue> convert(Map<String, Object> rawValues) {
        if (rawValues == null || rawValues.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, AttributeValue> result = new HashMap<>();
        for (Map.Entry<String, Object> entry : rawValues.entrySet()) {
            result.put(entry.getKey(), toAttributeValue(entry.getValue()));
        }

        return Collections.unmodifiableMap(result);
    }

    /**
     * Converts a single Java value to an {@link AttributeValue} using the appropriate converter.
     *
     * @param value The value to convert (must not be null).
     * @return The converted {@link AttributeValue}.
     * @throws IllegalArgumentException if no converter is found for the value type.
     */
    private AttributeValue toAttributeValue(Object value) {
        if (value == null) {
            return AttributeValue.builder().nul(true).build();
        }

        @SuppressWarnings("unchecked")
        EnhancedType<Object> type = (EnhancedType<Object>) EnhancedType.of(value.getClass());
        AttributeConverter<Object> converter = provider.converterFor(type);

        if (converter == null) {
            throw new IllegalArgumentException("No AttributeConverter found for type: " + value.getClass().getName());
        }

        return converter.transformFrom(value);
    }
}
