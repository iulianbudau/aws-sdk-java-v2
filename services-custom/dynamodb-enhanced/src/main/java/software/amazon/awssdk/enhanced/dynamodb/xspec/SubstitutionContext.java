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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An internal class to represent the substitution context for name maps and value maps.
 * <p>
 * To avoid attribute names that may conflict with the DynamoDB reserved words, the expressions builder will automatically
 * transform every component of a document path into the use of an "expression attribute name" (that begins with "#") as a
 * placeholder. The actual mapping from the "expression attribute name" to the actual attribute name is automatically taken care
 * of by the builder in a "name map". Similarly, the actual mapping from the "expression attribute value" (that begins with ":")
 * to the actual attribute value is automatically taken care of by the builder in a "value map". See more information at <a href=
 * "http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ExpressionPlaceholders.html" >Using Placeholders for Attribute
 * Names and Values</a>.
 */
final class SubstitutionContext {
    private final Map<String, Integer> nameToToken =
        new LinkedHashMap<>();
    private final Map<Object, Integer> valueToToken =
        new LinkedHashMap<>();

    /**
     * Returns the name token for the given name, creating a new token as necessary.
     */
    String nameTokenFor(String name) {
        Integer token = nameToToken.computeIfAbsent(name, k -> nameToToken.size());
        return "#" + token;
    }

    /**
     * Returns the value token for the given value, creating a new token as necessary.
     */
    String valueTokenFor(Object value) {
        Integer token = valueToToken.computeIfAbsent(value, k -> valueToToken.size());
        return ":" + token;
    }

    Map<String, String> getNameMap() {
        if (nameToToken.isEmpty()) {
            return null;
        }
        Map<String, String> out = new LinkedHashMap<>();
        nameToToken.forEach((key, value) -> out.put("#" + value, key));
        return out;
    }

    Map<String, Object> getValueMap() {
        if (valueToToken.isEmpty()) {
            return null;
        }
        Map<String, Object> out = new LinkedHashMap<>();
        valueToToken.forEach((key, value) -> out.put(":" + value, key));
        return out;
    }

    // For testing
    int numNameTokens() {
        return nameToToken.size();
    }

    // For testing
    int numValueTokens() {
        return valueToToken.size();
    }

    // For testing
    String getNameByToken(int token) {
        return nameToToken.entrySet().stream().filter(e -> e.getValue().intValue() == token).findFirst().map(Map.Entry::getKey).orElse(null);
    }

    // For testing
    Object getValueByToken(int token) {
        return valueToToken.entrySet().stream().filter(e -> e.getValue().intValue() == token).findFirst().map(Map.Entry::getKey).orElse(null);
    }

    @Override
    public String toString() {
        return "name-tokens: " + nameToToken.toString() + "\n"
               + "value-tokens: " + valueToToken.toString();
    }
}