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

import software.amazon.awssdk.annotations.Immutable;

/**
 * A path operand that refers to a <a href=
 * "http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html" >NULL</a> attribute in DynamoDB; used
 * for building expressions.
 * <p>
 * Use {@link ExpressionSpecBuilder#NULL(String)} to instantiate this class.
 */
@Immutable
public class NULL extends PathOperand {

    NULL(String path) {
        super(path);
    }

    /**
     * Returns a {@code SetAction} object (used for building update expression) of setting an attribute to null.
     */
    public final SetAction set() {
        return new SetAction(this, new LiteralOperand((Object) null));
    }
}
