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

package software.amazon.awssdk.retries.api.internal;

import java.time.Duration;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.retries.api.AcquireHedgeTokenResponse;
import software.amazon.awssdk.retries.api.RetryToken;
import software.amazon.awssdk.utils.Validate;

/**
 * Implementation of {@link AcquireHedgeTokenResponse}.
 */
@SdkInternalApi
public final class AcquireHedgeTokenResponseImpl implements AcquireHedgeTokenResponse {
    private final RetryToken token;
    private final Duration delayUntilThisAttempt;

    private AcquireHedgeTokenResponseImpl(RetryToken token, Duration delayUntilThisAttempt) {
        this.token = Validate.paramNotNull(token, "token");
        this.delayUntilThisAttempt = Validate.paramNotNull(delayUntilThisAttempt, "delayUntilThisAttempt");
    }

    @Override
    public RetryToken token() {
        return token;
    }

    @Override
    public Duration delayUntilThisAttempt() {
        return delayUntilThisAttempt;
    }

    public static AcquireHedgeTokenResponse create(RetryToken token, Duration delayUntilThisAttempt) {
        return new AcquireHedgeTokenResponseImpl(token, delayUntilThisAttempt);
    }
}
