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

package software.amazon.awssdk.retries.api;

import java.util.Optional;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.retries.api.internal.AcquireHedgeTokenRequestImpl;
import software.amazon.awssdk.utils.builder.CopyableBuilder;
import software.amazon.awssdk.utils.builder.ToCopyableBuilder;

/**
 * Request to acquire a token for a hedged attempt (attempt 2, 3, ΓÇª) from the retry strategy.
 *
 * <p>Used when hedging is enabled: multiple attempts are started at fixed delays; each hedged attempt
 * (after the first) should acquire a token via this method when the strategy supports it.
 *
 * @see RetryStrategy#acquireTokenForHedgeAttempt(AcquireHedgeTokenRequest)
 */
@SdkPublicApi
@ThreadSafe
public interface AcquireHedgeTokenRequest extends ToCopyableBuilder<AcquireHedgeTokenRequest.Builder, AcquireHedgeTokenRequest> {

    /**
     * The retry token from the initial acquire or a previous hedge acquire for this logical request.
     */
    RetryToken token();

    /**
     * The 1-based attempt index for this hedged attempt (2, 3, ΓÇª).
     */
    int attemptIndex();

    /**
     * Optional operation name for strategy logging or scoping.
     */
    Optional<String> operationName();

    /**
     * The delay to wait before starting this hedged attempt (from hedging config).
     */
    java.time.Duration delayUntilThisAttempt();

    /**
     * Creates a new request with the given values.
     */
    static Builder builder() {
        return AcquireHedgeTokenRequestImpl.builder();
    }

    interface Builder extends CopyableBuilder<Builder, AcquireHedgeTokenRequest> {
        Builder token(RetryToken token);

        Builder attemptIndex(int attemptIndex);

        Builder operationName(String operationName);

        Builder delayUntilThisAttempt(java.time.Duration delayUntilThisAttempt);

        AcquireHedgeTokenRequest build();
    }
}
