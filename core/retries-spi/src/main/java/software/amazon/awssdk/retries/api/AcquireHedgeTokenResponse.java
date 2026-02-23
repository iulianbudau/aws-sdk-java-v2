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

import java.time.Duration;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.retries.api.internal.AcquireHedgeTokenResponseImpl;

/**
 * Response from {@link RetryStrategy#acquireTokenForHedgeAttempt(AcquireHedgeTokenRequest)}.
 *
 * <p>Contains the new token to use for this hedged attempt and the delay to wait before starting it
 * (typically the same as the request's delay).
 */
@SdkPublicApi
@ThreadSafe
public interface AcquireHedgeTokenResponse {

    /**
     * The token to use for this hedged attempt.
     */
    RetryToken token();

    /**
     * The delay before starting this hedged attempt (e.g. from hedging config; may be {@link Duration#ZERO}).
     */
    Duration delayUntilThisAttempt();

    /**
     * Creates a new response with the given token and delay.
     */
    static AcquireHedgeTokenResponse create(RetryToken token, Duration delayUntilThisAttempt) {
        return AcquireHedgeTokenResponseImpl.create(token, delayUntilThisAttempt);
    }
}
