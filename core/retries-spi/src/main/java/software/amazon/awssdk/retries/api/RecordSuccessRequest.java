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
import software.amazon.awssdk.retries.api.internal.RecordSuccessRequestImpl;

/**
 * Request that the calling code makes to the {@link RetryStrategy} using
 * {@link RetryStrategy#recordSuccess(RecordSuccessRequest)} to notify that the attempted execution succeeded.
 */
@SdkPublicApi
@ThreadSafe
public interface RecordSuccessRequest {
    /**
     * A {@link RetryToken} acquired a previous {@link RetryStrategy#acquireInitialToken},
     * {@link RetryStrategy#refreshRetryToken}, or {@link RetryStrategy#acquireTokenForHedgeAttempt} call.
     */
    RetryToken token();

    /**
     * When present, the number of hedged attempts that were started for this logical request.
     * The strategy should release this many units so that net token consumption for a successful
     * hedged request is zero. When empty, behavior is unchanged (release based on token capacity).
     */
    Optional<Integer> hedgedAttemptsStarted();

    /**
     * Creates a new {@link RecordSuccessRequest} instance with the given token.
     */
    static RecordSuccessRequest create(RetryToken token) {
        return RecordSuccessRequestImpl.create(token);
    }

    /**
     * Creates a new {@link RecordSuccessRequest} with the given token and hedged attempt count
     * (for use when hedging succeeded and the strategy should release N units).
     */
    static RecordSuccessRequest create(RetryToken token, int hedgedAttemptsStarted) {
        return RecordSuccessRequestImpl.create(token, hedgedAttemptsStarted);
    }

}
