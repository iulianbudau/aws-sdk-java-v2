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

package software.amazon.awssdk.retries.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.retries.DefaultRetryStrategy;
import software.amazon.awssdk.retries.api.AcquireInitialTokenResponse;
import software.amazon.awssdk.retries.api.RecordSuccessRequest;
import software.amazon.awssdk.retries.api.RecordSuccessResponse;
import software.amazon.awssdk.retries.api.TokenAcquisitionFailedException;
import software.amazon.awssdk.retries.api.internal.AcquireInitialTokenRequestImpl;
import software.amazon.awssdk.retries.internal.circuitbreaker.TokenBucketStore;

class HedgingAccountingRetryStrategyTest {
    private static final String TEST_SCOPE = "hedging-test-scope";

    @Test
    void hedgeRetryableFailure_debitsBucketAndExhaustionBlocksFurtherHedges() {
        DefaultStandardRetryStrategy.Builder builder =
            (DefaultStandardRetryStrategy.Builder) DefaultRetryStrategy.standardStrategyBuilder();
        DefaultStandardRetryStrategy strategy = (DefaultStandardRetryStrategy) builder
            .tokenBucketStore(TokenBucketStore.builder().tokenBucketMaxCapacity(10).build())
            .tokenBucketExceptionCost(5)
            .build();

        AcquireInitialTokenResponse initial = strategy.acquireInitialToken(AcquireInitialTokenRequestImpl.create(TEST_SCOPE));

        assertThat(strategy.canStartHedgeAttempt(initial.token())).isTrue();
        assertThat(strategy.recordHedgeFailure(initial.token(), new IllegalArgumentException("retryable"))).isEqualTo(5);
        assertThat(strategy.canStartHedgeAttempt(initial.token())).isTrue();
        assertThat(strategy.recordHedgeFailure(initial.token(), new IllegalArgumentException("retryable"))).isEqualTo(5);
        assertThat(strategy.canStartHedgeAttempt(initial.token())).isFalse();

        assertThatThrownBy(() -> strategy.recordHedgeFailure(initial.token(), new IllegalArgumentException("retryable")))
            .isInstanceOf(TokenAcquisitionFailedException.class);
    }

    @Test
    void postOutageSuccess_refillsCapacity_untilMaxWithoutOverflow() {
        DefaultStandardRetryStrategy.Builder builder =
            (DefaultStandardRetryStrategy.Builder) DefaultRetryStrategy.standardStrategyBuilder();
        DefaultStandardRetryStrategy strategy = (DefaultStandardRetryStrategy) builder
            .tokenBucketStore(TokenBucketStore.builder().tokenBucketMaxCapacity(10).build())
            .tokenBucketExceptionCost(5)
            .build();

        AcquireInitialTokenResponse initial = strategy.acquireInitialToken(AcquireInitialTokenRequestImpl.create(TEST_SCOPE));
        strategy.recordHedgeFailure(initial.token(), new IllegalArgumentException("retryable"));
        strategy.recordHedgeFailure(initial.token(), new IllegalArgumentException("retryable"));

        RecordSuccessResponse replenished = strategy.recordSuccess(RecordSuccessRequest.create(initial.token(), 10));
        assertThat(((DefaultRetryToken) replenished.token()).capacityRemaining()).isEqualTo(10);

        RecordSuccessResponse overflowAttempt = strategy.recordSuccess(RecordSuccessRequest.create(initial.token(), 100));
        assertThat(((DefaultRetryToken) overflowAttempt.token()).capacityRemaining()).isEqualTo(10);
    }
}
