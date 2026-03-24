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

package software.amazon.awssdk.services.dynamodb;

import static software.amazon.awssdk.retries.api.BackoffStrategy.exponentialDelay;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.awscore.retry.AwsRetryPolicy;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.client.config.HedgingConfig;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.core.internal.retry.RetryPolicyAdapter;
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetrySetting;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;


/**
 * Default retry policy for DynamoDB Client.
 */
@SdkInternalApi
final class DynamoDbRetryPolicy {

    /**
     * Default max retry count for DynamoDB client, regardless of retry mode.
     **/
    private static final int MAX_ERROR_RETRY = 8;

    /**
     * Default attempts count for DynamoDB client, regardless of retry mode.
     **/
    private static final int MAX_ATTEMPTS = MAX_ERROR_RETRY + 1;

    /**
     * Default base sleep time for DynamoDB, regardless of retry mode.
     **/
    private static final Duration BASE_DELAY = Duration.ofMillis(25);

    /** Default delay before starting additional hedged attempts when hedging is enabled. */
    private static final Duration HEDGE_DELAY = Duration.ofMillis(10);

    /** Delay before the first hedged attempt for GetItem when using per-operation delay. */
    private static final Duration GET_ITEM_HEDGE_DELAY = Duration.ofMillis(8);

    /** Maximum number of hedged attempts (including the first) when hedging is enabled. */
    private static final int MAX_HEDGE_ATTEMPTS = 3;

    /** Operation names that are safe to hedge (idempotent reads). */
    private static final Set<String> HEDGEABLE_OPERATIONS =
        Collections.unmodifiableSet(new HashSet<>(Arrays.asList("GetItem", "Query", "Scan")));

    /**
     * The default back-off strategy for DynamoDB client, which increases
     * exponentially up to a max amount of delay. Compared to the SDK default
     * back-off strategy, it applies a smaller scale factor.
     */
    private static final BackoffStrategy BACKOFF_STRATEGY =
        FullJitterBackoffStrategy.builder()
                                 .baseDelay(BASE_DELAY)
                                 .maxBackoffTime(SdkDefaultRetrySetting.MAX_BACKOFF)
                                 .build();

    private DynamoDbRetryPolicy() {
    }

    /**
     * @deprecated Use instead {@link #resolveRetryStrategy}.
     */
    @Deprecated
    public static RetryPolicy resolveRetryPolicy(SdkClientConfiguration config) {
        RetryPolicy configuredRetryPolicy = config.option(SdkClientOption.RETRY_POLICY);
        if (configuredRetryPolicy != null) {
            return configuredRetryPolicy;
        }

        RetryMode retryMode = resolveRetryMode(config);
        return retryPolicyFor(retryMode);
    }

    public static RetryStrategy resolveRetryStrategy(SdkClientConfiguration config) {
        RetryStrategy configuredRetryStrategy = config.option(SdkClientOption.RETRY_STRATEGY);
        if (configuredRetryStrategy != null) {
            return configuredRetryStrategy;
        }

        RetryMode retryMode = resolveRetryMode(config);

        if (retryMode == RetryMode.ADAPTIVE) {
            return RetryPolicyAdapter.builder()
                                     .retryPolicy(retryPolicyFor(retryMode))
                                     .build();
        }

        return AwsRetryStrategy.forRetryMode(retryMode)
            .toBuilder()
            .maxAttempts(MAX_ATTEMPTS)
            .backoffStrategy(exponentialDelay(BASE_DELAY, SdkDefaultRetrySetting.MAX_BACKOFF))
            .build();
    }

    private static RetryPolicy retryPolicyFor(RetryMode retryMode) {
        return AwsRetryPolicy.forRetryMode(retryMode)
                             .toBuilder()
                             .additionalRetryConditionsAllowed(false)
                             .numRetries(MAX_ERROR_RETRY)
                             .backoffStrategy(BACKOFF_STRATEGY)
                             .build();
    }

    private static RetryMode resolveRetryMode(SdkClientConfiguration config) {
        return RetryMode.resolver()
                        .profileFile(config.option(SdkClientOption.PROFILE_FILE_SUPPLIER))
                        .profileName(config.option(SdkClientOption.PROFILE_NAME))
                        .defaultRetryMode(config.option(SdkClientOption.DEFAULT_RETRY_MODE))
                        .resolve();
    }

    /**
     * Default hedging config for DynamoDB: disabled by default, with {@link #HEDGE_DELAY} as default
     * delay, {@value #MAX_HEDGE_ATTEMPTS} max hedged attempts, GetItem-specific delay
     * {@link #GET_ITEM_HEDGE_DELAY}, and only GetItem, Query and Scan allow-listed as hedgeable.
     * Used by the DynamoDB client builder when hedging is not explicitly configured. Callers must
     * set {@code enabled(true)} to use hedging.
     */
    public static HedgingConfig defaultHedgingConfig() {
        Map<String, Duration> delayPerOperation = Collections.singletonMap("GetItem", GET_ITEM_HEDGE_DELAY);
        return HedgingConfig.builder()
                .enabled(false)
                .defaultDelay(HEDGE_DELAY)
                .maxHedgedAttempts(MAX_HEDGE_ATTEMPTS)
                .delayPerOperation(delayPerOperation)
                .hedgeableOperations(HEDGEABLE_OPERATIONS)
                .build();
    }
}
