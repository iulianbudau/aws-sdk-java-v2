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

package software.amazon.awssdk.core.internal.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.awssdk.core.client.config.SdkClientOption.HEDGING_LATENCY_TRACKER;
import static utils.HttpTestUtils.testClientConfiguration;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.internal.http.AmazonAsyncHttpClient;
import software.amazon.awssdk.core.internal.http.AmazonSyncHttpClient;
import software.amazon.awssdk.core.internal.http.HttpClientDependencies;
import software.amazon.awssdk.core.internal.http.pipeline.stages.utils.HedgingLatencyTracker;

/**
 * Verifies that per-request {@link SdkClientConfiguration} merges preserve the hedging latency tracker initialized by
 * the HTTP client.
 */
class BaseClientHandlerHedgingLatencyTrackerTest {

    @Test
    void mergeRequestClientConfiguration_preservesTrackerFromHttpClient() {
        SdkClientConfiguration requestConfiguration = testClientConfiguration();
        SdkClientConfiguration httpClientConfiguration = requestConfiguration.toBuilder()
                                                                             .lazyOptionIfAbsent(HEDGING_LATENCY_TRACKER,
                                                                                                 c -> new HedgingLatencyTracker())
                                                                             .build();

        SdkClientConfiguration merged =
            BaseClientHandler.mergeRequestClientConfiguration(requestConfiguration, httpClientConfiguration);

        assertThat(merged.option(HEDGING_LATENCY_TRACKER)).isNotNull();
        assertThat(merged.option(HEDGING_LATENCY_TRACKER))
            .isSameAs(httpClientConfiguration.option(HEDGING_LATENCY_TRACKER));
    }

    @Test
    void mergeRequestClientConfiguration_sameTrackerInstanceAcrossRepeatedMerges() {
        SdkClientConfiguration requestConfiguration = testClientConfiguration();
        SdkClientConfiguration httpClientConfiguration = requestConfiguration.toBuilder()
                                                                             .lazyOptionIfAbsent(HEDGING_LATENCY_TRACKER,
                                                                                                 c -> new HedgingLatencyTracker())
                                                                             .build();

        SdkClientConfiguration firstMerge =
            BaseClientHandler.mergeRequestClientConfiguration(requestConfiguration, httpClientConfiguration);
        SdkClientConfiguration secondMerge =
            BaseClientHandler.mergeRequestClientConfiguration(requestConfiguration, httpClientConfiguration);

        assertThat(firstMerge.option(HEDGING_LATENCY_TRACKER))
            .isSameAs(secondMerge.option(HEDGING_LATENCY_TRACKER));
    }

    @Test
    void mergeRequestClientConfiguration_createsTrackerWhenAbsentOnBothConfigurations() {
        SdkClientConfiguration requestConfiguration = testClientConfiguration();
        SdkClientConfiguration httpClientConfiguration = testClientConfiguration();

        SdkClientConfiguration merged =
            BaseClientHandler.mergeRequestClientConfiguration(requestConfiguration, httpClientConfiguration);

        assertThat(merged.option(HEDGING_LATENCY_TRACKER)).isNotNull();
    }

    @Test
    void invokePathMerge_asyncHttpClientDependenciesRetainTracker() {
        SdkClientConfiguration handlerConfiguration = testClientConfiguration();
        AmazonAsyncHttpClient httpClient = new AmazonAsyncHttpClient(handlerConfiguration);

        SdkClientConfiguration mergedConfiguration = applyInvokePathMerge(handlerConfiguration, httpClient);

        assertThat(mergedConfiguration.option(HEDGING_LATENCY_TRACKER)).isNotNull();
        assertThat(handlerConfiguration.option(HEDGING_LATENCY_TRACKER)).isNull();
    }

    @Test
    void invokePathMerge_syncHttpClientDependenciesRetainTracker() {
        SdkClientConfiguration handlerConfiguration = testClientConfiguration();
        AmazonSyncHttpClient httpClient = new AmazonSyncHttpClient(handlerConfiguration);

        SdkClientConfiguration mergedConfiguration = applyInvokePathMerge(handlerConfiguration, httpClient);

        assertThat(mergedConfiguration.option(HEDGING_LATENCY_TRACKER)).isNotNull();
        assertThat(handlerConfiguration.option(HEDGING_LATENCY_TRACKER)).isNull();
    }

    /**
     * Simulates {@link BaseAsyncClientHandler} / {@link BaseSyncClientHandler} replacing dependencies configuration on
     * each request.
     */
    private static SdkClientConfiguration applyInvokePathMerge(SdkClientConfiguration requestConfiguration,
                                                               AmazonAsyncHttpClient httpClient) {
        HttpClientDependencies.Builder dependenciesBuilder = httpClient.requestExecutionBuilder()
                                                                     .httpClientDependencies()
                                                                     .toBuilder();
        dependenciesBuilder.clientConfiguration(
            BaseClientHandler.mergeRequestClientConfiguration(requestConfiguration,
                                                              dependenciesBuilder.build().clientConfiguration()));
        return dependenciesBuilder.build().clientConfiguration();
    }

    private static SdkClientConfiguration applyInvokePathMerge(SdkClientConfiguration requestConfiguration,
                                                               AmazonSyncHttpClient httpClient) {
        HttpClientDependencies.Builder dependenciesBuilder = httpClient.requestExecutionBuilder()
                                                                     .httpClientDependencies()
                                                                     .toBuilder();
        dependenciesBuilder.clientConfiguration(
            BaseClientHandler.mergeRequestClientConfiguration(requestConfiguration,
                                                              dependenciesBuilder.build().clientConfiguration()));
        return dependenciesBuilder.build().clientConfiguration();
    }
}
