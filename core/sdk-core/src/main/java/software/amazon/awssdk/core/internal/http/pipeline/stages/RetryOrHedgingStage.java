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

package software.amazon.awssdk.core.internal.http.pipeline.stages;

import java.util.Optional;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.core.Response;
import software.amazon.awssdk.core.client.config.HedgingConfig;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.core.interceptor.SdkExecutionAttribute;
import software.amazon.awssdk.core.internal.http.HttpClientDependencies;
import software.amazon.awssdk.core.internal.http.RequestExecutionContext;
import software.amazon.awssdk.core.internal.http.pipeline.RequestPipeline;
import software.amazon.awssdk.core.internal.http.pipeline.RequestToResponsePipeline;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.utils.Logger;

/**
 * Sync orchestration stage that branches on resolved {@link HedgingConfig}: when hedging is enabled
 * and allowed for the operation, delegates to {@link HedgingStage}; otherwise delegates to
 * {@link RetryableStage}.
 * <p>
 * Hedging requires a replayable request body. Restrict to idempotent operations via
 * {@link HedgingConfig#hedgeableOperations()}.
 */
@SdkInternalApi
public final class RetryOrHedgingStage<OutputT> implements RequestToResponsePipeline<OutputT> {
    private static final Logger log = Logger.loggerFor(RetryOrHedgingStage.class);

    private final HttpClientDependencies dependencies;
    private final RetryableStage<OutputT> retryableStage;
    private final HedgingStage<OutputT> hedgingStage;

    public RetryOrHedgingStage(HttpClientDependencies dependencies,
                               RequestPipeline<SdkHttpFullRequest, Response<OutputT>> requestPipeline) {
        this.dependencies = dependencies;
        this.retryableStage = new RetryableStage<>(dependencies, requestPipeline);
        this.hedgingStage = new HedgingStage<>(dependencies, requestPipeline);
    }

    @Override
    public Response<OutputT> execute(SdkHttpFullRequest request, RequestExecutionContext context) throws Exception {
        HedgingConfig resolved = HedgingConfig.resolve(
            context.requestConfig().hedgingConfig(),
            Optional.ofNullable(dependencies.clientConfiguration().option(SdkClientOption.HEDGING_CONFIG)),
            () -> Optional.empty());
        String operationName = context.executionAttributes().getAttribute(SdkExecutionAttribute.OPERATION_NAME);
        HedgingConfig.OperationHedgingPolicy policy = resolved.policyForOperation(operationName);
        boolean shouldHedge = resolved.shouldHedge(operationName);
        log.debug(() -> buildHedgingSummary(resolved, shouldHedge, operationName, policy));

        if (!shouldHedge) {
            return retryableStage.execute(request, context);
        }
        return hedgingStage.execute(request, context);
    }

    private static String buildHedgingSummary(HedgingConfig resolved,
                                              boolean shouldHedge,
                                              String operationName,
                                              HedgingConfig.OperationHedgingPolicy policy) {
        HedgingConfig.DelayConfig delayConfig = policy.delayConfig();
        String delayConfigType = delayConfig == null ? "null" : delayConfig.getClass().getSimpleName();
        String percentile = "n/a";
        String sampleSize = "n/a";
        String minSamplesRequired = "n/a";
        String minDelay = "n/a";
        String maxDelay = "n/a";
        String fallbackDelay = "n/a";
        if (delayConfig instanceof HedgingConfig.AdaptiveDelayConfig) {
            HedgingConfig.AdaptiveDelayConfig adaptive = (HedgingConfig.AdaptiveDelayConfig) delayConfig;
            percentile = String.format("%.3f", adaptive.percentile());
            sampleSize = Integer.toString(adaptive.sampleSize());
            minSamplesRequired = Integer.toString(adaptive.minSamplesRequired());
            minDelay = adaptive.minDelay() == null ? "null" : adaptive.minDelay().toMillis() + "ms";
            maxDelay = adaptive.maxDelay() == null ? "null" : adaptive.maxDelay().toMillis() + "ms";
            fallbackDelay = adaptive.fallbackDelay().toMillis() + "ms";
        }
        return String.format(
            "[HEDGE-ADAPTIVE] reason=HEDGING_CONFIG_SUMMARY_SYNC enabled=%s shouldHedge=%s operationName=%s "
            + "delayConfigType=%s percentile=%s sampleSize=%s minSamplesRequired=%s minDelay=%s maxDelay=%s "
            + "fallbackDelay=%s maxHedgedAttempts=%d",
            resolved.enabled(),
            shouldHedge,
            operationName,
            delayConfigType,
            percentile,
            sampleSize,
            minSamplesRequired,
            minDelay,
            maxDelay,
            fallbackDelay,
            policy.maxHedgedAttempts());
    }
}
