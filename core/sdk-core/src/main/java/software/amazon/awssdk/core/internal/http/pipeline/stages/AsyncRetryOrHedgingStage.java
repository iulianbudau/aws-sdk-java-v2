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
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.core.Response;
import software.amazon.awssdk.core.client.config.HedgingConfig;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.core.interceptor.SdkExecutionAttribute;
import software.amazon.awssdk.core.internal.http.HttpClientDependencies;
import software.amazon.awssdk.core.internal.http.RequestExecutionContext;
import software.amazon.awssdk.core.internal.http.TransformingAsyncResponseHandler;
import software.amazon.awssdk.core.internal.http.pipeline.RequestPipeline;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.utils.Logger;

/**
 * Orchestration stage that branches on resolved {@link HedgingConfig}: when hedging is enabled and
 * allowed for the operation, delegates to {@link AsyncHedgingStage}; otherwise delegates to
 * {@link AsyncRetryableStage}.
 * <p>
 * Hedging requires a replayable request body (each attempt sends the same body). Restrict to idempotent
 * operations via {@link HedgingConfig#hedgeableOperations()}.
 */
@SdkInternalApi
public final class AsyncRetryOrHedgingStage<OutputT> implements RequestPipeline<SdkHttpFullRequest,
    CompletableFuture<Response<OutputT>>> {

    private static final Logger log = Logger.loggerFor(AsyncRetryOrHedgingStage.class);

    private final HttpClientDependencies dependencies;
    private final AsyncRetryableStage<OutputT> retryableStage;
    private final AsyncHedgingStage<OutputT> hedgingStage;

    /**
     * Creates a new AsyncRetryOrHedgingStage.
     *
     * @param responseHandler The response handler instance for the retry path (sequential execution)
     * @param responseHandlerFactory Factory to create fresh response handlers for hedging (concurrent execution).
     *                               Each hedge attempt gets its own isolated handler to prevent race conditions.
     * @param dependencies HTTP client dependencies
     * @param requestPipeline The downstream request pipeline
     */
    public AsyncRetryOrHedgingStage(TransformingAsyncResponseHandler<Response<OutputT>> responseHandler,
                                   Supplier<TransformingAsyncResponseHandler<Response<OutputT>>> responseHandlerFactory,
                                   HttpClientDependencies dependencies,
                                   RequestPipeline<SdkHttpFullRequest, CompletableFuture<Response<OutputT>>> requestPipeline) {
        this.dependencies = dependencies;
        this.retryableStage = new AsyncRetryableStage<>(responseHandler, dependencies, requestPipeline);
        this.hedgingStage = new AsyncHedgingStage<>(responseHandlerFactory, dependencies, requestPipeline);
    }

    @Override
    public CompletableFuture<Response<OutputT>> execute(SdkHttpFullRequest request,
                                                        RequestExecutionContext context) throws Exception {
        HedgingConfig resolved = HedgingConfig.resolve(
            context.requestConfig().hedgingConfig(),
            Optional.ofNullable(dependencies.clientConfiguration().option(SdkClientOption.HEDGING_CONFIG)),
            () -> Optional.empty());
        String operationName = context.executionAttributes().getAttribute(SdkExecutionAttribute.OPERATION_NAME);

        boolean shouldHedge = resolved.shouldHedge(operationName);
        log.debug(() -> String.format("[HEDGE-ROUTING] operation=%s, enabled=%s, shouldHedge=%s, maxAttempts=%d",
            operationName, resolved.enabled(), shouldHedge, resolved.maxHedgedAttempts()));
        
        if (!shouldHedge) {
            log.debug(() -> "[HEDGE-ROUTING] Using RETRY path");
            return retryableStage.execute(request, context);
        }
        log.debug(() -> "[HEDGE-ROUTING] Using HEDGING path");
        return hedgingStage.execute(request, context);
    }
}
