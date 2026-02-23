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

    private final HttpClientDependencies dependencies;
    private final AsyncRetryableStage<OutputT> retryableStage;
    private final AsyncHedgingStage<OutputT> hedgingStage;

    public AsyncRetryOrHedgingStage(TransformingAsyncResponseHandler<Response<OutputT>> responseHandler,
                                   HttpClientDependencies dependencies,
                                   RequestPipeline<SdkHttpFullRequest, CompletableFuture<Response<OutputT>>> requestPipeline) {
        this.dependencies = dependencies;
        this.retryableStage = new AsyncRetryableStage<>(responseHandler, dependencies, requestPipeline);
        this.hedgingStage = new AsyncHedgingStage<>(dependencies, requestPipeline);
    }

    @Override
    public CompletableFuture<Response<OutputT>> execute(SdkHttpFullRequest request,
                                                        RequestExecutionContext context) throws Exception {
        HedgingConfig resolved = HedgingConfig.resolve(
            context.requestConfig().hedgingConfig(),
            Optional.ofNullable(dependencies.clientConfiguration().option(SdkClientOption.HEDGING_CONFIG)),
            () -> Optional.empty());
        String operationName = context.executionAttributes().getAttribute(SdkExecutionAttribute.OPERATION_NAME);

        if (!resolved.shouldHedge(operationName)) {
            return retryableStage.execute(request, context);
        }
        return hedgingStage.execute(request, context);
    }
}
