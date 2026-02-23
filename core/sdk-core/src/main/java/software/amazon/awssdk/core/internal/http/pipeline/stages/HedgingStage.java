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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static software.amazon.awssdk.core.internal.InternalCoreExecutionAttribute.EXECUTION_ATTEMPT;
import static software.amazon.awssdk.core.internal.InternalCoreExecutionAttribute.RETRY_TOKEN;
import static software.amazon.awssdk.core.internal.http.pipeline.stages.utils.RetryableStageHelper.SDK_RETRY_INFO_HEADER;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.core.Response;
import software.amazon.awssdk.core.client.config.HedgingConfig;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.http.ExecutionContext;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.internal.http.HttpClientDependencies;
import software.amazon.awssdk.core.internal.http.RequestExecutionContext;
import software.amazon.awssdk.core.internal.http.pipeline.RequestPipeline;
import software.amazon.awssdk.core.internal.http.pipeline.RequestToResponsePipeline;
import software.amazon.awssdk.core.internal.http.pipeline.stages.utils.RetryableStageHelper;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.retries.api.AcquireHedgeTokenRequest;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.retries.api.RetryToken;
import software.amazon.awssdk.retries.api.TokenAcquisitionFailedException;

/**
 * Sync wrapper around the pipeline for a single request to provide hedging functionality.
 * When hedging is enabled, multiple attempts are started at fixed delays on an executor;
 * the first successful response wins and other attempts are cancelled.
 * <p>
 * Hedging requires a replayable request body. Restrict to idempotent operations via
 * {@link HedgingConfig#hedgeableOperations()}.
 */
@SdkInternalApi
public final class HedgingStage<OutputT> implements RequestToResponsePipeline<OutputT> {

    private final RequestPipeline<SdkHttpFullRequest, Response<OutputT>> requestPipeline;
    private final ScheduledExecutorService executor;
    private final HttpClientDependencies dependencies;

    public HedgingStage(HttpClientDependencies dependencies,
                       RequestPipeline<SdkHttpFullRequest, Response<OutputT>> requestPipeline) {
        this.dependencies = dependencies;
        this.executor = dependencies.clientConfiguration()
            .option(software.amazon.awssdk.core.client.config.SdkClientOption.SCHEDULED_EXECUTOR_SERVICE);
        this.requestPipeline = requestPipeline;
    }

    @Override
    public Response<OutputT> execute(SdkHttpFullRequest request, RequestExecutionContext context) throws Exception {
        return new HedgingExecutor(request, context).execute();
    }

    private final class HedgingExecutor {
        private final SdkHttpFullRequest request;
        private final RequestExecutionContext context;
        private final HedgingConfig hedgingConfig;
        private final String operationName;
        private final RetryStrategy retryStrategy;
        private final RetryableStageHelper helperForAttempt1;
        private final CompletableFuture<Response<OutputT>> userFuture;
        private final HedgingState state;

        private HedgingExecutor(SdkHttpFullRequest request, RequestExecutionContext context) {
            this.request = request;
            this.context = context;
            this.hedgingConfig = HedgingConfig.resolve(
                context.requestConfig().hedgingConfig(),
                java.util.Optional.ofNullable(dependencies.clientConfiguration()
                    .option(software.amazon.awssdk.core.client.config.SdkClientOption.HEDGING_CONFIG)),
                () -> java.util.Optional.empty());
            this.operationName = context.executionAttributes()
                .getAttribute(software.amazon.awssdk.core.interceptor.SdkExecutionAttribute.OPERATION_NAME);
            this.retryStrategy = dependencies.clientConfiguration()
                .option(software.amazon.awssdk.core.client.config.SdkClientOption.RETRY_STRATEGY);
            this.helperForAttempt1 = new RetryableStageHelper(request, context, dependencies);
            this.userFuture = new CompletableFuture<>();
            int maxAttempts = retryStrategy != null ? retryStrategy.maxAttempts() : 3;
            this.state = new HedgingState(hedgingConfig.maxHedgedAttempts(), maxAttempts);
        }

        public Response<OutputT> execute() throws Exception {
            Duration initialDelay = helperForAttempt1.acquireInitialToken();
            if (!initialDelay.isZero()) {
                MILLISECONDS.sleep(initialDelay.toMillis());
            }
            RetryToken initialToken = context.executionAttributes().getAttribute(RETRY_TOKEN);
            runHedgingLoop(initialToken);
            return waitForResult();
        }

        private void runHedgingLoop(RetryToken initialToken) {
            startAttempt1(initialToken);
            scheduleHedgedAttempts(initialToken);
        }

        private void startAttempt1(RetryToken initialToken) {
            state.incrementAttemptsStarted();
            helperForAttempt1.startingAttempt();
            helperForAttempt1.logSendingRequest();
            SdkHttpFullRequest request1 = request.toBuilder()
                .putHeader(SDK_RETRY_INFO_HEADER, "attempt=1; max=" + state.maxAttempts())
                .build();
            CompletableFuture<Response<OutputT>> future1 = CompletableFuture.supplyAsync(() -> {
                try {
                    return requestPipeline.execute(request1, context);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, executor);
            state.addAttemptFuture(future1);
            future1.whenComplete((response, ex) -> handleCompletion(response, ex, 1, context, initialToken));
        }

        private void scheduleHedgedAttempts(RetryToken initialToken) {
            for (int k = 2; k <= hedgingConfig.maxHedgedAttempts(); k++) {
                int attemptIndex = k;
                Duration delay = hedgingConfig.delayBeforeAttempt(k, operationName);
                long delayMs = delay.toMillis();
                RetryToken tokenForK = acquireHedgeToken(initialToken, attemptIndex, delay);
                if (tokenForK == null) {
                    continue;
                }
                RetryToken tokenK = tokenForK;
                executor.schedule(() -> {
                    if (userFuture.isDone()) {
                        return;
                    }
                    startHedgedAttempt(attemptIndex, tokenK);
                }, delayMs, MILLISECONDS);
            }
        }

        private RetryToken acquireHedgeToken(RetryToken initialToken, int attemptIndex, Duration delay) {
            if (retryStrategy == null || !retryStrategy.supportsHedging()) {
                return initialToken;
            }
            try {
                AcquireHedgeTokenRequest hedgeRequest = AcquireHedgeTokenRequest.builder()
                    .token(initialToken)
                    .attemptIndex(attemptIndex)
                    .operationName(operationName)
                    .delayUntilThisAttempt(delay)
                    .build();
                return retryStrategy.acquireTokenForHedgeAttempt(hedgeRequest).token();
            } catch (TokenAcquisitionFailedException e) {
                return null;
            }
        }

        private void startHedgedAttempt(int attemptIndex, RetryToken token) {
            state.incrementAttemptsStarted();
            RequestExecutionContext contextK = copyContextWithTokenAndAttempt(context, token, attemptIndex);
            SdkHttpFullRequest requestK = request.toBuilder()
                .putHeader(SDK_RETRY_INFO_HEADER, "attempt=" + attemptIndex + "; max=" + state.maxAttempts())
                .build();
            CompletableFuture<Response<OutputT>> futureK = CompletableFuture.supplyAsync(() -> {
                try {
                    return requestPipeline.execute(requestK, contextK);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, executor);
            state.addAttemptFuture(futureK);
            futureK.whenComplete((response, ex) -> handleCompletion(response, ex, attemptIndex, contextK, token));
        }

        private void handleCompletion(Response<OutputT> response,
                                     Throwable exception,
                                     int attemptIndex,
                                     RequestExecutionContext contextForAttempt,
                                     RetryToken token) {
            if (userFuture.isDone()) {
                return;
            }
            if (exception != null) {
                handleFailure(exception, attemptIndex);
                return;
            }
            if (response != null && response.isSuccess()) {
                handleSuccess(response, contextForAttempt, token);
                return;
            }
            if (response != null && !response.isSuccess()) {
                handleUnsuccessfulResponse(response, attemptIndex);
            }
        }

        private void handleFailure(Throwable exception, int attemptIndex) {
            Throwable cause = exception instanceof RuntimeException && exception.getCause() != null
                ? exception.getCause() : exception;
            SdkException sdkEx = cause instanceof SdkException ? (SdkException) cause
                : SdkException.create("Hedge attempt " + attemptIndex + " failed", cause);
            state.addFailure(sdkEx);
            if (state.incrementCompletedCount() >= hedgingConfig.maxHedgedAttempts()) {
                completeWithAggregatedFailure();
            }
        }

        private void handleSuccess(Response<OutputT> response,
                                   RequestExecutionContext contextForAttempt,
                                   RetryToken token) {
            if (userFuture.complete(response)) {
                state.cancelAllAttempts();
                if (retryStrategy != null && token != null) {
                    helperForAttempt1.recordSuccessWithTokenAndHedgeCount(token, state.attemptsStarted());
                }
            }
        }

        private void handleUnsuccessfulResponse(Response<OutputT> response, int attemptIndex) {
            helperForAttempt1.setLastResponse(response.httpResponse());
            helperForAttempt1.adjustClockIfClockSkew(response);
            SdkException sdkEx = response.exception();
            state.addFailure(sdkEx);
            if (state.incrementCompletedCount() >= hedgingConfig.maxHedgedAttempts()) {
                completeWithAggregatedFailure();
            }
        }

        private void completeWithAggregatedFailure() {
            if (userFuture.isDone()) {
                return;
            }
            List<Throwable> failures = state.failures();
            Throwable primary = failures.isEmpty()
                ? new IllegalStateException("All hedged attempts failed with no exception")
                : failures.get(0);
            if (primary instanceof SdkException) {
                SdkException sdkEx = (SdkException) primary;
                SdkException newEx = sdkEx.toBuilder().numAttempts(failures.size()).build();
                for (int i = 1; i < failures.size(); i++) {
                    newEx.addSuppressed(failures.get(i));
                }
                userFuture.completeExceptionally(newEx);
            } else {
                for (int i = 1; i < failures.size(); i++) {
                    primary.addSuppressed(failures.get(i));
                }
                userFuture.completeExceptionally(primary);
            }
        }

        private Response<OutputT> waitForResult() throws Exception {
            try {
                return userFuture.get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof Exception) {
                    throw (Exception) cause;
                }
                throw new Exception(cause);
            }
        }

        private RequestExecutionContext copyContextWithTokenAndAttempt(RequestExecutionContext context,
                                                                       RetryToken token,
                                                                       int attempt) {
            ExecutionAttributes attrs = context.executionAttributes().copy();
            attrs.putAttribute(RETRY_TOKEN, token);
            attrs.putAttribute(EXECUTION_ATTEMPT, attempt);
            ExecutionContext execContext = context.executionContext().toBuilder()
                .executionAttributes(attrs)
                .build();
            return RequestExecutionContext.builder()
                .originalRequest(context.originalRequest())
                .requestProvider(context.requestProvider())
                .executionContext(execContext)
                .build();
        }
    }

    /**
     * Encapsulates shared state for hedging execution to reduce parameter passing.
     */
    private static final class HedgingState {
        private final List<CompletableFuture<?>> attemptFutures;
        private final AtomicInteger completedCount;
        private final AtomicInteger attemptsStarted;
        private final List<Throwable> failures;
        private final Object failuresLock;
        private final int maxAttempts;

        HedgingState(int maxHedgedAttempts, int maxAttempts) {
            this.attemptFutures = new ArrayList<>(maxHedgedAttempts);
            this.completedCount = new AtomicInteger(0);
            this.attemptsStarted = new AtomicInteger(0);
            this.failures = new ArrayList<>();
            this.failuresLock = new Object();
            this.maxAttempts = maxAttempts;
        }

        void incrementAttemptsStarted() {
            attemptsStarted.incrementAndGet();
        }

        int attemptsStarted() {
            return attemptsStarted.get();
        }

        void addAttemptFuture(CompletableFuture<?> future) {
            synchronized (attemptFutures) {
                attemptFutures.add(future);
            }
        }

        void cancelAllAttempts() {
            for (CompletableFuture<?> f : attemptFutures) {
                f.cancel(false);
            }
        }

        void addFailure(Throwable failure) {
            synchronized (failuresLock) {
                failures.add(failure);
            }
        }

        List<Throwable> failures() {
            synchronized (failuresLock) {
                return new ArrayList<>(failures);
            }
        }

        int incrementCompletedCount() {
            return completedCount.incrementAndGet();
        }

        int maxAttempts() {
            return maxAttempts;
        }
    }
}
