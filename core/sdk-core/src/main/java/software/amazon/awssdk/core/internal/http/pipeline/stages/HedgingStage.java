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
import static software.amazon.awssdk.core.metrics.CoreMetric.HEDGE_COUNT;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.core.Response;
import software.amazon.awssdk.core.client.config.HedgingConfig;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.http.ExecutionContext;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.SdkExecutionAttribute;
import software.amazon.awssdk.core.internal.http.HttpClientDependencies;
import software.amazon.awssdk.core.internal.http.RequestExecutionContext;
import software.amazon.awssdk.core.internal.http.pipeline.RequestPipeline;
import software.amazon.awssdk.core.internal.http.pipeline.RequestToResponsePipeline;
import software.amazon.awssdk.core.internal.http.pipeline.stages.utils.HedgingStageHelper;
import software.amazon.awssdk.core.internal.http.pipeline.stages.utils.RetryableStageHelper;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.retries.api.AcquireHedgeTokenRequest;
import software.amazon.awssdk.retries.api.RecordSuccessRequest;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.retries.api.RetryToken;
import software.amazon.awssdk.retries.api.TokenAcquisitionFailedException;
import software.amazon.awssdk.utils.Logger;

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
    private static final Logger log = Logger.loggerFor(HedgingStage.class);

    private final RequestPipeline<SdkHttpFullRequest, Response<OutputT>> requestPipeline;
    private final ScheduledExecutorService executor;
    private final HttpClientDependencies dependencies;

    public HedgingStage(HttpClientDependencies dependencies,
                       RequestPipeline<SdkHttpFullRequest, Response<OutputT>> requestPipeline) {
        this.dependencies = dependencies;
        this.executor = dependencies.clientConfiguration()
            .option(SdkClientOption.SCHEDULED_EXECUTOR_SERVICE);
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
        private final ExecutionAttributes baseExecutionAttributes;
        private final AtomicBoolean hedgeCountReported = new AtomicBoolean(false);
        private final AtomicReference<HedgingExecutionState> executionState =
            new AtomicReference<>(HedgingExecutionState.RUNNING);
        private final AtomicInteger totalBudget = new AtomicInteger(0);
        private final AtomicInteger consumedBudget = new AtomicInteger(0);

        private HedgingExecutor(SdkHttpFullRequest request, RequestExecutionContext context) {
            this.request = request;
            this.context = context;
            this.baseExecutionAttributes = context.executionAttributes().copy();
            this.hedgingConfig = HedgingConfig.resolve(
                context.requestConfig().hedgingConfig(),
                java.util.Optional.ofNullable(dependencies.clientConfiguration()
                    .option(SdkClientOption.HEDGING_CONFIG)),
                Optional::empty);
            this.operationName = context.executionAttributes()
                .getAttribute(SdkExecutionAttribute.OPERATION_NAME);
            this.retryStrategy = dependencies.clientConfiguration()
                .option(SdkClientOption.RETRY_STRATEGY);
            this.helperForAttempt1 = new RetryableStageHelper(request, context, dependencies);
            this.userFuture = new CompletableFuture<>();
            this.state = new HedgingState(hedgingConfig.maxHedgedAttempts());
        }

        public Response<OutputT> execute() throws Exception {
            setupHedgeCountReporting();
            Duration initialDelay = helperForAttempt1.acquireInitialToken();
            if (!initialDelay.isZero()) {
                MILLISECONDS.sleep(initialDelay.toMillis());
            }
            RetryToken initialToken = context.executionAttributes().getAttribute(RETRY_TOKEN);
            runHedgingLoop(initialToken);
            return waitForResult();
        }

        private void setupHedgeCountReporting() {
            userFuture.whenComplete((r, t) -> state.cancelAllAttempts());
        }

        private boolean isRunning() {
            return executionState.get() == HedgingExecutionState.RUNNING;
        }

        private void consumeBudgetAndCheckExhaustion() {
            int consumed = consumedBudget.incrementAndGet();
            if (consumed >= totalBudget.get() && isRunning()) {
                completeWithAggregatedFailure();
            }
        }

        private void runHedgingLoop(RetryToken initialToken) {
            try {
                int maxAttempts = hedgingConfig.maxHedgedAttempts();
                totalBudget.set(maxAttempts);
                scheduleHedgedAttempts(initialToken);
                startAttempt1(initialToken);
            } catch (Throwable t) {
                if (!userFuture.isDone()) {
                    userFuture.completeExceptionally(t);
                }
            }
        }

        private void startAttempt1(RetryToken initialToken) {
            state.incrementAttemptsStarted();
            helperForAttempt1.startingAttempt();
            helperForAttempt1.logSendingRequest();
            SdkHttpFullRequest request1 = request.toBuilder()
                .putHeader(HedgingStageHelper.SDK_HEDGE_INFO_HEADER,
                    HedgingStageHelper.buildHedgeInfoHeaderValue(1, totalBudget.get(), 0))
                .build();
            CompletableFuture<Response<OutputT>> future1 = CompletableFuture.supplyAsync(() -> {
                try {
                    return requestPipeline.execute(request1, context);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, executor);
            state.addAttemptFuture(future1);
            future1.whenComplete((response, ex) -> handleCompletion(response, ex, 1, initialToken));
        }

        private void scheduleHedgedAttempts(RetryToken initialToken) {
            for (int k = 2; k <= hedgingConfig.maxHedgedAttempts(); k++) {
                int attemptIndex = k;
                Duration delay = hedgingConfig.delayBeforeAttempt(k, operationName);
                long delayMs = delay.toMillis();
                try {
                    ScheduledFuture<?> scheduled = executor.schedule(() -> {
                        if (!isRunning()) {
                            consumeBudgetAndCheckExhaustion();
                            return;
                        }
                        RetryToken tokenK = acquireHedgeToken(initialToken, attemptIndex, delay);
                        if (tokenK == null) {
                            consumeBudgetAndCheckExhaustion();
                            return;
                        }
                        try {
                            startHedgedAttempt(attemptIndex, tokenK, delayMs);
                        } catch (Throwable t) {
                            handleCompletion(null, t, attemptIndex, tokenK);
                        }
                    }, delayMs, MILLISECONDS);
                    state.addScheduledFuture(scheduled);
                } catch (RejectedExecutionException e) {
                    consumeBudgetAndCheckExhaustion();
                }
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

        private void startHedgedAttempt(int attemptIndex, RetryToken token, long delayMs) {
            state.incrementAttemptsStarted();
            RequestExecutionContext contextK = copyContextWithTokenAndAttempt(context, token, attemptIndex);
            SdkHttpFullRequest requestK = request.toBuilder()
                .putHeader(HedgingStageHelper.SDK_HEDGE_INFO_HEADER,
                    HedgingStageHelper.buildHedgeInfoHeaderValue(attemptIndex, totalBudget.get(), delayMs))
                .build();
            CompletableFuture<Response<OutputT>> futureK = CompletableFuture.supplyAsync(() -> {
                try {
                    return requestPipeline.execute(requestK, contextK);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, executor);
            state.addAttemptFuture(futureK);
            futureK.whenComplete((response, ex) -> handleCompletion(response, ex, attemptIndex, token));
        }

        private void handleCompletion(Response<OutputT> response,
                                     Throwable exception,
                                     int attemptIndex,
                                     RetryToken token) {
            try {
                // Early exit if already done
                if (userFuture.isDone()) {
                    return;
                }
                if (exception != null) {
                    if (isCancellation(exception)) {
                        consumeBudgetAndCheckExhaustion();
                        return;
                    }
                    handleFailure(exception, attemptIndex);
                    return;
                }
                if (response != null && response.isSuccess()) {
                    handleSuccess(response, token);
                    return;
                }
                if (response != null) {
                    handleUnsuccessfulResponse(response);
                    return;
                }
                state.addFailure(
                    SdkException.create("Hedge attempt completed with null response and null exception", null));
                consumeBudgetAndCheckExhaustion();
            } catch (Throwable t) {
                userFuture.completeExceptionally(
                    SdkException.create("Unexpected error in hedging handleCompletion", t));
            }
        }

        private void handleFailure(Throwable exception, int attemptIndex) {
            Throwable cause = exception instanceof RuntimeException && exception.getCause() != null
                ? exception.getCause() : exception;
            SdkException sdkEx = cause instanceof SdkException ? (SdkException) cause
                : SdkException.create("Hedge attempt " + attemptIndex + " failed", cause);
            state.addFailure(sdkEx);
            if (isNonRetryable(sdkEx)) {
                tryCompleteWithFailure(sdkEx);
                return;
            }
            consumeBudgetAndCheckExhaustion();
        }

        private void handleSuccess(Response<OutputT> response, RetryToken token) {
            if (executionState.compareAndSet(HedgingExecutionState.RUNNING, HedgingExecutionState.COMPLETING)) {
                reportHedgeCountOnce();
                if (retryStrategy != null && token != null) {
                    helperForAttempt1.recordSuccessForHedging(token, state.attemptsStarted());
                }
                state.cancelAllAttempts();
                userFuture.complete(response);
                executionState.set(HedgingExecutionState.COMPLETED);
            } else {
                consumeBudgetAndCheckExhaustion();
            }
        }

        private void tryCompleteWithFailure(Throwable failure) {
            if (executionState.compareAndSet(HedgingExecutionState.RUNNING, HedgingExecutionState.COMPLETING)) {
                state.cancelAllAttempts();
                reportHedgeCountOnce();
                releaseHedgeTokensOnFailure();
                userFuture.completeExceptionally(failure);
                executionState.set(HedgingExecutionState.COMPLETED);
            }
        }

        private void handleUnsuccessfulResponse(Response<OutputT> response) {
            helperForAttempt1.setLastResponse(response.httpResponse());
            helperForAttempt1.adjustClockIfClockSkew(response);
            SdkException sdkEx = response.exception();
            state.addFailure(sdkEx);
            if (sdkEx != null && isNonRetryable(sdkEx)) {
                tryCompleteWithFailure(sdkEx);
                return;
            }
            consumeBudgetAndCheckExhaustion();
        }

        private boolean isNonRetryable(SdkException sdkEx) {
            // Use the retry strategy's predicates for consistent retryability checks
            return !helperForAttempt1.isRetryableByPredicates(sdkEx);
        }

        private boolean isCancellation(Throwable exception) {
            if (exception instanceof CancellationException) {
                return true;
            }
            Throwable cause = exception.getCause();
            return cause != null && cause instanceof CancellationException;
        }

        private void completeWithAggregatedFailure() {
            if (!executionState.compareAndSet(HedgingExecutionState.RUNNING, HedgingExecutionState.COMPLETING)) {
                return;
            }
            state.cancelAllAttempts();
            reportHedgeCountOnce();
            releaseHedgeTokensOnFailure();
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
            executionState.set(HedgingExecutionState.COMPLETED);
        }

        private void releaseHedgeTokensOnFailure() {
            if (retryStrategy == null) {
                return;
            }
            int attemptsStarted = state.attemptsStarted();
            if (attemptsStarted <= 1) {
                // No hedge attempts were started, nothing to release
                return;
            }
            // Use the initial token to release hedge tokens. The recordSuccess method with hedgedAttemptsStarted
            // will release (N-1)*exceptionCost tokens back to the bucket.
            // Note: We call recordSuccess even on failure to release tokens - this is correct because
            // the token bucket should not penalize us for hedge attempts that were cancelled/failed.
            RetryToken initialToken = context.executionAttributes().getAttribute(RETRY_TOKEN);
            if (initialToken != null) {
                try {
                    // Call recordSuccess directly on retryStrategy to release tokens without reporting metrics
                    RecordSuccessRequest request = RecordSuccessRequest.create(initialToken, attemptsStarted);
                    retryStrategy.recordSuccess(request);
                } catch (Exception e) {
                    // Ignore any exception during token release - this is best-effort cleanup
                    log.debug(() -> "Failed to release hedge tokens on failure", e);
                }
            }
        }

        private void reportHedgeCountOnce() {
            if (hedgeCountReported.compareAndSet(false, true)) {
                int hedgeCount = Math.max(0, state.attemptsStarted() - 1);
                if (context.executionContext().metricCollector() != null) {
                    context.executionContext().metricCollector().reportMetric(HEDGE_COUNT, hedgeCount);
                }
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
            ExecutionAttributes attrs = baseExecutionAttributes.copy();
            attrs.putAttribute(RETRY_TOKEN, token);
            attrs.putAttribute(EXECUTION_ATTEMPT, attempt);
            ExecutionContext execContext = context.executionContext().toBuilder()
                .executionAttributes(attrs)
                .build();
            RequestExecutionContext newContext = RequestExecutionContext.builder()
                .originalRequest(context.originalRequest())
                .requestProvider(context.requestProvider())
                .executionContext(execContext)
                .build();
            newContext.apiCallTimeoutTracker(context.apiCallTimeoutTracker());
            newContext.apiCallAttemptTimeoutTracker(context.apiCallAttemptTimeoutTracker());
            return newContext;
        }
    }

    /**
     * Encapsulates shared state for hedging execution to reduce parameter passing.
     */
    private static final class HedgingState {
        private final List<CompletableFuture<?>> attemptFutures;
        private final List<ScheduledFuture<?>> scheduledFutures;
        private final Object futuresLock = new Object();
        private final AtomicInteger attemptsStarted;
        private final List<Throwable> failures;
        private final Object failuresLock;
        private final int maxHedgedAttempts;

        HedgingState(int maxHedgedAttempts) {
            this.attemptFutures = new ArrayList<>(maxHedgedAttempts);
            this.scheduledFutures = new ArrayList<>(maxHedgedAttempts);
            this.attemptsStarted = new AtomicInteger(0);
            this.failures = new ArrayList<>();
            this.failuresLock = new Object();
            this.maxHedgedAttempts = maxHedgedAttempts;
        }

        void incrementAttemptsStarted() {
            attemptsStarted.incrementAndGet();
        }

        int attemptsStarted() {
            return attemptsStarted.get();
        }

        void addAttemptFuture(CompletableFuture<?> future) {
            synchronized (futuresLock) {
                attemptFutures.add(future);
            }
        }

        void addScheduledFuture(ScheduledFuture<?> future) {
            synchronized (futuresLock) {
                scheduledFutures.add(future);
            }
        }

        void cancelAllAttempts() {
            List<CompletableFuture<?>> attemptSnapshot;
            List<ScheduledFuture<?>> scheduledSnapshot;
            synchronized (futuresLock) {
                attemptSnapshot = new ArrayList<>(attemptFutures);
                scheduledSnapshot = new ArrayList<>(scheduledFutures);
            }
            for (CompletableFuture<?> f : attemptSnapshot) {
                f.cancel(false);
            }
            for (ScheduledFuture<?> sf : scheduledSnapshot) {
                sf.cancel(false);
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

        int maxHedgedAttempts() {
            return maxHedgedAttempts;
        }
    }
}
