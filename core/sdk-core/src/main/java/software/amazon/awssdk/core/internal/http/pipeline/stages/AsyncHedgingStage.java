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
import static software.amazon.awssdk.core.internal.InternalCoreExecutionAttribute.HEDGING_RESPONSE_HANDLER;
import static software.amazon.awssdk.core.internal.InternalCoreExecutionAttribute.RETRY_TOKEN;
import static software.amazon.awssdk.core.metrics.CoreMetric.HEDGE_COUNT;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
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
import software.amazon.awssdk.core.internal.http.TransformingAsyncResponseHandler;
import software.amazon.awssdk.core.internal.http.pipeline.RequestPipeline;
import software.amazon.awssdk.core.internal.http.pipeline.stages.utils.HedgingStageHelper;
import software.amazon.awssdk.core.internal.http.pipeline.stages.utils.RetryableStageHelper;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.metrics.MetricCollector;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.retries.api.RetryToken;
import software.amazon.awssdk.retries.api.TokenAcquisitionFailedException;
import software.amazon.awssdk.utils.Logger;

/**
 * Wrapper around the pipeline for a single request to provide hedging functionality.
 * When hedging is enabled, multiple attempts are started at fixed delays; the first successful
 * response wins and other attempts are cancelled.
 * <p>
 * Each hedge attempt has its own isolated response handler to prevent race conditions.
 * <p>
 * Hedging requires a replayable request body (each attempt sends the same body). Restrict to idempotent
 * operations via {@link HedgingConfig#hedgeableOperations()}.
 */
@SdkInternalApi
public final class AsyncHedgingStage<OutputT> implements RequestPipeline<SdkHttpFullRequest,
    CompletableFuture<Response<OutputT>>> {

    private static final Logger log = Logger.loggerFor(AsyncHedgingStage.class);
    
    // Global counter for unique request IDs (for debugging/logging)
    private static final AtomicLong REQUEST_ID_COUNTER = new AtomicLong(0);

    private final Supplier<TransformingAsyncResponseHandler<Response<OutputT>>> responseHandlerFactory;
    private final RequestPipeline<SdkHttpFullRequest, CompletableFuture<Response<OutputT>>> requestPipeline;
    private final ScheduledExecutorService scheduledExecutor;
    private final HttpClientDependencies dependencies;

    public AsyncHedgingStage(Supplier<TransformingAsyncResponseHandler<Response<OutputT>>> responseHandlerFactory,
                            HttpClientDependencies dependencies,
                            RequestPipeline<SdkHttpFullRequest, CompletableFuture<Response<OutputT>>> requestPipeline) {
        this.responseHandlerFactory = responseHandlerFactory;
        this.dependencies = dependencies;
        this.scheduledExecutor = dependencies.clientConfiguration()
            .option(SdkClientOption.SCHEDULED_EXECUTOR_SERVICE);
        this.requestPipeline = requestPipeline;
    }

    /**
     * Backward-compatible constructor for testing and cases where response handler isolation is not needed.
     * Creates a stage without a response handler factory (handlers will not be isolated per attempt).
     */
    public AsyncHedgingStage(HttpClientDependencies dependencies,
                            RequestPipeline<SdkHttpFullRequest, CompletableFuture<Response<OutputT>>> requestPipeline) {
        this(() -> null, dependencies, requestPipeline);
    }

    @Override
    public CompletableFuture<Response<OutputT>> execute(SdkHttpFullRequest request,
                                                        RequestExecutionContext context) throws Exception {
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
        private final ExecutionAttributes baseExecutionAttributes;
        
        // State machine for atomic transitions
        private final AtomicReference<HedgingExecutionState> state = new AtomicReference<>(HedgingExecutionState.RUNNING);
        
        // Budget tracking: totalBudget = maxHedgedAttempts, consumed increments on each attempt completion/skip
        private final AtomicInteger totalBudget = new AtomicInteger(0);
        private final AtomicInteger consumedBudget = new AtomicInteger(0);
        private final AtomicInteger acquiredHedgeFailureCapacity = new AtomicInteger(0);
        
        // Track actual attempts started (only those that acquired tokens and began execution)
        private final AtomicInteger attemptsStarted = new AtomicInteger(0);
        
        // Collect failures for aggregation
        private final List<Throwable> failures = new ArrayList<>();
        private final Object failuresLock = new Object();
        
        // Track all attempt futures for cancellation
        private final List<CompletableFuture<?>> attemptFutures = new ArrayList<>();
        private final Object futuresLock = new Object();
        private final java.util.Set<Integer> debitedFailureAttempts = ConcurrentHashMap.newKeySet();
        
        // Track scheduled futures for cancellation
        private final List<ScheduledFuture<?>> scheduledFutures = new ArrayList<>();
        private final Object scheduledLock = new Object();
        private final AtomicBoolean hedgeAdmissionClosed = new AtomicBoolean(false);
        
        // Ensure hedge count is only reported once (same pattern as sync HedgingStage)
        private final AtomicBoolean hedgeCountReported = new AtomicBoolean(false);
        
        // Unique ID for this request (for debugging/logging)
        private final long requestId = REQUEST_ID_COUNTER.incrementAndGet();

        private HedgingExecutor(SdkHttpFullRequest request, RequestExecutionContext context) {
            this.request = request;
            this.context = context;
            this.baseExecutionAttributes = context.executionAttributes().copy();
            this.hedgingConfig = HedgingConfig.resolve(
                context.requestConfig().hedgingConfig(),
                Optional.ofNullable(dependencies.clientConfiguration().option(SdkClientOption.HEDGING_CONFIG)),
                Optional::empty);
            this.operationName = context.executionAttributes().getAttribute(SdkExecutionAttribute.OPERATION_NAME);
            this.retryStrategy = dependencies.clientConfiguration().option(SdkClientOption.RETRY_STRATEGY);
            this.helperForAttempt1 = new RetryableStageHelper(request, context, dependencies);
            this.userFuture = new CompletableFuture<>();
        }

        public CompletableFuture<Response<OutputT>> execute() {
            log.debug(() -> String.format("[HEDGE-R%d] execute() START - op=%s, maxAttempts=%d",
                requestId, operationName, hedgingConfig.maxHedgedAttempts()));
            try {
                Duration initialDelay = helperForAttempt1.acquireInitialToken();
                RetryToken initialToken = context.executionAttributes().getAttribute(RETRY_TOKEN);
                log.debug(() -> String.format("[HEDGE] initialToken acquired, initialDelay=%dms", initialDelay.toMillis()));
                if (!initialDelay.isZero()) {
                    scheduledExecutor.schedule(() -> runHedgingLoop(initialToken),
                        initialDelay.toMillis(), MILLISECONDS);
                } else {
                    runHedgingLoop(initialToken);
                }
            } catch (Throwable t) {
                log.debug(() -> "[HEDGE] execute() EXCEPTION: " + t.getMessage(), t);
                userFuture.completeExceptionally(t);
            }
            return userFuture;
        }

        private void runHedgingLoop(RetryToken initialToken) {
            log.debug(() -> String.format("[HEDGE] runHedgingLoop() START - maxAttempts=%d", 
                hedgingConfig.maxHedgedAttempts()));
            try {
                // Set budget upfront: 1 initial + (maxHedgedAttempts - 1) scheduled hedges
                int maxAttempts = hedgingConfig.maxHedgedAttempts();
                totalBudget.set(maxAttempts);
                
                // Schedule hedge attempts BEFORE starting attempt 1
                scheduleHedgedAttempts(initialToken);
                
                // Start the initial attempt
                log.debug(() -> "[HEDGE] Starting initial attempt (attempt 1)");
                startAttempt(1, initialToken);
            } catch (Throwable t) {
                log.debug(() -> "[HEDGE] runHedgingLoop() EXCEPTION: " + t.getMessage(), t);
                tryCompleteWithFailure(t);
            }
        }

        private void scheduleHedgedAttempts(RetryToken initialToken) {
            log.debug(() -> String.format("[HEDGE] scheduleHedgedAttempts() - scheduling attempts 2 to %d", 
                hedgingConfig.maxHedgedAttempts()));
            for (int k = 2; k <= hedgingConfig.maxHedgedAttempts(); k++) {
                int attemptIndex = k;
                Duration delay = hedgingConfig.delayBeforeAttempt(k, operationName);
                long delayMs = delay.toMillis();
                log.debug(() -> String.format("[HEDGE] Scheduling attempt %d with delay %dms", attemptIndex, delayMs));
                
                // Just-in-time token acquisition: schedule the task, acquire token when it runs
                try {
                    ScheduledFuture<?> scheduled = scheduledExecutor.schedule(() -> {
                        log.debug(() -> String.format("[HEDGE] Scheduled task for attempt %d TRIGGERED, state=%s", 
                            attemptIndex, state.get()));
                        
                        // Check if we should skip this attempt
                        if (!isRunning()) {
                            log.debug(() -> String.format("[HEDGE] Attempt %d SKIPPED - not running (state=%s)", 
                                attemptIndex, state.get()));
                            consumeBudgetAndCheckExhaustion();
                            return;
                        }
                        
                        if (!canStartAdditionalHedge(initialToken)) {
                            log.debug(() -> String.format("[HEDGE] Attempt %d - hedge admission DENIED", attemptIndex));
                            consumeBudgetAndCheckExhaustion();
                            return;
                        }
                        log.debug(() -> String.format("[HEDGE] Attempt %d - admission passed, starting attempt", attemptIndex));
                        
                        try {
                            startAttempt(attemptIndex, initialToken);
                        } catch (Throwable t) {
                            log.debug(() -> String.format("[HEDGE] Attempt %d - startAttempt EXCEPTION: %s", 
                                attemptIndex, t.getMessage()), t);
                            handleAttemptFailure(t, attemptIndex, initialToken);
                        }
                    }, delayMs, MILLISECONDS);
                    
                    synchronized (scheduledLock) {
                        scheduledFutures.add(scheduled);
                    }
                    log.debug(() -> String.format("[HEDGE] Attempt %d scheduled successfully", attemptIndex));
                } catch (RejectedExecutionException e) {
                    // Executor rejected the task
                    log.debug(() -> String.format("[HEDGE] Attempt %d - scheduler REJECTED: %s", 
                        attemptIndex, e.getMessage()));
                    consumeBudgetAndCheckExhaustion();
                }
            }
        }

        private void startAttempt(int attemptIndex, RetryToken token) {
            int currentCount = attemptsStarted.incrementAndGet();
            log.debug(() -> String.format("[HEDGE-R%d] startAttempt(%d) - attemptsStarted=%d, state=%s", 
                requestId, attemptIndex, currentCount, state.get()));
            
            // Check if we should actually proceed
            if (!isRunning()) {
                log.debug(() -> String.format("[HEDGE] startAttempt(%d) - ABORTED after increment, state=%s", 
                    attemptIndex, state.get()));
                consumeBudgetAndCheckExhaustion();
                return;
            }
            
            // Create isolated response handler for this attempt
            log.debug(() -> String.format("[HEDGE] startAttempt(%d) - creating isolated handler", attemptIndex));
            TransformingAsyncResponseHandler<Response<OutputT>> attemptHandler = createIsolatedHandler();
            log.debug(() -> String.format("[HEDGE] startAttempt(%d) - handler created: %s", 
                attemptIndex, attemptHandler != null ? attemptHandler.getClass().getSimpleName() : "NULL"));
            
            // Create isolated context for this attempt, with the per-attempt handler
            RequestExecutionContext attemptContext = createAttemptContext(token, attemptIndex, attemptHandler);
            
            helperForAttempt1.startingAttempt();
            helperForAttempt1.logSendingRequest();
            
            SdkHttpFullRequest attemptRequest = request.toBuilder()
                .putHeader(HedgingStageHelper.SDK_HEDGE_INFO_HEADER,
                    HedgingStageHelper.buildHedgeInfoHeaderValue(
                        attemptIndex,
                        totalBudget.get(),
                        hedgingConfig.delayBeforeAttempt(attemptIndex, operationName).toMillis()))
                .build();
            
            CompletableFuture<Response<OutputT>> attemptFuture;
            try {
                // Execute the pipeline - the handler.prepare() is called by MakeAsyncHttpRequestStage
                log.debug(() -> String.format("[HEDGE] startAttempt(%d) - executing pipeline", attemptIndex));
                attemptFuture = requestPipeline.execute(attemptRequest, attemptContext);
                log.debug(() -> String.format("[HEDGE] startAttempt(%d) - pipeline execution started", attemptIndex));
            } catch (Exception e) {
                log.debug(() -> String.format("[HEDGE] startAttempt(%d) - pipeline EXCEPTION: %s", 
                    attemptIndex, e.getMessage()), e);
                handleAttemptFailure(e, attemptIndex, token);
                return;
            }
            
            // Track this future for cancellation
            synchronized (futuresLock) {
                attemptFutures.add(attemptFuture);
            }
            
            // Handle completion
            attemptFuture.whenComplete((response, ex) -> {
                log.debug(() -> String.format("[HEDGE] Attempt %d COMPLETED - success=%s, exception=%s, state=%s", 
                    attemptIndex, 
                    response != null && response.isSuccess(), 
                    ex != null ? ex.getClass().getSimpleName() : "none",
                    state.get()));
                handleAttemptCompletion(response, ex, attemptIndex, token);
            });
        }

        private void handleAttemptCompletion(Response<OutputT> response, Throwable exception, 
                                            int attemptIndex, RetryToken token) {
            // Early exit if already completing
            if (!isRunning() && state.get() != HedgingExecutionState.RUNNING) {
                consumeBudgetAndCheckExhaustion();
                return;
            }
            
            if (exception != null) {
                // Check for cancellation - don't treat as failure
                if (isCancellation(exception)) {
                    consumeBudgetAndCheckExhaustion();
                    return;
                }
                handleAttemptFailure(exception, attemptIndex, token);
                return;
            }
            
            if (response != null && response.isSuccess()) {
                tryCompleteWithSuccess(response, token);
                return;
            }
            
            if (response != null) {
                // Non-success response (error)
                handleUnsuccessfulResponse(response, attemptIndex, token);
                return;
            }
            
            // Defensive: null response and null exception
            handleAttemptFailure(
                SdkException.create("Hedge attempt completed with null response and null exception", null),
                attemptIndex, token);
        }

        private void handleAttemptFailure(Throwable exception, int attemptIndex, RetryToken token) {
            Exception e = exception instanceof Exception ? (Exception) exception : new Exception(exception);
            SdkException sdkEx = e instanceof SdkException ? (SdkException) e
                : SdkException.create("Hedge attempt " + attemptIndex + " failed", e);
            
            addFailure(sdkEx);
            
            // Check if non-retryable - fail fast
            if (isNonRetryable(sdkEx)) {
                tryCompleteWithFailure(sdkEx);
                return;
            }

            recordHedgeFailureIfNeeded(token, sdkEx, attemptIndex);
            consumeBudgetAndCheckExhaustion();
        }

        private void handleUnsuccessfulResponse(Response<OutputT> response, int attemptIndex, RetryToken token) {
            helperForAttempt1.setLastResponse(response.httpResponse());
            helperForAttempt1.adjustClockIfClockSkew(response);
            
            SdkException sdkEx = response.exception();
            if (sdkEx != null) {
                addFailure(sdkEx);
                
                if (isNonRetryable(sdkEx)) {
                    tryCompleteWithFailure(sdkEx);
                    return;
                }
                recordHedgeFailureIfNeeded(token, sdkEx, attemptIndex);
            }
            
            consumeBudgetAndCheckExhaustion();
        }

        /**
         * Try to complete with success. Uses CAS to ensure only one winner.
         */
        private void tryCompleteWithSuccess(Response<OutputT> response, RetryToken token) {
            int currentStarted = attemptsStarted.get();
            log.debug(() -> String.format("[HEDGE-R%d] tryCompleteWithSuccess() - attemptsStarted=%d, state=%s", 
                requestId, currentStarted, state.get()));
            
            if (state.compareAndSet(HedgingExecutionState.RUNNING, HedgingExecutionState.COMPLETING)) {
                log.debug(() -> String.format("[HEDGE-R%d] WON THE RACE", requestId));
                
                // CRITICAL: Report metrics BEFORE completing userFuture!
                // When userFuture completes, downstream handlers will call metricCollector.collect()
                // which finalizes the metrics. Any metrics reported AFTER that will be lost.
                try {
                    int finalStarted = attemptsStarted.get();
                    log.debug(() -> String.format("[HEDGE-R%d] reportHedgeCount BEFORE userFuture.complete, attemptsStarted=%d", 
                        requestId, finalStarted));
                    reportHedgeCount();
                    if (retryStrategy != null && token != null) {
                        // Use recordSuccessForHedging to release tokens WITHOUT reporting RETRY_COUNT.
                        // Hedging uses parallel attempts, not sequential retries, so RETRY_COUNT is not applicable.
                        // HEDGE_COUNT is already reported by reportHedgeCount() above.
                        helperForAttempt1.recordSuccessForHedging(token, acquiredHedgeFailureCapacity.get());
                    }
                } catch (Exception e) {
                    log.debug(() -> "Failed to report metrics or release tokens", e);
                }
                
                // Cancel other attempts (best effort, failure here shouldn't affect the completed future)
                try {
                    cancelAllOtherAttempts();
                } catch (Exception e) {
                    log.debug(() -> "Failed to cancel other attempts", e);
                }
                
                // Now complete the user future - this triggers downstream handlers
                log.debug(() -> String.format("[HEDGE-R%d] completing userFuture", requestId));
                userFuture.complete(response);
                state.set(HedgingExecutionState.COMPLETED);
                return;
            }
            // Another attempt already won, this is a loser - just consume budget
            log.debug(() -> String.format("[HEDGE] tryCompleteWithSuccess() - LOST THE RACE (another attempt won)"));
            consumeBudgetAndCheckExhaustion();
        }

        /**
         * Try to complete with failure. Called when a non-retryable error occurs.
         */
        private void tryCompleteWithFailure(Throwable failure) {
            if (state.compareAndSet(HedgingExecutionState.RUNNING, HedgingExecutionState.COMPLETING)) {
                cancelAllOtherAttempts();
                reportHedgeCount();
                
                userFuture.completeExceptionally(failure);
                state.set(HedgingExecutionState.COMPLETED);
            }
        }

        /**
         * Called when budget is exhausted - complete with aggregated failure.
         */
        private void completeWithAggregatedFailure() {
            if (state.compareAndSet(HedgingExecutionState.RUNNING, HedgingExecutionState.COMPLETING)) {
                cancelAllOtherAttempts();
                reportHedgeCount();
                
                List<Throwable> allFailures = getFailures();
                Throwable primary = allFailures.isEmpty()
                    ? new IllegalStateException("All hedged attempts failed with no exception")
                    : allFailures.get(0);
                
                if (primary instanceof SdkException) {
                    SdkException sdkEx = (SdkException) primary;
                    SdkException newEx = sdkEx.toBuilder().numAttempts(allFailures.size()).build();
                    for (int i = 1; i < allFailures.size(); i++) {
                        newEx.addSuppressed(allFailures.get(i));
                    }
                    userFuture.completeExceptionally(newEx);
                } else {
                    for (int i = 1; i < allFailures.size(); i++) {
                        primary.addSuppressed(allFailures.get(i));
                    }
                    userFuture.completeExceptionally(primary);
                }
                
                state.set(HedgingExecutionState.COMPLETED);
            }
        }

        private void consumeBudgetAndCheckExhaustion() {
            int consumed = consumedBudget.incrementAndGet();
            if (consumed >= totalBudget.get() && isRunning()) {
                completeWithAggregatedFailure();
            }
        }

        private boolean isRunning() {
            return state.get() == HedgingExecutionState.RUNNING;
        }

        private void cancelAllOtherAttempts() {
            // Cancel all attempt futures
            List<CompletableFuture<?>> futuresToCancel;
            synchronized (futuresLock) {
                futuresToCancel = new ArrayList<>(attemptFutures);
            }
            for (CompletableFuture<?> f : futuresToCancel) {
                f.cancel(false);
            }
            
            // Cancel all scheduled futures
            List<ScheduledFuture<?>> scheduledToCancel;
            synchronized (scheduledLock) {
                scheduledToCancel = new ArrayList<>(scheduledFutures);
            }
            for (ScheduledFuture<?> sf : scheduledToCancel) {
                sf.cancel(false);
            }
        }

        private boolean canStartAdditionalHedge(RetryToken token) {
            if (hedgeAdmissionClosed.get()) {
                return false;
            }
            if (retryStrategy == null || !retryStrategy.supportsHedging()) {
                return isRunning();
            }
            try {
                if (!helperForAttempt1.canStartHedgeAttempt(token)) {
                    hedgeAdmissionClosed.set(true);
                    return false;
                }
                return isRunning();
            } catch (RuntimeException e) {
                log.debug(() -> "Failed to evaluate hedge admission; closing additional hedge starts", e);
                hedgeAdmissionClosed.set(true);
                return false;
            }
        }

        private void recordHedgeFailureIfNeeded(RetryToken token, Throwable failure, int attemptIndex) {
            if (token == null || retryStrategy == null || !retryStrategy.supportsHedging()) {
                return;
            }
            if (!debitedFailureAttempts.add(attemptIndex)) {
                return;
            }
            try {
                int acquired = helperForAttempt1.recordHedgeFailure(token, failure);
                acquiredHedgeFailureCapacity.addAndGet(acquired);
            } catch (TokenAcquisitionFailedException e) {
                hedgeAdmissionClosed.set(true);
            } catch (RuntimeException e) {
                debitedFailureAttempts.remove(attemptIndex);
                throw e;
            }
        }

        private void reportHedgeCount() {
            // Use AtomicBoolean guard to ensure we only report once (same pattern as sync HedgingStage)
            if (!hedgeCountReported.compareAndSet(false, true)) {
                log.debug(() -> String.format("[HEDGE-R%d] reportHedgeCount() - SKIPPED (already reported)", requestId));
                return;
            }
            
            int started = attemptsStarted.get();
            int hedgeCount = Math.max(0, started - 1);
            MetricCollector collector = context.executionContext().metricCollector();
            boolean hasCollector = collector != null;
            String collectorType = hasCollector ? collector.getClass().getSimpleName() : "null";
            log.debug(() -> String.format(
                "[HEDGE-R%d] reportHedgeCount() - attemptsStarted=%d, hedgeCount=%d, collectorType=%s", 
                requestId, started, hedgeCount, collectorType));
            if (hasCollector) {
                collector.reportMetric(HEDGE_COUNT, hedgeCount);
                // Log at INFO level for hedge > 0 to make it easier to find
                if (hedgeCount > 0) {
                    log.debug(() -> String.format(
                        "[HEDGE-R%d] REPORTED hedgeCount=%d to %s", requestId, hedgeCount, collectorType));
                } else {
                    log.debug(() -> String.format(
                        "[HEDGE-R%d] REPORTED hedgeCount=%d", requestId, hedgeCount));
                }
            } else {
                log.warn(() -> String.format(
                    "[HEDGE-R%d] NO metricCollector - hedgeCount=%d NOT reported!", requestId, hedgeCount));
            }
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
            return cause instanceof CancellationException;
        }

        private void addFailure(Throwable failure) {
            synchronized (failuresLock) {
                failures.add(failure);
            }
        }

        private List<Throwable> getFailures() {
            synchronized (failuresLock) {
                return new ArrayList<>(failures);
            }
        }

        /**
         * Creates an isolated response handler for a hedge attempt.
         * The factory should return a fresh handler instance for each call to ensure
         * proper isolation between concurrent hedge attempts.
         */
        private TransformingAsyncResponseHandler<Response<OutputT>> createIsolatedHandler() {
            return responseHandlerFactory.get();
        }

        private RequestExecutionContext createAttemptContext(RetryToken token, int attempt,
                                                              TransformingAsyncResponseHandler<Response<OutputT>> handler) {
            ExecutionAttributes attrs = baseExecutionAttributes.copy();
            attrs.putAttribute(RETRY_TOKEN, token);
            attrs.putAttribute(EXECUTION_ATTEMPT, attempt);
            // Set the per-attempt response handler for isolation
            attrs.putAttribute(HEDGING_RESPONSE_HANDLER, handler);
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
}
