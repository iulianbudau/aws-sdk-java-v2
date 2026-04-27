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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.core.client.config.SdkClientOption.HEDGING_CONFIG;
import static software.amazon.awssdk.core.client.config.SdkClientOption.HEDGING_LATENCY_TRACKER;
import static software.amazon.awssdk.core.client.config.SdkClientOption.RETRY_STRATEGY;
import static software.amazon.awssdk.core.client.config.SdkClientOption.SCHEDULED_EXECUTOR_SERVICE;
import static software.amazon.awssdk.core.internal.InternalCoreExecutionAttribute.EXECUTION_ATTEMPT;
import static software.amazon.awssdk.core.internal.InternalCoreExecutionAttribute.RETRY_TOKEN;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.Response;
import software.amazon.awssdk.core.SdkRequestOverrideConfiguration;
import software.amazon.awssdk.core.client.config.HedgingConfig;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.exception.NonRetryableException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.core.http.ExecutionContext;
import software.amazon.awssdk.core.http.NoopTestRequest;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.SdkExecutionAttribute;
import software.amazon.awssdk.core.internal.http.HttpClientDependencies;
import software.amazon.awssdk.core.internal.http.RequestExecutionContext;
import software.amazon.awssdk.core.internal.http.pipeline.RequestPipeline;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.metrics.MetricCollector;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.retries.api.AcquireInitialTokenResponse;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.retries.api.RetryToken;
import software.amazon.awssdk.core.internal.http.pipeline.stages.utils.HedgingLatencyTracker;
import software.amazon.awssdk.retries.internal.DefaultRetryToken;

public class AsyncHedgingStageTest {

    @Mock
    private RequestPipeline<SdkHttpFullRequest, CompletableFuture<Response<String>>> requestPipeline;

    @Mock
    private RetryStrategy retryStrategy;

    @Mock
    private MetricCollector metricCollector;

    private ScheduledExecutorService realExecutor;
    private AsyncHedgingStage<String> hedgingStage;
    private HttpClientDependencies dependencies;
    private SdkHttpFullRequest request;
    private RequestExecutionContext context;
    private RetryToken initialToken;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        realExecutor = Executors.newScheduledThreadPool(4);
        
        initialToken = DefaultRetryToken.builder()
            .scope("test")
            .attempt(1)
            .capacityAcquired(1)
            .capacityRemaining(100)
            .build();

        SdkClientConfiguration config = SdkClientConfiguration.builder()
            .option(RETRY_STRATEGY, retryStrategy)
            .option(HEDGING_CONFIG, HedgingConfig.builder()
                .enabled(true)
                .defaultPolicy(HedgingConfig.OperationHedgingPolicy.builder()
                                                                   .maxHedgedAttempts(3)
                                                                   .delayConfig(HedgingConfig.FixedDelayConfig.builder()
                                                                                                              .baseDelay(Duration.ofMillis(10))
                                                                                                              .build())
                                                                   .build())
                .build())
            .option(SCHEDULED_EXECUTOR_SERVICE, realExecutor)
            .build();

        dependencies = HttpClientDependencies.builder()
            .clientConfiguration(config)
            .build();

        hedgingStage = new AsyncHedgingStage<>(dependencies, requestPipeline);

        request = SdkHttpFullRequest.builder()
            .method(SdkHttpMethod.GET)
            .uri(java.net.URI.create("https://example.com"))
            .build();

        ExecutionAttributes attrs = new ExecutionAttributes();
        attrs.putAttribute(SdkExecutionAttribute.OPERATION_NAME, "GetItem");
        attrs.putAttribute(RETRY_TOKEN, initialToken);
        attrs.putAttribute(EXECUTION_ATTEMPT, 1);

        ExecutionContext executionContext = ExecutionContext.builder()
            .executionAttributes(attrs)
            .build();

        context = RequestExecutionContext.builder()
            .originalRequest(NoopTestRequest.builder()
                .overrideConfiguration(SdkRequestOverrideConfiguration.builder().build())
                .build())
            .executionContext(executionContext)
            .build();

        when(retryStrategy.acquireInitialToken(any()))
            .thenReturn(AcquireInitialTokenResponse.create(initialToken, Duration.ZERO));
        when(retryStrategy.maxAttempts()).thenReturn(3);
        when(retryStrategy.canStartHedgeAttempt(any())).thenReturn(true);
        // Mock recordSuccess to prevent NPE - it's called when an attempt succeeds
        when(retryStrategy.recordSuccess(any())).thenReturn(
            software.amazon.awssdk.retries.api.RecordSuccessResponse.create(initialToken));
    }

    @AfterEach
    public void tearDown() {
        if (realExecutor != null) {
            realExecutor.shutdownNow();
        }
    }

    @Test
    @Timeout(5)
    public void firstAttemptSucceeds_shouldCompleteImmediately() throws Exception {
        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();

        when(requestPipeline.execute(any(), any())).thenReturn(CompletableFuture.completedFuture(successResponse));

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);

        Response<String> response = result.get(2, TimeUnit.SECONDS);
        assertThat(response).isEqualTo(successResponse);
        // First attempt succeeds immediately, pipeline called at least once
        verify(requestPipeline, atLeastOnce()).execute(any(), any());
    }

    @Test
    @Timeout(5)
    public void firstAttemptFails_secondAttemptSucceeds_shouldCompleteWithSecond() throws Exception {
        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();

        SdkException failureException = RetryableException.builder().message("First attempt failed").build();
        Response<String> failureResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(500).build())
            .exception(failureException)
            .isSuccess(false)
            .build();

        AtomicInteger callCount = new AtomicInteger(0);
        when(requestPipeline.execute(any(), any())).thenAnswer(invocation -> {
            int count = callCount.incrementAndGet();
            if (count == 1) {
                // First attempt fails after a delay
                CompletableFuture<Response<String>> delayedFailure = new CompletableFuture<>();
                realExecutor.schedule(() -> delayedFailure.complete(failureResponse), 30, TimeUnit.MILLISECONDS);
                return delayedFailure;
            }
            return CompletableFuture.completedFuture(successResponse);
        });

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);

        Response<String> response = result.get(2, TimeUnit.SECONDS);
        assertThat(response).isEqualTo(successResponse);
        assertThat(callCount.get()).isGreaterThanOrEqualTo(2);
    }

    @Test
    @Timeout(5)
    public void hedgingRequestHeader_shouldContainAttemptMaxAndDelay() throws Exception {
        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();

        SdkException failureException = RetryableException.builder().message("First attempt failed").build();
        Response<String> failureResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(500).build())
            .exception(failureException)
            .isSuccess(false)
            .build();

        List<SdkHttpFullRequest> executedRequests = new ArrayList<>();
        AtomicInteger callCount = new AtomicInteger(0);
        when(requestPipeline.execute(any(), any())).thenAnswer(invocation -> {
            SdkHttpFullRequest attemptRequest = invocation.getArgument(0);
            executedRequests.add(attemptRequest);

            int count = callCount.incrementAndGet();
            if (count == 1) {
                CompletableFuture<Response<String>> delayedFailure = new CompletableFuture<>();
                realExecutor.schedule(() -> delayedFailure.complete(failureResponse), 30, TimeUnit.MILLISECONDS);
                return delayedFailure;
            }
            return CompletableFuture.completedFuture(successResponse);
        });

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);
        Response<String> response = result.get(2, TimeUnit.SECONDS);
        assertThat(response).isEqualTo(successResponse);

        assertThat(executedRequests.size()).isGreaterThanOrEqualTo(2);

        List<String> hedgeHeaders = new ArrayList<>();
        for (SdkHttpFullRequest r : executedRequests) {
            assertThat(r.firstMatchingHeader("amz-sdk-request")).isEmpty();
            hedgeHeaders.add(r.firstMatchingHeader("amz-sdk-hedge-request").orElse(null));
        }

        assertThat(hedgeHeaders).contains("attempt=1; max=3; delay=0ms");
        assertThat(hedgeHeaders)
            .anyMatch(h -> "attempt=2; max=3; delay=10ms".equals(h)
                || "attempt=3; max=3; delay=20ms".equals(h));
    }

    @Test
    @Timeout(5)
    public void allAttemptsFail_shouldCompleteWithAggregatedFailure() throws Exception {
        SdkException failureException = RetryableException.builder().message("Attempt failed").build();
        Response<String> failureResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(500).build())
            .exception(failureException)
            .isSuccess(false)
            .build();

        when(requestPipeline.execute(any(), any())).thenReturn(CompletableFuture.completedFuture(failureResponse));

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);

        assertThatThrownBy(() -> result.get(2, TimeUnit.SECONDS))
            .hasCauseInstanceOf(SdkException.class);
        assertThat(result.isCompletedExceptionally()).isTrue();
    }

    @Test
    @Timeout(5)
    public void retryStrategySupportsHedging_shouldCheckHedgeAdmission() throws Exception {
        when(retryStrategy.supportsHedging()).thenReturn(true);
        when(retryStrategy.canStartHedgeAttempt(any())).thenReturn(true);

        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();
        
        // First attempt completes after a delay so hedge attempts can start
        CompletableFuture<Response<String>> delayedSuccess = new CompletableFuture<>();
        when(requestPipeline.execute(any(), any())).thenReturn(delayedSuccess);

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);

        // Wait for hedge attempts to be scheduled and evaluate admission
        Thread.sleep(50);
        delayedSuccess.complete(successResponse);
        
        result.get(2, TimeUnit.SECONDS);
        verify(retryStrategy, times(2)).canStartHedgeAttempt(any());
    }

    @Test
    @Timeout(5)
    public void hedgeAdmissionFails_shouldSkipThatAttempt() throws Exception {
        when(retryStrategy.supportsHedging()).thenReturn(true);
        when(retryStrategy.canStartHedgeAttempt(any())).thenReturn(false);

        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();
        when(requestPipeline.execute(any(), any())).thenReturn(CompletableFuture.completedFuture(successResponse));

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);

        Response<String> response = result.get(2, TimeUnit.SECONDS);
        assertThat(response).isEqualTo(successResponse);
        // Should execute only attempt 1 since hedges fail admission
        verify(requestPipeline, times(1)).execute(any(), any());
    }

    @Test
    @Timeout(5)
    public void operationNotInHedgeableList_shouldNotHedge() throws Exception {
        HedgingConfig config = HedgingConfig.builder()
            .enabled(true)
            .defaultPolicy(HedgingConfig.OperationHedgingPolicy.builder()
                                                               .maxHedgedAttempts(3)
                                                               .delayConfig(HedgingConfig.FixedDelayConfig.builder()
                                                                                                          .baseDelay(Duration.ofMillis(10))
                                                                                                          .build())
                                                               .build())
            .hedgeableOperations(Collections.singleton("Query"))
            .build();

        SdkClientConfiguration clientConfig = SdkClientConfiguration.builder()
            .option(RETRY_STRATEGY, retryStrategy)
            .option(HEDGING_CONFIG, config)
            .option(SCHEDULED_EXECUTOR_SERVICE, realExecutor)
            .build();

        dependencies = HttpClientDependencies.builder()
            .clientConfiguration(clientConfig)
            .build();

        hedgingStage = new AsyncHedgingStage<>(dependencies, requestPipeline);

        ExecutionAttributes attrs = new ExecutionAttributes();
        attrs.putAttribute(SdkExecutionAttribute.OPERATION_NAME, "PutItem"); // Not in hedgeable list
        attrs.putAttribute(RETRY_TOKEN, initialToken);

        ExecutionContext executionContext = ExecutionContext.builder()
            .executionAttributes(attrs)
            .build();

        context = RequestExecutionContext.builder()
            .originalRequest(NoopTestRequest.builder()
                .overrideConfiguration(SdkRequestOverrideConfiguration.builder().build())
                .build())
            .executionContext(executionContext)
            .build();

        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();
        when(requestPipeline.execute(any(), any())).thenReturn(CompletableFuture.completedFuture(successResponse));

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);

        Response<String> response = result.get(2, TimeUnit.SECONDS);
        assertThat(response).isEqualTo(successResponse);
        // Should only execute once since hedging is disabled for this operation
        verify(requestPipeline, times(1)).execute(any(), any());
    }

    @Test
    @Timeout(5)
    public void requestPipelineThrowsException_shouldCompleteExceptionally() throws Exception {
        RuntimeException pipelineException = new RuntimeException("Pipeline error");
        when(requestPipeline.execute(any(), any())).thenThrow(pipelineException);

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);

        // The exception gets wrapped in SdkException by the hedging stage
        assertThatThrownBy(() -> result.get(2, TimeUnit.SECONDS))
            .hasCauseInstanceOf(SdkException.class)
            .hasRootCauseInstanceOf(RuntimeException.class)
            .hasRootCauseMessage("Pipeline error");
    }

    @Test
    @Timeout(5)
    public void successfulResponse_cancelsOtherAttempts() throws Exception {
        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();

        CompletableFuture<Response<String>> future1 = new CompletableFuture<>();
        CompletableFuture<Response<String>> future2 = new CompletableFuture<>();
        CompletableFuture<Response<String>> future3 = new CompletableFuture<>();

        AtomicInteger callCount = new AtomicInteger(0);
        when(requestPipeline.execute(any(), any())).thenAnswer(invocation -> {
            int count = callCount.incrementAndGet();
            if (count == 1) {
                // First attempt succeeds after delay
                realExecutor.schedule(() -> future1.complete(successResponse), 30, TimeUnit.MILLISECONDS);
                return future1;
            } else if (count == 2) {
                return future2;
            } else {
                return future3;
            }
        });

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);

        Response<String> response = result.get(2, TimeUnit.SECONDS);
        assertThat(response).isEqualTo(successResponse);
        
        // Give time for cancellation to propagate
        Thread.sleep(50);
        
        // Other attempts should be cancelled or done
        assertThat(future2.isCancelled() || future2.isDone())
            .as("future2 should be cancelled or done after first attempt succeeds")
            .isTrue();
    }

    @Test
    @Timeout(5)
    public void perOperationDelay_shouldUseOperationSpecificDelay() throws Exception {
        HedgingConfig config = HedgingConfig.builder()
            .enabled(true)
            .defaultPolicy(HedgingConfig.OperationHedgingPolicy.builder()
                                                               .maxHedgedAttempts(3)
                                                               .delayConfig(HedgingConfig.FixedDelayConfig.builder()
                                                                                                          .baseDelay(Duration.ofMillis(100))
                                                                                                          .build())
                                                               .build())
            .policyPerOperation(Collections.singletonMap("GetItem",
                                                         HedgingConfig.OperationHedgingPolicy.builder()
                                                                                            .maxHedgedAttempts(3)
                                                                                            .delayConfig(HedgingConfig.FixedDelayConfig
                                                                                                             .builder()
                                                                                                             .baseDelay(Duration.ofMillis(5))
                                                                                                             .build())
                                                                                            .build()))
            .build();

        SdkClientConfiguration clientConfig = SdkClientConfiguration.builder()
            .option(RETRY_STRATEGY, retryStrategy)
            .option(HEDGING_CONFIG, config)
            .option(SCHEDULED_EXECUTOR_SERVICE, realExecutor)
            .build();

        dependencies = HttpClientDependencies.builder()
            .clientConfiguration(clientConfig)
            .build();

        hedgingStage = new AsyncHedgingStage<>(dependencies, requestPipeline);

        when(retryStrategy.acquireInitialToken(any()))
            .thenReturn(AcquireInitialTokenResponse.create(initialToken, Duration.ZERO));
        when(retryStrategy.maxAttempts()).thenReturn(3);

        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();
        
        // First attempt is slow, allowing hedges to fire
        AtomicInteger callCount = new AtomicInteger(0);
        when(requestPipeline.execute(any(), any())).thenAnswer(invocation -> {
            int count = callCount.incrementAndGet();
            if (count == 1) {
                CompletableFuture<Response<String>> slow = new CompletableFuture<>();
                realExecutor.schedule(() -> slow.complete(successResponse), 50, TimeUnit.MILLISECONDS);
                return slow;
            }
            return CompletableFuture.completedFuture(successResponse);
        });

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);
        result.get(2, TimeUnit.SECONDS);

        // With 5ms delay, hedges should fire quickly and multiple calls should happen
        assertThat(callCount.get()).isGreaterThanOrEqualTo(2);
    }

    @Test
    @Timeout(5)
    public void recordSuccessWithHedgeCount_shouldCallHelper() throws Exception {
        when(retryStrategy.supportsHedging()).thenReturn(true);
        when(retryStrategy.canStartHedgeAttempt(any())).thenReturn(true);

        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();
        
        CompletableFuture<Response<String>> delayedSuccess = new CompletableFuture<>();
        when(requestPipeline.execute(any(), any())).thenReturn(delayedSuccess);

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);

        // Wait for scheduled tasks to run and acquire tokens
        Thread.sleep(50);
        delayedSuccess.complete(successResponse);

        Response<String> response = result.get(2, TimeUnit.SECONDS);
        assertThat(response).isEqualTo(successResponse);
        verify(retryStrategy, atLeastOnce()).supportsHedging();
    }

    @Test
    @Timeout(5)
    public void firstAttemptSucceeds_reportsHedgeCountZero() throws Exception {
        RequestExecutionContext contextWithMetrics = RequestExecutionContext.builder()
            .originalRequest(context.originalRequest())
            .executionContext(ExecutionContext.builder()
                .executionAttributes(context.executionContext().executionAttributes())
                .metricCollector(metricCollector)
                .build())
            .build();

        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();
        when(requestPipeline.execute(any(), any())).thenReturn(CompletableFuture.completedFuture(successResponse));

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, contextWithMetrics);
        Response<String> response = result.get(2, TimeUnit.SECONDS);

        assertThat(response).isEqualTo(successResponse);
        verify(metricCollector).reportMetric(eq(CoreMetric.HEDGE_COUNT), eq(0));
    }

    @Test
    @Timeout(5)
    public void firstAttemptFails_secondSucceeds_reportsHedgeCountOne() throws Exception {
        RequestExecutionContext contextWithMetrics = RequestExecutionContext.builder()
            .originalRequest(context.originalRequest())
            .executionContext(ExecutionContext.builder()
                .executionAttributes(context.executionContext().executionAttributes())
                .metricCollector(metricCollector)
                .build())
            .build();

        SdkClientConfiguration configWithTwoAttempts = SdkClientConfiguration.builder()
            .option(RETRY_STRATEGY, retryStrategy)
            .option(HEDGING_CONFIG, HedgingConfig.builder()
                .enabled(true)
                .defaultPolicy(HedgingConfig.OperationHedgingPolicy.builder()
                                                                   .maxHedgedAttempts(2)
                                                                   .delayConfig(HedgingConfig.FixedDelayConfig.builder()
                                                                                                              .baseDelay(Duration.ofMillis(10))
                                                                                                              .build())
                                                                   .build())
                .build())
            .option(SCHEDULED_EXECUTOR_SERVICE, realExecutor)
            .build();
        dependencies = HttpClientDependencies.builder().clientConfiguration(configWithTwoAttempts).build();
        hedgingStage = new AsyncHedgingStage<>(dependencies, requestPipeline);

        SdkException failureException = RetryableException.builder().message("First attempt failed").build();
        Response<String> failureResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(500).build())
            .exception(failureException)
            .isSuccess(false)
            .build();
        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();

        AtomicInteger callCount = new AtomicInteger(0);
        when(requestPipeline.execute(any(), any())).thenAnswer(invocation -> {
            int count = callCount.incrementAndGet();
            if (count == 1) {
                CompletableFuture<Response<String>> delayedFailure = new CompletableFuture<>();
                realExecutor.schedule(() -> delayedFailure.complete(failureResponse), 30, TimeUnit.MILLISECONDS);
                return delayedFailure;
            }
            return CompletableFuture.completedFuture(successResponse);
        });

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, contextWithMetrics);
        Response<String> response = result.get(2, TimeUnit.SECONDS);

        assertThat(response).isEqualTo(successResponse);
        assertThat(callCount.get()).isGreaterThanOrEqualTo(2);
        verify(metricCollector).reportMetric(eq(CoreMetric.HEDGE_COUNT), eq(1));
    }

    @Test
    @Timeout(5)
    public void allAttemptsFail_reportsHedgeCountTwo() throws Exception {
        RequestExecutionContext contextWithMetrics = RequestExecutionContext.builder()
            .originalRequest(context.originalRequest())
            .executionContext(ExecutionContext.builder()
                .executionAttributes(context.executionContext().executionAttributes())
                .metricCollector(metricCollector)
                .build())
            .build();

        SdkException failureException = RetryableException.builder().message("Attempt failed").build();
        Response<String> failureResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(500).build())
            .exception(failureException)
            .isSuccess(false)
            .build();

        when(requestPipeline.execute(any(), any())).thenReturn(CompletableFuture.completedFuture(failureResponse));

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, contextWithMetrics);
        
        assertThatThrownBy(() -> result.get(2, TimeUnit.SECONDS)).hasCauseInstanceOf(SdkException.class);

        verify(metricCollector).reportMetric(eq(CoreMetric.HEDGE_COUNT), eq(2));
    }

    /**
     * When the first attempt fails immediately (before attempt 2's scheduled start time), we must NOT fail fast:
     * we keep the hedge plan alive, let attempt 2 start at its scheduled time, and return its success
     * as long as the error is retryable.
     */
    @Test
    @Timeout(5)
    public void firstAttemptFailsImmediately_beforeSecondStarts_doesNotFailFast_secondSucceeds() throws Exception {
        when(retryStrategy.supportsHedging()).thenReturn(false);
        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();
        SdkException failureException = RetryableException.create("First attempt failed");
        Response<String> failureResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(500).build())
            .exception(failureException)
            .isSuccess(false)
            .build();

        AtomicInteger callCount = new AtomicInteger(0);
        when(requestPipeline.execute(any(), any())).thenAnswer(invocation -> {
            int count = callCount.incrementAndGet();
            if (count == 1) {
                // First attempt fails immediately
                return CompletableFuture.completedFuture(failureResponse);
            }
            return CompletableFuture.completedFuture(successResponse);
        });

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);
        Response<String> response = result.get(2, TimeUnit.SECONDS);

        assertThat(response).isEqualTo(successResponse);
        assertThat(callCount.get()).as("Should not fail fast; attempt 2 must have run").isGreaterThanOrEqualTo(2);
    }

    /**
     * Non-retryable error on first attempt: fail immediately, do not wait for hedge attempts.
     */
    @Test
    @Timeout(5)
    public void nonRetryableError_failsAfterFirstAttempt_doesNotStartHedgeAttempts() throws Exception {
        NonRetryableException nonRetryable = NonRetryableException.create("Validation failed");
        Response<String> failureResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(400).build())
            .exception(nonRetryable)
            .isSuccess(false)
            .build();
        when(requestPipeline.execute(any(), any())).thenReturn(CompletableFuture.completedFuture(failureResponse));

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);

        assertThatThrownBy(() -> result.get(2, TimeUnit.SECONDS))
            .hasCauseInstanceOf(NonRetryableException.class)
            .hasMessageContaining("Validation failed");
        verify(requestPipeline, times(1)).execute(any(), any());
    }

    /**
     * When fewer than maxHedgedAttempts are started (e.g. hedge admission fails for attempts 2 and 3),
     * and all started attempts fail, the user future must still complete with aggregated failure.
     */
    @Test
    @Timeout(5)
    public void fewerAttemptsStarted_allFail_shouldCompleteWithAggregatedFailure() throws Exception {
        when(retryStrategy.supportsHedging()).thenReturn(true);
        when(retryStrategy.canStartHedgeAttempt(any())).thenReturn(false);

        SdkException failureException = RetryableException.builder().message("Attempt 1 failed").build();
        Response<String> failureResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(500).build())
            .exception(failureException)
            .isSuccess(false)
            .build();
        
        CompletableFuture<Response<String>> delayedFailure = new CompletableFuture<>();
        when(requestPipeline.execute(any(), any())).thenReturn(delayedFailure);

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);

        // Wait for scheduled tasks to run and fail hedge admission
        Thread.sleep(50);
        delayedFailure.complete(failureResponse);

        assertThatThrownBy(() -> result.get(2, TimeUnit.SECONDS))
            .hasCauseInstanceOf(SdkException.class)
            .hasMessageContaining("Attempt 1 failed");
        assertThat(result.isCompletedExceptionally()).isTrue();
        verify(requestPipeline, times(1)).execute(any(), any());
    }

    /**
     * When an attempt future completes with (null, null), the user future must still complete exceptionally
     * so the caller is not left hanging.
     */
    @Test
    @Timeout(5)
    public void attemptCompletesWithNullResponseAndNullException_shouldCompleteExceptionally() throws Exception {
        when(requestPipeline.execute(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);

        assertThatThrownBy(() -> result.get(2, TimeUnit.SECONDS))
            .hasCauseInstanceOf(SdkException.class)
            .hasMessageContaining("null response and null exception");
        assertThat(result.isCompletedExceptionally()).isTrue();
    }
    
    @Test
    @Timeout(5)
    public void initialDelay_shouldScheduleAfterDelay() throws Exception {
        when(retryStrategy.acquireInitialToken(any())).thenAnswer(invocation -> {
            return AcquireInitialTokenResponse.create(initialToken, Duration.ofMillis(20));
        });

        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();
        when(requestPipeline.execute(any(), any())).thenReturn(CompletableFuture.completedFuture(successResponse));

        long startTime = System.currentTimeMillis();
        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);
        result.get(2, TimeUnit.SECONDS);
        long elapsed = System.currentTimeMillis() - startTime;

        // Should have waited at least 20ms for initial delay
        assertThat(elapsed).isGreaterThanOrEqualTo(15);
    }

    @Test
    @Timeout(5)
    public void adaptiveMode_headersUseLearnedDelayOnSubsequentCall() throws Exception {
        HedgingLatencyTracker tracker = new HedgingLatencyTracker();
        HedgingConfig.AdaptiveDelayConfig adaptiveCfg = HedgingConfig.AdaptiveDelayConfig.builder()
                                                                                          .percentile(99)
                                                                                          .sampleSize(100)
                                                                                          .minSamplesRequired(1)
                                                                                          .fallbackDelay(Duration.ofMillis(15))
                                                                                          .minDelay(Duration.ofMillis(6))
                                                                                          .maxDelay(Duration.ofMillis(6))
                                                                                          .build();
        HedgingConfig adaptiveConfig = HedgingConfig.builder()
                                                    .enabled(true)
                                                    .defaultPolicy(HedgingConfig.OperationHedgingPolicy.builder()
                                                                                                       .maxHedgedAttempts(3)
                                                                                                       .delayConfig(adaptiveCfg)
                                                                                                       .build())
                                                    .build();

        SdkClientConfiguration config = SdkClientConfiguration.builder()
                                                              .option(RETRY_STRATEGY, retryStrategy)
                                                              .option(HEDGING_CONFIG, adaptiveConfig)
                                                              .option(HEDGING_LATENCY_TRACKER, tracker)
                                                              .option(SCHEDULED_EXECUTOR_SERVICE, realExecutor)
                                                              .build();
        dependencies = HttpClientDependencies.builder().clientConfiguration(config).build();
        hedgingStage = new AsyncHedgingStage<>(dependencies, requestPipeline);

        Response<String> successResponse = Response.<String>builder()
                                                   .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
                                                   .isSuccess(true)
                                                   .build();
        when(requestPipeline.execute(any(), any())).thenReturn(CompletableFuture.completedFuture(successResponse));

        hedgingStage.execute(request, context).get(2, TimeUnit.SECONDS);

        List<SdkHttpFullRequest> captured = new ArrayList<>();
        AtomicInteger secondRunCount = new AtomicInteger(0);
        when(requestPipeline.execute(any(), any())).thenAnswer(invocation -> {
            captured.add(invocation.getArgument(0));
            int count = secondRunCount.incrementAndGet();
            if (count == 1) {
                CompletableFuture<Response<String>> delayed = new CompletableFuture<>();
                realExecutor.schedule(() -> delayed.complete(successResponse), 40, TimeUnit.MILLISECONDS);
                return delayed;
            }
            return CompletableFuture.completedFuture(successResponse);
        });

        hedgingStage.execute(request, context).get(2, TimeUnit.SECONDS);

        List<String> headers = new ArrayList<>();
        for (SdkHttpFullRequest r : captured) {
            headers.add(r.firstMatchingHeader("amz-sdk-hedge-request").orElse(""));
        }
        assertThat(headers).anyMatch(h -> h.contains("delay=6ms") || h.contains("delay=12ms"));
        assertThat(headers).noneMatch(h -> h.contains("delay=15ms") || h.contains("delay=30ms"));
    }
}
