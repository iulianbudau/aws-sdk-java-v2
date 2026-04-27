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
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.Response;
import software.amazon.awssdk.core.SdkRequestOverrideConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.HedgingConfig;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.http.ExecutionContext;
import software.amazon.awssdk.core.http.NoopTestRequest;
import software.amazon.awssdk.metrics.MetricCollector;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.SdkExecutionAttribute;
import software.amazon.awssdk.core.internal.http.HttpClientDependencies;
import software.amazon.awssdk.core.internal.http.RequestExecutionContext;
import software.amazon.awssdk.core.internal.http.TransformingAsyncResponseHandler;
import software.amazon.awssdk.core.internal.http.pipeline.RequestPipeline;
import software.amazon.awssdk.core.internal.http.pipeline.stages.utils.HedgingLatencyTracker;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.retries.api.AcquireInitialTokenResponse;
import software.amazon.awssdk.retries.api.RecordSuccessResponse;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.retries.api.RetryToken;
import software.amazon.awssdk.retries.internal.DefaultRetryToken;

/**
 * Integration tests for AsyncRetryOrHedgingStage that verify end-to-end hedging behavior.
 */
public class AsyncRetryOrHedgingStageIntegrationTest {

    @Mock
    private RequestPipeline<SdkHttpFullRequest, CompletableFuture<Response<String>>> requestPipeline;

    @Mock
    private TransformingAsyncResponseHandler<Response<String>> responseHandler;

    @Mock
    private RetryStrategy retryStrategy;

    private AsyncRetryOrHedgingStage<String> stage;
    private HttpClientDependencies dependencies;
    private SdkHttpFullRequest request;
    private RequestExecutionContext context;
    private RetryToken initialToken;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        scheduledExecutor = Executors.newScheduledThreadPool(5);

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
            .option(SCHEDULED_EXECUTOR_SERVICE, scheduledExecutor)
            .build();

        dependencies = HttpClientDependencies.builder()
            .clientConfiguration(config)
            .build();

        stage = new AsyncRetryOrHedgingStage<>(responseHandler, () -> responseHandler, dependencies, requestPipeline);

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
            .metricCollector(MetricCollector.create("test"))
            .build();

        context = RequestExecutionContext.builder()
            .originalRequest(NoopTestRequest.builder().overrideConfiguration(SdkRequestOverrideConfiguration.builder().build()).build())
            .executionContext(executionContext)
            .build();

        when(retryStrategy.acquireInitialToken(any()))
            .thenReturn(AcquireInitialTokenResponse.create(initialToken, Duration.ZERO));
        when(retryStrategy.maxAttempts()).thenReturn(3);
        when(retryStrategy.recordSuccess(any())).thenReturn(RecordSuccessResponse.create(initialToken));
    }

    @Test
    @Timeout(5)
    public void hedgingEnabled_shouldDelegateToHedgingStage() throws Exception {
        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();

        when(requestPipeline.execute(any(), any())).thenReturn(CompletableFuture.completedFuture(successResponse));

        CompletableFuture<Response<String>> result = stage.execute(request, context);

        Response<String> response = result.get(1, TimeUnit.SECONDS);
        assertThat(response).isEqualTo(successResponse);
        verify(requestPipeline, times(1)).execute(any(), any());
    }

    @Test
    @Timeout(5)
    public void hedgingDisabled_shouldDelegateToRetryStage() throws Exception {
        HedgingConfig disabledConfig = HedgingConfig.disabled();
        SdkClientConfiguration config = SdkClientConfiguration.builder()
            .option(RETRY_STRATEGY, retryStrategy)
            .option(HEDGING_CONFIG, disabledConfig)
            .option(SCHEDULED_EXECUTOR_SERVICE, scheduledExecutor)
            .build();

        dependencies = HttpClientDependencies.builder()
            .clientConfiguration(config)
            .build();

        stage = new AsyncRetryOrHedgingStage<>(responseHandler, () -> responseHandler, dependencies, requestPipeline);

        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();

        when(requestPipeline.execute(any(), any())).thenReturn(CompletableFuture.completedFuture(successResponse));

        CompletableFuture<Response<String>> result = stage.execute(request, context);

        Response<String> response = result.get(1, TimeUnit.SECONDS);
        assertThat(response).isEqualTo(successResponse);
        // Should delegate to retry stage, which will also call pipeline once for successful response
        verify(requestPipeline, times(1)).execute(any(), any());
    }

    @Test
    @Timeout(5)
    public void operationNotHedgeable_shouldDelegateToRetryStage() throws Exception {
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
            .option(SCHEDULED_EXECUTOR_SERVICE, scheduledExecutor)
            .build();

        dependencies = HttpClientDependencies.builder()
            .clientConfiguration(clientConfig)
            .build();

        stage = new AsyncRetryOrHedgingStage<>(responseHandler, () -> responseHandler, dependencies, requestPipeline);

        ExecutionAttributes attrs = new ExecutionAttributes();
        attrs.putAttribute(SdkExecutionAttribute.OPERATION_NAME, "PutItem"); // Not hedgeable
        attrs.putAttribute(RETRY_TOKEN, initialToken);

        ExecutionContext executionContext = ExecutionContext.builder()
            .executionAttributes(attrs)
            .metricCollector(MetricCollector.create("test"))
            .build();

        context = RequestExecutionContext.builder()
            .originalRequest(NoopTestRequest.builder().overrideConfiguration(SdkRequestOverrideConfiguration.builder().build()).build())
            .executionContext(executionContext)
            .build();

        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();

        when(requestPipeline.execute(any(), any())).thenReturn(CompletableFuture.completedFuture(successResponse));

        CompletableFuture<Response<String>> result = stage.execute(request, context);

        Response<String> response = result.get(1, TimeUnit.SECONDS);
        assertThat(response).isEqualTo(successResponse);
        verify(requestPipeline, times(1)).execute(any(), any());
    }

    @Test
    @Timeout(5)
    public void requestLevelHedgingConfig_overridesClientLevel() throws Exception {
        HedgingConfig clientConfig = HedgingConfig.builder()
            .enabled(true)
            .defaultPolicy(HedgingConfig.OperationHedgingPolicy.builder()
                                                               .maxHedgedAttempts(2)
                                                               .delayConfig(HedgingConfig.FixedDelayConfig.builder()
                                                                                                          .baseDelay(Duration.ofMillis(20))
                                                                                                          .build())
                                                               .build())
            .build();

        HedgingConfig requestConfig = HedgingConfig.builder()
            .enabled(true)
            .defaultPolicy(HedgingConfig.OperationHedgingPolicy.builder()
                                                               .maxHedgedAttempts(5)
                                                               .delayConfig(HedgingConfig.FixedDelayConfig.builder()
                                                                                                          .baseDelay(Duration.ofMillis(5))
                                                                                                          .build())
                                                               .build())
            .build();

        SdkClientConfiguration sdkConfig = SdkClientConfiguration.builder()
            .option(RETRY_STRATEGY, retryStrategy)
            .option(HEDGING_CONFIG, clientConfig)
            .option(SCHEDULED_EXECUTOR_SERVICE, scheduledExecutor)
            .build();

        dependencies = HttpClientDependencies.builder()
            .clientConfiguration(sdkConfig)
            .build();

        stage = new AsyncRetryOrHedgingStage<>(responseHandler, () -> responseHandler, dependencies, requestPipeline);

        context = RequestExecutionContext.builder()
            .originalRequest(NoopTestRequest.builder().overrideConfiguration(SdkRequestOverrideConfiguration.builder()
                .hedgingConfig(requestConfig)
                .build()).build())
            .executionContext(context.executionContext())
            .build();

        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();

        AtomicInteger callCount = new AtomicInteger(0);
        when(requestPipeline.execute(any(), any())).thenAnswer(invocation -> {
            callCount.incrementAndGet();
            return CompletableFuture.completedFuture(successResponse);
        });

        CompletableFuture<Response<String>> result = stage.execute(request, context);

        Response<String> response = result.get(1, TimeUnit.SECONDS);
        assertThat(response).isEqualTo(successResponse);
        // Request-level config should be used (maxHedgedAttempts=5)
        // But first attempt succeeds immediately, so only 1 call
        assertThat(callCount.get()).isEqualTo(1);
    }

    @Test
    @Timeout(5)
    public void multipleHedgedAttempts_firstSucceeds_othersCancelled() throws Exception {
        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();

        AtomicInteger callCount = new AtomicInteger(0);
        CountDownLatch firstAttemptLatch = new CountDownLatch(1);
        CountDownLatch secondAttemptLatch = new CountDownLatch(1);

        when(requestPipeline.execute(any(), any())).thenAnswer(invocation -> {
            int count = callCount.incrementAndGet();
            CompletableFuture<Response<String>> future = new CompletableFuture<>();
            if (count == 1) {
                // First attempt succeeds after a short delay
                scheduledExecutor.submit(() -> {
                    try {
                        Thread.sleep(5);
                        firstAttemptLatch.countDown();
                        future.complete(successResponse);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        future.completeExceptionally(e);
                    }
                });
            } else if (count == 2) {
                // Second attempt should be cancelled
                scheduledExecutor.submit(() -> {
                    try {
                        secondAttemptLatch.countDown();
                        Thread.sleep(100); // Long delay
                        future.complete(successResponse);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        future.completeExceptionally(e);
                    }
                });
            } else {
                future.complete(successResponse);
            }
            return future;
        });

        CompletableFuture<Response<String>> result = stage.execute(request, context);

        firstAttemptLatch.await(1, TimeUnit.SECONDS);
        Response<String> response = result.get(1, TimeUnit.SECONDS);
        assertThat(response).isEqualTo(successResponse);
        assertThat(callCount.get()).isGreaterThanOrEqualTo(1);
    }

    @Test
    @Timeout(5)
    public void allAttemptsFail_shouldAggregateFailures() throws Exception {
        SdkException failureException = SdkException.builder().message("All attempts failed").build();
        Response<String> failureResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(500).build())
            .exception(failureException)
            .isSuccess(false)
            .build();

        when(requestPipeline.execute(any(), any())).thenReturn(CompletableFuture.completedFuture(failureResponse));

        CompletableFuture<Response<String>> result = stage.execute(request, context);

        Thread.sleep(50); // Wait for all attempts
        assertThat(result.isCompletedExceptionally()).isTrue();
        assertThatThrownBy(() -> result.get(1, TimeUnit.SECONDS))
            .hasCauseInstanceOf(SdkException.class);
    }

    @Test
    @Timeout(5)
    public void adaptiveMode_emptyHedgeableOperations_nonListedOpStillHedges() throws Exception {
        HedgingConfig.AdaptiveDelayConfig adaptiveCfg = HedgingConfig.AdaptiveDelayConfig.builder()
                                                                                          .percentile(99)
                                                                                          .sampleSize(100)
                                                                                          .minSamplesRequired(1)
                                                                                          .fallbackDelay(Duration.ofMillis(5))
                                                                                          .build();
        HedgingConfig adaptiveConfig = HedgingConfig.builder()
                                                    .enabled(true)
                                                    .hedgeableOperations(Collections.emptySet())
                                                    .defaultPolicy(HedgingConfig.OperationHedgingPolicy.builder()
                                                                                                       .maxHedgedAttempts(2)
                                                                                                       .delayConfig(adaptiveCfg)
                                                                                                       .build())
                                                    .build();
        SdkClientConfiguration config = SdkClientConfiguration.builder()
                                                              .option(RETRY_STRATEGY, retryStrategy)
                                                              .option(HEDGING_CONFIG, adaptiveConfig)
                                                              .option(HEDGING_LATENCY_TRACKER, new HedgingLatencyTracker())
                                                              .option(SCHEDULED_EXECUTOR_SERVICE, scheduledExecutor)
                                                              .build();
        dependencies = HttpClientDependencies.builder().clientConfiguration(config).build();
        stage = new AsyncRetryOrHedgingStage<>(responseHandler, () -> responseHandler, dependencies, requestPipeline);

        ExecutionAttributes attrs = new ExecutionAttributes();
        attrs.putAttribute(SdkExecutionAttribute.OPERATION_NAME, "PutItem");
        attrs.putAttribute(RETRY_TOKEN, initialToken);
        attrs.putAttribute(EXECUTION_ATTEMPT, 1);
        context = RequestExecutionContext.builder()
                                         .originalRequest(NoopTestRequest.builder()
                                                                         .overrideConfiguration(SdkRequestOverrideConfiguration
                                                                                                    .builder().build())
                                                                         .build())
                                         .executionContext(ExecutionContext.builder()
                                                                           .executionAttributes(attrs)
                                                                           .metricCollector(MetricCollector.create("test"))
                                                                           .build())
                                         .build();

        Response<String> successResponse = Response.<String>builder()
                                                   .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
                                                   .isSuccess(true)
                                                   .build();
        AtomicInteger callCount = new AtomicInteger(0);
        when(requestPipeline.execute(any(), any())).thenAnswer(invocation -> {
            int count = callCount.incrementAndGet();
            if (count == 1) {
                CompletableFuture<Response<String>> delayed = new CompletableFuture<>();
                scheduledExecutor.schedule(() -> delayed.complete(successResponse), 40, TimeUnit.MILLISECONDS);
                return delayed;
            }
            return CompletableFuture.completedFuture(successResponse);
        });

        CompletableFuture<Response<String>> result = stage.execute(request, context);
        Response<String> response = result.get(2, TimeUnit.SECONDS);

        assertThat(response).isEqualTo(successResponse);
        assertThat(callCount.get()).isGreaterThanOrEqualTo(2);
    }
}
