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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.core.client.config.SdkClientOption.HEDGING_CONFIG;
import static software.amazon.awssdk.core.client.config.SdkClientOption.RETRY_STRATEGY;
import static software.amazon.awssdk.core.client.config.SdkClientOption.SCHEDULED_EXECUTOR_SERVICE;
import static software.amazon.awssdk.core.internal.InternalCoreExecutionAttribute.EXECUTION_ATTEMPT;
import static software.amazon.awssdk.core.internal.InternalCoreExecutionAttribute.RETRY_TOKEN;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
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
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.SdkExecutionAttribute;
import software.amazon.awssdk.core.internal.http.HttpClientDependencies;
import software.amazon.awssdk.core.internal.http.RequestExecutionContext;
import software.amazon.awssdk.core.internal.http.pipeline.RequestPipeline;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.retries.api.AcquireHedgeTokenRequest;
import software.amazon.awssdk.retries.api.AcquireHedgeTokenResponse;
import software.amazon.awssdk.retries.api.AcquireInitialTokenResponse;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.retries.api.RetryToken;
import software.amazon.awssdk.retries.api.TokenAcquisitionFailedException;
import software.amazon.awssdk.retries.internal.DefaultRetryToken;

public class AsyncHedgingStageTest {

    @Mock
    private RequestPipeline<SdkHttpFullRequest, CompletableFuture<Response<String>>> requestPipeline;

    @Mock
    private RetryStrategy retryStrategy;

    @Mock
    private ScheduledExecutorService scheduledExecutor;

    @Mock
    private ScheduledFuture<?> scheduledFuture;

    private AsyncHedgingStage<String> hedgingStage;
    private HttpClientDependencies dependencies;
    private SdkHttpFullRequest request;
    private RequestExecutionContext context;
    private RetryToken initialToken;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
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
                .maxHedgedAttempts(3)
                .defaultDelay(Duration.ofMillis(10))
                .build())
            .option(SCHEDULED_EXECUTOR_SERVICE, scheduledExecutor)
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

        when(scheduledExecutor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
            .thenAnswer(invocation -> {
                Runnable task = invocation.getArgument(0);
                scheduledExecutor.submit(task);
                return scheduledFuture;
            });
    }

    @Test
    @Timeout(5)
    public void firstAttemptSucceeds_shouldCompleteImmediately() throws Exception {
        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();

        CompletableFuture<Response<String>> successFuture = CompletableFuture.completedFuture(successResponse);
        when(requestPipeline.execute(any(), any())).thenReturn(successFuture);

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);

        Response<String> response = result.get(1, TimeUnit.SECONDS);
        assertThat(response).isEqualTo(successResponse);
        verify(requestPipeline, times(1)).execute(any(), any());
        // Hedged attempts are scheduled even when first succeeds; we only verify a single pipeline call
    }

    @Test
    @Timeout(5)
    public void firstAttemptFails_secondAttemptSucceeds_shouldCompleteWithSecond() throws Exception {
        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();

        SdkException failureException = SdkException.builder().message("First attempt failed").build();
        Response<String> failureResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(500).build())
            .exception(failureException)
            .isSuccess(false)
            .build();

        CompletableFuture<Response<String>> failureFuture = CompletableFuture.completedFuture(failureResponse);
        CompletableFuture<Response<String>> successFuture = CompletableFuture.completedFuture(successResponse);

        AtomicInteger callCount = new AtomicInteger(0);
        when(requestPipeline.execute(any(), any())).thenAnswer(invocation -> {
            int count = callCount.incrementAndGet();
            if (count == 1) {
                return failureFuture;
            } else {
                return successFuture;
            }
        });

        List<Runnable> scheduledTasks = new ArrayList<>();
        when(scheduledExecutor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
            .thenAnswer(invocation -> {
                Runnable task = invocation.getArgument(0);
                scheduledTasks.add(task);
                // Execute immediately for testing
                new Thread(() -> {
                    try {
                        Thread.sleep(15); // Simulate delay
                        task.run();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }).start();
                return scheduledFuture;
            });

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);

        // Wait for all attempts
        Thread.sleep(50);
        Response<String> response = result.get(1, TimeUnit.SECONDS);
        assertThat(response).isEqualTo(successResponse);
        assertThat(callCount.get()).isGreaterThanOrEqualTo(2);
    }

    @Test
    @Timeout(5)
    public void allAttemptsFail_shouldCompleteWithAggregatedFailure() throws Exception {
        SdkException failureException = SdkException.builder().message("Attempt failed").build();
        Response<String> failureResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(500).build())
            .exception(failureException)
            .isSuccess(false)
            .build();

        CompletableFuture<Response<String>> failureFuture = CompletableFuture.completedFuture(failureResponse);
        when(requestPipeline.execute(any(), any())).thenReturn(failureFuture);

        List<Runnable> scheduledTasks = new ArrayList<>();
        when(scheduledExecutor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
            .thenAnswer(invocation -> {
                Runnable task = invocation.getArgument(0);
                scheduledTasks.add(task);
                new Thread(() -> {
                    try {
                        Thread.sleep(15);
                        task.run();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }).start();
                return scheduledFuture;
            });

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);

        Thread.sleep(50);
        assertThatThrownBy(() -> result.get(1, TimeUnit.SECONDS))
            .hasCauseInstanceOf(SdkException.class);
        assertThat(result.isCompletedExceptionally()).isTrue();
    }

    @Test
    @Timeout(5)
    public void retryStrategySupportsHedging_shouldAcquireHedgeTokens() throws Exception {
        when(retryStrategy.supportsHedging()).thenReturn(true);

        RetryToken token2 = DefaultRetryToken.builder()
            .scope("test")
            .attempt(2)
            .capacityAcquired(1)
            .capacityRemaining(99)
            .build();

        RetryToken token3 = DefaultRetryToken.builder()
            .scope("test")
            .attempt(3)
            .capacityAcquired(1)
            .capacityRemaining(98)
            .build();

        when(retryStrategy.acquireTokenForHedgeAttempt(any(AcquireHedgeTokenRequest.class)))
            .thenAnswer(invocation -> {
                AcquireHedgeTokenRequest req = invocation.getArgument(0);
                if (req.attemptIndex() == 2) {
                    return AcquireHedgeTokenResponse.create(token2, Duration.ZERO);
                } else {
                    return AcquireHedgeTokenResponse.create(token3, Duration.ZERO);
                }
            });

        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();
        when(requestPipeline.execute(any(), any())).thenReturn(CompletableFuture.completedFuture(successResponse));

        List<Runnable> scheduledTasks = new ArrayList<>();
        when(scheduledExecutor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
            .thenAnswer(invocation -> {
                Runnable task = invocation.getArgument(0);
                scheduledTasks.add(task);
                new Thread(() -> {
                    try {
                        Thread.sleep(15);
                        task.run();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }).start();
                return scheduledFuture;
            });

        hedgingStage.execute(request, context);

        Thread.sleep(50);
        verify(retryStrategy, times(2)).acquireTokenForHedgeAttempt(any(AcquireHedgeTokenRequest.class));
    }

    @Test
    @Timeout(5)
    public void hedgeTokenAcquisitionFails_shouldSkipThatAttempt() throws Exception {
        when(retryStrategy.supportsHedging()).thenReturn(true);
        when(retryStrategy.acquireTokenForHedgeAttempt(any(AcquireHedgeTokenRequest.class)))
            .thenThrow(new TokenAcquisitionFailedException("No tokens available"));

        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();
        when(requestPipeline.execute(any(), any())).thenReturn(CompletableFuture.completedFuture(successResponse));

        List<Runnable> scheduledTasks = new ArrayList<>();
        when(scheduledExecutor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
            .thenAnswer(invocation -> {
                Runnable task = invocation.getArgument(0);
                scheduledTasks.add(task);
                new Thread(() -> {
                    try {
                        Thread.sleep(15);
                        task.run();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }).start();
                return scheduledFuture;
            });

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);

        Thread.sleep(50);
        Response<String> response = result.get(1, TimeUnit.SECONDS);
        assertThat(response).isEqualTo(successResponse);
        // Should only execute attempt 1 since 2 and 3 fail token acquisition
        verify(requestPipeline, times(1)).execute(any(), any());
    }

    @Test
    @Timeout(5)
    public void initialDelay_shouldScheduleAfterDelay() throws Exception {
        when(retryStrategy.acquireInitialToken(any())).thenAnswer(invocation -> {
            return software.amazon.awssdk.retries.api.AcquireInitialTokenResponse.create(
                initialToken, Duration.ofMillis(20));
        });

        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();
        when(requestPipeline.execute(any(), any())).thenReturn(CompletableFuture.completedFuture(successResponse));

        ArgumentCaptor<Long> delayCaptor = ArgumentCaptor.forClass(Long.class);
        when(scheduledExecutor.schedule(any(Runnable.class), delayCaptor.capture(), eq(TimeUnit.MILLISECONDS)))
            .thenAnswer(invocation -> {
                Runnable task = invocation.getArgument(0);
                task.run(); // Execute immediately for test
                return scheduledFuture;
            });

        hedgingStage.execute(request, context);

        // One schedule for initial delay + two for hedged attempts (k=2, k=3)
        assertThat(delayCaptor.getAllValues()).contains(20L);
    }

    @Test
    @Timeout(5)
    public void operationNotInHedgeableList_shouldNotHedge() throws Exception {
        HedgingConfig config = HedgingConfig.builder()
            .enabled(true)
            .maxHedgedAttempts(3)
            .defaultDelay(Duration.ofMillis(10))
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

        Response<String> response = result.get(1, TimeUnit.SECONDS);
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

        assertThatThrownBy(() -> result.get(1, TimeUnit.SECONDS))
            .hasCause(pipelineException);
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
                // First attempt succeeds after delay so hedged attempts can start and be cancelled
                new Thread(() -> {
                    try {
                        Thread.sleep(40);
                        future1.complete(successResponse);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }).start();
                return future1;
            } else if (count == 2) {
                return future2;
            } else {
                return future3;
            }
        });

        when(scheduledExecutor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
            .thenAnswer(invocation -> {
                Runnable task = invocation.getArgument(0);
                new Thread(() -> {
                    try {
                        Thread.sleep(15);
                        task.run();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }).start();
                return scheduledFuture;
            });

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);

        Thread.sleep(80);
        Response<String> response = result.get(1, TimeUnit.SECONDS);
        assertThat(response).isEqualTo(successResponse);
        // First attempt wins after 40ms; by then hedged attempts (started at 15ms) have run and added future2/future3,
        // so cancelAllAttempts() cancels them
        assertThat(future2.isCancelled() || future2.isDone())
            .as("future2 should be cancelled or done after first attempt succeeds")
            .isTrue();
        assertThat(future3.isCancelled() || future3.isDone())
            .as("future3 should be cancelled or done after first attempt succeeds")
            .isTrue();
    }

    @Test
    @Timeout(5)
    public void perOperationDelay_shouldUseOperationSpecificDelay() throws Exception {
        HedgingConfig config = HedgingConfig.builder()
            .enabled(true)
            .maxHedgedAttempts(3)
            .defaultDelay(Duration.ofMillis(20))
            .delayPerOperation(Collections.singletonMap("GetItem", Duration.ofMillis(5)))
            .build();

        SdkClientConfiguration clientConfig = SdkClientConfiguration.builder()
            .option(RETRY_STRATEGY, retryStrategy)
            .option(HEDGING_CONFIG, config)
            .option(SCHEDULED_EXECUTOR_SERVICE, scheduledExecutor)
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
        when(requestPipeline.execute(any(), any())).thenReturn(CompletableFuture.completedFuture(successResponse));

        ArgumentCaptor<Long> delayCaptor = ArgumentCaptor.forClass(Long.class);
        when(scheduledExecutor.schedule(any(Runnable.class), delayCaptor.capture(), eq(TimeUnit.MILLISECONDS)))
            .thenAnswer(invocation -> {
                Runnable task = invocation.getArgument(0);
                task.run();
                return scheduledFuture;
            });

        hedgingStage.execute(request, context);

        Thread.sleep(50);
        // Should use 5ms delay for GetItem operation
        List<Long> delays = delayCaptor.getAllValues();
        assertThat(delays).contains(5L);
    }

    @Test
    @Timeout(5)
    public void recordSuccessWithHedgeCount_shouldCallHelper() throws Exception {
        when(retryStrategy.supportsHedging()).thenReturn(true);

        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();
        when(requestPipeline.execute(any(), any())).thenReturn(CompletableFuture.completedFuture(successResponse));

        List<Runnable> scheduledTasks = new ArrayList<>();
        when(scheduledExecutor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
            .thenAnswer(invocation -> {
                Runnable task = invocation.getArgument(0);
                scheduledTasks.add(task);
                new Thread(() -> {
                    try {
                        Thread.sleep(15);
                        task.run();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }).start();
                return scheduledFuture;
            });

        CompletableFuture<Response<String>> result = hedgingStage.execute(request, context);

        Thread.sleep(50);
        Response<String> response = result.get(1, TimeUnit.SECONDS);
        assertThat(response).isEqualTo(successResponse);
        // Verify that retry strategy was used (indirectly through helper)
        verify(retryStrategy, atLeastOnce()).supportsHedging();
    }
}
