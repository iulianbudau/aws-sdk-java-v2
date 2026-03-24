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
import static org.mockito.Mockito.doAnswer;
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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
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
import software.amazon.awssdk.retries.api.AcquireHedgeTokenRequest;
import software.amazon.awssdk.retries.api.AcquireHedgeTokenResponse;
import software.amazon.awssdk.retries.api.AcquireInitialTokenResponse;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.retries.api.RetryToken;
import software.amazon.awssdk.retries.api.TokenAcquisitionFailedException;
import software.amazon.awssdk.retries.internal.DefaultRetryToken;

public class HedgingStageTest {

    @Mock
    private RequestPipeline<SdkHttpFullRequest, Response<String>> requestPipeline;

    @Mock
    private RetryStrategy retryStrategy;

    @Mock
    private ScheduledExecutorService scheduledExecutor;

    @Mock
    private ScheduledFuture<?> scheduledFuture;

    @Mock
    private MetricCollector metricCollector;

    private HedgingStage<String> hedgingStage;
    private HttpClientDependencies dependencies;
    private SdkHttpFullRequest request;
    private RequestExecutionContext context;
    private RetryToken initialToken;
    private ExecutorService executorService;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        executorService = Executors.newCachedThreadPool();

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

        hedgingStage = new HedgingStage<>(dependencies, requestPipeline);

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
            .originalRequest(NoopTestRequest.builder().overrideConfiguration(SdkRequestOverrideConfiguration.builder().build()).build())
            .executionContext(executionContext)
            .build();

        when(retryStrategy.acquireInitialToken(any()))
            .thenReturn(AcquireInitialTokenResponse.create(initialToken, Duration.ZERO));
        when(retryStrategy.maxAttempts()).thenReturn(3);

        // Sync HedgingStage uses same executor for supplyAsync (execute) and schedule; stub both so tasks run
        doAnswer(invocation -> {
            invocation.getArgument(0, Runnable.class).run();
            return null;
        }).when(scheduledExecutor).execute(any(Runnable.class));
        when(scheduledExecutor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
            .thenAnswer(invocation -> {
                Runnable task = invocation.getArgument(0);
                executorService.submit(() -> {
                    try {
                        Thread.sleep(15);
                        task.run();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                return scheduledFuture;
            });
    }

    @AfterEach
    public void tearDown() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    @Test
    @Timeout(5)
    public void firstAttemptSucceeds_shouldReturnImmediately() throws Exception {
        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();

        when(requestPipeline.execute(any(), any())).thenReturn(successResponse);

        Response<String> result = hedgingStage.execute(request, context);

        assertThat(result).isEqualTo(successResponse);
        verify(requestPipeline, times(1)).execute(any(), any());
        // Hedged attempts are scheduled even when first succeeds; we only verify a single pipeline call
    }

    @Test
    @Timeout(5)
    public void firstAttemptFails_secondAttemptSucceeds_shouldReturnSecond() throws Exception {
        // Run supplyAsync tasks on executorService so main thread can reach scheduleHedgedAttempts before attempt 1 completes
        doAnswer(invocation -> {
            executorService.submit(invocation.getArgument(0, Runnable.class));
            return null;
        }).when(scheduledExecutor).execute(any(Runnable.class));

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
                try {
                    Thread.sleep(40);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return failureResponse;
            } else {
                return successResponse;
            }
        });

        Response<String> result = hedgingStage.execute(request, context);

        assertThat(result).isEqualTo(successResponse);
        assertThat(callCount.get()).isGreaterThanOrEqualTo(2);
    }

    @Test
    @Timeout(5)
    public void hedgingRequestHeader_shouldContainAttemptMaxAndDelay() throws Exception {
        // Run supplyAsync tasks on executorService so main thread can reach scheduleHedgedAttempts before attempt 1 completes
        doAnswer(invocation -> {
            executorService.submit(invocation.getArgument(0, Runnable.class));
            return null;
        }).when(scheduledExecutor).execute(any(Runnable.class));

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

        // Hedged attempts run concurrently; collect executed requests in a thread-safe list.
        List<SdkHttpFullRequest> executedRequests = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger callCount = new AtomicInteger(0);
        when(requestPipeline.execute(any(), any())).thenAnswer(invocation -> {
            SdkHttpFullRequest attemptRequest = invocation.getArgument(0);
            executedRequests.add(attemptRequest);
            int count = callCount.incrementAndGet();
            if (count == 1) {
                return failureResponse;
            } else {
                return successResponse;
            }
        });

        Response<String> result = hedgingStage.execute(request, context);

        assertThat(result).isEqualTo(successResponse);
        assertThat(executedRequests.size()).isGreaterThanOrEqualTo(2);

        List<SdkHttpFullRequest> executedRequestsSnapshot;
        synchronized (executedRequests) {
            executedRequestsSnapshot = new ArrayList<>(executedRequests);
        }

        List<String> hedgeHeaders = new ArrayList<>();
        for (SdkHttpFullRequest r : executedRequestsSnapshot) {
            assertThat(r.firstMatchingHeader("amz-sdk-request")).isEmpty();
            hedgeHeaders.add(r.firstMatchingHeader("amz-sdk-hedge-request").orElse(null));
        }

        assertThat(hedgeHeaders).contains("attempt=1; max=3; delay=0ms", "attempt=2; max=3; delay=10ms");
    }

    @Test
    @Timeout(5)
    public void allAttemptsFail_shouldThrowAggregatedException() throws Exception {
        SdkException failureException = RetryableException.builder().message("Attempt failed").build();
        Response<String> failureResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(500).build())
            .exception(failureException)
            .isSuccess(false)
            .build();

        when(requestPipeline.execute(any(), any())).thenReturn(failureResponse);

        assertThatThrownBy(() -> hedgingStage.execute(request, context))
            .isInstanceOf(SdkException.class)
            .hasMessageContaining("Attempt failed");
    }

    @Test
    @Timeout(5)
    public void retryStrategySupportsHedging_shouldAcquireHedgeTokens() throws Exception {
        when(retryStrategy.supportsHedging()).thenReturn(true);
        when(retryStrategy.maxAttempts()).thenReturn(3);

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
        
        // Delay attempt 1 so scheduled tasks can acquire tokens before it completes
        CountDownLatch attempt1Started = new CountDownLatch(1);
        CountDownLatch canComplete = new CountDownLatch(1);
        when(requestPipeline.execute(any(), any())).thenAnswer(invocation -> {
            attempt1Started.countDown();
            canComplete.await(2, TimeUnit.SECONDS);
            return successResponse;
        });

        // Run scheduled tasks which will acquire tokens
        when(scheduledExecutor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
            .thenAnswer(invocation -> {
                Runnable task = invocation.getArgument(0);
                new Thread(() -> {
                    try {
                        attempt1Started.await(1, TimeUnit.SECONDS);
                        Thread.sleep(5);
                        task.run();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }).start();
                return scheduledFuture;
            });

        // Execute in separate thread since it blocks
        CompletableFuture<Response<String>> result = CompletableFuture.supplyAsync(() -> {
            try {
                return hedgingStage.execute(request, context);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, executorService);

        // Wait for scheduled tasks to acquire tokens
        Thread.sleep(50);
        // Allow attempt 1 to complete
        canComplete.countDown();
        
        result.get(2, TimeUnit.SECONDS);
        verify(retryStrategy, times(2)).acquireTokenForHedgeAttempt(any(AcquireHedgeTokenRequest.class));
    }

    @Test
    @Timeout(5)
    public void hedgeTokenAcquisitionFails_shouldSkipThatAttempt() throws Exception {
        when(retryStrategy.supportsHedging()).thenReturn(true);
        when(retryStrategy.maxAttempts()).thenReturn(3);
        when(retryStrategy.acquireTokenForHedgeAttempt(any(AcquireHedgeTokenRequest.class)))
            .thenThrow(new TokenAcquisitionFailedException("No tokens available"));

        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();
        when(requestPipeline.execute(any(), any())).thenReturn(successResponse);

        Response<String> result = hedgingStage.execute(request, context);

        assertThat(result).isEqualTo(successResponse);
        // Should only execute attempt 1 since 2 and 3 fail token acquisition
        verify(requestPipeline, times(1)).execute(any(), any());
    }

    @Test
    @Timeout(5)
    public void initialDelay_shouldWaitBeforeStarting() throws Exception {
        when(retryStrategy.acquireInitialToken(any())).thenAnswer(invocation -> {
            return software.amazon.awssdk.retries.api.AcquireInitialTokenResponse.create(
                initialToken, Duration.ofMillis(20));
        });

        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();
        when(requestPipeline.execute(any(), any())).thenReturn(successResponse);

        long startTime = System.currentTimeMillis();
        hedgingStage.execute(request, context);
        long elapsedTime = System.currentTimeMillis() - startTime;

        // Should have waited at least 20ms
        assertThat(elapsedTime).isGreaterThanOrEqualTo(15);
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

        hedgingStage = new HedgingStage<>(dependencies, requestPipeline);

        ExecutionAttributes attrs = new ExecutionAttributes();
        attrs.putAttribute(SdkExecutionAttribute.OPERATION_NAME, "PutItem"); // Not in hedgeable list
        attrs.putAttribute(RETRY_TOKEN, initialToken);

        ExecutionContext executionContext = ExecutionContext.builder()
            .executionAttributes(attrs)
            .build();

        context = RequestExecutionContext.builder()
            .originalRequest(NoopTestRequest.builder().overrideConfiguration(SdkRequestOverrideConfiguration.builder().build()).build())
            .executionContext(executionContext)
            .build();

        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();
        when(requestPipeline.execute(any(), any())).thenReturn(successResponse);

        Response<String> result = hedgingStage.execute(request, context);

        assertThat(result).isEqualTo(successResponse);
        // Should only execute once since hedging is disabled for this operation
        verify(requestPipeline, times(1)).execute(any(), any());
    }

    @Test
    @Timeout(5)
    public void requestPipelineThrowsException_shouldPropagateException() throws Exception {
        RuntimeException pipelineException = new RuntimeException("Pipeline error");
        when(requestPipeline.execute(any(), any())).thenThrow(pipelineException);

        assertThatThrownBy(() -> hedgingStage.execute(request, context))
            .isInstanceOf(SdkException.class)
            .hasCauseInstanceOf(RuntimeException.class)
            .getCause()
            .hasMessageContaining("Pipeline error");
    }

    @Test
    @Timeout(5)
    public void successfulResponse_cancelsOtherAttempts() throws Exception {
        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();

        AtomicInteger callCount = new AtomicInteger(0);
        when(requestPipeline.execute(any(), any())).thenAnswer(invocation -> {
            int count = callCount.incrementAndGet();
            if (count == 1) {
                // First attempt succeeds
                return successResponse;
            } else {
                // Other attempts should not be called if first succeeds immediately
                return successResponse;
            }
        });

        Response<String> result = hedgingStage.execute(request, context);

        assertThat(result).isEqualTo(successResponse);
        // First attempt succeeds immediately, so only one call
        verify(requestPipeline, times(1)).execute(any(), any());
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

        hedgingStage = new HedgingStage<>(dependencies, requestPipeline);

        Response<String> successResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .isSuccess(true)
            .build();
        when(requestPipeline.execute(any(), any())).thenReturn(successResponse);

        ArgumentCaptor<Long> delayCaptor = ArgumentCaptor.forClass(Long.class);
        when(scheduledExecutor.schedule(any(Runnable.class), delayCaptor.capture(), eq(TimeUnit.MILLISECONDS)))
            .thenAnswer(invocation -> {
                Runnable task = invocation.getArgument(0);
                executorService.submit(() -> {
                    try {
                        Thread.sleep(5);
                        task.run();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                return scheduledFuture;
            });

        hedgingStage.execute(request, context);

        Thread.sleep(50);
        // GetItem base delay 5ms: attempt 2 at 5ms, attempt 3 at 10ms (staggered)
        List<Long> delays = delayCaptor.getAllValues();
        assertThat(delays).contains(5L, 10L);
    }

    @Test
    @Timeout(5)
    public void exceptionFromPipeline_shouldHandleGracefully() throws Exception {
        SdkException exception = RetryableException.builder().message("Pipeline exception").build();
        Response<String> failureResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(500).build())
            .exception(exception)
            .isSuccess(false)
            .build();

        when(requestPipeline.execute(any(), any())).thenReturn(failureResponse);

        assertThatThrownBy(() -> hedgingStage.execute(request, context))
            .isInstanceOf(SdkException.class)
            .hasMessageContaining("Pipeline exception");
    }

    @Test
    @Timeout(5)
    public void multipleFailures_shouldAggregateExceptions() throws Exception {
        SdkException exception1 = RetryableException.builder().message("First failure").build();
        SdkException exception2 = RetryableException.builder().message("Second failure").build();
        SdkException exception3 = RetryableException.builder().message("Third failure").build();

        Response<String> failureResponse1 = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(500).build())
            .exception(exception1)
            .isSuccess(false)
            .build();

        Response<String> failureResponse2 = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(500).build())
            .exception(exception2)
            .isSuccess(false)
            .build();

        Response<String> failureResponse3 = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(500).build())
            .exception(exception3)
            .isSuccess(false)
            .build();

        AtomicInteger callCount = new AtomicInteger(0);
        when(requestPipeline.execute(any(), any())).thenAnswer(invocation -> {
            int count = callCount.incrementAndGet();
            if (count == 1) {
                return failureResponse1;
            } else if (count == 2) {
                return failureResponse2;
            } else {
                return failureResponse3;
            }
        });

        assertThatThrownBy(() -> hedgingStage.execute(request, context))
            .isInstanceOf(SdkException.class);
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
        when(requestPipeline.execute(any(), any())).thenReturn(successResponse);
        // Do not run scheduled hedge tasks so only attempt 1 runs → HEDGE_COUNT = 0
        when(scheduledExecutor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
            .thenReturn((ScheduledFuture) scheduledFuture);

        Response<String> result = hedgingStage.execute(request, contextWithMetrics);

        assertThat(result).isEqualTo(successResponse);
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
                .maxHedgedAttempts(2)
                .defaultDelay(Duration.ofMillis(10))
                .build())
            .option(SCHEDULED_EXECUTOR_SERVICE, scheduledExecutor)
            .build();
        dependencies = HttpClientDependencies.builder().clientConfiguration(configWithTwoAttempts).build();
        hedgingStage = new HedgingStage<>(dependencies, requestPipeline);
        doAnswer(invocation -> {
            invocation.getArgument(0, Runnable.class).run();
            return null;
        }).when(scheduledExecutor).execute(any(Runnable.class));
        when(scheduledExecutor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
            .thenAnswer(invocation -> {
                Runnable task = invocation.getArgument(0, Runnable.class);
                executorService.submit(() -> {
                    try {
                        Thread.sleep(15);
                        task.run();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                return scheduledFuture;
            });
        doAnswer(invocation -> {
            executorService.submit(invocation.getArgument(0, Runnable.class));
            return null;
        }).when(scheduledExecutor).execute(any(Runnable.class));

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
                try {
                    Thread.sleep(40);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return failureResponse;
            }
            return successResponse;
        });

        Response<String> result = hedgingStage.execute(request, contextWithMetrics);

        assertThat(result).isEqualTo(successResponse);
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
        AtomicInteger executeCallCount = new AtomicInteger(0);
        when(requestPipeline.execute(any(), any())).thenAnswer(invocation -> {
            int count = executeCallCount.incrementAndGet();
            if (count == 1) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return failureResponse;
        });
        when(scheduledExecutor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
            .thenAnswer(invocation -> {
                Runnable task = invocation.getArgument(0);
                executorService.submit(() -> {
                    try {
                        Thread.sleep(15);
                        task.run();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                return scheduledFuture;
            });
        // Run supplyAsync tasks asynchronously so main thread reaches scheduleHedgedAttempts before attempt 1 completes
        doAnswer(invocation -> {
            executorService.submit(invocation.getArgument(0, Runnable.class));
            return null;
        }).when(scheduledExecutor).execute(any(Runnable.class));

        assertThatThrownBy(() -> hedgingStage.execute(request, contextWithMetrics))
            .isInstanceOf(SdkException.class)
            .hasMessageContaining("Attempt failed");

        verify(metricCollector).reportMetric(eq(CoreMetric.HEDGE_COUNT), eq(2));
    }

    /**
     * When the first attempt fails immediately (before attempt 2 actually starts running), we must NOT fail fast:
     * we keep the hedge plan alive so attempt 2 can start and succeed, as long as the error is retryable.
     */
    @Test
    @Timeout(5)
    public void firstAttemptFailsImmediately_beforeSecondStarts_doesNotFailFast_secondSucceeds() throws Exception {
        when(retryStrategy.supportsHedging()).thenReturn(false);
        doAnswer(invocation -> {
            executorService.submit(invocation.getArgument(0, Runnable.class));
            return null;
        }).when(scheduledExecutor).execute(any(Runnable.class));

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
                // First attempt fails immediately, before attempt 2's delayed start
                return failureResponse;
            }
            return successResponse;
        });
        when(scheduledExecutor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
            .thenAnswer(invocation -> {
                Runnable task = invocation.getArgument(0);
                // Attempt 2 (and any others) begin after a delay; at the time of attempt 1's failure,
                // this task has only been scheduled, not yet executed.
                executorService.submit(() -> {
                    try {
                        Thread.sleep(30);
                        task.run();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                return scheduledFuture;
            });

        Response<String> result = hedgingStage.execute(request, context);

        assertThat(result).isEqualTo(successResponse);
        assertThat(callCount.get()).as("Should not fail fast; attempt 2 must have run").isGreaterThanOrEqualTo(2);
    }

    /**
     * Non-retryable error on first attempt: fail immediately after first attempt, do not start hedge attempts.
     * Scheduled tasks are not run so only attempt 1 executes; we assert pipeline is called once.
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
        when(requestPipeline.execute(any(), any())).thenReturn(failureResponse);
        when(scheduledExecutor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
            .thenReturn((ScheduledFuture) scheduledFuture);

        assertThatThrownBy(() -> hedgingStage.execute(request, context))
            .isInstanceOf(NonRetryableException.class)
            .hasMessageContaining("Validation failed");
        verify(requestPipeline, times(1)).execute(any(), any());
    }

    /**
     * When fewer than maxHedgedAttempts are started (e.g. hedge token acquisition fails for attempts 2 and 3),
     * and all started attempts fail, the stage must still complete with aggregated failure.
     */
    @Test
    @Timeout(5)
    public void fewerAttemptsStarted_allFail_shouldCompleteWithAggregatedFailure() throws Exception {
        when(retryStrategy.supportsHedging()).thenReturn(true);
        when(retryStrategy.acquireTokenForHedgeAttempt(any(AcquireHedgeTokenRequest.class)))
            .thenThrow(new TokenAcquisitionFailedException("No tokens"));

        SdkException failureException = RetryableException.builder().message("Attempt 1 failed").build();
        Response<String> failureResponse = Response.<String>builder()
            .httpResponse(SdkHttpResponse.builder().statusCode(500).build())
            .exception(failureException)
            .isSuccess(false)
            .build();
        when(requestPipeline.execute(any(), any())).thenReturn(failureResponse);
        when(scheduledExecutor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
            .thenAnswer(invocation -> {
                invocation.getArgument(0, Runnable.class).run();
                return scheduledFuture;
            });

        assertThatThrownBy(() -> hedgingStage.execute(request, context))
            .isInstanceOf(SdkException.class)
            .hasMessageContaining("Attempt 1 failed");
        verify(requestPipeline, times(1)).execute(any(), any());
    }

    /**
     * When an attempt completes with null (pipeline returns null), the stage must complete exceptionally
     * so the caller is not left hanging.
     */
    @Test
    @Timeout(5)
    public void attemptCompletesWithNullResponseAndNullException_shouldCompleteExceptionally() throws Exception {
        when(requestPipeline.execute(any(), any())).thenReturn(null);
        when(scheduledExecutor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
            .thenAnswer(invocation -> {
                invocation.getArgument(0, Runnable.class).run();
                return scheduledFuture;
            });

        assertThatThrownBy(() -> hedgingStage.execute(request, context))
            .isInstanceOf(SdkException.class)
            .hasMessageContaining("null response and null exception");
    }
}
