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

package software.amazon.awssdk.core.internal.http.pipeline.stages.utils;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.core.client.config.HedgingConfig;
import software.amazon.awssdk.utils.Logger;

@SdkInternalApi
public final class HedgingLatencyTracker {
    private static final Logger log = Logger.loggerFor(HedgingLatencyTracker.class);

    private final Map<String, OperationLatencyWindow> windowsByOperation = new ConcurrentHashMap<>();

    public void record(String operationName, Duration latency, HedgingConfig.AdaptiveDelayConfig config) {
        if (operationName == null || latency == null || config == null) {
            log.debug(() -> String.format(
                "[HEDGE-ADAPTIVE] reason=ADAPTIVE_RECORD_SKIPPED_INVALID_INPUT operationName=%s latencyNull=%s configNull=%s",
                operationName,
                latency == null,
                config == null));
            return;
        }
        OperationLatencyWindow window = windowsByOperation.computeIfAbsent(operationName,
                                                                           k -> new OperationLatencyWindow(config.sampleSize()));
        window.record(latency.toNanos());
        log.debug(() -> String.format(
            "[HEDGE-ADAPTIVE] reason=ADAPTIVE_RECORD operationName=%s recordedLatencyNanos=%d recordedLatencyMs=%d "
            + "sampleSize=%d currentWindowSize=%d",
            operationName,
            latency.toNanos(),
            latency.toMillis(),
            config.sampleSize(),
            window.currentSize()));
    }

    public Duration adaptiveDelay(String operationName, HedgingConfig.AdaptiveDelayConfig config) {
        if (operationName == null) {
            Duration fallback = config == null ? Duration.ZERO : config.fallbackDelay();
            log.debug(() -> String.format(
                "[HEDGE-ADAPTIVE] reason=ADAPTIVE_FALLBACK_OPERATION_NAME_NULL fallbackDelayMs=%d",
                fallback.toMillis()));
            return fallback;
        }
        if (config == null) {
            log.debug(() -> "[HEDGE-ADAPTIVE] reason=ADAPTIVE_FALLBACK_CONFIG_NULL fallbackDelayMs=0");
            return Duration.ZERO;
        }
        OperationLatencyWindow window = windowsByOperation.get(operationName);
        if (window == null) {
            log.debug(() -> String.format(
                "[HEDGE-ADAPTIVE] reason=ADAPTIVE_FALLBACK_WINDOW_MISSING operationName=%s fallbackDelayMs=%d",
                operationName,
                config.fallbackDelay().toMillis()));
            return config.fallbackDelay();
        }
        long[] snapshot = window.snapshot();
        if (snapshot.length < config.minSamplesRequired()) {
            log.debug(() -> String.format(
                "[HEDGE-ADAPTIVE] reason=ADAPTIVE_FALLBACK_INSUFFICIENT_SAMPLES operationName=%s snapshotLength=%d "
                + "minSamplesRequired=%d percentile=%.3f fallbackDelayMs=%d",
                operationName,
                snapshot.length,
                config.minSamplesRequired(),
                config.percentile(),
                config.fallbackDelay().toMillis()));
            return config.fallbackDelay();
        }
        AdaptiveDelayCalculator.CalculationResult result = AdaptiveDelayCalculator.compute(snapshot, config);
        log.debug(() -> String.format(
            "[HEDGE-ADAPTIVE] reason=ADAPTIVE_COMPUTED operationName=%s snapshotLength=%d minSamplesRequired=%d "
            + "percentile=%.3f computedDelayMs=%d clampedDelayMs=%d",
            operationName,
            snapshot.length,
            config.minSamplesRequired(),
            config.percentile(),
            result.computedDelay().toMillis(),
            result.clampedDelay().toMillis()));
        return result.clampedDelay();
    }
}

