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

@SdkInternalApi
public final class HedgingLatencyTracker {
    private final Map<String, OperationLatencyWindow> windowsByOperation = new ConcurrentHashMap<>();

    public void record(String operationName, Duration latency, HedgingConfig.AdaptiveDelayConfig config) {
        if (operationName == null || latency == null || config == null) {
            return;
        }
        windowsByOperation.computeIfAbsent(operationName, k -> new OperationLatencyWindow(config.sampleSize()))
                          .record(latency.toNanos());
    }

    public Duration adaptiveDelay(String operationName, HedgingConfig.AdaptiveDelayConfig config) {
        if (operationName == null || config == null) {
            return config == null ? Duration.ZERO : config.fallbackDelay();
        }
        OperationLatencyWindow window = windowsByOperation.get(operationName);
        if (window == null) {
            return config.fallbackDelay();
        }
        long[] snapshot = window.snapshot();
        return AdaptiveDelayCalculator.computeDuration(snapshot, config);
    }
}

