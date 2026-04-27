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
import java.util.Arrays;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.core.client.config.HedgingConfig;

@SdkInternalApi
public final class AdaptiveDelayCalculator {
    private AdaptiveDelayCalculator() {
    }

    public static Duration computeDuration(long[] latenciesNanos, HedgingConfig.AdaptiveDelayConfig config) {
        if (latenciesNanos.length < config.minSamplesRequired()) {
            return config.fallbackDelay();
        }

        long[] sorted = Arrays.copyOf(latenciesNanos, latenciesNanos.length);
        Arrays.sort(sorted);
        int rank = (int) Math.ceil((config.percentile() / 100d) * sorted.length);
        int index = Math.max(0, Math.min(sorted.length - 1, rank - 1));
        Duration computed = Duration.ofNanos(sorted[index]);
        return clamp(computed, config.minDelay(), config.maxDelay());
    }

    private static Duration clamp(Duration value, Duration min, Duration max) {
        Duration clamped = value;
        if (min != null && clamped.compareTo(min) < 0) {
            clamped = min;
        }
        if (max != null && clamped.compareTo(max) > 0) {
            clamped = max;
        }
        return clamped;
    }
}

