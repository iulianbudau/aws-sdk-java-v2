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
import software.amazon.awssdk.utils.Logger;

@SdkInternalApi
public final class AdaptiveDelayCalculator {
    private static final Logger log = Logger.loggerFor(AdaptiveDelayCalculator.class);

    private AdaptiveDelayCalculator() {
    }

    public static Duration computeDuration(long[] latenciesNanos, HedgingConfig.AdaptiveDelayConfig config) {
        return compute(latenciesNanos, config).clampedDelay();
    }

    public static CalculationResult compute(long[] latenciesNanos, HedgingConfig.AdaptiveDelayConfig config) {
        if (latenciesNanos.length < config.minSamplesRequired()) {
            log.debug(() -> String.format(
                "[HEDGE-ADAPTIVE] reason=ADAPTIVE_FALLBACK_INSUFFICIENT_SAMPLES_CALCULATOR percentile=%.3f "
                + "snapshotLength=%d minSamplesRequired=%d fallbackDelay=%dms",
                config.percentile(),
                latenciesNanos.length,
                config.minSamplesRequired(),
                config.fallbackDelay().toMillis()));
            return CalculationResult.fallback(config.fallbackDelay());
        }

        long[] sorted = Arrays.copyOf(latenciesNanos, latenciesNanos.length);
        Arrays.sort(sorted);
        int rank = (int) Math.ceil((config.percentile() / 100d) * sorted.length);
        int index = Math.max(0, Math.min(sorted.length - 1, rank - 1));
        long selectedLatencyNanos = sorted[index];
        Duration computed = Duration.ofNanos(selectedLatencyNanos);
        Duration clamped = clamp(computed, config.minDelay(), config.maxDelay());
        log.debug(() -> String.format(
            "[HEDGE-ADAPTIVE] reason=ADAPTIVE_CALCULATOR_SELECTION percentile=%.3f sortedLength=%d rank=%d index=%d "
            + "selectedLatencyNanos=%d selectedLatencyMs=%d computedDelayMs=%d minDelayMs=%s maxDelayMs=%s clampedDelayMs=%d",
            config.percentile(),
            sorted.length,
            rank,
            index,
            selectedLatencyNanos,
            Duration.ofNanos(selectedLatencyNanos).toMillis(),
            computed.toMillis(),
            config.minDelay() == null ? "null" : Long.toString(config.minDelay().toMillis()),
            config.maxDelay() == null ? "null" : Long.toString(config.maxDelay().toMillis()),
            clamped.toMillis()));
        return CalculationResult.computed(computed, clamped, rank, index, sorted.length, selectedLatencyNanos);
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

    public static final class CalculationResult {
        private final Duration computedDelay;
        private final Duration clampedDelay;
        private final int rank;
        private final int index;
        private final int sortedLength;
        private final long selectedLatencyNanos;
        private final boolean fallbackUsed;

        private CalculationResult(Duration computedDelay,
                                  Duration clampedDelay,
                                  int rank,
                                  int index,
                                  int sortedLength,
                                  long selectedLatencyNanos,
                                  boolean fallbackUsed) {
            this.computedDelay = computedDelay;
            this.clampedDelay = clampedDelay;
            this.rank = rank;
            this.index = index;
            this.sortedLength = sortedLength;
            this.selectedLatencyNanos = selectedLatencyNanos;
            this.fallbackUsed = fallbackUsed;
        }

        public static CalculationResult fallback(Duration fallbackDelay) {
            return new CalculationResult(fallbackDelay, fallbackDelay, -1, -1, 0, -1, true);
        }

        public static CalculationResult computed(Duration computedDelay,
                                                Duration clampedDelay,
                                                int rank,
                                                int index,
                                                int sortedLength,
                                                long selectedLatencyNanos) {
            return new CalculationResult(computedDelay, clampedDelay, rank, index, sortedLength, selectedLatencyNanos, false);
        }

        public Duration computedDelay() {
            return computedDelay;
        }

        public Duration clampedDelay() {
            return clampedDelay;
        }

        public int rank() {
            return rank;
        }

        public int index() {
            return index;
        }

        public int sortedLength() {
            return sortedLength;
        }

        public long selectedLatencyNanos() {
            return selectedLatencyNanos;
        }

        public boolean fallbackUsed() {
            return fallbackUsed;
        }
    }
}

