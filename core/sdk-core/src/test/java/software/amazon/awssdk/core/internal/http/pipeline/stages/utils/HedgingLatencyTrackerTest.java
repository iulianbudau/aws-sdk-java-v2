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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.client.config.HedgingConfig;

class HedgingLatencyTrackerTest {
    @Test
    void adaptiveDelay_returnsFallbackWhenNoSamples() {
        HedgingLatencyTracker tracker = new HedgingLatencyTracker();
        HedgingConfig.AdaptiveDelayConfig cfg = HedgingConfig.AdaptiveDelayConfig.builder()
                                                                                  .fallbackDelay(Duration.ofMillis(7))
                                                                                  .build();
        assertThat(tracker.adaptiveDelay("GetItem", cfg)).isEqualTo(Duration.ofMillis(7));
    }

    @Test
    void adaptiveDelay_usesPercentileWithSamples() {
        HedgingLatencyTracker tracker = new HedgingLatencyTracker();
        HedgingConfig.AdaptiveDelayConfig cfg = HedgingConfig.AdaptiveDelayConfig.builder()
                                                                                  .percentile(95)
                                                                                  .sampleSize(100)
                                                                                  .minSamplesRequired(3)
                                                                                  .fallbackDelay(Duration.ofMillis(5))
                                                                                  .build();
        tracker.record("GetItem", Duration.ofMillis(5), cfg);
        tracker.record("GetItem", Duration.ofMillis(10), cfg);
        tracker.record("GetItem", Duration.ofMillis(20), cfg);

        assertThat(tracker.adaptiveDelay("GetItem", cfg)).isEqualTo(Duration.ofMillis(20));
    }

    @Test
    void adaptiveDelay_handlesConcurrentWrites() throws Exception {
        HedgingLatencyTracker tracker = new HedgingLatencyTracker();
        HedgingConfig.AdaptiveDelayConfig cfg = HedgingConfig.AdaptiveDelayConfig.builder()
                                                                                  .sampleSize(1000)
                                                                                  .minSamplesRequired(100)
                                                                                  .build();

        ExecutorService executor = Executors.newFixedThreadPool(8);
        for (int i = 0; i < 2_000; i++) {
            int delay = i % 50 + 1;
            executor.submit(() -> tracker.record("Query", Duration.ofMillis(delay), cfg));
        }
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        Duration delay = tracker.adaptiveDelay("Query", cfg);
        assertThat(delay).isNotNull();
        assertThat(delay.toMillis()).isGreaterThan(0);
    }
}

