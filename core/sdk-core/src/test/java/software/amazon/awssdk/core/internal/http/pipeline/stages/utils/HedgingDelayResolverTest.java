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
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.client.config.HedgingConfig;

class HedgingDelayResolverTest {
    @Test
    void fixedMode_usesPerOperationThenStagger() {
        HedgingConfig.OperationHedgingPolicy policy =
            HedgingConfig.OperationHedgingPolicy.builder()
                                                .maxHedgedAttempts(3)
                                                .delayConfig(HedgingConfig.FixedDelayConfig.builder()
                                                                                           .baseDelay(Duration.ofMillis(5))
                                                                                           .build())
                                                .build();

        Duration delay = HedgingDelayResolver.resolveDelayBeforeAttempt(3, policy, "GetItem", null);
        assertThat(delay).isEqualTo(Duration.ofMillis(10));
    }

    @Test
    void adaptiveMode_usesFallbackWhenNoSamples() {
        HedgingConfig.AdaptiveDelayConfig adaptiveCfg = HedgingConfig.AdaptiveDelayConfig.builder()
                                                                                          .fallbackDelay(Duration.ofMillis(9))
                                                                                          .build();
        HedgingConfig.OperationHedgingPolicy policy =
            HedgingConfig.OperationHedgingPolicy.builder()
                                                .maxHedgedAttempts(3)
                                                .delayConfig(adaptiveCfg)
                                                .build();

        Duration delay = HedgingDelayResolver.resolveDelayBeforeAttempt(2, policy, "Query", new HedgingLatencyTracker());
        assertThat(delay).isEqualTo(Duration.ofMillis(9));
    }

    @Test
    void adaptiveMode_appliesMinMaxClamp() {
        HedgingConfig.AdaptiveDelayConfig adaptiveCfg = HedgingConfig.AdaptiveDelayConfig.builder()
                                                                                          .percentile(99)
                                                                                          .sampleSize(10)
                                                                                          .minSamplesRequired(1)
                                                                                          .fallbackDelay(Duration.ofMillis(2))
                                                                                          .minDelay(Duration.ofMillis(7))
                                                                                          .maxDelay(Duration.ofMillis(7))
                                                                                          .build();
        HedgingLatencyTracker tracker = new HedgingLatencyTracker();
        tracker.record("GetItem", Duration.ofMillis(30), adaptiveCfg);

        HedgingConfig.OperationHedgingPolicy policy =
            HedgingConfig.OperationHedgingPolicy.builder()
                                                .maxHedgedAttempts(3)
                                                .delayConfig(adaptiveCfg)
                                                .build();

        Duration delay = HedgingDelayResolver.resolveDelayBeforeAttempt(2, policy, "GetItem", tracker);
        assertThat(delay).isEqualTo(Duration.ofMillis(7));
    }
}

