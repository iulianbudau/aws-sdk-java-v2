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

package software.amazon.awssdk.core.client.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class HedgingConfigTest {

    @Test
    public void disabled_hasHedgingOff() {
        HedgingConfig config = HedgingConfig.disabled();
        assertThat(config.enabled()).isFalse();
        assertThat(config.defaultPolicy().maxHedgedAttempts()).isEqualTo(3);
    }

    @Test
    public void resolve_requestLevelWins() {
        HedgingConfig requestLevel = HedgingConfig.builder()
                                                  .enabled(true)
                                                  .defaultPolicy(HedgingConfig.OperationHedgingPolicy.builder()
                                                                                                      .maxHedgedAttempts(5)
                                                                                                      .build())
                                                  .build();
        HedgingConfig clientLevel = HedgingConfig.builder()
                                                 .enabled(true)
                                                 .defaultPolicy(HedgingConfig.OperationHedgingPolicy.builder()
                                                                                                     .maxHedgedAttempts(3)
                                                                                                     .build())
                                                 .build();
        HedgingConfig resolved = HedgingConfig.resolve(
            Optional.of(requestLevel),
            Optional.of(clientLevel),
            () -> Optional.of(clientLevel));
        assertThat(resolved.defaultPolicy().maxHedgedAttempts()).isEqualTo(5);
    }

    @Test
    public void resolve_clientLevelWhenNoRequest() {
        HedgingConfig clientLevel = HedgingConfig.builder()
                                                 .enabled(true)
                                                 .defaultPolicy(HedgingConfig.OperationHedgingPolicy.builder()
                                                                                                     .maxHedgedAttempts(3)
                                                                                                     .build())
                                                 .build();
        HedgingConfig resolved = HedgingConfig.resolve(
            Optional.empty(),
            Optional.of(clientLevel),
            () -> Optional.empty());
        assertThat(resolved.defaultPolicy().maxHedgedAttempts()).isEqualTo(3);
    }

    @Test
    public void shouldHedge_disabled_returnsFalse() {
        HedgingConfig config = HedgingConfig.disabled();
        assertThat(config.shouldHedge("GetItem")).isFalse();
    }

    @Test
    public void shouldHedge_emptyAllowList_returnsTrueWhenEnabled() {
        HedgingConfig config = HedgingConfig.builder()
            .enabled(true)
            .hedgeableOperations(Collections.emptySet())
            .build();
        assertThat(config.shouldHedge("GetItem")).isTrue();
        assertThat(config.shouldHedge(null)).isTrue();
    }

    @Test
    public void shouldHedge_withAllowList_onlyAllowedOps() {
        Set<String> allowed = new HashSet<>();
        allowed.add("GetItem");
        allowed.add("Query");
        HedgingConfig config = HedgingConfig.builder()
            .enabled(true)
            .hedgeableOperations(allowed)
            .build();
        assertThat(config.shouldHedge("GetItem")).isTrue();
        assertThat(config.shouldHedge("Query")).isTrue();
        assertThat(config.shouldHedge("PutItem")).isFalse();
        assertThat(config.shouldHedge(null)).isFalse();
    }

    @Test
    public void adaptiveDelayConfig_defaultsApplied() {
        HedgingConfig.AdaptiveDelayConfig cfg = HedgingConfig.AdaptiveDelayConfig.builder().build();
        assertThat(cfg.percentile()).isEqualTo(99.0);
        assertThat(cfg.sampleSize()).isEqualTo(1_000);
        assertThat(cfg.minSamplesRequired()).isEqualTo(20);
    }

    @Test
    public void adaptiveDelayConfig_invalidPercentile_throws() {
        assertThatThrownBy(() -> HedgingConfig.AdaptiveDelayConfig.builder().percentile(0).build())
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void adaptiveDelayConfig_invalidBounds_throws() {
        assertThatThrownBy(() -> HedgingConfig.AdaptiveDelayConfig.builder()
                                                                  .minDelay(Duration.ofMillis(10))
                                                                  .maxDelay(Duration.ofMillis(5))
                                                                  .build())
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void policyPerOperation_resolvesByName() {
        HedgingConfig.OperationHedgingPolicy defaultPolicy = HedgingConfig.OperationHedgingPolicy.builder()
                                                                                                  .maxHedgedAttempts(2)
                                                                                                  .delayConfig(HedgingConfig.FixedDelayConfig.builder()
                                                                                                                                    .baseDelay(Duration.ofMillis(10))
                                                                                                                                    .build())
                                                                                                  .build();
        HedgingConfig.OperationHedgingPolicy getItemPolicy = HedgingConfig.OperationHedgingPolicy.builder()
                                                                                                  .maxHedgedAttempts(5)
                                                                                                  .delayConfig(HedgingConfig.FixedDelayConfig.builder()
                                                                                                                                    .baseDelay(Duration.ofMillis(5))
                                                                                                                                    .build())
                                                                                                  .build();
        HedgingConfig config = HedgingConfig.builder()
                                            .enabled(true)
                                            .defaultPolicy(defaultPolicy)
                                            .policyPerOperation(Collections.singletonMap("GetItem", getItemPolicy))
                                            .build();

        assertThat(config.policyForOperation("GetItem").maxHedgedAttempts()).isEqualTo(5);
        assertThat(((HedgingConfig.FixedDelayConfig) config.policyForOperation("GetItem").delayConfig()).baseDelay())
            .isEqualTo(Duration.ofMillis(5));
        assertThat(config.policyForOperation("Query").maxHedgedAttempts()).isEqualTo(2);
    }
}
