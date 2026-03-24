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
        assertThat(config.maxHedgedAttempts()).isEqualTo(1);
    }

    @Test
    public void resolve_requestLevelWins() {
        HedgingConfig requestLevel = HedgingConfig.builder().enabled(true).maxHedgedAttempts(5).build();
        HedgingConfig clientLevel = HedgingConfig.builder().enabled(true).maxHedgedAttempts(3).build();
        HedgingConfig resolved = HedgingConfig.resolve(
            Optional.of(requestLevel),
            Optional.of(clientLevel),
            () -> Optional.of(clientLevel));
        assertThat(resolved.maxHedgedAttempts()).isEqualTo(5);
    }

    @Test
    public void resolve_clientLevelWhenNoRequest() {
        HedgingConfig clientLevel = HedgingConfig.builder().enabled(true).maxHedgedAttempts(3).build();
        HedgingConfig resolved = HedgingConfig.resolve(
            Optional.empty(),
            Optional.of(clientLevel),
            () -> Optional.empty());
        assertThat(resolved.maxHedgedAttempts()).isEqualTo(3);
    }

    @Test
    public void resolve_disabledWhenAllAbsent() {
        HedgingConfig resolved = HedgingConfig.resolve(
            Optional.empty(),
            Optional.empty(),
            () -> Optional.empty());
        assertThat(resolved.enabled()).isFalse();
    }

    @Test
    public void delayBeforeAttempt_attempt1_isZero() {
        HedgingConfig config = HedgingConfig.builder()
            .enabled(true)
            .defaultDelay(Duration.ofMillis(10))
            .maxHedgedAttempts(3)
            .build();
        assertThat(config.delayBeforeAttempt(1, "GetItem")).isEqualTo(Duration.ZERO);
    }

    @Test
    public void delayBeforeAttempt_usesPerOperationWhenPresent() {
        HedgingConfig config = HedgingConfig.builder()
            .enabled(true)
            .defaultDelay(Duration.ofMillis(10))
            .maxHedgedAttempts(3)
            .delayPerOperation(Collections.singletonMap("GetItem", Duration.ofMillis(5)))
            .build();
        // Staggered: attempt 2 = 1*base, attempt 3 = 2*base
        assertThat(config.delayBeforeAttempt(2, "GetItem")).isEqualTo(Duration.ofMillis(5));
        assertThat(config.delayBeforeAttempt(3, "GetItem")).isEqualTo(Duration.ofMillis(10));
        assertThat(config.delayBeforeAttempt(2, "OtherOp")).isEqualTo(Duration.ofMillis(10));
        assertThat(config.delayBeforeAttempt(3, "OtherOp")).isEqualTo(Duration.ofMillis(20));
    }

    @Test
    public void delayBeforeAttempt_staggeredByAttemptIndex() {
        HedgingConfig config = HedgingConfig.builder()
            .enabled(true)
            .defaultDelay(Duration.ofMillis(7))
            .maxHedgedAttempts(4)
            .build();
        assertThat(config.delayBeforeAttempt(1, "GetItem")).isEqualTo(Duration.ZERO);
        assertThat(config.delayBeforeAttempt(2, "GetItem")).isEqualTo(Duration.ofMillis(7));
        assertThat(config.delayBeforeAttempt(3, "GetItem")).isEqualTo(Duration.ofMillis(14));
        assertThat(config.delayBeforeAttempt(4, "GetItem")).isEqualTo(Duration.ofMillis(21));
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
            .maxHedgedAttempts(3)
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
            .maxHedgedAttempts(3)
            .hedgeableOperations(allowed)
            .build();
        assertThat(config.shouldHedge("GetItem")).isTrue();
        assertThat(config.shouldHedge("Query")).isTrue();
        assertThat(config.shouldHedge("PutItem")).isFalse();
        assertThat(config.shouldHedge(null)).isFalse();
    }
}
