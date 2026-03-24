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

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import software.amazon.awssdk.annotations.Immutable;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.utils.OptionalUtils;
import software.amazon.awssdk.utils.ToString;
import software.amazon.awssdk.utils.Validate;
import software.amazon.awssdk.utils.builder.CopyableBuilder;
import software.amazon.awssdk.utils.builder.ToCopyableBuilder;

/**
 * Configuration for request hedging. When hedging is enabled, the SDK starts multiple attempts at fixed delays
 * (e.g. 0 ms, 7 ms, 14 ms) without waiting for the previous attempt to finish. The first successful response wins;
 * other in-flight attempts are cancelled. This can improve tail latency for idempotent operations.
 * <p>
 * Configuration is resolved in order: request-level override ΓåÆ client override configuration ΓåÆ service default ΓåÆ
 * SDK default (hedging disabled).
 *
 * <p>Hedging is intended for <b>idempotent</b> operations only. Use {@link #hedgeableOperations()} to restrict
 * which operations may be hedged (e.g. GetItem, BatchGetItem, Query). The request body must be <b>replayable</b>
 * for each hedged attempt; when in doubt, limit hedging to read operations with no or small body.
 *
 * @see ClientOverrideConfiguration.Builder#hedgingConfig(HedgingConfig)
 */
@Immutable
@SdkPublicApi
public final class HedgingConfig implements ToCopyableBuilder<HedgingConfig.Builder, HedgingConfig> {

    private final boolean enabled;
    private final Duration defaultDelay;
    private final int maxHedgedAttempts;
    private final Map<String, Duration> delayPerOperation;
    private final Set<String> hedgeableOperations;

    private HedgingConfig(BuilderImpl builder) {
        this.enabled = builder.enabled;
        this.defaultDelay = builder.defaultDelay;
        this.maxHedgedAttempts = Validate.isPositive(builder.maxHedgedAttempts, "maxHedgedAttempts");
        this.delayPerOperation = builder.delayPerOperation == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(builder.delayPerOperation);
        this.hedgeableOperations = builder.hedgeableOperations == null
                ? Collections.emptySet()
                : Collections.unmodifiableSet(builder.hedgeableOperations);
    }

    /**
     * Create a builder for {@link HedgingConfig}.
     */
    public static Builder builder() {
        return new BuilderImpl();
    }

    /**
     * Resolve the effective hedging config from request-level, client-level, and service default.
     * Order: request first, then client override, then service default. When absent at all levels,
     * returns {@link #disabled()}.
     *
     * @param requestLevel optional config from request override (e.g. context.requestConfig().hedgingConfig())
     * @param clientLevel  optional config from client override (e.g. clientConfiguration.option(HEDGING_CONFIG))
     * @param serviceDefault supplier for service default (e.g. from DynamoDB client builder); may return empty
     * @return the resolved HedgingConfig
     */
    public static HedgingConfig resolve(Optional<HedgingConfig> requestLevel,
                                        Optional<HedgingConfig> clientLevel,
                                        Supplier<Optional<HedgingConfig>> serviceDefault) {
        return OptionalUtils.firstPresent(
                requestLevel,
                () -> clientLevel,
                serviceDefault)
                .orElseGet(HedgingConfig::disabled);
    }

    /**
     * Returns a disabled hedging config (default when not configured).
     */
    public static HedgingConfig disabled() {
        return builder().enabled(false).maxHedgedAttempts(1).build();
    }

    /**
     * Whether hedging is enabled. When false, the client uses normal retry behavior.
     */
    public boolean enabled() {
        return enabled;
    }

    /**
     * Default delay before starting each hedged attempt (after the first). Used when no per-operation delay is set.
     */
    public Duration defaultDelay() {
        return defaultDelay;
    }

    /**
     * Maximum number of hedged attempts (including the first). Must be at least 1.
     */
    public int maxHedgedAttempts() {
        return maxHedgedAttempts;
    }

    /**
     * Per-operation delay overrides, keyed by operation name (e.g. "GetItem", "PutItem"). Missing keys use
     * {@link #defaultDelay()}.
     */
    public Map<String, Duration> delayPerOperation() {
        return delayPerOperation;
    }

    /**
     * Optional set of operation names that are allowed to be hedged (idempotent operations). When non-empty,
     * hedging only runs when the request's operation name is in this set. When empty, hedging is not restricted
     * by operation (caller must ensure only idempotent operations are hedged).
     */
    public Set<String> hedgeableOperations() {
        return hedgeableOperations;
    }

    /**
     * Resolve the delay before starting hedged attempt at the given index (1-based). Attempt 1 has delay 0.
     * Subsequent attempts are staggered: attempt 2 at baseDelay, attempt 3 at 2*baseDelay, etc.
     * Base delay comes from {@link #delayPerOperation()} for the given operation if present,
     * otherwise {@link #defaultDelay()}.
     *
     * @param attemptIndex 1-based attempt index (1 = first attempt, 2 = first hedge, etc.)
     * @param operationName operation name from execution context, or null
     * @return delay before this attempt (zero for attempt 1; (attemptIndex - 1) * baseDelay for attempt 2+)
     */
    public Duration delayBeforeAttempt(int attemptIndex, String operationName) {
        if (attemptIndex <= 1) {
            return Duration.ZERO;
        }
        Duration baseDelay;
        if (operationName != null && delayPerOperation.containsKey(operationName)) {
            baseDelay = delayPerOperation.get(operationName);
        } else {
            baseDelay = defaultDelay != null ? defaultDelay : Duration.ZERO;
        }
        long baseMs = baseDelay.toMillis();
        long delayMs = baseMs * (attemptIndex - 1);
        return Duration.ofMillis(delayMs);
    }

    /**
     * Returns true if hedging is enabled and the operation is allowed (when hedgeableOperations is set).
     */
    public boolean shouldHedge(String operationName) {
        if (!enabled) {
            return false;
        }
        if (hedgeableOperations.isEmpty()) {
            return true;
        }
        return operationName != null && hedgeableOperations.contains(operationName);
    }

    @Override
    public Builder toBuilder() {
        return new BuilderImpl(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HedgingConfig that = (HedgingConfig) o;
        return enabled == that.enabled
                && maxHedgedAttempts == that.maxHedgedAttempts
                && Objects.equals(defaultDelay, that.defaultDelay)
                && Objects.equals(delayPerOperation, that.delayPerOperation)
                && Objects.equals(hedgeableOperations, that.hedgeableOperations);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(enabled)
                + Objects.hashCode(defaultDelay)
                + Objects.hashCode(maxHedgedAttempts)
                + Objects.hashCode(delayPerOperation)
                + Objects.hashCode(hedgeableOperations);
    }

    @Override
    public String toString() {
        return ToString.builder("HedgingConfig")
                .add("enabled", enabled)
                .add("defaultDelay", defaultDelay)
                .add("maxHedgedAttempts", maxHedgedAttempts)
                .add("delayPerOperation", delayPerOperation)
                .add("hedgeableOperations", hedgeableOperations)
                .build();
    }

    public interface Builder extends CopyableBuilder<Builder, HedgingConfig> {
        Builder enabled(boolean enabled);

        Builder defaultDelay(Duration defaultDelay);

        Builder maxHedgedAttempts(int maxHedgedAttempts);

        Builder delayPerOperation(Map<String, Duration> delayPerOperation);

        Builder hedgeableOperations(Set<String> hedgeableOperations);

        HedgingConfig build();
    }

    private static final class BuilderImpl implements Builder {
        private boolean enabled;
        private Duration defaultDelay;
        private int maxHedgedAttempts = 3;
        private Map<String, Duration> delayPerOperation;
        private Set<String> hedgeableOperations;

        BuilderImpl() {
        }

        BuilderImpl(HedgingConfig config) {
            this.enabled = config.enabled;
            this.defaultDelay = config.defaultDelay;
            this.maxHedgedAttempts = config.maxHedgedAttempts;
            this.delayPerOperation = config.delayPerOperation.isEmpty() ? null : new HashMap<>(config.delayPerOperation);
            this.hedgeableOperations = config.hedgeableOperations.isEmpty() ? null : new HashSet<>(config.hedgeableOperations);
        }

        @Override
        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        @Override
        public Builder defaultDelay(Duration defaultDelay) {
            this.defaultDelay = defaultDelay;
            return this;
        }

        @Override
        public Builder maxHedgedAttempts(int maxHedgedAttempts) {
            this.maxHedgedAttempts = maxHedgedAttempts;
            return this;
        }

        @Override
        public Builder delayPerOperation(Map<String, Duration> delayPerOperation) {
            this.delayPerOperation = delayPerOperation == null ? null : delayPerOperation;
            return this;
        }

        @Override
        public Builder hedgeableOperations(Set<String> hedgeableOperations) {
            this.hedgeableOperations = hedgeableOperations;
            return this;
        }

        @Override
        public HedgingConfig build() {
            return new HedgingConfig(this);
        }
    }
}