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
 * Configuration for request hedging. When hedging is enabled, the SDK starts multiple attempts at configured delays
 * without waiting for the previous attempt to finish. The first successful response wins; other in-flight attempts are
 * cancelled. This can improve tail latency for idempotent operations.
 * Delay mode can be configured as fixed or adaptive. Adaptive mode uses rolling historical latency per operation.
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

    private static final int DEFAULT_ADAPTIVE_SAMPLE_SIZE = 1_000;
    private static final double DEFAULT_ADAPTIVE_PERCENTILE = 99.0d;

    private final boolean enabled;
    private final Set<String> hedgeableOperations;
    private final OperationHedgingPolicy defaultPolicy;
    private final Map<String, OperationHedgingPolicy> policyPerOperation;

    private HedgingConfig(BuilderImpl builder) {
        this.enabled = builder.enabled;
        this.hedgeableOperations = builder.hedgeableOperations == null
                ? Collections.emptySet()
                : Collections.unmodifiableSet(builder.hedgeableOperations);
        this.defaultPolicy = builder.defaultPolicy == null ? OperationHedgingPolicy.defaultPolicy() : builder.defaultPolicy;
        this.policyPerOperation = builder.policyPerOperation == null
                                  ? Collections.emptyMap()
                                  : Collections.unmodifiableMap(builder.policyPerOperation);
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
        return builder().enabled(false).build();
    }

    /**
     * Whether hedging is enabled. When false, the client uses normal retry behavior.
     */
    public boolean enabled() {
        return enabled;
    }

    /**
     * Optional set of operation names that are allowed to be hedged (idempotent operations). When non-empty,
     * hedging only runs when the request's operation name is in this set. When empty, hedging is not restricted
     * by operation (caller must ensure only idempotent operations are hedged).
     */
    public Set<String> hedgeableOperations() {
        return hedgeableOperations;
    }

    public OperationHedgingPolicy defaultPolicy() {
        return defaultPolicy;
    }

    public Map<String, OperationHedgingPolicy> policyPerOperation() {
        return policyPerOperation;
    }

    public OperationHedgingPolicy policyForOperation(String operationName) {
        if (operationName != null && policyPerOperation.containsKey(operationName)) {
            return policyPerOperation.get(operationName);
        }
        return defaultPolicy;
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
                && Objects.equals(hedgeableOperations, that.hedgeableOperations)
                && Objects.equals(defaultPolicy, that.defaultPolicy)
                && Objects.equals(policyPerOperation, that.policyPerOperation);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(enabled)
                + Objects.hashCode(hedgeableOperations)
                + Objects.hashCode(defaultPolicy)
                + Objects.hashCode(policyPerOperation);
    }

    @Override
    public String toString() {
        return ToString.builder("HedgingConfig")
                .add("enabled", enabled)
                .add("hedgeableOperations", hedgeableOperations)
                .add("defaultPolicy", defaultPolicy)
                .add("policyPerOperation", policyPerOperation)
                .build();
    }

    public interface Builder extends CopyableBuilder<Builder, HedgingConfig> {
        Builder enabled(boolean enabled);

        Builder hedgeableOperations(Set<String> hedgeableOperations);

        Builder defaultPolicy(OperationHedgingPolicy defaultPolicy);

        Builder policyPerOperation(Map<String, OperationHedgingPolicy> policyPerOperation);

        HedgingConfig build();
    }

    private static final class BuilderImpl implements Builder {
        private boolean enabled;
        private Set<String> hedgeableOperations;
        private OperationHedgingPolicy defaultPolicy = OperationHedgingPolicy.defaultPolicy();
        private Map<String, OperationHedgingPolicy> policyPerOperation;

        BuilderImpl() {
        }

        BuilderImpl(HedgingConfig config) {
            this.enabled = config.enabled;
            this.hedgeableOperations = config.hedgeableOperations.isEmpty() ? null : new HashSet<>(config.hedgeableOperations);
            this.defaultPolicy = config.defaultPolicy;
            this.policyPerOperation = config.policyPerOperation.isEmpty() ? null : new HashMap<>(config.policyPerOperation);
        }

        @Override
        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        @Override
        public Builder hedgeableOperations(Set<String> hedgeableOperations) {
            this.hedgeableOperations = hedgeableOperations;
            return this;
        }

        @Override
        public Builder defaultPolicy(OperationHedgingPolicy defaultPolicy) {
            this.defaultPolicy = defaultPolicy == null ? OperationHedgingPolicy.defaultPolicy() : defaultPolicy;
            return this;
        }

        @Override
        public Builder policyPerOperation(Map<String, OperationHedgingPolicy> policyPerOperation) {
            this.policyPerOperation = policyPerOperation == null ? null : policyPerOperation;
            return this;
        }

        @Override
        public HedgingConfig build() {
            return new HedgingConfig(this);
        }
    }

    public interface DelayConfig {
    }

    @Immutable
    public static final class FixedDelayConfig implements DelayConfig,
                                                          ToCopyableBuilder<FixedDelayConfig.Builder, FixedDelayConfig> {
        private final Duration baseDelay;

        private FixedDelayConfig(BuilderImpl builder) {
            this.baseDelay = builder.baseDelay == null ? Duration.ZERO : builder.baseDelay;
            Validate.isTrue(!baseDelay.isNegative(), "baseDelay must be >= 0");
        }

        public static Builder builder() {
            return new BuilderImpl();
        }

        public Duration baseDelay() {
            return baseDelay;
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
            FixedDelayConfig that = (FixedDelayConfig) o;
            return Objects.equals(baseDelay, that.baseDelay);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(baseDelay);
        }

        @Override
        public String toString() {
            return ToString.builder("FixedDelayConfig")
                           .add("baseDelay", baseDelay)
                           .build();
        }

        public interface Builder extends CopyableBuilder<Builder, FixedDelayConfig> {
            Builder baseDelay(Duration baseDelay);
        }

        private static final class BuilderImpl implements Builder {
            private Duration baseDelay;

            private BuilderImpl() {
            }

            private BuilderImpl(FixedDelayConfig config) {
                this.baseDelay = config.baseDelay;
            }

            @Override
            public Builder baseDelay(Duration baseDelay) {
                this.baseDelay = baseDelay;
                return this;
            }

            @Override
            public FixedDelayConfig build() {
                return new FixedDelayConfig(this);
            }
        }
    }

    @Immutable
    public static final class OperationHedgingPolicy
        implements ToCopyableBuilder<OperationHedgingPolicy.Builder, OperationHedgingPolicy> {
        private final int maxHedgedAttempts;
        private final DelayConfig delayConfig;

        private OperationHedgingPolicy(BuilderImpl builder) {
            this.maxHedgedAttempts = Validate.isPositive(builder.maxHedgedAttempts, "maxHedgedAttempts");
            this.delayConfig = Validate.paramNotNull(builder.delayConfig, "delayConfig");
        }

        public static Builder builder() {
            return new BuilderImpl();
        }

        public static OperationHedgingPolicy defaultPolicy() {
            return builder()
                .maxHedgedAttempts(3)
                .delayConfig(FixedDelayConfig.builder().baseDelay(Duration.ZERO).build())
                .build();
        }

        public int maxHedgedAttempts() {
            return maxHedgedAttempts;
        }

        public DelayConfig delayConfig() {
            return delayConfig;
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
            OperationHedgingPolicy that = (OperationHedgingPolicy) o;
            return maxHedgedAttempts == that.maxHedgedAttempts && Objects.equals(delayConfig, that.delayConfig);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(maxHedgedAttempts) + Objects.hashCode(delayConfig);
        }

        @Override
        public String toString() {
            return ToString.builder("OperationHedgingPolicy")
                           .add("maxHedgedAttempts", maxHedgedAttempts)
                           .add("delayConfig", delayConfig)
                           .build();
        }

        public interface Builder extends CopyableBuilder<Builder, OperationHedgingPolicy> {
            Builder maxHedgedAttempts(int maxHedgedAttempts);

            Builder delayConfig(DelayConfig delayConfig);
        }

        private static final class BuilderImpl implements Builder {
            private int maxHedgedAttempts = 3;
            private DelayConfig delayConfig = FixedDelayConfig.builder().baseDelay(Duration.ZERO).build();

            private BuilderImpl() {
            }

            private BuilderImpl(OperationHedgingPolicy policy) {
                this.maxHedgedAttempts = policy.maxHedgedAttempts;
                this.delayConfig = policy.delayConfig;
            }

            @Override
            public Builder maxHedgedAttempts(int maxHedgedAttempts) {
                this.maxHedgedAttempts = maxHedgedAttempts;
                return this;
            }

            @Override
            public Builder delayConfig(DelayConfig delayConfig) {
                this.delayConfig = delayConfig;
                return this;
            }

            @Override
            public OperationHedgingPolicy build() {
                return new OperationHedgingPolicy(this);
            }
        }
    }

    @Immutable
    public static final class AdaptiveDelayConfig
        implements DelayConfig, ToCopyableBuilder<AdaptiveDelayConfig.Builder, AdaptiveDelayConfig> {
        private final double percentile;
        private final int sampleSize;
        private final int minSamplesRequired;
        private final Duration fallbackDelay;
        private final Duration minDelay;
        private final Duration maxDelay;

        private AdaptiveDelayConfig(BuilderImpl builder) {
            this.percentile = builder.percentile == null ? DEFAULT_ADAPTIVE_PERCENTILE : builder.percentile;
            this.sampleSize = builder.sampleSize == null ? DEFAULT_ADAPTIVE_SAMPLE_SIZE : builder.sampleSize;
            this.minSamplesRequired = builder.minSamplesRequired == null
                                      ? Math.min(20, sampleSize)
                                      : builder.minSamplesRequired;
            this.fallbackDelay = builder.fallbackDelay == null ? Duration.ZERO : builder.fallbackDelay;
            this.minDelay = builder.minDelay;
            this.maxDelay = builder.maxDelay;

            Validate.isTrue(percentile > 0 && percentile <= 100,
                            "percentile must be > 0 and <= 100");
            Validate.isTrue(sampleSize > 0, "sampleSize must be > 0");
            Validate.isTrue(minSamplesRequired >= 1 && minSamplesRequired <= sampleSize,
                            "minSamplesRequired must be >= 1 and <= sampleSize");
            Validate.isTrue(!fallbackDelay.isNegative(), "fallbackDelay must be >= 0");
            if (minDelay != null) {
                Validate.isTrue(!minDelay.isNegative(), "minDelay must be >= 0");
            }
            if (maxDelay != null) {
                Validate.isTrue(!maxDelay.isNegative(), "maxDelay must be >= 0");
            }
            if (minDelay != null && maxDelay != null) {
                Validate.isTrue(!maxDelay.minus(minDelay).isNegative(), "minDelay must be <= maxDelay");
            }
        }

        public static Builder builder() {
            return new BuilderImpl();
        }

        public double percentile() {
            return percentile;
        }

        public int sampleSize() {
            return sampleSize;
        }

        public int minSamplesRequired() {
            return minSamplesRequired;
        }

        public Duration fallbackDelay() {
            return fallbackDelay;
        }

        public Duration minDelay() {
            return minDelay;
        }

        public Duration maxDelay() {
            return maxDelay;
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
            AdaptiveDelayConfig that = (AdaptiveDelayConfig) o;
            return Double.compare(that.percentile, percentile) == 0 &&
                   sampleSize == that.sampleSize &&
                   minSamplesRequired == that.minSamplesRequired &&
                   Objects.equals(fallbackDelay, that.fallbackDelay) &&
                   Objects.equals(minDelay, that.minDelay) &&
                   Objects.equals(maxDelay, that.maxDelay);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(percentile)
                   + Objects.hashCode(sampleSize)
                   + Objects.hashCode(minSamplesRequired)
                   + Objects.hashCode(fallbackDelay)
                   + Objects.hashCode(minDelay)
                   + Objects.hashCode(maxDelay);
        }

        @Override
        public String toString() {
            return ToString.builder("AdaptiveDelayConfig")
                           .add("percentile", percentile)
                           .add("sampleSize", sampleSize)
                           .add("minSamplesRequired", minSamplesRequired)
                           .add("fallbackDelay", fallbackDelay)
                           .add("minDelay", minDelay)
                           .add("maxDelay", maxDelay)
                           .build();
        }

        public interface Builder extends CopyableBuilder<Builder, AdaptiveDelayConfig> {
            Builder percentile(double percentile);

            Builder sampleSize(int sampleSize);

            Builder minSamplesRequired(int minSamplesRequired);

            Builder fallbackDelay(Duration fallbackDelay);

            Builder minDelay(Duration minDelay);

            Builder maxDelay(Duration maxDelay);
        }

        private static final class BuilderImpl implements Builder {
            private Double percentile;
            private Integer sampleSize;
            private Integer minSamplesRequired;
            private Duration fallbackDelay;
            private Duration minDelay;
            private Duration maxDelay;

            private BuilderImpl() {
            }

            private BuilderImpl(AdaptiveDelayConfig config) {
                this.percentile = config.percentile;
                this.sampleSize = config.sampleSize;
                this.minSamplesRequired = config.minSamplesRequired;
                this.fallbackDelay = config.fallbackDelay;
                this.minDelay = config.minDelay;
                this.maxDelay = config.maxDelay;
            }

            @Override
            public Builder percentile(double percentile) {
                this.percentile = percentile;
                return this;
            }

            @Override
            public Builder sampleSize(int sampleSize) {
                this.sampleSize = sampleSize;
                return this;
            }

            @Override
            public Builder minSamplesRequired(int minSamplesRequired) {
                this.minSamplesRequired = minSamplesRequired;
                return this;
            }

            @Override
            public Builder fallbackDelay(Duration fallbackDelay) {
                this.fallbackDelay = fallbackDelay;
                return this;
            }

            @Override
            public Builder minDelay(Duration minDelay) {
                this.minDelay = minDelay;
                return this;
            }

            @Override
            public Builder maxDelay(Duration maxDelay) {
                this.maxDelay = maxDelay;
                return this;
            }

            @Override
            public AdaptiveDelayConfig build() {
                return new AdaptiveDelayConfig(this);
            }
        }
    }
}