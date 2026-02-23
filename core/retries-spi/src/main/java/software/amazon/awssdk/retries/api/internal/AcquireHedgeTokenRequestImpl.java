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

package software.amazon.awssdk.retries.api.internal;

import java.time.Duration;
import java.util.Optional;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.retries.api.AcquireHedgeTokenRequest;
import software.amazon.awssdk.retries.api.RetryToken;
import software.amazon.awssdk.utils.Validate;

/**
 * Implementation of {@link AcquireHedgeTokenRequest}.
 */
@SdkInternalApi
public final class AcquireHedgeTokenRequestImpl implements AcquireHedgeTokenRequest {
    private final RetryToken token;
    private final int attemptIndex;
    private final String operationName;
    private final Duration delayUntilThisAttempt;

    private AcquireHedgeTokenRequestImpl(Builder builder) {
        this.token = Validate.paramNotNull(builder.token, "token");
        this.attemptIndex = Validate.isPositive(builder.attemptIndex, "attemptIndex");
        this.operationName = builder.operationName;
        this.delayUntilThisAttempt = Validate.paramNotNull(builder.delayUntilThisAttempt, "delayUntilThisAttempt");
    }

    @Override
    public RetryToken token() {
        return token;
    }

    @Override
    public int attemptIndex() {
        return attemptIndex;
    }

    @Override
    public Optional<String> operationName() {
        return Optional.ofNullable(operationName);
    }

    @Override
    public Duration delayUntilThisAttempt() {
        return delayUntilThisAttempt;
    }

    @Override
    public Builder toBuilder() {
        return new Builder(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder implements AcquireHedgeTokenRequest.Builder {
        private RetryToken token;
        private int attemptIndex;
        private String operationName;
        private Duration delayUntilThisAttempt = Duration.ZERO;

        Builder() {
        }

        Builder(AcquireHedgeTokenRequestImpl request) {
            this.token = request.token;
            this.attemptIndex = request.attemptIndex;
            this.operationName = request.operationName;
            this.delayUntilThisAttempt = request.delayUntilThisAttempt;
        }

        @Override
        public Builder token(RetryToken token) {
            this.token = token;
            return this;
        }

        @Override
        public Builder attemptIndex(int attemptIndex) {
            this.attemptIndex = attemptIndex;
            return this;
        }

        @Override
        public Builder operationName(String operationName) {
            this.operationName = operationName;
            return this;
        }

        @Override
        public Builder delayUntilThisAttempt(Duration delayUntilThisAttempt) {
            this.delayUntilThisAttempt = delayUntilThisAttempt;
            return this;
        }

        @Override
        public AcquireHedgeTokenRequest build() {
            return new AcquireHedgeTokenRequestImpl(this);
        }
    }
}
