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
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.core.client.config.HedgingConfig;
import software.amazon.awssdk.utils.Logger;

@SdkInternalApi
public final class HedgingDelayResolver {
    private static final Logger log = Logger.loggerFor(HedgingDelayResolver.class);

    private HedgingDelayResolver() {
    }

    public static Duration resolveBaseDelay(HedgingConfig.OperationHedgingPolicy policy,
                                            String operationName,
                                            HedgingLatencyTracker tracker) {
        HedgingConfig.DelayConfig delayConfig = policy.delayConfig();
        String delayConfigType = delayConfig == null ? "null" : delayConfig.getClass().getSimpleName();
        log.debug(() -> String.format(
            "[HEDGE-ADAPTIVE] reason=ADAPTIVE_RESOLVE_BASE_DELAY operationName=%s delayConfigType=%s",
            operationName,
            delayConfigType));
        if (delayConfig instanceof HedgingConfig.AdaptiveDelayConfig) {
            HedgingConfig.AdaptiveDelayConfig adaptiveConfig = (HedgingConfig.AdaptiveDelayConfig) delayConfig;
            if (tracker == null) {
                log.debug(() -> String.format(
                    "[HEDGE-ADAPTIVE] reason=ADAPTIVE_FALLBACK_TRACKER_NULL operationName=%s fallbackDelayMs=%d",
                    operationName,
                    adaptiveConfig.fallbackDelay().toMillis()));
                return adaptiveConfig.fallbackDelay();
            }
            Duration resolved = tracker.adaptiveDelay(operationName, adaptiveConfig);
            log.debug(() -> String.format(
                "[HEDGE-ADAPTIVE] reason=ADAPTIVE_BASE_DELAY_RESOLVED operationName=%s trackerNull=false resolvedBaseDelayMs=%d",
                operationName,
                resolved.toMillis()));
            return resolved;
        }
        if (delayConfig instanceof HedgingConfig.FixedDelayConfig) {
            return ((HedgingConfig.FixedDelayConfig) delayConfig).baseDelay();
        }
        return Duration.ZERO;
    }

    public static Duration resolveDelayBeforeAttempt(int attemptIndex,
                                                     HedgingConfig.OperationHedgingPolicy policy,
                                                     String operationName,
                                                     HedgingLatencyTracker tracker) {
        if (attemptIndex <= 1) {
            return Duration.ZERO;
        }
        Duration baseDelay = resolveBaseDelay(policy, operationName, tracker);
        return Duration.ofMillis(baseDelay.toMillis() * (attemptIndex - 1));
    }
}

