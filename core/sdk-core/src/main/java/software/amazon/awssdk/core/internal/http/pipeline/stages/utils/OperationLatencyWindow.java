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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.utils.Validate;

/**
 * Lock-light rolling window for operation latencies.
 */
@SdkInternalApi
public final class OperationLatencyWindow {
    private final long[] latenciesNanos;
    private final AtomicLong writeIndex = new AtomicLong(0);
    private final AtomicInteger size = new AtomicInteger(0);

    public OperationLatencyWindow(int sampleSize) {
        Validate.isTrue(sampleSize > 0, "sampleSize must be > 0");
        this.latenciesNanos = new long[sampleSize];
    }

    public void record(long latencyNanos) {
        if (latencyNanos <= 0) {
            return;
        }
        long index = writeIndex.getAndIncrement();
        int slot = (int) (index % latenciesNanos.length);
        latenciesNanos[slot] = latencyNanos;
        size.updateAndGet(s -> s < latenciesNanos.length ? s + 1 : s);
    }

    public long[] snapshot() {
        int currentSize = size.get();
        if (currentSize == 0) {
            return new long[0];
        }
        return Arrays.copyOf(latenciesNanos, currentSize);
    }
}

