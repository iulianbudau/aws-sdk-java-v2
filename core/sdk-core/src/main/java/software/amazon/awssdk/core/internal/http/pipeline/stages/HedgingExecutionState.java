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

package software.amazon.awssdk.core.internal.http.pipeline.stages;

import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * Execution state for hedging. State transitions are atomic via CAS to ensure only one winner.
 * Shared by sync and async hedging stages.
 */
@SdkInternalApi
public enum HedgingExecutionState {
    /** Hedging is active, attempts can be started */
    RUNNING,
    /** A winner has been determined, completing the user future */
    COMPLETING,
    /** Execution is fully complete */
    COMPLETED
}
