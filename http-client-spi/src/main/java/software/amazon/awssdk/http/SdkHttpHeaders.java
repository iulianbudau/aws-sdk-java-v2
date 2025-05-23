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

package software.amazon.awssdk.http;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import software.amazon.awssdk.annotations.Immutable;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.utils.http.SdkHttpUtils;

/**
 * An immutable set of HTTP headers. {@link SdkHttpRequest} should be used for requests, and {@link SdkHttpResponse} should be
 * used for responses.
 */
@SdkPublicApi
@Immutable
public interface SdkHttpHeaders {
    /**
     * Returns a map of all HTTP headers in this message, sorted in case-insensitive order by their header name.
     *
     * <p>This will never be null. If there are no headers an empty map is returned.</p>
     *
     * @return An unmodifiable map of all headers in this message.
     */
    Map<String, List<String>> headers();

    /**
     * Perform a case-insensitive search for a particular header in this request, returning the first matching header, if one is
     * found.
     *
     * <p>This is useful for headers like 'Content-Type' or 'Content-Length' of which there is expected to be only one value
     * present.</p>
     *
     * <p>This is equivalent to invoking {@link SdkHttpUtils#firstMatchingHeader(Map, String)}</p>.
     *
     * @param header The header to search for (case insensitively).
     * @return The first header that matched the requested one, or empty if one was not found.
     */
    default Optional<String> firstMatchingHeader(String header) {
        return SdkHttpUtils.firstMatchingHeader(headers(), header);
    }

    /**
     * Returns whether any header key matches the provided predicate.
     *
     * @param predicate the predicate to apply to all header keys
     * @return true if any header key matches the provided predicate, otherwise {@code false}
     */
    default boolean anyMatchingHeader(Predicate<String> predicate) {
        return headers().keySet().stream().anyMatch(predicate);
    }

    default Optional<String> firstMatchingHeader(Collection<String> headersToFind) {
        return SdkHttpUtils.firstMatchingHeaderFromCollection(headers(), headersToFind);
    }

    default List<String> matchingHeaders(String header) {
        return SdkHttpUtils.allMatchingHeaders(headers(), header).collect(Collectors.toList());
    }

    default void forEachHeader(BiConsumer<? super String, ? super List<String>> consumer) {
        headers().forEach(consumer);
    }

    default int numHeaders() {
        return headers().size();
    }
}
