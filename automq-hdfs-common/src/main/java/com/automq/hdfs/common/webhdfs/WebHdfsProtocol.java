/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.hdfs.common.webhdfs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

/**
 * Stateless helpers shared by the synchronous {@code WebHdfsClient} and the asynchronous {@code AsyncWebHdfsClient}.
 * These are pure functions over already-received responses / string fragments and do no IO, so they are safe to call
 * from either the blocking or the {@code CompletableFuture} execution model.
 */
public final class WebHdfsProtocol {
    private static final ObjectMapper JSON = new ObjectMapper();

    private WebHdfsProtocol() {
    }

    /** Whether an HTTP status is one of the redirect codes WebHDFS uses to hand off to a DataNode. */
    public static boolean isRedirect(int sc) {
        return sc == 301 || sc == 302 || sc == 303 || sc == 307 || sc == 308;
    }

    /**
     * Extracts the DataNode upload URL from either a 3xx {@code Location} header or an HttpFS-style {@code noredirect}
     * 200 response whose JSON body carries a non-empty {@code Location}. Returns {@code null} if neither is present.
     */
    public static String writeRedirectLocation(HttpResponse<byte[]> resp) {
        int sc = resp.statusCode();
        if (isRedirect(sc)) {
            return resp.headers().firstValue("Location").orElse(null);
        }
        if (sc == 200 && resp.body() != null && resp.body().length > 0) {
            try {
                JsonNode loc = JSON.readTree(resp.body()).path("Location");
                if (!loc.isMissingNode() && !loc.asText().isEmpty()) {
                    return loc.asText();
                }
            } catch (IOException ignored) {
                // Fall through to null: the caller reports the original response as an error.
            }
        }
        return null;
    }

    /** Appends {@code &key=value} for each pair in {@code kv} to {@code sb}; a trailing odd element is ignored. */
    public static void appendParams(StringBuilder sb, String... kv) {
        for (int i = 0; i + 1 < kv.length; i += 2) {
            sb.append('&').append(kv[i]).append('=').append(kv[i + 1]);
        }
    }

    /** Uniform WebHDFS error message text (does not construct an exception; each caller wraps in its own type). */
    public static String errorMessage(String op, int statusCode, byte[] body) {
        String bodyText = body == null ? "" : new String(body, StandardCharsets.UTF_8);
        return String.format("WebHDFS %s failed, status=%d, body=%s", op, statusCode, bodyText);
    }
}
