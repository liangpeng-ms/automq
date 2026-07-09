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

package kafka.automq.table.io;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Minimal, self-contained WebHDFS REST client used by {@link WebHdfsFileIO}. It intentionally avoids any Hadoop
 * dependency: all HDFS access goes through the WebHDFS HTTP gateway using the JDK {@link HttpClient} and an Entra ID
 * (AAD) bearer token, mirroring the proven request shapes used by the AutoMQ WAL/object-storage HDFS backend
 * (one-shot {@code data=true} CREATE, ranged OPEN, GETFILESTATUS, MKDIRS, DELETE).
 *
 * <p>Location handling. Iceberg table locations are stored with the HDFS RPC scheme, e.g.
 * {@code hdfs://<namenode-authority>/abs/path} — whose authority is the NameNode, not the WebHDFS HTTP gateway. By
 * default the MT WebHDFS gateway is derived from that authority (see {@link #deriveGateway}); e.g.
 * {@code namenode2extra7-vipv4.MTPrime-PROD-MWHE02.MWHE02.ap.gbl} →
 * {@code https://hdfs-http-ipv4-mtprime-mwhe02-2.magnetar.binginternal.com:83/webhdfs/v1/MTPrime-MWHE02-2-Extra-7}. An
 * explicit {@code gatewayBase} override can be supplied (used by the unit-test emulator); every location is then
 * rewritten to {@code base + absPath} — only the absolute path is used.
 */
final class WebHdfsClient {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int MAX_REDIRECTS = 3;
    /** RPC NameNode host label: {@code namenode<index>[extra<n>]}. */
    private static final Pattern NAMENODE = Pattern.compile("namenode(\\d+)(?:extra(\\d+))?", Pattern.CASE_INSENSITIVE);

    private final HttpClient http;
    private final Supplier<String> token;
    private final Duration timeout;
    /** Optional WebHDFS gateway base including {@code /webhdfs/v1[/mount]}, without a trailing slash; null = derive from the hdfs:// authority. */
    private final String gatewayBase;

    WebHdfsClient(Supplier<String> token, Duration timeout) {
        this(token, timeout, null);
    }

    WebHdfsClient(Supplier<String> token, Duration timeout, String gatewayBase) {
        this.token = token;
        this.timeout = timeout;
        this.gatewayBase = normalizeGateway(gatewayBase);
        this.http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
    }

    private static String normalizeGateway(String gatewayBase) {
        if (gatewayBase == null || gatewayBase.isEmpty()) {
            return null;
        }
        String g = gatewayBase;
        while (g.endsWith("/")) {
            g = g.substring(0, g.length() - 1);
        }
        return g;
    }

    /**
     * Translate an Iceberg location into the WebHDFS REST base URL: {@code gatewayBase + absPath}, where the gateway is
     * the explicit override if configured, otherwise derived from the location's {@code hdfs://} authority.
     */
    String restUrl(String location) {
        URI u = URI.create(location);
        String path = u.getRawPath() == null ? "" : u.getRawPath();
        String base = gatewayBase != null ? gatewayBase : deriveGateway(u.getAuthority());
        return base + path;
    }

    /**
     * Derive the MT WebHDFS HTTP v2 gateway base from an HDFS RPC NameNode authority, following the MT naming
     * convention: authority {@code namenode<X>[extra<Y>]-vipv4.MTPrime-PROD-<CLUSTER>.<CLUSTER>.ap.gbl} maps to
     * {@code https://hdfs-http-ipv4-mtprime-<cluster>-<X>.magnetar.binginternal.com:83/webhdfs/v1/MTPrime-<CLUSTER>-<X>[-Extra-<Y>]}.
     */
    static String deriveGateway(String authority) {
        if (authority == null || authority.isEmpty()) {
            throw new IllegalArgumentException("hdfs location has no authority; cannot derive WebHDFS gateway");
        }
        String[] labels = authority.split("\\.");
        if (labels.length < 3) {
            throw new IllegalArgumentException("Unrecognized HDFS NameNode authority: " + authority);
        }
        Matcher m = NAMENODE.matcher(labels[0]);
        if (!m.find()) {
            throw new IllegalArgumentException("Unrecognized HDFS NameNode authority (no namenode<index>): " + authority);
        }
        String nnIndex = m.group(1);
        String extra = m.group(2);
        String cluster = labels[2]; // e.g. MWHE02 / DUBE01
        String host = "hdfs-http-ipv4-mtprime-" + cluster.toLowerCase(Locale.ROOT) + "-" + nnIndex
            + ".magnetar.binginternal.com:83";
        String subcluster = "MTPrime-" + cluster + "-" + nnIndex + (extra != null ? "-Extra-" + extra : "");
        return "https://" + host + "/webhdfs/v1/" + subcluster;
    }

    static String parentLocation(String location) {
        int idx = location.lastIndexOf('/');
        return idx <= 0 ? location : location.substring(0, idx);
    }

    long getLength(String location) {
        HttpResponse<byte[]> resp = send("GET", op(location, "GETFILESTATUS"), null, null);
        if (resp.statusCode() == 404) {
            throw new UncheckedIOException(new IOException("Not found: " + location));
        }
        expect(resp, "GETFILESTATUS", 200);
        try {
            return JSON.readTree(resp.body()).path("FileStatus").path("length").asLong();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    boolean exists(String location) {
        HttpResponse<byte[]> resp = send("GET", op(location, "GETFILESTATUS"), null, null);
        if (resp.statusCode() == 200) {
            return true;
        }
        if (resp.statusCode() == 404) {
            return false;
        }
        throw error("GETFILESTATUS", resp);
    }

    /** Open a streaming read starting at {@code offset} to end of file. Follows a DataNode redirect if returned. */
    InputStream open(String location, long offset) {
        URI uri = op(location, "OPEN", "offset", Long.toString(offset));
        HttpResponse<InputStream> resp = sendStream("GET", uri);
        if (resp.statusCode() == 404) {
            throw new UncheckedIOException(new IOException("Not found: " + location));
        }
        if (isRedirect(resp.statusCode())) {
            String loc = resp.headers().firstValue("Location").orElseThrow();
            drain(resp.body());
            resp = sendStream("GET", URI.create(loc));
        }
        if (resp.statusCode() != 200) {
            drain(resp.body());
            throw new UncheckedIOException(new IOException("OPEN " + location + " failed, status=" + resp.statusCode()));
        }
        return resp.body();
    }

    /**
     * Create/overwrite a file with a single-request WebHDFS write ({@code data=true}), with a DataNode-redirect
     * fallback for classic gateways. Iceberg does NOT need atomic data-file publish: file names are unique and a file
     * only becomes visible once referenced by a committed snapshot, so a torn/orphan file is never read (Iceberg's
     * orphan-file cleanup removes it later). Skipping temp+RENAME also avoids an extra NameNode metadata op.
     */
    void create(String location, Path localBody, boolean overwrite) {
        mkdirs(parentLocation(location));
        URI uri = op(location, "CREATE", "overwrite", Boolean.toString(overwrite), "data", "true");
        HttpResponse<byte[]> resp = sendBody("PUT", uri, bodyOf(localBody), "application/octet-stream");
        String redirect = writeRedirect(resp);
        if (redirect != null) {
            resp = sendBody("PUT", URI.create(redirect), bodyOf(localBody), "application/octet-stream");
        }
        expect(resp, "CREATE " + location, 201, 200);
    }

    private static HttpRequest.BodyPublisher bodyOf(Path localBody) {
        try {
            return HttpRequest.BodyPublishers.ofFile(localBody);
        } catch (java.io.FileNotFoundException e) {
            throw new UncheckedIOException(e);
        }
    }

    void mkdirs(String location) {
        HttpResponse<byte[]> resp = send("PUT", op(location, "MKDIRS"), null, null);
        expect(resp, "MKDIRS " + location, 200);
    }

    void delete(String location) {
        HttpResponse<byte[]> resp = send("DELETE", op(location, "DELETE", "recursive", "false"), null, null);
        if (resp.statusCode() != 200 && resp.statusCode() != 404) {
            throw error("DELETE " + location, resp);
        }
    }

    // ---- helpers ----

    private URI op(String location, String op, String... params) {
        StringBuilder sb = new StringBuilder(restUrl(location)).append("?op=").append(op);
        for (int i = 0; i + 1 < params.length; i += 2) {
            sb.append('&').append(params[i]).append('=').append(params[i + 1]);
        }
        return URI.create(sb.toString());
    }

    private HttpResponse<byte[]> send(String method, URI uri, byte[] reqBody, String contentType) {
        return send(method, uri, reqBody, contentType, 0);
    }

    private HttpResponse<byte[]> send(String method, URI uri, byte[] reqBody, String contentType, int redirects) {
        HttpRequest.Builder b = HttpRequest.newBuilder(uri).timeout(timeout)
            .header("Authorization", "Bearer " + token.get());
        if (contentType != null) {
            b.header("Content-Type", contentType);
        }
        b.method(method, reqBody == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(reqBody));
        try {
            HttpResponse<byte[]> resp = http.send(b.build(), HttpResponse.BodyHandlers.ofByteArray());
            if (isRedirect(resp.statusCode()) && redirects < MAX_REDIRECTS) {
                Optional<String> loc = resp.headers().firstValue("Location");
                if (loc.isPresent()) {
                    return send(method, URI.create(loc.get()), reqBody, contentType, redirects + 1);
                }
            }
            return resp;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedIOException(new IOException(e));
        }
    }

    private HttpResponse<byte[]> sendBody(String method, URI uri, HttpRequest.BodyPublisher body, String contentType) {
        HttpRequest req = HttpRequest.newBuilder(uri).timeout(timeout)
            .header("Authorization", "Bearer " + token.get())
            .header("Content-Type", contentType)
            .method(method, body)
            .build();
        try {
            return http.send(req, HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedIOException(new IOException(e));
        }
    }

    private HttpResponse<InputStream> sendStream(String method, URI uri) {
        HttpRequest req = HttpRequest.newBuilder(uri).timeout(timeout)
            .header("Authorization", "Bearer " + token.get())
            .method(method, HttpRequest.BodyPublishers.noBody())
            .build();
        try {
            return http.send(req, HttpResponse.BodyHandlers.ofInputStream());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedIOException(new IOException(e));
        }
    }

    private static boolean isRedirect(int sc) {
        return sc == 307 || sc == 308 || sc == 301 || sc == 302 || sc == 303;
    }

    private static String writeRedirect(HttpResponse<byte[]> resp) {
        if (isRedirect(resp.statusCode())) {
            return resp.headers().firstValue("Location").orElse(null);
        }
        if (resp.statusCode() == 200 && resp.body() != null && resp.body().length > 0) {
            try {
                JsonNode loc = JSON.readTree(resp.body()).path("Location");
                if (!loc.isMissingNode() && !loc.asText().isEmpty()) {
                    return loc.asText();
                }
            } catch (IOException ignored) {
                // fall through
            }
        }
        return null;
    }

    private static void expect(HttpResponse<byte[]> resp, String op, int... okCodes) {
        for (int ok : okCodes) {
            if (resp.statusCode() == ok) {
                return;
            }
        }
        throw error(op, resp);
    }

    private static UncheckedIOException error(String op, HttpResponse<byte[]> resp) {
        String body = resp.body() == null ? "" : new String(resp.body(), StandardCharsets.UTF_8);
        return new UncheckedIOException(new IOException(String.format("WebHDFS %s failed, status=%d, body=%s", op, resp.statusCode(), body)));
    }

    private static void drain(InputStream in) {
        try (InputStream s = in) {
            s.readAllBytes();
        } catch (IOException ignored) {
            // best effort
        }
    }
}
