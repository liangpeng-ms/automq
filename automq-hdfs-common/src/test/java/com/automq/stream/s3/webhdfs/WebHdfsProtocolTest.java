package com.automq.stream.s3.webhdfs;

import org.junit.jupiter.api.Test;

import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WebHdfsProtocolTest {

    @Test
    void isRedirect_coversAll3xxWebhdfsCodes() {
        for (int sc : new int[] {301, 302, 303, 307, 308}) {
            assertTrue(WebHdfsProtocol.isRedirect(sc), "expected redirect for " + sc);
        }
        for (int sc : new int[] {200, 201, 404, 500}) {
            assertFalse(WebHdfsProtocol.isRedirect(sc), "expected non-redirect for " + sc);
        }
    }

    @Test
    void writeRedirectLocation_fromHeader() {
        HttpResponse<byte[]> resp = fakeResponse(307, Map.of("Location", List.of("http://dn:50075/x")), new byte[0]);
        assertEquals("http://dn:50075/x", WebHdfsProtocol.writeRedirectLocation(resp));
    }

    @Test
    void writeRedirectLocation_fromNoRedirectJsonBody() {
        byte[] body = "{\"Location\":\"http://dn:50075/y\"}".getBytes(StandardCharsets.UTF_8);
        HttpResponse<byte[]> resp = fakeResponse(200, Map.of(), body);
        assertEquals("http://dn:50075/y", WebHdfsProtocol.writeRedirectLocation(resp));
    }

    @Test
    void writeRedirectLocation_nullWhenNoLocation() {
        HttpResponse<byte[]> resp = fakeResponse(200, Map.of(), "{}".getBytes(StandardCharsets.UTF_8));
        assertNull(WebHdfsProtocol.writeRedirectLocation(resp));
    }

    @Test
    void appendParams_buildsAmpersandPairs() {
        StringBuilder sb = new StringBuilder("base?op=OPEN");
        WebHdfsProtocol.appendParams(sb, "offset", "10", "length", "20");
        assertEquals("base?op=OPEN&offset=10&length=20", sb.toString());
    }

    @Test
    void errorMessage_formatsWithBody() {
        String msg = WebHdfsProtocol.errorMessage("CREATE /a", 500, "boom".getBytes(StandardCharsets.UTF_8));
        assertEquals("WebHDFS CREATE /a failed, status=500, body=boom", msg);
    }

    @Test
    void errorMessage_nullBodyIsEmpty() {
        String msg = WebHdfsProtocol.errorMessage("MKDIRS /a", 403, null);
        assertEquals("WebHDFS MKDIRS /a failed, status=403, body=", msg);
    }

    @Test
    void webHdfsException_carriesStatusCode() {
        WebHdfsException ex = new WebHdfsException(404, "nope");
        assertEquals(404, ex.statusCode());
        assertEquals("nope", ex.getMessage());
    }

    private static HttpResponse<byte[]> fakeResponse(int status, Map<String, List<String>> headers, byte[] body) {
        HttpHeaders hdrs = HttpHeaders.of(headers, (a, b) -> true);
        return new HttpResponse<>() {
            @Override
            public int statusCode() {
                return status;
            }

            @Override
            public java.net.http.HttpRequest request() {
                return null;
            }

            @Override
            public Optional<HttpResponse<byte[]>> previousResponse() {
                return Optional.empty();
            }

            @Override
            public HttpHeaders headers() {
                return hdrs;
            }

            @Override
            public byte[] body() {
                return body;
            }

            @Override
            public Optional<javax.net.ssl.SSLSession> sslSession() {
                return Optional.empty();
            }

            @Override
            public java.net.URI uri() {
                return java.net.URI.create("http://gw/webhdfs/v1/x");
            }

            @Override
            public java.net.http.HttpClient.Version version() {
                return java.net.http.HttpClient.Version.HTTP_1_1;
            }
        };
    }
}
