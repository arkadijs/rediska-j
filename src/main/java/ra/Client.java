package ra;

import com.ning.http.client.*;
import com.ning.http.client.extra.ThrottleRequestFilter;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

/** Full-text search Java API */
public class Client {
    private final String baseUri;
    private final String contentUri;
    private final AsyncHttpClient http;

    public Client(String host, int port, int concurrency, int timeoutMillis) {
        baseUri = "http://" + host + ":" + port + "/";
        contentUri = baseUri + "content";
        http = new AsyncHttpClient(
                    new AsyncHttpClientConfig.Builder()
                            .setAllowPoolingConnection(true)
                            .setFollowRedirects(false)
                            .setCompressionEnabled(false)
                            .setConnectionTimeoutInMs(timeoutMillis)
                            .addRequestFilter(new ThrottleRequestFilter(concurrency, timeoutMillis))
                            .build());
    }

    /** Represents result of Rediska API call via HTTP. */
    public class Result {
        /** The call was sucessfull or not shortcut. */
        public final boolean success;
        /** HTTP status code. */
        public final int code;
        /** HTTP status text. */
        public final String message;
        /** Rediska API time in nano-seconds. */
        public final long elapsedNanos;
        /** content-id assigned by plain PUT /content */
        public String assignedId;
        /** The list of content-id-s returned by {@link ra.Client#search(String)} */
        public List<String> ids;

        protected Result(boolean success, int code, String message, long elapsedNanos, List<String> ids) {
            this.success = success;
            this.code = code;
            this.message = message;
            this.elapsedNanos = elapsedNanos;
            setIds(ids);
        }

        protected final Result setAssignedId(String id) {
            this.assignedId = id;
            return this;
        }

        protected final Result setIds(List<String> ids) {
            this.ids = ids != null ? ids : Collections.EMPTY_LIST;
            return this;
        }
    }

    private long elapsed(Response resp) {
        String hdr = resp.getHeader("X-RA-Elapsed");
        if (hdr == null)
            return -1L;
        return Long.parseLong(hdr);
    }

    private List<String> parseJsonArray(String str) throws ParseException {
        return (JSONArray) new JSONParser().parse(str);
    }

    private Result genericResult(int requiredCode, Response resp) {
        return new Result(
                resp.getStatusCode() == requiredCode,
                resp.getStatusCode(), resp.getStatusText(), elapsed(resp),
                null
        );
    }

    private AsyncCompletionHandler<Result> genericHandler(final int code) {
        return new AsyncCompletionHandler<Result>() {
            @Override
            public Result onCompleted(Response resp) {
                return genericResult(code, resp);
            }
        };
    }

    /** Adds content to the search database. */
    public Future<Result> put(final String id, String content) throws IOException {
        String path = id != null ? contentUri + "/" + id : contentUri;
        return http.preparePut(path).setBodyEncoding("UTF-8").setBody(content).execute(
                new AsyncCompletionHandler<Result>() {
                    @Override
                    public Result onCompleted(Response resp) throws IOException, ParseException {
                        return genericResult(HttpURLConnection.HTTP_CREATED, resp)
                                .setAssignedId(
                                        // id == null &&
                                        resp.getStatusCode() == HttpURLConnection.HTTP_CREATED
                                                && resp.getContentType().startsWith("text/plain")
                                                && resp.getResponseBody()!= null
                                                && !resp.getResponseBody().isEmpty() ?
                                                resp.getResponseBody() : null);
                    }
                });
    }

    /** Searches content for terms in query.
     * @return Result.ids is a list of content-id-s that matches the query. */
    public Future<Result> search(String query) throws IOException {
        return http.prepareGet(contentUri).addQueryParameter("q", query).execute(
                new AsyncCompletionHandler<Result>() {
                    @Override
                    public Result onCompleted(Response resp) throws IOException, ParseException {
                        return genericResult(HttpURLConnection.HTTP_OK, resp)
                                .setIds(resp.getStatusCode() == HttpURLConnection.HTTP_OK ? parseJsonArray(resp.getResponseBody()) : null);
                    }
                });
    }

    /** Removes content from database that matches query. */
    public Future<Result> remove(String query) throws IOException {
        return remove("q", query);
    }

    private String join(List<String> list, char sep) {
        if (list.isEmpty() || (list.size() == 1 && list.get(0).isEmpty()))
            return "";
        StringBuilder sb = new StringBuilder();
        for (String item : list)
            sb.append(sep).append(item);
        return sb.toString().substring(1);
    }

    /** Removes content from database by content-id. */
    public Future<Result> remove(List<String> ids) throws IOException {
        return remove("id", join(ids, '|'));
    }

    private Future<Result> remove(String param, String value) throws IOException {
        return http.prepareDelete(contentUri).addQueryParameter(param, value).execute(genericHandler(HttpURLConnection.HTTP_OK));
    }

    public Future<Result> reset() throws IOException {
        return http.preparePost(baseUri + "reset").execute(genericHandler(HttpURLConnection.HTTP_OK));
    }
}
