import com.github.davidcarboni.cryptolite.Random;
import com.github.davidcarboni.httpino.Endpoint;
import com.github.davidcarboni.httpino.Host;
import com.github.davidcarboni.httpino.Http;
import com.github.davidcarboni.httpino.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import publishing.Result;
import publishing.UriInfo;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

public class Main {

    private static final ExecutorService pool = Executors.newFixedThreadPool(50);

    static String trainUrl = "http://localhost:8084";
    static String baseUri = "/previous/v/testcontent"; // where to put the test publish

    public static void main(String[] args) throws IOException, ParseException {

        boolean publishComplete = true;

        if (args.length > 0) {
            trainUrl = args[0];
            Log.print("Parameter 1 found: train url: %s", trainUrl);
        }

        if (args.length > 1) {
            baseUri = args[0];
            Log.print("Parameter 2 found: base url: %s", baseUri);
        }

        Log.print("Running publish client: %s", trainUrl);

        final Host theTrainHost = new Host(trainUrl);
        String encryptionPassword = Random.password(100);

        String transactionId = "";

        try {

            long start = System.currentTimeMillis();

            Log.print("Begin publish");
            transactionId = beginPublish(theTrainHost, encryptionPassword);

            Log.print("Time taken for Begin publish: %dms", (System.currentTimeMillis() - start));

            long startpublishFiles = System.currentTimeMillis();
            // commit each file in the resources - add the test content path to each file
            Log.print("Publish files");
            List<Future<IOException>> results = new ArrayList<>();

            List<String> files = new ArrayList<>();
            files.add("example-timeseries/timeseries-to-publish.zip");
            files.add("example-timeseries/datasets/pageunencrypted/data.json");
            files.add("example-timeseries/datasets/pageunencrypted/2015/csdb.csdb");
            files.add("example-timeseries/datasets/pageunencrypted/2015/csdb.csv");
            files.add("example-timeseries/datasets/pageunencrypted/2015/csdb.xlsx");
            files.add("example-timeseries/datasets/pageunencrypted/2015/data.json");

            for (String source : files) {
                publishFile(theTrainHost, encryptionPassword, transactionId, results, source);
            }

            // Check the publishing results:
            for (Future<IOException> result : results) {
                try {
                    IOException exception = result.get();
                    if (exception != null) throw exception;
                } catch (InterruptedException | ExecutionException e) {
                    throw new IOException("Error in file publish", e);
                }
            }

            Log.print("Time taken for publish file: %dms", (System.currentTimeMillis() - startpublishFiles));

            long startCommit = System.currentTimeMillis();

            Log.print("Commit publish");
            // If all has gone well so far, commit the publishing transaction:
            Result result = commitPublish(theTrainHost, transactionId, encryptionPassword);

            if (!result.error) {
                publishComplete = true;
            }


            Date startReported = DateConverter.toDate(result.transaction.startDate);
            Date endReported = DateConverter.toDate(result.transaction.endDate);
            Log.print("Time reported for start: %s", result.transaction.startDate);
            Log.print("Time reported for end: %s", result.transaction.endDate);
            Log.print("Time reported for publish: %d", ((endReported.getTime() - startReported.getTime())));
            Log.print("Time taken for Commit: %dms", (System.currentTimeMillis() - startCommit));
            Log.print("Total overall time for publish: %dms", (System.currentTimeMillis() - start));

        } catch (IOException e) {

            Log.print(e);
            // If an error was caught, attempt to roll back the transaction:
            rollbackPublish(theTrainHost, transactionId, encryptionPassword);

        } finally {

        }

        // return a non zero code if the publish failed.
        if (!publishComplete) {
            System.exit(1);
        }

        System.exit(0);
    }

    private static void publishFile(Host theTrainHost, String encryptionPassword, String transactionId, List<Future<IOException>> results, String source) {
        Path sourcePath = Paths.get(source);
        ClassLoader classLoader = Main.class.getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(source);

        String uri = baseUri + source;
        boolean zipped = false;
        String publishUri = uri;

        // if we have a recognised compressed file - set the zip header and set the correct uri so that the files
        // are unzipped to the correct place.
        if (sourcePath.getFileName().toString().equals("timeseries-to-publish.zip")) {
            zipped = true;
            publishUri = StringUtils.removeEnd(uri, "-to-publish.zip");
        }

        Log.print("Sending file: %s to destination uri: %s", sourcePath.toString(), uri);

        results.add(publishFile(theTrainHost, transactionId, encryptionPassword, publishUri, zipped, sourcePath, pool, inputStream));
    }


    /**
     * Starts a publishing transaction.
     *
     * @param host               The Train {@link Host}
     * @param encryptionPassword The password used to encrypt files during publishing.
     * @return The new transaction ID.
     */
    static String beginPublish(Host host, String encryptionPassword) throws IOException {
        String result = null;
        try (Http http = new Http()) {
            Endpoint begin = new Endpoint(host, "begin").setParameter("encryptionPassword", encryptionPassword);
            Response<Result> response = http.post(begin, Result.class);
            checkResponse(response);
            result = response.body.transaction.id;
        }
        return result;
    }

    /**
     * Submits files for asynchronous publishing.
     *
     * @param host               The Train {@link Host}
     * @param transactionId      The transaction to publish to.
     * @param encryptionPassword The password used to encrypt files duing publishing.
     * @param uri                The destination URI.
     * @param source             The data to be published.
     * @param pool               An {@link ExecutorService} to use for asynchronous execution.
     * @return A {@link Future} that will evaluate to {@code null} unless an error occurs in publishing a file, in which case the exception will be returned.
     * @throws IOException
     */
    private static Future<IOException> publishFile(
            final Host host,
            final String transactionId,
            final String encryptionPassword,
            final String publishUri,
            final boolean zipped,
            final Path source,
            ExecutorService pool,
            final InputStream inputStream
    ) {
        return pool.submit(new Callable<IOException>() {
            @Override
            public IOException call() throws Exception {
                IOException result = null;
                try (Http http = new Http()) {
                    Endpoint publish = new Endpoint(host, "publish")
                            .setParameter("transactionId", transactionId)
                            .setParameter("encryptionPassword", encryptionPassword)
                            .setParameter("zip", Boolean.toString(zipped))
                            .setParameter("uri", publishUri);

                    Response<Result> response = http.post(publish, inputStream, source.getFileName().toString(), Result.class);
                    checkResponse(response);

                } catch (IOException e) {
                    result = e;
                }
                return result;
            }
        });
    }

    /**
     * Commits a publishing transaction.
     *
     * @param host               The Train {@link Host}
     * @param transactionId      The transaction to publish to.
     * @param encryptionPassword The password used to encrypt files during publishing.
     * @return The {@link Result} returned by The Train
     * @throws IOException If any errors are encountered in making the request or reported in the {@link Result}.
     */
    static Result commitPublish(Host host, String transactionId, String encryptionPassword) throws IOException {
        return endPublish(host, "commit", transactionId, encryptionPassword);
    }

    /**
     * Rolls back a publishing transaction, suppressing any {@link IOException} and printing it out to the console instead.
     *
     * @param host               The Train {@link Host}
     * @param transactionId      The transaction to publish to.
     * @param encryptionPassword The password used to encrypt files during publishing.
     */
    static void rollbackPublish(Host host, String transactionId, String encryptionPassword) {
        try {
            endPublish(host, "rollback", transactionId, encryptionPassword);
        } catch (IOException e) {
            System.out.println("Error rolling back publish transaction:");
            System.out.println(ExceptionUtils.getStackTrace(e));
        }
    }

    static Result endPublish(Host host, String endpointName, String transactionId, String encryptionPassword) throws IOException {
        Result result;
        try (Http http = new Http()) {
            Endpoint endpoint = new Endpoint(host, endpointName)
                    .setParameter("transactionId", transactionId)
                    .setParameter("encryptionPassword", encryptionPassword);
            Response<Result> response = http.post(endpoint, Result.class);
            checkResponse(response);
            result = response.body;
        }
        return result;
    }

    static void checkResponse(Response<Result> response) throws IOException {

        if (response.statusLine.getStatusCode() != 200) {
            int code = response.statusLine.getStatusCode();
            String reason = response.statusLine.getReasonPhrase();
            String message = response.body != null ? response.body.message : "";
            throw new IOException("Error in request: " + code + " " + reason + " " + message);
        } else if (response.body.error == true) {
            throw new IOException("Result error: " + response.body.message);
        } else if (response.body.transaction.errors != null && response.body.transaction.errors.size() > 0) {
            throw new IOException("Transaction error: " + response.body.transaction.errors);
        } else if (response.body.transaction.uriInfos != null) {
            List<String> messages = new ArrayList<>();
            for (UriInfo uriInfo : response.body.transaction.uriInfos) {
                if (StringUtils.isNotBlank(uriInfo.error)) {
                    messages.add("URI error for " + uriInfo.uri + " (" + uriInfo.status + "): " + uriInfo.error);
                }
            }
            if (messages.size() > 0) {
                throw new IOException(messages.toString());
            }
        }
    }

}
