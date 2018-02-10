package ru.hardcoders.apns2fcm;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.BodyExtractors;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.netty.http.HttpResources;
import reactor.ipc.netty.resources.LoopResources;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class Apns2Fcm {

    private final BufferedWriter writer = new BufferedWriter(new FileWriter("output.csv"));

    public Apns2Fcm() throws IOException {}

    public static void main(String[] args) throws InterruptedException, IOException {

        if (args.length < 2 || args.length == 1 && "/help".equals(args[0])) {
            System.out.println("Usage: \n" +
                    "\n" +
                    " java -jar apns2fcm.jar poolSize=10 fileName=input.csv sandbox=true packageName=your.package.name serverKey=YOUR_SERVER_KEY\n" +
                    "\n" +
                    "Where \n" +
                    " - poolSize    is a thread pool size for batch-processing, \n" +
                    " - fileName    is an input file name contains APNs tokens line by line, \n" +
                    " - sandbox     : if it is 'true', indicates sandbox environment, \n" +
                    " - packageName is your application package name, \n" +
                    " - serverKey   is your FCM App Server API KEY. \n" +
                    "\n" +
                    "Output: a file output.csv contains APNs tokens in a first column, and FCM tokens in the second. \n");
        } else {
            new Apns2Fcm().run(args);
        }

    }

    private void run(String[] args) throws InterruptedException {

        Args options = Args.fromArgumentArray(args);

        final int poolSize = options.poolSize;
        final String fileName = options.fileName;
        final String packageName = options.packageName;
        final String serverKey = options.serverKey;
        final boolean sandbox = options.sandbox;

        HttpResources.set(LoopResources.create("tool", poolSize, true));
        WebClient client = WebClient.builder()
                .baseUrl("https://iid.googleapis.com/iid/v1:batchImport")
                .clientConnector(new ReactorClientHttpConnector())
                .defaultHeader("Authorization", "key=" + serverKey)
                .defaultHeader("Content-Type", "application/json")
                .build();


        final Scheduler parallelScheduler = Schedulers.newParallel("scheduler", poolSize);

        readFully(fileName).bufferTimeout(100, Duration.ofMillis(300L)).parallel(poolSize).runOn(parallelScheduler).doOnNext(list -> {
            CountDownLatch latch = new CountDownLatch(1);
            Request request = new Request();
            request.application = packageName;
            request.tokens = list;
            request.sandbox = sandbox;
            client.post().body(BodyInserters.fromObject(request))
                    .exchange()
                    .filter(x -> x.statusCode().is2xxSuccessful())
                    .flatMap(x -> x.body(BodyExtractors.toMono(Response.class))).flatMapMany(x -> {
                return Flux.fromIterable(x.results);
            })
            .doAfterTerminate(latch::countDown)
            .doOnError(this::terminateOnError)
            .subscribe(x -> {
                try {
                    writer.write(x.apns + "," + x.fcm + "\n");
                } catch (Exception e) {
                    terminateOnError(e);
                }
            });
            try {
                latch.await();
            } catch (InterruptedException e) { /* do nothing */ }
        })
                .sequential()
                .doAfterTerminate(() -> {
                    flushSafely(writer);
                    System.exit(0);
                })
                .doOnError(this::terminateOnError).subscribe();

        Thread.currentThread().join();
    }

    private void terminateOnError(Throwable x) {
        x.printStackTrace();
        flushSafely(writer);
        System.exit(-1);
    }

    private void flushSafely(Writer writer) {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Flux<String> readFully(String fileName) {
        EmitterProcessor<String> processor = EmitterProcessor.create(200);
        final Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    Files.lines(Paths.get(fileName)).forEach((line) -> {
                        processor.onNext(line);
                    });
                    processor.onComplete();
                } catch (IOException e) {
                    processor.onError(e);
                }
            }
        };
        thread.start();
        return processor.subscribeOn(Schedulers.elastic());
    }

    static class Result {

        @JsonProperty("apns_token")
        public String apns;

        public String status;

        @JsonProperty("registration_token")
        public String fcm;

    }

    static class Response {

        public List<Result> results = new ArrayList<>();

    }

    static class Request {

        public String application;
        public boolean sandbox = false;
        @JsonProperty("apns_tokens")
        public List<String> tokens = new LinkedList<>();

    }

    static class Args {

        int poolSize;
        String fileName;
        boolean sandbox;
        String packageName;
        String serverKey;

        private Args() {}

        static Args fromArgumentArray(String[] args) {

            Map<String, String> parsed = Arrays.asList(args).stream().map(x -> {
                String[] pair = x.split("=");
                return Collections.singletonMap(pair[0], pair[1]);
            }).reduce(new HashMap<>(), (x,y) -> { x.putAll(y); return x; });

            Args result = new Args();
            result.fileName = parsed.get("fileName");
            result.sandbox = Boolean.parseBoolean(parsed.getOrDefault("sandbox", "false"));
            result.poolSize = Integer.parseInt(parsed.getOrDefault("poolSize", "10"));
            result.packageName = parsed.get("packageName");
            result.serverKey = parsed.get("serverKey");

            Objects.requireNonNull(result.fileName, "You should specify a 'fileName'");
            Objects.requireNonNull(result.poolSize, "You should specify a 'poolSize'");
            Objects.requireNonNull(result.packageName, "You should specify a 'packageName'");
            Objects.requireNonNull(result.serverKey, "You should specify a 'serverKey'");

            return result;

        }

    }

}