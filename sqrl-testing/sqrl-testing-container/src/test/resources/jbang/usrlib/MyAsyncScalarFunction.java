//DEPS org.apache.flink:flink-table-common:1.19.3

import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.FunctionContext;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MyAsyncScalarFunction extends AsyncScalarFunction {

    private transient ExecutorService executor;

    @Override
    public void open(FunctionContext context) throws Exception {
        // Configure the thread pool to handle asynchronous calls
        this.executor = Executors.newFixedThreadPool(10);
    }

    @Override
    public void close() throws Exception {
        // Properly shut down the executor service
        if (executor != null && !executor.isShutdown()) {
            executor.shutdown();
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }

    public void eval(CompletableFuture<String> result, String param1, int param2) {
        executor.submit(() -> {
            try {
                // Simulate a delay to mimic an I/O-bound operation
                Thread.sleep(1000);
                String response = "Processed " + param1 + " with " + param2;
                result.complete(response); // Complete the future with the response
            } catch (Exception e) {
                result.completeExceptionally(e); // Complete exceptionally if an error occurs
            }
        });
    }
}
