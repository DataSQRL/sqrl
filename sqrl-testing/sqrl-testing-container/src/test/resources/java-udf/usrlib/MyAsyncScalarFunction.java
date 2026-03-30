//JDEPS org.apache.commons:commons-lang3:3.17.0
import org.apache.commons.lang3.StringUtils;
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
        this.executor = Executors.newFixedThreadPool(10);
    }

    @Override
    public void close() throws Exception {
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
                Thread.sleep(1000);
                String response = "Processed " + StringUtils.capitalize(param1) + " with " + param2;
                result.complete(response);
            } catch (Exception e) {
                result.completeExceptionally(e);
            }
        });
    }
}
