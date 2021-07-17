package ai.dataeng.sqml.execution;

import ai.dataeng.sqml.query.Query;
import ai.dataeng.sqml.vertex.TTime;
import ai.dataeng.sqml.vertex.Vertex;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LocalExecutionStrategy extends ExecutionStrategy {

  private ExecutorService executor =
      Executors.newFixedThreadPool(1, r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
      });

  public static ExecutionStrategy newExecutionStrategy() {
    return new LocalExecutionStrategy();
  }

  @Override
  public ExecutionResult execute(Query query) {
    return null;
  }

  @Override
  public void init(List<Vertex> vertices) {
    for (Vertex vertex : vertices) {
      try {
        vertex.init();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    executor.execute(() -> {
      while(true) {
        for (Vertex vertex : vertices) {
          try {
            Thread.sleep(100);//test
            System.out.println("Triggering");
            vertex.onNotify(new TTime(System.currentTimeMillis()));
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    });
  }

  @Override
  public void shutdown() {

  }
}