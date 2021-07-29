package ai.dataeng.sqml.source;

import ai.dataeng.sqml.schema.Schema;
import ai.dataeng.sqml.vertex.Edge;
import ai.dataeng.sqml.vertex.Message;
import ai.dataeng.sqml.vertex.TTime;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class HttpIngress extends Source {
  private final String url;
  private final int bufferSize;
  private BufferedReader in;

  public HttpIngress(String url, int bufferSize) {
    this.url = url;
    this.bufferSize = bufferSize;
  }

  @Override
  public void init() throws Exception {
    URL url = new URL(this.url);
    in = new BufferedReader(
        new InputStreamReader(url.openStream()), bufferSize);
  }

  @Override
  public void onNotify(TTime time) throws Exception {
    String line;
    try {
      in.reset();
    } catch (IOException e) {}

    try {
      tryRead(in);
    } catch (TimeoutException e) {
      return;
    }

    while ((line = in.readLine()) != null) {
      for (Edge listener : listeners) {
        this.sendBy(listener, new SourceMessage(line), time);
      }

      try {
        tryRead(in);
      } catch (TimeoutException e) {
        break;
      }
    }
  }

  private void tryRead(BufferedReader in)
      throws Exception {
    in.mark(1);

    CompletableFuture future = CompletableFuture.supplyAsync(()->{
      try {
        return in.read();
      } catch (IOException e) {
        e.printStackTrace();
        return null;
      }
    });
    try {
      future.get(500, TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException e) {
      in.reset();
      throw new TimeoutException(e.getMessage());
    }
    in.reset();
  }

  class SourceMessage extends Message {
    private final String message;

    public SourceMessage(String message) {
      this.message = message;
    }

    @Override
    public String toString() {
      return "SourceMessage{" +
          "message='" + message + '\'' +
          '}';
    }
  }

  ///

  public static Builder newHttpIngres() {
    return new Builder();
  }

  public static class Builder extends Source.Builder{
    private String url;
    private int bufferSizeInBytes = 1_000_000;

    public Builder url(String url) {
      this.url = url;
      return this;
    }

    public Builder bufferSize(int bufferSizeInBytes) {
      this.bufferSizeInBytes = bufferSizeInBytes;
      return this;
    }

    public Source build() {
      return new HttpIngress(url, bufferSizeInBytes);
    }
  }
}
