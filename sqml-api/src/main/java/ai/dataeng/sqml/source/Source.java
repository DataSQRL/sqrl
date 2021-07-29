package ai.dataeng.sqml.source;

import ai.dataeng.sqml.schema.Schema;
import ai.dataeng.sqml.vertex.Edge;
import ai.dataeng.sqml.vertex.Message;
import ai.dataeng.sqml.vertex.TTime;
import ai.dataeng.sqml.vertex.Vertex;
import java.util.ArrayList;
import java.util.List;

public abstract class Source extends Vertex {
  protected List<Edge> listeners = new ArrayList<>();

  public List<Edge> getListeners() {
    return listeners;
  }

  @Override
  public void onReceive(Edge e, Message m, TTime time) {
    throw new RuntimeException("Source vertex should not receive data");
  }

  public static abstract class Builder {
    protected Schema schema;

    public Builder schema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public abstract Source build();
  }
}
