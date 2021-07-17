package ai.dataeng.sqml.vertex;

public abstract class SqlVertexFactory {

  public abstract Vertex createVertex(Partition partition);
}
