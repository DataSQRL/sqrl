package ai.dataeng.sqml.vertex;

public abstract class Vertex {

  /**
   * Requests notification after all messages bearing the given time or earlier have been delivered.
   */
  public abstract void onNotify(TTime time) throws Exception;
  public abstract void onReceive(Edge e, Message m, TTime time) throws Exception;

  public void notifyAt(TTime time) throws Exception {}
  public void sendBy(Edge e, Message m, TTime time) throws Exception {
    System.out.println(m);
  }

  public void init() throws Exception {}
  public void onShutdown() throws Exception {}
  public void flush() throws Exception {}
}
