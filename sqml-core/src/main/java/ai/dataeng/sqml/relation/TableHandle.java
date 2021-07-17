package ai.dataeng.sqml.relation;

public final class TableHandle {
  private String tableName;

  public TableHandle() {

  }
  public TableHandle(String tableName) {

    this.tableName = tableName;
  }

  public String getName() {
    return tableName;
  }
}