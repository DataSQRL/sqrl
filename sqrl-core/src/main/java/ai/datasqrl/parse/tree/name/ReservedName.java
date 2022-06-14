package ai.datasqrl.parse.tree.name;

public class ReservedName extends AbstractName {

  private final String name;

  private ReservedName(String identifier) {
    this.name = identifier;
  }

  @Override
  public String getCanonical() {
    return name;
  }

  @Override
  public String getDisplay() {
    return name;
  }

  public static ReservedName UUID = new ReservedName("_uuid");
  public static ReservedName INGEST_TIME = new ReservedName("_ingest_time");
  public static ReservedName SOURCE_TIME = new ReservedName("_source_time");
  public static ReservedName ARRAY_IDX = new ReservedName("_idx");
  public static ReservedName PARENT = new ReservedName("parent");
  public static ReservedName ALL = new ReservedName("*");

}
