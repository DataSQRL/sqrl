package ai.dataeng.sqml.type;

public class SqmlType {

  private final String name;

  private SqmlType(String name) {

    this.name = name;
  }
  public static SqmlType STRING = new SqmlType("string");
  public static SqmlType DOUBLE = new SqmlType("double");
  public static SqmlType INTEGER = new SqmlType("integer");
  public static SqmlType RELATION = new SqmlType("relation");

  public static SqmlType of(String type) {
    return STRING;
  }
}
