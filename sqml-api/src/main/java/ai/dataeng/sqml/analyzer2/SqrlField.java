package ai.dataeng.sqml.analyzer2;

public class SqrlField {
  private final String name;
  private final String alias;

  public SqrlField(String name, String alias) {

    this.name = name;
    this.alias = alias;
  }

  public String getQualifiedName() {
    return String.format("%s.%s", alias, name);
  }
}
