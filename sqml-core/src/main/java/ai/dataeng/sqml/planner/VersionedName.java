package ai.dataeng.sqml.planner;

public class VersionedName {

  private final String name;

  public VersionedName(String name) {

    this.name = name;
  }

  public String getId() {
    return name;
  }
}
