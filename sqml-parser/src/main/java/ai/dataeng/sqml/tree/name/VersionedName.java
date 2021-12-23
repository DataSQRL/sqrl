package ai.dataeng.sqml.tree.name;

public class VersionedName extends SimpleName {

  private final String name;
  private final String displayName;
  private final int version;

  public VersionedName(String canonicalName, String displayName, int version) {
    super(displayName);
    this.name = canonicalName;
    this.displayName = displayName;
    this.version = version;
  }

  @Override
  public String getCanonical() {
    return name + "$" + version;
  }

  @Override
  public String getDisplay() {
    return displayName;
  }
}
