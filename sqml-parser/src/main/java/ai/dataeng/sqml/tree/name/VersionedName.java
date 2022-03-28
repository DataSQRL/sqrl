package ai.dataeng.sqml.tree.name;

import java.util.Objects;

public class VersionedName extends SimpleName {

  private final String canonicalName;
  private final String displayName;
  private final int version;

  public static final String ID_DELIMITER_REGEX = "\\$";
  public static final String ID_DELIMITER = "$";

  public VersionedName(String canonicalName, String displayName, int version) {
    super(displayName);
    this.canonicalName = canonicalName;
    this.displayName = displayName;
    this.version = version;
  }

  public static VersionedName parse(String name) {
    String[] parts = name.split("\\$");
    return new VersionedName(parts[0], parts[0], Integer.parseInt(parts[1]));
  }

  @Override
  public String getCanonical() {
    return canonicalName + "$" + version;
  }

  @Override
  public String getDisplay() {
    return displayName;
  }

  public static VersionedName of(Name name, int version) {
    return new VersionedName(name.getCanonical(), name.getDisplay(), version);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    VersionedName that = (VersionedName) o;
    return version == that.version && Objects.equals(canonicalName, that.canonicalName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), canonicalName, version);
  }

  @Override
  public String toString() {
    return canonicalName + ID_DELIMITER + version;
  }

  public int getVersion() {
    return version;
  }
}
