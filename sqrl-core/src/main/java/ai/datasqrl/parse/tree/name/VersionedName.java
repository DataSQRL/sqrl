package ai.datasqrl.parse.tree.name;

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
    String[] parts = name.split(ID_DELIMITER_REGEX);
    if (parts.length == 1) {
      return new VersionedName(NameCanonicalizer.SYSTEM.getCanonical(parts[0]), parts[0], 0);
    }
    return new VersionedName(NameCanonicalizer.SYSTEM.getCanonical(parts[0]), parts[0],
        Integer.parseInt(parts[1]));
  }

  @Override
  public String getCanonical() {
    if (version == 0) {
      return canonicalName;
    }
    return canonicalName + ID_DELIMITER + version;
  }

  @Override
  public String getDisplay() {
    return displayName;
  }

  public static VersionedName of(Name name, int version) {
    return new VersionedName(name.getCanonical(), name.getDisplay(), version);
  }

  public Name toName() {
    return new StandardName(this.canonicalName, this.displayName);
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
    return getCanonical();
  }

  public int getVersion() {
    return version;
  }
}
