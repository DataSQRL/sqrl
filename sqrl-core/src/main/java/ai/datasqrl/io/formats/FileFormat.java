package ai.datasqrl.io.formats;

import com.google.common.base.Strings;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

public enum FileFormat {

  CSV(CSVFormat.NAME) {
    @Override
    public Format getImplementation() {
      return new CSVFormat();
    }
  },
  JSON(JsonLineFormat.NAME) {
    @Override
    public Format getImplementation() {
      return new JsonLineFormat();
    }
  };

  private final String[] identifiers;

  FileFormat(String... identifiers) {
    this.identifiers = identifiers;
  }

  public abstract Format getImplementation();

  @Override
  public String toString() {
    return identifiers[0];
  }

  public boolean matches(String format) {
    if (Strings.isNullOrEmpty(format)) {
      return false;
    }
    format = normalizeFormat(format);
    for (int i = 0; i < identifiers.length; i++) {
      if (identifiers[i].equals(format)) {
        return true;
      }
    }
    return false;
  }

  private static final Map<String, FileFormat> EXTENSION_MAP = Arrays.stream(values())
      .flatMap(ft -> Arrays.stream(ft.identifiers).map(e -> Pair.of(ft, e)))
      .collect(Collectors.toMap(Pair::getRight, Pair::getLeft));

  public static boolean validFormat(String format) {
    return EXTENSION_MAP.containsKey(normalizeFormat(format));
  }

  public static FileFormat getFormat(String format) {
    FileFormat ft = EXTENSION_MAP.get(normalizeFormat(format));
    return ft;
  }

  private static final String normalizeFormat(String format) {
    return format.trim().toLowerCase();
  }
}
