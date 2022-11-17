package ai.datasqrl.config.error;

import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;

public class ErrorCode {
  public static final ErrorCode GENERIC_ERROR = new ErrorCode("E0000.MD");
  public static final ErrorCode IMPORT_NAMESPACE_CONFLICT = new ErrorCode("E0001.MD");
  public static final ErrorCode IMPORT_CANNOT_BE_ALIASED = new ErrorCode("E0002.MD");
  public static final ErrorCode IMPORT_STAR_CANNOT_HAVE_TIMESTAMP = new ErrorCode("E0003.MD");
  public static final ErrorCode IMPORT_IN_HEADER = new ErrorCode("E0004.MD");
  public static final ErrorCode MISSING_DEST_TABLE = new ErrorCode("E0005.MD");
  public static final ErrorCode TIMESTAMP_COLUMN_MISSING = new ErrorCode("E0006.MD");
  public static final ErrorCode TIMESTAMP_COLUMN_EXPRESSION = new ErrorCode("E0007.MD");
  public static final ErrorCode PATH_CONTAINS_RELATIONSHIP = new ErrorCode("E0008.MD");
  public static final ErrorCode MISSING_FIELD = new ErrorCode("E0009.MD");
  public static final ErrorCode MISSING_TABLE = new ErrorCode("E0010.MD");
  public static final ErrorCode ORDINAL_NOT_SUPPORTED = new ErrorCode("E0011.MD");
  public static final ErrorCode CANNOT_SHADOW_RELATIONSHIP = new ErrorCode("E0012.MD");
  public static final ErrorCode TO_MANY_PATH_NOT_ALLOWED = new ErrorCode("E0013.MD");
  public static final ErrorCode NESTED_DISTINCT_ON = new ErrorCode("E0014.MD");

  final URI file;

  @SneakyThrows
  ErrorCode(String fileName) {
    file = Resources.getResource("errorCodes/"+fileName).toURI();
    Preconditions.checkState(new File(file).exists());
  }

  @SneakyThrows
  public String getError() {
    return Files.readString(Path.of(file));
  }
}
