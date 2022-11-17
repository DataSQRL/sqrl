package ai.datasqrl.config.error;

import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;

public enum ErrorCode {
  GENERIC_ERROR("E0000.MD"),
  IMPORT_NAMESPACE_CONFLICT("E0001.MD"),
  IMPORT_CANNOT_BE_ALIASED("E0002.MD"),
  IMPORT_STAR_CANNOT_HAVE_TIMESTAMP("E0003.MD"),
  IMPORT_IN_HEADER("E0004.MD"),
  MISSING_DEST_TABLE("E0005.MD"),
  TIMESTAMP_COLUMN_MISSING("E0006.MD"),
  TIMESTAMP_COLUMN_EXPRESSION("E0007.MD"),
  PATH_CONTAINS_RELATIONSHIP("E0008.MD"),
  MISSING_FIELD("E0009.MD"),
  MISSING_TABLE("E0010.MD"),
  ORDINAL_NOT_SUPPORTED("E0011.MD"),
  CANNOT_SHADOW_RELATIONSHIP("E0012.MD"),
  TO_MANY_PATH_NOT_ALLOWED("E0013.MD"),
  NESTED_DISTINCT_ON("E0014.MD")
  ;

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
