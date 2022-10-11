package ai.datasqrl.errors;
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
  IMPORT_STAR_CANNOT_HAVE_TIMESTAMP("E0003.MD")
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
