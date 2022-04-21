package ai.datasqrl.config.error;

import ai.datasqrl.parse.tree.name.Name;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import lombok.Value;

@JsonSerialize(as = ErrorLocation.class)
public interface ErrorLocation {

  String getPrefix();

  @JsonIgnore
  default boolean hasPrefix() {
    return !Strings.isNullOrEmpty(getPrefix());
  }

  @JsonIgnore
  @NonNull String[] getPathArray();

  @JsonIgnore
  default String getPathAt(int index) {
    return getPathArray()[index];
  }

  @JsonIgnore
  default int getPathLength() {
    return getPathArray().length;
  }

  String getPath();

  File getFile();

  @JsonIgnore
  default boolean hasFile() {
    return getFile() != null;
  }

  ErrorLocation append(@NonNull ErrorLocation other);

  ErrorLocation resolve(@NonNull String location);

  default ErrorLocation resolve(@NonNull Name location) {
    return resolve(location.getDisplay());
  }

  default ErrorLocation atFile(int line, int offset) {
    Preconditions.checkArgument(line > 0 && offset > 0);
    return atFile(new File(line, offset));
  }

  ErrorLocation atFile(@NonNull File file);

  @Value
  class File {

    private final int line;
    private final int offset;

  }

}