package ai.dataeng.sqml.config.error;

import ai.dataeng.sqml.tree.name.Name;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import lombok.Value;

public interface ErrorLocation {

    String getPrefix();

    default boolean hasPrefix() {
        return !Strings.isNullOrEmpty(getPrefix());
    }

    @NonNull String[] getPath();

    default String getPathAt(int index) {
        return getPath()[index];
    }

    default int getPathLength() {
        return getPath().length;
    }

    File getFile();

    default boolean hasFile() {
        return getFile()!=null;
    }

    ErrorLocation append(@NonNull ErrorLocation other);

    ErrorLocation resolve(@NonNull String location);

    default ErrorLocation resolve(@NonNull Name location) {
        return resolve(location.getDisplay());
    }

    default ErrorLocation atFile(int line, int offset) {
        Preconditions.checkArgument(line>0 && offset>0);
        return atFile(new File(line,offset));
    }

    ErrorLocation atFile(@NonNull File file);

    @Value
    static class File {

        private final int line;
        private final int offset;

    }

}