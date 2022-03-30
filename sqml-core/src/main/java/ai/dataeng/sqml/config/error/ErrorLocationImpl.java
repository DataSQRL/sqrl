package ai.dataeng.sqml.config.error;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import lombok.Value;

import java.util.Arrays;

@Value
@JsonSerialize(as = ErrorLocation.class)
class ErrorLocationImpl implements ErrorLocation {

    private final String prefix;
    private final String[] names;
    private final File file;

    private ErrorLocationImpl(String prefix, File file, @NonNull String... names) {
        this.prefix = prefix;
        this.names = names;
        this.file = file;
    }

    public static ErrorLocation of(String prefix, @NonNull String loc) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(loc), "Invalid location provided");
        return new ErrorLocationImpl(prefix,null, new String[]{loc});
    }

    public static ErrorLocation of(String prefix, @NonNull ErrorLocation other) {
        Preconditions.checkArgument(!other.hasPrefix());
        return new ErrorLocationImpl(prefix,other.getFile(),other.getPathArray());
    }

    public static ErrorLocation of(String prefix, @NonNull File file) {
        return new ErrorLocationImpl(prefix,file,new String[]{});
    }

    @Override
    public ErrorLocation append(@NonNull ErrorLocation other) {
        Preconditions.checkArgument(!other.hasPrefix() && !this.hasFile());
        String[] othNames = other.getPathArray();
        String[] newnames = Arrays.copyOf(names, names.length + othNames.length);
        System.arraycopy(othNames,0,newnames,names.length,othNames.length);
        return new ErrorLocationImpl(this.prefix,other.getFile(),newnames);
    }

    @Override
    public @NonNull String[] getPathArray() {
        return names;
    }


    public ErrorLocation resolve(@NonNull String loc) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(loc), "Invalid location provided");
        String[] newnames = Arrays.copyOf(names, names.length + 1);
        newnames[names.length] = loc;
        return new ErrorLocationImpl(prefix, file, newnames);
    }

    @Override
    public ErrorLocation atFile(@NonNull File file) {
        Preconditions.checkArgument(!hasFile());
        return new ErrorLocationImpl(prefix,file,names);
    }

    @Override
    public String getPath() {
        if (names==null || names.length == 0) return "";
        return String.join("/",names);
    }

    @Override
    public String toString() {
        String result = getPath();
        if (prefix!=null) {
            if (Strings.isNullOrEmpty(result)) result = prefix;
            else result = prefix + "/" + result;
        }
        if (file!=null) {
            result += "@" + file.getLine() + ":" + file.getOffset();
        }
        return result;
    }

}
