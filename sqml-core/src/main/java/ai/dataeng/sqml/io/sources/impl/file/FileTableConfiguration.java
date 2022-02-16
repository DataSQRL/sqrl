package ai.dataeng.sqml.io.sources.impl.file;

import ai.dataeng.sqml.io.sources.SourceTableConfiguration;
import ai.dataeng.sqml.tree.name.Name;
import com.google.common.base.Preconditions;

import lombok.*;
import org.apache.commons.lang3.StringUtils;

@NoArgsConstructor
@EqualsAndHashCode
@ToString
@Getter
public class FileTableConfiguration implements SourceTableConfiguration {

    @NonNull String filePrefix;
    @NonNull String extension;
    @NonNull FileType fileType;
             boolean multipleParts;
    @NonNull Name name;

    public FileTableConfiguration(@NonNull String filePrefix, @NonNull String extension, @NonNull FileType fileType,
                                  boolean multipleParts, @NonNull Name name) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(filePrefix));
        Preconditions.checkArgument(StringUtils.isNotEmpty(extension));
        Preconditions.checkNotNull(extension);
        this.filePrefix = filePrefix;
        this.extension = extension;
        this.fileType = fileType;
        this.multipleParts = multipleParts;
        this.name = name;
    }


    @Override
    public @NonNull Name getTableName() {
        Preconditions.checkArgument(name !=null,"Table config has not been initialized");
        return name;
    }

    @Override
    public boolean isCompatible(SourceTableConfiguration otherConfig) {
        Preconditions.checkArgument((otherConfig instanceof FileTableConfiguration) && otherConfig.getTableName().equals(name));
        FileTableConfiguration otherTbl = (FileTableConfiguration) otherConfig;
        if (!filePrefix.equals(otherTbl.filePrefix)) return false;
        if (!extension.equals(otherTbl.extension)) return false;
        if (!fileType.equals(otherTbl.fileType)) return false;
        if (multipleParts!=otherTbl.multipleParts) return false;

        return true;
    }

}
