package ai.dataeng.sqml.io.sources.impl.file;

import ai.dataeng.sqml.io.sources.SourceTableConfiguration;
import ai.dataeng.sqml.tree.name.Name;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

@NoArgsConstructor
@EqualsAndHashCode
@ToString
@Getter
public class FileTableConfiguration implements SourceTableConfiguration {

    @NonNull Name filePrefix;
    @NonNull String absoluteBasePath;
    @NonNull String extension;
    @NonNull FileFormat fileFormat;
             boolean multipleParts;
    @NonNull Name name;

    public FileTableConfiguration(@NonNull Name filePrefix, @NonNull String absoluteBasePath,
                                  @NonNull String extension, @NonNull FileFormat fileFormat,
                                  boolean multipleParts, @NonNull Name name) {
        Preconditions.checkArgument(filePrefix!=null);
        Preconditions.checkArgument(StringUtils.isNotEmpty(absoluteBasePath));
        Preconditions.checkArgument(StringUtils.isNotEmpty(extension));
        Preconditions.checkNotNull(extension);
        this.filePrefix = filePrefix;
        this.absoluteBasePath = absoluteBasePath;
        this.extension = extension;
        this.fileFormat = fileFormat;
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
        if (!absoluteBasePath.equals(otherTbl.absoluteBasePath)) return false;
        if (!extension.equals(otherTbl.extension)) return false;
        if (!fileFormat.equals(otherTbl.fileFormat)) return false;
        if (multipleParts!=otherTbl.multipleParts) return false;

        return true;
    }

}
