package ai.dataeng.sqml.io.sources.impl.file;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.io.sources.DataSource;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.SourceTableConfiguration;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.io.sources.dataset.SourceDataset;
import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.h2.util.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A {@link SourceDataset} that treats all files matching a certain set of extensions in a given directory as {@link SourceTable}.
 *
 */
@Value
@Slf4j
public class FileSource implements DataSource {

    private final Name name;
    private final NameCanonicalizer canonicalizer;
    private final Path directoryPath;
    private final Pattern partPattern;
    private final FileSourceConfiguration configuration;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileSource that = (FileSource) o;
        return directoryPath.equals(that.directoryPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(directoryPath);
    }

    @Override
    public String toString() {
        return "DirectoryDataset{" +
                "directory=" + directoryPath +
                "name=" + name +
                '}';
    }


    @Override
    public @NonNull Name getDatasetName() {
        Preconditions.checkArgument(name !=null,"Configuration has not been initialized");
        return name;
    }

    @Override
    public @NonNull NameCanonicalizer getCanonicalizer() {
        Preconditions.checkArgument(canonicalizer !=null,"Configuration has not been initialized");
        return canonicalizer;
    }

    @Override
    public Collection<? extends SourceTableConfiguration> pollTables(long maxWait, TimeUnit timeUnit) throws InterruptedException {
        ProcessMessage.ProcessBundle<ConfigurationError> errors = new ProcessMessage.ProcessBundle<>();
        Map<Name, FileTableConfiguration> tablesByName = new HashMap<>();
        try {
            Files.list(directoryPath).filter(this::supportedFile).forEach(p -> {
                FileTableConfiguration table = getTable(p,errors);
                if (table!=null) {
                    FileTableConfiguration rep = tablesByName.get(table.getTableName());
                    if (rep==null) tablesByName.put(table.getTableName(),table);
                    else if (rep.isCompatible(table)) {
                        errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE, name.toString(),
                                "Table file [%s] is incompatible with same-name table [%s]",p,rep));
                    }
                }
            });
        } catch (IOException e) {
            errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE, name.toString(),
                    "Could not read directory [%s] during dataset refresh: %s",directoryPath,e));
        }

        ProcessMessage.ProcessBundle.logMessages(errors);
        return tablesByName.values();
    }

    private static final String getExtension(Path p) {
        return FilenameUtils.getExtension(p.getFileName().toString());
    }

    private boolean supportedFile(Path p) {
        return Files.isRegularFile(p) && FileFormat.validExtension(getExtension(p));
    }

    private FileTableConfiguration getTable(Path p, ProcessMessage.ProcessBundle<ConfigurationError> errors) {
        Preconditions.checkArgument(supportedFile(p));
        String absBasePath = p.getParent().toAbsolutePath().toString();
        String fileName = p.getFileName().toString();
        String extension = FilenameUtils.getExtension(fileName);
        fileName = FilenameUtils.removeExtension(fileName);
        //Match pattern
        boolean multipleParts = false;
        Matcher matcher = partPattern.matcher(fileName);
        if (matcher.find()) {
            String part = matcher.group(0);
            String number = matcher.group(1);
            int partNo;
            try {
                partNo = Integer.parseInt(number);
            } catch (NumberFormatException e) {
                partNo = -1;
            }
            if (partNo<0) {
                errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE, name.toString(),
                        "Identified part number [%s] of file [%s] is not a valid integer",number,p));
                return null;
            }
            multipleParts = true;
            //Remove part which is guaranteed to be at the end
            fileName = fileName.substring(0,fileName.length()-part.length());
        }
        if (StringUtils.isNullOrEmpty(fileName.trim())) {
            errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE, name.toString(),
                    "File name [%s] is not valid after removing extension and part number",p));
            return null;
        }
        FileFormat fileFormat = FileFormat.getFileTypeFromExtension(extension);
        if (fileFormat ==null) {
            errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE, name.toString(),
                    "File name [%s] does not have a valid extension",p));
            return null;
        }
        Name tableName = Name.of(fileName, canonicalizer); //tableName and prefix are identical in this case
        return new FileTableConfiguration(tableName, absBasePath, extension, fileFormat,multipleParts, tableName);
    }


    @Override
    public boolean isCompatible(@NonNull DataSource other) {
        Preconditions.checkArgument((other instanceof FileSource) && other.getDatasetName().equals(name));
        FileSource otherDir = (FileSource) other;

        if (!directoryPath.equals(otherDir.directoryPath)) return false;
        if (!canonicalizer.equals(otherDir.canonicalizer)) return false;

        return true;
    }

    @Override
    public DataSourceConfiguration getConfiguration() {
        return configuration;
    }

    public List<NumberedFile> getTableFiles(FileTableConfiguration table, int partNoOffset) throws IOException {
        List<NumberedFile> files = new ArrayList<>();
        Files.list(directoryPath).filter(Files::isRegularFile).forEach(p -> {
            String filename = p.getFileName().toString();
            if (FilenameUtils.getExtension(filename).equals(table.extension)) {
                filename = FilenameUtils.removeExtension(filename);
                if (!table.multipleParts && Name.of(filename,canonicalizer).equals(table.filePrefix)) {
                    files.add(new NumberedFile(0,p));
                } else if (table.multipleParts &&
                        Name.of(filename.substring(0,table.filePrefix.length()),canonicalizer).equals(table.filePrefix)) {
                    Matcher matcher = partPattern.matcher(filename);
                    if (matcher.find()) {
                        String number = matcher.group(1);
                        int partNo = Integer.parseInt(number);
                        files.add(new NumberedFile(partNo,p));
                    }
                } //else ignore file
            }

        });
        Collections.sort(files);
        return files;
    }

    @Value
    public static class NumberedFile implements Comparable<NumberedFile> {

        private final int number;
        private final Path file;

        @Override
        public int compareTo(NumberedFile o) {
            return Integer.compare(number,o.number);
        }
    }
}
