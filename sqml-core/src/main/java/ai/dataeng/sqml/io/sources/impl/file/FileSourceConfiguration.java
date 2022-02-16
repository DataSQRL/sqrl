package ai.dataeng.sqml.io.sources.impl.file;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.SourceTableConfiguration;
import ai.dataeng.sqml.io.sources.impl.CanonicalizerConfiguration;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.io.sources.dataset.SourceDataset;
import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import com.google.common.base.Preconditions;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.h2.util.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A {@link SourceDataset} that treats all files matching a certain set of extensions in a given directory as {@link SourceTable}.
 *
 */
@NoArgsConstructor
@Slf4j
public class FileSourceConfiguration implements DataSourceConfiguration {

    public static final String[] FILE_EXTENSIONS = {"csv", "json"};
    public static final String DEFAULT_PATTERN = "_(\\d+)";

    private String name;
    private CanonicalizerConfiguration canonicalizer;
    @NonNull
    private String path;
    private String pattern;

    private transient Name n = null;
    private transient NameCanonicalizer canon = null;
    private transient Path directoryPath = null;
    private transient Pattern partPattern = null;

    public FileSourceConfiguration(String name, String path) {
        this.name = name;
        this.path = path;
        this.canonicalizer = null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileSourceConfiguration that = (FileSourceConfiguration) o;
        return directoryPath.equals(that.directoryPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(directoryPath);
    }

    @Override
    public String toString() {
        return "DirectoryDataset{" +
                "directory=" + path +
                "name=" + name +
                '}';
    }

    @Override
    public ProcessMessage.ProcessBundle<ConfigurationError> validateAndInitialize() {
        ProcessMessage.ProcessBundle<ConfigurationError> errors = new ProcessMessage.ProcessBundle<>();
        canon = CanonicalizerConfiguration.get(canonicalizer);

        String locationName = name!=null?name:path;
        try {
            directoryPath = Path.of(path);
        } catch (InvalidPathException e) {
            errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE,locationName,"Path is invalid: %s", directoryPath));
        }
        if (!Files.exists(directoryPath) || !Files.isDirectory(directoryPath)) {
            errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE,locationName,"Path is invalid: %s", directoryPath));
        }
        if (!Files.isReadable(directoryPath)) {
            errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE,locationName,"Directory cannot be read: %s", directoryPath));
        }

        n = Name.of(StringUtils.isNullOrEmpty(name)?directoryPath.getFileName().toString():name,canon);

        partPattern = Pattern.compile((pattern!=null?pattern:DEFAULT_PATTERN)+"$");
        if (pattern==null) pattern = DEFAULT_PATTERN;
        return errors;
    }

    @Override
    public @NonNull Name getDatasetName() {
        Preconditions.checkArgument(n!=null,"Configuration has not been initialized");
        return n;
    }

    @Override
    public @NonNull NameCanonicalizer getCanonicalizer() {
        Preconditions.checkArgument(canon!=null,"Configuration has not been initialized");
        return canon;
    }

    @Override
    public Collection<? extends SourceTableConfiguration> pollTables(long maxWait, TimeUnit timeUnit) throws InterruptedException {
        ProcessMessage.ProcessBundle<ConfigurationError> errors = new ProcessMessage.ProcessBundle<>();
        Map<Name, FileTableConfiguration> tablesByName = null;
        try {
            Files.list(directoryPath).filter(this::supportedFile).forEach(p -> {
                FileTableConfiguration table = getTable(p,errors);
                if (table!=null) {
                    FileTableConfiguration rep = tablesByName.get(table.getTableName());
                    if (rep==null) tablesByName.put(table.getTableName(),table);
                    else if (rep.isCompatible(table)) {
                        errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE,n.toString(),
                                "Table file [%s] is incompatible with same-name table [%s]",p,rep));
                    }
                }
            });
        } catch (IOException e) {
            errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE,n.toString(),
                    "Could not read directory [%s] during dataset refresh: %s",directoryPath,e));
        }

        ProcessMessage.ProcessBundle.logMessages(errors);
        return tablesByName.values();
    }

    private static final String getExtension(Path p) {
        return FilenameUtils.getExtension(p.getFileName().toString());
    }

    private boolean supportedFile(Path p) {
        return Files.isRegularFile(p) && FileType.validExtension(getExtension(p));
    }

    private FileTableConfiguration getTable(Path p, ProcessMessage.ProcessBundle<ConfigurationError> errors) {
        Preconditions.checkArgument(supportedFile(p));
        String fileName = p.getFileName().toString();
        String extension = FilenameUtils.getExtension(fileName);
        fileName = FilenameUtils.removeExtension(fileName);
        fileName = fileName.substring(0,FilenameUtils.indexOfExtension(fileName));
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
                errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE,n.toString(),
                        "Identified part number [%s] of file [%s] is not a valid integer",number,p));
                return null;
            }
            multipleParts = true;
            //Remove part which is guaranteed to be at the end
            fileName = fileName.substring(0,fileName.length()-part.length());
        }
        if (StringUtils.isNullOrEmpty(fileName.trim())) {
            errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE,n.toString(),
                    "File name [%s] is not valid after removing extension and part number",p));
            return null;
        }
        FileType fileType = FileType.getFileTypeFromExtension(extension);
        if (fileType==null) {
            errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SOURCE,n.toString(),
                    "File name [%s] does not have a valid extension",p));
            return null;
        }
        return new FileTableConfiguration(fileName,extension,fileType,multipleParts,Name.of(fileName,canon));
    }


    @Override
    public boolean isCompatible(@NonNull DataSourceConfiguration other) {
        Preconditions.checkArgument((other instanceof FileSourceConfiguration) && other.getDatasetName().equals(n));
        FileSourceConfiguration otherDir = (FileSourceConfiguration) other;

        if (!directoryPath.equals(otherDir.directoryPath)) return false;
        if (!canon.equals(otherDir.canon)) return false;

        return true;
    }

    public List<NumberedFile> getTableFiles(FileTableConfiguration table, int partNoOffset) throws IOException {
        List<NumberedFile> files = new ArrayList<>();
        Files.list(directoryPath).filter(Files::isRegularFile).forEach(p -> {
            String filename = p.getFileName().toString();
            if (FilenameUtils.getExtension(filename).equals(table.extension)) {
                filename = FilenameUtils.removeExtension(filename);
                if (!table.multipleParts && filename.equals(table.filePrefix)) {
                    files.add(new NumberedFile(0,p));
                } else if (table.multipleParts && filename.startsWith(table.filePrefix)) {
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
