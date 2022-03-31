package ai.dataeng.sqml.io.sources.impl.file;

import ai.dataeng.sqml.io.sources.DataSource;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.SourceTableConfiguration;
import ai.dataeng.sqml.io.sources.dataset.SourceDataset;
import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import ai.dataeng.sqml.io.sources.formats.FileFormat;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.config.error.ErrorCollector;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;

import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link SourceDataset} that treats all files matching a certain set of extensions in a given directory as {@link SourceTable}.
 *
 */
@Value
@Slf4j
public class FileSource implements DataSource, Serializable {

    private final Name name;
    private final NameCanonicalizer canonicalizer;
    private final FilePath directoryPath;

    private Pattern partPattern;
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
    public Collection<SourceTableConfiguration> discoverTables(ErrorCollector errors) {
        Map<Name, SourceTableConfiguration> tablesByName = new HashMap<>();
        errors = errors.resolve(getDatasetName());
        gatherTables(directoryPath,tablesByName,errors);
        return tablesByName.values();
    }

    private void gatherTables(FilePath directory, Map<Name, SourceTableConfiguration> tablesByName,
                              ErrorCollector errors) {
        try {
            for (FilePath.Status fps : directory.listFiles()) {
                FilePath p = fps.getPath();
                if (fps.isDir()) {
                    gatherTables(p,tablesByName,errors);
                } else {
                    FilePath.NameComponents components = p.getComponents(partPattern);
                    if (FileFormat.validFormat(components.getFormat()) &&
                        Name.validName(components.getName())) {
                        SourceTableConfiguration table = new SourceTableConfiguration(components.getName(),
                                components.getFormat());
                        Name tblName = getCanonicalizer().name(table.getName());
                        SourceTableConfiguration otherTbl = tablesByName.get(tblName);
                        if (otherTbl==null) tablesByName.put(tblName,table);
                        else if (!otherTbl.getFormat().equalsIgnoreCase(table.getFormat())) {
                            errors.warn("Table file [%s] does not have the same format as table [%s]. File will be ignored",p,otherTbl);
                        }

                    }
                }
            }
        } catch (IOException e) {
            errors.fatal("Could not read directory [%s] during dataset refresh: %s",directory,e);
        }
    }

    public Collection<FilePath> getFilesForTable(SourceTableConfiguration tableConfig) throws IOException {
        List<FilePath> files = new ArrayList<>();
        gatherTableFiles(directoryPath,files,tableConfig);
        return files;
    }

    private void gatherTableFiles(FilePath directory, List<FilePath> files, SourceTableConfiguration tableConfig) throws IOException {
        for (FilePath.Status fps : directory.listFiles()) {
            FilePath p = fps.getPath();
            if (fps.isDir()) {
                gatherTableFiles(p,files,tableConfig);
            } else if (isTableFile(p,tableConfig)) {
                files.add(p);
            }
        }
    }

    public boolean isTableFile(FilePath file, SourceTableConfiguration tableConfig) {
        FilePath.NameComponents components = file.getComponents(partPattern);
        return canonicalizer.getCanonical(components.getName()).equals(tableConfig.getIdentifier()) &&
                tableConfig.getFileFormat().matches(components.getFormat());
    }

    @Override
    public boolean update(@NonNull DataSourceConfiguration config, @NonNull ErrorCollector errors) {
        errors.fatal("File data sources currently do not support updates");
        return false;
    }

    @Override
    public FileSourceConfiguration getConfiguration() {
        return configuration;
    }

//    public List<NumberedFile> getTableFiles(FileTableConfiguration table, int partNoOffset) throws IOException {
//        List<NumberedFile> files = new ArrayList<>();
//        Files.list(directoryPath).filter(Files::isRegularFile).forEach(p -> {
//            String filename = p.getFileName().toString();
//            if (FilenameUtils.getExtension(filename).equals(table.extension)) {
//                filename = FilenameUtils.removeExtension(filename);
//                if (!table.multipleParts && Name.of(filename,canonicalizer).equals(table.filePrefix)) {
//                    files.add(new NumberedFile(0,p));
//                } else if (table.multipleParts &&
//                        Name.of(filename.substring(0,table.filePrefix.length()),canonicalizer).equals(table.filePrefix)) {
//                    Matcher matcher = partPattern.matcher(filename);
//                    if (matcher.find()) {
//                        String number = matcher.group(1);
//                        int partNo = Integer.parseInt(number);
//                        files.add(new NumberedFile(partNo,p));
//                    }
//                } //else ignore file
//            }
//
//        });
//        Collections.sort(files);
//        return files;
//    }
//
//    @Value
//    public static class NumberedFile implements Comparable<NumberedFile> {
//
//        private final int number;
//        private final Path file;
//
//        @Override
//        public int compareTo(NumberedFile o) {
//            return Integer.compare(number,o.number);
//        }
//    }
}
