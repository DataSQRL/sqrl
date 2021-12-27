package ai.dataeng.sqml.importer.source.simplefile;

import ai.dataeng.sqml.execution.flink.ingest.DatasetRegistration;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.execution.flink.ingest.source.SourceDataset;
import ai.dataeng.sqml.execution.flink.ingest.source.SourceTable;
import ai.dataeng.sqml.execution.flink.ingest.source.SourceTableListener;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@link SourceDataset} that treats all files matching a certain set of extensions in a given directory as {@link SourceTable}.
 *
 * TODO: This dataset currently does not watch the directory for new files to add as {@link SourceTable}
 * to the registered {@link SourceTableListener}. It only looks for files when it is first created.
 */
public class DirectoryDataset implements SourceDataset {

    public static final String[] FILE_EXTENSIONS = {"csv", "json"};

    private final Path directory;
    private final DatasetRegistration registration;
    private final Map<Name,FileTable> tableFiles;

    private final Set<SourceTableListener> listeners = new HashSet<>();

    public DirectoryDataset(DatasetRegistration registration, Path directory) {
        Preconditions.checkArgument(Files.exists(directory) && Files.isDirectory(directory) && Files.isReadable(directory),
                "Not a readable directory: %s", directory);
        this.registration = registration;
        this.directory = directory;
        try {
            tableFiles = Files.list(directory).filter(f -> FileTable.supportedFile(f))
                        .map(f -> new FileTable(this,f)).collect(Collectors.toMap(
                                t->t.getName(), Function.identity()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addSourceTableListener(SourceTableListener listener) {
        final boolean newListener;
        synchronized (listeners) {
            newListener = listeners.add(listener);
        }
        if (newListener) {
            tableFiles.forEach((k,v) -> listener.registerSourceTable(v));
        }
    }


    @Override
    public Collection<? extends SourceTable> getTables() {
        return tableFiles.values();
    }

    @Override
    public SourceTable getTable(Name name) {
        return tableFiles.get(name);
    }

    @Override
    public DatasetRegistration getRegistration() {
        return registration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DirectoryDataset that = (DirectoryDataset) o;
        return directory.equals(that.directory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(directory);
    }

    @Override
    public String toString() {
        return "DirectoryDataset{" +
                "directory=" + directory +
                '}';
    }
}
