package ai.dataeng.sqml.source.simplefile;

import ai.dataeng.sqml.source.SourceDataset;
import ai.dataeng.sqml.source.SourceTable;
import ai.dataeng.sqml.source.SourceTableListener;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@link SourceDataset} that treats all files matching a certain set of extensions in a given directory as {@link ai.dataeng.sqml.source.SourceTable}.
 *
 * TODO: This dataset currently does not watch the directory for new files to add as {@link ai.dataeng.sqml.source.SourceTable}
 * to the registered {@link SourceTableListener}. It only looks for files when it is first created.
 */
public class DirectoryDataset implements SourceDataset {

    public static final String[] FILE_EXTENSIONS = {"csv", "json"};

    private final Path directory;
    private final String name;

    private final Map<String,FileTable> tableFiles;

    private final Set<SourceTableListener> listeners = new HashSet<>();

    public DirectoryDataset(Path directory, String name) {
        Preconditions.checkArgument(Files.exists(directory) && Files.isDirectory(directory) && Files.isReadable(directory),
                "Not a readable directory: %s", directory);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Not a valid name: %s", name);
        this.directory = directory;
        this.name = name;
        try {
            tableFiles = Files.list(directory).filter(f -> FileTable.supportedFile(f))
                        .map(f -> new FileTable(this,f)).collect(Collectors.toMap(FileTable::getName, Function.identity()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public DirectoryDataset(Path directory) {
        this(directory, directory.getFileName().toString());
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
    public String getName() {
        return name;
    }

    @Override
    public Collection<? extends SourceTable> getTables() {
        return tableFiles.values();
    }

    @Override
    public SourceTable getTable(String name) {
        return tableFiles.get(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DirectoryDataset that = (DirectoryDataset) o;
        return directory.equals(that.directory) && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(directory, name);
    }

    @Override
    public String toString() {
        return "DirectoryDataset{" +
                "directory=" + directory +
                ", name='" + name + '\'' +
                '}';
    }
}
