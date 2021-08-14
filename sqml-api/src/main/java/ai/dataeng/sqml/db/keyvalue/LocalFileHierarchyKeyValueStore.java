package ai.dataeng.sqml.db.keyvalue;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public class LocalFileHierarchyKeyValueStore implements HierarchyKeyValueStore {

    public static final String DEFAULT_EXTENSION = ".dat";

    private final Path basePath;
    private final String fileExtension = DEFAULT_EXTENSION;
    private final Kryo kryo = new Kryo();


    public LocalFileHierarchyKeyValueStore(Path basePath) {
        Preconditions.checkArgument(Files.isDirectory(basePath) && Files.isWritable(basePath));
        this.basePath = basePath;
    }

    @Override
    public void close() {
        //Nothing to close
    }

    @Override
    public <T> void put(T value, String firstKey, String... moreKeys) {
        Path file = getFile(firstKey, moreKeys);
        System.out.println("Writing to: " + file.toString());
        if (Files.notExists(file.getParent())) {
            try {
                Files.createDirectories(file.getParent());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        try (OutputStream outstream = Files.newOutputStream(file, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                Output out = new Output(outstream)) {
            kryo.writeObject(out, value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T get(Class<T> clazz, String firstKey, String... moreKeys) {
        Path file = getFile(firstKey, moreKeys);
        try {
            if (Files.notExists(file) || Files.size(file)==0) return null;
        } catch (IOException e) {
            return null;
        }
        try (InputStream instream = Files.newInputStream(file);
             Input in = new Input(instream)) {
            return kryo.readObject(in, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Path getFile(String firstKey, String... moreKeys) {
        Path keyPath;
        if (moreKeys!=null && moreKeys.length>0) {
            String[] copyKeys = Arrays.copyOf(moreKeys,moreKeys.length);
            copyKeys[copyKeys.length-1] = copyKeys[copyKeys.length-1] + fileExtension;
            keyPath = basePath.resolve(basePath.getFileSystem().getPath(firstKey, copyKeys));
        } else {
            keyPath = basePath.resolve(firstKey + fileExtension);
        }
        return keyPath;
    }

    public static class Factory implements HierarchyKeyValueStore.Factory<LocalFileHierarchyKeyValueStore> {

        private String baseDir;

        public Factory(String baseDir) {
            this.baseDir = baseDir;
        }

        @Override
        public LocalFileHierarchyKeyValueStore open() {
            Path basePath = Path.of(baseDir);
            if (Files.notExists(basePath)) {
                try {
                    Files.createDirectories(basePath);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return new LocalFileHierarchyKeyValueStore(basePath);
        }
    }

}
