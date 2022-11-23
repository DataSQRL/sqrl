package ai.datasqrl.config.metadata;

import ai.datasqrl.config.engines.FileDatabaseConfiguration;
import ai.datasqrl.config.provider.DatabaseConnectionProvider;
import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.config.provider.MetadataStoreProvider;
import ai.datasqrl.config.provider.SerializerProvider;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NonNull;

public class FileMetadataStore implements MetadataStore {

  public static final String DEFAULT_EXTENSION = ".dat";

  private final Path basePath;
  private final String fileExtension = DEFAULT_EXTENSION;
  private final Kryo kryo;


  public FileMetadataStore(Path basePath, Kryo kryo) {
    Preconditions.checkArgument(Files.isDirectory(basePath) && Files.isWritable(basePath));
    this.basePath = basePath;
    this.kryo = kryo;
  }

  @Override
  public void close() {
    //Nothing to close
  }

  @Override
  public <T> void put(T value, String firstKey, String... moreKeys) {
    Path file = getFile(firstKey, moreKeys);
//        System.out.println("Writing to: " + file.toString());
    if (Files.notExists(file.getParent())) {
      try {
        Files.createDirectories(file.getParent());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    try (OutputStream outstream = Files.newOutputStream(file, StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING);
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
      if (Files.notExists(file) || Files.size(file) == 0) {
        return null;
      }
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

  @Override
  public boolean remove(@NonNull String firstKey, String... moreKeys) {
    Path file = getFile(firstKey, moreKeys);
    try {
      return Files.deleteIfExists(file);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Set<String> getSubKeys(String... keys) {
    Path keyPath = basePath;
    if (keys != null && keys.length > 0) {
      keyPath = keyPath.resolve(basePath.getFileSystem().getPath(keys[0],
          Arrays.copyOfRange(keys, 1, keys.length)));
    }
    Set<String> result;
    try (Stream<Path> filesInDir = Files.list(keyPath)) {
      result = filesInDir.filter(p ->
          Files.isDirectory(p) || (Files.isRegularFile(p) &&
              com.google.common.io.Files.getFileExtension(getFileName(p))
                  .equalsIgnoreCase(fileExtension))
      ).map(p -> {
        if (Files.isDirectory(p)) {
          return getFileName(p);
        } else {
          //it's a file -> remove fileExtension
          String filename = getFileName(p);
          return filename.substring(0, filename.length() - fileExtension.length());
        }
      }).collect(Collectors.toSet());
    } catch (IOException e) {
      return Collections.EMPTY_SET;
    }
    return result;
  }

  private static String getFileName(Path path) {
    return path.getFileName().toString();
  }

  private Path getFile(String firstKey, String... moreKeys) {
    Path keyPath;
    if (moreKeys != null && moreKeys.length > 0) {
      String[] copyKeys = Arrays.copyOf(moreKeys, moreKeys.length);
      copyKeys[copyKeys.length - 1] = copyKeys[copyKeys.length - 1] + fileExtension;
      keyPath = basePath.resolve(basePath.getFileSystem().getPath(firstKey, copyKeys));
    } else {
      keyPath = basePath.resolve(firstKey + fileExtension);
    }
    return keyPath;
  }

  public static class Provider implements MetadataStoreProvider {

    @Override
    public MetadataStore openStore(DatabaseConnectionProvider dbConnection, SerializerProvider serializer) {
      Preconditions.checkArgument(dbConnection instanceof FileDatabaseConfiguration.ConnectionProvider);
      FileDatabaseConfiguration.ConnectionProvider conn = (FileDatabaseConfiguration.ConnectionProvider)dbConnection;
      Path basePath = conn.getDirectory();
      if (Files.notExists(basePath)) {
        try {
          Files.createDirectories(basePath);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return new FileMetadataStore(basePath, serializer.getSerializer());
    }

  }

}
