package ai.datasqrl.io.impl.file;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.formats.FileFormat;
import ai.datasqrl.io.formats.FormatConfiguration;
import ai.datasqrl.io.sources.DataSystemConfig;
import ai.datasqrl.io.sources.DataSystemConnector;
import ai.datasqrl.io.sources.DataSystemDiscovery;
import ai.datasqrl.io.sources.dataset.TableConfig;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NameCanonicalizer;
import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 */
@Slf4j
public abstract class DirectoryDataSystem implements DataSystemConnector, Serializable {

  @AllArgsConstructor
  @Getter
  public static class Connector implements DataSystemConnector, Serializable {

    final FilePath path;
    final Pattern partPattern;

    @Override
    public boolean hasSourceTimestamp() {
      return false;
    }

    public boolean isTableFile(FilePath file, TableConfig tableConfig) {
      FilePath.NameComponents components = file.getComponents(partPattern);
      NameCanonicalizer canonicalizer = tableConfig.getNameCanonicalizer();
      if (!canonicalizer.getCanonical(components.getName()).equals(tableConfig.getIdentifier())) {
        return false;
      }
      //If file has a format, it needs to match
      if (Strings.isNullOrEmpty(components.getFormat()) || tableConfig.getFormat()==null) {
        return true;
      } else {
        return tableConfig.getFormat().getFileFormat().matches(components.getFormat());
      }
    }

  }

  public static class Discovery extends DirectoryDataSystem.Connector implements DataSystemDiscovery {

    final DirectoryDataSystemConfig.Connector connectorConfig;

    public Discovery(FilePath path, Pattern partPattern, DirectoryDataSystemConfig.Connector connectorConfig) {
      super(path, partPattern);
      this.connectorConfig = connectorConfig;
    }

    @Override
    public @NonNull Optional<String> getDefaultName() {
      return Optional.of(path.getFileName());
    }

    @Override
    public boolean requiresFormat() {
      return false;
    }

    @Override
    public Collection<TableConfig> discoverTables(DataSystemConfig config, ErrorCollector errors) {
      Map<Name, TableConfig> tablesByName = new HashMap<>();
      gatherTables(path, tablesByName, config, errors);
      return tablesByName.values();
    }

    private void gatherTables(FilePath directory, Map<Name, TableConfig> tablesByName,
                              DataSystemConfig config, ErrorCollector errors) {
      try {
        for (FilePath.Status fps : directory.listFiles()) {
          FilePath p = fps.getPath();
          if (fps.isDir()) {
            gatherTables(p, tablesByName, config, errors);
          } else {
            FilePath.NameComponents components = p.getComponents(partPattern);
            if (Name.validName(components.getName())) {
              FormatConfiguration format = config.getFormat();
              FileFormat ff = FileFormat.getFormat(components.getFormat());
              if (format == null && ff != null) {
                format = ff.getImplementation().getDefaultConfiguration();
              } else if (format == null) {
                continue; //Unrecognized format
              }
              if (ff != null && !format.getFileFormat().equals(ff)) {
                errors.warn("File [%s] does not match configured format [%s]", p,
                        format.getFileFormat());
                continue;
              }
              TableConfig.TableConfigBuilder tblBuilder = TableConfig.copy(config);
              tblBuilder.identifier(components.getName());
              tblBuilder.name(components.getName());
              tblBuilder.datasource(connectorConfig);
              //infer format if not completely specified
              format.initialize(new InputPreview(tblBuilder.build()), errors.resolve("format"));
              tblBuilder.format(format);
              TableConfig table = tblBuilder.build();

              Name tblName = config.getNameCanonicalizer().name(components.getName());
              TableConfig otherTbl = tablesByName.get(tblName);
              if (otherTbl == null) {
                tablesByName.put(tblName, table);
              } else if (!otherTbl.getFormat().getFileFormat().equals(table.getFormat().getFileFormat())) {
                errors.warn("Table file [%s] does not have the same format [%s] of previously " +
                                "encountered table [%s]. File will be ignored",
                        p, otherTbl.getFormat().getFileFormat(), otherTbl.getIdentifier());
              }

            }
          }
        }
      } catch (IOException e) {
        errors.fatal("Could not read directory [%s] during dataset refresh: %s", directory, e);
      }
    }

    public Collection<FilePath> getFilesForTable(TableConfig tableConfig) throws IOException {
      List<FilePath> files = new ArrayList<>();
      gatherTableFiles(path, files, tableConfig);
      return files;
    }

    private void gatherTableFiles(FilePath directory, List<FilePath> files,
                                  TableConfig tableConfig) throws IOException {
      for (FilePath.Status fps : directory.listFiles()) {
        FilePath p = fps.getPath();
        if (fps.isDir()) {
          gatherTableFiles(p, files, tableConfig);
        } else if (isTableFile(p, tableConfig)) {
          files.add(p);
        }
      }
    }

    @Value
    private class InputPreview implements ai.datasqrl.io.impl.InputPreview {

      final TableConfig table;

      @Override
      public Stream<BufferedReader> getTextPreview() {
        Collection<FilePath> files = Collections.EMPTY_LIST;
        try {
          files = getFilesForTable(table);
        } catch (IOException e) {
          log.error("Could not preview files for table [%s]: %s", table, e);
        }
        return files.stream().map(fp -> getBufferedReader(fp, table))
                .filter(r -> r != null);
      }

      private BufferedReader getBufferedReader(FilePath fp, TableConfig config) {
        InputStream in = null;
        BufferedReader r = null;
        try {
          in = fp.read();
          r = new BufferedReader(new InputStreamReader(in, config.getCharsetObject()));
          return r;
        } catch (IOException e) {
          log.error("Could not read file [%s]: %s", fp, e);
          try {
            if (in != null) {
              in.close();
            }
            if (r != null) {
              r.close();
            }
          } catch (Exception ex) {
          }
          return null;
        }
      }

    }

  }

}
