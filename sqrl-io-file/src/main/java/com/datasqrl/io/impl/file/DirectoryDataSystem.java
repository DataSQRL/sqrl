/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.impl.file;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystemConfig;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.DataSystemDiscovery;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.formats.FileFormat;
import com.datasqrl.io.formats.FormatConfiguration;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.name.Name;
import com.datasqrl.name.NameCanonicalizer;
import com.google.common.base.Strings;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 *
 */
@Slf4j
public abstract class DirectoryDataSystem {

  @AllArgsConstructor
  @Getter
  public static class Connector implements DataSystemConnector, Serializable {

    final FilePathConfig pathConfig;
    final Pattern filenamePattern;

    @Override
    public boolean hasSourceTimestamp() {
      return false;
    }

    @Override
    public String getPrefix() {
      return "file";
    }

    public boolean isTableFile(FilePath file, TableConfig tableConfig) {
      Optional<FilePath.NameComponents> componentsOpt = file.getComponents(filenamePattern);
      NameCanonicalizer canonicalizer = tableConfig.getNameCanonicalizer();
      if (componentsOpt.isEmpty()) {
        return false;
      }
      FilePath.NameComponents components = componentsOpt.get();
      if (!canonicalizer.getCanonical(components.getIdentifier())
          .equals(tableConfig.getIdentifier())) {
        return false;
      }
      //If file has a format, it needs to match
      if (Strings.isNullOrEmpty(components.getFormat()) || tableConfig.getFormat() == null) {
        return true;
      } else {
        return tableConfig.getFormat().getFileFormat().matches(components.getFormat());
      }
    }

  }

  public static class Discovery extends DirectoryDataSystem.Connector implements
      DataSystemDiscovery {

    final DirectoryDataSystemConfig.Connector connectorConfig;

    public Discovery(FilePathConfig pathConfig, Pattern filenamePattern,
        DirectoryDataSystemConfig.Connector connectorConfig) {
      super(pathConfig, filenamePattern);
      this.connectorConfig = connectorConfig;
    }

    @Override
    public @NonNull Optional<String> getDefaultName() {
      if (pathConfig.isDirectory()) {
        return Optional.of(pathConfig.getDirectory().getFileName());
      } else {
        return Optional.empty();
      }
    }

    @Override
    public boolean requiresFormat(ExternalDataType type) {
      if (type.isSource()) {
        return false;
      } else {
        return true;
      }
    }

    @Override
    public Collection<TableConfig> discoverSources(DataSystemConfig config, ErrorCollector errors) {
      Map<Name, TableConfig> tablesByName = new HashMap<>();
      gatherTables(pathConfig, tablesByName, config, errors);
      return tablesByName.values();
    }

    private void gatherTables(FilePathConfig pathConfig, Map<Name, TableConfig> tablesByName,
        DataSystemConfig config, ErrorCollector errors) {
      try {
        for (FilePath.Status fps : pathConfig.listFiles()) {
          FilePath p = fps.getPath();
          if (fps.isDir()) {
            gatherTables(FilePathConfig.ofDirectory(p), tablesByName, config, errors);
          } else {
            Optional<FilePath.NameComponents> componentsOpt = p.getComponents(filenamePattern);
            if (componentsOpt.map(c -> Name.validName(c.getIdentifier())).orElse(false)) {
              FilePath.NameComponents components = componentsOpt.get();
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
              tblBuilder.identifier(components.getIdentifier());
              String name = components.getIdentifier();
              if (Strings.isNullOrEmpty(name)) {
                name = getDefaultName().get();
              }
              tblBuilder.name(name);
              tblBuilder.connector(connectorConfig);
              //infer format if not completely specified
              format.initialize(new InputPreview(tblBuilder.build()), errors.resolve("format"));
              tblBuilder.format(format);
              TableConfig table = tblBuilder.build();

              Name tblName = config.getNameCanonicalizer().name(components.getIdentifier());
              TableConfig otherTbl = tablesByName.get(tblName);
              if (otherTbl == null) {
                tablesByName.put(tblName, table);
              } else if (!otherTbl.getFormat().getFileFormat()
                  .equals(table.getFormat().getFileFormat())) {
                errors.warn("Table file [%s] does not have the same format [%s] of previously " +
                        "encountered table [%s]. File will be ignored",
                    p, otherTbl.getFormat().getFileFormat(), otherTbl.getIdentifier());
              }

            }
          }
        }
      } catch (IOException e) {
        errors.fatal("Could not read directory [%s] during discovery: %s", pathConfig, e);
      }
    }

    public Collection<FilePath> getFilesForTable(TableConfig tableConfig) throws IOException {
      List<FilePath> files = new ArrayList<>();
      gatherTableFiles(pathConfig, files, tableConfig);
      return files;
    }

    private void gatherTableFiles(FilePathConfig pathConfig, List<FilePath> files,
        TableConfig tableConfig) throws IOException {
      for (FilePath.Status fps : pathConfig.listFiles()) {
        FilePath p = fps.getPath();
        if (fps.isDir()) {
          gatherTableFiles(FilePathConfig.ofDirectory(p), files, tableConfig);
        } else if (isTableFile(p, tableConfig)) {
          files.add(p);
        }
      }
    }

    @Override
    public Optional<TableConfig> discoverSink(@NonNull Name sinkName,
        @NonNull DataSystemConfig config, @NonNull ErrorCollector errors) {
      TableConfig.TableConfigBuilder tblBuilder = TableConfig.copy(config);
      tblBuilder.type(ExternalDataType.sink);
      tblBuilder.identifier(sinkName.getCanonical());
      tblBuilder.name(sinkName.getDisplay());
      tblBuilder.connector(connectorConfig);
      return Optional.of(tblBuilder.build());
    }

    @Value
    private class InputPreview implements com.datasqrl.io.impl.InputPreview {

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
          log.error("Could not read file [{}]: {}", fp, e);
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
