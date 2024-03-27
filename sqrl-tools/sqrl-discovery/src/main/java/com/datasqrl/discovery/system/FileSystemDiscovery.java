package com.datasqrl.discovery.system;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.discovery.DataDiscoveryConfig;
import com.datasqrl.discovery.TablePattern;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.formats.Format;
import com.datasqrl.io.file.FilePath;
import com.datasqrl.io.file.FilePath.NameComponents;
import com.datasqrl.io.tables.ConnectorFactory;
import com.datasqrl.io.tables.TableConfig;
import com.google.auto.service.AutoService;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;


@AutoService(DataSystemDiscovery.class)
@Slf4j
public class FileSystemDiscovery implements DataSystemDiscovery {

  public static final String DEFAULT_TABLE_PATTERN = "([^\\.]+?)(?:_part.*)?";

  public static final String TYPE = "filesystem";

  private final FileTableConfigFactory tableConfigFactory = new FileTableConfigFactory();

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public boolean matchesArgument(String systemConfig) {
    //Check if argument is a local directory
    try {
      if (Files.isDirectory(Path.of(systemConfig))) {
        return true;
      }
    } catch (Throwable ignored) {}
    return false;
  }

  @Override
  public Collection<TableConfig> discoverTables(@NonNull DataDiscoveryConfig discoveryConfig, @NonNull
      String configFile) {
    FilePath path;
    if (matchesArgument(configFile)) {
      path = FilePath.fromJavaPath(Path.of(configFile));
    } else {
      path = new FilePath(configFile);
    }
    FilePathConfig pathConfig = FilePathConfig.ofDirectory(path);

    TableFinder tblFinder = new TableFinder(path, discoveryConfig.getErrors(),
        discoveryConfig.getTablePattern(DEFAULT_TABLE_PATTERN), discoveryConfig.getConnectorFactory());
    tblFinder.gatherTables(pathConfig);
    return tblFinder.getTablesByName().values();
  }


  @Value
  private class TableFinder {

    FilePath basePath;
    ErrorCollector errors;
    TablePattern tablePattern;
    ConnectorFactory connectorFactory;
    Map<Name, TableConfig> tablesByName = new HashMap<>();

    private void gatherTables(FilePathConfig pathConfig) {
      try {
        for (FilePath.Status fps : pathConfig.listFiles()) {
          FilePath p = fps.getPath();
          if (fps.isDir()) {
            gatherTables(FilePathConfig.ofDirectory(p));
          } else {
            Optional<NameComponents> componentsOpt = p.getComponents(tablePattern.get(true));
            if (componentsOpt.map(c -> Name.validName(c.getIdentifier())).orElse(false)) {
              NameComponents components = componentsOpt.get();
              Name tblName = Name.system(components.getIdentifier());

              Optional<Format> format = connectorFactory.getFormatForExtension(components.getFormat());
              if (format.isEmpty()) {
                errors.warn("File %s has unsupported format %s", p, components.getFormat());
                continue;
              }

              TableConfig.Builder builder = tableConfigFactory.forDiscovery(tblName, basePath,
                  tablePattern.substitute(tblName.getDisplay(), Optional.of("/"), Optional.of(
                      components.getSuffix())) , format.get());
              TableConfig table = builder.build();

              TableConfig otherTbl = tablesByName.get(tblName);
              if (otherTbl == null) {
                tablesByName.put(tblName, table);
              } else {
                Format otherFormat = otherTbl.getConnectorConfig().getFormat()
                    .get();
                if (!format.get().equals(otherFormat)) {
                  errors.warn("Table file [%s] does not have the same format [%s] of previously " +
                          "encountered table [%s]. File will be ignored",
                      p, otherFormat, otherTbl.getName());
                }
              }

            }
          }
        }
      } catch (IOException e) {
        errors.fatal("Could not read directory [%s] during discovery: %s", pathConfig, e);
      }
    }

  }
}
