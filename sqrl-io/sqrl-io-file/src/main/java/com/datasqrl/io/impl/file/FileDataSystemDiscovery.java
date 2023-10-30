package com.datasqrl.io.impl.file;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystemDiscovery;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.formats.FileFormatExtension;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.io.formats.TextLineFormat;
import com.datasqrl.io.impl.file.FilePath.NameComponents;
import com.datasqrl.io.tables.BaseTableConfig;
import com.datasqrl.io.tables.TableConfig;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FileDataSystemDiscovery extends DataSystemDiscovery.Base {


  final FilePathConfig pathConfig;
  final Pattern filenamePattern;

  public FileDataSystemDiscovery(TableConfig genericTable, FilePathConfig pathConfig,
      Pattern filenamePattern) {
    super(genericTable);
    this.pathConfig = pathConfig;
    this.filenamePattern = filenamePattern;
  }

  public static boolean isTableFile(@NonNull FilePath file, @NonNull TableConfig tableConfig) {
    BaseTableConfig baseConfig = tableConfig.getBase();
    FormatFactory format = tableConfig.getFormat();
    FileDataSystemConfig fileConfig = FileDataSystemConfig.fromConfig(
            tableConfig);
    return isTableFile(file, baseConfig, format, fileConfig);
  }

  public static boolean isTableFile(@NonNull FilePath file, @NonNull BaseTableConfig baseConfig,
                                    @NonNull FormatFactory format, @NonNull FileDataSystemConfig fileConfig) {
    Optional<NameComponents> componentsOpt = file.getComponents(fileConfig.getPattern());
    if (componentsOpt.isEmpty()) {
      return false;
    }
    NameComponents components = componentsOpt.get();
    if (!baseConfig.getCanonicalizer().getCanonical(components.getIdentifier())
        .equals(baseConfig.getIdentifier())) {
      return false;
    }
    return FileFormatExtension.matches(format, components.getFormat());
  }

  @Override
  public boolean requiresFormat(ExternalDataType type) {
    return type != ExternalDataType.source;
  }

  @Override
  public Collection<TableConfig> discoverSources(ErrorCollector errors) {
    Map<Name, TableConfig> tablesByName = new HashMap<>();
    gatherTables(pathConfig, tablesByName, errors);
    return tablesByName.values();
  }

  private void gatherTables(FilePathConfig pathConfig, Map<Name, TableConfig> tablesByName,
      ErrorCollector errors) {
    try {
      for (FilePath.Status fps : pathConfig.listFiles()) {
        FilePath p = fps.getPath();
        if (fps.isDir()) {
          gatherTables(FilePathConfig.ofDirectory(p), tablesByName, errors);
        } else {
          Optional<NameComponents> componentsOpt = p.getComponents(filenamePattern);
          if (componentsOpt.map(c -> Name.validName(c.getIdentifier())).orElse(false)) {
            NameComponents components = componentsOpt.get();
            Name tblName = Name.system(components.getIdentifier());
            TableConfig.Builder tblBuilder = copyGeneric(tblName, ExternalDataType.source);

            Optional<FormatFactory> extensionFormat = FileFormatExtension.getFormat(
                components.getFormat());
            FormatFactory chosenFormat = null;
            SqrlConfig formatConfig = tblBuilder.getFormatConfig();
            if (genericTable.hasFormat()) {
              chosenFormat = genericTable.getFormat();
              String chosenName = chosenFormat.getName();
              if (extensionFormat.map(ff -> !ff.getName().equals(chosenName)).orElse(false)) {
                errors.warn("File [%s] does not match configured format [%s]", p,
                    chosenFormat.getName());
              }
            } else if (extensionFormat.isPresent()) {
              chosenFormat = extensionFormat.get();
              formatConfig.setProperty(FormatFactory.FORMAT_NAME_KEY, chosenFormat.getName());
            } else {
              continue; //Unrecognized format
            }
            //infer format if not completely specified
            chosenFormat.inferConfig(formatConfig,
                new InputPreview(tblBuilder.build(), chosenFormat));
            TableConfig table = tblBuilder.build();

            TableConfig otherTbl = tablesByName.get(tblName);
            if (otherTbl == null) {
              tablesByName.put(tblName, table);
            } else {
              FormatFactory otherFormat = otherTbl.getFormat();
              if (!chosenFormat.getName().equals(otherFormat.getName())) {
                errors.warn("Table file [%s] does not have the same format [%s] of previously " +
                        "encountered table [%s]. File will be ignored",
                    p, otherFormat.getName(), otherTbl.getName());
              }
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


  @Value
  private class InputPreview implements com.datasqrl.io.impl.InputPreview {

    TableConfig tableConfig;
    FormatFactory format;

    @Override
    public Stream<BufferedReader> getTextPreview() {
      tableConfig.getErrors()
          .checkFatal(format instanceof TextLineFormat, "Format is not text based");
      Charset charset = ((TextLineFormat) format).getCharset(tableConfig.getFormatConfig());
      return getFiles().map(fp -> getBufferedReader(fp, charset))
          .filter(Objects::nonNull);
    }

    private Stream<FilePath> getFiles() {
      Collection<FilePath> files = Collections.EMPTY_LIST;
      try {
        files = getFilesForTable(tableConfig);
      } catch (IOException e) {
        log.error("Could not preview files for table [{}]: {}",
            tableConfig.getName(), e);
      }
      return files.stream();
    }

    private BufferedReader getBufferedReader(FilePath fp, Charset charset) {
      InputStream in = null;
      BufferedReader r = null;
      try {
        in = fp.read();
        r = new BufferedReader(new InputStreamReader(in, charset));
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

  @Override
  public Optional<TableConfig> discoverSink(@NonNull Name sinkName,
      @NonNull ErrorCollector errors) {
    TableConfig.Builder builder = copyGeneric(sinkName, ExternalDataType.sink);
    return Optional.of(builder.build());
  }

}
