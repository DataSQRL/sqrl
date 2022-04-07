package ai.dataeng.sqml.io.impl;

import ai.dataeng.sqml.io.sources.DataSource;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.SourceTableConfiguration;
import ai.dataeng.sqml.io.impl.file.FilePath;
import ai.dataeng.sqml.io.impl.file.DirectorySourceImplementation;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;

@Value
@Slf4j
public class InputPreview {

    final DataSource source;
    final SourceTableConfiguration table;

    public Stream<BufferedReader> getTextPreview() {
        if (source.getImplementation() instanceof DirectorySourceImplementation) {
            DirectorySourceImplementation directorySource = (DirectorySourceImplementation)source.getImplementation();
            Collection<FilePath> files = Collections.EMPTY_LIST;
            try {
                files = directorySource.getFilesForTable(table, source.getConfig());
            } catch (IOException e) {
                log.error("Could not preview files in [%s] for table [%s]: %s",source,table,e);
            }
            return files.stream().map(fp -> getBufferedReader(fp, source.getConfig())).filter(r -> r!=null);
        } else {
            //Preview not supported
            return Stream.empty();
        }
    }

    private static BufferedReader getBufferedReader(FilePath fp, DataSourceConfiguration config) {
        InputStream in = null;
        BufferedReader r = null;
        try {
            in = fp.read();
            r = new BufferedReader(new InputStreamReader(in, config.getCharsetObject()));
            return r;
        } catch (IOException e) {
            log.error("Could not read file [%s]: %s",fp,e);
            try {
                if (in != null) in.close();
                if (r != null) r.close();
            } catch (Exception ex) {}
            return null;
        }
    }

}
