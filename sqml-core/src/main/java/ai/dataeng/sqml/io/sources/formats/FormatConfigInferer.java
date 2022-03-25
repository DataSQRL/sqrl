package ai.dataeng.sqml.io.sources.formats;

import ai.dataeng.sqml.io.sources.DataSource;
import ai.dataeng.sqml.io.sources.SourceTableConfiguration;
import ai.dataeng.sqml.io.sources.impl.InputPreview;
import lombok.AllArgsConstructor;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

@AllArgsConstructor
public class FormatConfigInferer<C extends FormatConfiguration> {

    final Format.ConfigurationInference<C> inferer;
    final InputPreview preview;

    public Optional<C> inferConfig() {
        if (inferer instanceof TextLineFormat.ConfigurationInference) {
            TextLineFormat.ConfigurationInference<C> textInferer = (TextLineFormat.ConfigurationInference)inferer;
            Iterator<BufferedReader> inputs = preview.getTextPreview().iterator();
            while (inferer.getConfidence()<0.95 && inputs.hasNext()) {
                try (BufferedReader r = inputs.next()) {
                    textInferer.nextSegment(r);
                } catch (IOException e) {
                    //Ignore and continue
                }
            }
            return inferer.getConfiguration();
        } else {
            //We currently do not support inferring formats from non-text formats
            return Optional.empty();
        }
    }


}
