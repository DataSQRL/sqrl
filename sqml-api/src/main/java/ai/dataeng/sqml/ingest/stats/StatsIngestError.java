package ai.dataeng.sqml.ingest.stats;

import ai.dataeng.sqml.ingest.LocationConversionError;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import lombok.NonNull;

public class StatsIngestError extends LocationConversionError<DocumentPath> {

    public StatsIngestError(Severity severity, @NonNull DocumentPath location, String msg, Object... args) {
        super(severity, location, msg, args);
    }

    public static StatsIngestError fatal(DocumentPath location, String msg, Object... args) {
        return new StatsIngestError(Severity.FATAL, location, msg, args);
    }

    public static StatsIngestError warn(DocumentPath location, String msg, Object... args) {
        return new StatsIngestError(Severity.WARN, location, msg, args);
    }


    public StatsIngestError(@NonNull DocumentPath location, ConversionError error) {
        super(location, error);
    }

    public static StatsIngestError convert(DocumentPath location, ConversionError error) {
        return new StatsIngestError(location,error);
    }

}
