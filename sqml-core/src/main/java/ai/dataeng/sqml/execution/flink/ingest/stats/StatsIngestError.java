package ai.dataeng.sqml.execution.flink.ingest.stats;

import ai.dataeng.sqml.execution.flink.ingest.LocationConversionError;
import ai.dataeng.sqml.type.basic.ProcessMessage;
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


    public StatsIngestError(@NonNull DocumentPath location, ProcessMessage error) {
        super(location, error);
    }

    public static StatsIngestError convert(DocumentPath location, ProcessMessage error) {
        return new StatsIngestError(location,error);
    }

}
