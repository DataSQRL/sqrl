package ai.dataeng.sqml.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * A {@link SourceTable} defines an input source to be imported into an SQML script. A {@link SourceTable} is comprised
 * of records and is the smallest unit of data that one can refer to within an SQML script.
 */
public interface SourceTable {

    /**
     *
     * @return {@link SourceDataset} that this table is part of
     */
    public SourceDataset getDataset();

    /**
     * Returns the name of this table. It must be unique within its {@link SourceDataset}
     * @return
     */
    public String getName();

    /**
     * Produces a {@link DataStream} of {@link SourceRecord} for this table source.
     *
     * TODO: Need to figure out how to distinguish between replay from start vs continuous streaming
     *
     * @return
     */
    public DataStream<SourceRecord> getDataStream(StreamExecutionEnvironment env);



}
