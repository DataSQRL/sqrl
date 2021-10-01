package ai.dataeng.sqml.ingest.source;

/**
 * Environments register a {@link SourceTableListener} against a {@link SourceDataset} to be notified of all the {@link SourceTable}
 * in that dataset.
 */
public interface SourceTableListener {

    public void registerSourceTable(SourceTable sourceTable) throws DuplicateException;

    public static class DuplicateException extends RuntimeException {

        public DuplicateException(String msg) {
            super(msg);
        }

    }

}
