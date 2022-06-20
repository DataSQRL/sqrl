package ai.datasqrl.schema;

public class TableTimestamp {

    enum Status { DEFAULT, DEFINED, INFERRED }

    private boolean isLocked;
    private Status status;
    private Column column;


}
