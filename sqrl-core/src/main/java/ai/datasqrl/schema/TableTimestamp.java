package ai.datasqrl.schema;

import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
public class TableTimestamp {

    @Getter
    @AllArgsConstructor
    public enum Status {
        DEFAULT(false), DEFINED(true), INFERRED(true);

        private final boolean locked;
    }

    private Status status;
    private Column column;

    private TableTimestamp(Column column, Status status) {
        this.status = status;
        this.column = column;
    }

    public boolean isLocked() {
        return status.isLocked();
    }

    public void update(Column column, Status status) {
        Preconditions.checkState(!isLocked());
        this.column = column;
        this.status = status;
    }

    public static TableTimestamp of(Column column, Status status) {
        return new TableTimestamp(column, status);
    }

}
