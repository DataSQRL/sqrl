package ai.dataeng.sqml.physical.flink;

import lombok.Getter;

import java.io.Serializable;
import java.util.Arrays;

public class Row implements Serializable {
    private static final long serialVersionUID = 3L;

    private final Object[] values;

    public Row(Object... values) {
        this.values = values;
    }

    public int getArity() {
        return values.length;
    }

    public Object getValue(int position) {
        return values[position];
    }

    protected Object[] getRawValues() {
        return values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Row row = (Row) o;
        return Arrays.deepEquals(values, row.values);
    }

    @Override
    public int hashCode() {
        return Arrays.deepHashCode(values);
    }

    @Override
    public String toString() {
        return Arrays.deepToString(values);
    }
}
