package ai.datasqrl.schema.table;

import ai.datasqrl.parse.tree.name.Name;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
public abstract class Field {

    @NonNull
    protected final Name name;
    protected final int version;

    protected Field(@NonNull Name name, int version) {
        Preconditions.checkArgument(version>=0);
        this.name = name;
        this.version = version;
    }

    public Name getId() {
        if (version==0) return name;
        return name.suffix(Integer.toString(version));
    }

    public boolean isVisible() {
        return true;
    }

    @Override
    public String toString() {
        return getId().toString();
    }

}
