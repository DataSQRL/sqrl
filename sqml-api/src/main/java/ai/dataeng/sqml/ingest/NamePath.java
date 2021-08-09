package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.tree.QualifiedName;
import lombok.NonNull;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import scala.Array;

import java.util.Arrays;

@Value
public class NamePath {

    public static NamePath BASE = new NamePath(new String[0]);

    private final String[] names;

    private NamePath(@NonNull String[] names) {
        this.names = names;
    }

    public NamePath sub(@NonNull String name) {
        String[] newnames = Arrays.copyOf(names,names.length+1);
        newnames[names.length] = name.trim();
        return new NamePath(newnames);
    }

    public QualifiedName getQualifiedName(@NonNull String tableName) {
        return QualifiedName.of(tableName,names);
    }

    public QualifiedName getQualifiedName(@NonNull String datasetName, @NonNull String tableName) {
        String[] padNames = new String[names.length+1];
        padNames[0] = tableName;
        Array.copy(names,0,padNames,1, names.length);
        return QualifiedName.of(datasetName,padNames);
    }

    @Override
    public String toString() {
        if (names.length==0) return "/";
        return StringUtils.join(names,'.');
    }


}
