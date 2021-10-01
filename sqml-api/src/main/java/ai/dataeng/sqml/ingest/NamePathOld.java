package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.ingest.schema.SourceTableSchema;
import ai.dataeng.sqml.tree.QualifiedName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import lombok.NonNull;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import scala.Array;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

@Value
public class NamePathOld implements Iterable<String>, Serializable {

    public static NamePathOld ROOT = new NamePathOld(new String[0]);

    private final String[] names;

    private NamePathOld(@NonNull String... names) {
        this.names = names;
        for (int i = 0; i < names.length; i++) {
            names[i]= SourceTableSchema.normalizeName(names[i]);
        }
    }

    public static NamePathOld of(@NonNull String... names) {
        return new NamePathOld(names);
    }

    public NamePathOld resolve(@NonNull String name) {
        name = SourceTableSchema.normalizeName(name);
        String[] newnames = Arrays.copyOf(names,names.length+1);
        newnames[names.length] = name.trim();
        return new NamePathOld(newnames);
    }

    public NamePathOld resolve(@NonNull NamePathOld sub) {
        String[] newnames = Arrays.copyOf(names,names.length+sub.names.length);
        Array.copy(sub.names, 0, newnames, names.length, sub.names.length);
        return new NamePathOld(newnames);
    }

    public NamePathOld prefix(int depth) {
        if (depth==0) return ROOT;
        String[] newnames = Arrays.copyOf(names,depth);
        return new NamePathOld(newnames);
    }

    public int getNumComponents() {
        return names.length;
    }

    public String getComponent(int index) {
        Preconditions.checkArgument(index>=0 && index<getNumComponents());
        return names[index];
    }

    public String getLastComponent() {
        Preconditions.checkArgument(names.length>0);
        return names[names.length-1];
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
        return toString('.');
    }

    public String toString(char separator) {
        if (names.length==0) return "/";
        return StringUtils.join(names,separator);
    }


    @Override
    public Iterator<String> iterator() {
        return Iterators.forArray(names);
    }
}
