package ai.dataeng.sqml.ingest.stats;

import ai.dataeng.sqml.ingest.NamePathOld;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.NamePath;

import java.util.Arrays;

public class DocumentPath {

    public static DocumentPath ROOT = new DocumentPath(new String[0]);

    private final String[] names;

    private DocumentPath(String... names ){
        this.names = names;
    }

    public DocumentPath resolve(String sub) {
        String[] newnames = Arrays.copyOf(names,names.length+1);
        newnames[names.length] = sub;
        return new DocumentPath(newnames);
    }

}
