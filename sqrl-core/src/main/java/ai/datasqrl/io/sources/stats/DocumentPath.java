package ai.datasqrl.io.sources.stats;


import java.util.Arrays;

public class DocumentPath {

  public static DocumentPath ROOT = new DocumentPath();

  private final String[] names;

  private DocumentPath(String... names) {
    this.names = names;
  }

  public DocumentPath resolve(String sub) {
    String[] newnames = Arrays.copyOf(names, names.length + 1);
    newnames[names.length] = sub;
    return new DocumentPath(newnames);
  }

}
