package ai.dataeng.sqml.imports;


public interface ImportObject {
  public <R, C> R accept(ImportVisitor<R, C> visitor, C context);

  String getPath();
}
