package ai.datasqrl.parse.tree;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class ExportDefinition extends SqrlStatement {

    protected final NodeLocation location;
    protected final NamePath tablePath;
    protected final NamePath sinkPath;

    public ExportDefinition(NodeLocation location, NamePath tablePath, NamePath sinkPath) {
        super(Optional.ofNullable(location));
        this.location = location;
        this.tablePath = tablePath;
        this.sinkPath = sinkPath;
    }

    public NamePath getTablePath() { return tablePath; }

    public NamePath getSinkPath() { return sinkPath; }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitExportDefinition(this, context);
    }

    public List<? extends Node> getChildren() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExportDefinition that = (ExportDefinition) o;
        return Objects.equals(location, that.location) && tablePath.equals(that.tablePath) && sinkPath.equals(that.sinkPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(location, tablePath, sinkPath);
    }

    @Override
    public String toString() {
        return "Export{" +
                "location=" + location +
                ", tablePath=" + tablePath +
                ", sinkPath=" + sinkPath +
                "}";
    }
}
