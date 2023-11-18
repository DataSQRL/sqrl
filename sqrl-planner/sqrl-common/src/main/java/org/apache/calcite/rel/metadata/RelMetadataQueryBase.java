//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.calcite.rel.metadata;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Collation;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.flink.calcite.shaded.com.google.common.collect.HashBasedTable;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.calcite.shaded.com.google.common.collect.Table;

public class RelMetadataQueryBase {
  public final Table<RelNode, List, Object> map = HashBasedTable.create();
  public final JaninoRelMetadataProvider metadataProvider;
  public static final ThreadLocal<JaninoRelMetadataProvider> THREAD_PROVIDERS = new ThreadLocal();

  protected RelMetadataQueryBase(JaninoRelMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
  }

  protected static <H> H initialHandler(Class<H> handlerClass) {
    return handlerClass.cast(Proxy.newProxyInstance(RelMetadataQuery.class.getClassLoader(), new Class[]{handlerClass}, (proxy, method, args) -> {
      RelNode r = (RelNode)args[0];
      throw new JaninoRelMetadataProvider.NoHandler(r.getClass());
    }));
  }

  public static class Handler {

  }

  protected <M extends Metadata, H extends MetadataHandler<M>> H revise(Class<? extends RelNode> class_, MetadataDef<M> def) {
    try {
      return this.metadataProvider.revise(class_, def);
    } catch (Exception e) {
      return (H)new BuiltInMetadata.Collation.Handler(){

        @Override
        public MetadataDef<Collation> getDef() {
          return null;
        }

        @Override
        public ImmutableList<RelCollation> collations(RelNode var1, RelMetadataQuery var2) {
          return null;
        }
      };

    }
  }

  public boolean clearCache(RelNode rel) {
    Map<List, Object> row = this.map.row(rel);
    if (row.isEmpty()) {
      return false;
    } else {
      row.clear();
      return true;
    }
  }
}
