//package ai.dataeng.sqml;
//
//import ai.dataeng.sqml.planner.Table;
//import ai.dataeng.sqml.tree.name.Name;
//import ai.dataeng.sqml.tree.name.NamePath;
//import lombok.Value;
//
//public class NamePathWalker {
//  private static final TableVisitor<Void, WalkerContext> selfVisitor = new TableVisitor<>() {
//    @Override
//    public Void visitTable(Table table, WalkerContext walkerContext) {
//      walkerContext.visitor.visitSelf(table, walkerContext);
//      return null;
//    }
//  };
//
//  private static final TableVisitor<Void, WalkerContext> siblingVisitor = new TableVisitor<>() {
//    @Override
//    public Void visitTable(Table table, WalkerContext walkerContext) {
//      walkerContext.visitor.visitSibling(table, walkerContext);
//      return null;
//    }
//  };
//
//  private static final TableVisitor<Void, WalkerContext> parentVisitor = new TableVisitor<>() {
//    @Override
//    public Void visitTable(Table table, WalkerContext walkerContext) {
//      walkerContext.visitor.visitParent(table, walkerContext);
//      return null;
//    }
//  };
//
//  private static final TableVisitor<Void, WalkerContext> tableVisitor = new TableVisitor<>() {
//    @Override
//    public Void visitTable(Table table, WalkerContext walkerContext) {
//      walkerContext.visitor.visitTable(table, walkerContext);
//      return null;
//    }
//  };
//
//  public static void walk(NamePath namePath, Table table, Visitor visitor, Context context) {
//    if (namePath.isEmpty()) {
//      return;
//    }
//    if (namePath.getFirst().equals(Name.SELF_IDENTIFIER)) {
//      table.accept(selfVisitor, createContext(visitor, context, namePath, 0));
//    } else {
//      table.accept(tableVisitor, createContext(visitor, context, namePath, 0));
//    }
//
//    for (int i = 1; i < namePath.getLength(); i++) {
//      if (namePath.get(i).equals(Name.SELF_IDENTIFIER)) {
//        throw new RuntimeException(String.format("Cannot visit a self identifier nested in a path: %s", namePath));
//      } else if (namePath.get(i).equals(Name.PARENT_RELATIONSHIP)) {
//        table.accept(parentVisitor, createContext(visitor, context, namePath, i));
//      } else if (namePath.get(i).equals(Name.SIBLING_RELATIONSHIP)) {
//        table.accept(siblingVisitor, createContext(visitor, context, namePath, i));
//      } else {
//        table.accept(tableVisitor, createContext(visitor, context, namePath, i));
//      }
//    }
//  }
//
//  private static WalkerContext createContext(Visitor visitor, Context context, NamePath namePath,
//      int index) {
//    return new WalkerContext(visitor, context, namePath, index);
//  }
//
//  @Value
//  public static class WalkerContext {
//    Visitor visitor;
//    Context context;
//    NamePath namePath;
//    int index;
//  }
//
//  public class Context {
//
//  }
//
//  public class Visitor {
//
//    public void visitSelf(Table table, WalkerContext context) {
//
//    }
//
//    public void visitSibling(Table table, WalkerContext context) {
//
//    }
//
//    public void visitParent(Table table, WalkerContext context) {
//
//    }
//
//    public void visitTable(Table table, WalkerContext context) {
//
//    }
//  }
//}
