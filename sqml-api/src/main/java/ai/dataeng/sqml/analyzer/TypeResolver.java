package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.analyzer.TypeResolver.RelationResolverContext;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import java.util.Optional;
import java.util.Set;
import lombok.Value;

public class TypeResolver extends SqmlTypeVisitor<Set<Type>, RelationResolverContext> {
//  @Override
//  public Set<Type> visitSqmlType(Type type, RelationResolverContext context) {
//    return Set.of(type);
//  }
//
//  @Override
//  public Set<Type> visitRelation(RelationType type, RelationResolverContext context) {
//    if (context.getName().isEmpty()) {
//      return Set.of(type);
//    }
//    NamePart firstPart = context.name.get().getFirst();
//    SqmlTypeVisitor<Set<Type>, RelationResolverContext> that = this;
//
//    Set<Type> rel = firstPart.accept(new QualifiedNameVisitor<>() {
//      @Override
//      public Set<Type> visitDataPart(DataPart part, RelationResolverContext context) {
//        Set<Type> types = new HashSet<>();
//
//        type.getFields().stream()
//            .filter(input -> canResolvePartial(input, context.getName().get()))
//            .flatMap(f->f.getType().accept(that, createNextContext(f, context)).stream())
//            .forEach(t->types.add(t));
//
//        context.getScope().getRoot()
//            .getField(part.name)
//            .map(f->f.getType().accept(that, createNextContext(context, 1)))
//            .ifPresent(t -> types.addAll(t));
//
//        return types;
//      }
//
//      @Override
//      public Set<Type> visitParentPart(ParentPart part, RelationResolverContext context) {
//        Preconditions.checkState(type instanceof NamedRelationType, "Relation not named, cannot traverse to parent: %s", context.getName().get());
//        RelationType rel = type.getParent()
//            .orElseThrow(() -> new RuntimeException(String.format("Could not traverse to parent from %s",
//                context.getName().get())));
//        return rel.accept(that, createNextContext(context, 1));
//      }
//
//      @Override
//      public Set<Type> visitSelfPart(SelfPart part, RelationResolverContext context) {
//        RelationType rel = context.getScope()
//            .resolveRelation(context.getScope().getName().getPrefix().orElse(context.getScope()
//                .getName()))
//            .get();
//        Set<Type> t = rel.accept(that, createNextContext(context, 1));
//        return t;
//      }
//
//      @Override
//      public Set<Type> visitSibilingPart(SibilingPart part, RelationResolverContext context) {
//        return type.accept(that, createNextContext(context, 1));
//      }
//
//      private RelationResolverContext createNextContext(RelationResolverContext context, int offset) {
//        List<String> newParts = new ArrayList<>();
//        for (int i = offset; i < context.getName().get().getParts().size(); i++) {
//          newParts.add(context.getName().get().getParts().get(i));
//        }
//        if (newParts.isEmpty()) {
//          return new RelationResolverContext(Optional.empty(), context.getScope());
//        }
//        return new RelationResolverContext(
//            Optional.of(QualifiedName.of(newParts)),
//            context.getScope()
//        );
//      }
//
//      private RelationResolverContext createNextContext(Field f, RelationResolverContext context) {
//        QualifiedName name = context.getName().get();
//        if (name.getParts().size() > 1 &&
//            f.getRelationAlias().isPresent() &&
//            f.getRelationAlias().get().equalsIgnoreCase(name.getParts().get(0))) {
//          return createNextContext(context, 2);
//        }
//
//        return createNextContext(context, 1);
//      }
//
//      private boolean canResolvePartial(Field field, QualifiedName name) {
//        if (!field.getName().isPresent()) {
//          return false;
//        }
//        //if it matches the alias + field or field
//        if (field.getRelationAlias().isPresent()) {
//          if (name.getParts().size() > 1) {
//            return field.getRelationAlias().get().equalsIgnoreCase(name.getParts().get(0)) &&
//                field.getName().get().equalsIgnoreCase(name.getParts().get(1));
//          } else {
//            return field.getName().get().equalsIgnoreCase(name.getParts().get(0));
//          }
//        } else {
//          return field.getName().get().equalsIgnoreCase(name.getParts().get(0));
//        }
//      }
//
//    }, context);
//
//    return rel;
//  }
//
  @Value
  public static class RelationResolverContext {
    Optional<QualifiedName> name;
    Scope scope;
  }
}
