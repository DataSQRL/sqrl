package com.datasqrl.plan.table;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import org.apache.calcite.sql.validate.SqlValidatorUtil;

import com.datasqrl.canonicalizer.Name;
import com.google.common.base.Preconditions;

/**
 * NameAdjuster makes sure that any additional columns we add to a table (e.g. primary keys or timestamps) are unique and do
 * not clash with existing columns by `uniquifying` them using Calcite's standard way of doing this.
 * Because we want to preserve the names of the user-defined columns and primary key columns are added first, we have to use
 * this custom way of uniquifying column names.
 */
public class NameAdjuster {

    Set<String> names;

    public NameAdjuster(Collection<String> names) {
        this.names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        this.names.addAll(names);
        Preconditions.checkArgument(this.names.size() == names.size(), "Duplicate names in set of columns: %s", names);
    }

    public String uniquifyName(Name name) {
        return uniquifyName(name.getDisplay());
    }

    public String uniquifyName(String name) {
        var uniqueName = SqlValidatorUtil.uniquify(
                name,
                names,
                SqlValidatorUtil.EXPR_SUGGESTER);
        names.add(uniqueName);
        return uniqueName;
    }

    public void add(String name) {
        Preconditions.checkArgument(!contains(name),"Name is already in set: %s", name);
        names.add(name);
    }

    public boolean contains(String name) {
        return names.contains(name);
    }

    @Override
    public String toString() {
        return names.toString();
    }


}
