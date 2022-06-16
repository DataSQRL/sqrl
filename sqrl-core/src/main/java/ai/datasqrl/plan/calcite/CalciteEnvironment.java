package ai.datasqrl.plan.calcite;

import lombok.Getter;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;

@Getter
public class CalciteEnvironment {
    @Deprecated //TODO: move calcite out of schema planning phase
    public SqrlType2Calcite getTypeConverter() {
        return new SqrlType2Calcite(new JavaTypeFactoryImpl());
    }
}
