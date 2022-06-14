package ai.datasqrl.plan.calcite;

import lombok.Getter;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;

@Getter
public class CalciteEnvironment {

    private final RelOptCluster cluster;
    private final JavaTypeFactoryImpl typeFactory;

    public CalciteEnvironment() {
        this.typeFactory = new JavaTypeFactoryImpl(); //new FlinkTypeFactory(new FlinkTypeSystem());
        this.cluster = MultiphaseOptimizer.newCluster(typeFactory);
    }

    public SqrlType2Calcite getTypeConverter() {
        return new SqrlType2Calcite(typeFactory);
    }


}
