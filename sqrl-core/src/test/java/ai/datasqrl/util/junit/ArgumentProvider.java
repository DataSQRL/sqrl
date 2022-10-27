package ai.datasqrl.util.junit;

import com.google.common.base.Preconditions;
import org.junit.jupiter.params.provider.Arguments;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class ArgumentProvider {

    public static Stream<? extends Arguments> crossProduct(List<? extends Object>... argumentLists) {
        return crossProduct(List.of(argumentLists));
    }

    public static Stream<? extends Arguments> crossProduct(List<List<? extends Object>> argumentLists) {
        Preconditions.checkArgument(argumentLists.size()>0);
        Stream<? extends Arguments> base = argumentLists.get(0).stream().map(Arguments::of);
        for (int i = 1; i < argumentLists.size(); i++) {
            List<? extends Object> join = argumentLists.get(i);
            base = base.flatMap(args -> join.stream().map(x -> {
                Object[] argArr = args.get();
                Object[] newArgArr = Arrays.copyOf(argArr, argArr.length+1);
                newArgArr[argArr.length] = x;
                return Arguments.of(newArgArr);
            }));
        }
        return base;
    }

}
