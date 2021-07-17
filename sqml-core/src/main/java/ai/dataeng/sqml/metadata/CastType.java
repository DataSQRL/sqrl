/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.dataeng.sqml.metadata;

import static java.lang.String.format;

import ai.dataeng.sqml.common.OperatorType;
import ai.dataeng.sqml.common.QualifiedObjectName;

public enum CastType
{
    CAST(OperatorType.CAST.getFunctionName(), true),
    SATURATED_FLOOR_CAST(OperatorType.SATURATED_FLOOR_CAST.getFunctionName(), true);

    private final QualifiedObjectName castName;
    private final boolean isOperatorType;

    CastType(QualifiedObjectName castName, boolean isOperatorType)
    {
        this.castName = castName;
        this.isOperatorType = isOperatorType;
    }

    public QualifiedObjectName getCastName()
    {
        return castName;
    }

    public boolean isOperatorType()
    {
        return isOperatorType;
    }

    public static OperatorType toOperatorType(CastType castType)
    {
        switch (castType) {
            case CAST:
                return OperatorType.CAST;
            case SATURATED_FLOOR_CAST:
                return OperatorType.SATURATED_FLOOR_CAST;
            default:
                throw new IllegalArgumentException(format("No OperatorType for CastType %s", castType));
        }
    }
}
