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
package ai.dataeng.sqml.common.type;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

import io.airlift.slice.Slice;
import java.util.Objects;

public final class CharType
        extends AbstractVariableWidthType
{
    public static final int MAX_LENGTH = 65_536;

    private final int length;

    public static CharType createCharType(long length)
    {
        return new CharType(length);
    }

    private CharType(long length)
    {
        super(
                new TypeSignature(
                        StandardTypes.CHAR,
                        singletonList(TypeSignatureParameter.of(length))),
                Slice.class);

        if (length < 0 || length > MAX_LENGTH) {
            throw new RuntimeException(format("CHAR length scale must be in range [0, %s]", MAX_LENGTH));
        }
        this.length = (int) length;
    }

    public int getLength()
    {
        return length;
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CharType other = (CharType) o;

        return Objects.equals(this.length, other.length);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(length);
    }

    @Override
    public String getDisplayName()
    {
        return getTypeSignature().toString();
    }

    @Override
    public String toString()
    {
        return getDisplayName();
    }
}
