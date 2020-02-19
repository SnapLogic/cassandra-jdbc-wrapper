/*
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.github.adejanovski.cassandra.jdbc;

import java.nio.ByteBuffer;
import java.sql.Types;

public class JdbcInt extends AbstractJdbcType<Integer> {
    public static final JdbcInt instance = new JdbcInt();

    JdbcInt() {
    }

    public boolean isCaseSensitive() {
        return false;
    }

    public int getScale(Integer obj) {
        return 0;
    }

    public int getPrecision(Integer obj) {
        // max size is -2147483648
        return (obj == null) ? 11 : obj.toString().length();
    }

    public boolean isCurrency() {
        return false;
    }

    public boolean isSigned() {
        return true;
    }

    public String toString(Integer obj) {
        return (obj == null) ? null : obj.toString();
    }

    public boolean needsQuotes() {
        return false;
    }

    public String getString(ByteBuffer bytes) {
        if ((bytes == null) || !bytes.hasRemaining()) {
            return null;
        } else if (bytes.remaining() != 4) {
            throw new MarshalException(
                    "An integer is exactly 4 bytes: " + bytes.remaining());
        }

        return toString(compose(bytes));
    }

    public Class<Integer> getType() {
        return Integer.class;
    }

    public int getJdbcType() {
        return Types.INTEGER;
    }

    public Integer compose(Object value) {
        return (Integer) value;
    }

    public Object decompose(Integer value) {
        return value;
    }
}
