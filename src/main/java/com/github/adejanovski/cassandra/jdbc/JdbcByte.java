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

public class JdbcByte extends AbstractJdbcType<Byte> {
    public static final JdbcByte instance = new JdbcByte();

    JdbcByte() {
    }

    public boolean isCaseSensitive() {
        return false;
    }

    public int getScale(Byte obj) {
        return 0;
    }

    public int getPrecision(Byte obj) {
        return (obj == null) ? 4 : obj.toString().length();
    }

    public boolean isCurrency() {
        return false;
    }

    public boolean isSigned() {
        return true;
    }

    public String toString(Byte obj) {
        return (obj == null) ? null : obj.toString();
    }

    public boolean needsQuotes() {
        return false;
    }

    public String getString(ByteBuffer bytes) {
        if ((bytes == null) || !bytes.hasRemaining()) {
            return null;
        } else if (bytes.remaining() != 1) {
            throw new MarshalException(
                    "A date is exactly 1 byte: " + bytes.remaining());
        }

        return toString(compose(bytes));
    }

    public Class<Byte> getType() {
        return Byte.class;
    }

    public int getJdbcType() {
        return Types.TINYINT;
    }

    public Byte compose(Object obj) {
        return (Byte) obj;
    }

    public Object decompose(Byte value) {
        return value;
    }
}
