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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Types;

public class JdbcBigInteger extends AbstractJdbcType<BigInteger> {
    public static final JdbcBigInteger instance = new JdbcBigInteger();

    JdbcBigInteger() {
    }

    public boolean isCaseSensitive() {
        return false;
    }

    public int getScale(BigInteger obj) {
        return 0;
    }

    public int getPrecision(BigInteger obj) {
        return (obj == null) ? Integer.MAX_VALUE : obj.toString().length();
    }

    public boolean isCurrency() {
        return false;
    }

    public boolean isSigned() {
        return true;
    }

    public String toString(BigInteger obj) {
        return (obj == null) ? null : obj.toString();
    }

    public boolean needsQuotes() {
        return false;
    }

    public String getString(ByteBuffer bytes) {
        if ((bytes == null) || !bytes.hasRemaining()) {
            return null;
        }

        return toString(compose(bytes));
    }

    public Class<BigInteger> getType() {
        return BigInteger.class;
    }

    public int getJdbcType() {
        return Types.DECIMAL;
    }

    public BigInteger compose(Object obj) {
        return (BigInteger) obj;
    }

    public Object decompose(BigInteger value) {
        return value;
    }
}
