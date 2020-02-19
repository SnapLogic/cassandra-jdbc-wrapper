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
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;

public class JdbcTimestamp extends AbstractJdbcType<Timestamp> {

    public static final JdbcTimestamp instance = new JdbcTimestamp();

    JdbcTimestamp() {
    }

    public boolean isCaseSensitive() {
        return false;
    }

    public int getScale(Timestamp obj) {
        return -1;
    }

    public int getPrecision(Timestamp obj) {
        // format is always 'yyyy-mm-ddThh:mm:ss[.SSSSSS][-xxxx]'
        return (obj == null) ? 31 : toString(obj).length();
    }

    public boolean isCurrency() {
        return false;
    }

    public boolean isSigned() {
        return false;
    }

    public String toString(Timestamp obj) {
        return Utils.formatTimestamp(obj);
    }

    public boolean needsQuotes() {
        return false;
    }

    public String getString(ByteBuffer bytes) {
        if ((bytes == null) || !bytes.hasRemaining()) {
            return null;
        } else if (bytes.remaining() != 8) {
            throw new MarshalException(
                    "A timestamp is exactly 8 bytes (stored as a long): " + bytes.remaining());
        }

        // uses ISO-8601 formatted string
        return Utils.formatDate(new Date(bytes.getLong(bytes.position())));
    }

    public Class<Timestamp> getType() {
        return Timestamp.class;
    }

    public int getJdbcType() {
        return Types.TIMESTAMP;
    }

    public Timestamp compose(Object value) {
        return (Timestamp) value;
    }

    public Object decompose(Timestamp value) {
        return value;
    }
}
