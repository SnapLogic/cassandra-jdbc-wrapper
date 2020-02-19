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
import java.sql.Date;
import java.sql.Types;

public class JdbcDate extends AbstractJdbcType<Date> {

    public static final JdbcDate instance = new JdbcDate();

    JdbcDate() {
    }

    public boolean isCaseSensitive() {
        return false;
    }

    public int getScale(Date obj) {
        return -1;
    }

    public int getPrecision(Date obj) {
        // format is always 'yyyy-mm-dd'
        return 10;
    }

    public boolean isCurrency() {
        return false;
    }

    public boolean isSigned() {
        return false;
    }

    public String toString(Date obj) {
        return (obj == null) ? null : Utils.formatDate(obj);
    }

    public boolean needsQuotes() {
        return false;
    }

    public String getString(ByteBuffer bytes) {
        if ((bytes == null) || !bytes.hasRemaining()) {
            return null;
        } else if (bytes.remaining() != 8) {
            throw new MarshalException(
                    "A date is exactly 8 bytes (stored as an long): " + bytes.remaining());
        }

        return toString(new Date(bytes.getLong(bytes.position())));
    }

    public Class<Date> getType() {
        return Date.class;
    }

    public int getJdbcType() {
        return Types.DATE;
    }

    public Date compose(Object value) {
        return (Date) value;
    }

    public Object decompose(Date value) {
        return value;
    }
}
