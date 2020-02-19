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
import java.sql.Time;
import java.sql.Types;

public class JdbcTime extends AbstractJdbcType<Time> {

    public static final JdbcTime instance = new JdbcTime();

    JdbcTime() {
    }

    public boolean isCaseSensitive() {
        return false;
    }

    public int getScale(Time obj) {
        return -1;
    }

    public int getPrecision(Time obj) {
        // format is always 'hh:mm:ss[.SSSSSS]'
        return (obj == null) ? 15 : toString(obj).length();
    }

    public boolean isCurrency() {
        return false;
    }

    public boolean isSigned() {
        return false;
    }

    public String toString(Time obj) {
        return Utils.formatTime(obj);
    }

    public boolean needsQuotes() {
        return false;
    }

    public String getString(ByteBuffer bytes) {
        if ((bytes == null) || !bytes.hasRemaining()) {
            return null;
        } else if (bytes.remaining() != 8) {
            throw new MarshalException(
                    "A time is exactly 8 bytes (stored as an long): " + bytes.remaining());
        }

        return toString(new Time(bytes.getLong(bytes.position())));
    }

    public Class<Time> getType() {
        return Time.class;
    }

    public int getJdbcType() {
        return Types.TIME;
    }

    public Time compose(Object value) {
        return (Time) value;
    }

    public Object decompose(Time value) {
        return value;
    }
}
