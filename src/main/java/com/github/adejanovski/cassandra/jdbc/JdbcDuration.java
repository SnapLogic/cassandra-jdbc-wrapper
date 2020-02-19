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

// FIXME - ...
// Durations are internally 3 ints. CqlDuration?
public class JdbcDuration extends JdbcOther {

    public static final JdbcDuration instance = new JdbcDuration();

    JdbcDuration() {
    }

    public boolean isCaseSensitive() {
        return false;
    }

    public int getScale(String obj) {
        return -1;
    }

    public int getPrecision(String obj) {
        return -1;
    }

    public boolean isSigned() {
        return false;
    }

    public boolean needsQuotes() {
        return true;
    }

    public String getString(ByteBuffer bytes) {
        if ((bytes == null) || !bytes.hasRemaining()) {
            return null;
        } else if (bytes.remaining() != 12) {
            // three ints, or an int and two longs?
            throw new MarshalException(
                    "A duration is exactly 12 bytes (stored as three ints): " + bytes.remaining());
        }

        // uses ISO-8601 formatted string
        // may be able to use datastax duration class
        return "UNIMPLEMENTED";
    }

    public String compose(Object value) {
        // this is where datastax duration is converted to string
        return (value == null) ? null : value.toString();
    }

    public Object decompose(String value) {
        // this is where string is converted to datastax duration
        return value;
    }
}
