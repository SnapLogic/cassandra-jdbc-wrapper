package com.github.adejanovski.cassandra.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.Duration;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.google.common.collect.Maps;

public enum DataTypeEnum {

    // @formatter:off
    CUSTOM(0, ByteBuffer.class, DataType.Name.CUSTOM, null),
    ASCII(1, String.class, DataType.Name.ASCII, JdbcAscii.instance),
    BIGINT(2, Long.class, DataType.Name.BIGINT, JdbcLong.instance),
    BLOB(3, ByteBuffer.class, DataType.Name.BLOB, JdbcBytes.instance),
    BOOLEAN(4, Boolean.class, DataType.Name.BOOLEAN, JdbcBoolean.instance),
    COUNTER(5, Long.class, DataType.Name.COUNTER, JdbcCounterColumn.instance),
    DECIMAL(6, BigDecimal.class, DataType.Name.DECIMAL, JdbcDecimal.instance),
    DOUBLE(7, Double.class, DataType.Name.DOUBLE, JdbcDouble.instance),
    FLOAT(8, Float.class, DataType.Name.FLOAT, JdbcFloat.instance),
    INT(9, Integer.class, DataType.Name.INT, JdbcInt.instance),
    TEXT(10, String.class, DataType.Name.TEXT, null), // UTF8?
    TIMESTAMP(11, Timestamp.class, DataType.Name.TIMESTAMP, JdbcTimestamp.instance),
    UUID(12, UUID.class, DataType.Name.UUID, JdbcUUID.instance),
    VARCHAR(13, String.class, DataType.Name.VARCHAR, JdbcUTF8.instance),
    VARINT(14, BigInteger.class, DataType.Name.VARINT, JdbcBigInteger.instance),
    TIMEUUID(15, UUID.class, DataType.Name.TIMEUUID, JdbcTimeUUID.instance),
    INET(16, InetAddress.class, DataType.Name.INET, JdbcInetAddress.instance),

    LIST(32, List.class, DataType.Name.LIST, null),
    SET(34, Set.class, DataType.Name.SET, null),
    MAP(33, Map.class, DataType.Name.MAP, null),

    // API version 3
    UDT(48, UDTValue.class, DataType.Name.UDT, JdbcUdt.instance),
    TUPLE(49, TupleValue.class, DataType.Name.TUPLE, JdbcTuple.instance),

    // API version 4
    DATE(17, Date.class, DataType.Name.DATE, JdbcDate.instance),
    TIME(18, Time.class, DataType.Name.TIME, JdbcTime.instance),
    SMALLINT(19, Short.class, DataType.Name.SMALLINT, JdbcShort.instance),
    TINYINT(20, Byte.class, DataType.Name.TINYINT, JdbcByte.instance),

    // API version 5
    DURATION(21, Duration.class, DataType.Name.DURATION, JdbcDuration.instance);
    // @formatter:on

    final int protocolId;
    final Class<?> javaType;
    final Name cqlType;
    final AbstractJdbcType<?> jdbcType;
    final int precision;

    private static final DataTypeEnum[] nameToIds;
    private static final Map<DataType.Name, DataTypeEnum> cqlDataTypeToDataType;
    private static final Map<Class<?>, DataTypeEnum> javaClassToDataType;
    private static final Map<Class<?>, DataTypeEnum> jdbcTypeToDataType;

    static {

        cqlDataTypeToDataType = Maps.newHashMap();
        javaClassToDataType = Maps.newHashMap();
        jdbcTypeToDataType = Maps.newHashMap();

        int maxCode = -1;
        for (DataTypeEnum name : DataTypeEnum.values())
            maxCode = Math.max(maxCode, name.protocolId);
        nameToIds = new DataTypeEnum[maxCode + 1];
        for (DataTypeEnum name : DataTypeEnum.values()) {
            if (nameToIds[name.protocolId] != null)
                throw new IllegalStateException("Duplicate Id");
            nameToIds[name.protocolId] = name;

            cqlDataTypeToDataType.put(name.cqlType, name);
            javaClassToDataType.put(name.javaType, name);
            if (name.jdbcType != null) {
                jdbcTypeToDataType.put(name.jdbcType.getClass(), name);
            }
        }
    }

    private DataTypeEnum(int protocolId, Class<?> javaType, Name cqlType,
            AbstractJdbcType<?> jdbcType) {
        this.protocolId = protocolId;
        this.javaType = javaType;
        this.cqlType = cqlType;
        this.jdbcType = jdbcType;
        int precision = 0;
        if (jdbcType != null) {
            precision = jdbcType.getPrecision(null);
        }
        this.precision = precision;
    }

    static DataTypeEnum fromCqlTypeName(DataType.Name cqlTypeName) {
        return cqlDataTypeToDataType.get(cqlTypeName);
    }

    public static DataTypeEnum fromClass(Class<?> javaType) {
        return javaClassToDataType.get(javaType);
    }

    public static DataTypeEnum fromJdbcType(Class<?> jdbcType) {
        return jdbcTypeToDataType.get(jdbcType);
    }

    static DataTypeEnum fromProtocolId(int id) {
        DataTypeEnum name = nameToIds[id];
        if (name == null)
            throw new DriverInternalError("Unknown data type protocol id: " + id);
        return name;
    }

    /**
     * Returns whether this data type name represent the name of a collection type that is a list,
     * set or map.
     *
     * @return whether this data type name represent the name of a collection type.
     */
    public boolean isCollection() {
        switch (this) {
            case LIST:
            case SET:
            case MAP:
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns the default precision the type. (See CassandraResultSet)
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * Returns the Java Class corresponding to this CQL type name.
     *
     * The correspondence between CQL types and java ones is as follow:
     * <table>
     * <caption>DataType to Java class correspondence</caption>
     * <tr>
     * <th>DataType (CQL)</th>
     * <th>Java Class</th>
     * </tr>
     * <tr>
     * <td>ASCII</td>
     * <td>String</td>
     * </tr>
     * <tr>
     * <td>BIGINT</td>
     * <td>Long</td>
     * </tr>
     * <tr>
     * <td>BLOB</td>
     * <td>ByteBuffer</td>
     * </tr>
     * <tr>
     * <td>BOOLEAN</td>
     * <td>Boolean</td>
     * </tr>
     * <tr>
     * <td>COUNTER</td>
     * <td>Long</td>
     * </tr>
     * <tr>
     * <td>CUSTOM</td>
     * <td>ByteBuffer</td>
     * </tr>
     * <tr>
     * <td>DATE</td>
     * <td>java.sql.Date</td>
     * </tr>
     * <tr>
     * <td>DECIMAL</td>
     * <td>BigDecimal</td>
     * </tr>
     * <tr>
     * <td>DOUBLE</td>
     * <td>Double</td>
     * </tr>
     * <tr>
     * <td>DURATION</td>
     * <td>com.datastax.driver.core.Duration</td>
     * </tr>
     * <tr>
     * <td>FLOAT</td>
     * <td>Float</td>
     * </tr>
     * <tr>
     * <td>INET</td>
     * <td>InetAddress</td>
     * </tr>
     * <tr>
     * <td>INT</td>
     * <td>Integer</td>
     * </tr>
     * <tr>
     * <td>LIST</td>
     * <td>List</td>
     * </tr>
     * <tr>
     * <td>MAP</td>
     * <td>Map</td>
     * </tr>
     * <tr>
     * <td>SET</td>
     * <td>Set</td>
     * </tr>
     * <tr>
     * <td>SMALLINT</td>
     * <td>Short</td>
     * </tr>
     * <tr>
     * <td>TEXT</td>
     * <td>String</td>
     * </tr>
     * <tr>
     * <td>TIME</td>
     * <td>java.sql.Time</td>
     * </tr>
     * <tr>
     * <td>TIMESTAMP</td>
     * <td>java.sql.Timestamp</td>
     * </tr>
     * <tr>
     * <td>TIMEUUID</td>
     * <td>UUID</td>
     * </tr>
     * <tr>
     * <td>TINYINT</td>
     * <td>Byte</td>
     * </tr>
     * <tr>
     * <td>TUPLE</td>
     * <td>TupleValue</td>
     * </tr>
     * <tr>
     * <td>UDT</td>
     * <td>UDTValue</td>
     * </tr>
     * <tr>
     * <td>UUID</td>
     * <td>UUID</td>
     * </tr>
     * <tr>
     * <td>VARCHAR</td>
     * <td>String</td>
     * </tr>
     * <tr>
     * <td>VARINT</td>
     * <td>BigInteger</td>
     * </tr>
     * <--------------------------------------------------------------------
     * </table>
     *
     * @return the java Class corresponding to this CQL type name.
     */
    public Class<?> asJavaClass() {
        return javaType;
    }

    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }
}
