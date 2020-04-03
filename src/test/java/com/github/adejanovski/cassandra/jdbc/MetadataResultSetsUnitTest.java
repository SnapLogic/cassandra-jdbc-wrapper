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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MetadataResultSetsUnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataResultSetsUnitTest.class);

    private static String HOST = System.getProperty("host", ConnectionDetails.getHost());
    private static final int PORT = Integer
            .parseInt(System.getProperty("port", ConnectionDetails.getPort() + ""));
    private static final String KEYSPACE1 = "testks6";
    private static final String KEYSPACE2 = "testks7";
    private static final String DROP_KS = "DROP KEYSPACE \"%s\";";
    private static final String CREATE_KS = "CREATE KEYSPACE IF NOT EXISTS \"%s\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};";

    private static java.sql.Connection con = null;

    @SuppressWarnings("unused")
    private static CCMBridge ccmBridge = null;

    private static boolean suiteLaunch = true;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        /* System.setProperty("cassandra.version", "2.1.2"); */

        if (BuildCluster.HOST.equals(System.getProperty("host", ConnectionDetails.getHost()))) {
            BuildCluster.setUpBeforeSuite();
            suiteLaunch = false;
        }

        HOST = CCMBridge.ipOfNode(1);

        Class.forName("com.github.adejanovski.cassandra.jdbc.CassandraDriver");
        String URL = String.format("jdbc:cassandra://%s:%d/%s?version=3.0.0", HOST, PORT, "system");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Connection URL = '" + URL + "'");
        }

        con = DriverManager.getConnection(URL);
        Statement stmt = con.createStatement();

        // Drop Keyspace
        String dropKS1 = String.format(DROP_KS, KEYSPACE1);
        String dropKS2 = String.format(DROP_KS, KEYSPACE2);

        try {
            stmt.execute(dropKS1);
            stmt.execute(dropKS2);
        } catch (Exception e) {
            /* Exception on DROP is OK */}

        // Create KeySpace
        String createKS1 = String.format(CREATE_KS, KEYSPACE1);
        String createKS2 = String.format(CREATE_KS, KEYSPACE2);
        stmt = con.createStatement();
        stmt.execute("USE system;");
        stmt.execute(createKS1);
        stmt.execute(createKS2);

        // Use Keyspace
        String useKS1 = String.format("USE \"%s\";", KEYSPACE1);
        String useKS2 = String.format("USE \"%s\";", KEYSPACE2);
        stmt.execute(useKS1);

        // Create the target Column family
        String createCF1 = "CREATE COLUMNFAMILY test1 (keyname text PRIMARY KEY,"
                + " t1bValue boolean," + " t1iValue int"
                + ") WITH comment = 'first TABLE in the Keyspace'" + ";";

        String createCF2 = "CREATE COLUMNFAMILY test2 (keyname text PRIMARY KEY,"
                + " t2bValue boolean," + " t2iValue int"
                + ") WITH comment = 'second TABLE in the Keyspace'" + ";";

        stmt.execute(createCF1);
        stmt.execute(createCF2);
        stmt.execute(useKS2);
        stmt.execute(createCF1);
        stmt.execute(createCF2);

        stmt.close();
        con.close();

        // open it up again to see the new CF
        con = DriverManager.getConnection(
                String.format("jdbc:cassandra://%s:%d/%s?version=3.0.0", HOST, PORT, KEYSPACE1));

    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        if (con != null)
            con.close();
        if (!suiteLaunch) {
            BuildCluster.tearDownAfterSuite();
        }

    }

    private final String showColumn(int index, ResultSet result) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(index).append("]");
        sb.append(result.getObject(index));
        return sb.toString();
    }

    private final String toString(ResultSet result) throws SQLException {
        StringBuilder sb = new StringBuilder();

        while (result.next()) {
            ResultSetMetaData metadata = result.getMetaData();
            int colCount = metadata.getColumnCount();
            sb.append(String.format("(%d) ", result.getRow()));
            for (int i = 1; i <= colCount; i++) {
                sb.append(" " + showColumn(i, result));
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    private final String getColumnNames(ResultSetMetaData metaData) throws SQLException {
        StringBuilder sb = new StringBuilder();
        int count = metaData.getColumnCount();
        for (int i = 1; i <= count; i++) {
            sb.append(metaData.getColumnName(i));
            if (i < count)
                sb.append(", ");
        }
        return sb.toString();
    }

    // TESTS ------------------------------------------------------------------

    @Test
    public void testTableType() throws SQLException {
        CassandraStatement statement = (CassandraStatement) con.createStatement();
        ResultSet result = MetadataResultSets.instance.makeTableTypes(statement);

        if (LOG.isDebugEnabled()) {
            LOG.debug(toString(result));
        }
    }

    @Test
    public void testCatalogs() throws SQLException {
        CassandraStatement statement = (CassandraStatement) con.createStatement();
        ResultSet result = MetadataResultSets.instance.makeCatalogs(statement);

        if (LOG.isDebugEnabled()) {
            LOG.debug(toString(result));
        }
    }

    @Test
    public void testSchemas() throws SQLException {
        CassandraStatement statement = (CassandraStatement) con.createStatement();
        ResultSet result = MetadataResultSets.instance.makeSchemas(statement, null);

        if (LOG.isDebugEnabled()) {
            LOG.debug(getColumnNames(result.getMetaData()));
            LOG.debug(toString(result));
        }

        result = MetadataResultSets.instance.makeSchemas(statement, KEYSPACE2);
        if (LOG.isDebugEnabled()) {
            LOG.debug(toString(result));
        }
    }

    @Test
    public void testTables() throws SQLException {
        CassandraStatement statement = (CassandraStatement) con.createStatement();
        ResultSet result = MetadataResultSets.instance.makeTables(statement, null, null);

        if (LOG.isDebugEnabled()) {
            LOG.debug(getColumnNames(result.getMetaData()));
            LOG.debug(toString(result));
        }

        result = MetadataResultSets.instance.makeTables(statement, KEYSPACE2, null);
        if (LOG.isDebugEnabled()) {
            LOG.debug(toString(result));
        }

        result = MetadataResultSets.instance.makeTables(statement, null, "test1");
        if (LOG.isDebugEnabled()) {
            LOG.debug(toString(result));
        }

        result = MetadataResultSets.instance.makeTables(statement, KEYSPACE2, "test1");
        if (LOG.isDebugEnabled()) {
            LOG.debug(toString(result));
        }
    }

    @Test
    public void testColumns() throws SQLException {
        CassandraStatement statement = (CassandraStatement) con.createStatement();
        ResultSet result = MetadataResultSets.instance.makeColumns(statement, KEYSPACE1, "test1",
                null);

        if (LOG.isDebugEnabled()) {
            LOG.debug(getColumnNames(result.getMetaData()));
            LOG.debug(toString(result));
        }

        result = MetadataResultSets.instance.makeColumns(statement, KEYSPACE1, "test2", null);

        if (LOG.isDebugEnabled()) {
            LOG.debug(getColumnNames(result.getMetaData()));
            LOG.debug(toString(result));
        }

        // verify DECIMAL_DIGITS column data doesn't return null which bubbles
        // up as a NumberFormatException. This column data is queried by
        // the Snap jdbc code.
        assertEquals(0, result.getInt("DECIMAL_DIGITS"));
    }

    @Test
    public void testCollectionsMetadata() throws Exception {
        Statement stmt = con.createStatement();
        // java.util.Date now = new java.util.Date();

        // Create the target Column family with each basic data type available on Cassandra

        String createCF = "CREATE TABLE " + KEYSPACE1
                + ".collections_metadata(part_key text PRIMARY KEY, set1 set<text>, description text, map2 map<text,int>, list3 list<text>);";

        stmt.execute(createCF);
        stmt.close();

        Statement statement = con.createStatement();
        String insert = "INSERT INTO " + KEYSPACE1
                + ".collections_metadata(part_key, set1, description, map2, list3) VALUES ('part_key',{'val1','val2','val3'},'desc',{'val1':1,'val2':2},['val1','val2']);";

        ResultSet result = statement.executeQuery(insert);

        result = statement.executeQuery("select * from " + KEYSPACE1 + ".collections_metadata");

        assertTrue(result.next());
        assertEquals(5, result.getMetaData().getColumnCount());

        if (LOG.isDebugEnabled()) {
            for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
                LOG.debug("getColumnName : " + result.getMetaData().getColumnName(i));
                LOG.debug("getCatalogName : " + result.getMetaData().getCatalogName(i));
                LOG.debug("getColumnClassName : " + result.getMetaData().getColumnClassName(i));
                LOG.debug("getColumnDisplaySize : " + result.getMetaData().getColumnDisplaySize(i));
                LOG.debug("getColumnLabel : " + result.getMetaData().getColumnLabel(i));
                LOG.debug("getColumnType : " + result.getMetaData().getColumnType(i));
                LOG.debug("getColumnTypeName : " + result.getMetaData().getColumnTypeName(i));
                LOG.debug("getPrecision : " + result.getMetaData().getPrecision(i));
                LOG.debug("getScale : " + result.getMetaData().getScale(i));
                LOG.debug("getSchemaName : " + result.getMetaData().getSchemaName(i));
                LOG.debug("getTableName : " + result.getMetaData().getTableName(i));
            }
        }

        // columns are apparently ordered alphabetically and not according to the query order
        assertEquals("part_key", result.getMetaData().getColumnName(1));
        assertEquals("description", result.getMetaData().getColumnName(2));
        assertEquals("list3", result.getMetaData().getColumnName(3));
        assertEquals("map2", result.getMetaData().getColumnName(4));
        assertEquals("set1", result.getMetaData().getColumnName(5));

        assertEquals("part_key", result.getMetaData().getColumnLabel(1));
        assertEquals("description", result.getMetaData().getColumnLabel(2));
        assertEquals("list3", result.getMetaData().getColumnLabel(3));
        assertEquals("map2", result.getMetaData().getColumnLabel(4));
        assertEquals("set1", result.getMetaData().getColumnLabel(5));

        assertEquals("collections_metadata", result.getMetaData().getTableName(1));
        assertEquals("collections_metadata", result.getMetaData().getTableName(2));
        assertEquals("collections_metadata", result.getMetaData().getTableName(3));
        assertEquals("collections_metadata", result.getMetaData().getTableName(4));
        assertEquals("collections_metadata", result.getMetaData().getTableName(5));

        assertEquals("java.lang.String", result.getMetaData().getColumnClassName(1));
        assertEquals("java.lang.String", result.getMetaData().getColumnClassName(2));
        assertEquals("java.util.List", result.getMetaData().getColumnClassName(3));
        assertEquals("java.util.Map", result.getMetaData().getColumnClassName(4));
        assertEquals("java.util.Set", result.getMetaData().getColumnClassName(5));

        assertEquals(12, result.getMetaData().getColumnType(1));
        assertEquals(12, result.getMetaData().getColumnType(2));
        assertEquals(1111, result.getMetaData().getColumnType(3));
        assertEquals(1111, result.getMetaData().getColumnType(4));
        assertEquals(1111, result.getMetaData().getColumnType(5));

        statement.close();

    }

    @Test
    public void testNumericTypesMetadata() throws Exception {
        Statement stmt = con.createStatement();

        String createNumericTypesTable = "CREATE TABLE " + KEYSPACE1
            + ".numerictypes(tinyintcol tinyint, smallintcol smallint, intcol int primary key,"
            + " bigintcol bigint, varintcol varint, decimalcol decimal, floatcol float, doublecol double);";

        stmt.execute(createNumericTypesTable);
        stmt.close();

        Statement statement = con.createStatement();
        String insert = "INSERT INTO " + KEYSPACE1
            + ".numerictypes(tinyintcol, smallintcol, intcol, bigintcol, varintcol, decimalcol, floatcol, doublecol)"
            + " VALUES (127, 32767, 2147483647, 9223372036854775807, 2147483647, 12.34, 12.23, 12.23);";

        ResultSet result = statement.executeQuery(insert);

        result = statement.executeQuery("select * from " + KEYSPACE1 + ".numerictypes");

        assertTrue(result.next());
        assertEquals(8, result.getMetaData().getColumnCount());

        // columns are ordered alphabetically, key column first (ie, not projection order)
        int i = 1;
        ResultSetMetaData rsmd = result.getMetaData();

        // column #1: intcol int primary key
        JdbcInt jdbcInt = new JdbcInt();
        assertEquals("intcol", rsmd.getColumnName(i));
        assertEquals(jdbcInt.getType().getCanonicalName(), rsmd.getColumnClassName(i));
        assertEquals(jdbcInt.getPrecision(null), rsmd.getColumnDisplaySize(i));
        assertEquals(jdbcInt.getJdbcType(), rsmd.getColumnType(i));
        assertEquals("int", rsmd.getColumnTypeName(i));
        assertEquals(0, rsmd.getScale(i));
        assertEquals(jdbcInt.isCaseSensitive(), rsmd.isCaseSensitive(i));
        assertEquals(jdbcInt.isCurrency(), rsmd.isCurrency(i));
        assertEquals(jdbcInt.isSigned(), rsmd.isSigned(i));
        i++;

        // column #2: bigintcol bigint
        JdbcLong jdbcLong = new JdbcLong();
        assertEquals("bigintcol", rsmd.getColumnName(i));
        assertEquals(jdbcLong.getType().getCanonicalName(), rsmd.getColumnClassName(i));
        assertEquals(jdbcLong.getPrecision(null), rsmd.getColumnDisplaySize(i));
        assertEquals(jdbcLong.getJdbcType(), rsmd.getColumnType(i));
        assertEquals("bigint", rsmd.getColumnTypeName(i));
        assertEquals(0, rsmd.getScale(i));
        assertEquals(jdbcLong.isCaseSensitive(), rsmd.isCaseSensitive(i));
        assertEquals(jdbcLong.isCurrency(), rsmd.isCurrency(i));
        assertEquals(jdbcLong.isSigned(), rsmd.isSigned(i));
        i++;

        // column #3: decimalcol decimal
        JdbcDecimal jdbcDecimal = new JdbcDecimal();
        assertEquals("decimalcol", rsmd.getColumnName(i));
        assertEquals(jdbcDecimal.getType().getCanonicalName(), rsmd.getColumnClassName(i));
        assertEquals(40, rsmd.getColumnDisplaySize(i));
        assertEquals(jdbcDecimal.getJdbcType(), rsmd.getColumnType(i));
        assertEquals("decimal", rsmd.getColumnTypeName(i));
        assertEquals(0, rsmd.getPrecision(i));
        assertEquals(0, rsmd.getScale(i));
        assertEquals(jdbcDecimal.isCaseSensitive(), rsmd.isCaseSensitive(i));
        assertEquals(jdbcDecimal.isCurrency(), rsmd.isCurrency(i));
        assertEquals(jdbcDecimal.isSigned(), rsmd.isSigned(i));
        i++;

        // column #4: doublecol double
        JdbcDouble jdbcDouble = new JdbcDouble();
        assertEquals("doublecol", rsmd.getColumnName(i));
        assertEquals(jdbcDouble.getType().getCanonicalName(), rsmd.getColumnClassName(i));
        assertEquals(jdbcDouble.getPrecision(null), rsmd.getColumnDisplaySize(i));
        assertEquals(jdbcDouble.getJdbcType(), rsmd.getColumnType(i));
        assertEquals("double", rsmd.getColumnTypeName(i));
        assertEquals(0, rsmd.getScale(i));
        assertEquals(jdbcDouble.isCaseSensitive(), rsmd.isCaseSensitive(i));
        assertEquals(jdbcDouble.isCurrency(), rsmd.isCurrency(i));
        assertEquals(jdbcDouble.isSigned(), rsmd.isSigned(i));
        i++;

        // column #5: floatcol float
        JdbcFloat jdbcFloat = new JdbcFloat();
        assertEquals("floatcol", rsmd.getColumnName(i));
        assertEquals(jdbcFloat.getType().getCanonicalName(), rsmd.getColumnClassName(i));
        assertEquals(jdbcFloat.getPrecision(null), rsmd.getColumnDisplaySize(i));
        assertEquals(jdbcFloat.getJdbcType(), rsmd.getColumnType(i));
        assertEquals("float", rsmd.getColumnTypeName(i));
        assertEquals(0, rsmd.getScale(i));
        assertEquals(jdbcFloat.isCaseSensitive(), rsmd.isCaseSensitive(i));
        assertEquals(jdbcFloat.isCurrency(), rsmd.isCurrency(i));
        assertEquals(jdbcFloat.isSigned(), rsmd.isSigned(i));
        i++;

        // column #6: smallintcol smallint
        JdbcShort jdbcShort = new JdbcShort();
        assertEquals("smallintcol", rsmd.getColumnName(i));
        assertEquals(jdbcShort.getType().getCanonicalName(), rsmd.getColumnClassName(i));
        assertEquals(jdbcShort.getPrecision(null), rsmd.getColumnDisplaySize(i));
        assertEquals(jdbcShort.getJdbcType(), rsmd.getColumnType(i));
        assertEquals("smallint", rsmd.getColumnTypeName(i));
        assertEquals(0, rsmd.getScale(i));
        assertEquals(jdbcShort.isCaseSensitive(), rsmd.isCaseSensitive(i));
        assertEquals(jdbcShort.isCurrency(), rsmd.isCurrency(i));
        assertEquals(jdbcShort.isSigned(), rsmd.isSigned(i));
        i++;

        // column #7: tinyintcol tinyint
        JdbcByte jdbcByte = new JdbcByte();
        assertEquals("tinyintcol", rsmd.getColumnName(i));
        assertEquals(jdbcByte.getType().getCanonicalName(), rsmd.getColumnClassName(i));
        assertEquals(jdbcByte.getPrecision(null), rsmd.getColumnDisplaySize(i));
        assertEquals(jdbcByte.getJdbcType(), rsmd.getColumnType(i));
        assertEquals("tinyint", rsmd.getColumnTypeName(i));
        assertEquals(0, rsmd.getScale(i));
        assertEquals(jdbcByte.isCaseSensitive(), rsmd.isCaseSensitive(i));
        assertEquals(jdbcByte.isCurrency(), rsmd.isCurrency(i));
        assertEquals(jdbcByte.isSigned(), rsmd.isSigned(i));
        i++;

        // column #8: varintcol varint
        JdbcBigInteger jdbcBigInteger = new JdbcBigInteger();
        assertEquals("varintcol", rsmd.getColumnName(i));
        assertEquals(jdbcBigInteger.getType().getCanonicalName(), rsmd.getColumnClassName(i));
        assertEquals(40, rsmd.getColumnDisplaySize(i));
        assertEquals(jdbcBigInteger.getJdbcType(), rsmd.getColumnType(i));
        assertEquals("varint", rsmd.getColumnTypeName(i));
        assertEquals(0, rsmd.getScale(i));
        assertEquals(jdbcBigInteger.isCaseSensitive(), rsmd.isCaseSensitive(i));
        assertEquals(jdbcBigInteger.isCurrency(), rsmd.isCurrency(i));
        assertEquals(jdbcBigInteger.isSigned(), rsmd.isSigned(i));
        i++;

        statement.close();
    }

    @Test
    public void testDateTimeTypesMetadata() throws Exception {
        Statement stmt = con.createStatement();

        String createDateTimeTypesTable = "CREATE TABLE " + KEYSPACE1
            + ".datetimetypes(datecol date primary key, timecol time, timestampcol timestamp);";

        stmt.execute(createDateTimeTypesTable);
        stmt.close();

        Statement statement = con.createStatement();
        String insert = "INSERT INTO " + KEYSPACE1
            + ".datetimetypes(datecol, timecol, timestampcol)"
            + " VALUES ('2018-01-31', '23:00:00.999', '2015-05-03 13:30:54.234-0800');";

        ResultSet result = statement.executeQuery(insert);

        result = statement.executeQuery("select * from " + KEYSPACE1 + ".datetimetypes");

        assertTrue(result.next());
        assertEquals(3, result.getMetaData().getColumnCount());

        // columns are ordered alphabetically, key column first (ie, not projection order)
        int i = 1;
        ResultSetMetaData rsmd = result.getMetaData();

        // column #1: datecol date primary key
        JdbcDate jdbcDate = new JdbcDate();
        assertEquals("datecol", rsmd.getColumnName(i));
        assertEquals(jdbcDate.getType().getCanonicalName(), rsmd.getColumnClassName(i));
        assertEquals(jdbcDate.getPrecision(null), rsmd.getColumnDisplaySize(i));
        assertEquals(jdbcDate.getJdbcType(), rsmd.getColumnType(i));
        assertEquals("date", rsmd.getColumnTypeName(i));
        assertEquals(0, rsmd.getScale(i));
        assertEquals(jdbcDate.isCaseSensitive(), rsmd.isCaseSensitive(i));
        assertEquals(jdbcDate.isCurrency(), rsmd.isCurrency(i));
        assertEquals(jdbcDate.isSigned(), rsmd.isSigned(i));
        i++;

        // column #2: timecol time
        JdbcTime jdbcTime = new JdbcTime();
        assertEquals("timecol", rsmd.getColumnName(i));
        assertEquals(jdbcTime.getType().getCanonicalName(), rsmd.getColumnClassName(i));
        assertEquals(jdbcTime.getPrecision(null), rsmd.getColumnDisplaySize(i));
        assertEquals(jdbcTime.getJdbcType(), rsmd.getColumnType(i));
        assertEquals("time", rsmd.getColumnTypeName(i));
        assertEquals(0, rsmd.getScale(i));
        assertEquals(jdbcTime.isCaseSensitive(), rsmd.isCaseSensitive(i));
        assertEquals(jdbcTime.isCurrency(), rsmd.isCurrency(i));
        assertEquals(jdbcTime.isSigned(), rsmd.isSigned(i));
        i++;

        // column #3: timestampcol timestamp
        JdbcTimestamp jdbcTimestamp = new JdbcTimestamp();
        assertEquals("timestampcol", rsmd.getColumnName(i));
        assertEquals(jdbcTimestamp.getType().getCanonicalName(), rsmd.getColumnClassName(i));
        assertEquals(jdbcTimestamp.getPrecision(null), rsmd.getColumnDisplaySize(i));
        assertEquals(jdbcTimestamp.getJdbcType(), rsmd.getColumnType(i));
        assertEquals("timestamp", rsmd.getColumnTypeName(i));
        assertEquals(0, rsmd.getScale(i));
        assertEquals(jdbcTimestamp.isCaseSensitive(), rsmd.isCaseSensitive(i));
        assertEquals(jdbcTimestamp.isCurrency(), rsmd.isCurrency(i));
        assertEquals(jdbcTimestamp.isSigned(), rsmd.isSigned(i));
        i++;

        statement.close();
    }
}
