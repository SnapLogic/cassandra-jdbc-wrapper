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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test CQL Numeric Data Types
 */
public class NumericTypesUnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(NumericTypesUnitTest.class);

    private static String HOST = System.getProperty("host", ConnectionDetails.getHost());
    private static final int PORT = Integer
        .parseInt(System.getProperty("port", ConnectionDetails.getPort() + ""));

    private static final String KEYSPACE = "testks";
    private static final String SYSTEM = "system";
    private static final String CQLV3 = "3.0.0";

    private static java.sql.Connection con = null;

    @SuppressWarnings("unused")
    private static CCMBridge ccmBridge = null;

    private static boolean suiteLaunch = true;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        if (BuildCluster.HOST.equals(System.getProperty("host", ConnectionDetails.getHost()))) {
            BuildCluster.setUpBeforeSuite();
            suiteLaunch = false;
        }
        HOST = CCMBridge.ipOfNode(1);

        Class.forName("com.github.adejanovski.cassandra.jdbc.CassandraDriver");
        String URL = String.format("jdbc:cassandra://%s:%d/%s?version=%s",
            HOST, PORT, SYSTEM, CQLV3);

        con = DriverManager.getConnection(URL);

        if (LOG.isDebugEnabled())
            LOG.debug("URL         = '{}'", URL);

        Statement stmt = con.createStatement();

        String useKS = String.format("USE %s;", KEYSPACE);

        String dropKS = String.format("DROP KEYSPACE %s;", KEYSPACE);

        try {
            stmt.execute(dropKS);
        } catch (Exception e) {
            /* Exception on DROP is OK */
        }

        String createKS = String.format(
            "CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy',  'replication_factor' : 1  };",
            KEYSPACE);

        if (LOG.isDebugEnabled())
            LOG.debug("createKS    = '{}'", createKS);

        stmt = con.createStatement();
        stmt.execute("USE " + SYSTEM);
        stmt.execute(createKS);
        stmt.execute(useKS);

        String createDecimalTypeTable = "CREATE TABLE " + KEYSPACE
            + ".numerictypes(tinyintcol tinyint, smallintcol smallint, intcol int primary key,"
            + " bigintcol bigint, varintcol varint, decimalcol decimal, floatcol float, doublecol double);";

        if (LOG.isDebugEnabled())
            LOG.debug("createTable = '{}'", createDecimalTypeTable);

        stmt.execute(createDecimalTypeTable);
        stmt.close();
        con.close();

        // open it up again to see the new TABLE
        URL = String.format("jdbc:cassandra://%s:%d/%s?version=%s", HOST, PORT, KEYSPACE, CQLV3);
        con = DriverManager.getConnection(URL);
        if (LOG.isDebugEnabled())
            LOG.debug("URL         = '{}'", URL);

        Statement statement = con.createStatement();

        String insert = "INSERT INTO " + KEYSPACE
            + ".numerictypes(intcol, tinyintcol, smallintcol, bigintcol, varintcol, decimalcol, floatcol, doublecol)"
            + " VALUES (1, -128, -32768, -9223372036854775808, 12345, -23.34, 1.23456, 1.2345678901234);";
        statement.executeUpdate(insert);

        if (LOG.isDebugEnabled())
            LOG.debug("Unit Test: 'NumericTypesUnitTest' initialization complete.\n\n");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        if (con != null) {
            con.close();
        }
        if (!suiteLaunch) {
            BuildCluster.tearDownAfterSuite();
        }
    }

    @Test
    public void testSelect() throws Exception {
        Statement statement = con.createStatement();

        ResultSet result = statement.executeQuery("SELECT * FROM numerictypes WHERE intcol = 1;");
        result.next();

        ResultSetMetaData metadata = result.getMetaData();
        int colCount = metadata.getColumnCount();
        assertEquals(8, colCount);

        // columns ordered alphabetically, key column first
        assertEquals(1, result.getObject(1));
        assertEquals(-9223372036854775808L, result.getObject(2));
        assertEquals(new BigDecimal("-23.34"), result.getObject(3));
        assertEquals(1.2345678901234d, result.getObject(4));
        assertEquals(1.23456f, result.getObject(5));
        assertEquals(Short.MIN_VALUE, result.getObject(6));
        assertEquals(Byte.MIN_VALUE, result.getObject(7));
        assertEquals(new BigInteger("12345"), result.getObject(8));

        statement.close();
    }

    @Test
    public void testUpdate() throws Exception {
        String update = "UPDATE numerictypes SET tinyintcol = ? WHERE intcol = 1;";

        PreparedStatement pstmt = con.prepareStatement(update);
        pstmt.setObject(1, Byte.MAX_VALUE);
        pstmt.executeUpdate();

        ResultSet result = pstmt.executeQuery("SELECT * FROM numerictypes;");
        result.next();
        assertEquals(Byte.MAX_VALUE, result.getObject(7));

        pstmt.close();
    }

    @Test
    public void testDecimalType() throws Exception {

        String createDecimalTypeTable = "CREATE TABLE " + KEYSPACE
            + ".decimaltype(key int primary key, decimalcol decimal)";

        try (Statement stmt = con.createStatement();) {
            stmt.execute(createDecimalTypeTable);
        }

        String decimalValues[] = {
            "0",
            "0.123456789",
            "0.1234567890123456789012345",
            "10123456789",
            "123.456",
            "123456789",
            "123456789.123456789"
        };

        String insert = "INSERT INTO " + KEYSPACE + ".decimaltype(key, decimalcol) values (?, ?);";

        try (PreparedStatement pstmt = con.prepareStatement(insert);) {
            for (int i=1; i <= decimalValues.length; ++i) {
                pstmt.setInt(1, i);

                // test setters (setObject, setBigDecimal)
                if (i % 2 == 0) {
                    pstmt.setBigDecimal(2, new BigDecimal(decimalValues[i-1]));
                } else {
                    pstmt.setObject(2, new BigDecimal(decimalValues[i-1]));
                }
                pstmt.execute();
            }
        }

        try (Statement stmt = con.createStatement();) {
            ResultSet result = stmt.executeQuery(
                "select count(*) from " + KEYSPACE + ".decimaltype");
            result.next();
            assertEquals(decimalValues.length, result.getInt(1));   // row count

            result = stmt.executeQuery(
                "select * from " + KEYSPACE + ".decimaltype");

            while(result.next()) {
                int key = result.getInt(1);
                BigDecimal expected = new BigDecimal(decimalValues[key-1]);
                assertEquals(expected, result.getObject(2));
            }
        }
    }
}
