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

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static java.lang.String.format;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Test CQL Date/Time Data Types
 */
public class DateTimeTypesUnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(DateTimeTypesUnitTest.class);

    private static String HOST = System.getProperty("host", ConnectionDetails.getHost());
    private static final int PORT = Integer
        .parseInt(System.getProperty("port", ConnectionDetails.getPort() + ""));

    private static final String KEYSPACE = "testks4";
    private static final String SYSTEM = "system";
    private static final String CQLV3 = "3.0.0";

    private static final int DATECOL_IDX = 2;
    private static final int TIMECOL_IDX = 3;
    private static final int TIMESTAMPCOL_IDX = 4;

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

        if (LOG.isDebugEnabled()) {
            LOG.debug("URL         = '{}'", URL);
        }
        Statement stmt = con.createStatement();

        String useKS = String.format("USE %s;", KEYSPACE);

        String dropKS = String.format("DROP KEYSPACE \"%s\";", KEYSPACE);

        try {
            stmt.execute(dropKS);
        } catch (Exception e) {
            /* Exception on DROP is OK */
        }

        String createKS = String.format(
            "CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy',  'replication_factor' : 1  };",
            KEYSPACE);

        if (LOG.isDebugEnabled()) {
            LOG.debug("createKS    = '{}'", createKS);
        }
        stmt = con.createStatement();
        stmt.execute("USE " + SYSTEM);
        stmt.execute(createKS);
        stmt.execute(useKS);

        String createDateTimeTypesTable = "CREATE TABLE " + KEYSPACE
            + ".datetimetypes(intcol int primary key, datecol date, timecol time, timestampcol timestamp);";

        if (LOG.isDebugEnabled()) {
            LOG.debug("createTable = '{}'", createDateTimeTypesTable);
        }
        stmt.execute(createDateTimeTypesTable);
        stmt.close();
        con.close();

        // open it up again to see the new TABLE
        URL = String.format("jdbc:cassandra://%s:%d/%s?version=%s", HOST, PORT, KEYSPACE, CQLV3);
        con = DriverManager.getConnection(URL);

        if (LOG.isDebugEnabled()) {
            LOG.debug("URL         = '{}'", URL);
            LOG.debug("Unit Test: 'DateTimeTypesUnitTest' initialization complete.\n\n");
        }
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
    public void testDateDataType() throws Exception {

        LocalDate date = LocalDate.parse("2020-06-15");

        Statement stmt = con.createStatement();
        String insert = format("INSERT INTO datetimetypes (intcol, datecol) VALUES (1, '%s');", date);
        stmt.executeUpdate(insert);

        ResultSet result = stmt.executeQuery("SELECT * FROM datetimetypes WHERE intcol = 1;");
        result.next();

        long dbEpochMilli = ((com.datastax.driver.core.LocalDate)result.getObject(DATECOL_IDX))
            .getMillisSinceEpoch();

        Instant instant = date.atStartOfDay(ZoneOffset.UTC).toInstant();

        assertEquals(instant.toEpochMilli(), dbEpochMilli);

        stmt.close();
    }

    @Test
    public void testTimeDataType() throws Exception {

        String[] durations = { "PT12H34M56S", "PT12H34M56.789S" };

        Statement stmt = con.createStatement();

        for (int i=1; i <= durations.length; ++i) {
            Duration d = Duration.parse(durations[i-1]);

            if (LOG.isDebugEnabled()) {
                LOG.debug(format("[%s]", formatDuration(d)));
            }

            String insert = format("INSERT INTO datetimetypes (intcol, timecol) VALUES (%d, '%s');",
                i, formatDuration(d));
            stmt.executeUpdate(insert);

            ResultSet result = stmt.executeQuery(
                format("SELECT * FROM datetimetypes WHERE intcol = %d;", i));
            result.next();

            Time dbTime = (Time) result.getObject(TIMECOL_IDX);

            assertEquals(Instant.ofEpochMilli(d.toMillis()).toEpochMilli(),
                dbTime.getTime());
        }

        stmt.close();
    }

    @Test
    public void testTimestampDataType() throws Exception {

        String[] timestamps = {
            //"2020-01-10",
            //"2020-01-10 05:30",
            //"2020-01-10 05:30:15",
            //"2020-01-10T05:30:15.123",
            "2020-01-10T05:30:15.123-0800",
            "2020-01-10T05:30:15.123+0000",
            "2020-01-10T05:30:15+0100",
            "2020-01-10T05:30:15.123+0130",
            //"1999-11-23T23:59:59.999",
        };

        Statement stmt = con.createStatement();

        for (int i=1; i <= timestamps.length; ++i) {
            String timestamp = timestamps[i-1];

            if (LOG.isDebugEnabled()) {
                LOG.debug(format("[%s]", timestamp));
            }

            String insert = format(
                "INSERT INTO datetimetypes (intcol, timestampcol) VALUES (%d, '%s');",
                i, timestamp);
            stmt.executeUpdate(insert);

            ResultSet result = stmt.executeQuery(
                format("SELECT * FROM datetimetypes WHERE intcol = %d;", i));
            result.next();

            Timestamp dbTimestamp = (Timestamp) result.getObject(TIMESTAMPCOL_IDX);

            assertEquals(Utils.parseTimestamp(timestamp).getTime(), dbTimestamp.getTime());
        }

        stmt.close();
    }

    private String formatDuration(Duration dur) {
        long secs = dur.getSeconds();
        int nanosecs = dur.getNano();

        if (nanosecs != 0) {
            return format("%02d:%02d:%02d.%03d",
                secs/3600, (secs % 3600)/60, (secs % 60), nanosecs);
        } else {
            return format("%02d:%02d:%02d", secs/3600, (secs % 3600)/60, (secs % 60));
        }
    }
}
