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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class UtilsUnitTest {
    private static final Logger LOG = LoggerFactory.getLogger(CollectionsUnitTest.class);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Test
    public void testParseURL() throws Exception {
        String happypath = "jdbc:cassandra://localhost:9042/Keyspace1?version=3.0.0&consistency=QUORUM";
        Properties props = Utils.parseURL(happypath);
        assertEquals("localhost", props.getProperty(Utils.TAG_SERVER_NAME));
        assertEquals("9042", props.getProperty(Utils.TAG_PORT_NUMBER));
        assertEquals("Keyspace1", props.getProperty(Utils.TAG_DATABASE_NAME));
        assertEquals("3.0.0", props.getProperty(Utils.TAG_CQL_VERSION));
        assertEquals("QUORUM", props.getProperty(Utils.TAG_CONSISTENCY_LEVEL));

        String consistencyonly = "jdbc:cassandra://localhost/Keyspace1?consistency=QUORUM";
        props = Utils.parseURL(consistencyonly);
        assertEquals("localhost", props.getProperty(Utils.TAG_SERVER_NAME));
        assertEquals("9042", props.getProperty(Utils.TAG_PORT_NUMBER));
        assertEquals("Keyspace1", props.getProperty(Utils.TAG_DATABASE_NAME));
        assertEquals("QUORUM", props.getProperty(Utils.TAG_CONSISTENCY_LEVEL));
        assert (props.getProperty(Utils.TAG_CQL_VERSION) == null);

        String noport = "jdbc:cassandra://localhost/Keyspace1?version=2.0.0";
        props = Utils.parseURL(noport);
        assertEquals("localhost", props.getProperty(Utils.TAG_SERVER_NAME));
        assertEquals("9042", props.getProperty(Utils.TAG_PORT_NUMBER));
        assertEquals("Keyspace1", props.getProperty(Utils.TAG_DATABASE_NAME));
        assertEquals("2.0.0", props.getProperty(Utils.TAG_CQL_VERSION));

        String noversion = "jdbc:cassandra://localhost:9042/Keyspace1";
        props = Utils.parseURL(noversion);
        assertEquals("localhost", props.getProperty(Utils.TAG_SERVER_NAME));
        assertEquals("9042", props.getProperty(Utils.TAG_PORT_NUMBER));
        assertEquals("Keyspace1", props.getProperty(Utils.TAG_DATABASE_NAME));
        assert (props.getProperty(Utils.TAG_CQL_VERSION) == null);

        String nokeyspaceonly = "jdbc:cassandra://localhost:9042?version=2.0.0";
        props = Utils.parseURL(nokeyspaceonly);
        assertEquals("localhost", props.getProperty(Utils.TAG_SERVER_NAME));
        assertEquals("9042", props.getProperty(Utils.TAG_PORT_NUMBER));
        assert (props.getProperty(Utils.TAG_DATABASE_NAME) == null);
        assertEquals("2.0.0", props.getProperty(Utils.TAG_CQL_VERSION));

        String nokeyspaceorver = "jdbc:cassandra://localhost:9042";
        props = Utils.parseURL(nokeyspaceorver);
        assertEquals("localhost", props.getProperty(Utils.TAG_SERVER_NAME));
        assertEquals("9042", props.getProperty(Utils.TAG_PORT_NUMBER));
        assert (props.getProperty(Utils.TAG_DATABASE_NAME) == null);
        assert (props.getProperty(Utils.TAG_CQL_VERSION) == null);

        String withloadbalancingpolicy = "jdbc:cassandra://localhost:9042?loadbalancing=TokenAwarePolicy-DCAwareRoundRobinPolicy&primarydc=DC1";
        props = Utils.parseURL(withloadbalancingpolicy);
        assertEquals("localhost", props.getProperty(Utils.TAG_SERVER_NAME));
        assertEquals("9042", props.getProperty(Utils.TAG_PORT_NUMBER));
        assert (props.getProperty(Utils.TAG_DATABASE_NAME) == null);
        assert (props.getProperty(Utils.TAG_CQL_VERSION) == null);
        assertEquals("TokenAwarePolicy-DCAwareRoundRobinPolicy",
                props.getProperty(Utils.TAG_LOADBALANCING_POLICY));
        assertEquals("DC1", props.getProperty(Utils.TAG_PRIMARY_DC));
    }

    @Test
    public void testLoadBalancingPolicyParsing() throws Exception {
        String lbPolicyStr = "RoundRobinPolicy()";
        System.out.println(lbPolicyStr);
        assertTrue(Utils.parseLbPolicy(lbPolicyStr) instanceof RoundRobinPolicy);
        System.out.println("====================");
        lbPolicyStr = "TokenAwarePolicy(RoundRobinPolicy())";
        System.out.println(lbPolicyStr);
        assertTrue(Utils.parseLbPolicy(lbPolicyStr) instanceof TokenAwarePolicy);
        System.out.println("====================");
        lbPolicyStr = "DCAwareRoundRobinPolicy(\"dc1\")";
        System.out.println(lbPolicyStr);
        assertTrue(Utils.parseLbPolicy(lbPolicyStr) instanceof DCAwareRoundRobinPolicy);
        System.out.println("====================");
        lbPolicyStr = "TokenAwarePolicy(DCAwareRoundRobinPolicy(\"dc1\"))";
        System.out.println(lbPolicyStr);
        assertTrue(Utils.parseLbPolicy(lbPolicyStr) instanceof TokenAwarePolicy);
        System.out.println("====================");
        lbPolicyStr = "TokenAwarePolicy";
        System.out.println(lbPolicyStr);
        assertTrue(Utils.parseLbPolicy(lbPolicyStr) == null);
        System.out.println("====================");
        lbPolicyStr = "LatencyAwarePolicy(TokenAwarePolicy(RoundRobinPolicy()),(double) 10.5,(long) 1,(long) 10,(long)1,10)";
        System.out.println(lbPolicyStr);
        assertTrue(Utils.parseLbPolicy(lbPolicyStr) instanceof LatencyAwarePolicy);
        System.out.println("====================");

    }

    @Test
    public void testRetryPolicyParsing() throws Exception {
        String retryPolicyStr = "DefaultRetryPolicy";
        System.out.println(retryPolicyStr);
        assertTrue(Utils.parseRetryPolicy(retryPolicyStr) instanceof DefaultRetryPolicy);
        System.out.println("====================");
        retryPolicyStr = "DowngradingConsistencyRetryPolicy";
        System.out.println(retryPolicyStr);
        assertTrue(Utils
                .parseRetryPolicy(retryPolicyStr) instanceof DowngradingConsistencyRetryPolicy);
        System.out.println("====================");
        retryPolicyStr = "FallthroughRetryPolicy";
        System.out.println(retryPolicyStr);
        assertTrue(Utils.parseRetryPolicy(retryPolicyStr) instanceof FallthroughRetryPolicy);
        System.out.println("====================");

    }

    @Test
    public void testReconnectionPolicyParsing() throws Exception {
        String retryPolicyStr = "ConstantReconnectionPolicy((long)10)";
        System.out.println(retryPolicyStr);
        assertTrue(Utils
                .parseReconnectionPolicy(retryPolicyStr) instanceof ConstantReconnectionPolicy);
        System.out.println("====================");
        retryPolicyStr = "ExponentialReconnectionPolicy((long)10,(Long)100)";
        System.out.println(retryPolicyStr);
        assertTrue(Utils
                .parseReconnectionPolicy(retryPolicyStr) instanceof ExponentialReconnectionPolicy);
        System.out.println("====================");

    }

    @Test
    public void testCreateSubName() throws Exception {
        String happypath = "jdbc:cassandra://localhost:9042/Keyspace1?consistency=QUORUM&version=3.0.0";
        Properties props = Utils.parseURL(happypath);

        if (LOG.isDebugEnabled())
            LOG.debug("happypath    = '{}'", happypath);

        String result = Utils.createSubName(props);
        if (LOG.isDebugEnabled())
            LOG.debug("result       = '{}'", Utils.PROTOCOL + result);

        assertEquals(happypath, Utils.PROTOCOL + result);
    }

    @Test
    public void testParseDate() throws Exception {
        assertNull(Utils.parseDate(null));
        assertNull(Utils.parseDate(""));

        Date d = Utils.parseDate("2020-01-15");
        assertEquals(Utils.formatDate(d), "2020-01-15");

        try {
            Utils.parseDate("bad-date");
            fail();
        } catch (SQLException expected) {
            assertEquals(expected.getMessage(), Utils.BAD_DATE_FORMAT);
        }
    }

    @Test
    public void testParseTime() throws Exception {
        assertNull(Utils.parseTime(null));
        assertNull(Utils.parseTime(""));

        String input[] = {
          "15:26:00",
          "00:30:45.1234567"
        };

        String expected[] = {
          "15:26:00",
          "00:30:45" // java.sql.Time truncates to second.
        };

        for (int i=0; i < input.length; ++i) {
            Time t = Utils.parseTime(input[i]);
            assertEquals(Utils.formatTime(t), expected[i]);
        }

        try {
            Utils.parseTime("bad-time");
            fail();
        } catch (SQLException ex) {
            assertEquals(ex.getMessage(), Utils.BAD_TIME_FORMAT);
        }
    }

    @Test
    public void testParseTimestampValidFormat() throws Exception {

        // interpret date time without timezone as local time
        String input[] = {
          "2020-01-10",
          "   2020-01-10    ",
          "2020-01-10 05:30",
          "2020-01-10 05:30:15",
          "2020-01-10T05:30:15.123",
          "2020-01-10T05:30:15.123-0800",
          "2020-01-10T05:30:15.123+0000",
          "2020-01-10T05:30:15+0100",
          "2020-01-10T05:30:15.123+0130",
          "2020-01-10T05:30+0130",
          "1999-11-23T23:59:59.999"
        };

        // database stores time in utc
        String expected[] = {
          "2020-01-10T08:00:00Z",
          "2020-01-10T08:00:00Z",
          "2020-01-10T13:30:00Z",
          "2020-01-10T13:30:15Z",
          "2020-01-10T13:30:15.123Z",
          "2020-01-10T13:30:15.123Z",   // -0800
          "2020-01-10T05:30:15.123Z",   // +0000
          "2020-01-10T04:30:15Z",       // +0100
          "2020-01-10T04:00:15.123Z",   // +0130
          "2020-01-10T04:00:00Z",       // +0130
          "1999-11-24T07:59:59.999Z"
        };

        for (int i=0; i < input.length; ++i) {
            Timestamp ts = Utils.parseTimestamp(input[i]);
            assertEquals(Utils.formatTimestampAsEpoch(ts), expected[i]);
        }
    }

    @Test
    public void testParseTimestampInvalidFormat() throws Exception {
        assertNull(Utils.parseTimestamp(null));
        assertNull(Utils.parseTimestamp(""));

        String input[] = {
          "2020-01-10 T05:30:15",
          "2020-01-10T 05:30:15",
          "2020-01-10  05:30:15",
          "2020-01-10T05:30.123",
        };

        for (int i=0; i < input.length; ++i) {
            try {
                Utils.parseTimestamp(input[i]);
                fail();
            } catch (SQLException ex) {
                assertEquals(ex.getMessage(), Utils.BAD_TIMESTAMP_FORMAT);
            }
        }
    }
}
