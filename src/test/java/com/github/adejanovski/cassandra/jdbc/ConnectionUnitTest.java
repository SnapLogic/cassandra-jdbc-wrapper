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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLNonTransientException;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

//import com.datastax.driver.core.CCMBridge;

public class ConnectionUnitTest {
    private static String HOST = System.getProperty("host", ConnectionDetails.getHost());
    private static int PORT = Integer
            .parseInt(System.getProperty("port", ConnectionDetails.getPort() + ""));
    private static final String KEYSPACE = "system";

    // data-center instance name ($ ccm node1 ring)
    private static final String DATACENTER = "datacenter1";

    // private static final String CQLV3 = "3.0.0";
    // private static final String CONSISTENCY_QUORUM = "QUORUM";

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

    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        if (con != null)
            con.close();
        if (!suiteLaunch) {
            BuildCluster.tearDownAfterSuite();
        }
    }

    @Test
    public void loadBalancingPolicyTest() throws SQLException {
        System.out.println("Connecting to : " + String.format("jdbc:cassandra://%s:%d/%s", HOST,
                PORT, KEYSPACE + "?debug=true&loadbalancing=RoundRobinPolicy()"));
        System.out.println("Con1...");
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s", HOST, PORT,
                KEYSPACE + "?debug=true&loadbalancing=RoundRobinPolicy()"));
        System.out.println("Con2...");
        Connection con2 = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s",
                HOST, PORT, KEYSPACE + "?debug=true&loadbalancing=RoundRobinPolicy()"));
        con2.close();
        con.close();

        System.out.println("Connecting to : " + String.format("jdbc:cassandra://%s:%d/%s", HOST,
                PORT, KEYSPACE + "?debug=true&loadbalancing=TokenAwarePolicy(RoundRobinPolicy())"));
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s", HOST, PORT,
                KEYSPACE + "?debug=true&loadbalancing=TokenAwarePolicy(RoundRobinPolicy())"));
        con.close();

        System.out.println("Connecting to : " + String.format(
            "jdbc:cassandra://%s:%d/%s?debug=true&loadbalancing=DCAwareRoundRobinPolicy('%s')",
            HOST, PORT, KEYSPACE, DATACENTER));

        con = DriverManager.getConnection(String.format(
            "jdbc:cassandra://%s:%d/%s?debug=true&loadbalancing=DCAwareRoundRobinPolicy('%s')",
            HOST, PORT, KEYSPACE, DATACENTER));

        con.close();

        System.out.println( "Connecting to : " + String.format(
            "jdbc:cassandra://%s:%d/%s?debug=true&loadbalancing=TokenAwarePolicy(DCAwareRoundRobinPolicy('%s'))",
            HOST, PORT, KEYSPACE, DATACENTER));

        con = DriverManager.getConnection(String.format(
            "jdbc:cassandra://%s:%d/%s?debug=true&loadbalancing=TokenAwarePolicy(DCAwareRoundRobinPolicy('%s'))",
            HOST, PORT, KEYSPACE, DATACENTER));

        con.close();

        System.out.println(
                "Connecting to : " + String.format("jdbc:cassandra://%s:%d/%s", HOST, PORT, KEYSPACE
                        + "?debug=true&loadbalancing=LatencyAwarePolicy(TokenAwarePolicy(RoundRobinPolicy()),(double)10.5,(long)1,(long)10,(long)1,10)"));
        con = DriverManager
                .getConnection(String.format("jdbc:cassandra://%s:%d/%s", HOST, PORT, KEYSPACE
                        + "?debug=true&loadbalancing=LatencyAwarePolicy(TokenAwarePolicy(RoundRobinPolicy()),(double)10.5,(long)1,(long)10,(long)1,10)"));
        con.close();

    }

    @Test(expectedExceptions = SQLNonTransientException.class)
    public void latencyAwarePolicyFailTest() throws SQLException {
        System.out.println(
                "Connecting to : " + String.format("jdbc:cassandra://%s:%d/%s", HOST, PORT, KEYSPACE
                        + "?debug=true&loadbalancing=LatencyAwarePolicy(TokenAwarePolicy(RoundRobinPolicy()),(double)10.5,(long)1,(long)10,(int)1,10)"));
        con = DriverManager
                .getConnection(String.format("jdbc:cassandra://%s:%d/%s", HOST, PORT, KEYSPACE
                        + "?debug=true&loadbalancing=LatencyAwarePolicy(TokenAwarePolicy(RoundRobinPolicy()),(double)10.5,(long)1,(long)10,(int)1,10)"));
        con.close();
    }

    @Test
    public void latencyAwarePolicyFailPassTest() throws SQLException {
        System.out.println(
                "Connecting to : " + String.format("jdbc:cassandra://%s:%d/%s", HOST, PORT, KEYSPACE
                        + "?debug=false&loadbalancing=LatencyAwarePolicy(TokenAwarePolicy(RoundRobinPolicy()),(double)10.5,(long)1,(long)10,(int)1,10)"));
        con = DriverManager
                .getConnection(String.format("jdbc:cassandra://%s:%d/%s", HOST, PORT, KEYSPACE
                        + "?debug=false&loadbalancing=LatencyAwarePolicy(TokenAwarePolicy(RoundRobinPolicy()),(double)10.5,(long)1,(long)10,(int)1,10)"));
        con.close();
    }

    @Test
    public void retryPolicyTest() throws SQLException {

        System.out.println("Connecting to : " + String.format("jdbc:cassandra://%s:%d/%s", HOST,
                PORT, KEYSPACE + "?debug=true&retry=DefaultRetryPolicy"));
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s", HOST, PORT,
                KEYSPACE + "?debug=true&retry=DefaultRetryPolicy"));
        con.close();

        System.out.println("Connecting to : " + String.format("jdbc:cassandra://%s:%d/%s", HOST,
                PORT, KEYSPACE + "?debug=true&retry=DowngradingConsistencyRetryPolicy"));
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s", HOST, PORT,
                KEYSPACE + "?debug=true&retry=DowngradingConsistencyRetryPolicy"));
        con.close();

        System.out.println("Connecting to : " + String.format("jdbc:cassandra://%s:%d/%s", HOST,
                PORT, KEYSPACE + "?debug=true&retry=FallthroughRetryPolicy"));
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s", HOST, PORT,
                KEYSPACE + "?debug=true&retry=FallthroughRetryPolicy"));
        con.close();

    }

    @Test(expectedExceptions = SQLNonTransientException.class)
    public void retryPolicyFailTest() throws SQLException {

        System.out.println("Connecting to : " + String.format("jdbc:cassandra://%s:%d/%s", HOST,
                PORT, KEYSPACE + "?debug=true&retry=RetryFakePolicy"));
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s", HOST, PORT,
                KEYSPACE + "?debug=true&retry=RetryFakePolicy"));
        con.close();

    }

    @Test
    public void reconnectionPolicyTest() throws SQLException {
        System.out.println("Connecting to : " + String.format("jdbc:cassandra://%s:%d/%s", HOST,
                PORT, KEYSPACE + "?debug=true&reconnection=ConstantReconnectionPolicy((long)10)"));
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s", HOST, PORT,
                KEYSPACE + "?debug=true&reconnection=ConstantReconnectionPolicy((long)10)"));
        con.close();

        System.out.println(
                "Connecting to : " + String.format("jdbc:cassandra://%s:%d/%s", HOST, PORT, KEYSPACE
                        + "?debug=true&reconnection=ExponentialReconnectionPolicy((long)10,(long)100)"));
        con = DriverManager
                .getConnection(String.format("jdbc:cassandra://%s:%d/%s", HOST, PORT, KEYSPACE
                        + "?debug=true&reconnection=ExponentialReconnectionPolicy((long)10,(long)100)"));
        con.close();

    }

    @Test(expectedExceptions = SQLNonTransientException.class)
    public void reconnectionPolicyFailTest() throws SQLException {

        System.out.println("Connecting to : " + String.format("jdbc:cassandra://%s:%d/%s", HOST,
                PORT, KEYSPACE + "?debug=true&reconnection=ConstantReconnectionPolicy((int)10)"));
        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s", HOST, PORT,
                KEYSPACE + "?debug=true&reconnection=ConstantReconnectionPolicy((int)10)"));
        con.close();

    }

    @Test
    public void connectionFailTest() throws SQLException {

        System.out.println("Connecting to : " + String.format("jdbc:cassandra://%s1:%d/%s", HOST,
                PORT, KEYSPACE + "?debug=true&reconnection=ConstantReconnectionPolicy((long)10)"));
        try {
            con = DriverManager.getConnection(String.format("jdbc:cassandra://%s1:%d/%s", HOST,
                    PORT,
                    KEYSPACE + "?debug=true&reconnection=ConstantReconnectionPolicy((long)10)"));
            con.close();
        } catch (SQLNonTransientConnectionException e) {

        }

        con = DriverManager.getConnection(String.format("jdbc:cassandra://%s:%d/%s", HOST, PORT,
                KEYSPACE + "?debug=true&reconnection=ConstantReconnectionPolicy((long)10)"));
        con.close();

    }

}
