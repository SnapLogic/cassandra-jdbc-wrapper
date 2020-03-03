Cassandra JDBC Wrapper Unit Tests
=================================

The readme in project root briefly describes dependencies for running the
unit tests. This document explains this in a bit more detail and how to run
them.

* Install the Cassandra Cluster Manager (CCM).

    $ brew install ccm

* Determine which version of Cassandra you want to run. Browse releases at
  http://cassandra.apache.org/download/. For example, here is the latest release
  available when this was written.

    Apache Cassandra 3.11 release: 3.11.6, released on 2020-02-14

* Spin up a single node cluster. The first time this is run ccm will download
  the binary and set the cluster to use it. This may take a few minutes.
  Subsequent invocations will use cache version saved in `~/.ccm/repository`

    $ ccm create test -v 3.11.6

        09:59:07,995 ccm INFO Downloading
        http://archive.apache.org/dist/cassandra/3.11.6/apache-cassandra-3.11.6-bin.tar.gz
        Current cluster is now: test

    $ ccm populate -n 1

    $ ccm start

  Here is how you can see the listening ports.

    $ lsof -nP -i4TCP | grep LISTEN | grep java

        java  IPv4 0x17bd9e3dfee52bcf  TCP 127.0.0.1:7100 (LISTEN)
        java  IPv4 0x17bd9e3dfee53557  TCP 127.0.0.1:54787 (LISTEN)
        java  IPv4 0x17bd9e3e022ffedf  TCP 127.0.0.1:7000 (LISTEN)
        java  IPv4 0x17bd9e3e02ed7edf  TCP 127.0.0.1:9042 (LISTEN)

  Inspect cluster.

    $ ccm node1 ring

        Datacenter: datacenter1
        ==========
        Address    Rack   Status State   Load       Owns     Token

        127.0.0.1  rack1  Up     Normal  65.41 KiB  100.00%  -9223372036854775808

  View log.

    $ ccm node1 showlog

  Check status of cluster.

    $ ccm status

  Shutdown cluster.

    $ ccm stop

* Run the unit tests.

    $ ccm start (if not running)

    $ mvn clean test

        Tests run: 74, Failures: 0, Errors: 0, Skipped: 0

    $ ccm stop (run this when you want to shutdown the cluster)


Tips & Tricks
-------------

* Use `cqlsh` to inspect database

    If you set a breakpoint in one of the tests, you can inspect data in
    Cassandra using cqlsh. You can install with `pip install cql`.

    Here is an example of inspecting datetimetypes table after inserting this
    record (which is done by one of the tests).

        INSERT INTO datetimetypes (intcol, timestampcol) VALUES (1, '2020-01-10');

    Create rc file specifying cql version.

        $ vim cqlshrc
            [cql]
            version=3.4.4

    Now connect to cluster (assuming rc file is in same directory).

        $ cqlsh --cqlshrc=cqlshrc

            [cqlsh 5.0.1 | Cassandra 3.11.6 | CQL spec 3.4.4 | Native protocol v4]

            cqlsh> use testks;

            cqlsh:testks> expand on;

            cqlsh:testks> SELECT * FROM datetimetypes WHERE intcol = 1;

            @ Row 1
            --------------+---------------------------------
            intcol       | 1
            datecol      | null
            timecol      | null
            timestampcol | 2020-01-10 08:00:00.000000+0000

* Adjust Logging

    The logging defaults to DEBUG which may be a bit noisy. You can adjust
    log level in this file, for example, change DEBUG to INFO.

    File: src/test/resources/log4j.properties

        # Root logger option
        log4j.rootLogger=DEBUG, stdout


* Inspect Code Coverage

    Project uses JaCoCo for code coverage. This is bound to the test phase,
    so will be regenerated when you run this phase. So, either of these
    commands will generate a coverage report.

        $ mvn clean test
        OR
        $ mvn clean install

    You can inspect report here.

        $ open -a "Firefox" ./target/jacoco-ut/index.html
