package io.debezium.connector.db2as400;

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConfiguration;

public class As400StreamingConnectorIT extends AbstractConnectorTest {

    @Before
    public void beforeEach() {
        stopConnector();
        // DATABASE.createAndInitialize();
        // RO_DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        // Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            // Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test
    public void shouldProcessCreateUniqueIndex() throws SQLException, InterruptedException {
        // Testing.Files.delete(DB_HISTORY_PATH);

        Configuration config = Configuration.create()
                .with(JdbcConfiguration.HOSTNAME, "tracey.servers.jhc.co.uk")
                .with(JdbcConfiguration.PORT, "")
                .with(JdbcConfiguration.DATABASE, "")
                .with(As400ConnectorConfig.USER, "MSDEV")
                .with(As400ConnectorConfig.PASSWORD, "MSDEV")
                .with(As400ConnectorConfig.JOURNAL_LIBRARY, "QSQJRN")
                .with(As400ConnectorConfig.JOURNAL_FILE, "MSDEVT")
                .build();

        // Start the connector ...
        start(As400RpcConnector.class, config);

        // Wait for streaming to start

        // final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        // System.out.println("count=" +mbeanServer.getMBeanCount());
        // Set<ObjectInstance> instances = mbeanServer.queryMBeans(null, null);
        //
        // Iterator<ObjectInstance> iterator = instances.iterator();
        // try {
        // Thread.sleep(3000);
        // } catch(Exception e) {}
        // while (iterator.hasNext()) {
        // ObjectInstance instance = iterator.next();
        //// System.out.println("MBean Found:");
        // System.out.println("Class Name:" + instance.getClassName());
        // System.out.println("Object Name:" + instance.getObjectName());
        // System.out.println("****************************************");
        // }

        // "debezium." + connector + ":type=connector-metrics,context=" + context + ",server=" + server
        // Object Name:debezium.db2as400_server:type=connector-metrics,context=snapshot,server=tracey.servers.jhc.co.uk
        waitForStreamingRunning("db2as400_server", "tracey.servers.jhc.co.uk");
        System.out.println("Running");
        SourceRecords records = consumeRecordsByTopic(6);
        System.out.println(records.allRecordsInOrder().size());
        records.forEach(sourceRecord -> {
            System.out.println(sourceRecord);
        });

        // Consume the first records due to startup and initialization of the database ...
        // Testing.Print.enable();

        // try (As400JdbcConnection db = As400JdbcConnection.forTestDatabase("");) {
        // try (JdbcConnection connection = db.connect()) {
        // connection.execute(
        // "create table debtest (id char(20) not null)");
        // }
        // }

        // SourceRecords records = consumeRecordsByTopic(1);
        // final List<SourceRecord> migrationTestRecords = records.recordsForTopic(DATABASE.topicForTable("migration_test"));
        // assertThat(migrationTestRecords.size()).isEqualTo(1);
        // final SourceRecord record = migrationTestRecords.get(0);
        // assertThat(((Struct) record.key()).getString("mgb_no")).isEqualTo("2");
        // assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(13);
        // try (As400JdbcConnection db = As400JdbcConnection.forTestDatabase("");) {
        // try (JdbcConnection connection = db.connect()) {
        // connection.execute(
        // "drop table debtest");
        // }
        // }

        stopConnector();
    }
}
