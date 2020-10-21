/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;

public class As400SnapshotChangeEventSource extends RelationalSnapshotChangeEventSource {
    private static Logger log = LoggerFactory.getLogger(As400SnapshotChangeEventSource.class);

    private final As400ConnectorConfig connectorConfig;
    private final As400JdbcConnection jdbcConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final As400DatabaseSchema schema;

    public As400SnapshotChangeEventSource(As400ConnectorConfig connectorConfig, As400OffsetContext previousOffset, As400JdbcConnection jdbcConnection,
                                          As400DatabaseSchema schema,
                                          EventDispatcher<TableId> dispatcher, Clock clock, SnapshotProgressListener snapshotProgressListener) {

        super(connectorConfig, previousOffset, jdbcConnection, dispatcher, clock, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.schema = schema;
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext snapshotContext) throws Exception {
        Set<TableId> tables = jdbcConnection.readTableNames(null, null, null, new String[]{ "TABLE" });
        return tables;
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext snapshotContext)
            throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext snapshotContext) throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext, RelationalSnapshotContext snapshotContext)
            throws Exception {
        Set<String> schemas = snapshotContext.capturedTables.stream()
                .map(TableId::schema)
                .collect(Collectors.toSet());

        // reading info only for the schemas we're interested in as per the set of captured tables;
        // while the passed table name filter alone would skip all non-included tables, reading the schema
        // would take much longer that way
        for (String schema : schemas) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading structure of schema " + schema);
            }

            log.info("Reading structure of schema '{}'", schema);

            jdbcConnection.readSchema(
                    snapshotContext.tables,
                    null,
                    schema,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    null,
                    false);
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext snapshotContext) throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext snapshotContext, Table table)
            throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext snapshotContext, TableId tableId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected void complete(SnapshotContext snapshotContext) {
        // TODO Auto-generated method stub
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(OffsetContext previousOffset) {
        // TODO Auto-generated method stub
        return new SnapshottingTask(false, false);
    }

    @Override
    protected SnapshotContext prepare(ChangeEventSourceContext changeEventSourceContext) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

}
