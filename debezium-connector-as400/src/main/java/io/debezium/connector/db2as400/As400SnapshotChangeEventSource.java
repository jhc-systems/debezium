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

import com.fnz.db2.journal.retrieve.JournalPosition;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.util.Clock;

public class As400SnapshotChangeEventSource extends RelationalSnapshotChangeEventSource {
    private static Logger log = LoggerFactory.getLogger(As400SnapshotChangeEventSource.class);

    private final As400ConnectorConfig connectorConfig;
    private final As400JdbcConnection jdbcConnection;
    private As400RpcConnection rpcConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final As400DatabaseSchema schema;

    public As400SnapshotChangeEventSource(As400ConnectorConfig connectorConfig, As400OffsetContext previousOffset, As400RpcConnection rpcConnection,
                                          As400JdbcConnection jdbcConnection,
                                          As400DatabaseSchema schema,
                                          EventDispatcher<TableId> dispatcher, Clock clock, SnapshotProgressListener snapshotProgressListener) {

        super(connectorConfig, previousOffset, jdbcConnection, dispatcher, clock, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
        this.rpcConnection = rpcConnection;
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.schema = schema;
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext snapshotContext) throws Exception {
        Set<TableId> tables = jdbcConnection.readTableNames(jdbcConnection.database(), connectorConfig.getSchema(), null, new String[]{ "TABLE" });
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
        JournalPosition position = rpcConnection.getCurrentPosition();
        // move on past last entry so we don't process it again
        if (position.isOffsetSet()) {
            position.setOffset(position.getOffset() + 1);
        }
        snapshotContext.offset = new As400OffsetContext(connectorConfig, position);
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
                    jdbcConnection.getRealDatabaseName(),
                    schema,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    null,
                    false);

        }

        for (TableId id : snapshotContext.capturedTables) {
            Table table = snapshotContext.tables.forTable(id);
            schema.addSchema(table);
        }
        // System.out.println(snapshotContext.tables);
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext snapshotContext) throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext snapshotContext, Table table)
            throws Exception {
        return new SchemaChangeEvent(
                snapshotContext.offset.getPartition(),
                snapshotContext.offset.getOffset(),
                snapshotContext.offset.getSourceInfo(),
                snapshotContext.catalogName,
                table.id().schema(),
                null,
                table,
                SchemaChangeEventType.CREATE, true);
    }

    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext snapshotContext, TableId tableId) {
        return Optional.of(String.format("SELECT * FROM %s.%s", tableId.schema(), tableId.table()));
    }

    @Override
    protected void complete(SnapshotContext snapshotContext) {
        // TODO Auto-generated method stub
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(OffsetContext previousOffset) {

        if (previousOffset != null && !previousOffset.isSnapshotRunning()) {
            if (previousOffset instanceof As400OffsetContext) {
                JournalPosition pos = ((As400OffsetContext) previousOffset).getPosition();
                if (pos.isOffsetSet())
                    return new SnapshottingTask(false, false);
            }
        }
        return new SnapshottingTask(true, connectorConfig.getSnapshotMode().includeData());
    }

    @Override
    protected SnapshotContext prepare(ChangeEventSourceContext changeEventSourceContext) throws Exception {
        return new RelationalSnapshotContext(jdbcConnection.getRealDatabaseName());
    }

}
