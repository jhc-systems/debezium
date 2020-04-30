/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

public class As400ChangeEventSourceFactory implements ChangeEventSourceFactory {

    private final As400ConnectorConfig configuration;
    private final As400RpcConnection rpcConnection;
    private final As400JdbcConnection jdbcConnection;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final As400DatabaseSchema schema;

    public As400ChangeEventSourceFactory(As400ConnectorConfig configuration, As400RpcConnection rpcConnection, As400JdbcConnection jdbcConnection,
                                       ErrorHandler errorHandler, EventDispatcher<TableId> dispatcher, Clock clock, As400DatabaseSchema schema) {
        this.configuration = configuration;
        this.rpcConnection = rpcConnection;
        this.jdbcConnection = jdbcConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
    }

    @Override
    public SnapshotChangeEventSource getSnapshotChangeEventSource(OffsetContext offsetContext, SnapshotProgressListener snapshotProgressListener) {
        return new As400SnapshotChangeEventSource(configuration, (As400OffsetContext) offsetContext, jdbcConnection, schema, dispatcher, clock, snapshotProgressListener);
    }

    @Override
    public StreamingChangeEventSource getStreamingChangeEventSource(OffsetContext offsetContext) {
        return new As400StreamingChangeEventSource(
                configuration,
                (As400OffsetContext) offsetContext,
                rpcConnection,
                dispatcher,
                errorHandler,
                clock,
                schema);
    }
}
