package io.debezium.connector.db2as400;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.util.Clock;

public class As400SnapshotChangeEventSource extends RelationalSnapshotChangeEventSource {
	
    private final As400ConnectorConfig connectorConfig;
    private final As400JdbcConnection jdbcConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final As400DatabaseSchema schema;
    
    
    public As400SnapshotChangeEventSource(As400ConnectorConfig connectorConfig, As400OffsetContext previousOffset, As400JdbcConnection jdbcConnection, As400DatabaseSchema schema,
                                        EventDispatcher<TableId> dispatcher, Clock clock, SnapshotProgressListener snapshotProgressListener) {
    	
        super(connectorConfig, previousOffset, jdbcConnection, dispatcher, clock, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.schema = schema;
    }
	@Override
	protected Set<TableId> getAllTableIds(RelationalSnapshotContext snapshotContext) throws Exception {
		TableId t = new TableId(null, "MSDEVT", "TEST");
		// TODO get tables
		HashSet<TableId> s = new HashSet<>();
		s.add(t);
		
		return s;
	}

	@Override
	protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
			RelationalSnapshotContext snapshotContext) throws Exception {
		// TODO Auto-generated method stub
	}

	@Override
	protected void determineSnapshotOffset(RelationalSnapshotContext snapshotContext) throws Exception {
		// TODO Auto-generated method stub
	}

	@Override
	protected void readTableStructure(ChangeEventSourceContext sourceContext, RelationalSnapshotContext snapshotContext)
			throws Exception {
		// TODO Auto-generated method stub
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
	protected Optional<String> getSnapshotSelect(SnapshotContext snapshotContext, TableId tableId) {
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
