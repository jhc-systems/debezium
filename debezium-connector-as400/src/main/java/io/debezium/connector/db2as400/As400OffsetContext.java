package io.debezium.connector.db2as400;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Collect;

public class As400OffsetContext implements OffsetContext {
    Logger log = LoggerFactory.getLogger(As400OffsetContext.class);
    // TODO note believe there is a per journal offset
    private static final String SERVER_PARTITION_KEY = "server";
    public static final String EVENT_SEQUENCE = "event_sequence";
    private final Map<String, String> partition;
    private TransactionContext transactionContext;

    As400ConnectorConfig connectorConfig;
    SourceInfo sourceInfo;
    Long sequence;

    public As400OffsetContext(As400ConnectorConfig connectorConfig, Long sequence) {
        super();
        partition = Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        this.connectorConfig = connectorConfig;
        sourceInfo = new SourceInfo(connectorConfig);
        this.sequence = sequence;
    }

    public void setSequence(Long sequence) {
    	if (this.sequence > sequence) {
    		log.error("loop");
    	} else {
    		this.sequence = sequence;
    	}
    }

    public void setTransaction(TransactionContext transactionContext) {
        this.transactionContext = transactionContext;
    }

    public void endTransaction() {
        transactionContext = null;
    }

    @Override
    public Map<String, ?> getPartition() {
        return partition;
    }

    @Override
    public Map<String, Long> getOffset() {
        if (sourceInfo.isSnapshot()) {
        	log.error("SHAPSHOTS not supported yet");
            // TODO handle snapshots
            return null;
        }
        else {
        	log.debug("new offset {}", sequence);
            // TODO persist progress
            return Collect.hashMapOf(
                    SourceInfo.JOURNAL_KEY, 0L,
                    EVENT_SEQUENCE, sequence);
        }
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfo.schema();
    }

    public void setSourceTime(Date time) {
        sourceInfo.setSourceTime(time.toInstant());
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    @Override
    public boolean isSnapshotRunning() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void markLastSnapshotRecord() {
        // TODO Auto-generated method stub

    }

    @Override
    public void preSnapshotStart() {
        // TODO Auto-generated method stub

    }

    @Override
    public void preSnapshotCompletion() {
        // TODO Auto-generated method stub

    }

    @Override
    public void postSnapshotCompletion() {
        // TODO Auto-generated method stub

    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
        // sourceInfo.setTableId((TableId) collectionId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

}
