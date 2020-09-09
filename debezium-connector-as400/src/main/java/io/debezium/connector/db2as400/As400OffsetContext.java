package io.debezium.connector.db2as400;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Field;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Collect;

public class As400OffsetContext implements OffsetContext {
    Logger log = LoggerFactory.getLogger(As400OffsetContext.class);
    // TODO note believe there is a per journal offset
    private static final String SERVER_PARTITION_KEY = "server";
    public static final String EVENT_SEQUENCE = "offset.event_sequence";
    public static final String JOURNAL_LIB = "offset.journal_lib";
    public static final String JOURNAL_RECEIVER = "offset.journal_receiver";

    public static final Field EVENT_SEQUENCE_FIELD = Field.create(EVENT_SEQUENCE);
    public static final Field JOURNAL_LIB_FIELD = Field.create(JOURNAL_LIB);
    public static final Field JOURNAL_RECEIVER_FIELD = Field.create(JOURNAL_RECEIVER);

    private final Map<String, String> partition;
    private TransactionContext transactionContext;
    private static String[] empty = new String[]{};

    As400ConnectorConfig connectorConfig;
    SourceInfo sourceInfo;
    JournalPosition position;

    public As400OffsetContext(As400ConnectorConfig connectorConfig) {
        super();
        partition = Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        this.position = connectorConfig.getOffset();
        this.connectorConfig = connectorConfig;
        sourceInfo = new SourceInfo(connectorConfig);
    }

    public As400OffsetContext(As400ConnectorConfig connectorConfig, JournalPosition position) {
        super();
        partition = Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        this.position = position;
        this.connectorConfig = connectorConfig;
        sourceInfo = new SourceInfo(connectorConfig);
    }

    public void setSequence(Long sequence) {
        if (position.getOffset() > sequence) {
            log.error("loop currently {} set to {}", position.getOffset(), sequence, new Exception("please report this should never go backwards"));
        }
        else {
            position.setOffset(sequence);
        }
    }

    public JournalPosition getPosition() {
        return position;
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
    public Map<String, ?> getOffset() {
        if (sourceInfo.isSnapshot()) {
            log.error("SHAPSHOTS not supported yet");
            // TODO handle snapshots
            return null;
        }
        else {
            log.debug("new offset {}", position);
            // TODO persist progress
            return Collect.hashMapOf(
                    As400OffsetContext.EVENT_SEQUENCE, Long.toString(position.getOffset()),
                    As400OffsetContext.JOURNAL_RECEIVER, position.getJournalReciever(),
                    As400OffsetContext.JOURNAL_LIB, position.getJournalLib());
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

    public void setJournalReciever(String journalReciever, String journalLib) {
        position.setJournalReciever(journalReciever, journalLib);
    }

    public static class Loader implements OffsetContext.Loader {

        private final As400ConnectorConfig connectorConfig;

        public Loader(As400ConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Map<String, ?> getPartition() {
            return Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        }

        @Override
        public OffsetContext load(Map<String, ?> map) {
            Long offset = Long.valueOf((String) map.get(As400OffsetContext.EVENT_SEQUENCE));
            String receiver = (String) map.get(As400OffsetContext.JOURNAL_RECEIVER);
            String lib = (String) map.get(As400OffsetContext.JOURNAL_LIB);

            return new As400OffsetContext(connectorConfig, new JournalPosition(offset, receiver, lib));
        }
    }

}
