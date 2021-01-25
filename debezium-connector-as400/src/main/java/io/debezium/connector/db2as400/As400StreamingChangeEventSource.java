/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.JdbcFileDecoder;
import com.fnz.db2.journal.retrieve.JournalEntryDeocder;
import com.fnz.db2.journal.retrieve.JournalReciever;
import com.fnz.db2.journal.retrieve.JournalRecordDecoder;
import com.fnz.db2.journal.retrieve.SchemaCacheIF;

import io.debezium.connector.db2as400.As400RpcConnection.BlockingRecieverConsumer;
import io.debezium.connector.db2as400.As400RpcConnection.RpcException;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * <p>
 * A {@link StreamingChangeEventSource} A main loop polls using a RPC call for
 * new journal entries and turns them into change events.
 * </p>
 */
public class As400StreamingChangeEventSource implements StreamingChangeEventSource {
    private static final String NO_TRANSACTION_ID = "00000000000000000000";
    private JournalEntryDeocder<Object[]> fileDecoder;

    private static final Logger log = LoggerFactory.getLogger(As400StreamingChangeEventSource.class);

    private static final Logger LOGGER = LoggerFactory.getLogger(As400StreamingChangeEventSource.class);
    private HashMap<String, Object[]> beforeMap = new HashMap<>();
    private static Set<Character> alwaysProcess = Stream.of('J', 'C')
            .collect(Collectors.toCollection(HashSet::new));

    /**
     * Connection used for reading CDC tables.
     */
    private final As400RpcConnection dataConnection;
    private As400JdbcConnection jdbcConnection;

    /**
     * A separate connection for retrieving timestamps; without it, adaptive
     * buffering will not work.
     */
    private final EventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final As400DatabaseSchema schema;
    private final As400OffsetContext offsetContext;
    private final Duration pollInterval;
    private final As400ConnectorConfig connectorConfig;
    private final Map<String, TransactionContext> txMap = new HashMap<>();
    private final String database;

    public As400StreamingChangeEventSource(As400ConnectorConfig connectorConfig, As400OffsetContext offsetContext,
                                           As400RpcConnection dataConnection, As400JdbcConnection jdbcConnection, EventDispatcher<TableId> dispatcher,
                                           ErrorHandler errorHandler, Clock clock, As400DatabaseSchema schema) {
        this.connectorConfig = connectorConfig;
        this.dataConnection = dataConnection;
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        this.pollInterval = connectorConfig.getPollInterval();
        this.database = jdbcConnection.getRealDatabaseName();
        this.fileDecoder = new JdbcFileDecoder(jdbcConnection, dataConnection, connectorConfig.getSchema(), (SchemaCacheIF) schema);
    }

    private void cacheBefore(TableId tableId, Timestamp date, Object[] dataBefore) {
        String key = String.format("%s-%s", tableId.schema(), tableId.table());
        beforeMap.put(key, dataBefore);
    }

    private Object[] getBefore(TableId tableId, Timestamp date) {
        String key = String.format("%s-%s", tableId.schema(), tableId.table());
        Object[] dataBefore = beforeMap.remove(key);
        if (dataBefore == null) {
            log.info("before image found for {}", key);
        }
        else {
            log.warn("already had before image for {}", key);
        }
        return dataBefore;
    }

    @Override
    public void execute(ChangeEventSourceContext context) throws InterruptedException {
        final Metronome metronome = Metronome.sleeper(pollInterval, clock);
        // Integer offset = offsetContext.getSequence();
        while (context.isRunning()) {
            Exception ex = null;
            for (int i = 0; i < 5 && context.isRunning(); i++) { // allow retries
                try {
                    dataConnection.getJournalEntries(offsetContext, processJournalEntries(), () -> {
                        log.debug("sleep");
                        metronome.pause();
                    });
                    break;
                }
                catch (Exception e) {
                    ex = e;
                }
            }
            if (ex != null) {
                log.error("failed to process offset {}", offsetContext.getPosition().toString(), ex);
                errorHandler.setProducerThrowable(ex);
            }
        }
    }

    private BlockingRecieverConsumer processJournalEntries() {
        return (nextOffset, r, eheader) -> {
            try {
                String longName = jdbcConnection.getLongName(eheader.getLibrary(), eheader.getFile());
                TableId tableId = new TableId(database, eheader.getLibrary(), longName);

                boolean includeTable = connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId);

                if (!alwaysProcess.contains(eheader.getJournalCode()) && !includeTable) { // always process journal J and transaction C messages
                    log.debug("excluding table {} journal code {}", tableId, eheader.getJournalCode());
                    return;
                }

                String entryType = String.format("%s.%s", eheader.getJournalCode(), eheader.getEntryType());
                log.info("next event: {} type: {} table: {}", eheader.getSequenceNumber(), entryType, tableId.table());
                switch (entryType) {
                    case "C.SC": {
                        // start commit
                        String txId = eheader.getCommitCycle().toString();
                        log.debug("begin transaction: {}", txId);
                        TransactionContext txc = new TransactionContext();
                        txc.beginTransaction(txId);
                        txMap.put(txId, txc);
                        log.info("start transaction id {} tx {} table {}", nextOffset, txId, tableId);
                        dispatcher.dispatchTransactionStartedEvent(txId, offsetContext);
                    }
                        break;
                    case "C.CM": {
                        // end commit
                        // TOOD transaction must be provided by the OffsetContext
                        String txId = eheader.getCommitCycle().toString();
                        TransactionContext txc = txMap.remove(txId);
                        log.info("commit transaction id {} tx {} table {}", nextOffset, txId, tableId);
                        if (txc != null) {
                            txc.endTransaction();
                            dispatcher.dispatchTransactionCommittedEvent(offsetContext);
                        }
                    }
                        break;
                    case "R.UB": {
                        // before image
                        tableId.schema();
                        Object[] dataBefore = r.decode(fileDecoder);

                        cacheBefore(tableId, eheader.getTimestamp(), dataBefore);
                    }
                        break;
                    case "R.UP": {
                        // after image
                        // before image is meant to have been immediately before
                        Object[] dataBefore = getBefore(tableId, eheader.getTimestamp());
                        Object[] dataNext = r.decode(fileDecoder);

                        offsetContext.setSourceTime(eheader.getTimestamp());

                        String txId = eheader.getCommitCycle().toString();
                        TransactionContext txc = txMap.get(txId);
                        offsetContext.setTransaction(txc);

                        log.info("update event id {} tx {} table {}", nextOffset, txId, tableId);

                        dispatcher.dispatchDataChangeEvent(tableId,
                                new As400ChangeRecordEmitter(offsetContext, Operation.UPDATE, dataBefore, dataNext, clock));
                    }
                        break;
                    case "R.PX":
                    case "R.PT": {
                        // record added
                        Object[] dataNext = r.decode(fileDecoder);
                        offsetContext.setSourceTime(eheader.getTimestamp());

                        String txId = eheader.getCommitCycle().toString();
                        TransactionContext txc = txMap.get(txId);
                        offsetContext.setTransaction(txc);
                        if (txc != null) {
                            txc.event(tableId);
                        }

                        log.info("insert event id {} tx {} table {}", offsetContext.getPosition().toString(), txId, tableId);
                        dispatcher.dispatchDataChangeEvent(tableId,
                                new As400ChangeRecordEmitter(offsetContext, Operation.CREATE, null, dataNext, clock));
                    }
                        break;
                    case "R.DR":
                    case "R.DL": {
                        // record deleted
                        Object[] dataBefore = r.decode(fileDecoder);

                        offsetContext.setSourceTime(eheader.getTimestamp());

                        String txId = eheader.getCommitCycle().toString();
                        TransactionContext txc = txMap.get(txId);
                        offsetContext.setTransaction(txc);
                        if (txc != null) {
                            txc.event(tableId);
                        }

                        log.info("delete event id {} tx {} table {}", offsetContext.getPosition().toString(), txId, tableId);
                        dispatcher.dispatchDataChangeEvent(tableId,
                                new As400ChangeRecordEmitter(offsetContext, Operation.DELETE, dataBefore, null, clock));
                    }
                        break;
                    case "J.NR": {
                        JournalReciever startReciever = r.decode(new JournalRecordDecoder());
                        offsetContext.setJournalReciever(startReciever.getReciever(), startReciever.getLibrary());
                    }
                        break;
                }
            }
            catch (Exception e) {
                e.printStackTrace();
                throw new RpcException("", e);
            }
        };
    }

}
