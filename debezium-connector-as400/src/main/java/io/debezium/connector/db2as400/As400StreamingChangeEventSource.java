/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static Logger log = LoggerFactory.getLogger(As400StreamingChangeEventSource.class);

    private static final Logger LOGGER = LoggerFactory.getLogger(As400StreamingChangeEventSource.class);
    private HashMap<String, Object[]> beforeMap = new HashMap<>();

    /**
     * Connection used for reading CDC tables.
     */
    private final As400RpcConnection dataConnection;

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

    public As400StreamingChangeEventSource(As400ConnectorConfig connectorConfig, As400OffsetContext offsetContext,
                                           As400RpcConnection dataConnection, EventDispatcher<TableId> dispatcher,
                                           ErrorHandler errorHandler, Clock clock, As400DatabaseSchema schema) {
        this.connectorConfig = connectorConfig;
        this.dataConnection = dataConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        this.pollInterval = connectorConfig.getPollInterval();
    }

    private void cacheBefore(TableId tableId, String date, Object[] dataBefore) {
        String key = String.format("%s-%s-%s", tableId.schema(), tableId.table(), date);
        beforeMap.put(key, dataBefore);
    }

    private Object[] getBefore(TableId tableId, String date) {
        String key = String.format("%s-%s-%s", tableId.schema(), tableId.table(), date);
        Object[] dataBefore = beforeMap.remove(key);
        if (dataBefore == null) {
            log.warn("now before image found for {}", key);
        }
        return dataBefore;
    }

    @Override
    public void execute(ChangeEventSourceContext context) throws InterruptedException {
        final Metronome metronome = Metronome.sleeper(pollInterval, clock);
        Long offset = offsetContext.getOffset().get(SourceInfo.JOURNAL_KEY);
        log.info("fetch next batch at offset {}", offset);
        while (context.isRunning()) {
            try {
                offset = dataConnection.getJournalEntries(offset, (nextOffset, r, tableId, member) -> {
                    String entryType = String.format("%s.%s", r.getJournalCode(), r.getEntryType());
                    log.debug("next event: {} type: {} table: {}", r.getSequenceNumber(), entryType, tableId.table());
                    switch (entryType) {
                        case "C.SC": {
                            // start commit
                            String txId = r.getCommitCycleId();
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
                            String txId = r.getCommitCycleId();
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
                            DynamicRecordFormat recordFormat = dataConnection.getRecordFormat(tableId, member, schema);
                            Object[] dataBefore = r.getEntrySpecificData(recordFormat);
                            cacheBefore(tableId, r.getEntryDateString(), dataBefore);
                        }
                            break;
                        case "R.UP": {
                            // after image
                            // before image is meant to have been immediately before
                            Object[] dataBefore = getBefore(tableId, r.getEntryDateString());

                            DynamicRecordFormat recordFormat = dataConnection.getRecordFormat(tableId, member, schema);
                            Object[] dataNext = r.getEntrySpecificData(recordFormat);
                            offsetContext.setSequence(nextOffset);
                            offsetContext.setSourceTime(r.getEntryDateOrNow());


                            String txId = r.getCommitCycleId();
                            TransactionContext txc = txMap.get(txId);
                            offsetContext.setTransaction(txc);

                            log.info("update event id {} tx {} table {}", nextOffset, txId, tableId);

                            dispatcher.dispatchDataChangeEvent(tableId,
                                    new As400ChangeRecordEmitter(offsetContext, Operation.UPDATE, dataBefore, dataNext, clock));
                        }
                            break;
                        case "R.PT": {
                            // record added
                            DynamicRecordFormat recordFormat = dataConnection.getRecordFormat(tableId, member, schema);
                            Object[] dataNext = r.getEntrySpecificData(recordFormat);

                            offsetContext.setSequence(nextOffset);
                            offsetContext.setSourceTime(r.getEntryDateOrNow());

                            String txId = r.getCommitCycleId();
                            TransactionContext txc = txMap.get(txId);
                            offsetContext.setTransaction(txc);
                            if (txc != null) {
                                txc.event(tableId);
                            }

                            log.info("insert event id {} tx {} table {}", offsetContext.sequence, txId, tableId);
                            dispatcher.dispatchDataChangeEvent(tableId,
                                    new As400ChangeRecordEmitter(offsetContext, Operation.CREATE, null, dataNext, clock));
                        }
                            break;
                    }
                }, () -> {
                    log.debug("sleep");
                    metronome.pause();
                });
            }
            catch (Exception e) {
                errorHandler.setProducerThrowable(e);
            }
        }
    }

}
