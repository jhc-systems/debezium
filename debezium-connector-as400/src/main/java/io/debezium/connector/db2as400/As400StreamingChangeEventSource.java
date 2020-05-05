/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.time.Duration;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
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
        try {

            Long offset = offsetContext.getOffset().get(SourceInfo.JOURNAL_KEY);

            dataConnection.getJournalEntries(offset, (r, tableId, member) -> {
                System.out.println(r.getSequenceNumber() + " " + r.getJournalCode() + "." + r.getEntryType() + " " + tableId.table());
                String entryType = String.format("%s.%s", r.getJournalCode(), r.getEntryType());
                switch (entryType) {
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
                        offsetContext.setSequence(offset);
                        offsetContext.setSourceTime(r.getEntryDateOrNow());

                        dispatcher.dispatchDataChangeEvent(tableId,
                                new As400ChangeRecordEmitter(offsetContext, Operation.UPDATE, dataBefore, dataNext, clock));
                    }
                        break;
                    case "R.PT": {
                        // record added
                        DynamicRecordFormat recordFormat = dataConnection.getRecordFormat(tableId, member, schema);
                        Object[] dataNext = r.getEntrySpecificData(recordFormat);

                        offsetContext.setSequence(offset);
                        offsetContext.setSourceTime(r.getEntryDateOrNow());

                        dispatcher.dispatchDataChangeEvent(tableId,
                                new As400ChangeRecordEmitter(offsetContext, Operation.CREATE, null, dataNext, clock));
                    }
                        break;
                }
            });

        }
        catch (Exception e) {
            errorHandler.setProducerThrowable(e);
        }
    }

}
