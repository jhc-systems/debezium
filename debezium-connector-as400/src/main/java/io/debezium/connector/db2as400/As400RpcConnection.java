/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.Connect;
import com.fnz.db2.journal.retrieve.JournalInfoRetrieval;
import com.fnz.db2.journal.retrieve.JournalInfoRetrieval.JournalInfo;
import com.fnz.db2.journal.retrieve.JournalInfoRetrieval.JournalLib;
import com.fnz.db2.journal.retrieve.JournalPosition;
import com.fnz.db2.journal.retrieve.RetrieveJournal;
import com.fnz.db2.journal.retrieve.rjne0200.EntryHeader;
import com.ibm.as400.access.AS400;

public class As400RpcConnection implements AutoCloseable, Connect<AS400, IOException> {
    private static Logger log = LoggerFactory.getLogger(As400RpcConnection.class);

    private As400ConnectorConfig config;
    private JournalLib journalLibrary;
    private AS400 as400;

    public As400RpcConnection(As400ConnectorConfig config) {
        super();
        this.config = config;
        try {
            journalLibrary = JournalInfoRetrieval.getJournal(connection(), config.getSchema());
        }
        catch (IOException e) {
            log.error("Failed to fetch library", e);
        }
    }

    @Override
    public void close() throws Exception {
        this.as400.disconnectAllServices();
    }

    public AS400 connection() throws IOException {
        if (as400 == null || !as400.isConnectionAlive()) {
            log.debug("create new as400 connection");
            try {
                // need to both create a new object and connect
                this.as400 = new AS400(config.getHostName(), config.getUser(), config.getPassword());
                as400.connectService(AS400.COMMAND);
            }
            catch (Exception e) {
                log.error("Failed to reconnect", e);
                throw new IOException("Failed to reconnect", e);
            }
        }
        return as400;
    }

    public JournalPosition getCurrentPosition() throws RpcException {
        try {
            return JournalInfoRetrieval.getCurrentPosition(connection(), journalLibrary);
            // return new JournalPosition(null, null, null);
        }
        catch (Exception e) {
            throw new RpcException("Failed to find offset", e);
        }
    }

    public void getJournalEntries(As400OffsetContext offsetCtx, BlockingRecieverConsumer consumer, BlockingNoDataConsumer nodataConsumer) throws RpcException {
        RpcException exception = null;
        try {
            boolean foundData = false;
            RetrieveJournal r = new RetrieveJournal(connection(), journalLibrary, config.getSchema());
            JournalPosition position = offsetCtx.getPosition();
            boolean success = r.retrieveJournal(position);
            log.debug("QjoRetrieveJournalEntries at {} result {}", position, success);
            if (success) {
                while (r.nextEntry()) {
                    foundData = true;
                    try {
                        EntryHeader eheader = r.getEntryHeader();
                        Long currentOffset = eheader.getSequenceNumber() + 1;

                        consumer.accept(currentOffset, r, eheader);
                        offsetCtx.setSequence(currentOffset);
                    }
                    catch (Exception e) {
                        if (exception == null) {
                            exception = new RpcException("Failed to process record", e);
                        }
                        else {
                            exception.addSuppressed(e); // TODO dump failed record for diagnostics
                        }
                    }
                }
            }
            else {
                JournalInfo journalNow = JournalInfoRetrieval.getReceiver(connection(), journalLibrary);
                JournalPosition lastOffset = offsetCtx.getPosition();
                if (!journalNow.receiver.equals(lastOffset.getJournalReciever())) {
                    log.error("Lost data, we can't find any data for journal {} but we are now on new journal {} restarting with blank journal and offset",
                            journalNow.receiver, lastOffset.getJournal());
                    offsetCtx.setJournalReciever(null, null);
                }
            }

            if (!foundData)
                nodataConsumer.accept();

        }
        catch (Exception e) {
            throw new RpcException("Failed to process record", e);
        }
    }

    public static interface BlockingRecieverConsumer {
        void accept(Long offset, RetrieveJournal r, EntryHeader eheader) throws RpcException, InterruptedException;
    }

    public static interface BlockingNoDataConsumer {
        void accept() throws InterruptedException;
    }

    public static class RpcException extends Exception {

        public RpcException(String message, Throwable cause) {
            super(message, cause);
        }

        public RpcException(String message) {
            super(message);
        }

    }

}
