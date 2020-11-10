/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.JournalInfoRetrieval;
import com.fnz.db2.journal.retrieve.JournalInfoRetrieval.JournalInfo;
import com.fnz.db2.journal.retrieve.JournalPosition;
import com.fnz.db2.journal.retrieve.RetrieveJournal;
import com.fnz.db2.journal.retrieve.rjne0200.EntryHeader;
import com.ibm.as400.access.AS400;

import io.debezium.relational.TableId;

public class As400RpcConnection implements AutoCloseable {
    private static Logger log = LoggerFactory.getLogger(As400RpcConnection.class);

    private As400ConnectorConfig config;
    private final String journalLibrary;
    private final AS400 as400;

    public As400RpcConnection(As400ConnectorConfig config) {
        super();
        this.config = config;
        this.as400 = new AS400(config.getHostName(), config.getUser(), config.getPassword());
        journalLibrary = JournalInfoRetrieval.getJournal(as400, config.getSchema());
    }

    @Override
    public void close() throws Exception {
        this.as400.disconnectAllServices();
    }
    
    public JournalPosition getCurrentPosition() throws RpcException {
    	try {
    		return JournalInfoRetrieval.getCurrentPosition(as400, config.getSchema(),journalLibrary);
//    		return new JournalPosition(null, null, null);
    	} catch (Exception e) {
            throw new RpcException("Failed to find offset", e);
        }
    }

    public void getJournalEntries(As400OffsetContext offsetCtx, BlockingRecieverConsumer consumer, BlockingNoDataConsumer nodataConsumer) throws RpcException {
        RpcException exception = null;
        try {
        	RetrieveJournal r = new RetrieveJournal(as400, journalLibrary, config.getSchema());
            JournalPosition position = offsetCtx.getPosition();
            boolean success = r.retrieveJournal(position);
            log.debug("QjoRetrieveJournalEntries: " + success);
            if (success) {
                while (r.nextEntry()) {
                    // TODO try round inner loop?
                    try {
                    	EntryHeader eheader = r.getEntryHeader();
                        Long currentOffset = Long.valueOf(eheader.getSequenceNumber());
                        String file = eheader.getFile();
                        String lib = eheader.getLibrary();
                        String member = eheader.getMember();
                        TableId tableId = new TableId("", lib, file);
                        offsetCtx.setSequence(currentOffset + 1);
                        consumer.accept(currentOffset, r, tableId, member);
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
            } else {
            	 JournalInfo journalNow = JournalInfoRetrieval.getReceiver(as400, config.getSchema(), journalLibrary);
                JournalPosition lastOffset = offsetCtx.getPosition();
                if (!journalNow.receiver.equals(lastOffset.getJournalReciever())) {
                	log.error("Lost data, we can't find any data for journal {} but we are now on new journal {} restarting with blank journal and offset", journalNow.receiver, lastOffset.getJournal());
                	offsetCtx.setJournalReciever(null, null);
                }
                nodataConsumer.accept();
            }
        }
        catch (Exception e) {
            throw new RpcException("Failed to process record", e);
        }
    }

    public static interface BlockingRecieverConsumer {
        void accept(Long offset, RetrieveJournal r, TableId tableId, String member) throws RpcException, InterruptedException;
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
