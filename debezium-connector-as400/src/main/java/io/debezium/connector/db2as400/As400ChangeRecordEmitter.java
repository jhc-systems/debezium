package io.debezium.connector.db2as400;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.util.Clock;

public class As400ChangeRecordEmitter extends RelationalChangeRecordEmitter {

    private final Operation operation;
    private final Object[] data;
    private final Object[] dataNext;

    public As400ChangeRecordEmitter(OffsetContext offset, Operation operation, Object[] data, Object[] dataNext, Clock clock) {
        super(offset, clock);

        this.operation = operation;
        this.data = data;
        this.dataNext = dataNext;
    }

    @Override
    protected Operation getOperation() {
        return operation;
    }

    @Override
    protected Object[] getOldColumnValues() {
        return data;
    }

    @Override
    protected Object[] getNewColumnValues() {
        return dataNext;
    }

}
