package io.debezium.connector.db2as400;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Collect;

public class As400EventMetadataProvider implements EventMetadataProvider {

    @Override
    public Instant getEventTimestamp(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Long timestamp = value.getInt64(SourceInfo.TIMESTAMP_KEY);
        return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
    }

    @Override
    public Map<String, String> getEventSourcePosition(DataCollectionId source, OffsetContext offset, Object key,
                                                      Struct value) {
        Map<String, ?> map = offset.getOffset();
        if (map.containsKey(As400OffsetContext.EVENT_SEQUENCE))
            return Collect.hashMapOf(As400OffsetContext.EVENT_SEQUENCE,
                    ((Long) map.get(As400OffsetContext.EVENT_SEQUENCE)).toString());
        return null;
    }

    @Override
    public String getTransactionId(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        // TODO Auto-generated method stub
        return null;
    }

}
