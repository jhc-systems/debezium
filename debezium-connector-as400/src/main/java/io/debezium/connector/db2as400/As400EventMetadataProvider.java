package io.debezium.connector.db2as400;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;

public class As400EventMetadataProvider implements EventMetadataProvider {

	@Override
	public Instant getEventTimestamp(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> getEventSourcePosition(DataCollectionId source, OffsetContext offset, Object key,
			Struct value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getTransactionId(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
		// TODO Auto-generated method stub
		return null;
	}

}
