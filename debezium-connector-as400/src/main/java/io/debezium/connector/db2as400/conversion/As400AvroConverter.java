// package io.debezium.connector.db2as400.conversion;
//
// import java.util.Map;
//
// import org.apache.kafka.common.header.Headers;
// import org.apache.kafka.connect.data.Schema;
// import org.apache.kafka.connect.data.SchemaAndValue;
//
// import io.confluent.connect.avro.AvroConverter;
// import io.debezium.relational.TableSchemaBuilder;
//
// public class As400AvroConverter extends AvroConverter {
//
// @Override
// public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
// schema.name();
// schema.fields();
// schema.keySchema();
// schema.valueSchema();
// schema.parameters();
// TableSchemaBuilder.creat
// SchemaBuilder builder = SchemaBuilder.struct().name(schema.name()).;
//// builder.
//// name(schema.name()).namespace(schema.).fields().name("clientHash")
//// .type().fixed("MD5").size(16).noDefault().name("clientProtocol").type().nullable().stringType().noDefault()
//// .name("serverHash").type("MD5").noDefault().name("meta").type().nullable().map().values().bytesType().noDefault()
//// .endRecord();
//
// return super.fromConnectData(topic, headers, schema, value);
// }
//
// @Override
// public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
// return super.toConnectData(topic, headers, value);
// }
//
// @Override
// public void configure(Map<String, ?> configs, boolean isKey) {
// super.configure(configs, isKey);
// }
//
// @Override
// public byte[] fromConnectData(String arg0, Schema arg1, Object arg2) {
// return super.fromConnectData(arg0, arg1, arg2);
// }
//
// @Override
// public SchemaAndValue toConnectData(String arg0, byte[] arg1) {
// return super.toConnectData(arg0, arg1);
// }
//
// }
