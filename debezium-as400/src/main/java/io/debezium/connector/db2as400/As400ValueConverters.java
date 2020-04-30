package io.debezium.connector.db2as400;

import java.sql.Types;
import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

/**
 * Conversion of DB2 specific datatypes.
 *
 * @author Jiri Pechanec, Peter Urbanetz
 *
 */
public class As400ValueConverters extends JdbcValueConverters {

    public As400ValueConverters() {
    }

}
