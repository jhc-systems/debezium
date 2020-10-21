/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400.adaptors;

import static com.ibm.as400.access.AS400DataType.TYPE_ARRAY;
import static com.ibm.as400.access.AS400DataType.TYPE_BIN1;
import static com.ibm.as400.access.AS400DataType.TYPE_BIN2;
import static com.ibm.as400.access.AS400DataType.TYPE_BIN4;
import static com.ibm.as400.access.AS400DataType.TYPE_BIN8;
import static com.ibm.as400.access.AS400DataType.TYPE_BYTE_ARRAY;
import static com.ibm.as400.access.AS400DataType.TYPE_DATE;
import static com.ibm.as400.access.AS400DataType.TYPE_DECFLOAT;
import static com.ibm.as400.access.AS400DataType.TYPE_FLOAT4;
import static com.ibm.as400.access.AS400DataType.TYPE_FLOAT8;
import static com.ibm.as400.access.AS400DataType.TYPE_PACKED;
import static com.ibm.as400.access.AS400DataType.TYPE_STRUCTURE;
import static com.ibm.as400.access.AS400DataType.TYPE_TEXT;
import static com.ibm.as400.access.AS400DataType.TYPE_TIME;
import static com.ibm.as400.access.AS400DataType.TYPE_TIMESTAMP;
import static com.ibm.as400.access.AS400DataType.TYPE_UBIN1;
import static com.ibm.as400.access.AS400DataType.TYPE_UBIN2;
import static com.ibm.as400.access.AS400DataType.TYPE_UBIN4;
import static com.ibm.as400.access.AS400DataType.TYPE_UBIN8;
import static com.ibm.as400.access.AS400DataType.TYPE_ZONED;

import java.sql.Types;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.FieldDescription;

import io.debezium.connector.db2as400.DynamicRecordFormat;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

public class FieldDescriptionToTable {
    private static final Logger log = LoggerFactory.getLogger(FieldDescriptionToTable.class);

    public static Table toTable(TableId tableId, DynamicRecordFormat format) {
        ArrayList<Column> columns = new ArrayList<>();
        FieldDescription descriptions[] = format.getFieldDescriptions();
        String[] keys = sanitise(format.getKeyFieldNames());
        for (int i = 0; i < descriptions.length; i++) {
            ColumnEditor ce = Column.editor();
            FieldDescription description = descriptions[i];
            ce.name(sanitise(description.getFieldName()));
            // ce.charsetName("utf-8");
            int type = description.getDataType().getInstanceType();
            switch (type) {
                case TYPE_ARRAY:
                    throw new IllegalArgumentException("unsupported type " + description.getDataType());
                case TYPE_BIN2:
                    ce.jdbcType(Types.SMALLINT);
                    ce.type("INTEGER");
                    ce.length(5).scale(0);
                    break;
                case TYPE_BIN4:
                    ce.jdbcType(Types.INTEGER);
                    ce.type("INTEGER");
                    ce.length(10).scale(0);
                    break;
                case TYPE_BIN8: // long
                    ce.jdbcType(Types.BIGINT);
                    ce.type("INTEGER");
                    ce.length(20).scale(0);
                    break;
                case TYPE_BYTE_ARRAY:
                    log.error("unsupported type TYPE_BYTE_ARRAY");
                    throw new IllegalArgumentException("unsupported type TYPE_BYTE_ARRAY");
                case TYPE_FLOAT4:
                    ce.jdbcType(Types.FLOAT)
                            .type("FLOAT")
                            .length(126);
                    break;
                case TYPE_FLOAT8:
                    ce.jdbcType(Types.DOUBLE)
                            .type("DOUBLE")
                            .length(126);
                    break;
                case TYPE_PACKED:// packed decimal
                    ce.jdbcType(Types.DOUBLE)
                            .type("DOUBLE")
                            .length(126);
                    break;
                case TYPE_STRUCTURE:
                    log.error("unsupported type TYPE_STRUCTURE");
                    throw new IllegalArgumentException("unsupported type " + description.getDataType());
                case TYPE_TEXT:
                    ce.jdbcType(Types.VARCHAR)
                            .type("VARCHAR")
                            .length(description.getLength());
                    break;
                case TYPE_UBIN2:
                    ce.jdbcType(Types.SMALLINT);
                    ce.type("INTEGER");
                    ce.length(5).scale(0);
                    break;
                case TYPE_UBIN4:
                    ce.jdbcType(Types.INTEGER);
                    ce.type("INTEGER");
                    ce.length(10).scale(0);
                    break;
                case TYPE_ZONED:
                    ce.jdbcType(Types.DOUBLE)
                            .type("DOUBLE")
                            .length(126);
                    break;
                case TYPE_DECFLOAT:
                    ce.jdbcType(Types.DOUBLE)
                            .type("DOUBLE")
                            .length(126);
                    break;
                case TYPE_BIN1:
                    ce.jdbcType(Types.SMALLINT);
                    ce.type("INTEGER");
                    ce.length(3).scale(0);
                    break;
                case TYPE_UBIN1:
                    ce.jdbcType(Types.SMALLINT);
                    ce.type("INTEGER");
                    ce.length(5).scale(0);
                    break;
                case TYPE_UBIN8:
                    ce.jdbcType(Types.BIGINT);
                    ce.type("INTEGER");
                    ce.length(20).scale(0);
                    break;
                case TYPE_DATE:
                    ce.jdbcType(Types.DATE)
                            .type("DATE");
                    break;
                case TYPE_TIME:
                    ce.jdbcType(Types.TIME)
                            .type("DATE");
                    break;
                case TYPE_TIMESTAMP:
                    ce.jdbcType(Types.TIMESTAMP)
                            .type("DATE");
                    break;
            }
            columns.add(ce.create());
        }

        Table table = Table.editor()
                .tableId(tableId)
                .addColumns(columns)
                .setPrimaryKeyNames(keys)
                .create();
        return table;
    }

    private static String[] sanitise(String[] names) {
        String[] snames = new String[names.length];
        for (int i = 0; i < names.length; i++) {
            snames[i] = sanitise(names[i]);
        }
        return snames;
    }

    private static String sanitise(String name) {
        return name.replaceAll("@", "_a_")
                .replaceAll("\\$", "_d_")
                .replaceAll("#", "_h_")
                .replaceAll(" ", "_s_");
    }
}
