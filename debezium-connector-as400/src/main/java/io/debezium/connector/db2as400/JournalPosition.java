/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

public class JournalPosition {
    private Long offset;
    private String journalReciever;
    private String schema;
    private static String[] empty = new String[]{};

    public JournalPosition(JournalPosition position) {
        this.offset = position.offset;
        this.journalReciever = position.journalReciever;
        this.schema = position.schema;
    }

    public JournalPosition() {
    }

    public JournalPosition(Long offset, String journalReciever, String schema) {
        this.offset = offset;
        this.journalReciever = (journalReciever == null) ? null : journalReciever.trim();
        this.schema = (schema == null) ? null : schema.trim();
    }

    public long getOffset() {
        if (null == offset) {
            return 0;
        }
        return offset;
    }

    public boolean isOffsetSet() {
        return (null != offset);
    }

    public String getJournalReciever() {
        return journalReciever;
    }

    public String getSchema() {
        return schema;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public void setJournalReciever(String journalReciever, String schema) {
        this.journalReciever = (journalReciever == null) ? null : journalReciever.trim();
        this.schema = (schema == null) ? null : schema.trim();
        this.offset = 1L;
    }

    public String[] getJournal() {
        if (journalReciever != null && schema != null) {
            return new String[]{ journalReciever, schema, journalReciever, schema };
        }
        else {
            return empty;
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((schema == null) ? 0 : schema.hashCode());
        result = prime * result + ((journalReciever == null) ? 0 : journalReciever.hashCode());
        result = prime * result + ((offset == null) ? 0 : offset.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        JournalPosition other = (JournalPosition) obj;
        if (schema == null) {
            if (other.schema != null) {
                return false;
            }
        }
        else if (!schema.equals(other.schema)) {
            return false;
        }
        if (journalReciever == null) {
            if (other.journalReciever != null) {
                return false;
            }
        }
        else {
            if (!journalReciever.equals(other.journalReciever)) {
                return false;
            }
        }
        if (offset == null) {
            if (other.offset != null) {
                return false;
            }
        }
        else {
            if (!offset.equals(other.offset)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "As400JournalPosition [offset=" + offset + ", reciever=" + journalReciever + ", schema=" + schema + "]";
    }

}
