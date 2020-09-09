package io.debezium.connector.db2as400;

public class JournalPosition {
    private Long offset;
    private String journalReciever;
    private String journalLib;
    private static String[] empty = new String[]{};

    public JournalPosition(JournalPosition position) {
        this.offset = position.offset;
        this.journalReciever = position.journalReciever;
        this.journalLib = position.journalLib;
    }

    public JournalPosition() {
    }

    public JournalPosition(Long offset, String journalReciever, String journalLib) {
        this.offset = offset;
        this.journalReciever = journalReciever;
        this.journalLib = journalLib;
    }

    public long getOffset() {
        if (null == offset)
            return 0;
        return offset;
    }

    public boolean isOffsetSet() {
        return (null == offset);
    }

    public String getJournalReciever() {
        return journalReciever;
    }

    public String getJournalLib() {
        return journalLib;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public void setJournalReciever(String journalReciever, String journalLib) {
        this.journalReciever = journalReciever.trim();
        this.journalLib = journalLib.trim();
        this.offset = 1L;
    }

    public String[] getJournal() {
        if (journalReciever != null && journalLib != null)
            return new String[]{ journalReciever, journalLib, journalReciever, journalLib };
        else
            return empty;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((journalLib == null) ? 0 : journalLib.hashCode());
        result = prime * result + ((journalReciever == null) ? 0 : journalReciever.hashCode());
        result = prime * result + ((offset == null) ? 0 : offset.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        JournalPosition other = (JournalPosition) obj;
        if (journalLib == null) {
            if (other.journalLib != null)
                return false;
        }
        else if (!journalLib.equals(other.journalLib))
            return false;
        if (journalReciever == null) {
            if (other.journalReciever != null)
                return false;
        }
        else if (!journalReciever.equals(other.journalReciever))
            return false;
        if (offset == null) {
            if (other.offset != null)
                return false;
        }
        else if (!offset.equals(other.offset))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "As400JournalPosition [offset=" + offset + ", reciever=" + journalReciever + ", lib=" + journalLib + "]";
    }

}
