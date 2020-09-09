package io.debezium.connector.db2as400.adaptors;

import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.CharacterFieldDescription;
import com.ibm.as400.access.RecordFormat;

public class JornalRecordFormats {
    public static RecordFormat journalReciever() {
        RecordFormat rf = new RecordFormat();
        CharacterFieldDescription sj = new CharacterFieldDescription(new AS400Text(10), "start journal");
        rf.addFieldDescription(sj);
        CharacterFieldDescription sl = new CharacterFieldDescription(new AS400Text(10), "start lib");
        rf.addFieldDescription(sl);
        CharacterFieldDescription ej = new CharacterFieldDescription(new AS400Text(10), "end journal");
        rf.addFieldDescription(ej);
        CharacterFieldDescription el = new CharacterFieldDescription(new AS400Text(10), "end lib");
        rf.addFieldDescription(el);
        return rf;
    }

}
