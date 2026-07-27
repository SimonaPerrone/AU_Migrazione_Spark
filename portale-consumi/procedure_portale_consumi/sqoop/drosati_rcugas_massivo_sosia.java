// ORM class for table 'drosati.rcugas_massivo_sosia'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Thu Nov 07 21:01:30 CET 2019
// For connector: org.apache.sqoop.manager.oracle.OraOopConnManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class drosati_rcugas_massivo_sosia extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_PDR = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_CODICE_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_PDR = (String)value;
      }
    });
    setters.put("CAPACITA_TRASPORTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CAPACITA_TRASPORTO = (String)value;
      }
    });
    setters.put("MESE_VAL_CAP_TRASP", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MESE_VAL_CAP_TRASP = (String)value;
      }
    });
    setters.put("T_COD_TIPO_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COD_TIPO_PDR = (String)value;
      }
    });
    setters.put("T_DISALIMENTABILITA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_DISALIMENTABILITA = (String)value;
      }
    });
    setters.put("BILANCIAMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BILANCIAMENTO = (String)value;
      }
    });
    setters.put("N_ID_FORNITURA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_FORNITURA = (java.math.BigDecimal)value;
      }
    });
    setters.put("D_DATA_INIZIO_FOR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_INIZIO_FOR = (String)value;
      }
    });
    setters.put("DATA_FINE_FOR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATA_FINE_FOR = (String)value;
      }
    });
    setters.put("N_ID_AZ_UDD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_AZ_UDD = (java.math.BigDecimal)value;
      }
    });
    setters.put("PIVA_UDD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PIVA_UDD = (String)value;
      }
    });
    setters.put("N_ID_AZ_CC", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_AZ_CC = (java.math.BigDecimal)value;
      }
    });
    setters.put("PIVA_CC", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PIVA_CC = (String)value;
      }
    });
    setters.put("N_ID_CLIENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_CLIENTE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_PARTITA_IVA_CLI", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PARTITA_IVA_CLI = (String)value;
      }
    });
    setters.put("T_CODICE_FISCALE_CLI", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_FISCALE_CLI = (String)value;
      }
    });
    setters.put("B_CF_STRANIERO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_CF_STRANIERO = (String)value;
      }
    });
    setters.put("T_REFERENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_REFERENTE = (String)value;
      }
    });
    setters.put("T_NOME_REF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_NOME_REF = (String)value;
      }
    });
    setters.put("T_COGNOME_REF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COGNOME_REF = (String)value;
      }
    });
    setters.put("T_EMAIL_REF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_EMAIL_REF = (String)value;
      }
    });
    setters.put("T_TELEFONO_REF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TELEFONO_REF = (String)value;
      }
    });
    setters.put("T_RESIDENZA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_RESIDENZA = (String)value;
      }
    });
    setters.put("DATA_VAL_RES", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATA_VAL_RES = (String)value;
      }
    });
    setters.put("T_TOPONIMOPDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TOPONIMOPDR = (String)value;
      }
    });
    setters.put("T_NOMESTRADA_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_NOMESTRADA_PDR = (String)value;
      }
    });
    setters.put("T_CIVICO_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CIVICO_PDR = (String)value;
      }
    });
    setters.put("T_CAP_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CAP_PDR = (String)value;
      }
    });
    setters.put("T_COMUNE_ISTAT_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COMUNE_ISTAT_PDR = (String)value;
      }
    });
    setters.put("T_COMUNE_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COMUNE_PDR = (String)value;
      }
    });
    setters.put("T_PROVINCIA_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PROVINCIA_PDR = (String)value;
      }
    });
    setters.put("T_NAZIONE_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_NAZIONE_PDR = (String)value;
      }
    });
    setters.put("ALTRO_IND_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ALTRO_IND_PDR = (String)value;
      }
    });
    setters.put("T_TOPONIMO_FORN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TOPONIMO_FORN = (String)value;
      }
    });
    setters.put("T_NOMESTRADA_FORN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_NOMESTRADA_FORN = (String)value;
      }
    });
    setters.put("T_CIVICO_FORN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CIVICO_FORN = (String)value;
      }
    });
    setters.put("T_CAP_FORN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CAP_FORN = (String)value;
      }
    });
    setters.put("T_COMUNE_ISTATFORN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COMUNE_ISTATFORN = (String)value;
      }
    });
    setters.put("T_COMUNE_FORN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COMUNE_FORN = (String)value;
      }
    });
    setters.put("T_PROVINCIA_FORN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PROVINCIA_FORN = (String)value;
      }
    });
    setters.put("T_NAZIONE_FORN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_NAZIONE_FORN = (String)value;
      }
    });
    setters.put("ALTRO_IND_FORN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ALTRO_IND_FORN = (String)value;
      }
    });
    setters.put("T_ACCESSO_UI", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ACCESSO_UI = (String)value;
      }
    });
    setters.put("T_TIPO_FORNITURA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO_FORNITURA = (String)value;
      }
    });
    setters.put("T_ALIQUOTA_IVA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ALIQUOTA_IVA = (String)value;
      }
    });
    setters.put("T_ALIQUOTA_ACCISE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ALIQUOTA_ACCISE = (String)value;
      }
    });
    setters.put("T_ADD_REGIONALE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ADD_REGIONALE = (String)value;
      }
    });
    setters.put("T_ALTRE_INFO_IMPOSTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ALTRE_INFO_IMPOSTE = (String)value;
      }
    });
    setters.put("T_MATRICOLA_MISURATORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MATRICOLA_MISURATORE = (String)value;
      }
    });
    setters.put("T_CLASSE_MISURATORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CLASSE_MISURATORE = (String)value;
      }
    });
    setters.put("T_TIPO_MISURATORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO_MISURATORE = (String)value;
      }
    });
    setters.put("T_TELEGESTIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TELEGESTIONE = (String)value;
      }
    });
    setters.put("T_PRE_CONV", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PRE_CONV = (String)value;
      }
    });
    setters.put("T_MATRICOLA_CONVERTITORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MATRICOLA_CONVERTITORE = (String)value;
      }
    });
    setters.put("N_NUM_CIFRE_CONVERTITORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_NUM_CIFRE_CONVERTITORE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_ANNO_FABBRIC_CONVERTITORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ANNO_FABBRIC_CONVERTITORE = (String)value;
      }
    });
    setters.put("T_DATA_INST_CONVERTITORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_DATA_INST_CONVERTITORE = (String)value;
      }
    });
    setters.put("N_COEFF_CORREZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_COEFF_CORREZIONE = (java.math.BigDecimal)value;
      }
    });
    setters.put("PRESS_MISURE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PRESS_MISURE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_ACCESS_MISURATORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ACCESS_MISURATORE = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_NUM_CIFRE_MISURATORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_NUM_CIFRE_MISURATORE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_ANNO_FABBRIC_MISURATORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ANNO_FABBRIC_MISURATORE = (String)value;
      }
    });
    setters.put("T_DATA_INST_MISURATORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_DATA_INST_MISURATORE = (String)value;
      }
    });
    setters.put("T_MISURATORE_INTEGRATO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MISURATORE_INTEGRATO = (String)value;
      }
    });
    setters.put("N_POTENZIALITA_MASSIMA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_POTENZIALITA_MASSIMA = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_POTENZIALITA_TOT_INSTALLATA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_POTENZIALITA_TOT_INSTALLATA = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_MAX_PRELIEVO_ORARIO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_MAX_PRELIEVO_ORARIO = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_EROG_SERVIZIO_ENERG", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_EROG_SERVIZIO_ENERG = (String)value;
      }
    });
    setters.put("T_PARTITA_IVA_GESTCAL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PARTITA_IVA_GESTCAL = (String)value;
      }
    });
    setters.put("T_RAGIONE_SOCIALE_GESTCAL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_RAGIONE_SOCIALE_GESTCAL = (String)value;
      }
    });
    setters.put("T_TELEFONO_GESTCAL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TELEFONO_GESTCAL = (String)value;
      }
    });
    setters.put("T_EMAIL_GESTCAL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_EMAIL_GESTCAL = (String)value;
      }
    });
    setters.put("T_TOPONIMO_GESTCAL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TOPONIMO_GESTCAL = (String)value;
      }
    });
    setters.put("T_NOMESTRADA_GESTCAL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_NOMESTRADA_GESTCAL = (String)value;
      }
    });
    setters.put("T_CIVICO_GESTCAL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CIVICO_GESTCAL = (String)value;
      }
    });
    setters.put("T_CAP_GESTCAL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CAP_GESTCAL = (String)value;
      }
    });
    setters.put("T_COMUNE_ISTAT_GESTCAL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COMUNE_ISTAT_GESTCAL = (String)value;
      }
    });
    setters.put("T_COMUNE_GESTCAL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COMUNE_GESTCAL = (String)value;
      }
    });
    setters.put("T_PROVINCIA_GESTCAL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PROVINCIA_GESTCAL = (String)value;
      }
    });
    setters.put("T_NAZIONE_GESTCAL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_NAZIONE_GESTCAL = (String)value;
      }
    });
    setters.put("D_DATA_RIF_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_RIF_PDR = (String)value;
      }
    });
    setters.put("D_AGGIORNAMENTO_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_AGGIORNAMENTO_PDR = (String)value;
      }
    });
    setters.put("D_DATA_RIF_TECN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_RIF_TECN = (String)value;
      }
    });
    setters.put("D_AGGIORNAMENTO_TECN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_AGGIORNAMENTO_TECN = (String)value;
      }
    });
    setters.put("D_DATA_RIF_MIS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_RIF_MIS = (String)value;
      }
    });
    setters.put("D_AGGIORNAMENTO_MIS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_AGGIORNAMENTO_MIS = (String)value;
      }
    });
    setters.put("D_DATA_RIF_FORN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_RIF_FORN = (String)value;
      }
    });
    setters.put("D_AGGIORNAMENTO_FORN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_AGGIORNAMENTO_FORN = (String)value;
      }
    });
    setters.put("T_TIPO_BONUS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO_BONUS = (String)value;
      }
    });
    setters.put("D_DATA_INIZIO_EROG_BONUS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_INIZIO_EROG_BONUS = (String)value;
      }
    });
    setters.put("D_DATA_FINE_EROG_BONUS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_FINE_EROG_BONUS = (String)value;
      }
    });
    setters.put("D_DATA_RIF_BONUS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_RIF_BONUS = (String)value;
      }
    });
    setters.put("D_AGGIORNAMENTO_BONUS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_AGGIORNAMENTO_BONUS = (String)value;
      }
    });
    setters.put("D_DATA_AGGIORNAMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_AGGIORNAMENTO = (String)value;
      }
    });
    setters.put("N_ID_UDD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_UDD = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_VENDITORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_VENDITORE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_COD_PROFILO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COD_PROFILO = (String)value;
      }
    });
    setters.put("T_COD_CAT_USO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COD_CAT_USO = (String)value;
      }
    });
    setters.put("T_COD_CLASSE_PRELIEVO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COD_CLASSE_PRELIEVO = (String)value;
      }
    });
    setters.put("T_ANNO_TERMICO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ANNO_TERMICO = (String)value;
      }
    });
    setters.put("D_DATA_RIF_PREL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_RIF_PREL = (String)value;
      }
    });
    setters.put("T_TRATTAMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TRATTAMENTO = (String)value;
      }
    });
    setters.put("T_TOPONIMO_ESAZ", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TOPONIMO_ESAZ = (String)value;
      }
    });
    setters.put("T_NOMESTRADA_ESAZ", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_NOMESTRADA_ESAZ = (String)value;
      }
    });
    setters.put("T_CIVICO_ESAZ", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CIVICO_ESAZ = (String)value;
      }
    });
    setters.put("T_CAP_ESAZ", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CAP_ESAZ = (String)value;
      }
    });
    setters.put("T_COMUNE_ISTAT_ESAZ", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COMUNE_ISTAT_ESAZ = (String)value;
      }
    });
    setters.put("T_COMUNE_ESAZ", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COMUNE_ESAZ = (String)value;
      }
    });
    setters.put("T_PROVINCIA_ESAZ", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PROVINCIA_ESAZ = (String)value;
      }
    });
    setters.put("T_NAZIONE_ESAZ", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_NAZIONE_ESAZ = (String)value;
      }
    });
    setters.put("ALTRO_IND_ESAZ", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ALTRO_IND_ESAZ = (String)value;
      }
    });
    setters.put("T_CODICE_ATECO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_ATECO = (String)value;
      }
    });
    setters.put("T_PAGAMENTO_IVA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PAGAMENTO_IVA = (String)value;
      }
    });
    setters.put("T_CODICE_UFFICIO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_UFFICIO = (String)value;
      }
    });
    setters.put("T_CF_INTESTATARIO_FATT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CF_INTESTATARIO_FATT = (String)value;
      }
    });
    setters.put("T_CF_STRANIERO_FATT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CF_STRANIERO_FATT = (String)value;
      }
    });
    setters.put("T_PIVA_INTESTATARIO_FATT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PIVA_INTESTATARIO_FATT = (String)value;
      }
    });
    setters.put("T_NOME_INTESTATARIO_FATT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_NOME_INTESTATARIO_FATT = (String)value;
      }
    });
    setters.put("T_COGNOME_INTESTATARIO_FATT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COGNOME_INTESTATARIO_FATT = (String)value;
      }
    });
    setters.put("T_RAG_SOC_INTESTATARIO_FATT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_RAG_SOC_INTESTATARIO_FATT = (String)value;
      }
    });
    setters.put("T_ANNO_MESE_RINN_BONUS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ANNO_MESE_RINN_BONUS = (String)value;
      }
    });
    setters.put("D_DATA_INIZIO_BONUS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_INIZIO_BONUS = (String)value;
      }
    });
    setters.put("D_DATA_FINE_BONUS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_FINE_BONUS = (String)value;
      }
    });
    setters.put("N_PRELIEVO_ANNUO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_PRELIEVO_ANNUO = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_FATTORE_CORREZ_CLIMATICA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_FATTORE_CORREZ_CLIMATICA = (String)value;
      }
    });
    setters.put("T_ALTRO_IND_GESTCAL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ALTRO_IND_GESTCAL = (String)value;
      }
    });
    setters.put("T_TIPO_OP", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO_OP = (String)value;
      }
    });
    setters.put("T_PROCESSO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PROCESSO = (String)value;
      }
    });
    setters.put("N_ID_PRATICA_PROCESSO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_PRATICA_PROCESSO = (java.math.BigDecimal)value;
      }
    });
  }
  public drosati_rcugas_massivo_sosia() {
    init0();
  }
  private java.math.BigDecimal N_ID_PDR;
  public java.math.BigDecimal get_N_ID_PDR() {
    return N_ID_PDR;
  }
  public void set_N_ID_PDR(java.math.BigDecimal N_ID_PDR) {
    this.N_ID_PDR = N_ID_PDR;
  }
  public drosati_rcugas_massivo_sosia with_N_ID_PDR(java.math.BigDecimal N_ID_PDR) {
    this.N_ID_PDR = N_ID_PDR;
    return this;
  }
  private String T_CODICE_PDR;
  public String get_T_CODICE_PDR() {
    return T_CODICE_PDR;
  }
  public void set_T_CODICE_PDR(String T_CODICE_PDR) {
    this.T_CODICE_PDR = T_CODICE_PDR;
  }
  public drosati_rcugas_massivo_sosia with_T_CODICE_PDR(String T_CODICE_PDR) {
    this.T_CODICE_PDR = T_CODICE_PDR;
    return this;
  }
  private String CAPACITA_TRASPORTO;
  public String get_CAPACITA_TRASPORTO() {
    return CAPACITA_TRASPORTO;
  }
  public void set_CAPACITA_TRASPORTO(String CAPACITA_TRASPORTO) {
    this.CAPACITA_TRASPORTO = CAPACITA_TRASPORTO;
  }
  public drosati_rcugas_massivo_sosia with_CAPACITA_TRASPORTO(String CAPACITA_TRASPORTO) {
    this.CAPACITA_TRASPORTO = CAPACITA_TRASPORTO;
    return this;
  }
  private String MESE_VAL_CAP_TRASP;
  public String get_MESE_VAL_CAP_TRASP() {
    return MESE_VAL_CAP_TRASP;
  }
  public void set_MESE_VAL_CAP_TRASP(String MESE_VAL_CAP_TRASP) {
    this.MESE_VAL_CAP_TRASP = MESE_VAL_CAP_TRASP;
  }
  public drosati_rcugas_massivo_sosia with_MESE_VAL_CAP_TRASP(String MESE_VAL_CAP_TRASP) {
    this.MESE_VAL_CAP_TRASP = MESE_VAL_CAP_TRASP;
    return this;
  }
  private String T_COD_TIPO_PDR;
  public String get_T_COD_TIPO_PDR() {
    return T_COD_TIPO_PDR;
  }
  public void set_T_COD_TIPO_PDR(String T_COD_TIPO_PDR) {
    this.T_COD_TIPO_PDR = T_COD_TIPO_PDR;
  }
  public drosati_rcugas_massivo_sosia with_T_COD_TIPO_PDR(String T_COD_TIPO_PDR) {
    this.T_COD_TIPO_PDR = T_COD_TIPO_PDR;
    return this;
  }
  private String T_DISALIMENTABILITA;
  public String get_T_DISALIMENTABILITA() {
    return T_DISALIMENTABILITA;
  }
  public void set_T_DISALIMENTABILITA(String T_DISALIMENTABILITA) {
    this.T_DISALIMENTABILITA = T_DISALIMENTABILITA;
  }
  public drosati_rcugas_massivo_sosia with_T_DISALIMENTABILITA(String T_DISALIMENTABILITA) {
    this.T_DISALIMENTABILITA = T_DISALIMENTABILITA;
    return this;
  }
  private String BILANCIAMENTO;
  public String get_BILANCIAMENTO() {
    return BILANCIAMENTO;
  }
  public void set_BILANCIAMENTO(String BILANCIAMENTO) {
    this.BILANCIAMENTO = BILANCIAMENTO;
  }
  public drosati_rcugas_massivo_sosia with_BILANCIAMENTO(String BILANCIAMENTO) {
    this.BILANCIAMENTO = BILANCIAMENTO;
    return this;
  }
  private java.math.BigDecimal N_ID_FORNITURA;
  public java.math.BigDecimal get_N_ID_FORNITURA() {
    return N_ID_FORNITURA;
  }
  public void set_N_ID_FORNITURA(java.math.BigDecimal N_ID_FORNITURA) {
    this.N_ID_FORNITURA = N_ID_FORNITURA;
  }
  public drosati_rcugas_massivo_sosia with_N_ID_FORNITURA(java.math.BigDecimal N_ID_FORNITURA) {
    this.N_ID_FORNITURA = N_ID_FORNITURA;
    return this;
  }
  private String D_DATA_INIZIO_FOR;
  public String get_D_DATA_INIZIO_FOR() {
    return D_DATA_INIZIO_FOR;
  }
  public void set_D_DATA_INIZIO_FOR(String D_DATA_INIZIO_FOR) {
    this.D_DATA_INIZIO_FOR = D_DATA_INIZIO_FOR;
  }
  public drosati_rcugas_massivo_sosia with_D_DATA_INIZIO_FOR(String D_DATA_INIZIO_FOR) {
    this.D_DATA_INIZIO_FOR = D_DATA_INIZIO_FOR;
    return this;
  }
  private String DATA_FINE_FOR;
  public String get_DATA_FINE_FOR() {
    return DATA_FINE_FOR;
  }
  public void set_DATA_FINE_FOR(String DATA_FINE_FOR) {
    this.DATA_FINE_FOR = DATA_FINE_FOR;
  }
  public drosati_rcugas_massivo_sosia with_DATA_FINE_FOR(String DATA_FINE_FOR) {
    this.DATA_FINE_FOR = DATA_FINE_FOR;
    return this;
  }
  private java.math.BigDecimal N_ID_AZ_UDD;
  public java.math.BigDecimal get_N_ID_AZ_UDD() {
    return N_ID_AZ_UDD;
  }
  public void set_N_ID_AZ_UDD(java.math.BigDecimal N_ID_AZ_UDD) {
    this.N_ID_AZ_UDD = N_ID_AZ_UDD;
  }
  public drosati_rcugas_massivo_sosia with_N_ID_AZ_UDD(java.math.BigDecimal N_ID_AZ_UDD) {
    this.N_ID_AZ_UDD = N_ID_AZ_UDD;
    return this;
  }
  private String PIVA_UDD;
  public String get_PIVA_UDD() {
    return PIVA_UDD;
  }
  public void set_PIVA_UDD(String PIVA_UDD) {
    this.PIVA_UDD = PIVA_UDD;
  }
  public drosati_rcugas_massivo_sosia with_PIVA_UDD(String PIVA_UDD) {
    this.PIVA_UDD = PIVA_UDD;
    return this;
  }
  private java.math.BigDecimal N_ID_AZ_CC;
  public java.math.BigDecimal get_N_ID_AZ_CC() {
    return N_ID_AZ_CC;
  }
  public void set_N_ID_AZ_CC(java.math.BigDecimal N_ID_AZ_CC) {
    this.N_ID_AZ_CC = N_ID_AZ_CC;
  }
  public drosati_rcugas_massivo_sosia with_N_ID_AZ_CC(java.math.BigDecimal N_ID_AZ_CC) {
    this.N_ID_AZ_CC = N_ID_AZ_CC;
    return this;
  }
  private String PIVA_CC;
  public String get_PIVA_CC() {
    return PIVA_CC;
  }
  public void set_PIVA_CC(String PIVA_CC) {
    this.PIVA_CC = PIVA_CC;
  }
  public drosati_rcugas_massivo_sosia with_PIVA_CC(String PIVA_CC) {
    this.PIVA_CC = PIVA_CC;
    return this;
  }
  private java.math.BigDecimal N_ID_CLIENTE;
  public java.math.BigDecimal get_N_ID_CLIENTE() {
    return N_ID_CLIENTE;
  }
  public void set_N_ID_CLIENTE(java.math.BigDecimal N_ID_CLIENTE) {
    this.N_ID_CLIENTE = N_ID_CLIENTE;
  }
  public drosati_rcugas_massivo_sosia with_N_ID_CLIENTE(java.math.BigDecimal N_ID_CLIENTE) {
    this.N_ID_CLIENTE = N_ID_CLIENTE;
    return this;
  }
  private String T_PARTITA_IVA_CLI;
  public String get_T_PARTITA_IVA_CLI() {
    return T_PARTITA_IVA_CLI;
  }
  public void set_T_PARTITA_IVA_CLI(String T_PARTITA_IVA_CLI) {
    this.T_PARTITA_IVA_CLI = T_PARTITA_IVA_CLI;
  }
  public drosati_rcugas_massivo_sosia with_T_PARTITA_IVA_CLI(String T_PARTITA_IVA_CLI) {
    this.T_PARTITA_IVA_CLI = T_PARTITA_IVA_CLI;
    return this;
  }
  private String T_CODICE_FISCALE_CLI;
  public String get_T_CODICE_FISCALE_CLI() {
    return T_CODICE_FISCALE_CLI;
  }
  public void set_T_CODICE_FISCALE_CLI(String T_CODICE_FISCALE_CLI) {
    this.T_CODICE_FISCALE_CLI = T_CODICE_FISCALE_CLI;
  }
  public drosati_rcugas_massivo_sosia with_T_CODICE_FISCALE_CLI(String T_CODICE_FISCALE_CLI) {
    this.T_CODICE_FISCALE_CLI = T_CODICE_FISCALE_CLI;
    return this;
  }
  private String B_CF_STRANIERO;
  public String get_B_CF_STRANIERO() {
    return B_CF_STRANIERO;
  }
  public void set_B_CF_STRANIERO(String B_CF_STRANIERO) {
    this.B_CF_STRANIERO = B_CF_STRANIERO;
  }
  public drosati_rcugas_massivo_sosia with_B_CF_STRANIERO(String B_CF_STRANIERO) {
    this.B_CF_STRANIERO = B_CF_STRANIERO;
    return this;
  }
  private String T_REFERENTE;
  public String get_T_REFERENTE() {
    return T_REFERENTE;
  }
  public void set_T_REFERENTE(String T_REFERENTE) {
    this.T_REFERENTE = T_REFERENTE;
  }
  public drosati_rcugas_massivo_sosia with_T_REFERENTE(String T_REFERENTE) {
    this.T_REFERENTE = T_REFERENTE;
    return this;
  }
  private String T_NOME_REF;
  public String get_T_NOME_REF() {
    return T_NOME_REF;
  }
  public void set_T_NOME_REF(String T_NOME_REF) {
    this.T_NOME_REF = T_NOME_REF;
  }
  public drosati_rcugas_massivo_sosia with_T_NOME_REF(String T_NOME_REF) {
    this.T_NOME_REF = T_NOME_REF;
    return this;
  }
  private String T_COGNOME_REF;
  public String get_T_COGNOME_REF() {
    return T_COGNOME_REF;
  }
  public void set_T_COGNOME_REF(String T_COGNOME_REF) {
    this.T_COGNOME_REF = T_COGNOME_REF;
  }
  public drosati_rcugas_massivo_sosia with_T_COGNOME_REF(String T_COGNOME_REF) {
    this.T_COGNOME_REF = T_COGNOME_REF;
    return this;
  }
  private String T_EMAIL_REF;
  public String get_T_EMAIL_REF() {
    return T_EMAIL_REF;
  }
  public void set_T_EMAIL_REF(String T_EMAIL_REF) {
    this.T_EMAIL_REF = T_EMAIL_REF;
  }
  public drosati_rcugas_massivo_sosia with_T_EMAIL_REF(String T_EMAIL_REF) {
    this.T_EMAIL_REF = T_EMAIL_REF;
    return this;
  }
  private String T_TELEFONO_REF;
  public String get_T_TELEFONO_REF() {
    return T_TELEFONO_REF;
  }
  public void set_T_TELEFONO_REF(String T_TELEFONO_REF) {
    this.T_TELEFONO_REF = T_TELEFONO_REF;
  }
  public drosati_rcugas_massivo_sosia with_T_TELEFONO_REF(String T_TELEFONO_REF) {
    this.T_TELEFONO_REF = T_TELEFONO_REF;
    return this;
  }
  private String T_RESIDENZA;
  public String get_T_RESIDENZA() {
    return T_RESIDENZA;
  }
  public void set_T_RESIDENZA(String T_RESIDENZA) {
    this.T_RESIDENZA = T_RESIDENZA;
  }
  public drosati_rcugas_massivo_sosia with_T_RESIDENZA(String T_RESIDENZA) {
    this.T_RESIDENZA = T_RESIDENZA;
    return this;
  }
  private String DATA_VAL_RES;
  public String get_DATA_VAL_RES() {
    return DATA_VAL_RES;
  }
  public void set_DATA_VAL_RES(String DATA_VAL_RES) {
    this.DATA_VAL_RES = DATA_VAL_RES;
  }
  public drosati_rcugas_massivo_sosia with_DATA_VAL_RES(String DATA_VAL_RES) {
    this.DATA_VAL_RES = DATA_VAL_RES;
    return this;
  }
  private String T_TOPONIMOPDR;
  public String get_T_TOPONIMOPDR() {
    return T_TOPONIMOPDR;
  }
  public void set_T_TOPONIMOPDR(String T_TOPONIMOPDR) {
    this.T_TOPONIMOPDR = T_TOPONIMOPDR;
  }
  public drosati_rcugas_massivo_sosia with_T_TOPONIMOPDR(String T_TOPONIMOPDR) {
    this.T_TOPONIMOPDR = T_TOPONIMOPDR;
    return this;
  }
  private String T_NOMESTRADA_PDR;
  public String get_T_NOMESTRADA_PDR() {
    return T_NOMESTRADA_PDR;
  }
  public void set_T_NOMESTRADA_PDR(String T_NOMESTRADA_PDR) {
    this.T_NOMESTRADA_PDR = T_NOMESTRADA_PDR;
  }
  public drosati_rcugas_massivo_sosia with_T_NOMESTRADA_PDR(String T_NOMESTRADA_PDR) {
    this.T_NOMESTRADA_PDR = T_NOMESTRADA_PDR;
    return this;
  }
  private String T_CIVICO_PDR;
  public String get_T_CIVICO_PDR() {
    return T_CIVICO_PDR;
  }
  public void set_T_CIVICO_PDR(String T_CIVICO_PDR) {
    this.T_CIVICO_PDR = T_CIVICO_PDR;
  }
  public drosati_rcugas_massivo_sosia with_T_CIVICO_PDR(String T_CIVICO_PDR) {
    this.T_CIVICO_PDR = T_CIVICO_PDR;
    return this;
  }
  private String T_CAP_PDR;
  public String get_T_CAP_PDR() {
    return T_CAP_PDR;
  }
  public void set_T_CAP_PDR(String T_CAP_PDR) {
    this.T_CAP_PDR = T_CAP_PDR;
  }
  public drosati_rcugas_massivo_sosia with_T_CAP_PDR(String T_CAP_PDR) {
    this.T_CAP_PDR = T_CAP_PDR;
    return this;
  }
  private String T_COMUNE_ISTAT_PDR;
  public String get_T_COMUNE_ISTAT_PDR() {
    return T_COMUNE_ISTAT_PDR;
  }
  public void set_T_COMUNE_ISTAT_PDR(String T_COMUNE_ISTAT_PDR) {
    this.T_COMUNE_ISTAT_PDR = T_COMUNE_ISTAT_PDR;
  }
  public drosati_rcugas_massivo_sosia with_T_COMUNE_ISTAT_PDR(String T_COMUNE_ISTAT_PDR) {
    this.T_COMUNE_ISTAT_PDR = T_COMUNE_ISTAT_PDR;
    return this;
  }
  private String T_COMUNE_PDR;
  public String get_T_COMUNE_PDR() {
    return T_COMUNE_PDR;
  }
  public void set_T_COMUNE_PDR(String T_COMUNE_PDR) {
    this.T_COMUNE_PDR = T_COMUNE_PDR;
  }
  public drosati_rcugas_massivo_sosia with_T_COMUNE_PDR(String T_COMUNE_PDR) {
    this.T_COMUNE_PDR = T_COMUNE_PDR;
    return this;
  }
  private String T_PROVINCIA_PDR;
  public String get_T_PROVINCIA_PDR() {
    return T_PROVINCIA_PDR;
  }
  public void set_T_PROVINCIA_PDR(String T_PROVINCIA_PDR) {
    this.T_PROVINCIA_PDR = T_PROVINCIA_PDR;
  }
  public drosati_rcugas_massivo_sosia with_T_PROVINCIA_PDR(String T_PROVINCIA_PDR) {
    this.T_PROVINCIA_PDR = T_PROVINCIA_PDR;
    return this;
  }
  private String T_NAZIONE_PDR;
  public String get_T_NAZIONE_PDR() {
    return T_NAZIONE_PDR;
  }
  public void set_T_NAZIONE_PDR(String T_NAZIONE_PDR) {
    this.T_NAZIONE_PDR = T_NAZIONE_PDR;
  }
  public drosati_rcugas_massivo_sosia with_T_NAZIONE_PDR(String T_NAZIONE_PDR) {
    this.T_NAZIONE_PDR = T_NAZIONE_PDR;
    return this;
  }
  private String ALTRO_IND_PDR;
  public String get_ALTRO_IND_PDR() {
    return ALTRO_IND_PDR;
  }
  public void set_ALTRO_IND_PDR(String ALTRO_IND_PDR) {
    this.ALTRO_IND_PDR = ALTRO_IND_PDR;
  }
  public drosati_rcugas_massivo_sosia with_ALTRO_IND_PDR(String ALTRO_IND_PDR) {
    this.ALTRO_IND_PDR = ALTRO_IND_PDR;
    return this;
  }
  private String T_TOPONIMO_FORN;
  public String get_T_TOPONIMO_FORN() {
    return T_TOPONIMO_FORN;
  }
  public void set_T_TOPONIMO_FORN(String T_TOPONIMO_FORN) {
    this.T_TOPONIMO_FORN = T_TOPONIMO_FORN;
  }
  public drosati_rcugas_massivo_sosia with_T_TOPONIMO_FORN(String T_TOPONIMO_FORN) {
    this.T_TOPONIMO_FORN = T_TOPONIMO_FORN;
    return this;
  }
  private String T_NOMESTRADA_FORN;
  public String get_T_NOMESTRADA_FORN() {
    return T_NOMESTRADA_FORN;
  }
  public void set_T_NOMESTRADA_FORN(String T_NOMESTRADA_FORN) {
    this.T_NOMESTRADA_FORN = T_NOMESTRADA_FORN;
  }
  public drosati_rcugas_massivo_sosia with_T_NOMESTRADA_FORN(String T_NOMESTRADA_FORN) {
    this.T_NOMESTRADA_FORN = T_NOMESTRADA_FORN;
    return this;
  }
  private String T_CIVICO_FORN;
  public String get_T_CIVICO_FORN() {
    return T_CIVICO_FORN;
  }
  public void set_T_CIVICO_FORN(String T_CIVICO_FORN) {
    this.T_CIVICO_FORN = T_CIVICO_FORN;
  }
  public drosati_rcugas_massivo_sosia with_T_CIVICO_FORN(String T_CIVICO_FORN) {
    this.T_CIVICO_FORN = T_CIVICO_FORN;
    return this;
  }
  private String T_CAP_FORN;
  public String get_T_CAP_FORN() {
    return T_CAP_FORN;
  }
  public void set_T_CAP_FORN(String T_CAP_FORN) {
    this.T_CAP_FORN = T_CAP_FORN;
  }
  public drosati_rcugas_massivo_sosia with_T_CAP_FORN(String T_CAP_FORN) {
    this.T_CAP_FORN = T_CAP_FORN;
    return this;
  }
  private String T_COMUNE_ISTATFORN;
  public String get_T_COMUNE_ISTATFORN() {
    return T_COMUNE_ISTATFORN;
  }
  public void set_T_COMUNE_ISTATFORN(String T_COMUNE_ISTATFORN) {
    this.T_COMUNE_ISTATFORN = T_COMUNE_ISTATFORN;
  }
  public drosati_rcugas_massivo_sosia with_T_COMUNE_ISTATFORN(String T_COMUNE_ISTATFORN) {
    this.T_COMUNE_ISTATFORN = T_COMUNE_ISTATFORN;
    return this;
  }
  private String T_COMUNE_FORN;
  public String get_T_COMUNE_FORN() {
    return T_COMUNE_FORN;
  }
  public void set_T_COMUNE_FORN(String T_COMUNE_FORN) {
    this.T_COMUNE_FORN = T_COMUNE_FORN;
  }
  public drosati_rcugas_massivo_sosia with_T_COMUNE_FORN(String T_COMUNE_FORN) {
    this.T_COMUNE_FORN = T_COMUNE_FORN;
    return this;
  }
  private String T_PROVINCIA_FORN;
  public String get_T_PROVINCIA_FORN() {
    return T_PROVINCIA_FORN;
  }
  public void set_T_PROVINCIA_FORN(String T_PROVINCIA_FORN) {
    this.T_PROVINCIA_FORN = T_PROVINCIA_FORN;
  }
  public drosati_rcugas_massivo_sosia with_T_PROVINCIA_FORN(String T_PROVINCIA_FORN) {
    this.T_PROVINCIA_FORN = T_PROVINCIA_FORN;
    return this;
  }
  private String T_NAZIONE_FORN;
  public String get_T_NAZIONE_FORN() {
    return T_NAZIONE_FORN;
  }
  public void set_T_NAZIONE_FORN(String T_NAZIONE_FORN) {
    this.T_NAZIONE_FORN = T_NAZIONE_FORN;
  }
  public drosati_rcugas_massivo_sosia with_T_NAZIONE_FORN(String T_NAZIONE_FORN) {
    this.T_NAZIONE_FORN = T_NAZIONE_FORN;
    return this;
  }
  private String ALTRO_IND_FORN;
  public String get_ALTRO_IND_FORN() {
    return ALTRO_IND_FORN;
  }
  public void set_ALTRO_IND_FORN(String ALTRO_IND_FORN) {
    this.ALTRO_IND_FORN = ALTRO_IND_FORN;
  }
  public drosati_rcugas_massivo_sosia with_ALTRO_IND_FORN(String ALTRO_IND_FORN) {
    this.ALTRO_IND_FORN = ALTRO_IND_FORN;
    return this;
  }
  private String T_ACCESSO_UI;
  public String get_T_ACCESSO_UI() {
    return T_ACCESSO_UI;
  }
  public void set_T_ACCESSO_UI(String T_ACCESSO_UI) {
    this.T_ACCESSO_UI = T_ACCESSO_UI;
  }
  public drosati_rcugas_massivo_sosia with_T_ACCESSO_UI(String T_ACCESSO_UI) {
    this.T_ACCESSO_UI = T_ACCESSO_UI;
    return this;
  }
  private String T_TIPO_FORNITURA;
  public String get_T_TIPO_FORNITURA() {
    return T_TIPO_FORNITURA;
  }
  public void set_T_TIPO_FORNITURA(String T_TIPO_FORNITURA) {
    this.T_TIPO_FORNITURA = T_TIPO_FORNITURA;
  }
  public drosati_rcugas_massivo_sosia with_T_TIPO_FORNITURA(String T_TIPO_FORNITURA) {
    this.T_TIPO_FORNITURA = T_TIPO_FORNITURA;
    return this;
  }
  private String T_ALIQUOTA_IVA;
  public String get_T_ALIQUOTA_IVA() {
    return T_ALIQUOTA_IVA;
  }
  public void set_T_ALIQUOTA_IVA(String T_ALIQUOTA_IVA) {
    this.T_ALIQUOTA_IVA = T_ALIQUOTA_IVA;
  }
  public drosati_rcugas_massivo_sosia with_T_ALIQUOTA_IVA(String T_ALIQUOTA_IVA) {
    this.T_ALIQUOTA_IVA = T_ALIQUOTA_IVA;
    return this;
  }
  private String T_ALIQUOTA_ACCISE;
  public String get_T_ALIQUOTA_ACCISE() {
    return T_ALIQUOTA_ACCISE;
  }
  public void set_T_ALIQUOTA_ACCISE(String T_ALIQUOTA_ACCISE) {
    this.T_ALIQUOTA_ACCISE = T_ALIQUOTA_ACCISE;
  }
  public drosati_rcugas_massivo_sosia with_T_ALIQUOTA_ACCISE(String T_ALIQUOTA_ACCISE) {
    this.T_ALIQUOTA_ACCISE = T_ALIQUOTA_ACCISE;
    return this;
  }
  private String T_ADD_REGIONALE;
  public String get_T_ADD_REGIONALE() {
    return T_ADD_REGIONALE;
  }
  public void set_T_ADD_REGIONALE(String T_ADD_REGIONALE) {
    this.T_ADD_REGIONALE = T_ADD_REGIONALE;
  }
  public drosati_rcugas_massivo_sosia with_T_ADD_REGIONALE(String T_ADD_REGIONALE) {
    this.T_ADD_REGIONALE = T_ADD_REGIONALE;
    return this;
  }
  private String T_ALTRE_INFO_IMPOSTE;
  public String get_T_ALTRE_INFO_IMPOSTE() {
    return T_ALTRE_INFO_IMPOSTE;
  }
  public void set_T_ALTRE_INFO_IMPOSTE(String T_ALTRE_INFO_IMPOSTE) {
    this.T_ALTRE_INFO_IMPOSTE = T_ALTRE_INFO_IMPOSTE;
  }
  public drosati_rcugas_massivo_sosia with_T_ALTRE_INFO_IMPOSTE(String T_ALTRE_INFO_IMPOSTE) {
    this.T_ALTRE_INFO_IMPOSTE = T_ALTRE_INFO_IMPOSTE;
    return this;
  }
  private String T_MATRICOLA_MISURATORE;
  public String get_T_MATRICOLA_MISURATORE() {
    return T_MATRICOLA_MISURATORE;
  }
  public void set_T_MATRICOLA_MISURATORE(String T_MATRICOLA_MISURATORE) {
    this.T_MATRICOLA_MISURATORE = T_MATRICOLA_MISURATORE;
  }
  public drosati_rcugas_massivo_sosia with_T_MATRICOLA_MISURATORE(String T_MATRICOLA_MISURATORE) {
    this.T_MATRICOLA_MISURATORE = T_MATRICOLA_MISURATORE;
    return this;
  }
  private String T_CLASSE_MISURATORE;
  public String get_T_CLASSE_MISURATORE() {
    return T_CLASSE_MISURATORE;
  }
  public void set_T_CLASSE_MISURATORE(String T_CLASSE_MISURATORE) {
    this.T_CLASSE_MISURATORE = T_CLASSE_MISURATORE;
  }
  public drosati_rcugas_massivo_sosia with_T_CLASSE_MISURATORE(String T_CLASSE_MISURATORE) {
    this.T_CLASSE_MISURATORE = T_CLASSE_MISURATORE;
    return this;
  }
  private String T_TIPO_MISURATORE;
  public String get_T_TIPO_MISURATORE() {
    return T_TIPO_MISURATORE;
  }
  public void set_T_TIPO_MISURATORE(String T_TIPO_MISURATORE) {
    this.T_TIPO_MISURATORE = T_TIPO_MISURATORE;
  }
  public drosati_rcugas_massivo_sosia with_T_TIPO_MISURATORE(String T_TIPO_MISURATORE) {
    this.T_TIPO_MISURATORE = T_TIPO_MISURATORE;
    return this;
  }
  private String T_TELEGESTIONE;
  public String get_T_TELEGESTIONE() {
    return T_TELEGESTIONE;
  }
  public void set_T_TELEGESTIONE(String T_TELEGESTIONE) {
    this.T_TELEGESTIONE = T_TELEGESTIONE;
  }
  public drosati_rcugas_massivo_sosia with_T_TELEGESTIONE(String T_TELEGESTIONE) {
    this.T_TELEGESTIONE = T_TELEGESTIONE;
    return this;
  }
  private String T_PRE_CONV;
  public String get_T_PRE_CONV() {
    return T_PRE_CONV;
  }
  public void set_T_PRE_CONV(String T_PRE_CONV) {
    this.T_PRE_CONV = T_PRE_CONV;
  }
  public drosati_rcugas_massivo_sosia with_T_PRE_CONV(String T_PRE_CONV) {
    this.T_PRE_CONV = T_PRE_CONV;
    return this;
  }
  private String T_MATRICOLA_CONVERTITORE;
  public String get_T_MATRICOLA_CONVERTITORE() {
    return T_MATRICOLA_CONVERTITORE;
  }
  public void set_T_MATRICOLA_CONVERTITORE(String T_MATRICOLA_CONVERTITORE) {
    this.T_MATRICOLA_CONVERTITORE = T_MATRICOLA_CONVERTITORE;
  }
  public drosati_rcugas_massivo_sosia with_T_MATRICOLA_CONVERTITORE(String T_MATRICOLA_CONVERTITORE) {
    this.T_MATRICOLA_CONVERTITORE = T_MATRICOLA_CONVERTITORE;
    return this;
  }
  private java.math.BigDecimal N_NUM_CIFRE_CONVERTITORE;
  public java.math.BigDecimal get_N_NUM_CIFRE_CONVERTITORE() {
    return N_NUM_CIFRE_CONVERTITORE;
  }
  public void set_N_NUM_CIFRE_CONVERTITORE(java.math.BigDecimal N_NUM_CIFRE_CONVERTITORE) {
    this.N_NUM_CIFRE_CONVERTITORE = N_NUM_CIFRE_CONVERTITORE;
  }
  public drosati_rcugas_massivo_sosia with_N_NUM_CIFRE_CONVERTITORE(java.math.BigDecimal N_NUM_CIFRE_CONVERTITORE) {
    this.N_NUM_CIFRE_CONVERTITORE = N_NUM_CIFRE_CONVERTITORE;
    return this;
  }
  private String T_ANNO_FABBRIC_CONVERTITORE;
  public String get_T_ANNO_FABBRIC_CONVERTITORE() {
    return T_ANNO_FABBRIC_CONVERTITORE;
  }
  public void set_T_ANNO_FABBRIC_CONVERTITORE(String T_ANNO_FABBRIC_CONVERTITORE) {
    this.T_ANNO_FABBRIC_CONVERTITORE = T_ANNO_FABBRIC_CONVERTITORE;
  }
  public drosati_rcugas_massivo_sosia with_T_ANNO_FABBRIC_CONVERTITORE(String T_ANNO_FABBRIC_CONVERTITORE) {
    this.T_ANNO_FABBRIC_CONVERTITORE = T_ANNO_FABBRIC_CONVERTITORE;
    return this;
  }
  private String T_DATA_INST_CONVERTITORE;
  public String get_T_DATA_INST_CONVERTITORE() {
    return T_DATA_INST_CONVERTITORE;
  }
  public void set_T_DATA_INST_CONVERTITORE(String T_DATA_INST_CONVERTITORE) {
    this.T_DATA_INST_CONVERTITORE = T_DATA_INST_CONVERTITORE;
  }
  public drosati_rcugas_massivo_sosia with_T_DATA_INST_CONVERTITORE(String T_DATA_INST_CONVERTITORE) {
    this.T_DATA_INST_CONVERTITORE = T_DATA_INST_CONVERTITORE;
    return this;
  }
  private java.math.BigDecimal N_COEFF_CORREZIONE;
  public java.math.BigDecimal get_N_COEFF_CORREZIONE() {
    return N_COEFF_CORREZIONE;
  }
  public void set_N_COEFF_CORREZIONE(java.math.BigDecimal N_COEFF_CORREZIONE) {
    this.N_COEFF_CORREZIONE = N_COEFF_CORREZIONE;
  }
  public drosati_rcugas_massivo_sosia with_N_COEFF_CORREZIONE(java.math.BigDecimal N_COEFF_CORREZIONE) {
    this.N_COEFF_CORREZIONE = N_COEFF_CORREZIONE;
    return this;
  }
  private java.math.BigDecimal PRESS_MISURE;
  public java.math.BigDecimal get_PRESS_MISURE() {
    return PRESS_MISURE;
  }
  public void set_PRESS_MISURE(java.math.BigDecimal PRESS_MISURE) {
    this.PRESS_MISURE = PRESS_MISURE;
  }
  public drosati_rcugas_massivo_sosia with_PRESS_MISURE(java.math.BigDecimal PRESS_MISURE) {
    this.PRESS_MISURE = PRESS_MISURE;
    return this;
  }
  private java.math.BigDecimal T_ACCESS_MISURATORE;
  public java.math.BigDecimal get_T_ACCESS_MISURATORE() {
    return T_ACCESS_MISURATORE;
  }
  public void set_T_ACCESS_MISURATORE(java.math.BigDecimal T_ACCESS_MISURATORE) {
    this.T_ACCESS_MISURATORE = T_ACCESS_MISURATORE;
  }
  public drosati_rcugas_massivo_sosia with_T_ACCESS_MISURATORE(java.math.BigDecimal T_ACCESS_MISURATORE) {
    this.T_ACCESS_MISURATORE = T_ACCESS_MISURATORE;
    return this;
  }
  private java.math.BigDecimal N_NUM_CIFRE_MISURATORE;
  public java.math.BigDecimal get_N_NUM_CIFRE_MISURATORE() {
    return N_NUM_CIFRE_MISURATORE;
  }
  public void set_N_NUM_CIFRE_MISURATORE(java.math.BigDecimal N_NUM_CIFRE_MISURATORE) {
    this.N_NUM_CIFRE_MISURATORE = N_NUM_CIFRE_MISURATORE;
  }
  public drosati_rcugas_massivo_sosia with_N_NUM_CIFRE_MISURATORE(java.math.BigDecimal N_NUM_CIFRE_MISURATORE) {
    this.N_NUM_CIFRE_MISURATORE = N_NUM_CIFRE_MISURATORE;
    return this;
  }
  private String T_ANNO_FABBRIC_MISURATORE;
  public String get_T_ANNO_FABBRIC_MISURATORE() {
    return T_ANNO_FABBRIC_MISURATORE;
  }
  public void set_T_ANNO_FABBRIC_MISURATORE(String T_ANNO_FABBRIC_MISURATORE) {
    this.T_ANNO_FABBRIC_MISURATORE = T_ANNO_FABBRIC_MISURATORE;
  }
  public drosati_rcugas_massivo_sosia with_T_ANNO_FABBRIC_MISURATORE(String T_ANNO_FABBRIC_MISURATORE) {
    this.T_ANNO_FABBRIC_MISURATORE = T_ANNO_FABBRIC_MISURATORE;
    return this;
  }
  private String T_DATA_INST_MISURATORE;
  public String get_T_DATA_INST_MISURATORE() {
    return T_DATA_INST_MISURATORE;
  }
  public void set_T_DATA_INST_MISURATORE(String T_DATA_INST_MISURATORE) {
    this.T_DATA_INST_MISURATORE = T_DATA_INST_MISURATORE;
  }
  public drosati_rcugas_massivo_sosia with_T_DATA_INST_MISURATORE(String T_DATA_INST_MISURATORE) {
    this.T_DATA_INST_MISURATORE = T_DATA_INST_MISURATORE;
    return this;
  }
  private String T_MISURATORE_INTEGRATO;
  public String get_T_MISURATORE_INTEGRATO() {
    return T_MISURATORE_INTEGRATO;
  }
  public void set_T_MISURATORE_INTEGRATO(String T_MISURATORE_INTEGRATO) {
    this.T_MISURATORE_INTEGRATO = T_MISURATORE_INTEGRATO;
  }
  public drosati_rcugas_massivo_sosia with_T_MISURATORE_INTEGRATO(String T_MISURATORE_INTEGRATO) {
    this.T_MISURATORE_INTEGRATO = T_MISURATORE_INTEGRATO;
    return this;
  }
  private java.math.BigDecimal N_POTENZIALITA_MASSIMA;
  public java.math.BigDecimal get_N_POTENZIALITA_MASSIMA() {
    return N_POTENZIALITA_MASSIMA;
  }
  public void set_N_POTENZIALITA_MASSIMA(java.math.BigDecimal N_POTENZIALITA_MASSIMA) {
    this.N_POTENZIALITA_MASSIMA = N_POTENZIALITA_MASSIMA;
  }
  public drosati_rcugas_massivo_sosia with_N_POTENZIALITA_MASSIMA(java.math.BigDecimal N_POTENZIALITA_MASSIMA) {
    this.N_POTENZIALITA_MASSIMA = N_POTENZIALITA_MASSIMA;
    return this;
  }
  private java.math.BigDecimal N_POTENZIALITA_TOT_INSTALLATA;
  public java.math.BigDecimal get_N_POTENZIALITA_TOT_INSTALLATA() {
    return N_POTENZIALITA_TOT_INSTALLATA;
  }
  public void set_N_POTENZIALITA_TOT_INSTALLATA(java.math.BigDecimal N_POTENZIALITA_TOT_INSTALLATA) {
    this.N_POTENZIALITA_TOT_INSTALLATA = N_POTENZIALITA_TOT_INSTALLATA;
  }
  public drosati_rcugas_massivo_sosia with_N_POTENZIALITA_TOT_INSTALLATA(java.math.BigDecimal N_POTENZIALITA_TOT_INSTALLATA) {
    this.N_POTENZIALITA_TOT_INSTALLATA = N_POTENZIALITA_TOT_INSTALLATA;
    return this;
  }
  private java.math.BigDecimal N_MAX_PRELIEVO_ORARIO;
  public java.math.BigDecimal get_N_MAX_PRELIEVO_ORARIO() {
    return N_MAX_PRELIEVO_ORARIO;
  }
  public void set_N_MAX_PRELIEVO_ORARIO(java.math.BigDecimal N_MAX_PRELIEVO_ORARIO) {
    this.N_MAX_PRELIEVO_ORARIO = N_MAX_PRELIEVO_ORARIO;
  }
  public drosati_rcugas_massivo_sosia with_N_MAX_PRELIEVO_ORARIO(java.math.BigDecimal N_MAX_PRELIEVO_ORARIO) {
    this.N_MAX_PRELIEVO_ORARIO = N_MAX_PRELIEVO_ORARIO;
    return this;
  }
  private String T_EROG_SERVIZIO_ENERG;
  public String get_T_EROG_SERVIZIO_ENERG() {
    return T_EROG_SERVIZIO_ENERG;
  }
  public void set_T_EROG_SERVIZIO_ENERG(String T_EROG_SERVIZIO_ENERG) {
    this.T_EROG_SERVIZIO_ENERG = T_EROG_SERVIZIO_ENERG;
  }
  public drosati_rcugas_massivo_sosia with_T_EROG_SERVIZIO_ENERG(String T_EROG_SERVIZIO_ENERG) {
    this.T_EROG_SERVIZIO_ENERG = T_EROG_SERVIZIO_ENERG;
    return this;
  }
  private String T_PARTITA_IVA_GESTCAL;
  public String get_T_PARTITA_IVA_GESTCAL() {
    return T_PARTITA_IVA_GESTCAL;
  }
  public void set_T_PARTITA_IVA_GESTCAL(String T_PARTITA_IVA_GESTCAL) {
    this.T_PARTITA_IVA_GESTCAL = T_PARTITA_IVA_GESTCAL;
  }
  public drosati_rcugas_massivo_sosia with_T_PARTITA_IVA_GESTCAL(String T_PARTITA_IVA_GESTCAL) {
    this.T_PARTITA_IVA_GESTCAL = T_PARTITA_IVA_GESTCAL;
    return this;
  }
  private String T_RAGIONE_SOCIALE_GESTCAL;
  public String get_T_RAGIONE_SOCIALE_GESTCAL() {
    return T_RAGIONE_SOCIALE_GESTCAL;
  }
  public void set_T_RAGIONE_SOCIALE_GESTCAL(String T_RAGIONE_SOCIALE_GESTCAL) {
    this.T_RAGIONE_SOCIALE_GESTCAL = T_RAGIONE_SOCIALE_GESTCAL;
  }
  public drosati_rcugas_massivo_sosia with_T_RAGIONE_SOCIALE_GESTCAL(String T_RAGIONE_SOCIALE_GESTCAL) {
    this.T_RAGIONE_SOCIALE_GESTCAL = T_RAGIONE_SOCIALE_GESTCAL;
    return this;
  }
  private String T_TELEFONO_GESTCAL;
  public String get_T_TELEFONO_GESTCAL() {
    return T_TELEFONO_GESTCAL;
  }
  public void set_T_TELEFONO_GESTCAL(String T_TELEFONO_GESTCAL) {
    this.T_TELEFONO_GESTCAL = T_TELEFONO_GESTCAL;
  }
  public drosati_rcugas_massivo_sosia with_T_TELEFONO_GESTCAL(String T_TELEFONO_GESTCAL) {
    this.T_TELEFONO_GESTCAL = T_TELEFONO_GESTCAL;
    return this;
  }
  private String T_EMAIL_GESTCAL;
  public String get_T_EMAIL_GESTCAL() {
    return T_EMAIL_GESTCAL;
  }
  public void set_T_EMAIL_GESTCAL(String T_EMAIL_GESTCAL) {
    this.T_EMAIL_GESTCAL = T_EMAIL_GESTCAL;
  }
  public drosati_rcugas_massivo_sosia with_T_EMAIL_GESTCAL(String T_EMAIL_GESTCAL) {
    this.T_EMAIL_GESTCAL = T_EMAIL_GESTCAL;
    return this;
  }
  private String T_TOPONIMO_GESTCAL;
  public String get_T_TOPONIMO_GESTCAL() {
    return T_TOPONIMO_GESTCAL;
  }
  public void set_T_TOPONIMO_GESTCAL(String T_TOPONIMO_GESTCAL) {
    this.T_TOPONIMO_GESTCAL = T_TOPONIMO_GESTCAL;
  }
  public drosati_rcugas_massivo_sosia with_T_TOPONIMO_GESTCAL(String T_TOPONIMO_GESTCAL) {
    this.T_TOPONIMO_GESTCAL = T_TOPONIMO_GESTCAL;
    return this;
  }
  private String T_NOMESTRADA_GESTCAL;
  public String get_T_NOMESTRADA_GESTCAL() {
    return T_NOMESTRADA_GESTCAL;
  }
  public void set_T_NOMESTRADA_GESTCAL(String T_NOMESTRADA_GESTCAL) {
    this.T_NOMESTRADA_GESTCAL = T_NOMESTRADA_GESTCAL;
  }
  public drosati_rcugas_massivo_sosia with_T_NOMESTRADA_GESTCAL(String T_NOMESTRADA_GESTCAL) {
    this.T_NOMESTRADA_GESTCAL = T_NOMESTRADA_GESTCAL;
    return this;
  }
  private String T_CIVICO_GESTCAL;
  public String get_T_CIVICO_GESTCAL() {
    return T_CIVICO_GESTCAL;
  }
  public void set_T_CIVICO_GESTCAL(String T_CIVICO_GESTCAL) {
    this.T_CIVICO_GESTCAL = T_CIVICO_GESTCAL;
  }
  public drosati_rcugas_massivo_sosia with_T_CIVICO_GESTCAL(String T_CIVICO_GESTCAL) {
    this.T_CIVICO_GESTCAL = T_CIVICO_GESTCAL;
    return this;
  }
  private String T_CAP_GESTCAL;
  public String get_T_CAP_GESTCAL() {
    return T_CAP_GESTCAL;
  }
  public void set_T_CAP_GESTCAL(String T_CAP_GESTCAL) {
    this.T_CAP_GESTCAL = T_CAP_GESTCAL;
  }
  public drosati_rcugas_massivo_sosia with_T_CAP_GESTCAL(String T_CAP_GESTCAL) {
    this.T_CAP_GESTCAL = T_CAP_GESTCAL;
    return this;
  }
  private String T_COMUNE_ISTAT_GESTCAL;
  public String get_T_COMUNE_ISTAT_GESTCAL() {
    return T_COMUNE_ISTAT_GESTCAL;
  }
  public void set_T_COMUNE_ISTAT_GESTCAL(String T_COMUNE_ISTAT_GESTCAL) {
    this.T_COMUNE_ISTAT_GESTCAL = T_COMUNE_ISTAT_GESTCAL;
  }
  public drosati_rcugas_massivo_sosia with_T_COMUNE_ISTAT_GESTCAL(String T_COMUNE_ISTAT_GESTCAL) {
    this.T_COMUNE_ISTAT_GESTCAL = T_COMUNE_ISTAT_GESTCAL;
    return this;
  }
  private String T_COMUNE_GESTCAL;
  public String get_T_COMUNE_GESTCAL() {
    return T_COMUNE_GESTCAL;
  }
  public void set_T_COMUNE_GESTCAL(String T_COMUNE_GESTCAL) {
    this.T_COMUNE_GESTCAL = T_COMUNE_GESTCAL;
  }
  public drosati_rcugas_massivo_sosia with_T_COMUNE_GESTCAL(String T_COMUNE_GESTCAL) {
    this.T_COMUNE_GESTCAL = T_COMUNE_GESTCAL;
    return this;
  }
  private String T_PROVINCIA_GESTCAL;
  public String get_T_PROVINCIA_GESTCAL() {
    return T_PROVINCIA_GESTCAL;
  }
  public void set_T_PROVINCIA_GESTCAL(String T_PROVINCIA_GESTCAL) {
    this.T_PROVINCIA_GESTCAL = T_PROVINCIA_GESTCAL;
  }
  public drosati_rcugas_massivo_sosia with_T_PROVINCIA_GESTCAL(String T_PROVINCIA_GESTCAL) {
    this.T_PROVINCIA_GESTCAL = T_PROVINCIA_GESTCAL;
    return this;
  }
  private String T_NAZIONE_GESTCAL;
  public String get_T_NAZIONE_GESTCAL() {
    return T_NAZIONE_GESTCAL;
  }
  public void set_T_NAZIONE_GESTCAL(String T_NAZIONE_GESTCAL) {
    this.T_NAZIONE_GESTCAL = T_NAZIONE_GESTCAL;
  }
  public drosati_rcugas_massivo_sosia with_T_NAZIONE_GESTCAL(String T_NAZIONE_GESTCAL) {
    this.T_NAZIONE_GESTCAL = T_NAZIONE_GESTCAL;
    return this;
  }
  private String D_DATA_RIF_PDR;
  public String get_D_DATA_RIF_PDR() {
    return D_DATA_RIF_PDR;
  }
  public void set_D_DATA_RIF_PDR(String D_DATA_RIF_PDR) {
    this.D_DATA_RIF_PDR = D_DATA_RIF_PDR;
  }
  public drosati_rcugas_massivo_sosia with_D_DATA_RIF_PDR(String D_DATA_RIF_PDR) {
    this.D_DATA_RIF_PDR = D_DATA_RIF_PDR;
    return this;
  }
  private String D_AGGIORNAMENTO_PDR;
  public String get_D_AGGIORNAMENTO_PDR() {
    return D_AGGIORNAMENTO_PDR;
  }
  public void set_D_AGGIORNAMENTO_PDR(String D_AGGIORNAMENTO_PDR) {
    this.D_AGGIORNAMENTO_PDR = D_AGGIORNAMENTO_PDR;
  }
  public drosati_rcugas_massivo_sosia with_D_AGGIORNAMENTO_PDR(String D_AGGIORNAMENTO_PDR) {
    this.D_AGGIORNAMENTO_PDR = D_AGGIORNAMENTO_PDR;
    return this;
  }
  private String D_DATA_RIF_TECN;
  public String get_D_DATA_RIF_TECN() {
    return D_DATA_RIF_TECN;
  }
  public void set_D_DATA_RIF_TECN(String D_DATA_RIF_TECN) {
    this.D_DATA_RIF_TECN = D_DATA_RIF_TECN;
  }
  public drosati_rcugas_massivo_sosia with_D_DATA_RIF_TECN(String D_DATA_RIF_TECN) {
    this.D_DATA_RIF_TECN = D_DATA_RIF_TECN;
    return this;
  }
  private String D_AGGIORNAMENTO_TECN;
  public String get_D_AGGIORNAMENTO_TECN() {
    return D_AGGIORNAMENTO_TECN;
  }
  public void set_D_AGGIORNAMENTO_TECN(String D_AGGIORNAMENTO_TECN) {
    this.D_AGGIORNAMENTO_TECN = D_AGGIORNAMENTO_TECN;
  }
  public drosati_rcugas_massivo_sosia with_D_AGGIORNAMENTO_TECN(String D_AGGIORNAMENTO_TECN) {
    this.D_AGGIORNAMENTO_TECN = D_AGGIORNAMENTO_TECN;
    return this;
  }
  private String D_DATA_RIF_MIS;
  public String get_D_DATA_RIF_MIS() {
    return D_DATA_RIF_MIS;
  }
  public void set_D_DATA_RIF_MIS(String D_DATA_RIF_MIS) {
    this.D_DATA_RIF_MIS = D_DATA_RIF_MIS;
  }
  public drosati_rcugas_massivo_sosia with_D_DATA_RIF_MIS(String D_DATA_RIF_MIS) {
    this.D_DATA_RIF_MIS = D_DATA_RIF_MIS;
    return this;
  }
  private String D_AGGIORNAMENTO_MIS;
  public String get_D_AGGIORNAMENTO_MIS() {
    return D_AGGIORNAMENTO_MIS;
  }
  public void set_D_AGGIORNAMENTO_MIS(String D_AGGIORNAMENTO_MIS) {
    this.D_AGGIORNAMENTO_MIS = D_AGGIORNAMENTO_MIS;
  }
  public drosati_rcugas_massivo_sosia with_D_AGGIORNAMENTO_MIS(String D_AGGIORNAMENTO_MIS) {
    this.D_AGGIORNAMENTO_MIS = D_AGGIORNAMENTO_MIS;
    return this;
  }
  private String D_DATA_RIF_FORN;
  public String get_D_DATA_RIF_FORN() {
    return D_DATA_RIF_FORN;
  }
  public void set_D_DATA_RIF_FORN(String D_DATA_RIF_FORN) {
    this.D_DATA_RIF_FORN = D_DATA_RIF_FORN;
  }
  public drosati_rcugas_massivo_sosia with_D_DATA_RIF_FORN(String D_DATA_RIF_FORN) {
    this.D_DATA_RIF_FORN = D_DATA_RIF_FORN;
    return this;
  }
  private String D_AGGIORNAMENTO_FORN;
  public String get_D_AGGIORNAMENTO_FORN() {
    return D_AGGIORNAMENTO_FORN;
  }
  public void set_D_AGGIORNAMENTO_FORN(String D_AGGIORNAMENTO_FORN) {
    this.D_AGGIORNAMENTO_FORN = D_AGGIORNAMENTO_FORN;
  }
  public drosati_rcugas_massivo_sosia with_D_AGGIORNAMENTO_FORN(String D_AGGIORNAMENTO_FORN) {
    this.D_AGGIORNAMENTO_FORN = D_AGGIORNAMENTO_FORN;
    return this;
  }
  private String T_TIPO_BONUS;
  public String get_T_TIPO_BONUS() {
    return T_TIPO_BONUS;
  }
  public void set_T_TIPO_BONUS(String T_TIPO_BONUS) {
    this.T_TIPO_BONUS = T_TIPO_BONUS;
  }
  public drosati_rcugas_massivo_sosia with_T_TIPO_BONUS(String T_TIPO_BONUS) {
    this.T_TIPO_BONUS = T_TIPO_BONUS;
    return this;
  }
  private String D_DATA_INIZIO_EROG_BONUS;
  public String get_D_DATA_INIZIO_EROG_BONUS() {
    return D_DATA_INIZIO_EROG_BONUS;
  }
  public void set_D_DATA_INIZIO_EROG_BONUS(String D_DATA_INIZIO_EROG_BONUS) {
    this.D_DATA_INIZIO_EROG_BONUS = D_DATA_INIZIO_EROG_BONUS;
  }
  public drosati_rcugas_massivo_sosia with_D_DATA_INIZIO_EROG_BONUS(String D_DATA_INIZIO_EROG_BONUS) {
    this.D_DATA_INIZIO_EROG_BONUS = D_DATA_INIZIO_EROG_BONUS;
    return this;
  }
  private String D_DATA_FINE_EROG_BONUS;
  public String get_D_DATA_FINE_EROG_BONUS() {
    return D_DATA_FINE_EROG_BONUS;
  }
  public void set_D_DATA_FINE_EROG_BONUS(String D_DATA_FINE_EROG_BONUS) {
    this.D_DATA_FINE_EROG_BONUS = D_DATA_FINE_EROG_BONUS;
  }
  public drosati_rcugas_massivo_sosia with_D_DATA_FINE_EROG_BONUS(String D_DATA_FINE_EROG_BONUS) {
    this.D_DATA_FINE_EROG_BONUS = D_DATA_FINE_EROG_BONUS;
    return this;
  }
  private String D_DATA_RIF_BONUS;
  public String get_D_DATA_RIF_BONUS() {
    return D_DATA_RIF_BONUS;
  }
  public void set_D_DATA_RIF_BONUS(String D_DATA_RIF_BONUS) {
    this.D_DATA_RIF_BONUS = D_DATA_RIF_BONUS;
  }
  public drosati_rcugas_massivo_sosia with_D_DATA_RIF_BONUS(String D_DATA_RIF_BONUS) {
    this.D_DATA_RIF_BONUS = D_DATA_RIF_BONUS;
    return this;
  }
  private String D_AGGIORNAMENTO_BONUS;
  public String get_D_AGGIORNAMENTO_BONUS() {
    return D_AGGIORNAMENTO_BONUS;
  }
  public void set_D_AGGIORNAMENTO_BONUS(String D_AGGIORNAMENTO_BONUS) {
    this.D_AGGIORNAMENTO_BONUS = D_AGGIORNAMENTO_BONUS;
  }
  public drosati_rcugas_massivo_sosia with_D_AGGIORNAMENTO_BONUS(String D_AGGIORNAMENTO_BONUS) {
    this.D_AGGIORNAMENTO_BONUS = D_AGGIORNAMENTO_BONUS;
    return this;
  }
  private String D_DATA_AGGIORNAMENTO;
  public String get_D_DATA_AGGIORNAMENTO() {
    return D_DATA_AGGIORNAMENTO;
  }
  public void set_D_DATA_AGGIORNAMENTO(String D_DATA_AGGIORNAMENTO) {
    this.D_DATA_AGGIORNAMENTO = D_DATA_AGGIORNAMENTO;
  }
  public drosati_rcugas_massivo_sosia with_D_DATA_AGGIORNAMENTO(String D_DATA_AGGIORNAMENTO) {
    this.D_DATA_AGGIORNAMENTO = D_DATA_AGGIORNAMENTO;
    return this;
  }
  private java.math.BigDecimal N_ID_UDD;
  public java.math.BigDecimal get_N_ID_UDD() {
    return N_ID_UDD;
  }
  public void set_N_ID_UDD(java.math.BigDecimal N_ID_UDD) {
    this.N_ID_UDD = N_ID_UDD;
  }
  public drosati_rcugas_massivo_sosia with_N_ID_UDD(java.math.BigDecimal N_ID_UDD) {
    this.N_ID_UDD = N_ID_UDD;
    return this;
  }
  private java.math.BigDecimal N_ID_VENDITORE;
  public java.math.BigDecimal get_N_ID_VENDITORE() {
    return N_ID_VENDITORE;
  }
  public void set_N_ID_VENDITORE(java.math.BigDecimal N_ID_VENDITORE) {
    this.N_ID_VENDITORE = N_ID_VENDITORE;
  }
  public drosati_rcugas_massivo_sosia with_N_ID_VENDITORE(java.math.BigDecimal N_ID_VENDITORE) {
    this.N_ID_VENDITORE = N_ID_VENDITORE;
    return this;
  }
  private String T_COD_PROFILO;
  public String get_T_COD_PROFILO() {
    return T_COD_PROFILO;
  }
  public void set_T_COD_PROFILO(String T_COD_PROFILO) {
    this.T_COD_PROFILO = T_COD_PROFILO;
  }
  public drosati_rcugas_massivo_sosia with_T_COD_PROFILO(String T_COD_PROFILO) {
    this.T_COD_PROFILO = T_COD_PROFILO;
    return this;
  }
  private String T_COD_CAT_USO;
  public String get_T_COD_CAT_USO() {
    return T_COD_CAT_USO;
  }
  public void set_T_COD_CAT_USO(String T_COD_CAT_USO) {
    this.T_COD_CAT_USO = T_COD_CAT_USO;
  }
  public drosati_rcugas_massivo_sosia with_T_COD_CAT_USO(String T_COD_CAT_USO) {
    this.T_COD_CAT_USO = T_COD_CAT_USO;
    return this;
  }
  private String T_COD_CLASSE_PRELIEVO;
  public String get_T_COD_CLASSE_PRELIEVO() {
    return T_COD_CLASSE_PRELIEVO;
  }
  public void set_T_COD_CLASSE_PRELIEVO(String T_COD_CLASSE_PRELIEVO) {
    this.T_COD_CLASSE_PRELIEVO = T_COD_CLASSE_PRELIEVO;
  }
  public drosati_rcugas_massivo_sosia with_T_COD_CLASSE_PRELIEVO(String T_COD_CLASSE_PRELIEVO) {
    this.T_COD_CLASSE_PRELIEVO = T_COD_CLASSE_PRELIEVO;
    return this;
  }
  private String T_ANNO_TERMICO;
  public String get_T_ANNO_TERMICO() {
    return T_ANNO_TERMICO;
  }
  public void set_T_ANNO_TERMICO(String T_ANNO_TERMICO) {
    this.T_ANNO_TERMICO = T_ANNO_TERMICO;
  }
  public drosati_rcugas_massivo_sosia with_T_ANNO_TERMICO(String T_ANNO_TERMICO) {
    this.T_ANNO_TERMICO = T_ANNO_TERMICO;
    return this;
  }
  private String D_DATA_RIF_PREL;
  public String get_D_DATA_RIF_PREL() {
    return D_DATA_RIF_PREL;
  }
  public void set_D_DATA_RIF_PREL(String D_DATA_RIF_PREL) {
    this.D_DATA_RIF_PREL = D_DATA_RIF_PREL;
  }
  public drosati_rcugas_massivo_sosia with_D_DATA_RIF_PREL(String D_DATA_RIF_PREL) {
    this.D_DATA_RIF_PREL = D_DATA_RIF_PREL;
    return this;
  }
  private String T_TRATTAMENTO;
  public String get_T_TRATTAMENTO() {
    return T_TRATTAMENTO;
  }
  public void set_T_TRATTAMENTO(String T_TRATTAMENTO) {
    this.T_TRATTAMENTO = T_TRATTAMENTO;
  }
  public drosati_rcugas_massivo_sosia with_T_TRATTAMENTO(String T_TRATTAMENTO) {
    this.T_TRATTAMENTO = T_TRATTAMENTO;
    return this;
  }
  private String T_TOPONIMO_ESAZ;
  public String get_T_TOPONIMO_ESAZ() {
    return T_TOPONIMO_ESAZ;
  }
  public void set_T_TOPONIMO_ESAZ(String T_TOPONIMO_ESAZ) {
    this.T_TOPONIMO_ESAZ = T_TOPONIMO_ESAZ;
  }
  public drosati_rcugas_massivo_sosia with_T_TOPONIMO_ESAZ(String T_TOPONIMO_ESAZ) {
    this.T_TOPONIMO_ESAZ = T_TOPONIMO_ESAZ;
    return this;
  }
  private String T_NOMESTRADA_ESAZ;
  public String get_T_NOMESTRADA_ESAZ() {
    return T_NOMESTRADA_ESAZ;
  }
  public void set_T_NOMESTRADA_ESAZ(String T_NOMESTRADA_ESAZ) {
    this.T_NOMESTRADA_ESAZ = T_NOMESTRADA_ESAZ;
  }
  public drosati_rcugas_massivo_sosia with_T_NOMESTRADA_ESAZ(String T_NOMESTRADA_ESAZ) {
    this.T_NOMESTRADA_ESAZ = T_NOMESTRADA_ESAZ;
    return this;
  }
  private String T_CIVICO_ESAZ;
  public String get_T_CIVICO_ESAZ() {
    return T_CIVICO_ESAZ;
  }
  public void set_T_CIVICO_ESAZ(String T_CIVICO_ESAZ) {
    this.T_CIVICO_ESAZ = T_CIVICO_ESAZ;
  }
  public drosati_rcugas_massivo_sosia with_T_CIVICO_ESAZ(String T_CIVICO_ESAZ) {
    this.T_CIVICO_ESAZ = T_CIVICO_ESAZ;
    return this;
  }
  private String T_CAP_ESAZ;
  public String get_T_CAP_ESAZ() {
    return T_CAP_ESAZ;
  }
  public void set_T_CAP_ESAZ(String T_CAP_ESAZ) {
    this.T_CAP_ESAZ = T_CAP_ESAZ;
  }
  public drosati_rcugas_massivo_sosia with_T_CAP_ESAZ(String T_CAP_ESAZ) {
    this.T_CAP_ESAZ = T_CAP_ESAZ;
    return this;
  }
  private String T_COMUNE_ISTAT_ESAZ;
  public String get_T_COMUNE_ISTAT_ESAZ() {
    return T_COMUNE_ISTAT_ESAZ;
  }
  public void set_T_COMUNE_ISTAT_ESAZ(String T_COMUNE_ISTAT_ESAZ) {
    this.T_COMUNE_ISTAT_ESAZ = T_COMUNE_ISTAT_ESAZ;
  }
  public drosati_rcugas_massivo_sosia with_T_COMUNE_ISTAT_ESAZ(String T_COMUNE_ISTAT_ESAZ) {
    this.T_COMUNE_ISTAT_ESAZ = T_COMUNE_ISTAT_ESAZ;
    return this;
  }
  private String T_COMUNE_ESAZ;
  public String get_T_COMUNE_ESAZ() {
    return T_COMUNE_ESAZ;
  }
  public void set_T_COMUNE_ESAZ(String T_COMUNE_ESAZ) {
    this.T_COMUNE_ESAZ = T_COMUNE_ESAZ;
  }
  public drosati_rcugas_massivo_sosia with_T_COMUNE_ESAZ(String T_COMUNE_ESAZ) {
    this.T_COMUNE_ESAZ = T_COMUNE_ESAZ;
    return this;
  }
  private String T_PROVINCIA_ESAZ;
  public String get_T_PROVINCIA_ESAZ() {
    return T_PROVINCIA_ESAZ;
  }
  public void set_T_PROVINCIA_ESAZ(String T_PROVINCIA_ESAZ) {
    this.T_PROVINCIA_ESAZ = T_PROVINCIA_ESAZ;
  }
  public drosati_rcugas_massivo_sosia with_T_PROVINCIA_ESAZ(String T_PROVINCIA_ESAZ) {
    this.T_PROVINCIA_ESAZ = T_PROVINCIA_ESAZ;
    return this;
  }
  private String T_NAZIONE_ESAZ;
  public String get_T_NAZIONE_ESAZ() {
    return T_NAZIONE_ESAZ;
  }
  public void set_T_NAZIONE_ESAZ(String T_NAZIONE_ESAZ) {
    this.T_NAZIONE_ESAZ = T_NAZIONE_ESAZ;
  }
  public drosati_rcugas_massivo_sosia with_T_NAZIONE_ESAZ(String T_NAZIONE_ESAZ) {
    this.T_NAZIONE_ESAZ = T_NAZIONE_ESAZ;
    return this;
  }
  private String ALTRO_IND_ESAZ;
  public String get_ALTRO_IND_ESAZ() {
    return ALTRO_IND_ESAZ;
  }
  public void set_ALTRO_IND_ESAZ(String ALTRO_IND_ESAZ) {
    this.ALTRO_IND_ESAZ = ALTRO_IND_ESAZ;
  }
  public drosati_rcugas_massivo_sosia with_ALTRO_IND_ESAZ(String ALTRO_IND_ESAZ) {
    this.ALTRO_IND_ESAZ = ALTRO_IND_ESAZ;
    return this;
  }
  private String T_CODICE_ATECO;
  public String get_T_CODICE_ATECO() {
    return T_CODICE_ATECO;
  }
  public void set_T_CODICE_ATECO(String T_CODICE_ATECO) {
    this.T_CODICE_ATECO = T_CODICE_ATECO;
  }
  public drosati_rcugas_massivo_sosia with_T_CODICE_ATECO(String T_CODICE_ATECO) {
    this.T_CODICE_ATECO = T_CODICE_ATECO;
    return this;
  }
  private String T_PAGAMENTO_IVA;
  public String get_T_PAGAMENTO_IVA() {
    return T_PAGAMENTO_IVA;
  }
  public void set_T_PAGAMENTO_IVA(String T_PAGAMENTO_IVA) {
    this.T_PAGAMENTO_IVA = T_PAGAMENTO_IVA;
  }
  public drosati_rcugas_massivo_sosia with_T_PAGAMENTO_IVA(String T_PAGAMENTO_IVA) {
    this.T_PAGAMENTO_IVA = T_PAGAMENTO_IVA;
    return this;
  }
  private String T_CODICE_UFFICIO;
  public String get_T_CODICE_UFFICIO() {
    return T_CODICE_UFFICIO;
  }
  public void set_T_CODICE_UFFICIO(String T_CODICE_UFFICIO) {
    this.T_CODICE_UFFICIO = T_CODICE_UFFICIO;
  }
  public drosati_rcugas_massivo_sosia with_T_CODICE_UFFICIO(String T_CODICE_UFFICIO) {
    this.T_CODICE_UFFICIO = T_CODICE_UFFICIO;
    return this;
  }
  private String T_CF_INTESTATARIO_FATT;
  public String get_T_CF_INTESTATARIO_FATT() {
    return T_CF_INTESTATARIO_FATT;
  }
  public void set_T_CF_INTESTATARIO_FATT(String T_CF_INTESTATARIO_FATT) {
    this.T_CF_INTESTATARIO_FATT = T_CF_INTESTATARIO_FATT;
  }
  public drosati_rcugas_massivo_sosia with_T_CF_INTESTATARIO_FATT(String T_CF_INTESTATARIO_FATT) {
    this.T_CF_INTESTATARIO_FATT = T_CF_INTESTATARIO_FATT;
    return this;
  }
  private String T_CF_STRANIERO_FATT;
  public String get_T_CF_STRANIERO_FATT() {
    return T_CF_STRANIERO_FATT;
  }
  public void set_T_CF_STRANIERO_FATT(String T_CF_STRANIERO_FATT) {
    this.T_CF_STRANIERO_FATT = T_CF_STRANIERO_FATT;
  }
  public drosati_rcugas_massivo_sosia with_T_CF_STRANIERO_FATT(String T_CF_STRANIERO_FATT) {
    this.T_CF_STRANIERO_FATT = T_CF_STRANIERO_FATT;
    return this;
  }
  private String T_PIVA_INTESTATARIO_FATT;
  public String get_T_PIVA_INTESTATARIO_FATT() {
    return T_PIVA_INTESTATARIO_FATT;
  }
  public void set_T_PIVA_INTESTATARIO_FATT(String T_PIVA_INTESTATARIO_FATT) {
    this.T_PIVA_INTESTATARIO_FATT = T_PIVA_INTESTATARIO_FATT;
  }
  public drosati_rcugas_massivo_sosia with_T_PIVA_INTESTATARIO_FATT(String T_PIVA_INTESTATARIO_FATT) {
    this.T_PIVA_INTESTATARIO_FATT = T_PIVA_INTESTATARIO_FATT;
    return this;
  }
  private String T_NOME_INTESTATARIO_FATT;
  public String get_T_NOME_INTESTATARIO_FATT() {
    return T_NOME_INTESTATARIO_FATT;
  }
  public void set_T_NOME_INTESTATARIO_FATT(String T_NOME_INTESTATARIO_FATT) {
    this.T_NOME_INTESTATARIO_FATT = T_NOME_INTESTATARIO_FATT;
  }
  public drosati_rcugas_massivo_sosia with_T_NOME_INTESTATARIO_FATT(String T_NOME_INTESTATARIO_FATT) {
    this.T_NOME_INTESTATARIO_FATT = T_NOME_INTESTATARIO_FATT;
    return this;
  }
  private String T_COGNOME_INTESTATARIO_FATT;
  public String get_T_COGNOME_INTESTATARIO_FATT() {
    return T_COGNOME_INTESTATARIO_FATT;
  }
  public void set_T_COGNOME_INTESTATARIO_FATT(String T_COGNOME_INTESTATARIO_FATT) {
    this.T_COGNOME_INTESTATARIO_FATT = T_COGNOME_INTESTATARIO_FATT;
  }
  public drosati_rcugas_massivo_sosia with_T_COGNOME_INTESTATARIO_FATT(String T_COGNOME_INTESTATARIO_FATT) {
    this.T_COGNOME_INTESTATARIO_FATT = T_COGNOME_INTESTATARIO_FATT;
    return this;
  }
  private String T_RAG_SOC_INTESTATARIO_FATT;
  public String get_T_RAG_SOC_INTESTATARIO_FATT() {
    return T_RAG_SOC_INTESTATARIO_FATT;
  }
  public void set_T_RAG_SOC_INTESTATARIO_FATT(String T_RAG_SOC_INTESTATARIO_FATT) {
    this.T_RAG_SOC_INTESTATARIO_FATT = T_RAG_SOC_INTESTATARIO_FATT;
  }
  public drosati_rcugas_massivo_sosia with_T_RAG_SOC_INTESTATARIO_FATT(String T_RAG_SOC_INTESTATARIO_FATT) {
    this.T_RAG_SOC_INTESTATARIO_FATT = T_RAG_SOC_INTESTATARIO_FATT;
    return this;
  }
  private String T_ANNO_MESE_RINN_BONUS;
  public String get_T_ANNO_MESE_RINN_BONUS() {
    return T_ANNO_MESE_RINN_BONUS;
  }
  public void set_T_ANNO_MESE_RINN_BONUS(String T_ANNO_MESE_RINN_BONUS) {
    this.T_ANNO_MESE_RINN_BONUS = T_ANNO_MESE_RINN_BONUS;
  }
  public drosati_rcugas_massivo_sosia with_T_ANNO_MESE_RINN_BONUS(String T_ANNO_MESE_RINN_BONUS) {
    this.T_ANNO_MESE_RINN_BONUS = T_ANNO_MESE_RINN_BONUS;
    return this;
  }
  private String D_DATA_INIZIO_BONUS;
  public String get_D_DATA_INIZIO_BONUS() {
    return D_DATA_INIZIO_BONUS;
  }
  public void set_D_DATA_INIZIO_BONUS(String D_DATA_INIZIO_BONUS) {
    this.D_DATA_INIZIO_BONUS = D_DATA_INIZIO_BONUS;
  }
  public drosati_rcugas_massivo_sosia with_D_DATA_INIZIO_BONUS(String D_DATA_INIZIO_BONUS) {
    this.D_DATA_INIZIO_BONUS = D_DATA_INIZIO_BONUS;
    return this;
  }
  private String D_DATA_FINE_BONUS;
  public String get_D_DATA_FINE_BONUS() {
    return D_DATA_FINE_BONUS;
  }
  public void set_D_DATA_FINE_BONUS(String D_DATA_FINE_BONUS) {
    this.D_DATA_FINE_BONUS = D_DATA_FINE_BONUS;
  }
  public drosati_rcugas_massivo_sosia with_D_DATA_FINE_BONUS(String D_DATA_FINE_BONUS) {
    this.D_DATA_FINE_BONUS = D_DATA_FINE_BONUS;
    return this;
  }
  private java.math.BigDecimal N_PRELIEVO_ANNUO;
  public java.math.BigDecimal get_N_PRELIEVO_ANNUO() {
    return N_PRELIEVO_ANNUO;
  }
  public void set_N_PRELIEVO_ANNUO(java.math.BigDecimal N_PRELIEVO_ANNUO) {
    this.N_PRELIEVO_ANNUO = N_PRELIEVO_ANNUO;
  }
  public drosati_rcugas_massivo_sosia with_N_PRELIEVO_ANNUO(java.math.BigDecimal N_PRELIEVO_ANNUO) {
    this.N_PRELIEVO_ANNUO = N_PRELIEVO_ANNUO;
    return this;
  }
  private String T_FATTORE_CORREZ_CLIMATICA;
  public String get_T_FATTORE_CORREZ_CLIMATICA() {
    return T_FATTORE_CORREZ_CLIMATICA;
  }
  public void set_T_FATTORE_CORREZ_CLIMATICA(String T_FATTORE_CORREZ_CLIMATICA) {
    this.T_FATTORE_CORREZ_CLIMATICA = T_FATTORE_CORREZ_CLIMATICA;
  }
  public drosati_rcugas_massivo_sosia with_T_FATTORE_CORREZ_CLIMATICA(String T_FATTORE_CORREZ_CLIMATICA) {
    this.T_FATTORE_CORREZ_CLIMATICA = T_FATTORE_CORREZ_CLIMATICA;
    return this;
  }
  private String T_ALTRO_IND_GESTCAL;
  public String get_T_ALTRO_IND_GESTCAL() {
    return T_ALTRO_IND_GESTCAL;
  }
  public void set_T_ALTRO_IND_GESTCAL(String T_ALTRO_IND_GESTCAL) {
    this.T_ALTRO_IND_GESTCAL = T_ALTRO_IND_GESTCAL;
  }
  public drosati_rcugas_massivo_sosia with_T_ALTRO_IND_GESTCAL(String T_ALTRO_IND_GESTCAL) {
    this.T_ALTRO_IND_GESTCAL = T_ALTRO_IND_GESTCAL;
    return this;
  }
  private String T_TIPO_OP;
  public String get_T_TIPO_OP() {
    return T_TIPO_OP;
  }
  public void set_T_TIPO_OP(String T_TIPO_OP) {
    this.T_TIPO_OP = T_TIPO_OP;
  }
  public drosati_rcugas_massivo_sosia with_T_TIPO_OP(String T_TIPO_OP) {
    this.T_TIPO_OP = T_TIPO_OP;
    return this;
  }
  private String T_PROCESSO;
  public String get_T_PROCESSO() {
    return T_PROCESSO;
  }
  public void set_T_PROCESSO(String T_PROCESSO) {
    this.T_PROCESSO = T_PROCESSO;
  }
  public drosati_rcugas_massivo_sosia with_T_PROCESSO(String T_PROCESSO) {
    this.T_PROCESSO = T_PROCESSO;
    return this;
  }
  private java.math.BigDecimal N_ID_PRATICA_PROCESSO;
  public java.math.BigDecimal get_N_ID_PRATICA_PROCESSO() {
    return N_ID_PRATICA_PROCESSO;
  }
  public void set_N_ID_PRATICA_PROCESSO(java.math.BigDecimal N_ID_PRATICA_PROCESSO) {
    this.N_ID_PRATICA_PROCESSO = N_ID_PRATICA_PROCESSO;
  }
  public drosati_rcugas_massivo_sosia with_N_ID_PRATICA_PROCESSO(java.math.BigDecimal N_ID_PRATICA_PROCESSO) {
    this.N_ID_PRATICA_PROCESSO = N_ID_PRATICA_PROCESSO;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof drosati_rcugas_massivo_sosia)) {
      return false;
    }
    drosati_rcugas_massivo_sosia that = (drosati_rcugas_massivo_sosia) o;
    boolean equal = true;
    equal = equal && (this.N_ID_PDR == null ? that.N_ID_PDR == null : this.N_ID_PDR.equals(that.N_ID_PDR));
    equal = equal && (this.T_CODICE_PDR == null ? that.T_CODICE_PDR == null : this.T_CODICE_PDR.equals(that.T_CODICE_PDR));
    equal = equal && (this.CAPACITA_TRASPORTO == null ? that.CAPACITA_TRASPORTO == null : this.CAPACITA_TRASPORTO.equals(that.CAPACITA_TRASPORTO));
    equal = equal && (this.MESE_VAL_CAP_TRASP == null ? that.MESE_VAL_CAP_TRASP == null : this.MESE_VAL_CAP_TRASP.equals(that.MESE_VAL_CAP_TRASP));
    equal = equal && (this.T_COD_TIPO_PDR == null ? that.T_COD_TIPO_PDR == null : this.T_COD_TIPO_PDR.equals(that.T_COD_TIPO_PDR));
    equal = equal && (this.T_DISALIMENTABILITA == null ? that.T_DISALIMENTABILITA == null : this.T_DISALIMENTABILITA.equals(that.T_DISALIMENTABILITA));
    equal = equal && (this.BILANCIAMENTO == null ? that.BILANCIAMENTO == null : this.BILANCIAMENTO.equals(that.BILANCIAMENTO));
    equal = equal && (this.N_ID_FORNITURA == null ? that.N_ID_FORNITURA == null : this.N_ID_FORNITURA.equals(that.N_ID_FORNITURA));
    equal = equal && (this.D_DATA_INIZIO_FOR == null ? that.D_DATA_INIZIO_FOR == null : this.D_DATA_INIZIO_FOR.equals(that.D_DATA_INIZIO_FOR));
    equal = equal && (this.DATA_FINE_FOR == null ? that.DATA_FINE_FOR == null : this.DATA_FINE_FOR.equals(that.DATA_FINE_FOR));
    equal = equal && (this.N_ID_AZ_UDD == null ? that.N_ID_AZ_UDD == null : this.N_ID_AZ_UDD.equals(that.N_ID_AZ_UDD));
    equal = equal && (this.PIVA_UDD == null ? that.PIVA_UDD == null : this.PIVA_UDD.equals(that.PIVA_UDD));
    equal = equal && (this.N_ID_AZ_CC == null ? that.N_ID_AZ_CC == null : this.N_ID_AZ_CC.equals(that.N_ID_AZ_CC));
    equal = equal && (this.PIVA_CC == null ? that.PIVA_CC == null : this.PIVA_CC.equals(that.PIVA_CC));
    equal = equal && (this.N_ID_CLIENTE == null ? that.N_ID_CLIENTE == null : this.N_ID_CLIENTE.equals(that.N_ID_CLIENTE));
    equal = equal && (this.T_PARTITA_IVA_CLI == null ? that.T_PARTITA_IVA_CLI == null : this.T_PARTITA_IVA_CLI.equals(that.T_PARTITA_IVA_CLI));
    equal = equal && (this.T_CODICE_FISCALE_CLI == null ? that.T_CODICE_FISCALE_CLI == null : this.T_CODICE_FISCALE_CLI.equals(that.T_CODICE_FISCALE_CLI));
    equal = equal && (this.B_CF_STRANIERO == null ? that.B_CF_STRANIERO == null : this.B_CF_STRANIERO.equals(that.B_CF_STRANIERO));
    equal = equal && (this.T_REFERENTE == null ? that.T_REFERENTE == null : this.T_REFERENTE.equals(that.T_REFERENTE));
    equal = equal && (this.T_NOME_REF == null ? that.T_NOME_REF == null : this.T_NOME_REF.equals(that.T_NOME_REF));
    equal = equal && (this.T_COGNOME_REF == null ? that.T_COGNOME_REF == null : this.T_COGNOME_REF.equals(that.T_COGNOME_REF));
    equal = equal && (this.T_EMAIL_REF == null ? that.T_EMAIL_REF == null : this.T_EMAIL_REF.equals(that.T_EMAIL_REF));
    equal = equal && (this.T_TELEFONO_REF == null ? that.T_TELEFONO_REF == null : this.T_TELEFONO_REF.equals(that.T_TELEFONO_REF));
    equal = equal && (this.T_RESIDENZA == null ? that.T_RESIDENZA == null : this.T_RESIDENZA.equals(that.T_RESIDENZA));
    equal = equal && (this.DATA_VAL_RES == null ? that.DATA_VAL_RES == null : this.DATA_VAL_RES.equals(that.DATA_VAL_RES));
    equal = equal && (this.T_TOPONIMOPDR == null ? that.T_TOPONIMOPDR == null : this.T_TOPONIMOPDR.equals(that.T_TOPONIMOPDR));
    equal = equal && (this.T_NOMESTRADA_PDR == null ? that.T_NOMESTRADA_PDR == null : this.T_NOMESTRADA_PDR.equals(that.T_NOMESTRADA_PDR));
    equal = equal && (this.T_CIVICO_PDR == null ? that.T_CIVICO_PDR == null : this.T_CIVICO_PDR.equals(that.T_CIVICO_PDR));
    equal = equal && (this.T_CAP_PDR == null ? that.T_CAP_PDR == null : this.T_CAP_PDR.equals(that.T_CAP_PDR));
    equal = equal && (this.T_COMUNE_ISTAT_PDR == null ? that.T_COMUNE_ISTAT_PDR == null : this.T_COMUNE_ISTAT_PDR.equals(that.T_COMUNE_ISTAT_PDR));
    equal = equal && (this.T_COMUNE_PDR == null ? that.T_COMUNE_PDR == null : this.T_COMUNE_PDR.equals(that.T_COMUNE_PDR));
    equal = equal && (this.T_PROVINCIA_PDR == null ? that.T_PROVINCIA_PDR == null : this.T_PROVINCIA_PDR.equals(that.T_PROVINCIA_PDR));
    equal = equal && (this.T_NAZIONE_PDR == null ? that.T_NAZIONE_PDR == null : this.T_NAZIONE_PDR.equals(that.T_NAZIONE_PDR));
    equal = equal && (this.ALTRO_IND_PDR == null ? that.ALTRO_IND_PDR == null : this.ALTRO_IND_PDR.equals(that.ALTRO_IND_PDR));
    equal = equal && (this.T_TOPONIMO_FORN == null ? that.T_TOPONIMO_FORN == null : this.T_TOPONIMO_FORN.equals(that.T_TOPONIMO_FORN));
    equal = equal && (this.T_NOMESTRADA_FORN == null ? that.T_NOMESTRADA_FORN == null : this.T_NOMESTRADA_FORN.equals(that.T_NOMESTRADA_FORN));
    equal = equal && (this.T_CIVICO_FORN == null ? that.T_CIVICO_FORN == null : this.T_CIVICO_FORN.equals(that.T_CIVICO_FORN));
    equal = equal && (this.T_CAP_FORN == null ? that.T_CAP_FORN == null : this.T_CAP_FORN.equals(that.T_CAP_FORN));
    equal = equal && (this.T_COMUNE_ISTATFORN == null ? that.T_COMUNE_ISTATFORN == null : this.T_COMUNE_ISTATFORN.equals(that.T_COMUNE_ISTATFORN));
    equal = equal && (this.T_COMUNE_FORN == null ? that.T_COMUNE_FORN == null : this.T_COMUNE_FORN.equals(that.T_COMUNE_FORN));
    equal = equal && (this.T_PROVINCIA_FORN == null ? that.T_PROVINCIA_FORN == null : this.T_PROVINCIA_FORN.equals(that.T_PROVINCIA_FORN));
    equal = equal && (this.T_NAZIONE_FORN == null ? that.T_NAZIONE_FORN == null : this.T_NAZIONE_FORN.equals(that.T_NAZIONE_FORN));
    equal = equal && (this.ALTRO_IND_FORN == null ? that.ALTRO_IND_FORN == null : this.ALTRO_IND_FORN.equals(that.ALTRO_IND_FORN));
    equal = equal && (this.T_ACCESSO_UI == null ? that.T_ACCESSO_UI == null : this.T_ACCESSO_UI.equals(that.T_ACCESSO_UI));
    equal = equal && (this.T_TIPO_FORNITURA == null ? that.T_TIPO_FORNITURA == null : this.T_TIPO_FORNITURA.equals(that.T_TIPO_FORNITURA));
    equal = equal && (this.T_ALIQUOTA_IVA == null ? that.T_ALIQUOTA_IVA == null : this.T_ALIQUOTA_IVA.equals(that.T_ALIQUOTA_IVA));
    equal = equal && (this.T_ALIQUOTA_ACCISE == null ? that.T_ALIQUOTA_ACCISE == null : this.T_ALIQUOTA_ACCISE.equals(that.T_ALIQUOTA_ACCISE));
    equal = equal && (this.T_ADD_REGIONALE == null ? that.T_ADD_REGIONALE == null : this.T_ADD_REGIONALE.equals(that.T_ADD_REGIONALE));
    equal = equal && (this.T_ALTRE_INFO_IMPOSTE == null ? that.T_ALTRE_INFO_IMPOSTE == null : this.T_ALTRE_INFO_IMPOSTE.equals(that.T_ALTRE_INFO_IMPOSTE));
    equal = equal && (this.T_MATRICOLA_MISURATORE == null ? that.T_MATRICOLA_MISURATORE == null : this.T_MATRICOLA_MISURATORE.equals(that.T_MATRICOLA_MISURATORE));
    equal = equal && (this.T_CLASSE_MISURATORE == null ? that.T_CLASSE_MISURATORE == null : this.T_CLASSE_MISURATORE.equals(that.T_CLASSE_MISURATORE));
    equal = equal && (this.T_TIPO_MISURATORE == null ? that.T_TIPO_MISURATORE == null : this.T_TIPO_MISURATORE.equals(that.T_TIPO_MISURATORE));
    equal = equal && (this.T_TELEGESTIONE == null ? that.T_TELEGESTIONE == null : this.T_TELEGESTIONE.equals(that.T_TELEGESTIONE));
    equal = equal && (this.T_PRE_CONV == null ? that.T_PRE_CONV == null : this.T_PRE_CONV.equals(that.T_PRE_CONV));
    equal = equal && (this.T_MATRICOLA_CONVERTITORE == null ? that.T_MATRICOLA_CONVERTITORE == null : this.T_MATRICOLA_CONVERTITORE.equals(that.T_MATRICOLA_CONVERTITORE));
    equal = equal && (this.N_NUM_CIFRE_CONVERTITORE == null ? that.N_NUM_CIFRE_CONVERTITORE == null : this.N_NUM_CIFRE_CONVERTITORE.equals(that.N_NUM_CIFRE_CONVERTITORE));
    equal = equal && (this.T_ANNO_FABBRIC_CONVERTITORE == null ? that.T_ANNO_FABBRIC_CONVERTITORE == null : this.T_ANNO_FABBRIC_CONVERTITORE.equals(that.T_ANNO_FABBRIC_CONVERTITORE));
    equal = equal && (this.T_DATA_INST_CONVERTITORE == null ? that.T_DATA_INST_CONVERTITORE == null : this.T_DATA_INST_CONVERTITORE.equals(that.T_DATA_INST_CONVERTITORE));
    equal = equal && (this.N_COEFF_CORREZIONE == null ? that.N_COEFF_CORREZIONE == null : this.N_COEFF_CORREZIONE.equals(that.N_COEFF_CORREZIONE));
    equal = equal && (this.PRESS_MISURE == null ? that.PRESS_MISURE == null : this.PRESS_MISURE.equals(that.PRESS_MISURE));
    equal = equal && (this.T_ACCESS_MISURATORE == null ? that.T_ACCESS_MISURATORE == null : this.T_ACCESS_MISURATORE.equals(that.T_ACCESS_MISURATORE));
    equal = equal && (this.N_NUM_CIFRE_MISURATORE == null ? that.N_NUM_CIFRE_MISURATORE == null : this.N_NUM_CIFRE_MISURATORE.equals(that.N_NUM_CIFRE_MISURATORE));
    equal = equal && (this.T_ANNO_FABBRIC_MISURATORE == null ? that.T_ANNO_FABBRIC_MISURATORE == null : this.T_ANNO_FABBRIC_MISURATORE.equals(that.T_ANNO_FABBRIC_MISURATORE));
    equal = equal && (this.T_DATA_INST_MISURATORE == null ? that.T_DATA_INST_MISURATORE == null : this.T_DATA_INST_MISURATORE.equals(that.T_DATA_INST_MISURATORE));
    equal = equal && (this.T_MISURATORE_INTEGRATO == null ? that.T_MISURATORE_INTEGRATO == null : this.T_MISURATORE_INTEGRATO.equals(that.T_MISURATORE_INTEGRATO));
    equal = equal && (this.N_POTENZIALITA_MASSIMA == null ? that.N_POTENZIALITA_MASSIMA == null : this.N_POTENZIALITA_MASSIMA.equals(that.N_POTENZIALITA_MASSIMA));
    equal = equal && (this.N_POTENZIALITA_TOT_INSTALLATA == null ? that.N_POTENZIALITA_TOT_INSTALLATA == null : this.N_POTENZIALITA_TOT_INSTALLATA.equals(that.N_POTENZIALITA_TOT_INSTALLATA));
    equal = equal && (this.N_MAX_PRELIEVO_ORARIO == null ? that.N_MAX_PRELIEVO_ORARIO == null : this.N_MAX_PRELIEVO_ORARIO.equals(that.N_MAX_PRELIEVO_ORARIO));
    equal = equal && (this.T_EROG_SERVIZIO_ENERG == null ? that.T_EROG_SERVIZIO_ENERG == null : this.T_EROG_SERVIZIO_ENERG.equals(that.T_EROG_SERVIZIO_ENERG));
    equal = equal && (this.T_PARTITA_IVA_GESTCAL == null ? that.T_PARTITA_IVA_GESTCAL == null : this.T_PARTITA_IVA_GESTCAL.equals(that.T_PARTITA_IVA_GESTCAL));
    equal = equal && (this.T_RAGIONE_SOCIALE_GESTCAL == null ? that.T_RAGIONE_SOCIALE_GESTCAL == null : this.T_RAGIONE_SOCIALE_GESTCAL.equals(that.T_RAGIONE_SOCIALE_GESTCAL));
    equal = equal && (this.T_TELEFONO_GESTCAL == null ? that.T_TELEFONO_GESTCAL == null : this.T_TELEFONO_GESTCAL.equals(that.T_TELEFONO_GESTCAL));
    equal = equal && (this.T_EMAIL_GESTCAL == null ? that.T_EMAIL_GESTCAL == null : this.T_EMAIL_GESTCAL.equals(that.T_EMAIL_GESTCAL));
    equal = equal && (this.T_TOPONIMO_GESTCAL == null ? that.T_TOPONIMO_GESTCAL == null : this.T_TOPONIMO_GESTCAL.equals(that.T_TOPONIMO_GESTCAL));
    equal = equal && (this.T_NOMESTRADA_GESTCAL == null ? that.T_NOMESTRADA_GESTCAL == null : this.T_NOMESTRADA_GESTCAL.equals(that.T_NOMESTRADA_GESTCAL));
    equal = equal && (this.T_CIVICO_GESTCAL == null ? that.T_CIVICO_GESTCAL == null : this.T_CIVICO_GESTCAL.equals(that.T_CIVICO_GESTCAL));
    equal = equal && (this.T_CAP_GESTCAL == null ? that.T_CAP_GESTCAL == null : this.T_CAP_GESTCAL.equals(that.T_CAP_GESTCAL));
    equal = equal && (this.T_COMUNE_ISTAT_GESTCAL == null ? that.T_COMUNE_ISTAT_GESTCAL == null : this.T_COMUNE_ISTAT_GESTCAL.equals(that.T_COMUNE_ISTAT_GESTCAL));
    equal = equal && (this.T_COMUNE_GESTCAL == null ? that.T_COMUNE_GESTCAL == null : this.T_COMUNE_GESTCAL.equals(that.T_COMUNE_GESTCAL));
    equal = equal && (this.T_PROVINCIA_GESTCAL == null ? that.T_PROVINCIA_GESTCAL == null : this.T_PROVINCIA_GESTCAL.equals(that.T_PROVINCIA_GESTCAL));
    equal = equal && (this.T_NAZIONE_GESTCAL == null ? that.T_NAZIONE_GESTCAL == null : this.T_NAZIONE_GESTCAL.equals(that.T_NAZIONE_GESTCAL));
    equal = equal && (this.D_DATA_RIF_PDR == null ? that.D_DATA_RIF_PDR == null : this.D_DATA_RIF_PDR.equals(that.D_DATA_RIF_PDR));
    equal = equal && (this.D_AGGIORNAMENTO_PDR == null ? that.D_AGGIORNAMENTO_PDR == null : this.D_AGGIORNAMENTO_PDR.equals(that.D_AGGIORNAMENTO_PDR));
    equal = equal && (this.D_DATA_RIF_TECN == null ? that.D_DATA_RIF_TECN == null : this.D_DATA_RIF_TECN.equals(that.D_DATA_RIF_TECN));
    equal = equal && (this.D_AGGIORNAMENTO_TECN == null ? that.D_AGGIORNAMENTO_TECN == null : this.D_AGGIORNAMENTO_TECN.equals(that.D_AGGIORNAMENTO_TECN));
    equal = equal && (this.D_DATA_RIF_MIS == null ? that.D_DATA_RIF_MIS == null : this.D_DATA_RIF_MIS.equals(that.D_DATA_RIF_MIS));
    equal = equal && (this.D_AGGIORNAMENTO_MIS == null ? that.D_AGGIORNAMENTO_MIS == null : this.D_AGGIORNAMENTO_MIS.equals(that.D_AGGIORNAMENTO_MIS));
    equal = equal && (this.D_DATA_RIF_FORN == null ? that.D_DATA_RIF_FORN == null : this.D_DATA_RIF_FORN.equals(that.D_DATA_RIF_FORN));
    equal = equal && (this.D_AGGIORNAMENTO_FORN == null ? that.D_AGGIORNAMENTO_FORN == null : this.D_AGGIORNAMENTO_FORN.equals(that.D_AGGIORNAMENTO_FORN));
    equal = equal && (this.T_TIPO_BONUS == null ? that.T_TIPO_BONUS == null : this.T_TIPO_BONUS.equals(that.T_TIPO_BONUS));
    equal = equal && (this.D_DATA_INIZIO_EROG_BONUS == null ? that.D_DATA_INIZIO_EROG_BONUS == null : this.D_DATA_INIZIO_EROG_BONUS.equals(that.D_DATA_INIZIO_EROG_BONUS));
    equal = equal && (this.D_DATA_FINE_EROG_BONUS == null ? that.D_DATA_FINE_EROG_BONUS == null : this.D_DATA_FINE_EROG_BONUS.equals(that.D_DATA_FINE_EROG_BONUS));
    equal = equal && (this.D_DATA_RIF_BONUS == null ? that.D_DATA_RIF_BONUS == null : this.D_DATA_RIF_BONUS.equals(that.D_DATA_RIF_BONUS));
    equal = equal && (this.D_AGGIORNAMENTO_BONUS == null ? that.D_AGGIORNAMENTO_BONUS == null : this.D_AGGIORNAMENTO_BONUS.equals(that.D_AGGIORNAMENTO_BONUS));
    equal = equal && (this.D_DATA_AGGIORNAMENTO == null ? that.D_DATA_AGGIORNAMENTO == null : this.D_DATA_AGGIORNAMENTO.equals(that.D_DATA_AGGIORNAMENTO));
    equal = equal && (this.N_ID_UDD == null ? that.N_ID_UDD == null : this.N_ID_UDD.equals(that.N_ID_UDD));
    equal = equal && (this.N_ID_VENDITORE == null ? that.N_ID_VENDITORE == null : this.N_ID_VENDITORE.equals(that.N_ID_VENDITORE));
    equal = equal && (this.T_COD_PROFILO == null ? that.T_COD_PROFILO == null : this.T_COD_PROFILO.equals(that.T_COD_PROFILO));
    equal = equal && (this.T_COD_CAT_USO == null ? that.T_COD_CAT_USO == null : this.T_COD_CAT_USO.equals(that.T_COD_CAT_USO));
    equal = equal && (this.T_COD_CLASSE_PRELIEVO == null ? that.T_COD_CLASSE_PRELIEVO == null : this.T_COD_CLASSE_PRELIEVO.equals(that.T_COD_CLASSE_PRELIEVO));
    equal = equal && (this.T_ANNO_TERMICO == null ? that.T_ANNO_TERMICO == null : this.T_ANNO_TERMICO.equals(that.T_ANNO_TERMICO));
    equal = equal && (this.D_DATA_RIF_PREL == null ? that.D_DATA_RIF_PREL == null : this.D_DATA_RIF_PREL.equals(that.D_DATA_RIF_PREL));
    equal = equal && (this.T_TRATTAMENTO == null ? that.T_TRATTAMENTO == null : this.T_TRATTAMENTO.equals(that.T_TRATTAMENTO));
    equal = equal && (this.T_TOPONIMO_ESAZ == null ? that.T_TOPONIMO_ESAZ == null : this.T_TOPONIMO_ESAZ.equals(that.T_TOPONIMO_ESAZ));
    equal = equal && (this.T_NOMESTRADA_ESAZ == null ? that.T_NOMESTRADA_ESAZ == null : this.T_NOMESTRADA_ESAZ.equals(that.T_NOMESTRADA_ESAZ));
    equal = equal && (this.T_CIVICO_ESAZ == null ? that.T_CIVICO_ESAZ == null : this.T_CIVICO_ESAZ.equals(that.T_CIVICO_ESAZ));
    equal = equal && (this.T_CAP_ESAZ == null ? that.T_CAP_ESAZ == null : this.T_CAP_ESAZ.equals(that.T_CAP_ESAZ));
    equal = equal && (this.T_COMUNE_ISTAT_ESAZ == null ? that.T_COMUNE_ISTAT_ESAZ == null : this.T_COMUNE_ISTAT_ESAZ.equals(that.T_COMUNE_ISTAT_ESAZ));
    equal = equal && (this.T_COMUNE_ESAZ == null ? that.T_COMUNE_ESAZ == null : this.T_COMUNE_ESAZ.equals(that.T_COMUNE_ESAZ));
    equal = equal && (this.T_PROVINCIA_ESAZ == null ? that.T_PROVINCIA_ESAZ == null : this.T_PROVINCIA_ESAZ.equals(that.T_PROVINCIA_ESAZ));
    equal = equal && (this.T_NAZIONE_ESAZ == null ? that.T_NAZIONE_ESAZ == null : this.T_NAZIONE_ESAZ.equals(that.T_NAZIONE_ESAZ));
    equal = equal && (this.ALTRO_IND_ESAZ == null ? that.ALTRO_IND_ESAZ == null : this.ALTRO_IND_ESAZ.equals(that.ALTRO_IND_ESAZ));
    equal = equal && (this.T_CODICE_ATECO == null ? that.T_CODICE_ATECO == null : this.T_CODICE_ATECO.equals(that.T_CODICE_ATECO));
    equal = equal && (this.T_PAGAMENTO_IVA == null ? that.T_PAGAMENTO_IVA == null : this.T_PAGAMENTO_IVA.equals(that.T_PAGAMENTO_IVA));
    equal = equal && (this.T_CODICE_UFFICIO == null ? that.T_CODICE_UFFICIO == null : this.T_CODICE_UFFICIO.equals(that.T_CODICE_UFFICIO));
    equal = equal && (this.T_CF_INTESTATARIO_FATT == null ? that.T_CF_INTESTATARIO_FATT == null : this.T_CF_INTESTATARIO_FATT.equals(that.T_CF_INTESTATARIO_FATT));
    equal = equal && (this.T_CF_STRANIERO_FATT == null ? that.T_CF_STRANIERO_FATT == null : this.T_CF_STRANIERO_FATT.equals(that.T_CF_STRANIERO_FATT));
    equal = equal && (this.T_PIVA_INTESTATARIO_FATT == null ? that.T_PIVA_INTESTATARIO_FATT == null : this.T_PIVA_INTESTATARIO_FATT.equals(that.T_PIVA_INTESTATARIO_FATT));
    equal = equal && (this.T_NOME_INTESTATARIO_FATT == null ? that.T_NOME_INTESTATARIO_FATT == null : this.T_NOME_INTESTATARIO_FATT.equals(that.T_NOME_INTESTATARIO_FATT));
    equal = equal && (this.T_COGNOME_INTESTATARIO_FATT == null ? that.T_COGNOME_INTESTATARIO_FATT == null : this.T_COGNOME_INTESTATARIO_FATT.equals(that.T_COGNOME_INTESTATARIO_FATT));
    equal = equal && (this.T_RAG_SOC_INTESTATARIO_FATT == null ? that.T_RAG_SOC_INTESTATARIO_FATT == null : this.T_RAG_SOC_INTESTATARIO_FATT.equals(that.T_RAG_SOC_INTESTATARIO_FATT));
    equal = equal && (this.T_ANNO_MESE_RINN_BONUS == null ? that.T_ANNO_MESE_RINN_BONUS == null : this.T_ANNO_MESE_RINN_BONUS.equals(that.T_ANNO_MESE_RINN_BONUS));
    equal = equal && (this.D_DATA_INIZIO_BONUS == null ? that.D_DATA_INIZIO_BONUS == null : this.D_DATA_INIZIO_BONUS.equals(that.D_DATA_INIZIO_BONUS));
    equal = equal && (this.D_DATA_FINE_BONUS == null ? that.D_DATA_FINE_BONUS == null : this.D_DATA_FINE_BONUS.equals(that.D_DATA_FINE_BONUS));
    equal = equal && (this.N_PRELIEVO_ANNUO == null ? that.N_PRELIEVO_ANNUO == null : this.N_PRELIEVO_ANNUO.equals(that.N_PRELIEVO_ANNUO));
    equal = equal && (this.T_FATTORE_CORREZ_CLIMATICA == null ? that.T_FATTORE_CORREZ_CLIMATICA == null : this.T_FATTORE_CORREZ_CLIMATICA.equals(that.T_FATTORE_CORREZ_CLIMATICA));
    equal = equal && (this.T_ALTRO_IND_GESTCAL == null ? that.T_ALTRO_IND_GESTCAL == null : this.T_ALTRO_IND_GESTCAL.equals(that.T_ALTRO_IND_GESTCAL));
    equal = equal && (this.T_TIPO_OP == null ? that.T_TIPO_OP == null : this.T_TIPO_OP.equals(that.T_TIPO_OP));
    equal = equal && (this.T_PROCESSO == null ? that.T_PROCESSO == null : this.T_PROCESSO.equals(that.T_PROCESSO));
    equal = equal && (this.N_ID_PRATICA_PROCESSO == null ? that.N_ID_PRATICA_PROCESSO == null : this.N_ID_PRATICA_PROCESSO.equals(that.N_ID_PRATICA_PROCESSO));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof drosati_rcugas_massivo_sosia)) {
      return false;
    }
    drosati_rcugas_massivo_sosia that = (drosati_rcugas_massivo_sosia) o;
    boolean equal = true;
    equal = equal && (this.N_ID_PDR == null ? that.N_ID_PDR == null : this.N_ID_PDR.equals(that.N_ID_PDR));
    equal = equal && (this.T_CODICE_PDR == null ? that.T_CODICE_PDR == null : this.T_CODICE_PDR.equals(that.T_CODICE_PDR));
    equal = equal && (this.CAPACITA_TRASPORTO == null ? that.CAPACITA_TRASPORTO == null : this.CAPACITA_TRASPORTO.equals(that.CAPACITA_TRASPORTO));
    equal = equal && (this.MESE_VAL_CAP_TRASP == null ? that.MESE_VAL_CAP_TRASP == null : this.MESE_VAL_CAP_TRASP.equals(that.MESE_VAL_CAP_TRASP));
    equal = equal && (this.T_COD_TIPO_PDR == null ? that.T_COD_TIPO_PDR == null : this.T_COD_TIPO_PDR.equals(that.T_COD_TIPO_PDR));
    equal = equal && (this.T_DISALIMENTABILITA == null ? that.T_DISALIMENTABILITA == null : this.T_DISALIMENTABILITA.equals(that.T_DISALIMENTABILITA));
    equal = equal && (this.BILANCIAMENTO == null ? that.BILANCIAMENTO == null : this.BILANCIAMENTO.equals(that.BILANCIAMENTO));
    equal = equal && (this.N_ID_FORNITURA == null ? that.N_ID_FORNITURA == null : this.N_ID_FORNITURA.equals(that.N_ID_FORNITURA));
    equal = equal && (this.D_DATA_INIZIO_FOR == null ? that.D_DATA_INIZIO_FOR == null : this.D_DATA_INIZIO_FOR.equals(that.D_DATA_INIZIO_FOR));
    equal = equal && (this.DATA_FINE_FOR == null ? that.DATA_FINE_FOR == null : this.DATA_FINE_FOR.equals(that.DATA_FINE_FOR));
    equal = equal && (this.N_ID_AZ_UDD == null ? that.N_ID_AZ_UDD == null : this.N_ID_AZ_UDD.equals(that.N_ID_AZ_UDD));
    equal = equal && (this.PIVA_UDD == null ? that.PIVA_UDD == null : this.PIVA_UDD.equals(that.PIVA_UDD));
    equal = equal && (this.N_ID_AZ_CC == null ? that.N_ID_AZ_CC == null : this.N_ID_AZ_CC.equals(that.N_ID_AZ_CC));
    equal = equal && (this.PIVA_CC == null ? that.PIVA_CC == null : this.PIVA_CC.equals(that.PIVA_CC));
    equal = equal && (this.N_ID_CLIENTE == null ? that.N_ID_CLIENTE == null : this.N_ID_CLIENTE.equals(that.N_ID_CLIENTE));
    equal = equal && (this.T_PARTITA_IVA_CLI == null ? that.T_PARTITA_IVA_CLI == null : this.T_PARTITA_IVA_CLI.equals(that.T_PARTITA_IVA_CLI));
    equal = equal && (this.T_CODICE_FISCALE_CLI == null ? that.T_CODICE_FISCALE_CLI == null : this.T_CODICE_FISCALE_CLI.equals(that.T_CODICE_FISCALE_CLI));
    equal = equal && (this.B_CF_STRANIERO == null ? that.B_CF_STRANIERO == null : this.B_CF_STRANIERO.equals(that.B_CF_STRANIERO));
    equal = equal && (this.T_REFERENTE == null ? that.T_REFERENTE == null : this.T_REFERENTE.equals(that.T_REFERENTE));
    equal = equal && (this.T_NOME_REF == null ? that.T_NOME_REF == null : this.T_NOME_REF.equals(that.T_NOME_REF));
    equal = equal && (this.T_COGNOME_REF == null ? that.T_COGNOME_REF == null : this.T_COGNOME_REF.equals(that.T_COGNOME_REF));
    equal = equal && (this.T_EMAIL_REF == null ? that.T_EMAIL_REF == null : this.T_EMAIL_REF.equals(that.T_EMAIL_REF));
    equal = equal && (this.T_TELEFONO_REF == null ? that.T_TELEFONO_REF == null : this.T_TELEFONO_REF.equals(that.T_TELEFONO_REF));
    equal = equal && (this.T_RESIDENZA == null ? that.T_RESIDENZA == null : this.T_RESIDENZA.equals(that.T_RESIDENZA));
    equal = equal && (this.DATA_VAL_RES == null ? that.DATA_VAL_RES == null : this.DATA_VAL_RES.equals(that.DATA_VAL_RES));
    equal = equal && (this.T_TOPONIMOPDR == null ? that.T_TOPONIMOPDR == null : this.T_TOPONIMOPDR.equals(that.T_TOPONIMOPDR));
    equal = equal && (this.T_NOMESTRADA_PDR == null ? that.T_NOMESTRADA_PDR == null : this.T_NOMESTRADA_PDR.equals(that.T_NOMESTRADA_PDR));
    equal = equal && (this.T_CIVICO_PDR == null ? that.T_CIVICO_PDR == null : this.T_CIVICO_PDR.equals(that.T_CIVICO_PDR));
    equal = equal && (this.T_CAP_PDR == null ? that.T_CAP_PDR == null : this.T_CAP_PDR.equals(that.T_CAP_PDR));
    equal = equal && (this.T_COMUNE_ISTAT_PDR == null ? that.T_COMUNE_ISTAT_PDR == null : this.T_COMUNE_ISTAT_PDR.equals(that.T_COMUNE_ISTAT_PDR));
    equal = equal && (this.T_COMUNE_PDR == null ? that.T_COMUNE_PDR == null : this.T_COMUNE_PDR.equals(that.T_COMUNE_PDR));
    equal = equal && (this.T_PROVINCIA_PDR == null ? that.T_PROVINCIA_PDR == null : this.T_PROVINCIA_PDR.equals(that.T_PROVINCIA_PDR));
    equal = equal && (this.T_NAZIONE_PDR == null ? that.T_NAZIONE_PDR == null : this.T_NAZIONE_PDR.equals(that.T_NAZIONE_PDR));
    equal = equal && (this.ALTRO_IND_PDR == null ? that.ALTRO_IND_PDR == null : this.ALTRO_IND_PDR.equals(that.ALTRO_IND_PDR));
    equal = equal && (this.T_TOPONIMO_FORN == null ? that.T_TOPONIMO_FORN == null : this.T_TOPONIMO_FORN.equals(that.T_TOPONIMO_FORN));
    equal = equal && (this.T_NOMESTRADA_FORN == null ? that.T_NOMESTRADA_FORN == null : this.T_NOMESTRADA_FORN.equals(that.T_NOMESTRADA_FORN));
    equal = equal && (this.T_CIVICO_FORN == null ? that.T_CIVICO_FORN == null : this.T_CIVICO_FORN.equals(that.T_CIVICO_FORN));
    equal = equal && (this.T_CAP_FORN == null ? that.T_CAP_FORN == null : this.T_CAP_FORN.equals(that.T_CAP_FORN));
    equal = equal && (this.T_COMUNE_ISTATFORN == null ? that.T_COMUNE_ISTATFORN == null : this.T_COMUNE_ISTATFORN.equals(that.T_COMUNE_ISTATFORN));
    equal = equal && (this.T_COMUNE_FORN == null ? that.T_COMUNE_FORN == null : this.T_COMUNE_FORN.equals(that.T_COMUNE_FORN));
    equal = equal && (this.T_PROVINCIA_FORN == null ? that.T_PROVINCIA_FORN == null : this.T_PROVINCIA_FORN.equals(that.T_PROVINCIA_FORN));
    equal = equal && (this.T_NAZIONE_FORN == null ? that.T_NAZIONE_FORN == null : this.T_NAZIONE_FORN.equals(that.T_NAZIONE_FORN));
    equal = equal && (this.ALTRO_IND_FORN == null ? that.ALTRO_IND_FORN == null : this.ALTRO_IND_FORN.equals(that.ALTRO_IND_FORN));
    equal = equal && (this.T_ACCESSO_UI == null ? that.T_ACCESSO_UI == null : this.T_ACCESSO_UI.equals(that.T_ACCESSO_UI));
    equal = equal && (this.T_TIPO_FORNITURA == null ? that.T_TIPO_FORNITURA == null : this.T_TIPO_FORNITURA.equals(that.T_TIPO_FORNITURA));
    equal = equal && (this.T_ALIQUOTA_IVA == null ? that.T_ALIQUOTA_IVA == null : this.T_ALIQUOTA_IVA.equals(that.T_ALIQUOTA_IVA));
    equal = equal && (this.T_ALIQUOTA_ACCISE == null ? that.T_ALIQUOTA_ACCISE == null : this.T_ALIQUOTA_ACCISE.equals(that.T_ALIQUOTA_ACCISE));
    equal = equal && (this.T_ADD_REGIONALE == null ? that.T_ADD_REGIONALE == null : this.T_ADD_REGIONALE.equals(that.T_ADD_REGIONALE));
    equal = equal && (this.T_ALTRE_INFO_IMPOSTE == null ? that.T_ALTRE_INFO_IMPOSTE == null : this.T_ALTRE_INFO_IMPOSTE.equals(that.T_ALTRE_INFO_IMPOSTE));
    equal = equal && (this.T_MATRICOLA_MISURATORE == null ? that.T_MATRICOLA_MISURATORE == null : this.T_MATRICOLA_MISURATORE.equals(that.T_MATRICOLA_MISURATORE));
    equal = equal && (this.T_CLASSE_MISURATORE == null ? that.T_CLASSE_MISURATORE == null : this.T_CLASSE_MISURATORE.equals(that.T_CLASSE_MISURATORE));
    equal = equal && (this.T_TIPO_MISURATORE == null ? that.T_TIPO_MISURATORE == null : this.T_TIPO_MISURATORE.equals(that.T_TIPO_MISURATORE));
    equal = equal && (this.T_TELEGESTIONE == null ? that.T_TELEGESTIONE == null : this.T_TELEGESTIONE.equals(that.T_TELEGESTIONE));
    equal = equal && (this.T_PRE_CONV == null ? that.T_PRE_CONV == null : this.T_PRE_CONV.equals(that.T_PRE_CONV));
    equal = equal && (this.T_MATRICOLA_CONVERTITORE == null ? that.T_MATRICOLA_CONVERTITORE == null : this.T_MATRICOLA_CONVERTITORE.equals(that.T_MATRICOLA_CONVERTITORE));
    equal = equal && (this.N_NUM_CIFRE_CONVERTITORE == null ? that.N_NUM_CIFRE_CONVERTITORE == null : this.N_NUM_CIFRE_CONVERTITORE.equals(that.N_NUM_CIFRE_CONVERTITORE));
    equal = equal && (this.T_ANNO_FABBRIC_CONVERTITORE == null ? that.T_ANNO_FABBRIC_CONVERTITORE == null : this.T_ANNO_FABBRIC_CONVERTITORE.equals(that.T_ANNO_FABBRIC_CONVERTITORE));
    equal = equal && (this.T_DATA_INST_CONVERTITORE == null ? that.T_DATA_INST_CONVERTITORE == null : this.T_DATA_INST_CONVERTITORE.equals(that.T_DATA_INST_CONVERTITORE));
    equal = equal && (this.N_COEFF_CORREZIONE == null ? that.N_COEFF_CORREZIONE == null : this.N_COEFF_CORREZIONE.equals(that.N_COEFF_CORREZIONE));
    equal = equal && (this.PRESS_MISURE == null ? that.PRESS_MISURE == null : this.PRESS_MISURE.equals(that.PRESS_MISURE));
    equal = equal && (this.T_ACCESS_MISURATORE == null ? that.T_ACCESS_MISURATORE == null : this.T_ACCESS_MISURATORE.equals(that.T_ACCESS_MISURATORE));
    equal = equal && (this.N_NUM_CIFRE_MISURATORE == null ? that.N_NUM_CIFRE_MISURATORE == null : this.N_NUM_CIFRE_MISURATORE.equals(that.N_NUM_CIFRE_MISURATORE));
    equal = equal && (this.T_ANNO_FABBRIC_MISURATORE == null ? that.T_ANNO_FABBRIC_MISURATORE == null : this.T_ANNO_FABBRIC_MISURATORE.equals(that.T_ANNO_FABBRIC_MISURATORE));
    equal = equal && (this.T_DATA_INST_MISURATORE == null ? that.T_DATA_INST_MISURATORE == null : this.T_DATA_INST_MISURATORE.equals(that.T_DATA_INST_MISURATORE));
    equal = equal && (this.T_MISURATORE_INTEGRATO == null ? that.T_MISURATORE_INTEGRATO == null : this.T_MISURATORE_INTEGRATO.equals(that.T_MISURATORE_INTEGRATO));
    equal = equal && (this.N_POTENZIALITA_MASSIMA == null ? that.N_POTENZIALITA_MASSIMA == null : this.N_POTENZIALITA_MASSIMA.equals(that.N_POTENZIALITA_MASSIMA));
    equal = equal && (this.N_POTENZIALITA_TOT_INSTALLATA == null ? that.N_POTENZIALITA_TOT_INSTALLATA == null : this.N_POTENZIALITA_TOT_INSTALLATA.equals(that.N_POTENZIALITA_TOT_INSTALLATA));
    equal = equal && (this.N_MAX_PRELIEVO_ORARIO == null ? that.N_MAX_PRELIEVO_ORARIO == null : this.N_MAX_PRELIEVO_ORARIO.equals(that.N_MAX_PRELIEVO_ORARIO));
    equal = equal && (this.T_EROG_SERVIZIO_ENERG == null ? that.T_EROG_SERVIZIO_ENERG == null : this.T_EROG_SERVIZIO_ENERG.equals(that.T_EROG_SERVIZIO_ENERG));
    equal = equal && (this.T_PARTITA_IVA_GESTCAL == null ? that.T_PARTITA_IVA_GESTCAL == null : this.T_PARTITA_IVA_GESTCAL.equals(that.T_PARTITA_IVA_GESTCAL));
    equal = equal && (this.T_RAGIONE_SOCIALE_GESTCAL == null ? that.T_RAGIONE_SOCIALE_GESTCAL == null : this.T_RAGIONE_SOCIALE_GESTCAL.equals(that.T_RAGIONE_SOCIALE_GESTCAL));
    equal = equal && (this.T_TELEFONO_GESTCAL == null ? that.T_TELEFONO_GESTCAL == null : this.T_TELEFONO_GESTCAL.equals(that.T_TELEFONO_GESTCAL));
    equal = equal && (this.T_EMAIL_GESTCAL == null ? that.T_EMAIL_GESTCAL == null : this.T_EMAIL_GESTCAL.equals(that.T_EMAIL_GESTCAL));
    equal = equal && (this.T_TOPONIMO_GESTCAL == null ? that.T_TOPONIMO_GESTCAL == null : this.T_TOPONIMO_GESTCAL.equals(that.T_TOPONIMO_GESTCAL));
    equal = equal && (this.T_NOMESTRADA_GESTCAL == null ? that.T_NOMESTRADA_GESTCAL == null : this.T_NOMESTRADA_GESTCAL.equals(that.T_NOMESTRADA_GESTCAL));
    equal = equal && (this.T_CIVICO_GESTCAL == null ? that.T_CIVICO_GESTCAL == null : this.T_CIVICO_GESTCAL.equals(that.T_CIVICO_GESTCAL));
    equal = equal && (this.T_CAP_GESTCAL == null ? that.T_CAP_GESTCAL == null : this.T_CAP_GESTCAL.equals(that.T_CAP_GESTCAL));
    equal = equal && (this.T_COMUNE_ISTAT_GESTCAL == null ? that.T_COMUNE_ISTAT_GESTCAL == null : this.T_COMUNE_ISTAT_GESTCAL.equals(that.T_COMUNE_ISTAT_GESTCAL));
    equal = equal && (this.T_COMUNE_GESTCAL == null ? that.T_COMUNE_GESTCAL == null : this.T_COMUNE_GESTCAL.equals(that.T_COMUNE_GESTCAL));
    equal = equal && (this.T_PROVINCIA_GESTCAL == null ? that.T_PROVINCIA_GESTCAL == null : this.T_PROVINCIA_GESTCAL.equals(that.T_PROVINCIA_GESTCAL));
    equal = equal && (this.T_NAZIONE_GESTCAL == null ? that.T_NAZIONE_GESTCAL == null : this.T_NAZIONE_GESTCAL.equals(that.T_NAZIONE_GESTCAL));
    equal = equal && (this.D_DATA_RIF_PDR == null ? that.D_DATA_RIF_PDR == null : this.D_DATA_RIF_PDR.equals(that.D_DATA_RIF_PDR));
    equal = equal && (this.D_AGGIORNAMENTO_PDR == null ? that.D_AGGIORNAMENTO_PDR == null : this.D_AGGIORNAMENTO_PDR.equals(that.D_AGGIORNAMENTO_PDR));
    equal = equal && (this.D_DATA_RIF_TECN == null ? that.D_DATA_RIF_TECN == null : this.D_DATA_RIF_TECN.equals(that.D_DATA_RIF_TECN));
    equal = equal && (this.D_AGGIORNAMENTO_TECN == null ? that.D_AGGIORNAMENTO_TECN == null : this.D_AGGIORNAMENTO_TECN.equals(that.D_AGGIORNAMENTO_TECN));
    equal = equal && (this.D_DATA_RIF_MIS == null ? that.D_DATA_RIF_MIS == null : this.D_DATA_RIF_MIS.equals(that.D_DATA_RIF_MIS));
    equal = equal && (this.D_AGGIORNAMENTO_MIS == null ? that.D_AGGIORNAMENTO_MIS == null : this.D_AGGIORNAMENTO_MIS.equals(that.D_AGGIORNAMENTO_MIS));
    equal = equal && (this.D_DATA_RIF_FORN == null ? that.D_DATA_RIF_FORN == null : this.D_DATA_RIF_FORN.equals(that.D_DATA_RIF_FORN));
    equal = equal && (this.D_AGGIORNAMENTO_FORN == null ? that.D_AGGIORNAMENTO_FORN == null : this.D_AGGIORNAMENTO_FORN.equals(that.D_AGGIORNAMENTO_FORN));
    equal = equal && (this.T_TIPO_BONUS == null ? that.T_TIPO_BONUS == null : this.T_TIPO_BONUS.equals(that.T_TIPO_BONUS));
    equal = equal && (this.D_DATA_INIZIO_EROG_BONUS == null ? that.D_DATA_INIZIO_EROG_BONUS == null : this.D_DATA_INIZIO_EROG_BONUS.equals(that.D_DATA_INIZIO_EROG_BONUS));
    equal = equal && (this.D_DATA_FINE_EROG_BONUS == null ? that.D_DATA_FINE_EROG_BONUS == null : this.D_DATA_FINE_EROG_BONUS.equals(that.D_DATA_FINE_EROG_BONUS));
    equal = equal && (this.D_DATA_RIF_BONUS == null ? that.D_DATA_RIF_BONUS == null : this.D_DATA_RIF_BONUS.equals(that.D_DATA_RIF_BONUS));
    equal = equal && (this.D_AGGIORNAMENTO_BONUS == null ? that.D_AGGIORNAMENTO_BONUS == null : this.D_AGGIORNAMENTO_BONUS.equals(that.D_AGGIORNAMENTO_BONUS));
    equal = equal && (this.D_DATA_AGGIORNAMENTO == null ? that.D_DATA_AGGIORNAMENTO == null : this.D_DATA_AGGIORNAMENTO.equals(that.D_DATA_AGGIORNAMENTO));
    equal = equal && (this.N_ID_UDD == null ? that.N_ID_UDD == null : this.N_ID_UDD.equals(that.N_ID_UDD));
    equal = equal && (this.N_ID_VENDITORE == null ? that.N_ID_VENDITORE == null : this.N_ID_VENDITORE.equals(that.N_ID_VENDITORE));
    equal = equal && (this.T_COD_PROFILO == null ? that.T_COD_PROFILO == null : this.T_COD_PROFILO.equals(that.T_COD_PROFILO));
    equal = equal && (this.T_COD_CAT_USO == null ? that.T_COD_CAT_USO == null : this.T_COD_CAT_USO.equals(that.T_COD_CAT_USO));
    equal = equal && (this.T_COD_CLASSE_PRELIEVO == null ? that.T_COD_CLASSE_PRELIEVO == null : this.T_COD_CLASSE_PRELIEVO.equals(that.T_COD_CLASSE_PRELIEVO));
    equal = equal && (this.T_ANNO_TERMICO == null ? that.T_ANNO_TERMICO == null : this.T_ANNO_TERMICO.equals(that.T_ANNO_TERMICO));
    equal = equal && (this.D_DATA_RIF_PREL == null ? that.D_DATA_RIF_PREL == null : this.D_DATA_RIF_PREL.equals(that.D_DATA_RIF_PREL));
    equal = equal && (this.T_TRATTAMENTO == null ? that.T_TRATTAMENTO == null : this.T_TRATTAMENTO.equals(that.T_TRATTAMENTO));
    equal = equal && (this.T_TOPONIMO_ESAZ == null ? that.T_TOPONIMO_ESAZ == null : this.T_TOPONIMO_ESAZ.equals(that.T_TOPONIMO_ESAZ));
    equal = equal && (this.T_NOMESTRADA_ESAZ == null ? that.T_NOMESTRADA_ESAZ == null : this.T_NOMESTRADA_ESAZ.equals(that.T_NOMESTRADA_ESAZ));
    equal = equal && (this.T_CIVICO_ESAZ == null ? that.T_CIVICO_ESAZ == null : this.T_CIVICO_ESAZ.equals(that.T_CIVICO_ESAZ));
    equal = equal && (this.T_CAP_ESAZ == null ? that.T_CAP_ESAZ == null : this.T_CAP_ESAZ.equals(that.T_CAP_ESAZ));
    equal = equal && (this.T_COMUNE_ISTAT_ESAZ == null ? that.T_COMUNE_ISTAT_ESAZ == null : this.T_COMUNE_ISTAT_ESAZ.equals(that.T_COMUNE_ISTAT_ESAZ));
    equal = equal && (this.T_COMUNE_ESAZ == null ? that.T_COMUNE_ESAZ == null : this.T_COMUNE_ESAZ.equals(that.T_COMUNE_ESAZ));
    equal = equal && (this.T_PROVINCIA_ESAZ == null ? that.T_PROVINCIA_ESAZ == null : this.T_PROVINCIA_ESAZ.equals(that.T_PROVINCIA_ESAZ));
    equal = equal && (this.T_NAZIONE_ESAZ == null ? that.T_NAZIONE_ESAZ == null : this.T_NAZIONE_ESAZ.equals(that.T_NAZIONE_ESAZ));
    equal = equal && (this.ALTRO_IND_ESAZ == null ? that.ALTRO_IND_ESAZ == null : this.ALTRO_IND_ESAZ.equals(that.ALTRO_IND_ESAZ));
    equal = equal && (this.T_CODICE_ATECO == null ? that.T_CODICE_ATECO == null : this.T_CODICE_ATECO.equals(that.T_CODICE_ATECO));
    equal = equal && (this.T_PAGAMENTO_IVA == null ? that.T_PAGAMENTO_IVA == null : this.T_PAGAMENTO_IVA.equals(that.T_PAGAMENTO_IVA));
    equal = equal && (this.T_CODICE_UFFICIO == null ? that.T_CODICE_UFFICIO == null : this.T_CODICE_UFFICIO.equals(that.T_CODICE_UFFICIO));
    equal = equal && (this.T_CF_INTESTATARIO_FATT == null ? that.T_CF_INTESTATARIO_FATT == null : this.T_CF_INTESTATARIO_FATT.equals(that.T_CF_INTESTATARIO_FATT));
    equal = equal && (this.T_CF_STRANIERO_FATT == null ? that.T_CF_STRANIERO_FATT == null : this.T_CF_STRANIERO_FATT.equals(that.T_CF_STRANIERO_FATT));
    equal = equal && (this.T_PIVA_INTESTATARIO_FATT == null ? that.T_PIVA_INTESTATARIO_FATT == null : this.T_PIVA_INTESTATARIO_FATT.equals(that.T_PIVA_INTESTATARIO_FATT));
    equal = equal && (this.T_NOME_INTESTATARIO_FATT == null ? that.T_NOME_INTESTATARIO_FATT == null : this.T_NOME_INTESTATARIO_FATT.equals(that.T_NOME_INTESTATARIO_FATT));
    equal = equal && (this.T_COGNOME_INTESTATARIO_FATT == null ? that.T_COGNOME_INTESTATARIO_FATT == null : this.T_COGNOME_INTESTATARIO_FATT.equals(that.T_COGNOME_INTESTATARIO_FATT));
    equal = equal && (this.T_RAG_SOC_INTESTATARIO_FATT == null ? that.T_RAG_SOC_INTESTATARIO_FATT == null : this.T_RAG_SOC_INTESTATARIO_FATT.equals(that.T_RAG_SOC_INTESTATARIO_FATT));
    equal = equal && (this.T_ANNO_MESE_RINN_BONUS == null ? that.T_ANNO_MESE_RINN_BONUS == null : this.T_ANNO_MESE_RINN_BONUS.equals(that.T_ANNO_MESE_RINN_BONUS));
    equal = equal && (this.D_DATA_INIZIO_BONUS == null ? that.D_DATA_INIZIO_BONUS == null : this.D_DATA_INIZIO_BONUS.equals(that.D_DATA_INIZIO_BONUS));
    equal = equal && (this.D_DATA_FINE_BONUS == null ? that.D_DATA_FINE_BONUS == null : this.D_DATA_FINE_BONUS.equals(that.D_DATA_FINE_BONUS));
    equal = equal && (this.N_PRELIEVO_ANNUO == null ? that.N_PRELIEVO_ANNUO == null : this.N_PRELIEVO_ANNUO.equals(that.N_PRELIEVO_ANNUO));
    equal = equal && (this.T_FATTORE_CORREZ_CLIMATICA == null ? that.T_FATTORE_CORREZ_CLIMATICA == null : this.T_FATTORE_CORREZ_CLIMATICA.equals(that.T_FATTORE_CORREZ_CLIMATICA));
    equal = equal && (this.T_ALTRO_IND_GESTCAL == null ? that.T_ALTRO_IND_GESTCAL == null : this.T_ALTRO_IND_GESTCAL.equals(that.T_ALTRO_IND_GESTCAL));
    equal = equal && (this.T_TIPO_OP == null ? that.T_TIPO_OP == null : this.T_TIPO_OP.equals(that.T_TIPO_OP));
    equal = equal && (this.T_PROCESSO == null ? that.T_PROCESSO == null : this.T_PROCESSO.equals(that.T_PROCESSO));
    equal = equal && (this.N_ID_PRATICA_PROCESSO == null ? that.N_ID_PRATICA_PROCESSO == null : this.N_ID_PRATICA_PROCESSO.equals(that.N_ID_PRATICA_PROCESSO));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_PDR = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.T_CODICE_PDR = JdbcWritableBridge.readString(2, __dbResults);
    this.CAPACITA_TRASPORTO = JdbcWritableBridge.readString(3, __dbResults);
    this.MESE_VAL_CAP_TRASP = JdbcWritableBridge.readString(4, __dbResults);
    this.T_COD_TIPO_PDR = JdbcWritableBridge.readString(5, __dbResults);
    this.T_DISALIMENTABILITA = JdbcWritableBridge.readString(6, __dbResults);
    this.BILANCIAMENTO = JdbcWritableBridge.readString(7, __dbResults);
    this.N_ID_FORNITURA = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.D_DATA_INIZIO_FOR = JdbcWritableBridge.readString(9, __dbResults);
    this.DATA_FINE_FOR = JdbcWritableBridge.readString(10, __dbResults);
    this.N_ID_AZ_UDD = JdbcWritableBridge.readBigDecimal(11, __dbResults);
    this.PIVA_UDD = JdbcWritableBridge.readString(12, __dbResults);
    this.N_ID_AZ_CC = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.PIVA_CC = JdbcWritableBridge.readString(14, __dbResults);
    this.N_ID_CLIENTE = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.T_PARTITA_IVA_CLI = JdbcWritableBridge.readString(16, __dbResults);
    this.T_CODICE_FISCALE_CLI = JdbcWritableBridge.readString(17, __dbResults);
    this.B_CF_STRANIERO = JdbcWritableBridge.readString(18, __dbResults);
    this.T_REFERENTE = JdbcWritableBridge.readString(19, __dbResults);
    this.T_NOME_REF = JdbcWritableBridge.readString(20, __dbResults);
    this.T_COGNOME_REF = JdbcWritableBridge.readString(21, __dbResults);
    this.T_EMAIL_REF = JdbcWritableBridge.readString(22, __dbResults);
    this.T_TELEFONO_REF = JdbcWritableBridge.readString(23, __dbResults);
    this.T_RESIDENZA = JdbcWritableBridge.readString(24, __dbResults);
    this.DATA_VAL_RES = JdbcWritableBridge.readString(25, __dbResults);
    this.T_TOPONIMOPDR = JdbcWritableBridge.readString(26, __dbResults);
    this.T_NOMESTRADA_PDR = JdbcWritableBridge.readString(27, __dbResults);
    this.T_CIVICO_PDR = JdbcWritableBridge.readString(28, __dbResults);
    this.T_CAP_PDR = JdbcWritableBridge.readString(29, __dbResults);
    this.T_COMUNE_ISTAT_PDR = JdbcWritableBridge.readString(30, __dbResults);
    this.T_COMUNE_PDR = JdbcWritableBridge.readString(31, __dbResults);
    this.T_PROVINCIA_PDR = JdbcWritableBridge.readString(32, __dbResults);
    this.T_NAZIONE_PDR = JdbcWritableBridge.readString(33, __dbResults);
    this.ALTRO_IND_PDR = JdbcWritableBridge.readString(34, __dbResults);
    this.T_TOPONIMO_FORN = JdbcWritableBridge.readString(35, __dbResults);
    this.T_NOMESTRADA_FORN = JdbcWritableBridge.readString(36, __dbResults);
    this.T_CIVICO_FORN = JdbcWritableBridge.readString(37, __dbResults);
    this.T_CAP_FORN = JdbcWritableBridge.readString(38, __dbResults);
    this.T_COMUNE_ISTATFORN = JdbcWritableBridge.readString(39, __dbResults);
    this.T_COMUNE_FORN = JdbcWritableBridge.readString(40, __dbResults);
    this.T_PROVINCIA_FORN = JdbcWritableBridge.readString(41, __dbResults);
    this.T_NAZIONE_FORN = JdbcWritableBridge.readString(42, __dbResults);
    this.ALTRO_IND_FORN = JdbcWritableBridge.readString(43, __dbResults);
    this.T_ACCESSO_UI = JdbcWritableBridge.readString(44, __dbResults);
    this.T_TIPO_FORNITURA = JdbcWritableBridge.readString(45, __dbResults);
    this.T_ALIQUOTA_IVA = JdbcWritableBridge.readString(46, __dbResults);
    this.T_ALIQUOTA_ACCISE = JdbcWritableBridge.readString(47, __dbResults);
    this.T_ADD_REGIONALE = JdbcWritableBridge.readString(48, __dbResults);
    this.T_ALTRE_INFO_IMPOSTE = JdbcWritableBridge.readString(49, __dbResults);
    this.T_MATRICOLA_MISURATORE = JdbcWritableBridge.readString(50, __dbResults);
    this.T_CLASSE_MISURATORE = JdbcWritableBridge.readString(51, __dbResults);
    this.T_TIPO_MISURATORE = JdbcWritableBridge.readString(52, __dbResults);
    this.T_TELEGESTIONE = JdbcWritableBridge.readString(53, __dbResults);
    this.T_PRE_CONV = JdbcWritableBridge.readString(54, __dbResults);
    this.T_MATRICOLA_CONVERTITORE = JdbcWritableBridge.readString(55, __dbResults);
    this.N_NUM_CIFRE_CONVERTITORE = JdbcWritableBridge.readBigDecimal(56, __dbResults);
    this.T_ANNO_FABBRIC_CONVERTITORE = JdbcWritableBridge.readString(57, __dbResults);
    this.T_DATA_INST_CONVERTITORE = JdbcWritableBridge.readString(58, __dbResults);
    this.N_COEFF_CORREZIONE = JdbcWritableBridge.readBigDecimal(59, __dbResults);
    this.PRESS_MISURE = JdbcWritableBridge.readBigDecimal(60, __dbResults);
    this.T_ACCESS_MISURATORE = JdbcWritableBridge.readBigDecimal(61, __dbResults);
    this.N_NUM_CIFRE_MISURATORE = JdbcWritableBridge.readBigDecimal(62, __dbResults);
    this.T_ANNO_FABBRIC_MISURATORE = JdbcWritableBridge.readString(63, __dbResults);
    this.T_DATA_INST_MISURATORE = JdbcWritableBridge.readString(64, __dbResults);
    this.T_MISURATORE_INTEGRATO = JdbcWritableBridge.readString(65, __dbResults);
    this.N_POTENZIALITA_MASSIMA = JdbcWritableBridge.readBigDecimal(66, __dbResults);
    this.N_POTENZIALITA_TOT_INSTALLATA = JdbcWritableBridge.readBigDecimal(67, __dbResults);
    this.N_MAX_PRELIEVO_ORARIO = JdbcWritableBridge.readBigDecimal(68, __dbResults);
    this.T_EROG_SERVIZIO_ENERG = JdbcWritableBridge.readString(69, __dbResults);
    this.T_PARTITA_IVA_GESTCAL = JdbcWritableBridge.readString(70, __dbResults);
    this.T_RAGIONE_SOCIALE_GESTCAL = JdbcWritableBridge.readString(71, __dbResults);
    this.T_TELEFONO_GESTCAL = JdbcWritableBridge.readString(72, __dbResults);
    this.T_EMAIL_GESTCAL = JdbcWritableBridge.readString(73, __dbResults);
    this.T_TOPONIMO_GESTCAL = JdbcWritableBridge.readString(74, __dbResults);
    this.T_NOMESTRADA_GESTCAL = JdbcWritableBridge.readString(75, __dbResults);
    this.T_CIVICO_GESTCAL = JdbcWritableBridge.readString(76, __dbResults);
    this.T_CAP_GESTCAL = JdbcWritableBridge.readString(77, __dbResults);
    this.T_COMUNE_ISTAT_GESTCAL = JdbcWritableBridge.readString(78, __dbResults);
    this.T_COMUNE_GESTCAL = JdbcWritableBridge.readString(79, __dbResults);
    this.T_PROVINCIA_GESTCAL = JdbcWritableBridge.readString(80, __dbResults);
    this.T_NAZIONE_GESTCAL = JdbcWritableBridge.readString(81, __dbResults);
    this.D_DATA_RIF_PDR = JdbcWritableBridge.readString(82, __dbResults);
    this.D_AGGIORNAMENTO_PDR = JdbcWritableBridge.readString(83, __dbResults);
    this.D_DATA_RIF_TECN = JdbcWritableBridge.readString(84, __dbResults);
    this.D_AGGIORNAMENTO_TECN = JdbcWritableBridge.readString(85, __dbResults);
    this.D_DATA_RIF_MIS = JdbcWritableBridge.readString(86, __dbResults);
    this.D_AGGIORNAMENTO_MIS = JdbcWritableBridge.readString(87, __dbResults);
    this.D_DATA_RIF_FORN = JdbcWritableBridge.readString(88, __dbResults);
    this.D_AGGIORNAMENTO_FORN = JdbcWritableBridge.readString(89, __dbResults);
    this.T_TIPO_BONUS = JdbcWritableBridge.readString(90, __dbResults);
    this.D_DATA_INIZIO_EROG_BONUS = JdbcWritableBridge.readString(91, __dbResults);
    this.D_DATA_FINE_EROG_BONUS = JdbcWritableBridge.readString(92, __dbResults);
    this.D_DATA_RIF_BONUS = JdbcWritableBridge.readString(93, __dbResults);
    this.D_AGGIORNAMENTO_BONUS = JdbcWritableBridge.readString(94, __dbResults);
    this.D_DATA_AGGIORNAMENTO = JdbcWritableBridge.readString(95, __dbResults);
    this.N_ID_UDD = JdbcWritableBridge.readBigDecimal(96, __dbResults);
    this.N_ID_VENDITORE = JdbcWritableBridge.readBigDecimal(97, __dbResults);
    this.T_COD_PROFILO = JdbcWritableBridge.readString(98, __dbResults);
    this.T_COD_CAT_USO = JdbcWritableBridge.readString(99, __dbResults);
    this.T_COD_CLASSE_PRELIEVO = JdbcWritableBridge.readString(100, __dbResults);
    this.T_ANNO_TERMICO = JdbcWritableBridge.readString(101, __dbResults);
    this.D_DATA_RIF_PREL = JdbcWritableBridge.readString(102, __dbResults);
    this.T_TRATTAMENTO = JdbcWritableBridge.readString(103, __dbResults);
    this.T_TOPONIMO_ESAZ = JdbcWritableBridge.readString(104, __dbResults);
    this.T_NOMESTRADA_ESAZ = JdbcWritableBridge.readString(105, __dbResults);
    this.T_CIVICO_ESAZ = JdbcWritableBridge.readString(106, __dbResults);
    this.T_CAP_ESAZ = JdbcWritableBridge.readString(107, __dbResults);
    this.T_COMUNE_ISTAT_ESAZ = JdbcWritableBridge.readString(108, __dbResults);
    this.T_COMUNE_ESAZ = JdbcWritableBridge.readString(109, __dbResults);
    this.T_PROVINCIA_ESAZ = JdbcWritableBridge.readString(110, __dbResults);
    this.T_NAZIONE_ESAZ = JdbcWritableBridge.readString(111, __dbResults);
    this.ALTRO_IND_ESAZ = JdbcWritableBridge.readString(112, __dbResults);
    this.T_CODICE_ATECO = JdbcWritableBridge.readString(113, __dbResults);
    this.T_PAGAMENTO_IVA = JdbcWritableBridge.readString(114, __dbResults);
    this.T_CODICE_UFFICIO = JdbcWritableBridge.readString(115, __dbResults);
    this.T_CF_INTESTATARIO_FATT = JdbcWritableBridge.readString(116, __dbResults);
    this.T_CF_STRANIERO_FATT = JdbcWritableBridge.readString(117, __dbResults);
    this.T_PIVA_INTESTATARIO_FATT = JdbcWritableBridge.readString(118, __dbResults);
    this.T_NOME_INTESTATARIO_FATT = JdbcWritableBridge.readString(119, __dbResults);
    this.T_COGNOME_INTESTATARIO_FATT = JdbcWritableBridge.readString(120, __dbResults);
    this.T_RAG_SOC_INTESTATARIO_FATT = JdbcWritableBridge.readString(121, __dbResults);
    this.T_ANNO_MESE_RINN_BONUS = JdbcWritableBridge.readString(122, __dbResults);
    this.D_DATA_INIZIO_BONUS = JdbcWritableBridge.readString(123, __dbResults);
    this.D_DATA_FINE_BONUS = JdbcWritableBridge.readString(124, __dbResults);
    this.N_PRELIEVO_ANNUO = JdbcWritableBridge.readBigDecimal(125, __dbResults);
    this.T_FATTORE_CORREZ_CLIMATICA = JdbcWritableBridge.readString(126, __dbResults);
    this.T_ALTRO_IND_GESTCAL = JdbcWritableBridge.readString(127, __dbResults);
    this.T_TIPO_OP = JdbcWritableBridge.readString(128, __dbResults);
    this.T_PROCESSO = JdbcWritableBridge.readString(129, __dbResults);
    this.N_ID_PRATICA_PROCESSO = JdbcWritableBridge.readBigDecimal(130, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_PDR = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.T_CODICE_PDR = JdbcWritableBridge.readString(2, __dbResults);
    this.CAPACITA_TRASPORTO = JdbcWritableBridge.readString(3, __dbResults);
    this.MESE_VAL_CAP_TRASP = JdbcWritableBridge.readString(4, __dbResults);
    this.T_COD_TIPO_PDR = JdbcWritableBridge.readString(5, __dbResults);
    this.T_DISALIMENTABILITA = JdbcWritableBridge.readString(6, __dbResults);
    this.BILANCIAMENTO = JdbcWritableBridge.readString(7, __dbResults);
    this.N_ID_FORNITURA = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.D_DATA_INIZIO_FOR = JdbcWritableBridge.readString(9, __dbResults);
    this.DATA_FINE_FOR = JdbcWritableBridge.readString(10, __dbResults);
    this.N_ID_AZ_UDD = JdbcWritableBridge.readBigDecimal(11, __dbResults);
    this.PIVA_UDD = JdbcWritableBridge.readString(12, __dbResults);
    this.N_ID_AZ_CC = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.PIVA_CC = JdbcWritableBridge.readString(14, __dbResults);
    this.N_ID_CLIENTE = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.T_PARTITA_IVA_CLI = JdbcWritableBridge.readString(16, __dbResults);
    this.T_CODICE_FISCALE_CLI = JdbcWritableBridge.readString(17, __dbResults);
    this.B_CF_STRANIERO = JdbcWritableBridge.readString(18, __dbResults);
    this.T_REFERENTE = JdbcWritableBridge.readString(19, __dbResults);
    this.T_NOME_REF = JdbcWritableBridge.readString(20, __dbResults);
    this.T_COGNOME_REF = JdbcWritableBridge.readString(21, __dbResults);
    this.T_EMAIL_REF = JdbcWritableBridge.readString(22, __dbResults);
    this.T_TELEFONO_REF = JdbcWritableBridge.readString(23, __dbResults);
    this.T_RESIDENZA = JdbcWritableBridge.readString(24, __dbResults);
    this.DATA_VAL_RES = JdbcWritableBridge.readString(25, __dbResults);
    this.T_TOPONIMOPDR = JdbcWritableBridge.readString(26, __dbResults);
    this.T_NOMESTRADA_PDR = JdbcWritableBridge.readString(27, __dbResults);
    this.T_CIVICO_PDR = JdbcWritableBridge.readString(28, __dbResults);
    this.T_CAP_PDR = JdbcWritableBridge.readString(29, __dbResults);
    this.T_COMUNE_ISTAT_PDR = JdbcWritableBridge.readString(30, __dbResults);
    this.T_COMUNE_PDR = JdbcWritableBridge.readString(31, __dbResults);
    this.T_PROVINCIA_PDR = JdbcWritableBridge.readString(32, __dbResults);
    this.T_NAZIONE_PDR = JdbcWritableBridge.readString(33, __dbResults);
    this.ALTRO_IND_PDR = JdbcWritableBridge.readString(34, __dbResults);
    this.T_TOPONIMO_FORN = JdbcWritableBridge.readString(35, __dbResults);
    this.T_NOMESTRADA_FORN = JdbcWritableBridge.readString(36, __dbResults);
    this.T_CIVICO_FORN = JdbcWritableBridge.readString(37, __dbResults);
    this.T_CAP_FORN = JdbcWritableBridge.readString(38, __dbResults);
    this.T_COMUNE_ISTATFORN = JdbcWritableBridge.readString(39, __dbResults);
    this.T_COMUNE_FORN = JdbcWritableBridge.readString(40, __dbResults);
    this.T_PROVINCIA_FORN = JdbcWritableBridge.readString(41, __dbResults);
    this.T_NAZIONE_FORN = JdbcWritableBridge.readString(42, __dbResults);
    this.ALTRO_IND_FORN = JdbcWritableBridge.readString(43, __dbResults);
    this.T_ACCESSO_UI = JdbcWritableBridge.readString(44, __dbResults);
    this.T_TIPO_FORNITURA = JdbcWritableBridge.readString(45, __dbResults);
    this.T_ALIQUOTA_IVA = JdbcWritableBridge.readString(46, __dbResults);
    this.T_ALIQUOTA_ACCISE = JdbcWritableBridge.readString(47, __dbResults);
    this.T_ADD_REGIONALE = JdbcWritableBridge.readString(48, __dbResults);
    this.T_ALTRE_INFO_IMPOSTE = JdbcWritableBridge.readString(49, __dbResults);
    this.T_MATRICOLA_MISURATORE = JdbcWritableBridge.readString(50, __dbResults);
    this.T_CLASSE_MISURATORE = JdbcWritableBridge.readString(51, __dbResults);
    this.T_TIPO_MISURATORE = JdbcWritableBridge.readString(52, __dbResults);
    this.T_TELEGESTIONE = JdbcWritableBridge.readString(53, __dbResults);
    this.T_PRE_CONV = JdbcWritableBridge.readString(54, __dbResults);
    this.T_MATRICOLA_CONVERTITORE = JdbcWritableBridge.readString(55, __dbResults);
    this.N_NUM_CIFRE_CONVERTITORE = JdbcWritableBridge.readBigDecimal(56, __dbResults);
    this.T_ANNO_FABBRIC_CONVERTITORE = JdbcWritableBridge.readString(57, __dbResults);
    this.T_DATA_INST_CONVERTITORE = JdbcWritableBridge.readString(58, __dbResults);
    this.N_COEFF_CORREZIONE = JdbcWritableBridge.readBigDecimal(59, __dbResults);
    this.PRESS_MISURE = JdbcWritableBridge.readBigDecimal(60, __dbResults);
    this.T_ACCESS_MISURATORE = JdbcWritableBridge.readBigDecimal(61, __dbResults);
    this.N_NUM_CIFRE_MISURATORE = JdbcWritableBridge.readBigDecimal(62, __dbResults);
    this.T_ANNO_FABBRIC_MISURATORE = JdbcWritableBridge.readString(63, __dbResults);
    this.T_DATA_INST_MISURATORE = JdbcWritableBridge.readString(64, __dbResults);
    this.T_MISURATORE_INTEGRATO = JdbcWritableBridge.readString(65, __dbResults);
    this.N_POTENZIALITA_MASSIMA = JdbcWritableBridge.readBigDecimal(66, __dbResults);
    this.N_POTENZIALITA_TOT_INSTALLATA = JdbcWritableBridge.readBigDecimal(67, __dbResults);
    this.N_MAX_PRELIEVO_ORARIO = JdbcWritableBridge.readBigDecimal(68, __dbResults);
    this.T_EROG_SERVIZIO_ENERG = JdbcWritableBridge.readString(69, __dbResults);
    this.T_PARTITA_IVA_GESTCAL = JdbcWritableBridge.readString(70, __dbResults);
    this.T_RAGIONE_SOCIALE_GESTCAL = JdbcWritableBridge.readString(71, __dbResults);
    this.T_TELEFONO_GESTCAL = JdbcWritableBridge.readString(72, __dbResults);
    this.T_EMAIL_GESTCAL = JdbcWritableBridge.readString(73, __dbResults);
    this.T_TOPONIMO_GESTCAL = JdbcWritableBridge.readString(74, __dbResults);
    this.T_NOMESTRADA_GESTCAL = JdbcWritableBridge.readString(75, __dbResults);
    this.T_CIVICO_GESTCAL = JdbcWritableBridge.readString(76, __dbResults);
    this.T_CAP_GESTCAL = JdbcWritableBridge.readString(77, __dbResults);
    this.T_COMUNE_ISTAT_GESTCAL = JdbcWritableBridge.readString(78, __dbResults);
    this.T_COMUNE_GESTCAL = JdbcWritableBridge.readString(79, __dbResults);
    this.T_PROVINCIA_GESTCAL = JdbcWritableBridge.readString(80, __dbResults);
    this.T_NAZIONE_GESTCAL = JdbcWritableBridge.readString(81, __dbResults);
    this.D_DATA_RIF_PDR = JdbcWritableBridge.readString(82, __dbResults);
    this.D_AGGIORNAMENTO_PDR = JdbcWritableBridge.readString(83, __dbResults);
    this.D_DATA_RIF_TECN = JdbcWritableBridge.readString(84, __dbResults);
    this.D_AGGIORNAMENTO_TECN = JdbcWritableBridge.readString(85, __dbResults);
    this.D_DATA_RIF_MIS = JdbcWritableBridge.readString(86, __dbResults);
    this.D_AGGIORNAMENTO_MIS = JdbcWritableBridge.readString(87, __dbResults);
    this.D_DATA_RIF_FORN = JdbcWritableBridge.readString(88, __dbResults);
    this.D_AGGIORNAMENTO_FORN = JdbcWritableBridge.readString(89, __dbResults);
    this.T_TIPO_BONUS = JdbcWritableBridge.readString(90, __dbResults);
    this.D_DATA_INIZIO_EROG_BONUS = JdbcWritableBridge.readString(91, __dbResults);
    this.D_DATA_FINE_EROG_BONUS = JdbcWritableBridge.readString(92, __dbResults);
    this.D_DATA_RIF_BONUS = JdbcWritableBridge.readString(93, __dbResults);
    this.D_AGGIORNAMENTO_BONUS = JdbcWritableBridge.readString(94, __dbResults);
    this.D_DATA_AGGIORNAMENTO = JdbcWritableBridge.readString(95, __dbResults);
    this.N_ID_UDD = JdbcWritableBridge.readBigDecimal(96, __dbResults);
    this.N_ID_VENDITORE = JdbcWritableBridge.readBigDecimal(97, __dbResults);
    this.T_COD_PROFILO = JdbcWritableBridge.readString(98, __dbResults);
    this.T_COD_CAT_USO = JdbcWritableBridge.readString(99, __dbResults);
    this.T_COD_CLASSE_PRELIEVO = JdbcWritableBridge.readString(100, __dbResults);
    this.T_ANNO_TERMICO = JdbcWritableBridge.readString(101, __dbResults);
    this.D_DATA_RIF_PREL = JdbcWritableBridge.readString(102, __dbResults);
    this.T_TRATTAMENTO = JdbcWritableBridge.readString(103, __dbResults);
    this.T_TOPONIMO_ESAZ = JdbcWritableBridge.readString(104, __dbResults);
    this.T_NOMESTRADA_ESAZ = JdbcWritableBridge.readString(105, __dbResults);
    this.T_CIVICO_ESAZ = JdbcWritableBridge.readString(106, __dbResults);
    this.T_CAP_ESAZ = JdbcWritableBridge.readString(107, __dbResults);
    this.T_COMUNE_ISTAT_ESAZ = JdbcWritableBridge.readString(108, __dbResults);
    this.T_COMUNE_ESAZ = JdbcWritableBridge.readString(109, __dbResults);
    this.T_PROVINCIA_ESAZ = JdbcWritableBridge.readString(110, __dbResults);
    this.T_NAZIONE_ESAZ = JdbcWritableBridge.readString(111, __dbResults);
    this.ALTRO_IND_ESAZ = JdbcWritableBridge.readString(112, __dbResults);
    this.T_CODICE_ATECO = JdbcWritableBridge.readString(113, __dbResults);
    this.T_PAGAMENTO_IVA = JdbcWritableBridge.readString(114, __dbResults);
    this.T_CODICE_UFFICIO = JdbcWritableBridge.readString(115, __dbResults);
    this.T_CF_INTESTATARIO_FATT = JdbcWritableBridge.readString(116, __dbResults);
    this.T_CF_STRANIERO_FATT = JdbcWritableBridge.readString(117, __dbResults);
    this.T_PIVA_INTESTATARIO_FATT = JdbcWritableBridge.readString(118, __dbResults);
    this.T_NOME_INTESTATARIO_FATT = JdbcWritableBridge.readString(119, __dbResults);
    this.T_COGNOME_INTESTATARIO_FATT = JdbcWritableBridge.readString(120, __dbResults);
    this.T_RAG_SOC_INTESTATARIO_FATT = JdbcWritableBridge.readString(121, __dbResults);
    this.T_ANNO_MESE_RINN_BONUS = JdbcWritableBridge.readString(122, __dbResults);
    this.D_DATA_INIZIO_BONUS = JdbcWritableBridge.readString(123, __dbResults);
    this.D_DATA_FINE_BONUS = JdbcWritableBridge.readString(124, __dbResults);
    this.N_PRELIEVO_ANNUO = JdbcWritableBridge.readBigDecimal(125, __dbResults);
    this.T_FATTORE_CORREZ_CLIMATICA = JdbcWritableBridge.readString(126, __dbResults);
    this.T_ALTRO_IND_GESTCAL = JdbcWritableBridge.readString(127, __dbResults);
    this.T_TIPO_OP = JdbcWritableBridge.readString(128, __dbResults);
    this.T_PROCESSO = JdbcWritableBridge.readString(129, __dbResults);
    this.N_ID_PRATICA_PROCESSO = JdbcWritableBridge.readBigDecimal(130, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_PDR, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_PDR, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CAPACITA_TRASPORTO, 3 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MESE_VAL_CAP_TRASP, 4 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_TIPO_PDR, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DISALIMENTABILITA, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(BILANCIAMENTO, 7 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_FORNITURA, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INIZIO_FOR, 9 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(DATA_FINE_FOR, 10 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_AZ_UDD, 11 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PIVA_UDD, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_AZ_CC, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PIVA_CC, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CLIENTE, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_PARTITA_IVA_CLI, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_FISCALE_CLI, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_CF_STRANIERO, 18 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_REFERENTE, 19 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_NOME_REF, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COGNOME_REF, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_EMAIL_REF, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TELEFONO_REF, 23 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_RESIDENZA, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATA_VAL_RES, 25 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_TOPONIMOPDR, 26 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOMESTRADA_PDR, 27 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CIVICO_PDR, 28 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CAP_PDR, 29 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNE_ISTAT_PDR, 30 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNE_PDR, 31 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PROVINCIA_PDR, 32 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NAZIONE_PDR, 33 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ALTRO_IND_PDR, 34 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TOPONIMO_FORN, 35 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOMESTRADA_FORN, 36 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CIVICO_FORN, 37 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CAP_FORN, 38 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNE_ISTATFORN, 39 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNE_FORN, 40 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PROVINCIA_FORN, 41 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NAZIONE_FORN, 42 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ALTRO_IND_FORN, 43 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ACCESSO_UI, 44 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_FORNITURA, 45 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ALIQUOTA_IVA, 46 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ALIQUOTA_ACCISE, 47 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ADD_REGIONALE, 48 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ALTRE_INFO_IMPOSTE, 49 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MATRICOLA_MISURATORE, 50 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CLASSE_MISURATORE, 51 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_MISURATORE, 52 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TELEGESTIONE, 53 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PRE_CONV, 54 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MATRICOLA_CONVERTITORE, 55 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_CIFRE_CONVERTITORE, 56 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO_FABBRIC_CONVERTITORE, 57 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DATA_INST_CONVERTITORE, 58 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_COEFF_CORREZIONE, 59 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(PRESS_MISURE, 60 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(T_ACCESS_MISURATORE, 61 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_CIFRE_MISURATORE, 62 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO_FABBRIC_MISURATORE, 63 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DATA_INST_MISURATORE, 64 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MISURATORE_INTEGRATO, 65 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_POTENZIALITA_MASSIMA, 66 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_POTENZIALITA_TOT_INSTALLATA, 67 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_MAX_PRELIEVO_ORARIO, 68 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_EROG_SERVIZIO_ENERG, 69 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_PARTITA_IVA_GESTCAL, 70 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_RAGIONE_SOCIALE_GESTCAL, 71 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TELEFONO_GESTCAL, 72 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_EMAIL_GESTCAL, 73 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TOPONIMO_GESTCAL, 74 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOMESTRADA_GESTCAL, 75 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CIVICO_GESTCAL, 76 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CAP_GESTCAL, 77 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNE_ISTAT_GESTCAL, 78 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNE_GESTCAL, 79 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PROVINCIA_GESTCAL, 80 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NAZIONE_GESTCAL, 81 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF_PDR, 82 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO_PDR, 83 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF_TECN, 84 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO_TECN, 85 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF_MIS, 86 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO_MIS, 87 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF_FORN, 88 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO_FORN, 89 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_BONUS, 90 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INIZIO_EROG_BONUS, 91 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_FINE_EROG_BONUS, 92 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF_BONUS, 93 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO_BONUS, 94 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_AGGIORNAMENTO, 95 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UDD, 96 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_VENDITORE, 97 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_PROFILO, 98 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CAT_USO, 99 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CLASSE_PRELIEVO, 100 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO_TERMICO, 101 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF_PREL, 102 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_TRATTAMENTO, 103 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TOPONIMO_ESAZ, 104 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOMESTRADA_ESAZ, 105 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CIVICO_ESAZ, 106 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CAP_ESAZ, 107 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNE_ISTAT_ESAZ, 108 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNE_ESAZ, 109 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PROVINCIA_ESAZ, 110 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NAZIONE_ESAZ, 111 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ALTRO_IND_ESAZ, 112 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_ATECO, 113 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PAGAMENTO_IVA, 114 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_UFFICIO, 115 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CF_INTESTATARIO_FATT, 116 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CF_STRANIERO_FATT, 117 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA_INTESTATARIO_FATT, 118 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOME_INTESTATARIO_FATT, 119 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COGNOME_INTESTATARIO_FATT, 120 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_RAG_SOC_INTESTATARIO_FATT, 121 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO_MESE_RINN_BONUS, 122 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INIZIO_BONUS, 123 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_FINE_BONUS, 124 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_PRELIEVO_ANNUO, 125 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_FATTORE_CORREZ_CLIMATICA, 126 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ALTRO_IND_GESTCAL, 127 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_OP, 128 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PROCESSO, 129 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PRATICA_PROCESSO, 130 + __off, 2, __dbStmt);
    return 130;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_PDR, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_PDR, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CAPACITA_TRASPORTO, 3 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MESE_VAL_CAP_TRASP, 4 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_TIPO_PDR, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DISALIMENTABILITA, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(BILANCIAMENTO, 7 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_FORNITURA, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INIZIO_FOR, 9 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(DATA_FINE_FOR, 10 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_AZ_UDD, 11 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PIVA_UDD, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_AZ_CC, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PIVA_CC, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CLIENTE, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_PARTITA_IVA_CLI, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_FISCALE_CLI, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_CF_STRANIERO, 18 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_REFERENTE, 19 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_NOME_REF, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COGNOME_REF, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_EMAIL_REF, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TELEFONO_REF, 23 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_RESIDENZA, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATA_VAL_RES, 25 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_TOPONIMOPDR, 26 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOMESTRADA_PDR, 27 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CIVICO_PDR, 28 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CAP_PDR, 29 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNE_ISTAT_PDR, 30 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNE_PDR, 31 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PROVINCIA_PDR, 32 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NAZIONE_PDR, 33 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ALTRO_IND_PDR, 34 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TOPONIMO_FORN, 35 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOMESTRADA_FORN, 36 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CIVICO_FORN, 37 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CAP_FORN, 38 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNE_ISTATFORN, 39 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNE_FORN, 40 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PROVINCIA_FORN, 41 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NAZIONE_FORN, 42 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ALTRO_IND_FORN, 43 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ACCESSO_UI, 44 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_FORNITURA, 45 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ALIQUOTA_IVA, 46 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ALIQUOTA_ACCISE, 47 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ADD_REGIONALE, 48 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ALTRE_INFO_IMPOSTE, 49 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MATRICOLA_MISURATORE, 50 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CLASSE_MISURATORE, 51 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_MISURATORE, 52 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TELEGESTIONE, 53 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PRE_CONV, 54 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MATRICOLA_CONVERTITORE, 55 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_CIFRE_CONVERTITORE, 56 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO_FABBRIC_CONVERTITORE, 57 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DATA_INST_CONVERTITORE, 58 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_COEFF_CORREZIONE, 59 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(PRESS_MISURE, 60 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(T_ACCESS_MISURATORE, 61 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_CIFRE_MISURATORE, 62 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO_FABBRIC_MISURATORE, 63 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DATA_INST_MISURATORE, 64 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MISURATORE_INTEGRATO, 65 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_POTENZIALITA_MASSIMA, 66 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_POTENZIALITA_TOT_INSTALLATA, 67 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_MAX_PRELIEVO_ORARIO, 68 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_EROG_SERVIZIO_ENERG, 69 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_PARTITA_IVA_GESTCAL, 70 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_RAGIONE_SOCIALE_GESTCAL, 71 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TELEFONO_GESTCAL, 72 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_EMAIL_GESTCAL, 73 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TOPONIMO_GESTCAL, 74 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOMESTRADA_GESTCAL, 75 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CIVICO_GESTCAL, 76 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CAP_GESTCAL, 77 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNE_ISTAT_GESTCAL, 78 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNE_GESTCAL, 79 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PROVINCIA_GESTCAL, 80 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NAZIONE_GESTCAL, 81 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF_PDR, 82 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO_PDR, 83 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF_TECN, 84 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO_TECN, 85 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF_MIS, 86 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO_MIS, 87 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF_FORN, 88 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO_FORN, 89 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_BONUS, 90 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INIZIO_EROG_BONUS, 91 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_FINE_EROG_BONUS, 92 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF_BONUS, 93 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO_BONUS, 94 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_AGGIORNAMENTO, 95 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UDD, 96 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_VENDITORE, 97 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_PROFILO, 98 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CAT_USO, 99 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CLASSE_PRELIEVO, 100 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO_TERMICO, 101 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF_PREL, 102 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_TRATTAMENTO, 103 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TOPONIMO_ESAZ, 104 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOMESTRADA_ESAZ, 105 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CIVICO_ESAZ, 106 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CAP_ESAZ, 107 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNE_ISTAT_ESAZ, 108 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNE_ESAZ, 109 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PROVINCIA_ESAZ, 110 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NAZIONE_ESAZ, 111 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ALTRO_IND_ESAZ, 112 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_ATECO, 113 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PAGAMENTO_IVA, 114 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_UFFICIO, 115 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CF_INTESTATARIO_FATT, 116 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CF_STRANIERO_FATT, 117 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA_INTESTATARIO_FATT, 118 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOME_INTESTATARIO_FATT, 119 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COGNOME_INTESTATARIO_FATT, 120 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_RAG_SOC_INTESTATARIO_FATT, 121 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO_MESE_RINN_BONUS, 122 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INIZIO_BONUS, 123 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_FINE_BONUS, 124 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_PRELIEVO_ANNUO, 125 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_FATTORE_CORREZ_CLIMATICA, 126 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ALTRO_IND_GESTCAL, 127 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_OP, 128 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PROCESSO, 129 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PRATICA_PROCESSO, 130 + __off, 2, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_PDR = null;
    } else {
    this.N_ID_PDR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_PDR = null;
    } else {
    this.T_CODICE_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CAPACITA_TRASPORTO = null;
    } else {
    this.CAPACITA_TRASPORTO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MESE_VAL_CAP_TRASP = null;
    } else {
    this.MESE_VAL_CAP_TRASP = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COD_TIPO_PDR = null;
    } else {
    this.T_COD_TIPO_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_DISALIMENTABILITA = null;
    } else {
    this.T_DISALIMENTABILITA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BILANCIAMENTO = null;
    } else {
    this.BILANCIAMENTO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_FORNITURA = null;
    } else {
    this.N_ID_FORNITURA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_INIZIO_FOR = null;
    } else {
    this.D_DATA_INIZIO_FOR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATA_FINE_FOR = null;
    } else {
    this.DATA_FINE_FOR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_AZ_UDD = null;
    } else {
    this.N_ID_AZ_UDD = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PIVA_UDD = null;
    } else {
    this.PIVA_UDD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_AZ_CC = null;
    } else {
    this.N_ID_AZ_CC = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PIVA_CC = null;
    } else {
    this.PIVA_CC = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_CLIENTE = null;
    } else {
    this.N_ID_CLIENTE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PARTITA_IVA_CLI = null;
    } else {
    this.T_PARTITA_IVA_CLI = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_FISCALE_CLI = null;
    } else {
    this.T_CODICE_FISCALE_CLI = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_CF_STRANIERO = null;
    } else {
    this.B_CF_STRANIERO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_REFERENTE = null;
    } else {
    this.T_REFERENTE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_NOME_REF = null;
    } else {
    this.T_NOME_REF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COGNOME_REF = null;
    } else {
    this.T_COGNOME_REF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_EMAIL_REF = null;
    } else {
    this.T_EMAIL_REF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TELEFONO_REF = null;
    } else {
    this.T_TELEFONO_REF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_RESIDENZA = null;
    } else {
    this.T_RESIDENZA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATA_VAL_RES = null;
    } else {
    this.DATA_VAL_RES = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TOPONIMOPDR = null;
    } else {
    this.T_TOPONIMOPDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_NOMESTRADA_PDR = null;
    } else {
    this.T_NOMESTRADA_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CIVICO_PDR = null;
    } else {
    this.T_CIVICO_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CAP_PDR = null;
    } else {
    this.T_CAP_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COMUNE_ISTAT_PDR = null;
    } else {
    this.T_COMUNE_ISTAT_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COMUNE_PDR = null;
    } else {
    this.T_COMUNE_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PROVINCIA_PDR = null;
    } else {
    this.T_PROVINCIA_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_NAZIONE_PDR = null;
    } else {
    this.T_NAZIONE_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ALTRO_IND_PDR = null;
    } else {
    this.ALTRO_IND_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TOPONIMO_FORN = null;
    } else {
    this.T_TOPONIMO_FORN = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_NOMESTRADA_FORN = null;
    } else {
    this.T_NOMESTRADA_FORN = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CIVICO_FORN = null;
    } else {
    this.T_CIVICO_FORN = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CAP_FORN = null;
    } else {
    this.T_CAP_FORN = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COMUNE_ISTATFORN = null;
    } else {
    this.T_COMUNE_ISTATFORN = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COMUNE_FORN = null;
    } else {
    this.T_COMUNE_FORN = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PROVINCIA_FORN = null;
    } else {
    this.T_PROVINCIA_FORN = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_NAZIONE_FORN = null;
    } else {
    this.T_NAZIONE_FORN = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ALTRO_IND_FORN = null;
    } else {
    this.ALTRO_IND_FORN = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ACCESSO_UI = null;
    } else {
    this.T_ACCESSO_UI = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO_FORNITURA = null;
    } else {
    this.T_TIPO_FORNITURA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ALIQUOTA_IVA = null;
    } else {
    this.T_ALIQUOTA_IVA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ALIQUOTA_ACCISE = null;
    } else {
    this.T_ALIQUOTA_ACCISE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ADD_REGIONALE = null;
    } else {
    this.T_ADD_REGIONALE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ALTRE_INFO_IMPOSTE = null;
    } else {
    this.T_ALTRE_INFO_IMPOSTE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MATRICOLA_MISURATORE = null;
    } else {
    this.T_MATRICOLA_MISURATORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CLASSE_MISURATORE = null;
    } else {
    this.T_CLASSE_MISURATORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO_MISURATORE = null;
    } else {
    this.T_TIPO_MISURATORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TELEGESTIONE = null;
    } else {
    this.T_TELEGESTIONE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PRE_CONV = null;
    } else {
    this.T_PRE_CONV = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MATRICOLA_CONVERTITORE = null;
    } else {
    this.T_MATRICOLA_CONVERTITORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_NUM_CIFRE_CONVERTITORE = null;
    } else {
    this.N_NUM_CIFRE_CONVERTITORE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ANNO_FABBRIC_CONVERTITORE = null;
    } else {
    this.T_ANNO_FABBRIC_CONVERTITORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_DATA_INST_CONVERTITORE = null;
    } else {
    this.T_DATA_INST_CONVERTITORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_COEFF_CORREZIONE = null;
    } else {
    this.N_COEFF_CORREZIONE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PRESS_MISURE = null;
    } else {
    this.PRESS_MISURE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ACCESS_MISURATORE = null;
    } else {
    this.T_ACCESS_MISURATORE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_NUM_CIFRE_MISURATORE = null;
    } else {
    this.N_NUM_CIFRE_MISURATORE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ANNO_FABBRIC_MISURATORE = null;
    } else {
    this.T_ANNO_FABBRIC_MISURATORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_DATA_INST_MISURATORE = null;
    } else {
    this.T_DATA_INST_MISURATORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MISURATORE_INTEGRATO = null;
    } else {
    this.T_MISURATORE_INTEGRATO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_POTENZIALITA_MASSIMA = null;
    } else {
    this.N_POTENZIALITA_MASSIMA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_POTENZIALITA_TOT_INSTALLATA = null;
    } else {
    this.N_POTENZIALITA_TOT_INSTALLATA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_MAX_PRELIEVO_ORARIO = null;
    } else {
    this.N_MAX_PRELIEVO_ORARIO = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_EROG_SERVIZIO_ENERG = null;
    } else {
    this.T_EROG_SERVIZIO_ENERG = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PARTITA_IVA_GESTCAL = null;
    } else {
    this.T_PARTITA_IVA_GESTCAL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_RAGIONE_SOCIALE_GESTCAL = null;
    } else {
    this.T_RAGIONE_SOCIALE_GESTCAL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TELEFONO_GESTCAL = null;
    } else {
    this.T_TELEFONO_GESTCAL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_EMAIL_GESTCAL = null;
    } else {
    this.T_EMAIL_GESTCAL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TOPONIMO_GESTCAL = null;
    } else {
    this.T_TOPONIMO_GESTCAL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_NOMESTRADA_GESTCAL = null;
    } else {
    this.T_NOMESTRADA_GESTCAL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CIVICO_GESTCAL = null;
    } else {
    this.T_CIVICO_GESTCAL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CAP_GESTCAL = null;
    } else {
    this.T_CAP_GESTCAL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COMUNE_ISTAT_GESTCAL = null;
    } else {
    this.T_COMUNE_ISTAT_GESTCAL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COMUNE_GESTCAL = null;
    } else {
    this.T_COMUNE_GESTCAL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PROVINCIA_GESTCAL = null;
    } else {
    this.T_PROVINCIA_GESTCAL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_NAZIONE_GESTCAL = null;
    } else {
    this.T_NAZIONE_GESTCAL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_RIF_PDR = null;
    } else {
    this.D_DATA_RIF_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_AGGIORNAMENTO_PDR = null;
    } else {
    this.D_AGGIORNAMENTO_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_RIF_TECN = null;
    } else {
    this.D_DATA_RIF_TECN = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_AGGIORNAMENTO_TECN = null;
    } else {
    this.D_AGGIORNAMENTO_TECN = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_RIF_MIS = null;
    } else {
    this.D_DATA_RIF_MIS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_AGGIORNAMENTO_MIS = null;
    } else {
    this.D_AGGIORNAMENTO_MIS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_RIF_FORN = null;
    } else {
    this.D_DATA_RIF_FORN = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_AGGIORNAMENTO_FORN = null;
    } else {
    this.D_AGGIORNAMENTO_FORN = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO_BONUS = null;
    } else {
    this.T_TIPO_BONUS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_INIZIO_EROG_BONUS = null;
    } else {
    this.D_DATA_INIZIO_EROG_BONUS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_FINE_EROG_BONUS = null;
    } else {
    this.D_DATA_FINE_EROG_BONUS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_RIF_BONUS = null;
    } else {
    this.D_DATA_RIF_BONUS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_AGGIORNAMENTO_BONUS = null;
    } else {
    this.D_AGGIORNAMENTO_BONUS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_AGGIORNAMENTO = null;
    } else {
    this.D_DATA_AGGIORNAMENTO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_UDD = null;
    } else {
    this.N_ID_UDD = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_VENDITORE = null;
    } else {
    this.N_ID_VENDITORE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COD_PROFILO = null;
    } else {
    this.T_COD_PROFILO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COD_CAT_USO = null;
    } else {
    this.T_COD_CAT_USO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COD_CLASSE_PRELIEVO = null;
    } else {
    this.T_COD_CLASSE_PRELIEVO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ANNO_TERMICO = null;
    } else {
    this.T_ANNO_TERMICO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_RIF_PREL = null;
    } else {
    this.D_DATA_RIF_PREL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TRATTAMENTO = null;
    } else {
    this.T_TRATTAMENTO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TOPONIMO_ESAZ = null;
    } else {
    this.T_TOPONIMO_ESAZ = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_NOMESTRADA_ESAZ = null;
    } else {
    this.T_NOMESTRADA_ESAZ = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CIVICO_ESAZ = null;
    } else {
    this.T_CIVICO_ESAZ = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CAP_ESAZ = null;
    } else {
    this.T_CAP_ESAZ = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COMUNE_ISTAT_ESAZ = null;
    } else {
    this.T_COMUNE_ISTAT_ESAZ = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COMUNE_ESAZ = null;
    } else {
    this.T_COMUNE_ESAZ = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PROVINCIA_ESAZ = null;
    } else {
    this.T_PROVINCIA_ESAZ = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_NAZIONE_ESAZ = null;
    } else {
    this.T_NAZIONE_ESAZ = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ALTRO_IND_ESAZ = null;
    } else {
    this.ALTRO_IND_ESAZ = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_ATECO = null;
    } else {
    this.T_CODICE_ATECO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PAGAMENTO_IVA = null;
    } else {
    this.T_PAGAMENTO_IVA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_UFFICIO = null;
    } else {
    this.T_CODICE_UFFICIO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CF_INTESTATARIO_FATT = null;
    } else {
    this.T_CF_INTESTATARIO_FATT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CF_STRANIERO_FATT = null;
    } else {
    this.T_CF_STRANIERO_FATT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PIVA_INTESTATARIO_FATT = null;
    } else {
    this.T_PIVA_INTESTATARIO_FATT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_NOME_INTESTATARIO_FATT = null;
    } else {
    this.T_NOME_INTESTATARIO_FATT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COGNOME_INTESTATARIO_FATT = null;
    } else {
    this.T_COGNOME_INTESTATARIO_FATT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_RAG_SOC_INTESTATARIO_FATT = null;
    } else {
    this.T_RAG_SOC_INTESTATARIO_FATT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ANNO_MESE_RINN_BONUS = null;
    } else {
    this.T_ANNO_MESE_RINN_BONUS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_INIZIO_BONUS = null;
    } else {
    this.D_DATA_INIZIO_BONUS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_FINE_BONUS = null;
    } else {
    this.D_DATA_FINE_BONUS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_PRELIEVO_ANNUO = null;
    } else {
    this.N_PRELIEVO_ANNUO = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_FATTORE_CORREZ_CLIMATICA = null;
    } else {
    this.T_FATTORE_CORREZ_CLIMATICA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ALTRO_IND_GESTCAL = null;
    } else {
    this.T_ALTRO_IND_GESTCAL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO_OP = null;
    } else {
    this.T_TIPO_OP = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PROCESSO = null;
    } else {
    this.T_PROCESSO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_PRATICA_PROCESSO = null;
    } else {
    this.N_ID_PRATICA_PROCESSO = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PDR, __dataOut);
    }
    if (null == this.T_CODICE_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_PDR);
    }
    if (null == this.CAPACITA_TRASPORTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CAPACITA_TRASPORTO);
    }
    if (null == this.MESE_VAL_CAP_TRASP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MESE_VAL_CAP_TRASP);
    }
    if (null == this.T_COD_TIPO_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_TIPO_PDR);
    }
    if (null == this.T_DISALIMENTABILITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DISALIMENTABILITA);
    }
    if (null == this.BILANCIAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILANCIAMENTO);
    }
    if (null == this.N_ID_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FORNITURA, __dataOut);
    }
    if (null == this.D_DATA_INIZIO_FOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INIZIO_FOR);
    }
    if (null == this.DATA_FINE_FOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_FINE_FOR);
    }
    if (null == this.N_ID_AZ_UDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_AZ_UDD, __dataOut);
    }
    if (null == this.PIVA_UDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PIVA_UDD);
    }
    if (null == this.N_ID_AZ_CC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_AZ_CC, __dataOut);
    }
    if (null == this.PIVA_CC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PIVA_CC);
    }
    if (null == this.N_ID_CLIENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CLIENTE, __dataOut);
    }
    if (null == this.T_PARTITA_IVA_CLI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PARTITA_IVA_CLI);
    }
    if (null == this.T_CODICE_FISCALE_CLI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_FISCALE_CLI);
    }
    if (null == this.B_CF_STRANIERO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_CF_STRANIERO);
    }
    if (null == this.T_REFERENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_REFERENTE);
    }
    if (null == this.T_NOME_REF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOME_REF);
    }
    if (null == this.T_COGNOME_REF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COGNOME_REF);
    }
    if (null == this.T_EMAIL_REF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_EMAIL_REF);
    }
    if (null == this.T_TELEFONO_REF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TELEFONO_REF);
    }
    if (null == this.T_RESIDENZA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RESIDENZA);
    }
    if (null == this.DATA_VAL_RES) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_VAL_RES);
    }
    if (null == this.T_TOPONIMOPDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TOPONIMOPDR);
    }
    if (null == this.T_NOMESTRADA_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOMESTRADA_PDR);
    }
    if (null == this.T_CIVICO_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CIVICO_PDR);
    }
    if (null == this.T_CAP_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CAP_PDR);
    }
    if (null == this.T_COMUNE_ISTAT_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNE_ISTAT_PDR);
    }
    if (null == this.T_COMUNE_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNE_PDR);
    }
    if (null == this.T_PROVINCIA_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PROVINCIA_PDR);
    }
    if (null == this.T_NAZIONE_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NAZIONE_PDR);
    }
    if (null == this.ALTRO_IND_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ALTRO_IND_PDR);
    }
    if (null == this.T_TOPONIMO_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TOPONIMO_FORN);
    }
    if (null == this.T_NOMESTRADA_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOMESTRADA_FORN);
    }
    if (null == this.T_CIVICO_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CIVICO_FORN);
    }
    if (null == this.T_CAP_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CAP_FORN);
    }
    if (null == this.T_COMUNE_ISTATFORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNE_ISTATFORN);
    }
    if (null == this.T_COMUNE_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNE_FORN);
    }
    if (null == this.T_PROVINCIA_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PROVINCIA_FORN);
    }
    if (null == this.T_NAZIONE_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NAZIONE_FORN);
    }
    if (null == this.ALTRO_IND_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ALTRO_IND_FORN);
    }
    if (null == this.T_ACCESSO_UI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ACCESSO_UI);
    }
    if (null == this.T_TIPO_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_FORNITURA);
    }
    if (null == this.T_ALIQUOTA_IVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ALIQUOTA_IVA);
    }
    if (null == this.T_ALIQUOTA_ACCISE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ALIQUOTA_ACCISE);
    }
    if (null == this.T_ADD_REGIONALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ADD_REGIONALE);
    }
    if (null == this.T_ALTRE_INFO_IMPOSTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ALTRE_INFO_IMPOSTE);
    }
    if (null == this.T_MATRICOLA_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MATRICOLA_MISURATORE);
    }
    if (null == this.T_CLASSE_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CLASSE_MISURATORE);
    }
    if (null == this.T_TIPO_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_MISURATORE);
    }
    if (null == this.T_TELEGESTIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TELEGESTIONE);
    }
    if (null == this.T_PRE_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PRE_CONV);
    }
    if (null == this.T_MATRICOLA_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MATRICOLA_CONVERTITORE);
    }
    if (null == this.N_NUM_CIFRE_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_CIFRE_CONVERTITORE, __dataOut);
    }
    if (null == this.T_ANNO_FABBRIC_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO_FABBRIC_CONVERTITORE);
    }
    if (null == this.T_DATA_INST_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DATA_INST_CONVERTITORE);
    }
    if (null == this.N_COEFF_CORREZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_COEFF_CORREZIONE, __dataOut);
    }
    if (null == this.PRESS_MISURE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.PRESS_MISURE, __dataOut);
    }
    if (null == this.T_ACCESS_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.T_ACCESS_MISURATORE, __dataOut);
    }
    if (null == this.N_NUM_CIFRE_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_CIFRE_MISURATORE, __dataOut);
    }
    if (null == this.T_ANNO_FABBRIC_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO_FABBRIC_MISURATORE);
    }
    if (null == this.T_DATA_INST_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DATA_INST_MISURATORE);
    }
    if (null == this.T_MISURATORE_INTEGRATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MISURATORE_INTEGRATO);
    }
    if (null == this.N_POTENZIALITA_MASSIMA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_POTENZIALITA_MASSIMA, __dataOut);
    }
    if (null == this.N_POTENZIALITA_TOT_INSTALLATA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_POTENZIALITA_TOT_INSTALLATA, __dataOut);
    }
    if (null == this.N_MAX_PRELIEVO_ORARIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_MAX_PRELIEVO_ORARIO, __dataOut);
    }
    if (null == this.T_EROG_SERVIZIO_ENERG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_EROG_SERVIZIO_ENERG);
    }
    if (null == this.T_PARTITA_IVA_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PARTITA_IVA_GESTCAL);
    }
    if (null == this.T_RAGIONE_SOCIALE_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RAGIONE_SOCIALE_GESTCAL);
    }
    if (null == this.T_TELEFONO_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TELEFONO_GESTCAL);
    }
    if (null == this.T_EMAIL_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_EMAIL_GESTCAL);
    }
    if (null == this.T_TOPONIMO_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TOPONIMO_GESTCAL);
    }
    if (null == this.T_NOMESTRADA_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOMESTRADA_GESTCAL);
    }
    if (null == this.T_CIVICO_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CIVICO_GESTCAL);
    }
    if (null == this.T_CAP_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CAP_GESTCAL);
    }
    if (null == this.T_COMUNE_ISTAT_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNE_ISTAT_GESTCAL);
    }
    if (null == this.T_COMUNE_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNE_GESTCAL);
    }
    if (null == this.T_PROVINCIA_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PROVINCIA_GESTCAL);
    }
    if (null == this.T_NAZIONE_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NAZIONE_GESTCAL);
    }
    if (null == this.D_DATA_RIF_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF_PDR);
    }
    if (null == this.D_AGGIORNAMENTO_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_AGGIORNAMENTO_PDR);
    }
    if (null == this.D_DATA_RIF_TECN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF_TECN);
    }
    if (null == this.D_AGGIORNAMENTO_TECN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_AGGIORNAMENTO_TECN);
    }
    if (null == this.D_DATA_RIF_MIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF_MIS);
    }
    if (null == this.D_AGGIORNAMENTO_MIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_AGGIORNAMENTO_MIS);
    }
    if (null == this.D_DATA_RIF_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF_FORN);
    }
    if (null == this.D_AGGIORNAMENTO_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_AGGIORNAMENTO_FORN);
    }
    if (null == this.T_TIPO_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_BONUS);
    }
    if (null == this.D_DATA_INIZIO_EROG_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INIZIO_EROG_BONUS);
    }
    if (null == this.D_DATA_FINE_EROG_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_FINE_EROG_BONUS);
    }
    if (null == this.D_DATA_RIF_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF_BONUS);
    }
    if (null == this.D_AGGIORNAMENTO_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_AGGIORNAMENTO_BONUS);
    }
    if (null == this.D_DATA_AGGIORNAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_AGGIORNAMENTO);
    }
    if (null == this.N_ID_UDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UDD, __dataOut);
    }
    if (null == this.N_ID_VENDITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_VENDITORE, __dataOut);
    }
    if (null == this.T_COD_PROFILO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_PROFILO);
    }
    if (null == this.T_COD_CAT_USO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_CAT_USO);
    }
    if (null == this.T_COD_CLASSE_PRELIEVO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_CLASSE_PRELIEVO);
    }
    if (null == this.T_ANNO_TERMICO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO_TERMICO);
    }
    if (null == this.D_DATA_RIF_PREL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF_PREL);
    }
    if (null == this.T_TRATTAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TRATTAMENTO);
    }
    if (null == this.T_TOPONIMO_ESAZ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TOPONIMO_ESAZ);
    }
    if (null == this.T_NOMESTRADA_ESAZ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOMESTRADA_ESAZ);
    }
    if (null == this.T_CIVICO_ESAZ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CIVICO_ESAZ);
    }
    if (null == this.T_CAP_ESAZ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CAP_ESAZ);
    }
    if (null == this.T_COMUNE_ISTAT_ESAZ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNE_ISTAT_ESAZ);
    }
    if (null == this.T_COMUNE_ESAZ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNE_ESAZ);
    }
    if (null == this.T_PROVINCIA_ESAZ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PROVINCIA_ESAZ);
    }
    if (null == this.T_NAZIONE_ESAZ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NAZIONE_ESAZ);
    }
    if (null == this.ALTRO_IND_ESAZ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ALTRO_IND_ESAZ);
    }
    if (null == this.T_CODICE_ATECO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_ATECO);
    }
    if (null == this.T_PAGAMENTO_IVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PAGAMENTO_IVA);
    }
    if (null == this.T_CODICE_UFFICIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_UFFICIO);
    }
    if (null == this.T_CF_INTESTATARIO_FATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CF_INTESTATARIO_FATT);
    }
    if (null == this.T_CF_STRANIERO_FATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CF_STRANIERO_FATT);
    }
    if (null == this.T_PIVA_INTESTATARIO_FATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA_INTESTATARIO_FATT);
    }
    if (null == this.T_NOME_INTESTATARIO_FATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOME_INTESTATARIO_FATT);
    }
    if (null == this.T_COGNOME_INTESTATARIO_FATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COGNOME_INTESTATARIO_FATT);
    }
    if (null == this.T_RAG_SOC_INTESTATARIO_FATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RAG_SOC_INTESTATARIO_FATT);
    }
    if (null == this.T_ANNO_MESE_RINN_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO_MESE_RINN_BONUS);
    }
    if (null == this.D_DATA_INIZIO_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INIZIO_BONUS);
    }
    if (null == this.D_DATA_FINE_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_FINE_BONUS);
    }
    if (null == this.N_PRELIEVO_ANNUO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_PRELIEVO_ANNUO, __dataOut);
    }
    if (null == this.T_FATTORE_CORREZ_CLIMATICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_FATTORE_CORREZ_CLIMATICA);
    }
    if (null == this.T_ALTRO_IND_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ALTRO_IND_GESTCAL);
    }
    if (null == this.T_TIPO_OP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_OP);
    }
    if (null == this.T_PROCESSO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PROCESSO);
    }
    if (null == this.N_ID_PRATICA_PROCESSO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PRATICA_PROCESSO, __dataOut);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PDR, __dataOut);
    }
    if (null == this.T_CODICE_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_PDR);
    }
    if (null == this.CAPACITA_TRASPORTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CAPACITA_TRASPORTO);
    }
    if (null == this.MESE_VAL_CAP_TRASP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MESE_VAL_CAP_TRASP);
    }
    if (null == this.T_COD_TIPO_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_TIPO_PDR);
    }
    if (null == this.T_DISALIMENTABILITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DISALIMENTABILITA);
    }
    if (null == this.BILANCIAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILANCIAMENTO);
    }
    if (null == this.N_ID_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FORNITURA, __dataOut);
    }
    if (null == this.D_DATA_INIZIO_FOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INIZIO_FOR);
    }
    if (null == this.DATA_FINE_FOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_FINE_FOR);
    }
    if (null == this.N_ID_AZ_UDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_AZ_UDD, __dataOut);
    }
    if (null == this.PIVA_UDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PIVA_UDD);
    }
    if (null == this.N_ID_AZ_CC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_AZ_CC, __dataOut);
    }
    if (null == this.PIVA_CC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PIVA_CC);
    }
    if (null == this.N_ID_CLIENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CLIENTE, __dataOut);
    }
    if (null == this.T_PARTITA_IVA_CLI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PARTITA_IVA_CLI);
    }
    if (null == this.T_CODICE_FISCALE_CLI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_FISCALE_CLI);
    }
    if (null == this.B_CF_STRANIERO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_CF_STRANIERO);
    }
    if (null == this.T_REFERENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_REFERENTE);
    }
    if (null == this.T_NOME_REF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOME_REF);
    }
    if (null == this.T_COGNOME_REF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COGNOME_REF);
    }
    if (null == this.T_EMAIL_REF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_EMAIL_REF);
    }
    if (null == this.T_TELEFONO_REF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TELEFONO_REF);
    }
    if (null == this.T_RESIDENZA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RESIDENZA);
    }
    if (null == this.DATA_VAL_RES) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_VAL_RES);
    }
    if (null == this.T_TOPONIMOPDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TOPONIMOPDR);
    }
    if (null == this.T_NOMESTRADA_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOMESTRADA_PDR);
    }
    if (null == this.T_CIVICO_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CIVICO_PDR);
    }
    if (null == this.T_CAP_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CAP_PDR);
    }
    if (null == this.T_COMUNE_ISTAT_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNE_ISTAT_PDR);
    }
    if (null == this.T_COMUNE_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNE_PDR);
    }
    if (null == this.T_PROVINCIA_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PROVINCIA_PDR);
    }
    if (null == this.T_NAZIONE_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NAZIONE_PDR);
    }
    if (null == this.ALTRO_IND_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ALTRO_IND_PDR);
    }
    if (null == this.T_TOPONIMO_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TOPONIMO_FORN);
    }
    if (null == this.T_NOMESTRADA_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOMESTRADA_FORN);
    }
    if (null == this.T_CIVICO_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CIVICO_FORN);
    }
    if (null == this.T_CAP_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CAP_FORN);
    }
    if (null == this.T_COMUNE_ISTATFORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNE_ISTATFORN);
    }
    if (null == this.T_COMUNE_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNE_FORN);
    }
    if (null == this.T_PROVINCIA_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PROVINCIA_FORN);
    }
    if (null == this.T_NAZIONE_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NAZIONE_FORN);
    }
    if (null == this.ALTRO_IND_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ALTRO_IND_FORN);
    }
    if (null == this.T_ACCESSO_UI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ACCESSO_UI);
    }
    if (null == this.T_TIPO_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_FORNITURA);
    }
    if (null == this.T_ALIQUOTA_IVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ALIQUOTA_IVA);
    }
    if (null == this.T_ALIQUOTA_ACCISE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ALIQUOTA_ACCISE);
    }
    if (null == this.T_ADD_REGIONALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ADD_REGIONALE);
    }
    if (null == this.T_ALTRE_INFO_IMPOSTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ALTRE_INFO_IMPOSTE);
    }
    if (null == this.T_MATRICOLA_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MATRICOLA_MISURATORE);
    }
    if (null == this.T_CLASSE_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CLASSE_MISURATORE);
    }
    if (null == this.T_TIPO_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_MISURATORE);
    }
    if (null == this.T_TELEGESTIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TELEGESTIONE);
    }
    if (null == this.T_PRE_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PRE_CONV);
    }
    if (null == this.T_MATRICOLA_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MATRICOLA_CONVERTITORE);
    }
    if (null == this.N_NUM_CIFRE_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_CIFRE_CONVERTITORE, __dataOut);
    }
    if (null == this.T_ANNO_FABBRIC_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO_FABBRIC_CONVERTITORE);
    }
    if (null == this.T_DATA_INST_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DATA_INST_CONVERTITORE);
    }
    if (null == this.N_COEFF_CORREZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_COEFF_CORREZIONE, __dataOut);
    }
    if (null == this.PRESS_MISURE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.PRESS_MISURE, __dataOut);
    }
    if (null == this.T_ACCESS_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.T_ACCESS_MISURATORE, __dataOut);
    }
    if (null == this.N_NUM_CIFRE_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_CIFRE_MISURATORE, __dataOut);
    }
    if (null == this.T_ANNO_FABBRIC_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO_FABBRIC_MISURATORE);
    }
    if (null == this.T_DATA_INST_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DATA_INST_MISURATORE);
    }
    if (null == this.T_MISURATORE_INTEGRATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MISURATORE_INTEGRATO);
    }
    if (null == this.N_POTENZIALITA_MASSIMA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_POTENZIALITA_MASSIMA, __dataOut);
    }
    if (null == this.N_POTENZIALITA_TOT_INSTALLATA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_POTENZIALITA_TOT_INSTALLATA, __dataOut);
    }
    if (null == this.N_MAX_PRELIEVO_ORARIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_MAX_PRELIEVO_ORARIO, __dataOut);
    }
    if (null == this.T_EROG_SERVIZIO_ENERG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_EROG_SERVIZIO_ENERG);
    }
    if (null == this.T_PARTITA_IVA_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PARTITA_IVA_GESTCAL);
    }
    if (null == this.T_RAGIONE_SOCIALE_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RAGIONE_SOCIALE_GESTCAL);
    }
    if (null == this.T_TELEFONO_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TELEFONO_GESTCAL);
    }
    if (null == this.T_EMAIL_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_EMAIL_GESTCAL);
    }
    if (null == this.T_TOPONIMO_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TOPONIMO_GESTCAL);
    }
    if (null == this.T_NOMESTRADA_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOMESTRADA_GESTCAL);
    }
    if (null == this.T_CIVICO_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CIVICO_GESTCAL);
    }
    if (null == this.T_CAP_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CAP_GESTCAL);
    }
    if (null == this.T_COMUNE_ISTAT_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNE_ISTAT_GESTCAL);
    }
    if (null == this.T_COMUNE_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNE_GESTCAL);
    }
    if (null == this.T_PROVINCIA_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PROVINCIA_GESTCAL);
    }
    if (null == this.T_NAZIONE_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NAZIONE_GESTCAL);
    }
    if (null == this.D_DATA_RIF_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF_PDR);
    }
    if (null == this.D_AGGIORNAMENTO_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_AGGIORNAMENTO_PDR);
    }
    if (null == this.D_DATA_RIF_TECN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF_TECN);
    }
    if (null == this.D_AGGIORNAMENTO_TECN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_AGGIORNAMENTO_TECN);
    }
    if (null == this.D_DATA_RIF_MIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF_MIS);
    }
    if (null == this.D_AGGIORNAMENTO_MIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_AGGIORNAMENTO_MIS);
    }
    if (null == this.D_DATA_RIF_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF_FORN);
    }
    if (null == this.D_AGGIORNAMENTO_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_AGGIORNAMENTO_FORN);
    }
    if (null == this.T_TIPO_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_BONUS);
    }
    if (null == this.D_DATA_INIZIO_EROG_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INIZIO_EROG_BONUS);
    }
    if (null == this.D_DATA_FINE_EROG_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_FINE_EROG_BONUS);
    }
    if (null == this.D_DATA_RIF_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF_BONUS);
    }
    if (null == this.D_AGGIORNAMENTO_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_AGGIORNAMENTO_BONUS);
    }
    if (null == this.D_DATA_AGGIORNAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_AGGIORNAMENTO);
    }
    if (null == this.N_ID_UDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UDD, __dataOut);
    }
    if (null == this.N_ID_VENDITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_VENDITORE, __dataOut);
    }
    if (null == this.T_COD_PROFILO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_PROFILO);
    }
    if (null == this.T_COD_CAT_USO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_CAT_USO);
    }
    if (null == this.T_COD_CLASSE_PRELIEVO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_CLASSE_PRELIEVO);
    }
    if (null == this.T_ANNO_TERMICO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO_TERMICO);
    }
    if (null == this.D_DATA_RIF_PREL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF_PREL);
    }
    if (null == this.T_TRATTAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TRATTAMENTO);
    }
    if (null == this.T_TOPONIMO_ESAZ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TOPONIMO_ESAZ);
    }
    if (null == this.T_NOMESTRADA_ESAZ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOMESTRADA_ESAZ);
    }
    if (null == this.T_CIVICO_ESAZ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CIVICO_ESAZ);
    }
    if (null == this.T_CAP_ESAZ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CAP_ESAZ);
    }
    if (null == this.T_COMUNE_ISTAT_ESAZ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNE_ISTAT_ESAZ);
    }
    if (null == this.T_COMUNE_ESAZ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNE_ESAZ);
    }
    if (null == this.T_PROVINCIA_ESAZ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PROVINCIA_ESAZ);
    }
    if (null == this.T_NAZIONE_ESAZ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NAZIONE_ESAZ);
    }
    if (null == this.ALTRO_IND_ESAZ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ALTRO_IND_ESAZ);
    }
    if (null == this.T_CODICE_ATECO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_ATECO);
    }
    if (null == this.T_PAGAMENTO_IVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PAGAMENTO_IVA);
    }
    if (null == this.T_CODICE_UFFICIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_UFFICIO);
    }
    if (null == this.T_CF_INTESTATARIO_FATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CF_INTESTATARIO_FATT);
    }
    if (null == this.T_CF_STRANIERO_FATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CF_STRANIERO_FATT);
    }
    if (null == this.T_PIVA_INTESTATARIO_FATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA_INTESTATARIO_FATT);
    }
    if (null == this.T_NOME_INTESTATARIO_FATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOME_INTESTATARIO_FATT);
    }
    if (null == this.T_COGNOME_INTESTATARIO_FATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COGNOME_INTESTATARIO_FATT);
    }
    if (null == this.T_RAG_SOC_INTESTATARIO_FATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RAG_SOC_INTESTATARIO_FATT);
    }
    if (null == this.T_ANNO_MESE_RINN_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO_MESE_RINN_BONUS);
    }
    if (null == this.D_DATA_INIZIO_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INIZIO_BONUS);
    }
    if (null == this.D_DATA_FINE_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_FINE_BONUS);
    }
    if (null == this.N_PRELIEVO_ANNUO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_PRELIEVO_ANNUO, __dataOut);
    }
    if (null == this.T_FATTORE_CORREZ_CLIMATICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_FATTORE_CORREZ_CLIMATICA);
    }
    if (null == this.T_ALTRO_IND_GESTCAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ALTRO_IND_GESTCAL);
    }
    if (null == this.T_TIPO_OP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_OP);
    }
    if (null == this.T_PROCESSO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PROCESSO);
    }
    if (null == this.N_ID_PRATICA_PROCESSO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PRATICA_PROCESSO, __dataOut);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 8, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PDR==null?"\\N":N_ID_PDR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_PDR==null?"\\N":T_CODICE_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(CAPACITA_TRASPORTO==null?"\\N":CAPACITA_TRASPORTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(MESE_VAL_CAP_TRASP==null?"\\N":MESE_VAL_CAP_TRASP, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_TIPO_PDR==null?"\\N":T_COD_TIPO_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DISALIMENTABILITA==null?"\\N":T_DISALIMENTABILITA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(BILANCIAMENTO==null?"\\N":BILANCIAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_FORNITURA==null?"\\N":N_ID_FORNITURA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INIZIO_FOR==null?"\\N":D_DATA_INIZIO_FOR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATA_FINE_FOR==null?"\\N":DATA_FINE_FOR, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_AZ_UDD==null?"\\N":N_ID_AZ_UDD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PIVA_UDD==null?"\\N":PIVA_UDD, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_AZ_CC==null?"\\N":N_ID_AZ_CC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PIVA_CC==null?"\\N":PIVA_CC, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CLIENTE==null?"\\N":N_ID_CLIENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PARTITA_IVA_CLI==null?"\\N":T_PARTITA_IVA_CLI, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_FISCALE_CLI==null?"\\N":T_CODICE_FISCALE_CLI, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_CF_STRANIERO==null?"\\N":B_CF_STRANIERO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_REFERENTE==null?"\\N":T_REFERENTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOME_REF==null?"\\N":T_NOME_REF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COGNOME_REF==null?"\\N":T_COGNOME_REF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_EMAIL_REF==null?"\\N":T_EMAIL_REF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TELEFONO_REF==null?"\\N":T_TELEFONO_REF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RESIDENZA==null?"\\N":T_RESIDENZA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATA_VAL_RES==null?"\\N":DATA_VAL_RES, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TOPONIMOPDR==null?"\\N":T_TOPONIMOPDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOMESTRADA_PDR==null?"\\N":T_NOMESTRADA_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CIVICO_PDR==null?"\\N":T_CIVICO_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CAP_PDR==null?"\\N":T_CAP_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNE_ISTAT_PDR==null?"\\N":T_COMUNE_ISTAT_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNE_PDR==null?"\\N":T_COMUNE_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PROVINCIA_PDR==null?"\\N":T_PROVINCIA_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NAZIONE_PDR==null?"\\N":T_NAZIONE_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(ALTRO_IND_PDR==null?"\\N":ALTRO_IND_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TOPONIMO_FORN==null?"\\N":T_TOPONIMO_FORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOMESTRADA_FORN==null?"\\N":T_NOMESTRADA_FORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CIVICO_FORN==null?"\\N":T_CIVICO_FORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CAP_FORN==null?"\\N":T_CAP_FORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNE_ISTATFORN==null?"\\N":T_COMUNE_ISTATFORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNE_FORN==null?"\\N":T_COMUNE_FORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PROVINCIA_FORN==null?"\\N":T_PROVINCIA_FORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NAZIONE_FORN==null?"\\N":T_NAZIONE_FORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(ALTRO_IND_FORN==null?"\\N":ALTRO_IND_FORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ACCESSO_UI==null?"\\N":T_ACCESSO_UI, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_FORNITURA==null?"\\N":T_TIPO_FORNITURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ALIQUOTA_IVA==null?"\\N":T_ALIQUOTA_IVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ALIQUOTA_ACCISE==null?"\\N":T_ALIQUOTA_ACCISE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ADD_REGIONALE==null?"\\N":T_ADD_REGIONALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ALTRE_INFO_IMPOSTE==null?"\\N":T_ALTRE_INFO_IMPOSTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MATRICOLA_MISURATORE==null?"\\N":T_MATRICOLA_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CLASSE_MISURATORE==null?"\\N":T_CLASSE_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_MISURATORE==null?"\\N":T_TIPO_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TELEGESTIONE==null?"\\N":T_TELEGESTIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PRE_CONV==null?"\\N":T_PRE_CONV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MATRICOLA_CONVERTITORE==null?"\\N":T_MATRICOLA_CONVERTITORE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_CIFRE_CONVERTITORE==null?"\\N":N_NUM_CIFRE_CONVERTITORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO_FABBRIC_CONVERTITORE==null?"\\N":T_ANNO_FABBRIC_CONVERTITORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DATA_INST_CONVERTITORE==null?"\\N":T_DATA_INST_CONVERTITORE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_COEFF_CORREZIONE==null?"\\N":N_COEFF_CORREZIONE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PRESS_MISURE==null?"\\N":PRESS_MISURE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(T_ACCESS_MISURATORE==null?"\\N":T_ACCESS_MISURATORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_CIFRE_MISURATORE==null?"\\N":N_NUM_CIFRE_MISURATORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO_FABBRIC_MISURATORE==null?"\\N":T_ANNO_FABBRIC_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DATA_INST_MISURATORE==null?"\\N":T_DATA_INST_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MISURATORE_INTEGRATO==null?"\\N":T_MISURATORE_INTEGRATO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_POTENZIALITA_MASSIMA==null?"\\N":N_POTENZIALITA_MASSIMA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_POTENZIALITA_TOT_INSTALLATA==null?"\\N":N_POTENZIALITA_TOT_INSTALLATA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_MAX_PRELIEVO_ORARIO==null?"\\N":N_MAX_PRELIEVO_ORARIO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_EROG_SERVIZIO_ENERG==null?"\\N":T_EROG_SERVIZIO_ENERG, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PARTITA_IVA_GESTCAL==null?"\\N":T_PARTITA_IVA_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RAGIONE_SOCIALE_GESTCAL==null?"\\N":T_RAGIONE_SOCIALE_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TELEFONO_GESTCAL==null?"\\N":T_TELEFONO_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_EMAIL_GESTCAL==null?"\\N":T_EMAIL_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TOPONIMO_GESTCAL==null?"\\N":T_TOPONIMO_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOMESTRADA_GESTCAL==null?"\\N":T_NOMESTRADA_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CIVICO_GESTCAL==null?"\\N":T_CIVICO_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CAP_GESTCAL==null?"\\N":T_CAP_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNE_ISTAT_GESTCAL==null?"\\N":T_COMUNE_ISTAT_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNE_GESTCAL==null?"\\N":T_COMUNE_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PROVINCIA_GESTCAL==null?"\\N":T_PROVINCIA_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NAZIONE_GESTCAL==null?"\\N":T_NAZIONE_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF_PDR==null?"\\N":D_DATA_RIF_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO_PDR==null?"\\N":D_AGGIORNAMENTO_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF_TECN==null?"\\N":D_DATA_RIF_TECN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO_TECN==null?"\\N":D_AGGIORNAMENTO_TECN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF_MIS==null?"\\N":D_DATA_RIF_MIS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO_MIS==null?"\\N":D_AGGIORNAMENTO_MIS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF_FORN==null?"\\N":D_DATA_RIF_FORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO_FORN==null?"\\N":D_AGGIORNAMENTO_FORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_BONUS==null?"\\N":T_TIPO_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INIZIO_EROG_BONUS==null?"\\N":D_DATA_INIZIO_EROG_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_FINE_EROG_BONUS==null?"\\N":D_DATA_FINE_EROG_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF_BONUS==null?"\\N":D_DATA_RIF_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO_BONUS==null?"\\N":D_AGGIORNAMENTO_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_AGGIORNAMENTO==null?"\\N":D_DATA_AGGIORNAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UDD==null?"\\N":N_ID_UDD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_VENDITORE==null?"\\N":N_ID_VENDITORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_PROFILO==null?"\\N":T_COD_PROFILO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CAT_USO==null?"\\N":T_COD_CAT_USO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CLASSE_PRELIEVO==null?"\\N":T_COD_CLASSE_PRELIEVO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO_TERMICO==null?"\\N":T_ANNO_TERMICO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF_PREL==null?"\\N":D_DATA_RIF_PREL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TRATTAMENTO==null?"\\N":T_TRATTAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TOPONIMO_ESAZ==null?"\\N":T_TOPONIMO_ESAZ, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOMESTRADA_ESAZ==null?"\\N":T_NOMESTRADA_ESAZ, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CIVICO_ESAZ==null?"\\N":T_CIVICO_ESAZ, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CAP_ESAZ==null?"\\N":T_CAP_ESAZ, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNE_ISTAT_ESAZ==null?"\\N":T_COMUNE_ISTAT_ESAZ, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNE_ESAZ==null?"\\N":T_COMUNE_ESAZ, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PROVINCIA_ESAZ==null?"\\N":T_PROVINCIA_ESAZ, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NAZIONE_ESAZ==null?"\\N":T_NAZIONE_ESAZ, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(ALTRO_IND_ESAZ==null?"\\N":ALTRO_IND_ESAZ, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_ATECO==null?"\\N":T_CODICE_ATECO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PAGAMENTO_IVA==null?"\\N":T_PAGAMENTO_IVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_UFFICIO==null?"\\N":T_CODICE_UFFICIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CF_INTESTATARIO_FATT==null?"\\N":T_CF_INTESTATARIO_FATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CF_STRANIERO_FATT==null?"\\N":T_CF_STRANIERO_FATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA_INTESTATARIO_FATT==null?"\\N":T_PIVA_INTESTATARIO_FATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOME_INTESTATARIO_FATT==null?"\\N":T_NOME_INTESTATARIO_FATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COGNOME_INTESTATARIO_FATT==null?"\\N":T_COGNOME_INTESTATARIO_FATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RAG_SOC_INTESTATARIO_FATT==null?"\\N":T_RAG_SOC_INTESTATARIO_FATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO_MESE_RINN_BONUS==null?"\\N":T_ANNO_MESE_RINN_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INIZIO_BONUS==null?"\\N":D_DATA_INIZIO_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_FINE_BONUS==null?"\\N":D_DATA_FINE_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_PRELIEVO_ANNUO==null?"\\N":N_PRELIEVO_ANNUO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_FATTORE_CORREZ_CLIMATICA==null?"\\N":T_FATTORE_CORREZ_CLIMATICA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ALTRO_IND_GESTCAL==null?"\\N":T_ALTRO_IND_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_OP==null?"\\N":T_TIPO_OP, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PROCESSO==null?"\\N":T_PROCESSO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PRATICA_PROCESSO==null?"\\N":N_ID_PRATICA_PROCESSO.toPlainString(), delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PDR==null?"\\N":N_ID_PDR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_PDR==null?"\\N":T_CODICE_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(CAPACITA_TRASPORTO==null?"\\N":CAPACITA_TRASPORTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(MESE_VAL_CAP_TRASP==null?"\\N":MESE_VAL_CAP_TRASP, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_TIPO_PDR==null?"\\N":T_COD_TIPO_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DISALIMENTABILITA==null?"\\N":T_DISALIMENTABILITA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(BILANCIAMENTO==null?"\\N":BILANCIAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_FORNITURA==null?"\\N":N_ID_FORNITURA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INIZIO_FOR==null?"\\N":D_DATA_INIZIO_FOR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATA_FINE_FOR==null?"\\N":DATA_FINE_FOR, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_AZ_UDD==null?"\\N":N_ID_AZ_UDD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PIVA_UDD==null?"\\N":PIVA_UDD, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_AZ_CC==null?"\\N":N_ID_AZ_CC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PIVA_CC==null?"\\N":PIVA_CC, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CLIENTE==null?"\\N":N_ID_CLIENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PARTITA_IVA_CLI==null?"\\N":T_PARTITA_IVA_CLI, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_FISCALE_CLI==null?"\\N":T_CODICE_FISCALE_CLI, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_CF_STRANIERO==null?"\\N":B_CF_STRANIERO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_REFERENTE==null?"\\N":T_REFERENTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOME_REF==null?"\\N":T_NOME_REF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COGNOME_REF==null?"\\N":T_COGNOME_REF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_EMAIL_REF==null?"\\N":T_EMAIL_REF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TELEFONO_REF==null?"\\N":T_TELEFONO_REF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RESIDENZA==null?"\\N":T_RESIDENZA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATA_VAL_RES==null?"\\N":DATA_VAL_RES, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TOPONIMOPDR==null?"\\N":T_TOPONIMOPDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOMESTRADA_PDR==null?"\\N":T_NOMESTRADA_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CIVICO_PDR==null?"\\N":T_CIVICO_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CAP_PDR==null?"\\N":T_CAP_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNE_ISTAT_PDR==null?"\\N":T_COMUNE_ISTAT_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNE_PDR==null?"\\N":T_COMUNE_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PROVINCIA_PDR==null?"\\N":T_PROVINCIA_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NAZIONE_PDR==null?"\\N":T_NAZIONE_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(ALTRO_IND_PDR==null?"\\N":ALTRO_IND_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TOPONIMO_FORN==null?"\\N":T_TOPONIMO_FORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOMESTRADA_FORN==null?"\\N":T_NOMESTRADA_FORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CIVICO_FORN==null?"\\N":T_CIVICO_FORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CAP_FORN==null?"\\N":T_CAP_FORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNE_ISTATFORN==null?"\\N":T_COMUNE_ISTATFORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNE_FORN==null?"\\N":T_COMUNE_FORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PROVINCIA_FORN==null?"\\N":T_PROVINCIA_FORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NAZIONE_FORN==null?"\\N":T_NAZIONE_FORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(ALTRO_IND_FORN==null?"\\N":ALTRO_IND_FORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ACCESSO_UI==null?"\\N":T_ACCESSO_UI, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_FORNITURA==null?"\\N":T_TIPO_FORNITURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ALIQUOTA_IVA==null?"\\N":T_ALIQUOTA_IVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ALIQUOTA_ACCISE==null?"\\N":T_ALIQUOTA_ACCISE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ADD_REGIONALE==null?"\\N":T_ADD_REGIONALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ALTRE_INFO_IMPOSTE==null?"\\N":T_ALTRE_INFO_IMPOSTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MATRICOLA_MISURATORE==null?"\\N":T_MATRICOLA_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CLASSE_MISURATORE==null?"\\N":T_CLASSE_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_MISURATORE==null?"\\N":T_TIPO_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TELEGESTIONE==null?"\\N":T_TELEGESTIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PRE_CONV==null?"\\N":T_PRE_CONV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MATRICOLA_CONVERTITORE==null?"\\N":T_MATRICOLA_CONVERTITORE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_CIFRE_CONVERTITORE==null?"\\N":N_NUM_CIFRE_CONVERTITORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO_FABBRIC_CONVERTITORE==null?"\\N":T_ANNO_FABBRIC_CONVERTITORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DATA_INST_CONVERTITORE==null?"\\N":T_DATA_INST_CONVERTITORE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_COEFF_CORREZIONE==null?"\\N":N_COEFF_CORREZIONE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PRESS_MISURE==null?"\\N":PRESS_MISURE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(T_ACCESS_MISURATORE==null?"\\N":T_ACCESS_MISURATORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_CIFRE_MISURATORE==null?"\\N":N_NUM_CIFRE_MISURATORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO_FABBRIC_MISURATORE==null?"\\N":T_ANNO_FABBRIC_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DATA_INST_MISURATORE==null?"\\N":T_DATA_INST_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MISURATORE_INTEGRATO==null?"\\N":T_MISURATORE_INTEGRATO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_POTENZIALITA_MASSIMA==null?"\\N":N_POTENZIALITA_MASSIMA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_POTENZIALITA_TOT_INSTALLATA==null?"\\N":N_POTENZIALITA_TOT_INSTALLATA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_MAX_PRELIEVO_ORARIO==null?"\\N":N_MAX_PRELIEVO_ORARIO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_EROG_SERVIZIO_ENERG==null?"\\N":T_EROG_SERVIZIO_ENERG, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PARTITA_IVA_GESTCAL==null?"\\N":T_PARTITA_IVA_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RAGIONE_SOCIALE_GESTCAL==null?"\\N":T_RAGIONE_SOCIALE_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TELEFONO_GESTCAL==null?"\\N":T_TELEFONO_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_EMAIL_GESTCAL==null?"\\N":T_EMAIL_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TOPONIMO_GESTCAL==null?"\\N":T_TOPONIMO_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOMESTRADA_GESTCAL==null?"\\N":T_NOMESTRADA_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CIVICO_GESTCAL==null?"\\N":T_CIVICO_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CAP_GESTCAL==null?"\\N":T_CAP_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNE_ISTAT_GESTCAL==null?"\\N":T_COMUNE_ISTAT_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNE_GESTCAL==null?"\\N":T_COMUNE_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PROVINCIA_GESTCAL==null?"\\N":T_PROVINCIA_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NAZIONE_GESTCAL==null?"\\N":T_NAZIONE_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF_PDR==null?"\\N":D_DATA_RIF_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO_PDR==null?"\\N":D_AGGIORNAMENTO_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF_TECN==null?"\\N":D_DATA_RIF_TECN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO_TECN==null?"\\N":D_AGGIORNAMENTO_TECN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF_MIS==null?"\\N":D_DATA_RIF_MIS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO_MIS==null?"\\N":D_AGGIORNAMENTO_MIS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF_FORN==null?"\\N":D_DATA_RIF_FORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO_FORN==null?"\\N":D_AGGIORNAMENTO_FORN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_BONUS==null?"\\N":T_TIPO_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INIZIO_EROG_BONUS==null?"\\N":D_DATA_INIZIO_EROG_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_FINE_EROG_BONUS==null?"\\N":D_DATA_FINE_EROG_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF_BONUS==null?"\\N":D_DATA_RIF_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO_BONUS==null?"\\N":D_AGGIORNAMENTO_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_AGGIORNAMENTO==null?"\\N":D_DATA_AGGIORNAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UDD==null?"\\N":N_ID_UDD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_VENDITORE==null?"\\N":N_ID_VENDITORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_PROFILO==null?"\\N":T_COD_PROFILO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CAT_USO==null?"\\N":T_COD_CAT_USO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CLASSE_PRELIEVO==null?"\\N":T_COD_CLASSE_PRELIEVO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO_TERMICO==null?"\\N":T_ANNO_TERMICO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF_PREL==null?"\\N":D_DATA_RIF_PREL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TRATTAMENTO==null?"\\N":T_TRATTAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TOPONIMO_ESAZ==null?"\\N":T_TOPONIMO_ESAZ, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOMESTRADA_ESAZ==null?"\\N":T_NOMESTRADA_ESAZ, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CIVICO_ESAZ==null?"\\N":T_CIVICO_ESAZ, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CAP_ESAZ==null?"\\N":T_CAP_ESAZ, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNE_ISTAT_ESAZ==null?"\\N":T_COMUNE_ISTAT_ESAZ, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNE_ESAZ==null?"\\N":T_COMUNE_ESAZ, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PROVINCIA_ESAZ==null?"\\N":T_PROVINCIA_ESAZ, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NAZIONE_ESAZ==null?"\\N":T_NAZIONE_ESAZ, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(ALTRO_IND_ESAZ==null?"\\N":ALTRO_IND_ESAZ, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_ATECO==null?"\\N":T_CODICE_ATECO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PAGAMENTO_IVA==null?"\\N":T_PAGAMENTO_IVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_UFFICIO==null?"\\N":T_CODICE_UFFICIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CF_INTESTATARIO_FATT==null?"\\N":T_CF_INTESTATARIO_FATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CF_STRANIERO_FATT==null?"\\N":T_CF_STRANIERO_FATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA_INTESTATARIO_FATT==null?"\\N":T_PIVA_INTESTATARIO_FATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOME_INTESTATARIO_FATT==null?"\\N":T_NOME_INTESTATARIO_FATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COGNOME_INTESTATARIO_FATT==null?"\\N":T_COGNOME_INTESTATARIO_FATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RAG_SOC_INTESTATARIO_FATT==null?"\\N":T_RAG_SOC_INTESTATARIO_FATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO_MESE_RINN_BONUS==null?"\\N":T_ANNO_MESE_RINN_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INIZIO_BONUS==null?"\\N":D_DATA_INIZIO_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_FINE_BONUS==null?"\\N":D_DATA_FINE_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_PRELIEVO_ANNUO==null?"\\N":N_PRELIEVO_ANNUO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_FATTORE_CORREZ_CLIMATICA==null?"\\N":T_FATTORE_CORREZ_CLIMATICA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ALTRO_IND_GESTCAL==null?"\\N":T_ALTRO_IND_GESTCAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_OP==null?"\\N":T_TIPO_OP, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PROCESSO==null?"\\N":T_PROCESSO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PRATICA_PROCESSO==null?"\\N":N_ID_PRATICA_PROCESSO.toPlainString(), delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 8, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PDR = null; } else {
      this.N_ID_PDR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_PDR = null; } else {
      this.T_CODICE_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CAPACITA_TRASPORTO = null; } else {
      this.CAPACITA_TRASPORTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MESE_VAL_CAP_TRASP = null; } else {
      this.MESE_VAL_CAP_TRASP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_TIPO_PDR = null; } else {
      this.T_COD_TIPO_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DISALIMENTABILITA = null; } else {
      this.T_DISALIMENTABILITA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILANCIAMENTO = null; } else {
      this.BILANCIAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_FORNITURA = null; } else {
      this.N_ID_FORNITURA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INIZIO_FOR = null; } else {
      this.D_DATA_INIZIO_FOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_FINE_FOR = null; } else {
      this.DATA_FINE_FOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_AZ_UDD = null; } else {
      this.N_ID_AZ_UDD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PIVA_UDD = null; } else {
      this.PIVA_UDD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_AZ_CC = null; } else {
      this.N_ID_AZ_CC = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PIVA_CC = null; } else {
      this.PIVA_CC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CLIENTE = null; } else {
      this.N_ID_CLIENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PARTITA_IVA_CLI = null; } else {
      this.T_PARTITA_IVA_CLI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_FISCALE_CLI = null; } else {
      this.T_CODICE_FISCALE_CLI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_CF_STRANIERO = null; } else {
      this.B_CF_STRANIERO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_REFERENTE = null; } else {
      this.T_REFERENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOME_REF = null; } else {
      this.T_NOME_REF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COGNOME_REF = null; } else {
      this.T_COGNOME_REF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_EMAIL_REF = null; } else {
      this.T_EMAIL_REF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TELEFONO_REF = null; } else {
      this.T_TELEFONO_REF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RESIDENZA = null; } else {
      this.T_RESIDENZA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_VAL_RES = null; } else {
      this.DATA_VAL_RES = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TOPONIMOPDR = null; } else {
      this.T_TOPONIMOPDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOMESTRADA_PDR = null; } else {
      this.T_NOMESTRADA_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CIVICO_PDR = null; } else {
      this.T_CIVICO_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CAP_PDR = null; } else {
      this.T_CAP_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNE_ISTAT_PDR = null; } else {
      this.T_COMUNE_ISTAT_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNE_PDR = null; } else {
      this.T_COMUNE_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PROVINCIA_PDR = null; } else {
      this.T_PROVINCIA_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NAZIONE_PDR = null; } else {
      this.T_NAZIONE_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ALTRO_IND_PDR = null; } else {
      this.ALTRO_IND_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TOPONIMO_FORN = null; } else {
      this.T_TOPONIMO_FORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOMESTRADA_FORN = null; } else {
      this.T_NOMESTRADA_FORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CIVICO_FORN = null; } else {
      this.T_CIVICO_FORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CAP_FORN = null; } else {
      this.T_CAP_FORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNE_ISTATFORN = null; } else {
      this.T_COMUNE_ISTATFORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNE_FORN = null; } else {
      this.T_COMUNE_FORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PROVINCIA_FORN = null; } else {
      this.T_PROVINCIA_FORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NAZIONE_FORN = null; } else {
      this.T_NAZIONE_FORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ALTRO_IND_FORN = null; } else {
      this.ALTRO_IND_FORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ACCESSO_UI = null; } else {
      this.T_ACCESSO_UI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_FORNITURA = null; } else {
      this.T_TIPO_FORNITURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ALIQUOTA_IVA = null; } else {
      this.T_ALIQUOTA_IVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ALIQUOTA_ACCISE = null; } else {
      this.T_ALIQUOTA_ACCISE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ADD_REGIONALE = null; } else {
      this.T_ADD_REGIONALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ALTRE_INFO_IMPOSTE = null; } else {
      this.T_ALTRE_INFO_IMPOSTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MATRICOLA_MISURATORE = null; } else {
      this.T_MATRICOLA_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CLASSE_MISURATORE = null; } else {
      this.T_CLASSE_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_MISURATORE = null; } else {
      this.T_TIPO_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TELEGESTIONE = null; } else {
      this.T_TELEGESTIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PRE_CONV = null; } else {
      this.T_PRE_CONV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MATRICOLA_CONVERTITORE = null; } else {
      this.T_MATRICOLA_CONVERTITORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_CIFRE_CONVERTITORE = null; } else {
      this.N_NUM_CIFRE_CONVERTITORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO_FABBRIC_CONVERTITORE = null; } else {
      this.T_ANNO_FABBRIC_CONVERTITORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DATA_INST_CONVERTITORE = null; } else {
      this.T_DATA_INST_CONVERTITORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_COEFF_CORREZIONE = null; } else {
      this.N_COEFF_CORREZIONE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PRESS_MISURE = null; } else {
      this.PRESS_MISURE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.T_ACCESS_MISURATORE = null; } else {
      this.T_ACCESS_MISURATORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_CIFRE_MISURATORE = null; } else {
      this.N_NUM_CIFRE_MISURATORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO_FABBRIC_MISURATORE = null; } else {
      this.T_ANNO_FABBRIC_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DATA_INST_MISURATORE = null; } else {
      this.T_DATA_INST_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MISURATORE_INTEGRATO = null; } else {
      this.T_MISURATORE_INTEGRATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_POTENZIALITA_MASSIMA = null; } else {
      this.N_POTENZIALITA_MASSIMA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_POTENZIALITA_TOT_INSTALLATA = null; } else {
      this.N_POTENZIALITA_TOT_INSTALLATA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_MAX_PRELIEVO_ORARIO = null; } else {
      this.N_MAX_PRELIEVO_ORARIO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_EROG_SERVIZIO_ENERG = null; } else {
      this.T_EROG_SERVIZIO_ENERG = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PARTITA_IVA_GESTCAL = null; } else {
      this.T_PARTITA_IVA_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RAGIONE_SOCIALE_GESTCAL = null; } else {
      this.T_RAGIONE_SOCIALE_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TELEFONO_GESTCAL = null; } else {
      this.T_TELEFONO_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_EMAIL_GESTCAL = null; } else {
      this.T_EMAIL_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TOPONIMO_GESTCAL = null; } else {
      this.T_TOPONIMO_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOMESTRADA_GESTCAL = null; } else {
      this.T_NOMESTRADA_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CIVICO_GESTCAL = null; } else {
      this.T_CIVICO_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CAP_GESTCAL = null; } else {
      this.T_CAP_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNE_ISTAT_GESTCAL = null; } else {
      this.T_COMUNE_ISTAT_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNE_GESTCAL = null; } else {
      this.T_COMUNE_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PROVINCIA_GESTCAL = null; } else {
      this.T_PROVINCIA_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NAZIONE_GESTCAL = null; } else {
      this.T_NAZIONE_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RIF_PDR = null; } else {
      this.D_DATA_RIF_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_AGGIORNAMENTO_PDR = null; } else {
      this.D_AGGIORNAMENTO_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RIF_TECN = null; } else {
      this.D_DATA_RIF_TECN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_AGGIORNAMENTO_TECN = null; } else {
      this.D_AGGIORNAMENTO_TECN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RIF_MIS = null; } else {
      this.D_DATA_RIF_MIS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_AGGIORNAMENTO_MIS = null; } else {
      this.D_AGGIORNAMENTO_MIS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RIF_FORN = null; } else {
      this.D_DATA_RIF_FORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_AGGIORNAMENTO_FORN = null; } else {
      this.D_AGGIORNAMENTO_FORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_BONUS = null; } else {
      this.T_TIPO_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INIZIO_EROG_BONUS = null; } else {
      this.D_DATA_INIZIO_EROG_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_FINE_EROG_BONUS = null; } else {
      this.D_DATA_FINE_EROG_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RIF_BONUS = null; } else {
      this.D_DATA_RIF_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_AGGIORNAMENTO_BONUS = null; } else {
      this.D_AGGIORNAMENTO_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_AGGIORNAMENTO = null; } else {
      this.D_DATA_AGGIORNAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UDD = null; } else {
      this.N_ID_UDD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_VENDITORE = null; } else {
      this.N_ID_VENDITORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_PROFILO = null; } else {
      this.T_COD_PROFILO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_CAT_USO = null; } else {
      this.T_COD_CAT_USO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_CLASSE_PRELIEVO = null; } else {
      this.T_COD_CLASSE_PRELIEVO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO_TERMICO = null; } else {
      this.T_ANNO_TERMICO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RIF_PREL = null; } else {
      this.D_DATA_RIF_PREL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TRATTAMENTO = null; } else {
      this.T_TRATTAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TOPONIMO_ESAZ = null; } else {
      this.T_TOPONIMO_ESAZ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOMESTRADA_ESAZ = null; } else {
      this.T_NOMESTRADA_ESAZ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CIVICO_ESAZ = null; } else {
      this.T_CIVICO_ESAZ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CAP_ESAZ = null; } else {
      this.T_CAP_ESAZ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNE_ISTAT_ESAZ = null; } else {
      this.T_COMUNE_ISTAT_ESAZ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNE_ESAZ = null; } else {
      this.T_COMUNE_ESAZ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PROVINCIA_ESAZ = null; } else {
      this.T_PROVINCIA_ESAZ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NAZIONE_ESAZ = null; } else {
      this.T_NAZIONE_ESAZ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ALTRO_IND_ESAZ = null; } else {
      this.ALTRO_IND_ESAZ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_ATECO = null; } else {
      this.T_CODICE_ATECO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PAGAMENTO_IVA = null; } else {
      this.T_PAGAMENTO_IVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_UFFICIO = null; } else {
      this.T_CODICE_UFFICIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CF_INTESTATARIO_FATT = null; } else {
      this.T_CF_INTESTATARIO_FATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CF_STRANIERO_FATT = null; } else {
      this.T_CF_STRANIERO_FATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA_INTESTATARIO_FATT = null; } else {
      this.T_PIVA_INTESTATARIO_FATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOME_INTESTATARIO_FATT = null; } else {
      this.T_NOME_INTESTATARIO_FATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COGNOME_INTESTATARIO_FATT = null; } else {
      this.T_COGNOME_INTESTATARIO_FATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RAG_SOC_INTESTATARIO_FATT = null; } else {
      this.T_RAG_SOC_INTESTATARIO_FATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO_MESE_RINN_BONUS = null; } else {
      this.T_ANNO_MESE_RINN_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INIZIO_BONUS = null; } else {
      this.D_DATA_INIZIO_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_FINE_BONUS = null; } else {
      this.D_DATA_FINE_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_PRELIEVO_ANNUO = null; } else {
      this.N_PRELIEVO_ANNUO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_FATTORE_CORREZ_CLIMATICA = null; } else {
      this.T_FATTORE_CORREZ_CLIMATICA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ALTRO_IND_GESTCAL = null; } else {
      this.T_ALTRO_IND_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_OP = null; } else {
      this.T_TIPO_OP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PROCESSO = null; } else {
      this.T_PROCESSO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PRATICA_PROCESSO = null; } else {
      this.N_ID_PRATICA_PROCESSO = new java.math.BigDecimal(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PDR = null; } else {
      this.N_ID_PDR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_PDR = null; } else {
      this.T_CODICE_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CAPACITA_TRASPORTO = null; } else {
      this.CAPACITA_TRASPORTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MESE_VAL_CAP_TRASP = null; } else {
      this.MESE_VAL_CAP_TRASP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_TIPO_PDR = null; } else {
      this.T_COD_TIPO_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DISALIMENTABILITA = null; } else {
      this.T_DISALIMENTABILITA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILANCIAMENTO = null; } else {
      this.BILANCIAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_FORNITURA = null; } else {
      this.N_ID_FORNITURA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INIZIO_FOR = null; } else {
      this.D_DATA_INIZIO_FOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_FINE_FOR = null; } else {
      this.DATA_FINE_FOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_AZ_UDD = null; } else {
      this.N_ID_AZ_UDD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PIVA_UDD = null; } else {
      this.PIVA_UDD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_AZ_CC = null; } else {
      this.N_ID_AZ_CC = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PIVA_CC = null; } else {
      this.PIVA_CC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CLIENTE = null; } else {
      this.N_ID_CLIENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PARTITA_IVA_CLI = null; } else {
      this.T_PARTITA_IVA_CLI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_FISCALE_CLI = null; } else {
      this.T_CODICE_FISCALE_CLI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_CF_STRANIERO = null; } else {
      this.B_CF_STRANIERO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_REFERENTE = null; } else {
      this.T_REFERENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOME_REF = null; } else {
      this.T_NOME_REF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COGNOME_REF = null; } else {
      this.T_COGNOME_REF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_EMAIL_REF = null; } else {
      this.T_EMAIL_REF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TELEFONO_REF = null; } else {
      this.T_TELEFONO_REF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RESIDENZA = null; } else {
      this.T_RESIDENZA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_VAL_RES = null; } else {
      this.DATA_VAL_RES = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TOPONIMOPDR = null; } else {
      this.T_TOPONIMOPDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOMESTRADA_PDR = null; } else {
      this.T_NOMESTRADA_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CIVICO_PDR = null; } else {
      this.T_CIVICO_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CAP_PDR = null; } else {
      this.T_CAP_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNE_ISTAT_PDR = null; } else {
      this.T_COMUNE_ISTAT_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNE_PDR = null; } else {
      this.T_COMUNE_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PROVINCIA_PDR = null; } else {
      this.T_PROVINCIA_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NAZIONE_PDR = null; } else {
      this.T_NAZIONE_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ALTRO_IND_PDR = null; } else {
      this.ALTRO_IND_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TOPONIMO_FORN = null; } else {
      this.T_TOPONIMO_FORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOMESTRADA_FORN = null; } else {
      this.T_NOMESTRADA_FORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CIVICO_FORN = null; } else {
      this.T_CIVICO_FORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CAP_FORN = null; } else {
      this.T_CAP_FORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNE_ISTATFORN = null; } else {
      this.T_COMUNE_ISTATFORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNE_FORN = null; } else {
      this.T_COMUNE_FORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PROVINCIA_FORN = null; } else {
      this.T_PROVINCIA_FORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NAZIONE_FORN = null; } else {
      this.T_NAZIONE_FORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ALTRO_IND_FORN = null; } else {
      this.ALTRO_IND_FORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ACCESSO_UI = null; } else {
      this.T_ACCESSO_UI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_FORNITURA = null; } else {
      this.T_TIPO_FORNITURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ALIQUOTA_IVA = null; } else {
      this.T_ALIQUOTA_IVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ALIQUOTA_ACCISE = null; } else {
      this.T_ALIQUOTA_ACCISE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ADD_REGIONALE = null; } else {
      this.T_ADD_REGIONALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ALTRE_INFO_IMPOSTE = null; } else {
      this.T_ALTRE_INFO_IMPOSTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MATRICOLA_MISURATORE = null; } else {
      this.T_MATRICOLA_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CLASSE_MISURATORE = null; } else {
      this.T_CLASSE_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_MISURATORE = null; } else {
      this.T_TIPO_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TELEGESTIONE = null; } else {
      this.T_TELEGESTIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PRE_CONV = null; } else {
      this.T_PRE_CONV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MATRICOLA_CONVERTITORE = null; } else {
      this.T_MATRICOLA_CONVERTITORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_CIFRE_CONVERTITORE = null; } else {
      this.N_NUM_CIFRE_CONVERTITORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO_FABBRIC_CONVERTITORE = null; } else {
      this.T_ANNO_FABBRIC_CONVERTITORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DATA_INST_CONVERTITORE = null; } else {
      this.T_DATA_INST_CONVERTITORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_COEFF_CORREZIONE = null; } else {
      this.N_COEFF_CORREZIONE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PRESS_MISURE = null; } else {
      this.PRESS_MISURE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.T_ACCESS_MISURATORE = null; } else {
      this.T_ACCESS_MISURATORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_CIFRE_MISURATORE = null; } else {
      this.N_NUM_CIFRE_MISURATORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO_FABBRIC_MISURATORE = null; } else {
      this.T_ANNO_FABBRIC_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DATA_INST_MISURATORE = null; } else {
      this.T_DATA_INST_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MISURATORE_INTEGRATO = null; } else {
      this.T_MISURATORE_INTEGRATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_POTENZIALITA_MASSIMA = null; } else {
      this.N_POTENZIALITA_MASSIMA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_POTENZIALITA_TOT_INSTALLATA = null; } else {
      this.N_POTENZIALITA_TOT_INSTALLATA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_MAX_PRELIEVO_ORARIO = null; } else {
      this.N_MAX_PRELIEVO_ORARIO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_EROG_SERVIZIO_ENERG = null; } else {
      this.T_EROG_SERVIZIO_ENERG = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PARTITA_IVA_GESTCAL = null; } else {
      this.T_PARTITA_IVA_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RAGIONE_SOCIALE_GESTCAL = null; } else {
      this.T_RAGIONE_SOCIALE_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TELEFONO_GESTCAL = null; } else {
      this.T_TELEFONO_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_EMAIL_GESTCAL = null; } else {
      this.T_EMAIL_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TOPONIMO_GESTCAL = null; } else {
      this.T_TOPONIMO_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOMESTRADA_GESTCAL = null; } else {
      this.T_NOMESTRADA_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CIVICO_GESTCAL = null; } else {
      this.T_CIVICO_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CAP_GESTCAL = null; } else {
      this.T_CAP_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNE_ISTAT_GESTCAL = null; } else {
      this.T_COMUNE_ISTAT_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNE_GESTCAL = null; } else {
      this.T_COMUNE_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PROVINCIA_GESTCAL = null; } else {
      this.T_PROVINCIA_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NAZIONE_GESTCAL = null; } else {
      this.T_NAZIONE_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RIF_PDR = null; } else {
      this.D_DATA_RIF_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_AGGIORNAMENTO_PDR = null; } else {
      this.D_AGGIORNAMENTO_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RIF_TECN = null; } else {
      this.D_DATA_RIF_TECN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_AGGIORNAMENTO_TECN = null; } else {
      this.D_AGGIORNAMENTO_TECN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RIF_MIS = null; } else {
      this.D_DATA_RIF_MIS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_AGGIORNAMENTO_MIS = null; } else {
      this.D_AGGIORNAMENTO_MIS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RIF_FORN = null; } else {
      this.D_DATA_RIF_FORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_AGGIORNAMENTO_FORN = null; } else {
      this.D_AGGIORNAMENTO_FORN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_BONUS = null; } else {
      this.T_TIPO_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INIZIO_EROG_BONUS = null; } else {
      this.D_DATA_INIZIO_EROG_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_FINE_EROG_BONUS = null; } else {
      this.D_DATA_FINE_EROG_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RIF_BONUS = null; } else {
      this.D_DATA_RIF_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_AGGIORNAMENTO_BONUS = null; } else {
      this.D_AGGIORNAMENTO_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_AGGIORNAMENTO = null; } else {
      this.D_DATA_AGGIORNAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UDD = null; } else {
      this.N_ID_UDD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_VENDITORE = null; } else {
      this.N_ID_VENDITORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_PROFILO = null; } else {
      this.T_COD_PROFILO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_CAT_USO = null; } else {
      this.T_COD_CAT_USO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_CLASSE_PRELIEVO = null; } else {
      this.T_COD_CLASSE_PRELIEVO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO_TERMICO = null; } else {
      this.T_ANNO_TERMICO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RIF_PREL = null; } else {
      this.D_DATA_RIF_PREL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TRATTAMENTO = null; } else {
      this.T_TRATTAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TOPONIMO_ESAZ = null; } else {
      this.T_TOPONIMO_ESAZ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOMESTRADA_ESAZ = null; } else {
      this.T_NOMESTRADA_ESAZ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CIVICO_ESAZ = null; } else {
      this.T_CIVICO_ESAZ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CAP_ESAZ = null; } else {
      this.T_CAP_ESAZ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNE_ISTAT_ESAZ = null; } else {
      this.T_COMUNE_ISTAT_ESAZ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNE_ESAZ = null; } else {
      this.T_COMUNE_ESAZ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PROVINCIA_ESAZ = null; } else {
      this.T_PROVINCIA_ESAZ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NAZIONE_ESAZ = null; } else {
      this.T_NAZIONE_ESAZ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ALTRO_IND_ESAZ = null; } else {
      this.ALTRO_IND_ESAZ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_ATECO = null; } else {
      this.T_CODICE_ATECO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PAGAMENTO_IVA = null; } else {
      this.T_PAGAMENTO_IVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_UFFICIO = null; } else {
      this.T_CODICE_UFFICIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CF_INTESTATARIO_FATT = null; } else {
      this.T_CF_INTESTATARIO_FATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CF_STRANIERO_FATT = null; } else {
      this.T_CF_STRANIERO_FATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA_INTESTATARIO_FATT = null; } else {
      this.T_PIVA_INTESTATARIO_FATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOME_INTESTATARIO_FATT = null; } else {
      this.T_NOME_INTESTATARIO_FATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COGNOME_INTESTATARIO_FATT = null; } else {
      this.T_COGNOME_INTESTATARIO_FATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RAG_SOC_INTESTATARIO_FATT = null; } else {
      this.T_RAG_SOC_INTESTATARIO_FATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO_MESE_RINN_BONUS = null; } else {
      this.T_ANNO_MESE_RINN_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INIZIO_BONUS = null; } else {
      this.D_DATA_INIZIO_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_FINE_BONUS = null; } else {
      this.D_DATA_FINE_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_PRELIEVO_ANNUO = null; } else {
      this.N_PRELIEVO_ANNUO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_FATTORE_CORREZ_CLIMATICA = null; } else {
      this.T_FATTORE_CORREZ_CLIMATICA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ALTRO_IND_GESTCAL = null; } else {
      this.T_ALTRO_IND_GESTCAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_OP = null; } else {
      this.T_TIPO_OP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PROCESSO = null; } else {
      this.T_PROCESSO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PRATICA_PROCESSO = null; } else {
      this.N_ID_PRATICA_PROCESSO = new java.math.BigDecimal(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    drosati_rcugas_massivo_sosia o = (drosati_rcugas_massivo_sosia) super.clone();
    return o;
  }

  public void clone0(drosati_rcugas_massivo_sosia o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_PDR", this.N_ID_PDR);
    __sqoop$field_map.put("T_CODICE_PDR", this.T_CODICE_PDR);
    __sqoop$field_map.put("CAPACITA_TRASPORTO", this.CAPACITA_TRASPORTO);
    __sqoop$field_map.put("MESE_VAL_CAP_TRASP", this.MESE_VAL_CAP_TRASP);
    __sqoop$field_map.put("T_COD_TIPO_PDR", this.T_COD_TIPO_PDR);
    __sqoop$field_map.put("T_DISALIMENTABILITA", this.T_DISALIMENTABILITA);
    __sqoop$field_map.put("BILANCIAMENTO", this.BILANCIAMENTO);
    __sqoop$field_map.put("N_ID_FORNITURA", this.N_ID_FORNITURA);
    __sqoop$field_map.put("D_DATA_INIZIO_FOR", this.D_DATA_INIZIO_FOR);
    __sqoop$field_map.put("DATA_FINE_FOR", this.DATA_FINE_FOR);
    __sqoop$field_map.put("N_ID_AZ_UDD", this.N_ID_AZ_UDD);
    __sqoop$field_map.put("PIVA_UDD", this.PIVA_UDD);
    __sqoop$field_map.put("N_ID_AZ_CC", this.N_ID_AZ_CC);
    __sqoop$field_map.put("PIVA_CC", this.PIVA_CC);
    __sqoop$field_map.put("N_ID_CLIENTE", this.N_ID_CLIENTE);
    __sqoop$field_map.put("T_PARTITA_IVA_CLI", this.T_PARTITA_IVA_CLI);
    __sqoop$field_map.put("T_CODICE_FISCALE_CLI", this.T_CODICE_FISCALE_CLI);
    __sqoop$field_map.put("B_CF_STRANIERO", this.B_CF_STRANIERO);
    __sqoop$field_map.put("T_REFERENTE", this.T_REFERENTE);
    __sqoop$field_map.put("T_NOME_REF", this.T_NOME_REF);
    __sqoop$field_map.put("T_COGNOME_REF", this.T_COGNOME_REF);
    __sqoop$field_map.put("T_EMAIL_REF", this.T_EMAIL_REF);
    __sqoop$field_map.put("T_TELEFONO_REF", this.T_TELEFONO_REF);
    __sqoop$field_map.put("T_RESIDENZA", this.T_RESIDENZA);
    __sqoop$field_map.put("DATA_VAL_RES", this.DATA_VAL_RES);
    __sqoop$field_map.put("T_TOPONIMOPDR", this.T_TOPONIMOPDR);
    __sqoop$field_map.put("T_NOMESTRADA_PDR", this.T_NOMESTRADA_PDR);
    __sqoop$field_map.put("T_CIVICO_PDR", this.T_CIVICO_PDR);
    __sqoop$field_map.put("T_CAP_PDR", this.T_CAP_PDR);
    __sqoop$field_map.put("T_COMUNE_ISTAT_PDR", this.T_COMUNE_ISTAT_PDR);
    __sqoop$field_map.put("T_COMUNE_PDR", this.T_COMUNE_PDR);
    __sqoop$field_map.put("T_PROVINCIA_PDR", this.T_PROVINCIA_PDR);
    __sqoop$field_map.put("T_NAZIONE_PDR", this.T_NAZIONE_PDR);
    __sqoop$field_map.put("ALTRO_IND_PDR", this.ALTRO_IND_PDR);
    __sqoop$field_map.put("T_TOPONIMO_FORN", this.T_TOPONIMO_FORN);
    __sqoop$field_map.put("T_NOMESTRADA_FORN", this.T_NOMESTRADA_FORN);
    __sqoop$field_map.put("T_CIVICO_FORN", this.T_CIVICO_FORN);
    __sqoop$field_map.put("T_CAP_FORN", this.T_CAP_FORN);
    __sqoop$field_map.put("T_COMUNE_ISTATFORN", this.T_COMUNE_ISTATFORN);
    __sqoop$field_map.put("T_COMUNE_FORN", this.T_COMUNE_FORN);
    __sqoop$field_map.put("T_PROVINCIA_FORN", this.T_PROVINCIA_FORN);
    __sqoop$field_map.put("T_NAZIONE_FORN", this.T_NAZIONE_FORN);
    __sqoop$field_map.put("ALTRO_IND_FORN", this.ALTRO_IND_FORN);
    __sqoop$field_map.put("T_ACCESSO_UI", this.T_ACCESSO_UI);
    __sqoop$field_map.put("T_TIPO_FORNITURA", this.T_TIPO_FORNITURA);
    __sqoop$field_map.put("T_ALIQUOTA_IVA", this.T_ALIQUOTA_IVA);
    __sqoop$field_map.put("T_ALIQUOTA_ACCISE", this.T_ALIQUOTA_ACCISE);
    __sqoop$field_map.put("T_ADD_REGIONALE", this.T_ADD_REGIONALE);
    __sqoop$field_map.put("T_ALTRE_INFO_IMPOSTE", this.T_ALTRE_INFO_IMPOSTE);
    __sqoop$field_map.put("T_MATRICOLA_MISURATORE", this.T_MATRICOLA_MISURATORE);
    __sqoop$field_map.put("T_CLASSE_MISURATORE", this.T_CLASSE_MISURATORE);
    __sqoop$field_map.put("T_TIPO_MISURATORE", this.T_TIPO_MISURATORE);
    __sqoop$field_map.put("T_TELEGESTIONE", this.T_TELEGESTIONE);
    __sqoop$field_map.put("T_PRE_CONV", this.T_PRE_CONV);
    __sqoop$field_map.put("T_MATRICOLA_CONVERTITORE", this.T_MATRICOLA_CONVERTITORE);
    __sqoop$field_map.put("N_NUM_CIFRE_CONVERTITORE", this.N_NUM_CIFRE_CONVERTITORE);
    __sqoop$field_map.put("T_ANNO_FABBRIC_CONVERTITORE", this.T_ANNO_FABBRIC_CONVERTITORE);
    __sqoop$field_map.put("T_DATA_INST_CONVERTITORE", this.T_DATA_INST_CONVERTITORE);
    __sqoop$field_map.put("N_COEFF_CORREZIONE", this.N_COEFF_CORREZIONE);
    __sqoop$field_map.put("PRESS_MISURE", this.PRESS_MISURE);
    __sqoop$field_map.put("T_ACCESS_MISURATORE", this.T_ACCESS_MISURATORE);
    __sqoop$field_map.put("N_NUM_CIFRE_MISURATORE", this.N_NUM_CIFRE_MISURATORE);
    __sqoop$field_map.put("T_ANNO_FABBRIC_MISURATORE", this.T_ANNO_FABBRIC_MISURATORE);
    __sqoop$field_map.put("T_DATA_INST_MISURATORE", this.T_DATA_INST_MISURATORE);
    __sqoop$field_map.put("T_MISURATORE_INTEGRATO", this.T_MISURATORE_INTEGRATO);
    __sqoop$field_map.put("N_POTENZIALITA_MASSIMA", this.N_POTENZIALITA_MASSIMA);
    __sqoop$field_map.put("N_POTENZIALITA_TOT_INSTALLATA", this.N_POTENZIALITA_TOT_INSTALLATA);
    __sqoop$field_map.put("N_MAX_PRELIEVO_ORARIO", this.N_MAX_PRELIEVO_ORARIO);
    __sqoop$field_map.put("T_EROG_SERVIZIO_ENERG", this.T_EROG_SERVIZIO_ENERG);
    __sqoop$field_map.put("T_PARTITA_IVA_GESTCAL", this.T_PARTITA_IVA_GESTCAL);
    __sqoop$field_map.put("T_RAGIONE_SOCIALE_GESTCAL", this.T_RAGIONE_SOCIALE_GESTCAL);
    __sqoop$field_map.put("T_TELEFONO_GESTCAL", this.T_TELEFONO_GESTCAL);
    __sqoop$field_map.put("T_EMAIL_GESTCAL", this.T_EMAIL_GESTCAL);
    __sqoop$field_map.put("T_TOPONIMO_GESTCAL", this.T_TOPONIMO_GESTCAL);
    __sqoop$field_map.put("T_NOMESTRADA_GESTCAL", this.T_NOMESTRADA_GESTCAL);
    __sqoop$field_map.put("T_CIVICO_GESTCAL", this.T_CIVICO_GESTCAL);
    __sqoop$field_map.put("T_CAP_GESTCAL", this.T_CAP_GESTCAL);
    __sqoop$field_map.put("T_COMUNE_ISTAT_GESTCAL", this.T_COMUNE_ISTAT_GESTCAL);
    __sqoop$field_map.put("T_COMUNE_GESTCAL", this.T_COMUNE_GESTCAL);
    __sqoop$field_map.put("T_PROVINCIA_GESTCAL", this.T_PROVINCIA_GESTCAL);
    __sqoop$field_map.put("T_NAZIONE_GESTCAL", this.T_NAZIONE_GESTCAL);
    __sqoop$field_map.put("D_DATA_RIF_PDR", this.D_DATA_RIF_PDR);
    __sqoop$field_map.put("D_AGGIORNAMENTO_PDR", this.D_AGGIORNAMENTO_PDR);
    __sqoop$field_map.put("D_DATA_RIF_TECN", this.D_DATA_RIF_TECN);
    __sqoop$field_map.put("D_AGGIORNAMENTO_TECN", this.D_AGGIORNAMENTO_TECN);
    __sqoop$field_map.put("D_DATA_RIF_MIS", this.D_DATA_RIF_MIS);
    __sqoop$field_map.put("D_AGGIORNAMENTO_MIS", this.D_AGGIORNAMENTO_MIS);
    __sqoop$field_map.put("D_DATA_RIF_FORN", this.D_DATA_RIF_FORN);
    __sqoop$field_map.put("D_AGGIORNAMENTO_FORN", this.D_AGGIORNAMENTO_FORN);
    __sqoop$field_map.put("T_TIPO_BONUS", this.T_TIPO_BONUS);
    __sqoop$field_map.put("D_DATA_INIZIO_EROG_BONUS", this.D_DATA_INIZIO_EROG_BONUS);
    __sqoop$field_map.put("D_DATA_FINE_EROG_BONUS", this.D_DATA_FINE_EROG_BONUS);
    __sqoop$field_map.put("D_DATA_RIF_BONUS", this.D_DATA_RIF_BONUS);
    __sqoop$field_map.put("D_AGGIORNAMENTO_BONUS", this.D_AGGIORNAMENTO_BONUS);
    __sqoop$field_map.put("D_DATA_AGGIORNAMENTO", this.D_DATA_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_UDD", this.N_ID_UDD);
    __sqoop$field_map.put("N_ID_VENDITORE", this.N_ID_VENDITORE);
    __sqoop$field_map.put("T_COD_PROFILO", this.T_COD_PROFILO);
    __sqoop$field_map.put("T_COD_CAT_USO", this.T_COD_CAT_USO);
    __sqoop$field_map.put("T_COD_CLASSE_PRELIEVO", this.T_COD_CLASSE_PRELIEVO);
    __sqoop$field_map.put("T_ANNO_TERMICO", this.T_ANNO_TERMICO);
    __sqoop$field_map.put("D_DATA_RIF_PREL", this.D_DATA_RIF_PREL);
    __sqoop$field_map.put("T_TRATTAMENTO", this.T_TRATTAMENTO);
    __sqoop$field_map.put("T_TOPONIMO_ESAZ", this.T_TOPONIMO_ESAZ);
    __sqoop$field_map.put("T_NOMESTRADA_ESAZ", this.T_NOMESTRADA_ESAZ);
    __sqoop$field_map.put("T_CIVICO_ESAZ", this.T_CIVICO_ESAZ);
    __sqoop$field_map.put("T_CAP_ESAZ", this.T_CAP_ESAZ);
    __sqoop$field_map.put("T_COMUNE_ISTAT_ESAZ", this.T_COMUNE_ISTAT_ESAZ);
    __sqoop$field_map.put("T_COMUNE_ESAZ", this.T_COMUNE_ESAZ);
    __sqoop$field_map.put("T_PROVINCIA_ESAZ", this.T_PROVINCIA_ESAZ);
    __sqoop$field_map.put("T_NAZIONE_ESAZ", this.T_NAZIONE_ESAZ);
    __sqoop$field_map.put("ALTRO_IND_ESAZ", this.ALTRO_IND_ESAZ);
    __sqoop$field_map.put("T_CODICE_ATECO", this.T_CODICE_ATECO);
    __sqoop$field_map.put("T_PAGAMENTO_IVA", this.T_PAGAMENTO_IVA);
    __sqoop$field_map.put("T_CODICE_UFFICIO", this.T_CODICE_UFFICIO);
    __sqoop$field_map.put("T_CF_INTESTATARIO_FATT", this.T_CF_INTESTATARIO_FATT);
    __sqoop$field_map.put("T_CF_STRANIERO_FATT", this.T_CF_STRANIERO_FATT);
    __sqoop$field_map.put("T_PIVA_INTESTATARIO_FATT", this.T_PIVA_INTESTATARIO_FATT);
    __sqoop$field_map.put("T_NOME_INTESTATARIO_FATT", this.T_NOME_INTESTATARIO_FATT);
    __sqoop$field_map.put("T_COGNOME_INTESTATARIO_FATT", this.T_COGNOME_INTESTATARIO_FATT);
    __sqoop$field_map.put("T_RAG_SOC_INTESTATARIO_FATT", this.T_RAG_SOC_INTESTATARIO_FATT);
    __sqoop$field_map.put("T_ANNO_MESE_RINN_BONUS", this.T_ANNO_MESE_RINN_BONUS);
    __sqoop$field_map.put("D_DATA_INIZIO_BONUS", this.D_DATA_INIZIO_BONUS);
    __sqoop$field_map.put("D_DATA_FINE_BONUS", this.D_DATA_FINE_BONUS);
    __sqoop$field_map.put("N_PRELIEVO_ANNUO", this.N_PRELIEVO_ANNUO);
    __sqoop$field_map.put("T_FATTORE_CORREZ_CLIMATICA", this.T_FATTORE_CORREZ_CLIMATICA);
    __sqoop$field_map.put("T_ALTRO_IND_GESTCAL", this.T_ALTRO_IND_GESTCAL);
    __sqoop$field_map.put("T_TIPO_OP", this.T_TIPO_OP);
    __sqoop$field_map.put("T_PROCESSO", this.T_PROCESSO);
    __sqoop$field_map.put("N_ID_PRATICA_PROCESSO", this.N_ID_PRATICA_PROCESSO);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_PDR", this.N_ID_PDR);
    __sqoop$field_map.put("T_CODICE_PDR", this.T_CODICE_PDR);
    __sqoop$field_map.put("CAPACITA_TRASPORTO", this.CAPACITA_TRASPORTO);
    __sqoop$field_map.put("MESE_VAL_CAP_TRASP", this.MESE_VAL_CAP_TRASP);
    __sqoop$field_map.put("T_COD_TIPO_PDR", this.T_COD_TIPO_PDR);
    __sqoop$field_map.put("T_DISALIMENTABILITA", this.T_DISALIMENTABILITA);
    __sqoop$field_map.put("BILANCIAMENTO", this.BILANCIAMENTO);
    __sqoop$field_map.put("N_ID_FORNITURA", this.N_ID_FORNITURA);
    __sqoop$field_map.put("D_DATA_INIZIO_FOR", this.D_DATA_INIZIO_FOR);
    __sqoop$field_map.put("DATA_FINE_FOR", this.DATA_FINE_FOR);
    __sqoop$field_map.put("N_ID_AZ_UDD", this.N_ID_AZ_UDD);
    __sqoop$field_map.put("PIVA_UDD", this.PIVA_UDD);
    __sqoop$field_map.put("N_ID_AZ_CC", this.N_ID_AZ_CC);
    __sqoop$field_map.put("PIVA_CC", this.PIVA_CC);
    __sqoop$field_map.put("N_ID_CLIENTE", this.N_ID_CLIENTE);
    __sqoop$field_map.put("T_PARTITA_IVA_CLI", this.T_PARTITA_IVA_CLI);
    __sqoop$field_map.put("T_CODICE_FISCALE_CLI", this.T_CODICE_FISCALE_CLI);
    __sqoop$field_map.put("B_CF_STRANIERO", this.B_CF_STRANIERO);
    __sqoop$field_map.put("T_REFERENTE", this.T_REFERENTE);
    __sqoop$field_map.put("T_NOME_REF", this.T_NOME_REF);
    __sqoop$field_map.put("T_COGNOME_REF", this.T_COGNOME_REF);
    __sqoop$field_map.put("T_EMAIL_REF", this.T_EMAIL_REF);
    __sqoop$field_map.put("T_TELEFONO_REF", this.T_TELEFONO_REF);
    __sqoop$field_map.put("T_RESIDENZA", this.T_RESIDENZA);
    __sqoop$field_map.put("DATA_VAL_RES", this.DATA_VAL_RES);
    __sqoop$field_map.put("T_TOPONIMOPDR", this.T_TOPONIMOPDR);
    __sqoop$field_map.put("T_NOMESTRADA_PDR", this.T_NOMESTRADA_PDR);
    __sqoop$field_map.put("T_CIVICO_PDR", this.T_CIVICO_PDR);
    __sqoop$field_map.put("T_CAP_PDR", this.T_CAP_PDR);
    __sqoop$field_map.put("T_COMUNE_ISTAT_PDR", this.T_COMUNE_ISTAT_PDR);
    __sqoop$field_map.put("T_COMUNE_PDR", this.T_COMUNE_PDR);
    __sqoop$field_map.put("T_PROVINCIA_PDR", this.T_PROVINCIA_PDR);
    __sqoop$field_map.put("T_NAZIONE_PDR", this.T_NAZIONE_PDR);
    __sqoop$field_map.put("ALTRO_IND_PDR", this.ALTRO_IND_PDR);
    __sqoop$field_map.put("T_TOPONIMO_FORN", this.T_TOPONIMO_FORN);
    __sqoop$field_map.put("T_NOMESTRADA_FORN", this.T_NOMESTRADA_FORN);
    __sqoop$field_map.put("T_CIVICO_FORN", this.T_CIVICO_FORN);
    __sqoop$field_map.put("T_CAP_FORN", this.T_CAP_FORN);
    __sqoop$field_map.put("T_COMUNE_ISTATFORN", this.T_COMUNE_ISTATFORN);
    __sqoop$field_map.put("T_COMUNE_FORN", this.T_COMUNE_FORN);
    __sqoop$field_map.put("T_PROVINCIA_FORN", this.T_PROVINCIA_FORN);
    __sqoop$field_map.put("T_NAZIONE_FORN", this.T_NAZIONE_FORN);
    __sqoop$field_map.put("ALTRO_IND_FORN", this.ALTRO_IND_FORN);
    __sqoop$field_map.put("T_ACCESSO_UI", this.T_ACCESSO_UI);
    __sqoop$field_map.put("T_TIPO_FORNITURA", this.T_TIPO_FORNITURA);
    __sqoop$field_map.put("T_ALIQUOTA_IVA", this.T_ALIQUOTA_IVA);
    __sqoop$field_map.put("T_ALIQUOTA_ACCISE", this.T_ALIQUOTA_ACCISE);
    __sqoop$field_map.put("T_ADD_REGIONALE", this.T_ADD_REGIONALE);
    __sqoop$field_map.put("T_ALTRE_INFO_IMPOSTE", this.T_ALTRE_INFO_IMPOSTE);
    __sqoop$field_map.put("T_MATRICOLA_MISURATORE", this.T_MATRICOLA_MISURATORE);
    __sqoop$field_map.put("T_CLASSE_MISURATORE", this.T_CLASSE_MISURATORE);
    __sqoop$field_map.put("T_TIPO_MISURATORE", this.T_TIPO_MISURATORE);
    __sqoop$field_map.put("T_TELEGESTIONE", this.T_TELEGESTIONE);
    __sqoop$field_map.put("T_PRE_CONV", this.T_PRE_CONV);
    __sqoop$field_map.put("T_MATRICOLA_CONVERTITORE", this.T_MATRICOLA_CONVERTITORE);
    __sqoop$field_map.put("N_NUM_CIFRE_CONVERTITORE", this.N_NUM_CIFRE_CONVERTITORE);
    __sqoop$field_map.put("T_ANNO_FABBRIC_CONVERTITORE", this.T_ANNO_FABBRIC_CONVERTITORE);
    __sqoop$field_map.put("T_DATA_INST_CONVERTITORE", this.T_DATA_INST_CONVERTITORE);
    __sqoop$field_map.put("N_COEFF_CORREZIONE", this.N_COEFF_CORREZIONE);
    __sqoop$field_map.put("PRESS_MISURE", this.PRESS_MISURE);
    __sqoop$field_map.put("T_ACCESS_MISURATORE", this.T_ACCESS_MISURATORE);
    __sqoop$field_map.put("N_NUM_CIFRE_MISURATORE", this.N_NUM_CIFRE_MISURATORE);
    __sqoop$field_map.put("T_ANNO_FABBRIC_MISURATORE", this.T_ANNO_FABBRIC_MISURATORE);
    __sqoop$field_map.put("T_DATA_INST_MISURATORE", this.T_DATA_INST_MISURATORE);
    __sqoop$field_map.put("T_MISURATORE_INTEGRATO", this.T_MISURATORE_INTEGRATO);
    __sqoop$field_map.put("N_POTENZIALITA_MASSIMA", this.N_POTENZIALITA_MASSIMA);
    __sqoop$field_map.put("N_POTENZIALITA_TOT_INSTALLATA", this.N_POTENZIALITA_TOT_INSTALLATA);
    __sqoop$field_map.put("N_MAX_PRELIEVO_ORARIO", this.N_MAX_PRELIEVO_ORARIO);
    __sqoop$field_map.put("T_EROG_SERVIZIO_ENERG", this.T_EROG_SERVIZIO_ENERG);
    __sqoop$field_map.put("T_PARTITA_IVA_GESTCAL", this.T_PARTITA_IVA_GESTCAL);
    __sqoop$field_map.put("T_RAGIONE_SOCIALE_GESTCAL", this.T_RAGIONE_SOCIALE_GESTCAL);
    __sqoop$field_map.put("T_TELEFONO_GESTCAL", this.T_TELEFONO_GESTCAL);
    __sqoop$field_map.put("T_EMAIL_GESTCAL", this.T_EMAIL_GESTCAL);
    __sqoop$field_map.put("T_TOPONIMO_GESTCAL", this.T_TOPONIMO_GESTCAL);
    __sqoop$field_map.put("T_NOMESTRADA_GESTCAL", this.T_NOMESTRADA_GESTCAL);
    __sqoop$field_map.put("T_CIVICO_GESTCAL", this.T_CIVICO_GESTCAL);
    __sqoop$field_map.put("T_CAP_GESTCAL", this.T_CAP_GESTCAL);
    __sqoop$field_map.put("T_COMUNE_ISTAT_GESTCAL", this.T_COMUNE_ISTAT_GESTCAL);
    __sqoop$field_map.put("T_COMUNE_GESTCAL", this.T_COMUNE_GESTCAL);
    __sqoop$field_map.put("T_PROVINCIA_GESTCAL", this.T_PROVINCIA_GESTCAL);
    __sqoop$field_map.put("T_NAZIONE_GESTCAL", this.T_NAZIONE_GESTCAL);
    __sqoop$field_map.put("D_DATA_RIF_PDR", this.D_DATA_RIF_PDR);
    __sqoop$field_map.put("D_AGGIORNAMENTO_PDR", this.D_AGGIORNAMENTO_PDR);
    __sqoop$field_map.put("D_DATA_RIF_TECN", this.D_DATA_RIF_TECN);
    __sqoop$field_map.put("D_AGGIORNAMENTO_TECN", this.D_AGGIORNAMENTO_TECN);
    __sqoop$field_map.put("D_DATA_RIF_MIS", this.D_DATA_RIF_MIS);
    __sqoop$field_map.put("D_AGGIORNAMENTO_MIS", this.D_AGGIORNAMENTO_MIS);
    __sqoop$field_map.put("D_DATA_RIF_FORN", this.D_DATA_RIF_FORN);
    __sqoop$field_map.put("D_AGGIORNAMENTO_FORN", this.D_AGGIORNAMENTO_FORN);
    __sqoop$field_map.put("T_TIPO_BONUS", this.T_TIPO_BONUS);
    __sqoop$field_map.put("D_DATA_INIZIO_EROG_BONUS", this.D_DATA_INIZIO_EROG_BONUS);
    __sqoop$field_map.put("D_DATA_FINE_EROG_BONUS", this.D_DATA_FINE_EROG_BONUS);
    __sqoop$field_map.put("D_DATA_RIF_BONUS", this.D_DATA_RIF_BONUS);
    __sqoop$field_map.put("D_AGGIORNAMENTO_BONUS", this.D_AGGIORNAMENTO_BONUS);
    __sqoop$field_map.put("D_DATA_AGGIORNAMENTO", this.D_DATA_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_UDD", this.N_ID_UDD);
    __sqoop$field_map.put("N_ID_VENDITORE", this.N_ID_VENDITORE);
    __sqoop$field_map.put("T_COD_PROFILO", this.T_COD_PROFILO);
    __sqoop$field_map.put("T_COD_CAT_USO", this.T_COD_CAT_USO);
    __sqoop$field_map.put("T_COD_CLASSE_PRELIEVO", this.T_COD_CLASSE_PRELIEVO);
    __sqoop$field_map.put("T_ANNO_TERMICO", this.T_ANNO_TERMICO);
    __sqoop$field_map.put("D_DATA_RIF_PREL", this.D_DATA_RIF_PREL);
    __sqoop$field_map.put("T_TRATTAMENTO", this.T_TRATTAMENTO);
    __sqoop$field_map.put("T_TOPONIMO_ESAZ", this.T_TOPONIMO_ESAZ);
    __sqoop$field_map.put("T_NOMESTRADA_ESAZ", this.T_NOMESTRADA_ESAZ);
    __sqoop$field_map.put("T_CIVICO_ESAZ", this.T_CIVICO_ESAZ);
    __sqoop$field_map.put("T_CAP_ESAZ", this.T_CAP_ESAZ);
    __sqoop$field_map.put("T_COMUNE_ISTAT_ESAZ", this.T_COMUNE_ISTAT_ESAZ);
    __sqoop$field_map.put("T_COMUNE_ESAZ", this.T_COMUNE_ESAZ);
    __sqoop$field_map.put("T_PROVINCIA_ESAZ", this.T_PROVINCIA_ESAZ);
    __sqoop$field_map.put("T_NAZIONE_ESAZ", this.T_NAZIONE_ESAZ);
    __sqoop$field_map.put("ALTRO_IND_ESAZ", this.ALTRO_IND_ESAZ);
    __sqoop$field_map.put("T_CODICE_ATECO", this.T_CODICE_ATECO);
    __sqoop$field_map.put("T_PAGAMENTO_IVA", this.T_PAGAMENTO_IVA);
    __sqoop$field_map.put("T_CODICE_UFFICIO", this.T_CODICE_UFFICIO);
    __sqoop$field_map.put("T_CF_INTESTATARIO_FATT", this.T_CF_INTESTATARIO_FATT);
    __sqoop$field_map.put("T_CF_STRANIERO_FATT", this.T_CF_STRANIERO_FATT);
    __sqoop$field_map.put("T_PIVA_INTESTATARIO_FATT", this.T_PIVA_INTESTATARIO_FATT);
    __sqoop$field_map.put("T_NOME_INTESTATARIO_FATT", this.T_NOME_INTESTATARIO_FATT);
    __sqoop$field_map.put("T_COGNOME_INTESTATARIO_FATT", this.T_COGNOME_INTESTATARIO_FATT);
    __sqoop$field_map.put("T_RAG_SOC_INTESTATARIO_FATT", this.T_RAG_SOC_INTESTATARIO_FATT);
    __sqoop$field_map.put("T_ANNO_MESE_RINN_BONUS", this.T_ANNO_MESE_RINN_BONUS);
    __sqoop$field_map.put("D_DATA_INIZIO_BONUS", this.D_DATA_INIZIO_BONUS);
    __sqoop$field_map.put("D_DATA_FINE_BONUS", this.D_DATA_FINE_BONUS);
    __sqoop$field_map.put("N_PRELIEVO_ANNUO", this.N_PRELIEVO_ANNUO);
    __sqoop$field_map.put("T_FATTORE_CORREZ_CLIMATICA", this.T_FATTORE_CORREZ_CLIMATICA);
    __sqoop$field_map.put("T_ALTRO_IND_GESTCAL", this.T_ALTRO_IND_GESTCAL);
    __sqoop$field_map.put("T_TIPO_OP", this.T_TIPO_OP);
    __sqoop$field_map.put("T_PROCESSO", this.T_PROCESSO);
    __sqoop$field_map.put("N_ID_PRATICA_PROCESSO", this.N_ID_PRATICA_PROCESSO);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
