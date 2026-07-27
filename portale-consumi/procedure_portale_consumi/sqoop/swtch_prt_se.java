// ORM class for table 'swtch.prt_se'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 14:21:34 CEST 2019
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

public class swtch_prt_se extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_SE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_SE = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_PRATICA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_PRATICA = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_STATO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_STATO = (String)value;
      }
    });
    setters.put("T_PROT_RICH", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PROT_RICH = (String)value;
      }
    });
    setters.put("T_CODICE_POD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_POD = (String)value;
      }
    });
    setters.put("T_CLI_RCU_CF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CLI_RCU_CF = (String)value;
      }
    });
    setters.put("T_CLI_RCU_PIVA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CLI_RCU_PIVA = (String)value;
      }
    });
    setters.put("B_CLI_CF_STRANIERO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_CLI_CF_STRANIERO = (String)value;
      }
    });
    setters.put("B_CLI_PIVA_ESTERA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_CLI_PIVA_ESTERA = (String)value;
      }
    });
    setters.put("D_DATA_CONTRATTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_CONTRATTO = (String)value;
      }
    });
    setters.put("D_DATA_DECORRENZA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_DECORRENZA = (String)value;
      }
    });
    setters.put("B_REVOCA_TIMOE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_REVOCA_TIMOE = (String)value;
      }
    });
    setters.put("B_ACQUISTO_CREDITO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_ACQUISTO_CREDITO = (String)value;
      }
    });
    setters.put("T_COD_CONTR_DISP", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COD_CONTR_DISP = (String)value;
      }
    });
    setters.put("T_CONTR_CONNESSIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CONTR_CONNESSIONE = (String)value;
      }
    });
    setters.put("B_AMMISSIBILE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_AMMISSIBILE = (String)value;
      }
    });
    setters.put("T_COD_CAUSALE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COD_CAUSALE = (String)value;
      }
    });
    setters.put("T_MOTIVAZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MOTIVAZIONE = (String)value;
      }
    });
    setters.put("N_ID_RICH", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_RICH = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_RUOLO_RICH", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_RUOLO_RICH = (String)value;
      }
    });
    setters.put("N_ID_DISTR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_DISTR = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_UDD_U", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_UDD_U = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_CC_U", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_CC_U = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_CC_E", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_CC_E = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_PROT_DISTR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PROT_DISTR = (String)value;
      }
    });
    setters.put("B_DATI_TIMOE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_DATI_TIMOE = (String)value;
      }
    });
    setters.put("T_COD_ESITO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COD_ESITO = (String)value;
      }
    });
    setters.put("T_DETT_ESITO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_DETT_ESITO = (String)value;
      }
    });
    setters.put("D_DATA_FLUSSO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_FLUSSO = (String)value;
      }
    });
    setters.put("D_DATA_STATO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_STATO = (String)value;
      }
    });
    setters.put("B_INVALIDATA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_INVALIDATA = (String)value;
      }
    });
    setters.put("N_ID_CLIENTE_RCU", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_CLIENTE_RCU = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_RIFERIMENTO_EVENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_RIFERIMENTO_EVENTO = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_TIPO_EVENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_TIPO_EVENTO = (java.math.BigDecimal)value;
      }
    });
    setters.put("B_INFRAMESE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_INFRAMESE = (String)value;
      }
    });
  }
  public swtch_prt_se() {
    init0();
  }
  private java.math.BigDecimal N_ID_SE;
  public java.math.BigDecimal get_N_ID_SE() {
    return N_ID_SE;
  }
  public void set_N_ID_SE(java.math.BigDecimal N_ID_SE) {
    this.N_ID_SE = N_ID_SE;
  }
  public swtch_prt_se with_N_ID_SE(java.math.BigDecimal N_ID_SE) {
    this.N_ID_SE = N_ID_SE;
    return this;
  }
  private java.math.BigDecimal N_ID_PRATICA;
  public java.math.BigDecimal get_N_ID_PRATICA() {
    return N_ID_PRATICA;
  }
  public void set_N_ID_PRATICA(java.math.BigDecimal N_ID_PRATICA) {
    this.N_ID_PRATICA = N_ID_PRATICA;
  }
  public swtch_prt_se with_N_ID_PRATICA(java.math.BigDecimal N_ID_PRATICA) {
    this.N_ID_PRATICA = N_ID_PRATICA;
    return this;
  }
  private String T_STATO;
  public String get_T_STATO() {
    return T_STATO;
  }
  public void set_T_STATO(String T_STATO) {
    this.T_STATO = T_STATO;
  }
  public swtch_prt_se with_T_STATO(String T_STATO) {
    this.T_STATO = T_STATO;
    return this;
  }
  private String T_PROT_RICH;
  public String get_T_PROT_RICH() {
    return T_PROT_RICH;
  }
  public void set_T_PROT_RICH(String T_PROT_RICH) {
    this.T_PROT_RICH = T_PROT_RICH;
  }
  public swtch_prt_se with_T_PROT_RICH(String T_PROT_RICH) {
    this.T_PROT_RICH = T_PROT_RICH;
    return this;
  }
  private String T_CODICE_POD;
  public String get_T_CODICE_POD() {
    return T_CODICE_POD;
  }
  public void set_T_CODICE_POD(String T_CODICE_POD) {
    this.T_CODICE_POD = T_CODICE_POD;
  }
  public swtch_prt_se with_T_CODICE_POD(String T_CODICE_POD) {
    this.T_CODICE_POD = T_CODICE_POD;
    return this;
  }
  private String T_CLI_RCU_CF;
  public String get_T_CLI_RCU_CF() {
    return T_CLI_RCU_CF;
  }
  public void set_T_CLI_RCU_CF(String T_CLI_RCU_CF) {
    this.T_CLI_RCU_CF = T_CLI_RCU_CF;
  }
  public swtch_prt_se with_T_CLI_RCU_CF(String T_CLI_RCU_CF) {
    this.T_CLI_RCU_CF = T_CLI_RCU_CF;
    return this;
  }
  private String T_CLI_RCU_PIVA;
  public String get_T_CLI_RCU_PIVA() {
    return T_CLI_RCU_PIVA;
  }
  public void set_T_CLI_RCU_PIVA(String T_CLI_RCU_PIVA) {
    this.T_CLI_RCU_PIVA = T_CLI_RCU_PIVA;
  }
  public swtch_prt_se with_T_CLI_RCU_PIVA(String T_CLI_RCU_PIVA) {
    this.T_CLI_RCU_PIVA = T_CLI_RCU_PIVA;
    return this;
  }
  private String B_CLI_CF_STRANIERO;
  public String get_B_CLI_CF_STRANIERO() {
    return B_CLI_CF_STRANIERO;
  }
  public void set_B_CLI_CF_STRANIERO(String B_CLI_CF_STRANIERO) {
    this.B_CLI_CF_STRANIERO = B_CLI_CF_STRANIERO;
  }
  public swtch_prt_se with_B_CLI_CF_STRANIERO(String B_CLI_CF_STRANIERO) {
    this.B_CLI_CF_STRANIERO = B_CLI_CF_STRANIERO;
    return this;
  }
  private String B_CLI_PIVA_ESTERA;
  public String get_B_CLI_PIVA_ESTERA() {
    return B_CLI_PIVA_ESTERA;
  }
  public void set_B_CLI_PIVA_ESTERA(String B_CLI_PIVA_ESTERA) {
    this.B_CLI_PIVA_ESTERA = B_CLI_PIVA_ESTERA;
  }
  public swtch_prt_se with_B_CLI_PIVA_ESTERA(String B_CLI_PIVA_ESTERA) {
    this.B_CLI_PIVA_ESTERA = B_CLI_PIVA_ESTERA;
    return this;
  }
  private String D_DATA_CONTRATTO;
  public String get_D_DATA_CONTRATTO() {
    return D_DATA_CONTRATTO;
  }
  public void set_D_DATA_CONTRATTO(String D_DATA_CONTRATTO) {
    this.D_DATA_CONTRATTO = D_DATA_CONTRATTO;
  }
  public swtch_prt_se with_D_DATA_CONTRATTO(String D_DATA_CONTRATTO) {
    this.D_DATA_CONTRATTO = D_DATA_CONTRATTO;
    return this;
  }
  private String D_DATA_DECORRENZA;
  public String get_D_DATA_DECORRENZA() {
    return D_DATA_DECORRENZA;
  }
  public void set_D_DATA_DECORRENZA(String D_DATA_DECORRENZA) {
    this.D_DATA_DECORRENZA = D_DATA_DECORRENZA;
  }
  public swtch_prt_se with_D_DATA_DECORRENZA(String D_DATA_DECORRENZA) {
    this.D_DATA_DECORRENZA = D_DATA_DECORRENZA;
    return this;
  }
  private String B_REVOCA_TIMOE;
  public String get_B_REVOCA_TIMOE() {
    return B_REVOCA_TIMOE;
  }
  public void set_B_REVOCA_TIMOE(String B_REVOCA_TIMOE) {
    this.B_REVOCA_TIMOE = B_REVOCA_TIMOE;
  }
  public swtch_prt_se with_B_REVOCA_TIMOE(String B_REVOCA_TIMOE) {
    this.B_REVOCA_TIMOE = B_REVOCA_TIMOE;
    return this;
  }
  private String B_ACQUISTO_CREDITO;
  public String get_B_ACQUISTO_CREDITO() {
    return B_ACQUISTO_CREDITO;
  }
  public void set_B_ACQUISTO_CREDITO(String B_ACQUISTO_CREDITO) {
    this.B_ACQUISTO_CREDITO = B_ACQUISTO_CREDITO;
  }
  public swtch_prt_se with_B_ACQUISTO_CREDITO(String B_ACQUISTO_CREDITO) {
    this.B_ACQUISTO_CREDITO = B_ACQUISTO_CREDITO;
    return this;
  }
  private String T_COD_CONTR_DISP;
  public String get_T_COD_CONTR_DISP() {
    return T_COD_CONTR_DISP;
  }
  public void set_T_COD_CONTR_DISP(String T_COD_CONTR_DISP) {
    this.T_COD_CONTR_DISP = T_COD_CONTR_DISP;
  }
  public swtch_prt_se with_T_COD_CONTR_DISP(String T_COD_CONTR_DISP) {
    this.T_COD_CONTR_DISP = T_COD_CONTR_DISP;
    return this;
  }
  private String T_CONTR_CONNESSIONE;
  public String get_T_CONTR_CONNESSIONE() {
    return T_CONTR_CONNESSIONE;
  }
  public void set_T_CONTR_CONNESSIONE(String T_CONTR_CONNESSIONE) {
    this.T_CONTR_CONNESSIONE = T_CONTR_CONNESSIONE;
  }
  public swtch_prt_se with_T_CONTR_CONNESSIONE(String T_CONTR_CONNESSIONE) {
    this.T_CONTR_CONNESSIONE = T_CONTR_CONNESSIONE;
    return this;
  }
  private String B_AMMISSIBILE;
  public String get_B_AMMISSIBILE() {
    return B_AMMISSIBILE;
  }
  public void set_B_AMMISSIBILE(String B_AMMISSIBILE) {
    this.B_AMMISSIBILE = B_AMMISSIBILE;
  }
  public swtch_prt_se with_B_AMMISSIBILE(String B_AMMISSIBILE) {
    this.B_AMMISSIBILE = B_AMMISSIBILE;
    return this;
  }
  private String T_COD_CAUSALE;
  public String get_T_COD_CAUSALE() {
    return T_COD_CAUSALE;
  }
  public void set_T_COD_CAUSALE(String T_COD_CAUSALE) {
    this.T_COD_CAUSALE = T_COD_CAUSALE;
  }
  public swtch_prt_se with_T_COD_CAUSALE(String T_COD_CAUSALE) {
    this.T_COD_CAUSALE = T_COD_CAUSALE;
    return this;
  }
  private String T_MOTIVAZIONE;
  public String get_T_MOTIVAZIONE() {
    return T_MOTIVAZIONE;
  }
  public void set_T_MOTIVAZIONE(String T_MOTIVAZIONE) {
    this.T_MOTIVAZIONE = T_MOTIVAZIONE;
  }
  public swtch_prt_se with_T_MOTIVAZIONE(String T_MOTIVAZIONE) {
    this.T_MOTIVAZIONE = T_MOTIVAZIONE;
    return this;
  }
  private java.math.BigDecimal N_ID_RICH;
  public java.math.BigDecimal get_N_ID_RICH() {
    return N_ID_RICH;
  }
  public void set_N_ID_RICH(java.math.BigDecimal N_ID_RICH) {
    this.N_ID_RICH = N_ID_RICH;
  }
  public swtch_prt_se with_N_ID_RICH(java.math.BigDecimal N_ID_RICH) {
    this.N_ID_RICH = N_ID_RICH;
    return this;
  }
  private String T_RUOLO_RICH;
  public String get_T_RUOLO_RICH() {
    return T_RUOLO_RICH;
  }
  public void set_T_RUOLO_RICH(String T_RUOLO_RICH) {
    this.T_RUOLO_RICH = T_RUOLO_RICH;
  }
  public swtch_prt_se with_T_RUOLO_RICH(String T_RUOLO_RICH) {
    this.T_RUOLO_RICH = T_RUOLO_RICH;
    return this;
  }
  private java.math.BigDecimal N_ID_DISTR;
  public java.math.BigDecimal get_N_ID_DISTR() {
    return N_ID_DISTR;
  }
  public void set_N_ID_DISTR(java.math.BigDecimal N_ID_DISTR) {
    this.N_ID_DISTR = N_ID_DISTR;
  }
  public swtch_prt_se with_N_ID_DISTR(java.math.BigDecimal N_ID_DISTR) {
    this.N_ID_DISTR = N_ID_DISTR;
    return this;
  }
  private java.math.BigDecimal N_ID_UDD_U;
  public java.math.BigDecimal get_N_ID_UDD_U() {
    return N_ID_UDD_U;
  }
  public void set_N_ID_UDD_U(java.math.BigDecimal N_ID_UDD_U) {
    this.N_ID_UDD_U = N_ID_UDD_U;
  }
  public swtch_prt_se with_N_ID_UDD_U(java.math.BigDecimal N_ID_UDD_U) {
    this.N_ID_UDD_U = N_ID_UDD_U;
    return this;
  }
  private java.math.BigDecimal N_ID_CC_U;
  public java.math.BigDecimal get_N_ID_CC_U() {
    return N_ID_CC_U;
  }
  public void set_N_ID_CC_U(java.math.BigDecimal N_ID_CC_U) {
    this.N_ID_CC_U = N_ID_CC_U;
  }
  public swtch_prt_se with_N_ID_CC_U(java.math.BigDecimal N_ID_CC_U) {
    this.N_ID_CC_U = N_ID_CC_U;
    return this;
  }
  private java.math.BigDecimal N_ID_CC_E;
  public java.math.BigDecimal get_N_ID_CC_E() {
    return N_ID_CC_E;
  }
  public void set_N_ID_CC_E(java.math.BigDecimal N_ID_CC_E) {
    this.N_ID_CC_E = N_ID_CC_E;
  }
  public swtch_prt_se with_N_ID_CC_E(java.math.BigDecimal N_ID_CC_E) {
    this.N_ID_CC_E = N_ID_CC_E;
    return this;
  }
  private String T_PROT_DISTR;
  public String get_T_PROT_DISTR() {
    return T_PROT_DISTR;
  }
  public void set_T_PROT_DISTR(String T_PROT_DISTR) {
    this.T_PROT_DISTR = T_PROT_DISTR;
  }
  public swtch_prt_se with_T_PROT_DISTR(String T_PROT_DISTR) {
    this.T_PROT_DISTR = T_PROT_DISTR;
    return this;
  }
  private String B_DATI_TIMOE;
  public String get_B_DATI_TIMOE() {
    return B_DATI_TIMOE;
  }
  public void set_B_DATI_TIMOE(String B_DATI_TIMOE) {
    this.B_DATI_TIMOE = B_DATI_TIMOE;
  }
  public swtch_prt_se with_B_DATI_TIMOE(String B_DATI_TIMOE) {
    this.B_DATI_TIMOE = B_DATI_TIMOE;
    return this;
  }
  private String T_COD_ESITO;
  public String get_T_COD_ESITO() {
    return T_COD_ESITO;
  }
  public void set_T_COD_ESITO(String T_COD_ESITO) {
    this.T_COD_ESITO = T_COD_ESITO;
  }
  public swtch_prt_se with_T_COD_ESITO(String T_COD_ESITO) {
    this.T_COD_ESITO = T_COD_ESITO;
    return this;
  }
  private String T_DETT_ESITO;
  public String get_T_DETT_ESITO() {
    return T_DETT_ESITO;
  }
  public void set_T_DETT_ESITO(String T_DETT_ESITO) {
    this.T_DETT_ESITO = T_DETT_ESITO;
  }
  public swtch_prt_se with_T_DETT_ESITO(String T_DETT_ESITO) {
    this.T_DETT_ESITO = T_DETT_ESITO;
    return this;
  }
  private String D_DATA_FLUSSO;
  public String get_D_DATA_FLUSSO() {
    return D_DATA_FLUSSO;
  }
  public void set_D_DATA_FLUSSO(String D_DATA_FLUSSO) {
    this.D_DATA_FLUSSO = D_DATA_FLUSSO;
  }
  public swtch_prt_se with_D_DATA_FLUSSO(String D_DATA_FLUSSO) {
    this.D_DATA_FLUSSO = D_DATA_FLUSSO;
    return this;
  }
  private String D_DATA_STATO;
  public String get_D_DATA_STATO() {
    return D_DATA_STATO;
  }
  public void set_D_DATA_STATO(String D_DATA_STATO) {
    this.D_DATA_STATO = D_DATA_STATO;
  }
  public swtch_prt_se with_D_DATA_STATO(String D_DATA_STATO) {
    this.D_DATA_STATO = D_DATA_STATO;
    return this;
  }
  private String B_INVALIDATA;
  public String get_B_INVALIDATA() {
    return B_INVALIDATA;
  }
  public void set_B_INVALIDATA(String B_INVALIDATA) {
    this.B_INVALIDATA = B_INVALIDATA;
  }
  public swtch_prt_se with_B_INVALIDATA(String B_INVALIDATA) {
    this.B_INVALIDATA = B_INVALIDATA;
    return this;
  }
  private java.math.BigDecimal N_ID_CLIENTE_RCU;
  public java.math.BigDecimal get_N_ID_CLIENTE_RCU() {
    return N_ID_CLIENTE_RCU;
  }
  public void set_N_ID_CLIENTE_RCU(java.math.BigDecimal N_ID_CLIENTE_RCU) {
    this.N_ID_CLIENTE_RCU = N_ID_CLIENTE_RCU;
  }
  public swtch_prt_se with_N_ID_CLIENTE_RCU(java.math.BigDecimal N_ID_CLIENTE_RCU) {
    this.N_ID_CLIENTE_RCU = N_ID_CLIENTE_RCU;
    return this;
  }
  private java.math.BigDecimal N_ID_RIFERIMENTO_EVENTO;
  public java.math.BigDecimal get_N_ID_RIFERIMENTO_EVENTO() {
    return N_ID_RIFERIMENTO_EVENTO;
  }
  public void set_N_ID_RIFERIMENTO_EVENTO(java.math.BigDecimal N_ID_RIFERIMENTO_EVENTO) {
    this.N_ID_RIFERIMENTO_EVENTO = N_ID_RIFERIMENTO_EVENTO;
  }
  public swtch_prt_se with_N_ID_RIFERIMENTO_EVENTO(java.math.BigDecimal N_ID_RIFERIMENTO_EVENTO) {
    this.N_ID_RIFERIMENTO_EVENTO = N_ID_RIFERIMENTO_EVENTO;
    return this;
  }
  private java.math.BigDecimal N_ID_TIPO_EVENTO;
  public java.math.BigDecimal get_N_ID_TIPO_EVENTO() {
    return N_ID_TIPO_EVENTO;
  }
  public void set_N_ID_TIPO_EVENTO(java.math.BigDecimal N_ID_TIPO_EVENTO) {
    this.N_ID_TIPO_EVENTO = N_ID_TIPO_EVENTO;
  }
  public swtch_prt_se with_N_ID_TIPO_EVENTO(java.math.BigDecimal N_ID_TIPO_EVENTO) {
    this.N_ID_TIPO_EVENTO = N_ID_TIPO_EVENTO;
    return this;
  }
  private String B_INFRAMESE;
  public String get_B_INFRAMESE() {
    return B_INFRAMESE;
  }
  public void set_B_INFRAMESE(String B_INFRAMESE) {
    this.B_INFRAMESE = B_INFRAMESE;
  }
  public swtch_prt_se with_B_INFRAMESE(String B_INFRAMESE) {
    this.B_INFRAMESE = B_INFRAMESE;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof swtch_prt_se)) {
      return false;
    }
    swtch_prt_se that = (swtch_prt_se) o;
    boolean equal = true;
    equal = equal && (this.N_ID_SE == null ? that.N_ID_SE == null : this.N_ID_SE.equals(that.N_ID_SE));
    equal = equal && (this.N_ID_PRATICA == null ? that.N_ID_PRATICA == null : this.N_ID_PRATICA.equals(that.N_ID_PRATICA));
    equal = equal && (this.T_STATO == null ? that.T_STATO == null : this.T_STATO.equals(that.T_STATO));
    equal = equal && (this.T_PROT_RICH == null ? that.T_PROT_RICH == null : this.T_PROT_RICH.equals(that.T_PROT_RICH));
    equal = equal && (this.T_CODICE_POD == null ? that.T_CODICE_POD == null : this.T_CODICE_POD.equals(that.T_CODICE_POD));
    equal = equal && (this.T_CLI_RCU_CF == null ? that.T_CLI_RCU_CF == null : this.T_CLI_RCU_CF.equals(that.T_CLI_RCU_CF));
    equal = equal && (this.T_CLI_RCU_PIVA == null ? that.T_CLI_RCU_PIVA == null : this.T_CLI_RCU_PIVA.equals(that.T_CLI_RCU_PIVA));
    equal = equal && (this.B_CLI_CF_STRANIERO == null ? that.B_CLI_CF_STRANIERO == null : this.B_CLI_CF_STRANIERO.equals(that.B_CLI_CF_STRANIERO));
    equal = equal && (this.B_CLI_PIVA_ESTERA == null ? that.B_CLI_PIVA_ESTERA == null : this.B_CLI_PIVA_ESTERA.equals(that.B_CLI_PIVA_ESTERA));
    equal = equal && (this.D_DATA_CONTRATTO == null ? that.D_DATA_CONTRATTO == null : this.D_DATA_CONTRATTO.equals(that.D_DATA_CONTRATTO));
    equal = equal && (this.D_DATA_DECORRENZA == null ? that.D_DATA_DECORRENZA == null : this.D_DATA_DECORRENZA.equals(that.D_DATA_DECORRENZA));
    equal = equal && (this.B_REVOCA_TIMOE == null ? that.B_REVOCA_TIMOE == null : this.B_REVOCA_TIMOE.equals(that.B_REVOCA_TIMOE));
    equal = equal && (this.B_ACQUISTO_CREDITO == null ? that.B_ACQUISTO_CREDITO == null : this.B_ACQUISTO_CREDITO.equals(that.B_ACQUISTO_CREDITO));
    equal = equal && (this.T_COD_CONTR_DISP == null ? that.T_COD_CONTR_DISP == null : this.T_COD_CONTR_DISP.equals(that.T_COD_CONTR_DISP));
    equal = equal && (this.T_CONTR_CONNESSIONE == null ? that.T_CONTR_CONNESSIONE == null : this.T_CONTR_CONNESSIONE.equals(that.T_CONTR_CONNESSIONE));
    equal = equal && (this.B_AMMISSIBILE == null ? that.B_AMMISSIBILE == null : this.B_AMMISSIBILE.equals(that.B_AMMISSIBILE));
    equal = equal && (this.T_COD_CAUSALE == null ? that.T_COD_CAUSALE == null : this.T_COD_CAUSALE.equals(that.T_COD_CAUSALE));
    equal = equal && (this.T_MOTIVAZIONE == null ? that.T_MOTIVAZIONE == null : this.T_MOTIVAZIONE.equals(that.T_MOTIVAZIONE));
    equal = equal && (this.N_ID_RICH == null ? that.N_ID_RICH == null : this.N_ID_RICH.equals(that.N_ID_RICH));
    equal = equal && (this.T_RUOLO_RICH == null ? that.T_RUOLO_RICH == null : this.T_RUOLO_RICH.equals(that.T_RUOLO_RICH));
    equal = equal && (this.N_ID_DISTR == null ? that.N_ID_DISTR == null : this.N_ID_DISTR.equals(that.N_ID_DISTR));
    equal = equal && (this.N_ID_UDD_U == null ? that.N_ID_UDD_U == null : this.N_ID_UDD_U.equals(that.N_ID_UDD_U));
    equal = equal && (this.N_ID_CC_U == null ? that.N_ID_CC_U == null : this.N_ID_CC_U.equals(that.N_ID_CC_U));
    equal = equal && (this.N_ID_CC_E == null ? that.N_ID_CC_E == null : this.N_ID_CC_E.equals(that.N_ID_CC_E));
    equal = equal && (this.T_PROT_DISTR == null ? that.T_PROT_DISTR == null : this.T_PROT_DISTR.equals(that.T_PROT_DISTR));
    equal = equal && (this.B_DATI_TIMOE == null ? that.B_DATI_TIMOE == null : this.B_DATI_TIMOE.equals(that.B_DATI_TIMOE));
    equal = equal && (this.T_COD_ESITO == null ? that.T_COD_ESITO == null : this.T_COD_ESITO.equals(that.T_COD_ESITO));
    equal = equal && (this.T_DETT_ESITO == null ? that.T_DETT_ESITO == null : this.T_DETT_ESITO.equals(that.T_DETT_ESITO));
    equal = equal && (this.D_DATA_FLUSSO == null ? that.D_DATA_FLUSSO == null : this.D_DATA_FLUSSO.equals(that.D_DATA_FLUSSO));
    equal = equal && (this.D_DATA_STATO == null ? that.D_DATA_STATO == null : this.D_DATA_STATO.equals(that.D_DATA_STATO));
    equal = equal && (this.B_INVALIDATA == null ? that.B_INVALIDATA == null : this.B_INVALIDATA.equals(that.B_INVALIDATA));
    equal = equal && (this.N_ID_CLIENTE_RCU == null ? that.N_ID_CLIENTE_RCU == null : this.N_ID_CLIENTE_RCU.equals(that.N_ID_CLIENTE_RCU));
    equal = equal && (this.N_ID_RIFERIMENTO_EVENTO == null ? that.N_ID_RIFERIMENTO_EVENTO == null : this.N_ID_RIFERIMENTO_EVENTO.equals(that.N_ID_RIFERIMENTO_EVENTO));
    equal = equal && (this.N_ID_TIPO_EVENTO == null ? that.N_ID_TIPO_EVENTO == null : this.N_ID_TIPO_EVENTO.equals(that.N_ID_TIPO_EVENTO));
    equal = equal && (this.B_INFRAMESE == null ? that.B_INFRAMESE == null : this.B_INFRAMESE.equals(that.B_INFRAMESE));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof swtch_prt_se)) {
      return false;
    }
    swtch_prt_se that = (swtch_prt_se) o;
    boolean equal = true;
    equal = equal && (this.N_ID_SE == null ? that.N_ID_SE == null : this.N_ID_SE.equals(that.N_ID_SE));
    equal = equal && (this.N_ID_PRATICA == null ? that.N_ID_PRATICA == null : this.N_ID_PRATICA.equals(that.N_ID_PRATICA));
    equal = equal && (this.T_STATO == null ? that.T_STATO == null : this.T_STATO.equals(that.T_STATO));
    equal = equal && (this.T_PROT_RICH == null ? that.T_PROT_RICH == null : this.T_PROT_RICH.equals(that.T_PROT_RICH));
    equal = equal && (this.T_CODICE_POD == null ? that.T_CODICE_POD == null : this.T_CODICE_POD.equals(that.T_CODICE_POD));
    equal = equal && (this.T_CLI_RCU_CF == null ? that.T_CLI_RCU_CF == null : this.T_CLI_RCU_CF.equals(that.T_CLI_RCU_CF));
    equal = equal && (this.T_CLI_RCU_PIVA == null ? that.T_CLI_RCU_PIVA == null : this.T_CLI_RCU_PIVA.equals(that.T_CLI_RCU_PIVA));
    equal = equal && (this.B_CLI_CF_STRANIERO == null ? that.B_CLI_CF_STRANIERO == null : this.B_CLI_CF_STRANIERO.equals(that.B_CLI_CF_STRANIERO));
    equal = equal && (this.B_CLI_PIVA_ESTERA == null ? that.B_CLI_PIVA_ESTERA == null : this.B_CLI_PIVA_ESTERA.equals(that.B_CLI_PIVA_ESTERA));
    equal = equal && (this.D_DATA_CONTRATTO == null ? that.D_DATA_CONTRATTO == null : this.D_DATA_CONTRATTO.equals(that.D_DATA_CONTRATTO));
    equal = equal && (this.D_DATA_DECORRENZA == null ? that.D_DATA_DECORRENZA == null : this.D_DATA_DECORRENZA.equals(that.D_DATA_DECORRENZA));
    equal = equal && (this.B_REVOCA_TIMOE == null ? that.B_REVOCA_TIMOE == null : this.B_REVOCA_TIMOE.equals(that.B_REVOCA_TIMOE));
    equal = equal && (this.B_ACQUISTO_CREDITO == null ? that.B_ACQUISTO_CREDITO == null : this.B_ACQUISTO_CREDITO.equals(that.B_ACQUISTO_CREDITO));
    equal = equal && (this.T_COD_CONTR_DISP == null ? that.T_COD_CONTR_DISP == null : this.T_COD_CONTR_DISP.equals(that.T_COD_CONTR_DISP));
    equal = equal && (this.T_CONTR_CONNESSIONE == null ? that.T_CONTR_CONNESSIONE == null : this.T_CONTR_CONNESSIONE.equals(that.T_CONTR_CONNESSIONE));
    equal = equal && (this.B_AMMISSIBILE == null ? that.B_AMMISSIBILE == null : this.B_AMMISSIBILE.equals(that.B_AMMISSIBILE));
    equal = equal && (this.T_COD_CAUSALE == null ? that.T_COD_CAUSALE == null : this.T_COD_CAUSALE.equals(that.T_COD_CAUSALE));
    equal = equal && (this.T_MOTIVAZIONE == null ? that.T_MOTIVAZIONE == null : this.T_MOTIVAZIONE.equals(that.T_MOTIVAZIONE));
    equal = equal && (this.N_ID_RICH == null ? that.N_ID_RICH == null : this.N_ID_RICH.equals(that.N_ID_RICH));
    equal = equal && (this.T_RUOLO_RICH == null ? that.T_RUOLO_RICH == null : this.T_RUOLO_RICH.equals(that.T_RUOLO_RICH));
    equal = equal && (this.N_ID_DISTR == null ? that.N_ID_DISTR == null : this.N_ID_DISTR.equals(that.N_ID_DISTR));
    equal = equal && (this.N_ID_UDD_U == null ? that.N_ID_UDD_U == null : this.N_ID_UDD_U.equals(that.N_ID_UDD_U));
    equal = equal && (this.N_ID_CC_U == null ? that.N_ID_CC_U == null : this.N_ID_CC_U.equals(that.N_ID_CC_U));
    equal = equal && (this.N_ID_CC_E == null ? that.N_ID_CC_E == null : this.N_ID_CC_E.equals(that.N_ID_CC_E));
    equal = equal && (this.T_PROT_DISTR == null ? that.T_PROT_DISTR == null : this.T_PROT_DISTR.equals(that.T_PROT_DISTR));
    equal = equal && (this.B_DATI_TIMOE == null ? that.B_DATI_TIMOE == null : this.B_DATI_TIMOE.equals(that.B_DATI_TIMOE));
    equal = equal && (this.T_COD_ESITO == null ? that.T_COD_ESITO == null : this.T_COD_ESITO.equals(that.T_COD_ESITO));
    equal = equal && (this.T_DETT_ESITO == null ? that.T_DETT_ESITO == null : this.T_DETT_ESITO.equals(that.T_DETT_ESITO));
    equal = equal && (this.D_DATA_FLUSSO == null ? that.D_DATA_FLUSSO == null : this.D_DATA_FLUSSO.equals(that.D_DATA_FLUSSO));
    equal = equal && (this.D_DATA_STATO == null ? that.D_DATA_STATO == null : this.D_DATA_STATO.equals(that.D_DATA_STATO));
    equal = equal && (this.B_INVALIDATA == null ? that.B_INVALIDATA == null : this.B_INVALIDATA.equals(that.B_INVALIDATA));
    equal = equal && (this.N_ID_CLIENTE_RCU == null ? that.N_ID_CLIENTE_RCU == null : this.N_ID_CLIENTE_RCU.equals(that.N_ID_CLIENTE_RCU));
    equal = equal && (this.N_ID_RIFERIMENTO_EVENTO == null ? that.N_ID_RIFERIMENTO_EVENTO == null : this.N_ID_RIFERIMENTO_EVENTO.equals(that.N_ID_RIFERIMENTO_EVENTO));
    equal = equal && (this.N_ID_TIPO_EVENTO == null ? that.N_ID_TIPO_EVENTO == null : this.N_ID_TIPO_EVENTO.equals(that.N_ID_TIPO_EVENTO));
    equal = equal && (this.B_INFRAMESE == null ? that.B_INFRAMESE == null : this.B_INFRAMESE.equals(that.B_INFRAMESE));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_SE = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_PRATICA = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_STATO = JdbcWritableBridge.readString(3, __dbResults);
    this.T_PROT_RICH = JdbcWritableBridge.readString(4, __dbResults);
    this.T_CODICE_POD = JdbcWritableBridge.readString(5, __dbResults);
    this.T_CLI_RCU_CF = JdbcWritableBridge.readString(6, __dbResults);
    this.T_CLI_RCU_PIVA = JdbcWritableBridge.readString(7, __dbResults);
    this.B_CLI_CF_STRANIERO = JdbcWritableBridge.readString(8, __dbResults);
    this.B_CLI_PIVA_ESTERA = JdbcWritableBridge.readString(9, __dbResults);
    this.D_DATA_CONTRATTO = JdbcWritableBridge.readString(10, __dbResults);
    this.D_DATA_DECORRENZA = JdbcWritableBridge.readString(11, __dbResults);
    this.B_REVOCA_TIMOE = JdbcWritableBridge.readString(12, __dbResults);
    this.B_ACQUISTO_CREDITO = JdbcWritableBridge.readString(13, __dbResults);
    this.T_COD_CONTR_DISP = JdbcWritableBridge.readString(14, __dbResults);
    this.T_CONTR_CONNESSIONE = JdbcWritableBridge.readString(15, __dbResults);
    this.B_AMMISSIBILE = JdbcWritableBridge.readString(16, __dbResults);
    this.T_COD_CAUSALE = JdbcWritableBridge.readString(17, __dbResults);
    this.T_MOTIVAZIONE = JdbcWritableBridge.readString(18, __dbResults);
    this.N_ID_RICH = JdbcWritableBridge.readBigDecimal(19, __dbResults);
    this.T_RUOLO_RICH = JdbcWritableBridge.readString(20, __dbResults);
    this.N_ID_DISTR = JdbcWritableBridge.readBigDecimal(21, __dbResults);
    this.N_ID_UDD_U = JdbcWritableBridge.readBigDecimal(22, __dbResults);
    this.N_ID_CC_U = JdbcWritableBridge.readBigDecimal(23, __dbResults);
    this.N_ID_CC_E = JdbcWritableBridge.readBigDecimal(24, __dbResults);
    this.T_PROT_DISTR = JdbcWritableBridge.readString(25, __dbResults);
    this.B_DATI_TIMOE = JdbcWritableBridge.readString(26, __dbResults);
    this.T_COD_ESITO = JdbcWritableBridge.readString(27, __dbResults);
    this.T_DETT_ESITO = JdbcWritableBridge.readString(28, __dbResults);
    this.D_DATA_FLUSSO = JdbcWritableBridge.readString(29, __dbResults);
    this.D_DATA_STATO = JdbcWritableBridge.readString(30, __dbResults);
    this.B_INVALIDATA = JdbcWritableBridge.readString(31, __dbResults);
    this.N_ID_CLIENTE_RCU = JdbcWritableBridge.readBigDecimal(32, __dbResults);
    this.N_ID_RIFERIMENTO_EVENTO = JdbcWritableBridge.readBigDecimal(33, __dbResults);
    this.N_ID_TIPO_EVENTO = JdbcWritableBridge.readBigDecimal(34, __dbResults);
    this.B_INFRAMESE = JdbcWritableBridge.readString(35, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_SE = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_PRATICA = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_STATO = JdbcWritableBridge.readString(3, __dbResults);
    this.T_PROT_RICH = JdbcWritableBridge.readString(4, __dbResults);
    this.T_CODICE_POD = JdbcWritableBridge.readString(5, __dbResults);
    this.T_CLI_RCU_CF = JdbcWritableBridge.readString(6, __dbResults);
    this.T_CLI_RCU_PIVA = JdbcWritableBridge.readString(7, __dbResults);
    this.B_CLI_CF_STRANIERO = JdbcWritableBridge.readString(8, __dbResults);
    this.B_CLI_PIVA_ESTERA = JdbcWritableBridge.readString(9, __dbResults);
    this.D_DATA_CONTRATTO = JdbcWritableBridge.readString(10, __dbResults);
    this.D_DATA_DECORRENZA = JdbcWritableBridge.readString(11, __dbResults);
    this.B_REVOCA_TIMOE = JdbcWritableBridge.readString(12, __dbResults);
    this.B_ACQUISTO_CREDITO = JdbcWritableBridge.readString(13, __dbResults);
    this.T_COD_CONTR_DISP = JdbcWritableBridge.readString(14, __dbResults);
    this.T_CONTR_CONNESSIONE = JdbcWritableBridge.readString(15, __dbResults);
    this.B_AMMISSIBILE = JdbcWritableBridge.readString(16, __dbResults);
    this.T_COD_CAUSALE = JdbcWritableBridge.readString(17, __dbResults);
    this.T_MOTIVAZIONE = JdbcWritableBridge.readString(18, __dbResults);
    this.N_ID_RICH = JdbcWritableBridge.readBigDecimal(19, __dbResults);
    this.T_RUOLO_RICH = JdbcWritableBridge.readString(20, __dbResults);
    this.N_ID_DISTR = JdbcWritableBridge.readBigDecimal(21, __dbResults);
    this.N_ID_UDD_U = JdbcWritableBridge.readBigDecimal(22, __dbResults);
    this.N_ID_CC_U = JdbcWritableBridge.readBigDecimal(23, __dbResults);
    this.N_ID_CC_E = JdbcWritableBridge.readBigDecimal(24, __dbResults);
    this.T_PROT_DISTR = JdbcWritableBridge.readString(25, __dbResults);
    this.B_DATI_TIMOE = JdbcWritableBridge.readString(26, __dbResults);
    this.T_COD_ESITO = JdbcWritableBridge.readString(27, __dbResults);
    this.T_DETT_ESITO = JdbcWritableBridge.readString(28, __dbResults);
    this.D_DATA_FLUSSO = JdbcWritableBridge.readString(29, __dbResults);
    this.D_DATA_STATO = JdbcWritableBridge.readString(30, __dbResults);
    this.B_INVALIDATA = JdbcWritableBridge.readString(31, __dbResults);
    this.N_ID_CLIENTE_RCU = JdbcWritableBridge.readBigDecimal(32, __dbResults);
    this.N_ID_RIFERIMENTO_EVENTO = JdbcWritableBridge.readBigDecimal(33, __dbResults);
    this.N_ID_TIPO_EVENTO = JdbcWritableBridge.readBigDecimal(34, __dbResults);
    this.B_INFRAMESE = JdbcWritableBridge.readString(35, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_SE, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PRATICA, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PROT_RICH, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_POD, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CLI_RCU_CF, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CLI_RCU_PIVA, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_CLI_CF_STRANIERO, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_CLI_PIVA_ESTERA, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_CONTRATTO, 10 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_DECORRENZA, 11 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(B_REVOCA_TIMOE, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_ACQUISTO_CREDITO, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CONTR_DISP, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CONTR_CONNESSIONE, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_AMMISSIBILE, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CAUSALE, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MOTIVAZIONE, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_RICH, 19 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_RUOLO_RICH, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTR, 21 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UDD_U, 22 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CC_U, 23 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CC_E, 24 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_PROT_DISTR, 25 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_DATI_TIMOE, 26 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_ESITO, 27 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DETT_ESITO, 28 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_FLUSSO, 29 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_STATO, 30 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(B_INVALIDATA, 31 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CLIENTE_RCU, 32 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_RIFERIMENTO_EVENTO, 33 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TIPO_EVENTO, 34 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_INFRAMESE, 35 + __off, 12, __dbStmt);
    return 35;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_SE, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PRATICA, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PROT_RICH, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_POD, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CLI_RCU_CF, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CLI_RCU_PIVA, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_CLI_CF_STRANIERO, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_CLI_PIVA_ESTERA, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_CONTRATTO, 10 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_DECORRENZA, 11 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(B_REVOCA_TIMOE, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_ACQUISTO_CREDITO, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CONTR_DISP, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CONTR_CONNESSIONE, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_AMMISSIBILE, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CAUSALE, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MOTIVAZIONE, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_RICH, 19 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_RUOLO_RICH, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTR, 21 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UDD_U, 22 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CC_U, 23 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CC_E, 24 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_PROT_DISTR, 25 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_DATI_TIMOE, 26 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_ESITO, 27 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DETT_ESITO, 28 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_FLUSSO, 29 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_STATO, 30 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(B_INVALIDATA, 31 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CLIENTE_RCU, 32 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_RIFERIMENTO_EVENTO, 33 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TIPO_EVENTO, 34 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_INFRAMESE, 35 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_SE = null;
    } else {
    this.N_ID_SE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_PRATICA = null;
    } else {
    this.N_ID_PRATICA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_STATO = null;
    } else {
    this.T_STATO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PROT_RICH = null;
    } else {
    this.T_PROT_RICH = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_POD = null;
    } else {
    this.T_CODICE_POD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CLI_RCU_CF = null;
    } else {
    this.T_CLI_RCU_CF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CLI_RCU_PIVA = null;
    } else {
    this.T_CLI_RCU_PIVA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_CLI_CF_STRANIERO = null;
    } else {
    this.B_CLI_CF_STRANIERO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_CLI_PIVA_ESTERA = null;
    } else {
    this.B_CLI_PIVA_ESTERA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_CONTRATTO = null;
    } else {
    this.D_DATA_CONTRATTO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_DECORRENZA = null;
    } else {
    this.D_DATA_DECORRENZA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_REVOCA_TIMOE = null;
    } else {
    this.B_REVOCA_TIMOE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_ACQUISTO_CREDITO = null;
    } else {
    this.B_ACQUISTO_CREDITO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COD_CONTR_DISP = null;
    } else {
    this.T_COD_CONTR_DISP = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CONTR_CONNESSIONE = null;
    } else {
    this.T_CONTR_CONNESSIONE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_AMMISSIBILE = null;
    } else {
    this.B_AMMISSIBILE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COD_CAUSALE = null;
    } else {
    this.T_COD_CAUSALE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MOTIVAZIONE = null;
    } else {
    this.T_MOTIVAZIONE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_RICH = null;
    } else {
    this.N_ID_RICH = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_RUOLO_RICH = null;
    } else {
    this.T_RUOLO_RICH = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_DISTR = null;
    } else {
    this.N_ID_DISTR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_UDD_U = null;
    } else {
    this.N_ID_UDD_U = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_CC_U = null;
    } else {
    this.N_ID_CC_U = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_CC_E = null;
    } else {
    this.N_ID_CC_E = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PROT_DISTR = null;
    } else {
    this.T_PROT_DISTR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_DATI_TIMOE = null;
    } else {
    this.B_DATI_TIMOE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COD_ESITO = null;
    } else {
    this.T_COD_ESITO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_DETT_ESITO = null;
    } else {
    this.T_DETT_ESITO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_FLUSSO = null;
    } else {
    this.D_DATA_FLUSSO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_STATO = null;
    } else {
    this.D_DATA_STATO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_INVALIDATA = null;
    } else {
    this.B_INVALIDATA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_CLIENTE_RCU = null;
    } else {
    this.N_ID_CLIENTE_RCU = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_RIFERIMENTO_EVENTO = null;
    } else {
    this.N_ID_RIFERIMENTO_EVENTO = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_TIPO_EVENTO = null;
    } else {
    this.N_ID_TIPO_EVENTO = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_INFRAMESE = null;
    } else {
    this.B_INFRAMESE = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_SE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_SE, __dataOut);
    }
    if (null == this.N_ID_PRATICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PRATICA, __dataOut);
    }
    if (null == this.T_STATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_STATO);
    }
    if (null == this.T_PROT_RICH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PROT_RICH);
    }
    if (null == this.T_CODICE_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_POD);
    }
    if (null == this.T_CLI_RCU_CF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CLI_RCU_CF);
    }
    if (null == this.T_CLI_RCU_PIVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CLI_RCU_PIVA);
    }
    if (null == this.B_CLI_CF_STRANIERO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_CLI_CF_STRANIERO);
    }
    if (null == this.B_CLI_PIVA_ESTERA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_CLI_PIVA_ESTERA);
    }
    if (null == this.D_DATA_CONTRATTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_CONTRATTO);
    }
    if (null == this.D_DATA_DECORRENZA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_DECORRENZA);
    }
    if (null == this.B_REVOCA_TIMOE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_REVOCA_TIMOE);
    }
    if (null == this.B_ACQUISTO_CREDITO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_ACQUISTO_CREDITO);
    }
    if (null == this.T_COD_CONTR_DISP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_CONTR_DISP);
    }
    if (null == this.T_CONTR_CONNESSIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CONTR_CONNESSIONE);
    }
    if (null == this.B_AMMISSIBILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_AMMISSIBILE);
    }
    if (null == this.T_COD_CAUSALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_CAUSALE);
    }
    if (null == this.T_MOTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MOTIVAZIONE);
    }
    if (null == this.N_ID_RICH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_RICH, __dataOut);
    }
    if (null == this.T_RUOLO_RICH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RUOLO_RICH);
    }
    if (null == this.N_ID_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISTR, __dataOut);
    }
    if (null == this.N_ID_UDD_U) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UDD_U, __dataOut);
    }
    if (null == this.N_ID_CC_U) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CC_U, __dataOut);
    }
    if (null == this.N_ID_CC_E) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CC_E, __dataOut);
    }
    if (null == this.T_PROT_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PROT_DISTR);
    }
    if (null == this.B_DATI_TIMOE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_DATI_TIMOE);
    }
    if (null == this.T_COD_ESITO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_ESITO);
    }
    if (null == this.T_DETT_ESITO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DETT_ESITO);
    }
    if (null == this.D_DATA_FLUSSO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_FLUSSO);
    }
    if (null == this.D_DATA_STATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_STATO);
    }
    if (null == this.B_INVALIDATA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_INVALIDATA);
    }
    if (null == this.N_ID_CLIENTE_RCU) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CLIENTE_RCU, __dataOut);
    }
    if (null == this.N_ID_RIFERIMENTO_EVENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_RIFERIMENTO_EVENTO, __dataOut);
    }
    if (null == this.N_ID_TIPO_EVENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_TIPO_EVENTO, __dataOut);
    }
    if (null == this.B_INFRAMESE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_INFRAMESE);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_SE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_SE, __dataOut);
    }
    if (null == this.N_ID_PRATICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PRATICA, __dataOut);
    }
    if (null == this.T_STATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_STATO);
    }
    if (null == this.T_PROT_RICH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PROT_RICH);
    }
    if (null == this.T_CODICE_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_POD);
    }
    if (null == this.T_CLI_RCU_CF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CLI_RCU_CF);
    }
    if (null == this.T_CLI_RCU_PIVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CLI_RCU_PIVA);
    }
    if (null == this.B_CLI_CF_STRANIERO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_CLI_CF_STRANIERO);
    }
    if (null == this.B_CLI_PIVA_ESTERA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_CLI_PIVA_ESTERA);
    }
    if (null == this.D_DATA_CONTRATTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_CONTRATTO);
    }
    if (null == this.D_DATA_DECORRENZA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_DECORRENZA);
    }
    if (null == this.B_REVOCA_TIMOE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_REVOCA_TIMOE);
    }
    if (null == this.B_ACQUISTO_CREDITO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_ACQUISTO_CREDITO);
    }
    if (null == this.T_COD_CONTR_DISP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_CONTR_DISP);
    }
    if (null == this.T_CONTR_CONNESSIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CONTR_CONNESSIONE);
    }
    if (null == this.B_AMMISSIBILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_AMMISSIBILE);
    }
    if (null == this.T_COD_CAUSALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_CAUSALE);
    }
    if (null == this.T_MOTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MOTIVAZIONE);
    }
    if (null == this.N_ID_RICH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_RICH, __dataOut);
    }
    if (null == this.T_RUOLO_RICH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RUOLO_RICH);
    }
    if (null == this.N_ID_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISTR, __dataOut);
    }
    if (null == this.N_ID_UDD_U) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UDD_U, __dataOut);
    }
    if (null == this.N_ID_CC_U) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CC_U, __dataOut);
    }
    if (null == this.N_ID_CC_E) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CC_E, __dataOut);
    }
    if (null == this.T_PROT_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PROT_DISTR);
    }
    if (null == this.B_DATI_TIMOE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_DATI_TIMOE);
    }
    if (null == this.T_COD_ESITO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_ESITO);
    }
    if (null == this.T_DETT_ESITO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DETT_ESITO);
    }
    if (null == this.D_DATA_FLUSSO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_FLUSSO);
    }
    if (null == this.D_DATA_STATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_STATO);
    }
    if (null == this.B_INVALIDATA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_INVALIDATA);
    }
    if (null == this.N_ID_CLIENTE_RCU) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CLIENTE_RCU, __dataOut);
    }
    if (null == this.N_ID_RIFERIMENTO_EVENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_RIFERIMENTO_EVENTO, __dataOut);
    }
    if (null == this.N_ID_TIPO_EVENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_TIPO_EVENTO, __dataOut);
    }
    if (null == this.B_INFRAMESE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_INFRAMESE);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_SE==null?"":N_ID_SE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PRATICA==null?"":N_ID_PRATICA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO==null?"":T_STATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PROT_RICH==null?"":T_PROT_RICH, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_POD==null?"":T_CODICE_POD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CLI_RCU_CF==null?"":T_CLI_RCU_CF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CLI_RCU_PIVA==null?"":T_CLI_RCU_PIVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_CLI_CF_STRANIERO==null?"":B_CLI_CF_STRANIERO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_CLI_PIVA_ESTERA==null?"":B_CLI_PIVA_ESTERA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_CONTRATTO==null?"":D_DATA_CONTRATTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_DECORRENZA==null?"":D_DATA_DECORRENZA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_REVOCA_TIMOE==null?"":B_REVOCA_TIMOE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_ACQUISTO_CREDITO==null?"":B_ACQUISTO_CREDITO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CONTR_DISP==null?"":T_COD_CONTR_DISP, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CONTR_CONNESSIONE==null?"":T_CONTR_CONNESSIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_AMMISSIBILE==null?"":B_AMMISSIBILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CAUSALE==null?"":T_COD_CAUSALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MOTIVAZIONE==null?"":T_MOTIVAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_RICH==null?"":N_ID_RICH.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RUOLO_RICH==null?"":T_RUOLO_RICH, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISTR==null?"":N_ID_DISTR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UDD_U==null?"":N_ID_UDD_U.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CC_U==null?"":N_ID_CC_U.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CC_E==null?"":N_ID_CC_E.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PROT_DISTR==null?"":T_PROT_DISTR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_DATI_TIMOE==null?"":B_DATI_TIMOE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_ESITO==null?"":T_COD_ESITO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DETT_ESITO==null?"":T_DETT_ESITO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_FLUSSO==null?"":D_DATA_FLUSSO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_STATO==null?"":D_DATA_STATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_INVALIDATA==null?"":B_INVALIDATA, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CLIENTE_RCU==null?"":N_ID_CLIENTE_RCU.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_RIFERIMENTO_EVENTO==null?"":N_ID_RIFERIMENTO_EVENTO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TIPO_EVENTO==null?"":N_ID_TIPO_EVENTO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_INFRAMESE==null?"":B_INFRAMESE, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_SE==null?"":N_ID_SE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PRATICA==null?"":N_ID_PRATICA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO==null?"":T_STATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PROT_RICH==null?"":T_PROT_RICH, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_POD==null?"":T_CODICE_POD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CLI_RCU_CF==null?"":T_CLI_RCU_CF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CLI_RCU_PIVA==null?"":T_CLI_RCU_PIVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_CLI_CF_STRANIERO==null?"":B_CLI_CF_STRANIERO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_CLI_PIVA_ESTERA==null?"":B_CLI_PIVA_ESTERA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_CONTRATTO==null?"":D_DATA_CONTRATTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_DECORRENZA==null?"":D_DATA_DECORRENZA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_REVOCA_TIMOE==null?"":B_REVOCA_TIMOE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_ACQUISTO_CREDITO==null?"":B_ACQUISTO_CREDITO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CONTR_DISP==null?"":T_COD_CONTR_DISP, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CONTR_CONNESSIONE==null?"":T_CONTR_CONNESSIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_AMMISSIBILE==null?"":B_AMMISSIBILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CAUSALE==null?"":T_COD_CAUSALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MOTIVAZIONE==null?"":T_MOTIVAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_RICH==null?"":N_ID_RICH.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RUOLO_RICH==null?"":T_RUOLO_RICH, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISTR==null?"":N_ID_DISTR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UDD_U==null?"":N_ID_UDD_U.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CC_U==null?"":N_ID_CC_U.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CC_E==null?"":N_ID_CC_E.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PROT_DISTR==null?"":T_PROT_DISTR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_DATI_TIMOE==null?"":B_DATI_TIMOE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_ESITO==null?"":T_COD_ESITO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DETT_ESITO==null?"":T_DETT_ESITO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_FLUSSO==null?"":D_DATA_FLUSSO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_STATO==null?"":D_DATA_STATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_INVALIDATA==null?"":B_INVALIDATA, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CLIENTE_RCU==null?"":N_ID_CLIENTE_RCU.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_RIFERIMENTO_EVENTO==null?"":N_ID_RIFERIMENTO_EVENTO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TIPO_EVENTO==null?"":N_ID_TIPO_EVENTO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_INFRAMESE==null?"":B_INFRAMESE, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_SE = null; } else {
      this.N_ID_SE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PRATICA = null; } else {
      this.N_ID_PRATICA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_STATO = null; } else {
      this.T_STATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PROT_RICH = null; } else {
      this.T_PROT_RICH = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_POD = null; } else {
      this.T_CODICE_POD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CLI_RCU_CF = null; } else {
      this.T_CLI_RCU_CF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CLI_RCU_PIVA = null; } else {
      this.T_CLI_RCU_PIVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_CLI_CF_STRANIERO = null; } else {
      this.B_CLI_CF_STRANIERO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_CLI_PIVA_ESTERA = null; } else {
      this.B_CLI_PIVA_ESTERA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_CONTRATTO = null; } else {
      this.D_DATA_CONTRATTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_DECORRENZA = null; } else {
      this.D_DATA_DECORRENZA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_REVOCA_TIMOE = null; } else {
      this.B_REVOCA_TIMOE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_ACQUISTO_CREDITO = null; } else {
      this.B_ACQUISTO_CREDITO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_CONTR_DISP = null; } else {
      this.T_COD_CONTR_DISP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CONTR_CONNESSIONE = null; } else {
      this.T_CONTR_CONNESSIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_AMMISSIBILE = null; } else {
      this.B_AMMISSIBILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_CAUSALE = null; } else {
      this.T_COD_CAUSALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MOTIVAZIONE = null; } else {
      this.T_MOTIVAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_RICH = null; } else {
      this.N_ID_RICH = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RUOLO_RICH = null; } else {
      this.T_RUOLO_RICH = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISTR = null; } else {
      this.N_ID_DISTR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UDD_U = null; } else {
      this.N_ID_UDD_U = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CC_U = null; } else {
      this.N_ID_CC_U = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CC_E = null; } else {
      this.N_ID_CC_E = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PROT_DISTR = null; } else {
      this.T_PROT_DISTR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_DATI_TIMOE = null; } else {
      this.B_DATI_TIMOE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_ESITO = null; } else {
      this.T_COD_ESITO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DETT_ESITO = null; } else {
      this.T_DETT_ESITO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_FLUSSO = null; } else {
      this.D_DATA_FLUSSO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_STATO = null; } else {
      this.D_DATA_STATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_INVALIDATA = null; } else {
      this.B_INVALIDATA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CLIENTE_RCU = null; } else {
      this.N_ID_CLIENTE_RCU = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_RIFERIMENTO_EVENTO = null; } else {
      this.N_ID_RIFERIMENTO_EVENTO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_TIPO_EVENTO = null; } else {
      this.N_ID_TIPO_EVENTO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_INFRAMESE = null; } else {
      this.B_INFRAMESE = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_SE = null; } else {
      this.N_ID_SE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PRATICA = null; } else {
      this.N_ID_PRATICA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_STATO = null; } else {
      this.T_STATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PROT_RICH = null; } else {
      this.T_PROT_RICH = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_POD = null; } else {
      this.T_CODICE_POD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CLI_RCU_CF = null; } else {
      this.T_CLI_RCU_CF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CLI_RCU_PIVA = null; } else {
      this.T_CLI_RCU_PIVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_CLI_CF_STRANIERO = null; } else {
      this.B_CLI_CF_STRANIERO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_CLI_PIVA_ESTERA = null; } else {
      this.B_CLI_PIVA_ESTERA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_CONTRATTO = null; } else {
      this.D_DATA_CONTRATTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_DECORRENZA = null; } else {
      this.D_DATA_DECORRENZA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_REVOCA_TIMOE = null; } else {
      this.B_REVOCA_TIMOE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_ACQUISTO_CREDITO = null; } else {
      this.B_ACQUISTO_CREDITO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_CONTR_DISP = null; } else {
      this.T_COD_CONTR_DISP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CONTR_CONNESSIONE = null; } else {
      this.T_CONTR_CONNESSIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_AMMISSIBILE = null; } else {
      this.B_AMMISSIBILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_CAUSALE = null; } else {
      this.T_COD_CAUSALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MOTIVAZIONE = null; } else {
      this.T_MOTIVAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_RICH = null; } else {
      this.N_ID_RICH = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RUOLO_RICH = null; } else {
      this.T_RUOLO_RICH = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISTR = null; } else {
      this.N_ID_DISTR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UDD_U = null; } else {
      this.N_ID_UDD_U = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CC_U = null; } else {
      this.N_ID_CC_U = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CC_E = null; } else {
      this.N_ID_CC_E = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PROT_DISTR = null; } else {
      this.T_PROT_DISTR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_DATI_TIMOE = null; } else {
      this.B_DATI_TIMOE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_ESITO = null; } else {
      this.T_COD_ESITO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DETT_ESITO = null; } else {
      this.T_DETT_ESITO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_FLUSSO = null; } else {
      this.D_DATA_FLUSSO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_STATO = null; } else {
      this.D_DATA_STATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_INVALIDATA = null; } else {
      this.B_INVALIDATA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CLIENTE_RCU = null; } else {
      this.N_ID_CLIENTE_RCU = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_RIFERIMENTO_EVENTO = null; } else {
      this.N_ID_RIFERIMENTO_EVENTO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_TIPO_EVENTO = null; } else {
      this.N_ID_TIPO_EVENTO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_INFRAMESE = null; } else {
      this.B_INFRAMESE = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    swtch_prt_se o = (swtch_prt_se) super.clone();
    return o;
  }

  public void clone0(swtch_prt_se o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_SE", this.N_ID_SE);
    __sqoop$field_map.put("N_ID_PRATICA", this.N_ID_PRATICA);
    __sqoop$field_map.put("T_STATO", this.T_STATO);
    __sqoop$field_map.put("T_PROT_RICH", this.T_PROT_RICH);
    __sqoop$field_map.put("T_CODICE_POD", this.T_CODICE_POD);
    __sqoop$field_map.put("T_CLI_RCU_CF", this.T_CLI_RCU_CF);
    __sqoop$field_map.put("T_CLI_RCU_PIVA", this.T_CLI_RCU_PIVA);
    __sqoop$field_map.put("B_CLI_CF_STRANIERO", this.B_CLI_CF_STRANIERO);
    __sqoop$field_map.put("B_CLI_PIVA_ESTERA", this.B_CLI_PIVA_ESTERA);
    __sqoop$field_map.put("D_DATA_CONTRATTO", this.D_DATA_CONTRATTO);
    __sqoop$field_map.put("D_DATA_DECORRENZA", this.D_DATA_DECORRENZA);
    __sqoop$field_map.put("B_REVOCA_TIMOE", this.B_REVOCA_TIMOE);
    __sqoop$field_map.put("B_ACQUISTO_CREDITO", this.B_ACQUISTO_CREDITO);
    __sqoop$field_map.put("T_COD_CONTR_DISP", this.T_COD_CONTR_DISP);
    __sqoop$field_map.put("T_CONTR_CONNESSIONE", this.T_CONTR_CONNESSIONE);
    __sqoop$field_map.put("B_AMMISSIBILE", this.B_AMMISSIBILE);
    __sqoop$field_map.put("T_COD_CAUSALE", this.T_COD_CAUSALE);
    __sqoop$field_map.put("T_MOTIVAZIONE", this.T_MOTIVAZIONE);
    __sqoop$field_map.put("N_ID_RICH", this.N_ID_RICH);
    __sqoop$field_map.put("T_RUOLO_RICH", this.T_RUOLO_RICH);
    __sqoop$field_map.put("N_ID_DISTR", this.N_ID_DISTR);
    __sqoop$field_map.put("N_ID_UDD_U", this.N_ID_UDD_U);
    __sqoop$field_map.put("N_ID_CC_U", this.N_ID_CC_U);
    __sqoop$field_map.put("N_ID_CC_E", this.N_ID_CC_E);
    __sqoop$field_map.put("T_PROT_DISTR", this.T_PROT_DISTR);
    __sqoop$field_map.put("B_DATI_TIMOE", this.B_DATI_TIMOE);
    __sqoop$field_map.put("T_COD_ESITO", this.T_COD_ESITO);
    __sqoop$field_map.put("T_DETT_ESITO", this.T_DETT_ESITO);
    __sqoop$field_map.put("D_DATA_FLUSSO", this.D_DATA_FLUSSO);
    __sqoop$field_map.put("D_DATA_STATO", this.D_DATA_STATO);
    __sqoop$field_map.put("B_INVALIDATA", this.B_INVALIDATA);
    __sqoop$field_map.put("N_ID_CLIENTE_RCU", this.N_ID_CLIENTE_RCU);
    __sqoop$field_map.put("N_ID_RIFERIMENTO_EVENTO", this.N_ID_RIFERIMENTO_EVENTO);
    __sqoop$field_map.put("N_ID_TIPO_EVENTO", this.N_ID_TIPO_EVENTO);
    __sqoop$field_map.put("B_INFRAMESE", this.B_INFRAMESE);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_SE", this.N_ID_SE);
    __sqoop$field_map.put("N_ID_PRATICA", this.N_ID_PRATICA);
    __sqoop$field_map.put("T_STATO", this.T_STATO);
    __sqoop$field_map.put("T_PROT_RICH", this.T_PROT_RICH);
    __sqoop$field_map.put("T_CODICE_POD", this.T_CODICE_POD);
    __sqoop$field_map.put("T_CLI_RCU_CF", this.T_CLI_RCU_CF);
    __sqoop$field_map.put("T_CLI_RCU_PIVA", this.T_CLI_RCU_PIVA);
    __sqoop$field_map.put("B_CLI_CF_STRANIERO", this.B_CLI_CF_STRANIERO);
    __sqoop$field_map.put("B_CLI_PIVA_ESTERA", this.B_CLI_PIVA_ESTERA);
    __sqoop$field_map.put("D_DATA_CONTRATTO", this.D_DATA_CONTRATTO);
    __sqoop$field_map.put("D_DATA_DECORRENZA", this.D_DATA_DECORRENZA);
    __sqoop$field_map.put("B_REVOCA_TIMOE", this.B_REVOCA_TIMOE);
    __sqoop$field_map.put("B_ACQUISTO_CREDITO", this.B_ACQUISTO_CREDITO);
    __sqoop$field_map.put("T_COD_CONTR_DISP", this.T_COD_CONTR_DISP);
    __sqoop$field_map.put("T_CONTR_CONNESSIONE", this.T_CONTR_CONNESSIONE);
    __sqoop$field_map.put("B_AMMISSIBILE", this.B_AMMISSIBILE);
    __sqoop$field_map.put("T_COD_CAUSALE", this.T_COD_CAUSALE);
    __sqoop$field_map.put("T_MOTIVAZIONE", this.T_MOTIVAZIONE);
    __sqoop$field_map.put("N_ID_RICH", this.N_ID_RICH);
    __sqoop$field_map.put("T_RUOLO_RICH", this.T_RUOLO_RICH);
    __sqoop$field_map.put("N_ID_DISTR", this.N_ID_DISTR);
    __sqoop$field_map.put("N_ID_UDD_U", this.N_ID_UDD_U);
    __sqoop$field_map.put("N_ID_CC_U", this.N_ID_CC_U);
    __sqoop$field_map.put("N_ID_CC_E", this.N_ID_CC_E);
    __sqoop$field_map.put("T_PROT_DISTR", this.T_PROT_DISTR);
    __sqoop$field_map.put("B_DATI_TIMOE", this.B_DATI_TIMOE);
    __sqoop$field_map.put("T_COD_ESITO", this.T_COD_ESITO);
    __sqoop$field_map.put("T_DETT_ESITO", this.T_DETT_ESITO);
    __sqoop$field_map.put("D_DATA_FLUSSO", this.D_DATA_FLUSSO);
    __sqoop$field_map.put("D_DATA_STATO", this.D_DATA_STATO);
    __sqoop$field_map.put("B_INVALIDATA", this.B_INVALIDATA);
    __sqoop$field_map.put("N_ID_CLIENTE_RCU", this.N_ID_CLIENTE_RCU);
    __sqoop$field_map.put("N_ID_RIFERIMENTO_EVENTO", this.N_ID_RIFERIMENTO_EVENTO);
    __sqoop$field_map.put("N_ID_TIPO_EVENTO", this.N_ID_TIPO_EVENTO);
    __sqoop$field_map.put("B_INFRAMESE", this.B_INFRAMESE);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
