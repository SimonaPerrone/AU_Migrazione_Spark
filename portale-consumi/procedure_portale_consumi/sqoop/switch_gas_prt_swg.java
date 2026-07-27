// ORM class for table 'switch_gas.prt_swg'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 14:02:43 CEST 2019
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

public class switch_gas_prt_swg extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_SWG", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_SWG = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_PRATICA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_PRATICA = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_CODICE_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_PDR = (String)value;
      }
    });
    setters.put("T_STATO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_STATO = (String)value;
      }
    });
    setters.put("N_ID_RICH", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_RICH = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_DISTR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_DISTR = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_CC_ENTRANTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_CC_ENTRANTE = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_CC_USCENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_CC_USCENTE = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_UDD_USCENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_UDD_USCENTE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_TITOLO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TITOLO = (String)value;
      }
    });
    setters.put("D_DATA_RICHIESTA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_RICHIESTA = (String)value;
      }
    });
    setters.put("T_CP_UTENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CP_UTENTE = (String)value;
      }
    });
    setters.put("T_CP_DISTRIBUTORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CP_DISTRIBUTORE = (String)value;
      }
    });
    setters.put("T_CF_CLIENTE_FINALE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CF_CLIENTE_FINALE = (String)value;
      }
    });
    setters.put("T_PIVA_CLIENTE_FINALE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PIVA_CLIENTE_FINALE = (String)value;
      }
    });
    setters.put("B_CF_STRANIERO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_CF_STRANIERO = (String)value;
      }
    });
    setters.put("B_PERSONA_FISICA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_PERSONA_FISICA = (String)value;
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
    setters.put("T_ESITO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ESITO = (String)value;
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
    setters.put("D_DATA_STATO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_STATO = (String)value;
      }
    });
    setters.put("D_DATA_INSERIMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_INSERIMENTO = (String)value;
      }
    });
    setters.put("T_TIPO_FORNITURA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO_FORNITURA = (String)value;
      }
    });
    setters.put("T_PIVA_CC", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PIVA_CC = (String)value;
      }
    });
    setters.put("T_REVOCA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_REVOCA = (String)value;
      }
    });
    setters.put("T_TIMG_PDR_CHIUSO_MOR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIMG_PDR_CHIUSO_MOR = (String)value;
      }
    });
    setters.put("T_TIMG_DATE_SOSP", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIMG_DATE_SOSP = (String)value;
      }
    });
    setters.put("T_TIMG_DATE_SOST", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIMG_DATE_SOST = (String)value;
      }
    });
    setters.put("T_TIMG_ACC_MIS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIMG_ACC_MIS = (String)value;
      }
    });
    setters.put("T_TIMG_PRESENZA_CMOR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIMG_PRESENZA_CMOR = (String)value;
      }
    });
    setters.put("D_TIVG_DATA_ATT_FDD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_TIVG_DATA_ATT_FDD = (String)value;
      }
    });
    setters.put("D_TIVG_DATA_CHIUSURA_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_TIVG_DATA_CHIUSURA_PDR = (String)value;
      }
    });
    setters.put("B_TIMG_TIVG_CALCOLATO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_TIMG_TIVG_CALCOLATO = (String)value;
      }
    });
    setters.put("B_ANN_IN_RITARDO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_ANN_IN_RITARDO = (String)value;
      }
    });
    setters.put("T_CAUS_ANNULLAMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CAUS_ANNULLAMENTO = (String)value;
      }
    });
    setters.put("T_MOTIV_ANNULLAMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MOTIV_ANNULLAMENTO = (String)value;
      }
    });
  }
  public switch_gas_prt_swg() {
    init0();
  }
  private java.math.BigDecimal N_ID_SWG;
  public java.math.BigDecimal get_N_ID_SWG() {
    return N_ID_SWG;
  }
  public void set_N_ID_SWG(java.math.BigDecimal N_ID_SWG) {
    this.N_ID_SWG = N_ID_SWG;
  }
  public switch_gas_prt_swg with_N_ID_SWG(java.math.BigDecimal N_ID_SWG) {
    this.N_ID_SWG = N_ID_SWG;
    return this;
  }
  private java.math.BigDecimal N_ID_PRATICA;
  public java.math.BigDecimal get_N_ID_PRATICA() {
    return N_ID_PRATICA;
  }
  public void set_N_ID_PRATICA(java.math.BigDecimal N_ID_PRATICA) {
    this.N_ID_PRATICA = N_ID_PRATICA;
  }
  public switch_gas_prt_swg with_N_ID_PRATICA(java.math.BigDecimal N_ID_PRATICA) {
    this.N_ID_PRATICA = N_ID_PRATICA;
    return this;
  }
  private String T_CODICE_PDR;
  public String get_T_CODICE_PDR() {
    return T_CODICE_PDR;
  }
  public void set_T_CODICE_PDR(String T_CODICE_PDR) {
    this.T_CODICE_PDR = T_CODICE_PDR;
  }
  public switch_gas_prt_swg with_T_CODICE_PDR(String T_CODICE_PDR) {
    this.T_CODICE_PDR = T_CODICE_PDR;
    return this;
  }
  private String T_STATO;
  public String get_T_STATO() {
    return T_STATO;
  }
  public void set_T_STATO(String T_STATO) {
    this.T_STATO = T_STATO;
  }
  public switch_gas_prt_swg with_T_STATO(String T_STATO) {
    this.T_STATO = T_STATO;
    return this;
  }
  private java.math.BigDecimal N_ID_RICH;
  public java.math.BigDecimal get_N_ID_RICH() {
    return N_ID_RICH;
  }
  public void set_N_ID_RICH(java.math.BigDecimal N_ID_RICH) {
    this.N_ID_RICH = N_ID_RICH;
  }
  public switch_gas_prt_swg with_N_ID_RICH(java.math.BigDecimal N_ID_RICH) {
    this.N_ID_RICH = N_ID_RICH;
    return this;
  }
  private java.math.BigDecimal N_ID_DISTR;
  public java.math.BigDecimal get_N_ID_DISTR() {
    return N_ID_DISTR;
  }
  public void set_N_ID_DISTR(java.math.BigDecimal N_ID_DISTR) {
    this.N_ID_DISTR = N_ID_DISTR;
  }
  public switch_gas_prt_swg with_N_ID_DISTR(java.math.BigDecimal N_ID_DISTR) {
    this.N_ID_DISTR = N_ID_DISTR;
    return this;
  }
  private java.math.BigDecimal N_ID_CC_ENTRANTE;
  public java.math.BigDecimal get_N_ID_CC_ENTRANTE() {
    return N_ID_CC_ENTRANTE;
  }
  public void set_N_ID_CC_ENTRANTE(java.math.BigDecimal N_ID_CC_ENTRANTE) {
    this.N_ID_CC_ENTRANTE = N_ID_CC_ENTRANTE;
  }
  public switch_gas_prt_swg with_N_ID_CC_ENTRANTE(java.math.BigDecimal N_ID_CC_ENTRANTE) {
    this.N_ID_CC_ENTRANTE = N_ID_CC_ENTRANTE;
    return this;
  }
  private java.math.BigDecimal N_ID_CC_USCENTE;
  public java.math.BigDecimal get_N_ID_CC_USCENTE() {
    return N_ID_CC_USCENTE;
  }
  public void set_N_ID_CC_USCENTE(java.math.BigDecimal N_ID_CC_USCENTE) {
    this.N_ID_CC_USCENTE = N_ID_CC_USCENTE;
  }
  public switch_gas_prt_swg with_N_ID_CC_USCENTE(java.math.BigDecimal N_ID_CC_USCENTE) {
    this.N_ID_CC_USCENTE = N_ID_CC_USCENTE;
    return this;
  }
  private java.math.BigDecimal N_ID_UDD_USCENTE;
  public java.math.BigDecimal get_N_ID_UDD_USCENTE() {
    return N_ID_UDD_USCENTE;
  }
  public void set_N_ID_UDD_USCENTE(java.math.BigDecimal N_ID_UDD_USCENTE) {
    this.N_ID_UDD_USCENTE = N_ID_UDD_USCENTE;
  }
  public switch_gas_prt_swg with_N_ID_UDD_USCENTE(java.math.BigDecimal N_ID_UDD_USCENTE) {
    this.N_ID_UDD_USCENTE = N_ID_UDD_USCENTE;
    return this;
  }
  private String T_TITOLO;
  public String get_T_TITOLO() {
    return T_TITOLO;
  }
  public void set_T_TITOLO(String T_TITOLO) {
    this.T_TITOLO = T_TITOLO;
  }
  public switch_gas_prt_swg with_T_TITOLO(String T_TITOLO) {
    this.T_TITOLO = T_TITOLO;
    return this;
  }
  private String D_DATA_RICHIESTA;
  public String get_D_DATA_RICHIESTA() {
    return D_DATA_RICHIESTA;
  }
  public void set_D_DATA_RICHIESTA(String D_DATA_RICHIESTA) {
    this.D_DATA_RICHIESTA = D_DATA_RICHIESTA;
  }
  public switch_gas_prt_swg with_D_DATA_RICHIESTA(String D_DATA_RICHIESTA) {
    this.D_DATA_RICHIESTA = D_DATA_RICHIESTA;
    return this;
  }
  private String T_CP_UTENTE;
  public String get_T_CP_UTENTE() {
    return T_CP_UTENTE;
  }
  public void set_T_CP_UTENTE(String T_CP_UTENTE) {
    this.T_CP_UTENTE = T_CP_UTENTE;
  }
  public switch_gas_prt_swg with_T_CP_UTENTE(String T_CP_UTENTE) {
    this.T_CP_UTENTE = T_CP_UTENTE;
    return this;
  }
  private String T_CP_DISTRIBUTORE;
  public String get_T_CP_DISTRIBUTORE() {
    return T_CP_DISTRIBUTORE;
  }
  public void set_T_CP_DISTRIBUTORE(String T_CP_DISTRIBUTORE) {
    this.T_CP_DISTRIBUTORE = T_CP_DISTRIBUTORE;
  }
  public switch_gas_prt_swg with_T_CP_DISTRIBUTORE(String T_CP_DISTRIBUTORE) {
    this.T_CP_DISTRIBUTORE = T_CP_DISTRIBUTORE;
    return this;
  }
  private String T_CF_CLIENTE_FINALE;
  public String get_T_CF_CLIENTE_FINALE() {
    return T_CF_CLIENTE_FINALE;
  }
  public void set_T_CF_CLIENTE_FINALE(String T_CF_CLIENTE_FINALE) {
    this.T_CF_CLIENTE_FINALE = T_CF_CLIENTE_FINALE;
  }
  public switch_gas_prt_swg with_T_CF_CLIENTE_FINALE(String T_CF_CLIENTE_FINALE) {
    this.T_CF_CLIENTE_FINALE = T_CF_CLIENTE_FINALE;
    return this;
  }
  private String T_PIVA_CLIENTE_FINALE;
  public String get_T_PIVA_CLIENTE_FINALE() {
    return T_PIVA_CLIENTE_FINALE;
  }
  public void set_T_PIVA_CLIENTE_FINALE(String T_PIVA_CLIENTE_FINALE) {
    this.T_PIVA_CLIENTE_FINALE = T_PIVA_CLIENTE_FINALE;
  }
  public switch_gas_prt_swg with_T_PIVA_CLIENTE_FINALE(String T_PIVA_CLIENTE_FINALE) {
    this.T_PIVA_CLIENTE_FINALE = T_PIVA_CLIENTE_FINALE;
    return this;
  }
  private String B_CF_STRANIERO;
  public String get_B_CF_STRANIERO() {
    return B_CF_STRANIERO;
  }
  public void set_B_CF_STRANIERO(String B_CF_STRANIERO) {
    this.B_CF_STRANIERO = B_CF_STRANIERO;
  }
  public switch_gas_prt_swg with_B_CF_STRANIERO(String B_CF_STRANIERO) {
    this.B_CF_STRANIERO = B_CF_STRANIERO;
    return this;
  }
  private String B_PERSONA_FISICA;
  public String get_B_PERSONA_FISICA() {
    return B_PERSONA_FISICA;
  }
  public void set_B_PERSONA_FISICA(String B_PERSONA_FISICA) {
    this.B_PERSONA_FISICA = B_PERSONA_FISICA;
  }
  public switch_gas_prt_swg with_B_PERSONA_FISICA(String B_PERSONA_FISICA) {
    this.B_PERSONA_FISICA = B_PERSONA_FISICA;
    return this;
  }
  private String D_DATA_CONTRATTO;
  public String get_D_DATA_CONTRATTO() {
    return D_DATA_CONTRATTO;
  }
  public void set_D_DATA_CONTRATTO(String D_DATA_CONTRATTO) {
    this.D_DATA_CONTRATTO = D_DATA_CONTRATTO;
  }
  public switch_gas_prt_swg with_D_DATA_CONTRATTO(String D_DATA_CONTRATTO) {
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
  public switch_gas_prt_swg with_D_DATA_DECORRENZA(String D_DATA_DECORRENZA) {
    this.D_DATA_DECORRENZA = D_DATA_DECORRENZA;
    return this;
  }
  private String B_AMMISSIBILE;
  public String get_B_AMMISSIBILE() {
    return B_AMMISSIBILE;
  }
  public void set_B_AMMISSIBILE(String B_AMMISSIBILE) {
    this.B_AMMISSIBILE = B_AMMISSIBILE;
  }
  public switch_gas_prt_swg with_B_AMMISSIBILE(String B_AMMISSIBILE) {
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
  public switch_gas_prt_swg with_T_COD_CAUSALE(String T_COD_CAUSALE) {
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
  public switch_gas_prt_swg with_T_MOTIVAZIONE(String T_MOTIVAZIONE) {
    this.T_MOTIVAZIONE = T_MOTIVAZIONE;
    return this;
  }
  private String T_ESITO;
  public String get_T_ESITO() {
    return T_ESITO;
  }
  public void set_T_ESITO(String T_ESITO) {
    this.T_ESITO = T_ESITO;
  }
  public switch_gas_prt_swg with_T_ESITO(String T_ESITO) {
    this.T_ESITO = T_ESITO;
    return this;
  }
  private String T_COD_ESITO;
  public String get_T_COD_ESITO() {
    return T_COD_ESITO;
  }
  public void set_T_COD_ESITO(String T_COD_ESITO) {
    this.T_COD_ESITO = T_COD_ESITO;
  }
  public switch_gas_prt_swg with_T_COD_ESITO(String T_COD_ESITO) {
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
  public switch_gas_prt_swg with_T_DETT_ESITO(String T_DETT_ESITO) {
    this.T_DETT_ESITO = T_DETT_ESITO;
    return this;
  }
  private String D_DATA_STATO;
  public String get_D_DATA_STATO() {
    return D_DATA_STATO;
  }
  public void set_D_DATA_STATO(String D_DATA_STATO) {
    this.D_DATA_STATO = D_DATA_STATO;
  }
  public switch_gas_prt_swg with_D_DATA_STATO(String D_DATA_STATO) {
    this.D_DATA_STATO = D_DATA_STATO;
    return this;
  }
  private String D_DATA_INSERIMENTO;
  public String get_D_DATA_INSERIMENTO() {
    return D_DATA_INSERIMENTO;
  }
  public void set_D_DATA_INSERIMENTO(String D_DATA_INSERIMENTO) {
    this.D_DATA_INSERIMENTO = D_DATA_INSERIMENTO;
  }
  public switch_gas_prt_swg with_D_DATA_INSERIMENTO(String D_DATA_INSERIMENTO) {
    this.D_DATA_INSERIMENTO = D_DATA_INSERIMENTO;
    return this;
  }
  private String T_TIPO_FORNITURA;
  public String get_T_TIPO_FORNITURA() {
    return T_TIPO_FORNITURA;
  }
  public void set_T_TIPO_FORNITURA(String T_TIPO_FORNITURA) {
    this.T_TIPO_FORNITURA = T_TIPO_FORNITURA;
  }
  public switch_gas_prt_swg with_T_TIPO_FORNITURA(String T_TIPO_FORNITURA) {
    this.T_TIPO_FORNITURA = T_TIPO_FORNITURA;
    return this;
  }
  private String T_PIVA_CC;
  public String get_T_PIVA_CC() {
    return T_PIVA_CC;
  }
  public void set_T_PIVA_CC(String T_PIVA_CC) {
    this.T_PIVA_CC = T_PIVA_CC;
  }
  public switch_gas_prt_swg with_T_PIVA_CC(String T_PIVA_CC) {
    this.T_PIVA_CC = T_PIVA_CC;
    return this;
  }
  private String T_REVOCA;
  public String get_T_REVOCA() {
    return T_REVOCA;
  }
  public void set_T_REVOCA(String T_REVOCA) {
    this.T_REVOCA = T_REVOCA;
  }
  public switch_gas_prt_swg with_T_REVOCA(String T_REVOCA) {
    this.T_REVOCA = T_REVOCA;
    return this;
  }
  private String T_TIMG_PDR_CHIUSO_MOR;
  public String get_T_TIMG_PDR_CHIUSO_MOR() {
    return T_TIMG_PDR_CHIUSO_MOR;
  }
  public void set_T_TIMG_PDR_CHIUSO_MOR(String T_TIMG_PDR_CHIUSO_MOR) {
    this.T_TIMG_PDR_CHIUSO_MOR = T_TIMG_PDR_CHIUSO_MOR;
  }
  public switch_gas_prt_swg with_T_TIMG_PDR_CHIUSO_MOR(String T_TIMG_PDR_CHIUSO_MOR) {
    this.T_TIMG_PDR_CHIUSO_MOR = T_TIMG_PDR_CHIUSO_MOR;
    return this;
  }
  private String T_TIMG_DATE_SOSP;
  public String get_T_TIMG_DATE_SOSP() {
    return T_TIMG_DATE_SOSP;
  }
  public void set_T_TIMG_DATE_SOSP(String T_TIMG_DATE_SOSP) {
    this.T_TIMG_DATE_SOSP = T_TIMG_DATE_SOSP;
  }
  public switch_gas_prt_swg with_T_TIMG_DATE_SOSP(String T_TIMG_DATE_SOSP) {
    this.T_TIMG_DATE_SOSP = T_TIMG_DATE_SOSP;
    return this;
  }
  private String T_TIMG_DATE_SOST;
  public String get_T_TIMG_DATE_SOST() {
    return T_TIMG_DATE_SOST;
  }
  public void set_T_TIMG_DATE_SOST(String T_TIMG_DATE_SOST) {
    this.T_TIMG_DATE_SOST = T_TIMG_DATE_SOST;
  }
  public switch_gas_prt_swg with_T_TIMG_DATE_SOST(String T_TIMG_DATE_SOST) {
    this.T_TIMG_DATE_SOST = T_TIMG_DATE_SOST;
    return this;
  }
  private String T_TIMG_ACC_MIS;
  public String get_T_TIMG_ACC_MIS() {
    return T_TIMG_ACC_MIS;
  }
  public void set_T_TIMG_ACC_MIS(String T_TIMG_ACC_MIS) {
    this.T_TIMG_ACC_MIS = T_TIMG_ACC_MIS;
  }
  public switch_gas_prt_swg with_T_TIMG_ACC_MIS(String T_TIMG_ACC_MIS) {
    this.T_TIMG_ACC_MIS = T_TIMG_ACC_MIS;
    return this;
  }
  private String T_TIMG_PRESENZA_CMOR;
  public String get_T_TIMG_PRESENZA_CMOR() {
    return T_TIMG_PRESENZA_CMOR;
  }
  public void set_T_TIMG_PRESENZA_CMOR(String T_TIMG_PRESENZA_CMOR) {
    this.T_TIMG_PRESENZA_CMOR = T_TIMG_PRESENZA_CMOR;
  }
  public switch_gas_prt_swg with_T_TIMG_PRESENZA_CMOR(String T_TIMG_PRESENZA_CMOR) {
    this.T_TIMG_PRESENZA_CMOR = T_TIMG_PRESENZA_CMOR;
    return this;
  }
  private String D_TIVG_DATA_ATT_FDD;
  public String get_D_TIVG_DATA_ATT_FDD() {
    return D_TIVG_DATA_ATT_FDD;
  }
  public void set_D_TIVG_DATA_ATT_FDD(String D_TIVG_DATA_ATT_FDD) {
    this.D_TIVG_DATA_ATT_FDD = D_TIVG_DATA_ATT_FDD;
  }
  public switch_gas_prt_swg with_D_TIVG_DATA_ATT_FDD(String D_TIVG_DATA_ATT_FDD) {
    this.D_TIVG_DATA_ATT_FDD = D_TIVG_DATA_ATT_FDD;
    return this;
  }
  private String D_TIVG_DATA_CHIUSURA_PDR;
  public String get_D_TIVG_DATA_CHIUSURA_PDR() {
    return D_TIVG_DATA_CHIUSURA_PDR;
  }
  public void set_D_TIVG_DATA_CHIUSURA_PDR(String D_TIVG_DATA_CHIUSURA_PDR) {
    this.D_TIVG_DATA_CHIUSURA_PDR = D_TIVG_DATA_CHIUSURA_PDR;
  }
  public switch_gas_prt_swg with_D_TIVG_DATA_CHIUSURA_PDR(String D_TIVG_DATA_CHIUSURA_PDR) {
    this.D_TIVG_DATA_CHIUSURA_PDR = D_TIVG_DATA_CHIUSURA_PDR;
    return this;
  }
  private String B_TIMG_TIVG_CALCOLATO;
  public String get_B_TIMG_TIVG_CALCOLATO() {
    return B_TIMG_TIVG_CALCOLATO;
  }
  public void set_B_TIMG_TIVG_CALCOLATO(String B_TIMG_TIVG_CALCOLATO) {
    this.B_TIMG_TIVG_CALCOLATO = B_TIMG_TIVG_CALCOLATO;
  }
  public switch_gas_prt_swg with_B_TIMG_TIVG_CALCOLATO(String B_TIMG_TIVG_CALCOLATO) {
    this.B_TIMG_TIVG_CALCOLATO = B_TIMG_TIVG_CALCOLATO;
    return this;
  }
  private String B_ANN_IN_RITARDO;
  public String get_B_ANN_IN_RITARDO() {
    return B_ANN_IN_RITARDO;
  }
  public void set_B_ANN_IN_RITARDO(String B_ANN_IN_RITARDO) {
    this.B_ANN_IN_RITARDO = B_ANN_IN_RITARDO;
  }
  public switch_gas_prt_swg with_B_ANN_IN_RITARDO(String B_ANN_IN_RITARDO) {
    this.B_ANN_IN_RITARDO = B_ANN_IN_RITARDO;
    return this;
  }
  private String T_CAUS_ANNULLAMENTO;
  public String get_T_CAUS_ANNULLAMENTO() {
    return T_CAUS_ANNULLAMENTO;
  }
  public void set_T_CAUS_ANNULLAMENTO(String T_CAUS_ANNULLAMENTO) {
    this.T_CAUS_ANNULLAMENTO = T_CAUS_ANNULLAMENTO;
  }
  public switch_gas_prt_swg with_T_CAUS_ANNULLAMENTO(String T_CAUS_ANNULLAMENTO) {
    this.T_CAUS_ANNULLAMENTO = T_CAUS_ANNULLAMENTO;
    return this;
  }
  private String T_MOTIV_ANNULLAMENTO;
  public String get_T_MOTIV_ANNULLAMENTO() {
    return T_MOTIV_ANNULLAMENTO;
  }
  public void set_T_MOTIV_ANNULLAMENTO(String T_MOTIV_ANNULLAMENTO) {
    this.T_MOTIV_ANNULLAMENTO = T_MOTIV_ANNULLAMENTO;
  }
  public switch_gas_prt_swg with_T_MOTIV_ANNULLAMENTO(String T_MOTIV_ANNULLAMENTO) {
    this.T_MOTIV_ANNULLAMENTO = T_MOTIV_ANNULLAMENTO;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof switch_gas_prt_swg)) {
      return false;
    }
    switch_gas_prt_swg that = (switch_gas_prt_swg) o;
    boolean equal = true;
    equal = equal && (this.N_ID_SWG == null ? that.N_ID_SWG == null : this.N_ID_SWG.equals(that.N_ID_SWG));
    equal = equal && (this.N_ID_PRATICA == null ? that.N_ID_PRATICA == null : this.N_ID_PRATICA.equals(that.N_ID_PRATICA));
    equal = equal && (this.T_CODICE_PDR == null ? that.T_CODICE_PDR == null : this.T_CODICE_PDR.equals(that.T_CODICE_PDR));
    equal = equal && (this.T_STATO == null ? that.T_STATO == null : this.T_STATO.equals(that.T_STATO));
    equal = equal && (this.N_ID_RICH == null ? that.N_ID_RICH == null : this.N_ID_RICH.equals(that.N_ID_RICH));
    equal = equal && (this.N_ID_DISTR == null ? that.N_ID_DISTR == null : this.N_ID_DISTR.equals(that.N_ID_DISTR));
    equal = equal && (this.N_ID_CC_ENTRANTE == null ? that.N_ID_CC_ENTRANTE == null : this.N_ID_CC_ENTRANTE.equals(that.N_ID_CC_ENTRANTE));
    equal = equal && (this.N_ID_CC_USCENTE == null ? that.N_ID_CC_USCENTE == null : this.N_ID_CC_USCENTE.equals(that.N_ID_CC_USCENTE));
    equal = equal && (this.N_ID_UDD_USCENTE == null ? that.N_ID_UDD_USCENTE == null : this.N_ID_UDD_USCENTE.equals(that.N_ID_UDD_USCENTE));
    equal = equal && (this.T_TITOLO == null ? that.T_TITOLO == null : this.T_TITOLO.equals(that.T_TITOLO));
    equal = equal && (this.D_DATA_RICHIESTA == null ? that.D_DATA_RICHIESTA == null : this.D_DATA_RICHIESTA.equals(that.D_DATA_RICHIESTA));
    equal = equal && (this.T_CP_UTENTE == null ? that.T_CP_UTENTE == null : this.T_CP_UTENTE.equals(that.T_CP_UTENTE));
    equal = equal && (this.T_CP_DISTRIBUTORE == null ? that.T_CP_DISTRIBUTORE == null : this.T_CP_DISTRIBUTORE.equals(that.T_CP_DISTRIBUTORE));
    equal = equal && (this.T_CF_CLIENTE_FINALE == null ? that.T_CF_CLIENTE_FINALE == null : this.T_CF_CLIENTE_FINALE.equals(that.T_CF_CLIENTE_FINALE));
    equal = equal && (this.T_PIVA_CLIENTE_FINALE == null ? that.T_PIVA_CLIENTE_FINALE == null : this.T_PIVA_CLIENTE_FINALE.equals(that.T_PIVA_CLIENTE_FINALE));
    equal = equal && (this.B_CF_STRANIERO == null ? that.B_CF_STRANIERO == null : this.B_CF_STRANIERO.equals(that.B_CF_STRANIERO));
    equal = equal && (this.B_PERSONA_FISICA == null ? that.B_PERSONA_FISICA == null : this.B_PERSONA_FISICA.equals(that.B_PERSONA_FISICA));
    equal = equal && (this.D_DATA_CONTRATTO == null ? that.D_DATA_CONTRATTO == null : this.D_DATA_CONTRATTO.equals(that.D_DATA_CONTRATTO));
    equal = equal && (this.D_DATA_DECORRENZA == null ? that.D_DATA_DECORRENZA == null : this.D_DATA_DECORRENZA.equals(that.D_DATA_DECORRENZA));
    equal = equal && (this.B_AMMISSIBILE == null ? that.B_AMMISSIBILE == null : this.B_AMMISSIBILE.equals(that.B_AMMISSIBILE));
    equal = equal && (this.T_COD_CAUSALE == null ? that.T_COD_CAUSALE == null : this.T_COD_CAUSALE.equals(that.T_COD_CAUSALE));
    equal = equal && (this.T_MOTIVAZIONE == null ? that.T_MOTIVAZIONE == null : this.T_MOTIVAZIONE.equals(that.T_MOTIVAZIONE));
    equal = equal && (this.T_ESITO == null ? that.T_ESITO == null : this.T_ESITO.equals(that.T_ESITO));
    equal = equal && (this.T_COD_ESITO == null ? that.T_COD_ESITO == null : this.T_COD_ESITO.equals(that.T_COD_ESITO));
    equal = equal && (this.T_DETT_ESITO == null ? that.T_DETT_ESITO == null : this.T_DETT_ESITO.equals(that.T_DETT_ESITO));
    equal = equal && (this.D_DATA_STATO == null ? that.D_DATA_STATO == null : this.D_DATA_STATO.equals(that.D_DATA_STATO));
    equal = equal && (this.D_DATA_INSERIMENTO == null ? that.D_DATA_INSERIMENTO == null : this.D_DATA_INSERIMENTO.equals(that.D_DATA_INSERIMENTO));
    equal = equal && (this.T_TIPO_FORNITURA == null ? that.T_TIPO_FORNITURA == null : this.T_TIPO_FORNITURA.equals(that.T_TIPO_FORNITURA));
    equal = equal && (this.T_PIVA_CC == null ? that.T_PIVA_CC == null : this.T_PIVA_CC.equals(that.T_PIVA_CC));
    equal = equal && (this.T_REVOCA == null ? that.T_REVOCA == null : this.T_REVOCA.equals(that.T_REVOCA));
    equal = equal && (this.T_TIMG_PDR_CHIUSO_MOR == null ? that.T_TIMG_PDR_CHIUSO_MOR == null : this.T_TIMG_PDR_CHIUSO_MOR.equals(that.T_TIMG_PDR_CHIUSO_MOR));
    equal = equal && (this.T_TIMG_DATE_SOSP == null ? that.T_TIMG_DATE_SOSP == null : this.T_TIMG_DATE_SOSP.equals(that.T_TIMG_DATE_SOSP));
    equal = equal && (this.T_TIMG_DATE_SOST == null ? that.T_TIMG_DATE_SOST == null : this.T_TIMG_DATE_SOST.equals(that.T_TIMG_DATE_SOST));
    equal = equal && (this.T_TIMG_ACC_MIS == null ? that.T_TIMG_ACC_MIS == null : this.T_TIMG_ACC_MIS.equals(that.T_TIMG_ACC_MIS));
    equal = equal && (this.T_TIMG_PRESENZA_CMOR == null ? that.T_TIMG_PRESENZA_CMOR == null : this.T_TIMG_PRESENZA_CMOR.equals(that.T_TIMG_PRESENZA_CMOR));
    equal = equal && (this.D_TIVG_DATA_ATT_FDD == null ? that.D_TIVG_DATA_ATT_FDD == null : this.D_TIVG_DATA_ATT_FDD.equals(that.D_TIVG_DATA_ATT_FDD));
    equal = equal && (this.D_TIVG_DATA_CHIUSURA_PDR == null ? that.D_TIVG_DATA_CHIUSURA_PDR == null : this.D_TIVG_DATA_CHIUSURA_PDR.equals(that.D_TIVG_DATA_CHIUSURA_PDR));
    equal = equal && (this.B_TIMG_TIVG_CALCOLATO == null ? that.B_TIMG_TIVG_CALCOLATO == null : this.B_TIMG_TIVG_CALCOLATO.equals(that.B_TIMG_TIVG_CALCOLATO));
    equal = equal && (this.B_ANN_IN_RITARDO == null ? that.B_ANN_IN_RITARDO == null : this.B_ANN_IN_RITARDO.equals(that.B_ANN_IN_RITARDO));
    equal = equal && (this.T_CAUS_ANNULLAMENTO == null ? that.T_CAUS_ANNULLAMENTO == null : this.T_CAUS_ANNULLAMENTO.equals(that.T_CAUS_ANNULLAMENTO));
    equal = equal && (this.T_MOTIV_ANNULLAMENTO == null ? that.T_MOTIV_ANNULLAMENTO == null : this.T_MOTIV_ANNULLAMENTO.equals(that.T_MOTIV_ANNULLAMENTO));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof switch_gas_prt_swg)) {
      return false;
    }
    switch_gas_prt_swg that = (switch_gas_prt_swg) o;
    boolean equal = true;
    equal = equal && (this.N_ID_SWG == null ? that.N_ID_SWG == null : this.N_ID_SWG.equals(that.N_ID_SWG));
    equal = equal && (this.N_ID_PRATICA == null ? that.N_ID_PRATICA == null : this.N_ID_PRATICA.equals(that.N_ID_PRATICA));
    equal = equal && (this.T_CODICE_PDR == null ? that.T_CODICE_PDR == null : this.T_CODICE_PDR.equals(that.T_CODICE_PDR));
    equal = equal && (this.T_STATO == null ? that.T_STATO == null : this.T_STATO.equals(that.T_STATO));
    equal = equal && (this.N_ID_RICH == null ? that.N_ID_RICH == null : this.N_ID_RICH.equals(that.N_ID_RICH));
    equal = equal && (this.N_ID_DISTR == null ? that.N_ID_DISTR == null : this.N_ID_DISTR.equals(that.N_ID_DISTR));
    equal = equal && (this.N_ID_CC_ENTRANTE == null ? that.N_ID_CC_ENTRANTE == null : this.N_ID_CC_ENTRANTE.equals(that.N_ID_CC_ENTRANTE));
    equal = equal && (this.N_ID_CC_USCENTE == null ? that.N_ID_CC_USCENTE == null : this.N_ID_CC_USCENTE.equals(that.N_ID_CC_USCENTE));
    equal = equal && (this.N_ID_UDD_USCENTE == null ? that.N_ID_UDD_USCENTE == null : this.N_ID_UDD_USCENTE.equals(that.N_ID_UDD_USCENTE));
    equal = equal && (this.T_TITOLO == null ? that.T_TITOLO == null : this.T_TITOLO.equals(that.T_TITOLO));
    equal = equal && (this.D_DATA_RICHIESTA == null ? that.D_DATA_RICHIESTA == null : this.D_DATA_RICHIESTA.equals(that.D_DATA_RICHIESTA));
    equal = equal && (this.T_CP_UTENTE == null ? that.T_CP_UTENTE == null : this.T_CP_UTENTE.equals(that.T_CP_UTENTE));
    equal = equal && (this.T_CP_DISTRIBUTORE == null ? that.T_CP_DISTRIBUTORE == null : this.T_CP_DISTRIBUTORE.equals(that.T_CP_DISTRIBUTORE));
    equal = equal && (this.T_CF_CLIENTE_FINALE == null ? that.T_CF_CLIENTE_FINALE == null : this.T_CF_CLIENTE_FINALE.equals(that.T_CF_CLIENTE_FINALE));
    equal = equal && (this.T_PIVA_CLIENTE_FINALE == null ? that.T_PIVA_CLIENTE_FINALE == null : this.T_PIVA_CLIENTE_FINALE.equals(that.T_PIVA_CLIENTE_FINALE));
    equal = equal && (this.B_CF_STRANIERO == null ? that.B_CF_STRANIERO == null : this.B_CF_STRANIERO.equals(that.B_CF_STRANIERO));
    equal = equal && (this.B_PERSONA_FISICA == null ? that.B_PERSONA_FISICA == null : this.B_PERSONA_FISICA.equals(that.B_PERSONA_FISICA));
    equal = equal && (this.D_DATA_CONTRATTO == null ? that.D_DATA_CONTRATTO == null : this.D_DATA_CONTRATTO.equals(that.D_DATA_CONTRATTO));
    equal = equal && (this.D_DATA_DECORRENZA == null ? that.D_DATA_DECORRENZA == null : this.D_DATA_DECORRENZA.equals(that.D_DATA_DECORRENZA));
    equal = equal && (this.B_AMMISSIBILE == null ? that.B_AMMISSIBILE == null : this.B_AMMISSIBILE.equals(that.B_AMMISSIBILE));
    equal = equal && (this.T_COD_CAUSALE == null ? that.T_COD_CAUSALE == null : this.T_COD_CAUSALE.equals(that.T_COD_CAUSALE));
    equal = equal && (this.T_MOTIVAZIONE == null ? that.T_MOTIVAZIONE == null : this.T_MOTIVAZIONE.equals(that.T_MOTIVAZIONE));
    equal = equal && (this.T_ESITO == null ? that.T_ESITO == null : this.T_ESITO.equals(that.T_ESITO));
    equal = equal && (this.T_COD_ESITO == null ? that.T_COD_ESITO == null : this.T_COD_ESITO.equals(that.T_COD_ESITO));
    equal = equal && (this.T_DETT_ESITO == null ? that.T_DETT_ESITO == null : this.T_DETT_ESITO.equals(that.T_DETT_ESITO));
    equal = equal && (this.D_DATA_STATO == null ? that.D_DATA_STATO == null : this.D_DATA_STATO.equals(that.D_DATA_STATO));
    equal = equal && (this.D_DATA_INSERIMENTO == null ? that.D_DATA_INSERIMENTO == null : this.D_DATA_INSERIMENTO.equals(that.D_DATA_INSERIMENTO));
    equal = equal && (this.T_TIPO_FORNITURA == null ? that.T_TIPO_FORNITURA == null : this.T_TIPO_FORNITURA.equals(that.T_TIPO_FORNITURA));
    equal = equal && (this.T_PIVA_CC == null ? that.T_PIVA_CC == null : this.T_PIVA_CC.equals(that.T_PIVA_CC));
    equal = equal && (this.T_REVOCA == null ? that.T_REVOCA == null : this.T_REVOCA.equals(that.T_REVOCA));
    equal = equal && (this.T_TIMG_PDR_CHIUSO_MOR == null ? that.T_TIMG_PDR_CHIUSO_MOR == null : this.T_TIMG_PDR_CHIUSO_MOR.equals(that.T_TIMG_PDR_CHIUSO_MOR));
    equal = equal && (this.T_TIMG_DATE_SOSP == null ? that.T_TIMG_DATE_SOSP == null : this.T_TIMG_DATE_SOSP.equals(that.T_TIMG_DATE_SOSP));
    equal = equal && (this.T_TIMG_DATE_SOST == null ? that.T_TIMG_DATE_SOST == null : this.T_TIMG_DATE_SOST.equals(that.T_TIMG_DATE_SOST));
    equal = equal && (this.T_TIMG_ACC_MIS == null ? that.T_TIMG_ACC_MIS == null : this.T_TIMG_ACC_MIS.equals(that.T_TIMG_ACC_MIS));
    equal = equal && (this.T_TIMG_PRESENZA_CMOR == null ? that.T_TIMG_PRESENZA_CMOR == null : this.T_TIMG_PRESENZA_CMOR.equals(that.T_TIMG_PRESENZA_CMOR));
    equal = equal && (this.D_TIVG_DATA_ATT_FDD == null ? that.D_TIVG_DATA_ATT_FDD == null : this.D_TIVG_DATA_ATT_FDD.equals(that.D_TIVG_DATA_ATT_FDD));
    equal = equal && (this.D_TIVG_DATA_CHIUSURA_PDR == null ? that.D_TIVG_DATA_CHIUSURA_PDR == null : this.D_TIVG_DATA_CHIUSURA_PDR.equals(that.D_TIVG_DATA_CHIUSURA_PDR));
    equal = equal && (this.B_TIMG_TIVG_CALCOLATO == null ? that.B_TIMG_TIVG_CALCOLATO == null : this.B_TIMG_TIVG_CALCOLATO.equals(that.B_TIMG_TIVG_CALCOLATO));
    equal = equal && (this.B_ANN_IN_RITARDO == null ? that.B_ANN_IN_RITARDO == null : this.B_ANN_IN_RITARDO.equals(that.B_ANN_IN_RITARDO));
    equal = equal && (this.T_CAUS_ANNULLAMENTO == null ? that.T_CAUS_ANNULLAMENTO == null : this.T_CAUS_ANNULLAMENTO.equals(that.T_CAUS_ANNULLAMENTO));
    equal = equal && (this.T_MOTIV_ANNULLAMENTO == null ? that.T_MOTIV_ANNULLAMENTO == null : this.T_MOTIV_ANNULLAMENTO.equals(that.T_MOTIV_ANNULLAMENTO));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_SWG = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_PRATICA = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_CODICE_PDR = JdbcWritableBridge.readString(3, __dbResults);
    this.T_STATO = JdbcWritableBridge.readString(4, __dbResults);
    this.N_ID_RICH = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.N_ID_DISTR = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.N_ID_CC_ENTRANTE = JdbcWritableBridge.readBigDecimal(7, __dbResults);
    this.N_ID_CC_USCENTE = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.N_ID_UDD_USCENTE = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.T_TITOLO = JdbcWritableBridge.readString(10, __dbResults);
    this.D_DATA_RICHIESTA = JdbcWritableBridge.readString(11, __dbResults);
    this.T_CP_UTENTE = JdbcWritableBridge.readString(12, __dbResults);
    this.T_CP_DISTRIBUTORE = JdbcWritableBridge.readString(13, __dbResults);
    this.T_CF_CLIENTE_FINALE = JdbcWritableBridge.readString(14, __dbResults);
    this.T_PIVA_CLIENTE_FINALE = JdbcWritableBridge.readString(15, __dbResults);
    this.B_CF_STRANIERO = JdbcWritableBridge.readString(16, __dbResults);
    this.B_PERSONA_FISICA = JdbcWritableBridge.readString(17, __dbResults);
    this.D_DATA_CONTRATTO = JdbcWritableBridge.readString(18, __dbResults);
    this.D_DATA_DECORRENZA = JdbcWritableBridge.readString(19, __dbResults);
    this.B_AMMISSIBILE = JdbcWritableBridge.readString(20, __dbResults);
    this.T_COD_CAUSALE = JdbcWritableBridge.readString(21, __dbResults);
    this.T_MOTIVAZIONE = JdbcWritableBridge.readString(22, __dbResults);
    this.T_ESITO = JdbcWritableBridge.readString(23, __dbResults);
    this.T_COD_ESITO = JdbcWritableBridge.readString(24, __dbResults);
    this.T_DETT_ESITO = JdbcWritableBridge.readString(25, __dbResults);
    this.D_DATA_STATO = JdbcWritableBridge.readString(26, __dbResults);
    this.D_DATA_INSERIMENTO = JdbcWritableBridge.readString(27, __dbResults);
    this.T_TIPO_FORNITURA = JdbcWritableBridge.readString(28, __dbResults);
    this.T_PIVA_CC = JdbcWritableBridge.readString(29, __dbResults);
    this.T_REVOCA = JdbcWritableBridge.readString(30, __dbResults);
    this.T_TIMG_PDR_CHIUSO_MOR = JdbcWritableBridge.readString(31, __dbResults);
    this.T_TIMG_DATE_SOSP = JdbcWritableBridge.readString(32, __dbResults);
    this.T_TIMG_DATE_SOST = JdbcWritableBridge.readString(33, __dbResults);
    this.T_TIMG_ACC_MIS = JdbcWritableBridge.readString(34, __dbResults);
    this.T_TIMG_PRESENZA_CMOR = JdbcWritableBridge.readString(35, __dbResults);
    this.D_TIVG_DATA_ATT_FDD = JdbcWritableBridge.readString(36, __dbResults);
    this.D_TIVG_DATA_CHIUSURA_PDR = JdbcWritableBridge.readString(37, __dbResults);
    this.B_TIMG_TIVG_CALCOLATO = JdbcWritableBridge.readString(38, __dbResults);
    this.B_ANN_IN_RITARDO = JdbcWritableBridge.readString(39, __dbResults);
    this.T_CAUS_ANNULLAMENTO = JdbcWritableBridge.readString(40, __dbResults);
    this.T_MOTIV_ANNULLAMENTO = JdbcWritableBridge.readString(41, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_SWG = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_PRATICA = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_CODICE_PDR = JdbcWritableBridge.readString(3, __dbResults);
    this.T_STATO = JdbcWritableBridge.readString(4, __dbResults);
    this.N_ID_RICH = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.N_ID_DISTR = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.N_ID_CC_ENTRANTE = JdbcWritableBridge.readBigDecimal(7, __dbResults);
    this.N_ID_CC_USCENTE = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.N_ID_UDD_USCENTE = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.T_TITOLO = JdbcWritableBridge.readString(10, __dbResults);
    this.D_DATA_RICHIESTA = JdbcWritableBridge.readString(11, __dbResults);
    this.T_CP_UTENTE = JdbcWritableBridge.readString(12, __dbResults);
    this.T_CP_DISTRIBUTORE = JdbcWritableBridge.readString(13, __dbResults);
    this.T_CF_CLIENTE_FINALE = JdbcWritableBridge.readString(14, __dbResults);
    this.T_PIVA_CLIENTE_FINALE = JdbcWritableBridge.readString(15, __dbResults);
    this.B_CF_STRANIERO = JdbcWritableBridge.readString(16, __dbResults);
    this.B_PERSONA_FISICA = JdbcWritableBridge.readString(17, __dbResults);
    this.D_DATA_CONTRATTO = JdbcWritableBridge.readString(18, __dbResults);
    this.D_DATA_DECORRENZA = JdbcWritableBridge.readString(19, __dbResults);
    this.B_AMMISSIBILE = JdbcWritableBridge.readString(20, __dbResults);
    this.T_COD_CAUSALE = JdbcWritableBridge.readString(21, __dbResults);
    this.T_MOTIVAZIONE = JdbcWritableBridge.readString(22, __dbResults);
    this.T_ESITO = JdbcWritableBridge.readString(23, __dbResults);
    this.T_COD_ESITO = JdbcWritableBridge.readString(24, __dbResults);
    this.T_DETT_ESITO = JdbcWritableBridge.readString(25, __dbResults);
    this.D_DATA_STATO = JdbcWritableBridge.readString(26, __dbResults);
    this.D_DATA_INSERIMENTO = JdbcWritableBridge.readString(27, __dbResults);
    this.T_TIPO_FORNITURA = JdbcWritableBridge.readString(28, __dbResults);
    this.T_PIVA_CC = JdbcWritableBridge.readString(29, __dbResults);
    this.T_REVOCA = JdbcWritableBridge.readString(30, __dbResults);
    this.T_TIMG_PDR_CHIUSO_MOR = JdbcWritableBridge.readString(31, __dbResults);
    this.T_TIMG_DATE_SOSP = JdbcWritableBridge.readString(32, __dbResults);
    this.T_TIMG_DATE_SOST = JdbcWritableBridge.readString(33, __dbResults);
    this.T_TIMG_ACC_MIS = JdbcWritableBridge.readString(34, __dbResults);
    this.T_TIMG_PRESENZA_CMOR = JdbcWritableBridge.readString(35, __dbResults);
    this.D_TIVG_DATA_ATT_FDD = JdbcWritableBridge.readString(36, __dbResults);
    this.D_TIVG_DATA_CHIUSURA_PDR = JdbcWritableBridge.readString(37, __dbResults);
    this.B_TIMG_TIVG_CALCOLATO = JdbcWritableBridge.readString(38, __dbResults);
    this.B_ANN_IN_RITARDO = JdbcWritableBridge.readString(39, __dbResults);
    this.T_CAUS_ANNULLAMENTO = JdbcWritableBridge.readString(40, __dbResults);
    this.T_MOTIV_ANNULLAMENTO = JdbcWritableBridge.readString(41, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_SWG, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PRATICA, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_PDR, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_RICH, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTR, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CC_ENTRANTE, 7 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CC_USCENTE, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UDD_USCENTE, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_TITOLO, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RICHIESTA, 11 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_CP_UTENTE, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CP_DISTRIBUTORE, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CF_CLIENTE_FINALE, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA_CLIENTE_FINALE, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_CF_STRANIERO, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_PERSONA_FISICA, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_CONTRATTO, 18 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_DECORRENZA, 19 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(B_AMMISSIBILE, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CAUSALE, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MOTIVAZIONE, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ESITO, 23 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_ESITO, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DETT_ESITO, 25 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_STATO, 26 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INSERIMENTO, 27 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_FORNITURA, 28 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA_CC, 29 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_REVOCA, 30 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIMG_PDR_CHIUSO_MOR, 31 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIMG_DATE_SOSP, 32 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIMG_DATE_SOST, 33 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIMG_ACC_MIS, 34 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIMG_PRESENZA_CMOR, 35 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_TIVG_DATA_ATT_FDD, 36 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_TIVG_DATA_CHIUSURA_PDR, 37 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(B_TIMG_TIVG_CALCOLATO, 38 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_ANN_IN_RITARDO, 39 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CAUS_ANNULLAMENTO, 40 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MOTIV_ANNULLAMENTO, 41 + __off, 12, __dbStmt);
    return 41;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_SWG, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PRATICA, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_PDR, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_RICH, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTR, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CC_ENTRANTE, 7 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CC_USCENTE, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UDD_USCENTE, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_TITOLO, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RICHIESTA, 11 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_CP_UTENTE, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CP_DISTRIBUTORE, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CF_CLIENTE_FINALE, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA_CLIENTE_FINALE, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_CF_STRANIERO, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_PERSONA_FISICA, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_CONTRATTO, 18 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_DECORRENZA, 19 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(B_AMMISSIBILE, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CAUSALE, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MOTIVAZIONE, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ESITO, 23 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_ESITO, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DETT_ESITO, 25 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_STATO, 26 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INSERIMENTO, 27 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_FORNITURA, 28 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA_CC, 29 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_REVOCA, 30 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIMG_PDR_CHIUSO_MOR, 31 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIMG_DATE_SOSP, 32 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIMG_DATE_SOST, 33 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIMG_ACC_MIS, 34 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIMG_PRESENZA_CMOR, 35 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_TIVG_DATA_ATT_FDD, 36 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_TIVG_DATA_CHIUSURA_PDR, 37 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(B_TIMG_TIVG_CALCOLATO, 38 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_ANN_IN_RITARDO, 39 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CAUS_ANNULLAMENTO, 40 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MOTIV_ANNULLAMENTO, 41 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_SWG = null;
    } else {
    this.N_ID_SWG = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_PRATICA = null;
    } else {
    this.N_ID_PRATICA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_PDR = null;
    } else {
    this.T_CODICE_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_STATO = null;
    } else {
    this.T_STATO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_RICH = null;
    } else {
    this.N_ID_RICH = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_DISTR = null;
    } else {
    this.N_ID_DISTR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_CC_ENTRANTE = null;
    } else {
    this.N_ID_CC_ENTRANTE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_CC_USCENTE = null;
    } else {
    this.N_ID_CC_USCENTE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_UDD_USCENTE = null;
    } else {
    this.N_ID_UDD_USCENTE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TITOLO = null;
    } else {
    this.T_TITOLO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_RICHIESTA = null;
    } else {
    this.D_DATA_RICHIESTA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CP_UTENTE = null;
    } else {
    this.T_CP_UTENTE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CP_DISTRIBUTORE = null;
    } else {
    this.T_CP_DISTRIBUTORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CF_CLIENTE_FINALE = null;
    } else {
    this.T_CF_CLIENTE_FINALE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PIVA_CLIENTE_FINALE = null;
    } else {
    this.T_PIVA_CLIENTE_FINALE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_CF_STRANIERO = null;
    } else {
    this.B_CF_STRANIERO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_PERSONA_FISICA = null;
    } else {
    this.B_PERSONA_FISICA = Text.readString(__dataIn);
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
        this.T_ESITO = null;
    } else {
    this.T_ESITO = Text.readString(__dataIn);
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
        this.D_DATA_STATO = null;
    } else {
    this.D_DATA_STATO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_INSERIMENTO = null;
    } else {
    this.D_DATA_INSERIMENTO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO_FORNITURA = null;
    } else {
    this.T_TIPO_FORNITURA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PIVA_CC = null;
    } else {
    this.T_PIVA_CC = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_REVOCA = null;
    } else {
    this.T_REVOCA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIMG_PDR_CHIUSO_MOR = null;
    } else {
    this.T_TIMG_PDR_CHIUSO_MOR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIMG_DATE_SOSP = null;
    } else {
    this.T_TIMG_DATE_SOSP = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIMG_DATE_SOST = null;
    } else {
    this.T_TIMG_DATE_SOST = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIMG_ACC_MIS = null;
    } else {
    this.T_TIMG_ACC_MIS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIMG_PRESENZA_CMOR = null;
    } else {
    this.T_TIMG_PRESENZA_CMOR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_TIVG_DATA_ATT_FDD = null;
    } else {
    this.D_TIVG_DATA_ATT_FDD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_TIVG_DATA_CHIUSURA_PDR = null;
    } else {
    this.D_TIVG_DATA_CHIUSURA_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_TIMG_TIVG_CALCOLATO = null;
    } else {
    this.B_TIMG_TIVG_CALCOLATO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_ANN_IN_RITARDO = null;
    } else {
    this.B_ANN_IN_RITARDO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CAUS_ANNULLAMENTO = null;
    } else {
    this.T_CAUS_ANNULLAMENTO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MOTIV_ANNULLAMENTO = null;
    } else {
    this.T_MOTIV_ANNULLAMENTO = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_SWG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_SWG, __dataOut);
    }
    if (null == this.N_ID_PRATICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PRATICA, __dataOut);
    }
    if (null == this.T_CODICE_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_PDR);
    }
    if (null == this.T_STATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_STATO);
    }
    if (null == this.N_ID_RICH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_RICH, __dataOut);
    }
    if (null == this.N_ID_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISTR, __dataOut);
    }
    if (null == this.N_ID_CC_ENTRANTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CC_ENTRANTE, __dataOut);
    }
    if (null == this.N_ID_CC_USCENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CC_USCENTE, __dataOut);
    }
    if (null == this.N_ID_UDD_USCENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UDD_USCENTE, __dataOut);
    }
    if (null == this.T_TITOLO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TITOLO);
    }
    if (null == this.D_DATA_RICHIESTA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RICHIESTA);
    }
    if (null == this.T_CP_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CP_UTENTE);
    }
    if (null == this.T_CP_DISTRIBUTORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CP_DISTRIBUTORE);
    }
    if (null == this.T_CF_CLIENTE_FINALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CF_CLIENTE_FINALE);
    }
    if (null == this.T_PIVA_CLIENTE_FINALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA_CLIENTE_FINALE);
    }
    if (null == this.B_CF_STRANIERO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_CF_STRANIERO);
    }
    if (null == this.B_PERSONA_FISICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_PERSONA_FISICA);
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
    if (null == this.T_ESITO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ESITO);
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
    if (null == this.D_DATA_STATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_STATO);
    }
    if (null == this.D_DATA_INSERIMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INSERIMENTO);
    }
    if (null == this.T_TIPO_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_FORNITURA);
    }
    if (null == this.T_PIVA_CC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA_CC);
    }
    if (null == this.T_REVOCA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_REVOCA);
    }
    if (null == this.T_TIMG_PDR_CHIUSO_MOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIMG_PDR_CHIUSO_MOR);
    }
    if (null == this.T_TIMG_DATE_SOSP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIMG_DATE_SOSP);
    }
    if (null == this.T_TIMG_DATE_SOST) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIMG_DATE_SOST);
    }
    if (null == this.T_TIMG_ACC_MIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIMG_ACC_MIS);
    }
    if (null == this.T_TIMG_PRESENZA_CMOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIMG_PRESENZA_CMOR);
    }
    if (null == this.D_TIVG_DATA_ATT_FDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_TIVG_DATA_ATT_FDD);
    }
    if (null == this.D_TIVG_DATA_CHIUSURA_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_TIVG_DATA_CHIUSURA_PDR);
    }
    if (null == this.B_TIMG_TIVG_CALCOLATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_TIMG_TIVG_CALCOLATO);
    }
    if (null == this.B_ANN_IN_RITARDO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_ANN_IN_RITARDO);
    }
    if (null == this.T_CAUS_ANNULLAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CAUS_ANNULLAMENTO);
    }
    if (null == this.T_MOTIV_ANNULLAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MOTIV_ANNULLAMENTO);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_SWG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_SWG, __dataOut);
    }
    if (null == this.N_ID_PRATICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PRATICA, __dataOut);
    }
    if (null == this.T_CODICE_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_PDR);
    }
    if (null == this.T_STATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_STATO);
    }
    if (null == this.N_ID_RICH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_RICH, __dataOut);
    }
    if (null == this.N_ID_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISTR, __dataOut);
    }
    if (null == this.N_ID_CC_ENTRANTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CC_ENTRANTE, __dataOut);
    }
    if (null == this.N_ID_CC_USCENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CC_USCENTE, __dataOut);
    }
    if (null == this.N_ID_UDD_USCENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UDD_USCENTE, __dataOut);
    }
    if (null == this.T_TITOLO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TITOLO);
    }
    if (null == this.D_DATA_RICHIESTA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RICHIESTA);
    }
    if (null == this.T_CP_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CP_UTENTE);
    }
    if (null == this.T_CP_DISTRIBUTORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CP_DISTRIBUTORE);
    }
    if (null == this.T_CF_CLIENTE_FINALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CF_CLIENTE_FINALE);
    }
    if (null == this.T_PIVA_CLIENTE_FINALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA_CLIENTE_FINALE);
    }
    if (null == this.B_CF_STRANIERO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_CF_STRANIERO);
    }
    if (null == this.B_PERSONA_FISICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_PERSONA_FISICA);
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
    if (null == this.T_ESITO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ESITO);
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
    if (null == this.D_DATA_STATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_STATO);
    }
    if (null == this.D_DATA_INSERIMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INSERIMENTO);
    }
    if (null == this.T_TIPO_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_FORNITURA);
    }
    if (null == this.T_PIVA_CC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA_CC);
    }
    if (null == this.T_REVOCA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_REVOCA);
    }
    if (null == this.T_TIMG_PDR_CHIUSO_MOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIMG_PDR_CHIUSO_MOR);
    }
    if (null == this.T_TIMG_DATE_SOSP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIMG_DATE_SOSP);
    }
    if (null == this.T_TIMG_DATE_SOST) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIMG_DATE_SOST);
    }
    if (null == this.T_TIMG_ACC_MIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIMG_ACC_MIS);
    }
    if (null == this.T_TIMG_PRESENZA_CMOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIMG_PRESENZA_CMOR);
    }
    if (null == this.D_TIVG_DATA_ATT_FDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_TIVG_DATA_ATT_FDD);
    }
    if (null == this.D_TIVG_DATA_CHIUSURA_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_TIVG_DATA_CHIUSURA_PDR);
    }
    if (null == this.B_TIMG_TIVG_CALCOLATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_TIMG_TIVG_CALCOLATO);
    }
    if (null == this.B_ANN_IN_RITARDO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_ANN_IN_RITARDO);
    }
    if (null == this.T_CAUS_ANNULLAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CAUS_ANNULLAMENTO);
    }
    if (null == this.T_MOTIV_ANNULLAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MOTIV_ANNULLAMENTO);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_SWG==null?"":N_ID_SWG.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PRATICA==null?"":N_ID_PRATICA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_PDR==null?"":T_CODICE_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO==null?"":T_STATO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_RICH==null?"":N_ID_RICH.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISTR==null?"":N_ID_DISTR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CC_ENTRANTE==null?"":N_ID_CC_ENTRANTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CC_USCENTE==null?"":N_ID_CC_USCENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UDD_USCENTE==null?"":N_ID_UDD_USCENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TITOLO==null?"":T_TITOLO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RICHIESTA==null?"":D_DATA_RICHIESTA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CP_UTENTE==null?"":T_CP_UTENTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CP_DISTRIBUTORE==null?"":T_CP_DISTRIBUTORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CF_CLIENTE_FINALE==null?"":T_CF_CLIENTE_FINALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA_CLIENTE_FINALE==null?"":T_PIVA_CLIENTE_FINALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_CF_STRANIERO==null?"":B_CF_STRANIERO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_PERSONA_FISICA==null?"":B_PERSONA_FISICA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_CONTRATTO==null?"":D_DATA_CONTRATTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_DECORRENZA==null?"":D_DATA_DECORRENZA, " ", delimiters));
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
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ESITO==null?"":T_ESITO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_ESITO==null?"":T_COD_ESITO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DETT_ESITO==null?"":T_DETT_ESITO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_STATO==null?"":D_DATA_STATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INSERIMENTO==null?"":D_DATA_INSERIMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_FORNITURA==null?"":T_TIPO_FORNITURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA_CC==null?"":T_PIVA_CC, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_REVOCA==null?"":T_REVOCA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIMG_PDR_CHIUSO_MOR==null?"":T_TIMG_PDR_CHIUSO_MOR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIMG_DATE_SOSP==null?"":T_TIMG_DATE_SOSP, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIMG_DATE_SOST==null?"":T_TIMG_DATE_SOST, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIMG_ACC_MIS==null?"":T_TIMG_ACC_MIS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIMG_PRESENZA_CMOR==null?"":T_TIMG_PRESENZA_CMOR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_TIVG_DATA_ATT_FDD==null?"":D_TIVG_DATA_ATT_FDD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_TIVG_DATA_CHIUSURA_PDR==null?"":D_TIVG_DATA_CHIUSURA_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_TIMG_TIVG_CALCOLATO==null?"":B_TIMG_TIVG_CALCOLATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_ANN_IN_RITARDO==null?"":B_ANN_IN_RITARDO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CAUS_ANNULLAMENTO==null?"":T_CAUS_ANNULLAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MOTIV_ANNULLAMENTO==null?"":T_MOTIV_ANNULLAMENTO, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_SWG==null?"":N_ID_SWG.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PRATICA==null?"":N_ID_PRATICA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_PDR==null?"":T_CODICE_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO==null?"":T_STATO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_RICH==null?"":N_ID_RICH.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISTR==null?"":N_ID_DISTR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CC_ENTRANTE==null?"":N_ID_CC_ENTRANTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CC_USCENTE==null?"":N_ID_CC_USCENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UDD_USCENTE==null?"":N_ID_UDD_USCENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TITOLO==null?"":T_TITOLO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RICHIESTA==null?"":D_DATA_RICHIESTA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CP_UTENTE==null?"":T_CP_UTENTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CP_DISTRIBUTORE==null?"":T_CP_DISTRIBUTORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CF_CLIENTE_FINALE==null?"":T_CF_CLIENTE_FINALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA_CLIENTE_FINALE==null?"":T_PIVA_CLIENTE_FINALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_CF_STRANIERO==null?"":B_CF_STRANIERO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_PERSONA_FISICA==null?"":B_PERSONA_FISICA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_CONTRATTO==null?"":D_DATA_CONTRATTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_DECORRENZA==null?"":D_DATA_DECORRENZA, " ", delimiters));
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
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ESITO==null?"":T_ESITO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_ESITO==null?"":T_COD_ESITO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DETT_ESITO==null?"":T_DETT_ESITO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_STATO==null?"":D_DATA_STATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INSERIMENTO==null?"":D_DATA_INSERIMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_FORNITURA==null?"":T_TIPO_FORNITURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA_CC==null?"":T_PIVA_CC, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_REVOCA==null?"":T_REVOCA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIMG_PDR_CHIUSO_MOR==null?"":T_TIMG_PDR_CHIUSO_MOR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIMG_DATE_SOSP==null?"":T_TIMG_DATE_SOSP, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIMG_DATE_SOST==null?"":T_TIMG_DATE_SOST, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIMG_ACC_MIS==null?"":T_TIMG_ACC_MIS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIMG_PRESENZA_CMOR==null?"":T_TIMG_PRESENZA_CMOR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_TIVG_DATA_ATT_FDD==null?"":D_TIVG_DATA_ATT_FDD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_TIVG_DATA_CHIUSURA_PDR==null?"":D_TIVG_DATA_CHIUSURA_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_TIMG_TIVG_CALCOLATO==null?"":B_TIMG_TIVG_CALCOLATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_ANN_IN_RITARDO==null?"":B_ANN_IN_RITARDO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CAUS_ANNULLAMENTO==null?"":T_CAUS_ANNULLAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MOTIV_ANNULLAMENTO==null?"":T_MOTIV_ANNULLAMENTO, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_SWG = null; } else {
      this.N_ID_SWG = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PRATICA = null; } else {
      this.N_ID_PRATICA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_PDR = null; } else {
      this.T_CODICE_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_STATO = null; } else {
      this.T_STATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_RICH = null; } else {
      this.N_ID_RICH = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISTR = null; } else {
      this.N_ID_DISTR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CC_ENTRANTE = null; } else {
      this.N_ID_CC_ENTRANTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CC_USCENTE = null; } else {
      this.N_ID_CC_USCENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UDD_USCENTE = null; } else {
      this.N_ID_UDD_USCENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TITOLO = null; } else {
      this.T_TITOLO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RICHIESTA = null; } else {
      this.D_DATA_RICHIESTA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CP_UTENTE = null; } else {
      this.T_CP_UTENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CP_DISTRIBUTORE = null; } else {
      this.T_CP_DISTRIBUTORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CF_CLIENTE_FINALE = null; } else {
      this.T_CF_CLIENTE_FINALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA_CLIENTE_FINALE = null; } else {
      this.T_PIVA_CLIENTE_FINALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_CF_STRANIERO = null; } else {
      this.B_CF_STRANIERO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_PERSONA_FISICA = null; } else {
      this.B_PERSONA_FISICA = __cur_str;
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
    if (__cur_str.equals("null")) { this.T_ESITO = null; } else {
      this.T_ESITO = __cur_str;
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
    if (__cur_str.equals("null")) { this.D_DATA_STATO = null; } else {
      this.D_DATA_STATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INSERIMENTO = null; } else {
      this.D_DATA_INSERIMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_FORNITURA = null; } else {
      this.T_TIPO_FORNITURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA_CC = null; } else {
      this.T_PIVA_CC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_REVOCA = null; } else {
      this.T_REVOCA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIMG_PDR_CHIUSO_MOR = null; } else {
      this.T_TIMG_PDR_CHIUSO_MOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIMG_DATE_SOSP = null; } else {
      this.T_TIMG_DATE_SOSP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIMG_DATE_SOST = null; } else {
      this.T_TIMG_DATE_SOST = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIMG_ACC_MIS = null; } else {
      this.T_TIMG_ACC_MIS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIMG_PRESENZA_CMOR = null; } else {
      this.T_TIMG_PRESENZA_CMOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_TIVG_DATA_ATT_FDD = null; } else {
      this.D_TIVG_DATA_ATT_FDD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_TIVG_DATA_CHIUSURA_PDR = null; } else {
      this.D_TIVG_DATA_CHIUSURA_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_TIMG_TIVG_CALCOLATO = null; } else {
      this.B_TIMG_TIVG_CALCOLATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_ANN_IN_RITARDO = null; } else {
      this.B_ANN_IN_RITARDO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CAUS_ANNULLAMENTO = null; } else {
      this.T_CAUS_ANNULLAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MOTIV_ANNULLAMENTO = null; } else {
      this.T_MOTIV_ANNULLAMENTO = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_SWG = null; } else {
      this.N_ID_SWG = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PRATICA = null; } else {
      this.N_ID_PRATICA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_PDR = null; } else {
      this.T_CODICE_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_STATO = null; } else {
      this.T_STATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_RICH = null; } else {
      this.N_ID_RICH = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISTR = null; } else {
      this.N_ID_DISTR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CC_ENTRANTE = null; } else {
      this.N_ID_CC_ENTRANTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CC_USCENTE = null; } else {
      this.N_ID_CC_USCENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UDD_USCENTE = null; } else {
      this.N_ID_UDD_USCENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TITOLO = null; } else {
      this.T_TITOLO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RICHIESTA = null; } else {
      this.D_DATA_RICHIESTA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CP_UTENTE = null; } else {
      this.T_CP_UTENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CP_DISTRIBUTORE = null; } else {
      this.T_CP_DISTRIBUTORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CF_CLIENTE_FINALE = null; } else {
      this.T_CF_CLIENTE_FINALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA_CLIENTE_FINALE = null; } else {
      this.T_PIVA_CLIENTE_FINALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_CF_STRANIERO = null; } else {
      this.B_CF_STRANIERO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_PERSONA_FISICA = null; } else {
      this.B_PERSONA_FISICA = __cur_str;
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
    if (__cur_str.equals("null")) { this.T_ESITO = null; } else {
      this.T_ESITO = __cur_str;
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
    if (__cur_str.equals("null")) { this.D_DATA_STATO = null; } else {
      this.D_DATA_STATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INSERIMENTO = null; } else {
      this.D_DATA_INSERIMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_FORNITURA = null; } else {
      this.T_TIPO_FORNITURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA_CC = null; } else {
      this.T_PIVA_CC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_REVOCA = null; } else {
      this.T_REVOCA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIMG_PDR_CHIUSO_MOR = null; } else {
      this.T_TIMG_PDR_CHIUSO_MOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIMG_DATE_SOSP = null; } else {
      this.T_TIMG_DATE_SOSP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIMG_DATE_SOST = null; } else {
      this.T_TIMG_DATE_SOST = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIMG_ACC_MIS = null; } else {
      this.T_TIMG_ACC_MIS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIMG_PRESENZA_CMOR = null; } else {
      this.T_TIMG_PRESENZA_CMOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_TIVG_DATA_ATT_FDD = null; } else {
      this.D_TIVG_DATA_ATT_FDD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_TIVG_DATA_CHIUSURA_PDR = null; } else {
      this.D_TIVG_DATA_CHIUSURA_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_TIMG_TIVG_CALCOLATO = null; } else {
      this.B_TIMG_TIVG_CALCOLATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_ANN_IN_RITARDO = null; } else {
      this.B_ANN_IN_RITARDO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CAUS_ANNULLAMENTO = null; } else {
      this.T_CAUS_ANNULLAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MOTIV_ANNULLAMENTO = null; } else {
      this.T_MOTIV_ANNULLAMENTO = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    switch_gas_prt_swg o = (switch_gas_prt_swg) super.clone();
    return o;
  }

  public void clone0(switch_gas_prt_swg o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_SWG", this.N_ID_SWG);
    __sqoop$field_map.put("N_ID_PRATICA", this.N_ID_PRATICA);
    __sqoop$field_map.put("T_CODICE_PDR", this.T_CODICE_PDR);
    __sqoop$field_map.put("T_STATO", this.T_STATO);
    __sqoop$field_map.put("N_ID_RICH", this.N_ID_RICH);
    __sqoop$field_map.put("N_ID_DISTR", this.N_ID_DISTR);
    __sqoop$field_map.put("N_ID_CC_ENTRANTE", this.N_ID_CC_ENTRANTE);
    __sqoop$field_map.put("N_ID_CC_USCENTE", this.N_ID_CC_USCENTE);
    __sqoop$field_map.put("N_ID_UDD_USCENTE", this.N_ID_UDD_USCENTE);
    __sqoop$field_map.put("T_TITOLO", this.T_TITOLO);
    __sqoop$field_map.put("D_DATA_RICHIESTA", this.D_DATA_RICHIESTA);
    __sqoop$field_map.put("T_CP_UTENTE", this.T_CP_UTENTE);
    __sqoop$field_map.put("T_CP_DISTRIBUTORE", this.T_CP_DISTRIBUTORE);
    __sqoop$field_map.put("T_CF_CLIENTE_FINALE", this.T_CF_CLIENTE_FINALE);
    __sqoop$field_map.put("T_PIVA_CLIENTE_FINALE", this.T_PIVA_CLIENTE_FINALE);
    __sqoop$field_map.put("B_CF_STRANIERO", this.B_CF_STRANIERO);
    __sqoop$field_map.put("B_PERSONA_FISICA", this.B_PERSONA_FISICA);
    __sqoop$field_map.put("D_DATA_CONTRATTO", this.D_DATA_CONTRATTO);
    __sqoop$field_map.put("D_DATA_DECORRENZA", this.D_DATA_DECORRENZA);
    __sqoop$field_map.put("B_AMMISSIBILE", this.B_AMMISSIBILE);
    __sqoop$field_map.put("T_COD_CAUSALE", this.T_COD_CAUSALE);
    __sqoop$field_map.put("T_MOTIVAZIONE", this.T_MOTIVAZIONE);
    __sqoop$field_map.put("T_ESITO", this.T_ESITO);
    __sqoop$field_map.put("T_COD_ESITO", this.T_COD_ESITO);
    __sqoop$field_map.put("T_DETT_ESITO", this.T_DETT_ESITO);
    __sqoop$field_map.put("D_DATA_STATO", this.D_DATA_STATO);
    __sqoop$field_map.put("D_DATA_INSERIMENTO", this.D_DATA_INSERIMENTO);
    __sqoop$field_map.put("T_TIPO_FORNITURA", this.T_TIPO_FORNITURA);
    __sqoop$field_map.put("T_PIVA_CC", this.T_PIVA_CC);
    __sqoop$field_map.put("T_REVOCA", this.T_REVOCA);
    __sqoop$field_map.put("T_TIMG_PDR_CHIUSO_MOR", this.T_TIMG_PDR_CHIUSO_MOR);
    __sqoop$field_map.put("T_TIMG_DATE_SOSP", this.T_TIMG_DATE_SOSP);
    __sqoop$field_map.put("T_TIMG_DATE_SOST", this.T_TIMG_DATE_SOST);
    __sqoop$field_map.put("T_TIMG_ACC_MIS", this.T_TIMG_ACC_MIS);
    __sqoop$field_map.put("T_TIMG_PRESENZA_CMOR", this.T_TIMG_PRESENZA_CMOR);
    __sqoop$field_map.put("D_TIVG_DATA_ATT_FDD", this.D_TIVG_DATA_ATT_FDD);
    __sqoop$field_map.put("D_TIVG_DATA_CHIUSURA_PDR", this.D_TIVG_DATA_CHIUSURA_PDR);
    __sqoop$field_map.put("B_TIMG_TIVG_CALCOLATO", this.B_TIMG_TIVG_CALCOLATO);
    __sqoop$field_map.put("B_ANN_IN_RITARDO", this.B_ANN_IN_RITARDO);
    __sqoop$field_map.put("T_CAUS_ANNULLAMENTO", this.T_CAUS_ANNULLAMENTO);
    __sqoop$field_map.put("T_MOTIV_ANNULLAMENTO", this.T_MOTIV_ANNULLAMENTO);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_SWG", this.N_ID_SWG);
    __sqoop$field_map.put("N_ID_PRATICA", this.N_ID_PRATICA);
    __sqoop$field_map.put("T_CODICE_PDR", this.T_CODICE_PDR);
    __sqoop$field_map.put("T_STATO", this.T_STATO);
    __sqoop$field_map.put("N_ID_RICH", this.N_ID_RICH);
    __sqoop$field_map.put("N_ID_DISTR", this.N_ID_DISTR);
    __sqoop$field_map.put("N_ID_CC_ENTRANTE", this.N_ID_CC_ENTRANTE);
    __sqoop$field_map.put("N_ID_CC_USCENTE", this.N_ID_CC_USCENTE);
    __sqoop$field_map.put("N_ID_UDD_USCENTE", this.N_ID_UDD_USCENTE);
    __sqoop$field_map.put("T_TITOLO", this.T_TITOLO);
    __sqoop$field_map.put("D_DATA_RICHIESTA", this.D_DATA_RICHIESTA);
    __sqoop$field_map.put("T_CP_UTENTE", this.T_CP_UTENTE);
    __sqoop$field_map.put("T_CP_DISTRIBUTORE", this.T_CP_DISTRIBUTORE);
    __sqoop$field_map.put("T_CF_CLIENTE_FINALE", this.T_CF_CLIENTE_FINALE);
    __sqoop$field_map.put("T_PIVA_CLIENTE_FINALE", this.T_PIVA_CLIENTE_FINALE);
    __sqoop$field_map.put("B_CF_STRANIERO", this.B_CF_STRANIERO);
    __sqoop$field_map.put("B_PERSONA_FISICA", this.B_PERSONA_FISICA);
    __sqoop$field_map.put("D_DATA_CONTRATTO", this.D_DATA_CONTRATTO);
    __sqoop$field_map.put("D_DATA_DECORRENZA", this.D_DATA_DECORRENZA);
    __sqoop$field_map.put("B_AMMISSIBILE", this.B_AMMISSIBILE);
    __sqoop$field_map.put("T_COD_CAUSALE", this.T_COD_CAUSALE);
    __sqoop$field_map.put("T_MOTIVAZIONE", this.T_MOTIVAZIONE);
    __sqoop$field_map.put("T_ESITO", this.T_ESITO);
    __sqoop$field_map.put("T_COD_ESITO", this.T_COD_ESITO);
    __sqoop$field_map.put("T_DETT_ESITO", this.T_DETT_ESITO);
    __sqoop$field_map.put("D_DATA_STATO", this.D_DATA_STATO);
    __sqoop$field_map.put("D_DATA_INSERIMENTO", this.D_DATA_INSERIMENTO);
    __sqoop$field_map.put("T_TIPO_FORNITURA", this.T_TIPO_FORNITURA);
    __sqoop$field_map.put("T_PIVA_CC", this.T_PIVA_CC);
    __sqoop$field_map.put("T_REVOCA", this.T_REVOCA);
    __sqoop$field_map.put("T_TIMG_PDR_CHIUSO_MOR", this.T_TIMG_PDR_CHIUSO_MOR);
    __sqoop$field_map.put("T_TIMG_DATE_SOSP", this.T_TIMG_DATE_SOSP);
    __sqoop$field_map.put("T_TIMG_DATE_SOST", this.T_TIMG_DATE_SOST);
    __sqoop$field_map.put("T_TIMG_ACC_MIS", this.T_TIMG_ACC_MIS);
    __sqoop$field_map.put("T_TIMG_PRESENZA_CMOR", this.T_TIMG_PRESENZA_CMOR);
    __sqoop$field_map.put("D_TIVG_DATA_ATT_FDD", this.D_TIVG_DATA_ATT_FDD);
    __sqoop$field_map.put("D_TIVG_DATA_CHIUSURA_PDR", this.D_TIVG_DATA_CHIUSURA_PDR);
    __sqoop$field_map.put("B_TIMG_TIVG_CALCOLATO", this.B_TIMG_TIVG_CALCOLATO);
    __sqoop$field_map.put("B_ANN_IN_RITARDO", this.B_ANN_IN_RITARDO);
    __sqoop$field_map.put("T_CAUS_ANNULLAMENTO", this.T_CAUS_ANNULLAMENTO);
    __sqoop$field_map.put("T_MOTIV_ANNULLAMENTO", this.T_MOTIV_ANNULLAMENTO);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
