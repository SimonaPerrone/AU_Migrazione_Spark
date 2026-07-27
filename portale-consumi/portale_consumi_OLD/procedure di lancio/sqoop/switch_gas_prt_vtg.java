// ORM class for table 'switch_gas.prt_vtg'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Sep 14 08:00:17 CEST 2019
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

public class switch_gas_prt_vtg extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_VTG", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_VTG = (java.math.BigDecimal)value;
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
    setters.put("T_CODICE_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_PDR = (String)value;
      }
    });
    setters.put("T_CODICE_REMI", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_REMI = (String)value;
      }
    });
    setters.put("D_DATA_RICHIESTA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_RICHIESTA = (String)value;
      }
    });
    setters.put("D_DATA_DEC_RICHIESTA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_DEC_RICHIESTA = (String)value;
      }
    });
    setters.put("D_DATA_DEC", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_DEC = (String)value;
      }
    });
    setters.put("T_TITOLO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TITOLO = (String)value;
      }
    });
    setters.put("N_ID_NVG1", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_NVG1 = (java.math.BigDecimal)value;
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
    setters.put("T_MOTIVO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MOTIVO = (String)value;
      }
    });
    setters.put("N_ID_UTENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_UTENTE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_PIVA_UTENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PIVA_UTENTE = (String)value;
      }
    });
    setters.put("T_RUOLO_UTENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_RUOLO_UTENTE = (String)value;
      }
    });
    setters.put("T_CP_UTENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CP_UTENTE = (String)value;
      }
    });
    setters.put("N_ID_DISTR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_DISTR = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_CP_DISTR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CP_DISTR = (String)value;
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
    setters.put("N_ID_UDD_RCU_E", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_UDD_RCU_E = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_CC_E", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_CC_E = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_RUOLO_CC_E", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_RUOLO_CC_E = (String)value;
      }
    });
    setters.put("T_PIVA_CC", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PIVA_CC = (String)value;
      }
    });
    setters.put("N_ID_CLIENTE_FINALE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_CLIENTE_FINALE = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_FORNITURA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_FORNITURA = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_DISPACCIAMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_DISPACCIAMENTO = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_COD_CAUSALE_DISTR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_COD_CAUSALE_DISTR = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_TIPOLOGIA_VOLTURA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPOLOGIA_VOLTURA = (String)value;
      }
    });
    setters.put("T_TIPO_FORNITURA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO_FORNITURA = (String)value;
      }
    });
    setters.put("T_CF_COD_FISC", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CF_COD_FISC = (String)value;
      }
    });
    setters.put("T_CF_PIVA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CF_PIVA = (String)value;
      }
    });
    setters.put("B_CF_COD_FISC_ESTERO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_CF_COD_FISC_ESTERO = (String)value;
      }
    });
    setters.put("B_CF_PERSONA_FISICA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_CF_PERSONA_FISICA = (String)value;
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
    setters.put("N_MAX_ORE_CONG", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_MAX_ORE_CONG = (java.math.BigDecimal)value;
      }
    });
    setters.put("D_DATA_STATO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_STATO = (String)value;
      }
    });
  }
  public switch_gas_prt_vtg() {
    init0();
  }
  private java.math.BigDecimal N_ID_VTG;
  public java.math.BigDecimal get_N_ID_VTG() {
    return N_ID_VTG;
  }
  public void set_N_ID_VTG(java.math.BigDecimal N_ID_VTG) {
    this.N_ID_VTG = N_ID_VTG;
  }
  public switch_gas_prt_vtg with_N_ID_VTG(java.math.BigDecimal N_ID_VTG) {
    this.N_ID_VTG = N_ID_VTG;
    return this;
  }
  private java.math.BigDecimal N_ID_PRATICA;
  public java.math.BigDecimal get_N_ID_PRATICA() {
    return N_ID_PRATICA;
  }
  public void set_N_ID_PRATICA(java.math.BigDecimal N_ID_PRATICA) {
    this.N_ID_PRATICA = N_ID_PRATICA;
  }
  public switch_gas_prt_vtg with_N_ID_PRATICA(java.math.BigDecimal N_ID_PRATICA) {
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
  public switch_gas_prt_vtg with_T_STATO(String T_STATO) {
    this.T_STATO = T_STATO;
    return this;
  }
  private String T_CODICE_PDR;
  public String get_T_CODICE_PDR() {
    return T_CODICE_PDR;
  }
  public void set_T_CODICE_PDR(String T_CODICE_PDR) {
    this.T_CODICE_PDR = T_CODICE_PDR;
  }
  public switch_gas_prt_vtg with_T_CODICE_PDR(String T_CODICE_PDR) {
    this.T_CODICE_PDR = T_CODICE_PDR;
    return this;
  }
  private String T_CODICE_REMI;
  public String get_T_CODICE_REMI() {
    return T_CODICE_REMI;
  }
  public void set_T_CODICE_REMI(String T_CODICE_REMI) {
    this.T_CODICE_REMI = T_CODICE_REMI;
  }
  public switch_gas_prt_vtg with_T_CODICE_REMI(String T_CODICE_REMI) {
    this.T_CODICE_REMI = T_CODICE_REMI;
    return this;
  }
  private String D_DATA_RICHIESTA;
  public String get_D_DATA_RICHIESTA() {
    return D_DATA_RICHIESTA;
  }
  public void set_D_DATA_RICHIESTA(String D_DATA_RICHIESTA) {
    this.D_DATA_RICHIESTA = D_DATA_RICHIESTA;
  }
  public switch_gas_prt_vtg with_D_DATA_RICHIESTA(String D_DATA_RICHIESTA) {
    this.D_DATA_RICHIESTA = D_DATA_RICHIESTA;
    return this;
  }
  private String D_DATA_DEC_RICHIESTA;
  public String get_D_DATA_DEC_RICHIESTA() {
    return D_DATA_DEC_RICHIESTA;
  }
  public void set_D_DATA_DEC_RICHIESTA(String D_DATA_DEC_RICHIESTA) {
    this.D_DATA_DEC_RICHIESTA = D_DATA_DEC_RICHIESTA;
  }
  public switch_gas_prt_vtg with_D_DATA_DEC_RICHIESTA(String D_DATA_DEC_RICHIESTA) {
    this.D_DATA_DEC_RICHIESTA = D_DATA_DEC_RICHIESTA;
    return this;
  }
  private String D_DATA_DEC;
  public String get_D_DATA_DEC() {
    return D_DATA_DEC;
  }
  public void set_D_DATA_DEC(String D_DATA_DEC) {
    this.D_DATA_DEC = D_DATA_DEC;
  }
  public switch_gas_prt_vtg with_D_DATA_DEC(String D_DATA_DEC) {
    this.D_DATA_DEC = D_DATA_DEC;
    return this;
  }
  private String T_TITOLO;
  public String get_T_TITOLO() {
    return T_TITOLO;
  }
  public void set_T_TITOLO(String T_TITOLO) {
    this.T_TITOLO = T_TITOLO;
  }
  public switch_gas_prt_vtg with_T_TITOLO(String T_TITOLO) {
    this.T_TITOLO = T_TITOLO;
    return this;
  }
  private java.math.BigDecimal N_ID_NVG1;
  public java.math.BigDecimal get_N_ID_NVG1() {
    return N_ID_NVG1;
  }
  public void set_N_ID_NVG1(java.math.BigDecimal N_ID_NVG1) {
    this.N_ID_NVG1 = N_ID_NVG1;
  }
  public switch_gas_prt_vtg with_N_ID_NVG1(java.math.BigDecimal N_ID_NVG1) {
    this.N_ID_NVG1 = N_ID_NVG1;
    return this;
  }
  private String B_AMMISSIBILE;
  public String get_B_AMMISSIBILE() {
    return B_AMMISSIBILE;
  }
  public void set_B_AMMISSIBILE(String B_AMMISSIBILE) {
    this.B_AMMISSIBILE = B_AMMISSIBILE;
  }
  public switch_gas_prt_vtg with_B_AMMISSIBILE(String B_AMMISSIBILE) {
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
  public switch_gas_prt_vtg with_T_COD_CAUSALE(String T_COD_CAUSALE) {
    this.T_COD_CAUSALE = T_COD_CAUSALE;
    return this;
  }
  private String T_MOTIVO;
  public String get_T_MOTIVO() {
    return T_MOTIVO;
  }
  public void set_T_MOTIVO(String T_MOTIVO) {
    this.T_MOTIVO = T_MOTIVO;
  }
  public switch_gas_prt_vtg with_T_MOTIVO(String T_MOTIVO) {
    this.T_MOTIVO = T_MOTIVO;
    return this;
  }
  private java.math.BigDecimal N_ID_UTENTE;
  public java.math.BigDecimal get_N_ID_UTENTE() {
    return N_ID_UTENTE;
  }
  public void set_N_ID_UTENTE(java.math.BigDecimal N_ID_UTENTE) {
    this.N_ID_UTENTE = N_ID_UTENTE;
  }
  public switch_gas_prt_vtg with_N_ID_UTENTE(java.math.BigDecimal N_ID_UTENTE) {
    this.N_ID_UTENTE = N_ID_UTENTE;
    return this;
  }
  private String T_PIVA_UTENTE;
  public String get_T_PIVA_UTENTE() {
    return T_PIVA_UTENTE;
  }
  public void set_T_PIVA_UTENTE(String T_PIVA_UTENTE) {
    this.T_PIVA_UTENTE = T_PIVA_UTENTE;
  }
  public switch_gas_prt_vtg with_T_PIVA_UTENTE(String T_PIVA_UTENTE) {
    this.T_PIVA_UTENTE = T_PIVA_UTENTE;
    return this;
  }
  private String T_RUOLO_UTENTE;
  public String get_T_RUOLO_UTENTE() {
    return T_RUOLO_UTENTE;
  }
  public void set_T_RUOLO_UTENTE(String T_RUOLO_UTENTE) {
    this.T_RUOLO_UTENTE = T_RUOLO_UTENTE;
  }
  public switch_gas_prt_vtg with_T_RUOLO_UTENTE(String T_RUOLO_UTENTE) {
    this.T_RUOLO_UTENTE = T_RUOLO_UTENTE;
    return this;
  }
  private String T_CP_UTENTE;
  public String get_T_CP_UTENTE() {
    return T_CP_UTENTE;
  }
  public void set_T_CP_UTENTE(String T_CP_UTENTE) {
    this.T_CP_UTENTE = T_CP_UTENTE;
  }
  public switch_gas_prt_vtg with_T_CP_UTENTE(String T_CP_UTENTE) {
    this.T_CP_UTENTE = T_CP_UTENTE;
    return this;
  }
  private java.math.BigDecimal N_ID_DISTR;
  public java.math.BigDecimal get_N_ID_DISTR() {
    return N_ID_DISTR;
  }
  public void set_N_ID_DISTR(java.math.BigDecimal N_ID_DISTR) {
    this.N_ID_DISTR = N_ID_DISTR;
  }
  public switch_gas_prt_vtg with_N_ID_DISTR(java.math.BigDecimal N_ID_DISTR) {
    this.N_ID_DISTR = N_ID_DISTR;
    return this;
  }
  private String T_CP_DISTR;
  public String get_T_CP_DISTR() {
    return T_CP_DISTR;
  }
  public void set_T_CP_DISTR(String T_CP_DISTR) {
    this.T_CP_DISTR = T_CP_DISTR;
  }
  public switch_gas_prt_vtg with_T_CP_DISTR(String T_CP_DISTR) {
    this.T_CP_DISTR = T_CP_DISTR;
    return this;
  }
  private java.math.BigDecimal N_ID_UDD_U;
  public java.math.BigDecimal get_N_ID_UDD_U() {
    return N_ID_UDD_U;
  }
  public void set_N_ID_UDD_U(java.math.BigDecimal N_ID_UDD_U) {
    this.N_ID_UDD_U = N_ID_UDD_U;
  }
  public switch_gas_prt_vtg with_N_ID_UDD_U(java.math.BigDecimal N_ID_UDD_U) {
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
  public switch_gas_prt_vtg with_N_ID_CC_U(java.math.BigDecimal N_ID_CC_U) {
    this.N_ID_CC_U = N_ID_CC_U;
    return this;
  }
  private java.math.BigDecimal N_ID_UDD_RCU_E;
  public java.math.BigDecimal get_N_ID_UDD_RCU_E() {
    return N_ID_UDD_RCU_E;
  }
  public void set_N_ID_UDD_RCU_E(java.math.BigDecimal N_ID_UDD_RCU_E) {
    this.N_ID_UDD_RCU_E = N_ID_UDD_RCU_E;
  }
  public switch_gas_prt_vtg with_N_ID_UDD_RCU_E(java.math.BigDecimal N_ID_UDD_RCU_E) {
    this.N_ID_UDD_RCU_E = N_ID_UDD_RCU_E;
    return this;
  }
  private java.math.BigDecimal N_ID_CC_E;
  public java.math.BigDecimal get_N_ID_CC_E() {
    return N_ID_CC_E;
  }
  public void set_N_ID_CC_E(java.math.BigDecimal N_ID_CC_E) {
    this.N_ID_CC_E = N_ID_CC_E;
  }
  public switch_gas_prt_vtg with_N_ID_CC_E(java.math.BigDecimal N_ID_CC_E) {
    this.N_ID_CC_E = N_ID_CC_E;
    return this;
  }
  private String T_RUOLO_CC_E;
  public String get_T_RUOLO_CC_E() {
    return T_RUOLO_CC_E;
  }
  public void set_T_RUOLO_CC_E(String T_RUOLO_CC_E) {
    this.T_RUOLO_CC_E = T_RUOLO_CC_E;
  }
  public switch_gas_prt_vtg with_T_RUOLO_CC_E(String T_RUOLO_CC_E) {
    this.T_RUOLO_CC_E = T_RUOLO_CC_E;
    return this;
  }
  private String T_PIVA_CC;
  public String get_T_PIVA_CC() {
    return T_PIVA_CC;
  }
  public void set_T_PIVA_CC(String T_PIVA_CC) {
    this.T_PIVA_CC = T_PIVA_CC;
  }
  public switch_gas_prt_vtg with_T_PIVA_CC(String T_PIVA_CC) {
    this.T_PIVA_CC = T_PIVA_CC;
    return this;
  }
  private java.math.BigDecimal N_ID_CLIENTE_FINALE;
  public java.math.BigDecimal get_N_ID_CLIENTE_FINALE() {
    return N_ID_CLIENTE_FINALE;
  }
  public void set_N_ID_CLIENTE_FINALE(java.math.BigDecimal N_ID_CLIENTE_FINALE) {
    this.N_ID_CLIENTE_FINALE = N_ID_CLIENTE_FINALE;
  }
  public switch_gas_prt_vtg with_N_ID_CLIENTE_FINALE(java.math.BigDecimal N_ID_CLIENTE_FINALE) {
    this.N_ID_CLIENTE_FINALE = N_ID_CLIENTE_FINALE;
    return this;
  }
  private java.math.BigDecimal N_ID_FORNITURA;
  public java.math.BigDecimal get_N_ID_FORNITURA() {
    return N_ID_FORNITURA;
  }
  public void set_N_ID_FORNITURA(java.math.BigDecimal N_ID_FORNITURA) {
    this.N_ID_FORNITURA = N_ID_FORNITURA;
  }
  public switch_gas_prt_vtg with_N_ID_FORNITURA(java.math.BigDecimal N_ID_FORNITURA) {
    this.N_ID_FORNITURA = N_ID_FORNITURA;
    return this;
  }
  private java.math.BigDecimal N_ID_DISPACCIAMENTO;
  public java.math.BigDecimal get_N_ID_DISPACCIAMENTO() {
    return N_ID_DISPACCIAMENTO;
  }
  public void set_N_ID_DISPACCIAMENTO(java.math.BigDecimal N_ID_DISPACCIAMENTO) {
    this.N_ID_DISPACCIAMENTO = N_ID_DISPACCIAMENTO;
  }
  public switch_gas_prt_vtg with_N_ID_DISPACCIAMENTO(java.math.BigDecimal N_ID_DISPACCIAMENTO) {
    this.N_ID_DISPACCIAMENTO = N_ID_DISPACCIAMENTO;
    return this;
  }
  private java.math.BigDecimal N_ID_COD_CAUSALE_DISTR;
  public java.math.BigDecimal get_N_ID_COD_CAUSALE_DISTR() {
    return N_ID_COD_CAUSALE_DISTR;
  }
  public void set_N_ID_COD_CAUSALE_DISTR(java.math.BigDecimal N_ID_COD_CAUSALE_DISTR) {
    this.N_ID_COD_CAUSALE_DISTR = N_ID_COD_CAUSALE_DISTR;
  }
  public switch_gas_prt_vtg with_N_ID_COD_CAUSALE_DISTR(java.math.BigDecimal N_ID_COD_CAUSALE_DISTR) {
    this.N_ID_COD_CAUSALE_DISTR = N_ID_COD_CAUSALE_DISTR;
    return this;
  }
  private String T_TIPOLOGIA_VOLTURA;
  public String get_T_TIPOLOGIA_VOLTURA() {
    return T_TIPOLOGIA_VOLTURA;
  }
  public void set_T_TIPOLOGIA_VOLTURA(String T_TIPOLOGIA_VOLTURA) {
    this.T_TIPOLOGIA_VOLTURA = T_TIPOLOGIA_VOLTURA;
  }
  public switch_gas_prt_vtg with_T_TIPOLOGIA_VOLTURA(String T_TIPOLOGIA_VOLTURA) {
    this.T_TIPOLOGIA_VOLTURA = T_TIPOLOGIA_VOLTURA;
    return this;
  }
  private String T_TIPO_FORNITURA;
  public String get_T_TIPO_FORNITURA() {
    return T_TIPO_FORNITURA;
  }
  public void set_T_TIPO_FORNITURA(String T_TIPO_FORNITURA) {
    this.T_TIPO_FORNITURA = T_TIPO_FORNITURA;
  }
  public switch_gas_prt_vtg with_T_TIPO_FORNITURA(String T_TIPO_FORNITURA) {
    this.T_TIPO_FORNITURA = T_TIPO_FORNITURA;
    return this;
  }
  private String T_CF_COD_FISC;
  public String get_T_CF_COD_FISC() {
    return T_CF_COD_FISC;
  }
  public void set_T_CF_COD_FISC(String T_CF_COD_FISC) {
    this.T_CF_COD_FISC = T_CF_COD_FISC;
  }
  public switch_gas_prt_vtg with_T_CF_COD_FISC(String T_CF_COD_FISC) {
    this.T_CF_COD_FISC = T_CF_COD_FISC;
    return this;
  }
  private String T_CF_PIVA;
  public String get_T_CF_PIVA() {
    return T_CF_PIVA;
  }
  public void set_T_CF_PIVA(String T_CF_PIVA) {
    this.T_CF_PIVA = T_CF_PIVA;
  }
  public switch_gas_prt_vtg with_T_CF_PIVA(String T_CF_PIVA) {
    this.T_CF_PIVA = T_CF_PIVA;
    return this;
  }
  private String B_CF_COD_FISC_ESTERO;
  public String get_B_CF_COD_FISC_ESTERO() {
    return B_CF_COD_FISC_ESTERO;
  }
  public void set_B_CF_COD_FISC_ESTERO(String B_CF_COD_FISC_ESTERO) {
    this.B_CF_COD_FISC_ESTERO = B_CF_COD_FISC_ESTERO;
  }
  public switch_gas_prt_vtg with_B_CF_COD_FISC_ESTERO(String B_CF_COD_FISC_ESTERO) {
    this.B_CF_COD_FISC_ESTERO = B_CF_COD_FISC_ESTERO;
    return this;
  }
  private String B_CF_PERSONA_FISICA;
  public String get_B_CF_PERSONA_FISICA() {
    return B_CF_PERSONA_FISICA;
  }
  public void set_B_CF_PERSONA_FISICA(String B_CF_PERSONA_FISICA) {
    this.B_CF_PERSONA_FISICA = B_CF_PERSONA_FISICA;
  }
  public switch_gas_prt_vtg with_B_CF_PERSONA_FISICA(String B_CF_PERSONA_FISICA) {
    this.B_CF_PERSONA_FISICA = B_CF_PERSONA_FISICA;
    return this;
  }
  private String T_ESITO;
  public String get_T_ESITO() {
    return T_ESITO;
  }
  public void set_T_ESITO(String T_ESITO) {
    this.T_ESITO = T_ESITO;
  }
  public switch_gas_prt_vtg with_T_ESITO(String T_ESITO) {
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
  public switch_gas_prt_vtg with_T_COD_ESITO(String T_COD_ESITO) {
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
  public switch_gas_prt_vtg with_T_DETT_ESITO(String T_DETT_ESITO) {
    this.T_DETT_ESITO = T_DETT_ESITO;
    return this;
  }
  private java.math.BigDecimal N_MAX_ORE_CONG;
  public java.math.BigDecimal get_N_MAX_ORE_CONG() {
    return N_MAX_ORE_CONG;
  }
  public void set_N_MAX_ORE_CONG(java.math.BigDecimal N_MAX_ORE_CONG) {
    this.N_MAX_ORE_CONG = N_MAX_ORE_CONG;
  }
  public switch_gas_prt_vtg with_N_MAX_ORE_CONG(java.math.BigDecimal N_MAX_ORE_CONG) {
    this.N_MAX_ORE_CONG = N_MAX_ORE_CONG;
    return this;
  }
  private String D_DATA_STATO;
  public String get_D_DATA_STATO() {
    return D_DATA_STATO;
  }
  public void set_D_DATA_STATO(String D_DATA_STATO) {
    this.D_DATA_STATO = D_DATA_STATO;
  }
  public switch_gas_prt_vtg with_D_DATA_STATO(String D_DATA_STATO) {
    this.D_DATA_STATO = D_DATA_STATO;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof switch_gas_prt_vtg)) {
      return false;
    }
    switch_gas_prt_vtg that = (switch_gas_prt_vtg) o;
    boolean equal = true;
    equal = equal && (this.N_ID_VTG == null ? that.N_ID_VTG == null : this.N_ID_VTG.equals(that.N_ID_VTG));
    equal = equal && (this.N_ID_PRATICA == null ? that.N_ID_PRATICA == null : this.N_ID_PRATICA.equals(that.N_ID_PRATICA));
    equal = equal && (this.T_STATO == null ? that.T_STATO == null : this.T_STATO.equals(that.T_STATO));
    equal = equal && (this.T_CODICE_PDR == null ? that.T_CODICE_PDR == null : this.T_CODICE_PDR.equals(that.T_CODICE_PDR));
    equal = equal && (this.T_CODICE_REMI == null ? that.T_CODICE_REMI == null : this.T_CODICE_REMI.equals(that.T_CODICE_REMI));
    equal = equal && (this.D_DATA_RICHIESTA == null ? that.D_DATA_RICHIESTA == null : this.D_DATA_RICHIESTA.equals(that.D_DATA_RICHIESTA));
    equal = equal && (this.D_DATA_DEC_RICHIESTA == null ? that.D_DATA_DEC_RICHIESTA == null : this.D_DATA_DEC_RICHIESTA.equals(that.D_DATA_DEC_RICHIESTA));
    equal = equal && (this.D_DATA_DEC == null ? that.D_DATA_DEC == null : this.D_DATA_DEC.equals(that.D_DATA_DEC));
    equal = equal && (this.T_TITOLO == null ? that.T_TITOLO == null : this.T_TITOLO.equals(that.T_TITOLO));
    equal = equal && (this.N_ID_NVG1 == null ? that.N_ID_NVG1 == null : this.N_ID_NVG1.equals(that.N_ID_NVG1));
    equal = equal && (this.B_AMMISSIBILE == null ? that.B_AMMISSIBILE == null : this.B_AMMISSIBILE.equals(that.B_AMMISSIBILE));
    equal = equal && (this.T_COD_CAUSALE == null ? that.T_COD_CAUSALE == null : this.T_COD_CAUSALE.equals(that.T_COD_CAUSALE));
    equal = equal && (this.T_MOTIVO == null ? that.T_MOTIVO == null : this.T_MOTIVO.equals(that.T_MOTIVO));
    equal = equal && (this.N_ID_UTENTE == null ? that.N_ID_UTENTE == null : this.N_ID_UTENTE.equals(that.N_ID_UTENTE));
    equal = equal && (this.T_PIVA_UTENTE == null ? that.T_PIVA_UTENTE == null : this.T_PIVA_UTENTE.equals(that.T_PIVA_UTENTE));
    equal = equal && (this.T_RUOLO_UTENTE == null ? that.T_RUOLO_UTENTE == null : this.T_RUOLO_UTENTE.equals(that.T_RUOLO_UTENTE));
    equal = equal && (this.T_CP_UTENTE == null ? that.T_CP_UTENTE == null : this.T_CP_UTENTE.equals(that.T_CP_UTENTE));
    equal = equal && (this.N_ID_DISTR == null ? that.N_ID_DISTR == null : this.N_ID_DISTR.equals(that.N_ID_DISTR));
    equal = equal && (this.T_CP_DISTR == null ? that.T_CP_DISTR == null : this.T_CP_DISTR.equals(that.T_CP_DISTR));
    equal = equal && (this.N_ID_UDD_U == null ? that.N_ID_UDD_U == null : this.N_ID_UDD_U.equals(that.N_ID_UDD_U));
    equal = equal && (this.N_ID_CC_U == null ? that.N_ID_CC_U == null : this.N_ID_CC_U.equals(that.N_ID_CC_U));
    equal = equal && (this.N_ID_UDD_RCU_E == null ? that.N_ID_UDD_RCU_E == null : this.N_ID_UDD_RCU_E.equals(that.N_ID_UDD_RCU_E));
    equal = equal && (this.N_ID_CC_E == null ? that.N_ID_CC_E == null : this.N_ID_CC_E.equals(that.N_ID_CC_E));
    equal = equal && (this.T_RUOLO_CC_E == null ? that.T_RUOLO_CC_E == null : this.T_RUOLO_CC_E.equals(that.T_RUOLO_CC_E));
    equal = equal && (this.T_PIVA_CC == null ? that.T_PIVA_CC == null : this.T_PIVA_CC.equals(that.T_PIVA_CC));
    equal = equal && (this.N_ID_CLIENTE_FINALE == null ? that.N_ID_CLIENTE_FINALE == null : this.N_ID_CLIENTE_FINALE.equals(that.N_ID_CLIENTE_FINALE));
    equal = equal && (this.N_ID_FORNITURA == null ? that.N_ID_FORNITURA == null : this.N_ID_FORNITURA.equals(that.N_ID_FORNITURA));
    equal = equal && (this.N_ID_DISPACCIAMENTO == null ? that.N_ID_DISPACCIAMENTO == null : this.N_ID_DISPACCIAMENTO.equals(that.N_ID_DISPACCIAMENTO));
    equal = equal && (this.N_ID_COD_CAUSALE_DISTR == null ? that.N_ID_COD_CAUSALE_DISTR == null : this.N_ID_COD_CAUSALE_DISTR.equals(that.N_ID_COD_CAUSALE_DISTR));
    equal = equal && (this.T_TIPOLOGIA_VOLTURA == null ? that.T_TIPOLOGIA_VOLTURA == null : this.T_TIPOLOGIA_VOLTURA.equals(that.T_TIPOLOGIA_VOLTURA));
    equal = equal && (this.T_TIPO_FORNITURA == null ? that.T_TIPO_FORNITURA == null : this.T_TIPO_FORNITURA.equals(that.T_TIPO_FORNITURA));
    equal = equal && (this.T_CF_COD_FISC == null ? that.T_CF_COD_FISC == null : this.T_CF_COD_FISC.equals(that.T_CF_COD_FISC));
    equal = equal && (this.T_CF_PIVA == null ? that.T_CF_PIVA == null : this.T_CF_PIVA.equals(that.T_CF_PIVA));
    equal = equal && (this.B_CF_COD_FISC_ESTERO == null ? that.B_CF_COD_FISC_ESTERO == null : this.B_CF_COD_FISC_ESTERO.equals(that.B_CF_COD_FISC_ESTERO));
    equal = equal && (this.B_CF_PERSONA_FISICA == null ? that.B_CF_PERSONA_FISICA == null : this.B_CF_PERSONA_FISICA.equals(that.B_CF_PERSONA_FISICA));
    equal = equal && (this.T_ESITO == null ? that.T_ESITO == null : this.T_ESITO.equals(that.T_ESITO));
    equal = equal && (this.T_COD_ESITO == null ? that.T_COD_ESITO == null : this.T_COD_ESITO.equals(that.T_COD_ESITO));
    equal = equal && (this.T_DETT_ESITO == null ? that.T_DETT_ESITO == null : this.T_DETT_ESITO.equals(that.T_DETT_ESITO));
    equal = equal && (this.N_MAX_ORE_CONG == null ? that.N_MAX_ORE_CONG == null : this.N_MAX_ORE_CONG.equals(that.N_MAX_ORE_CONG));
    equal = equal && (this.D_DATA_STATO == null ? that.D_DATA_STATO == null : this.D_DATA_STATO.equals(that.D_DATA_STATO));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof switch_gas_prt_vtg)) {
      return false;
    }
    switch_gas_prt_vtg that = (switch_gas_prt_vtg) o;
    boolean equal = true;
    equal = equal && (this.N_ID_VTG == null ? that.N_ID_VTG == null : this.N_ID_VTG.equals(that.N_ID_VTG));
    equal = equal && (this.N_ID_PRATICA == null ? that.N_ID_PRATICA == null : this.N_ID_PRATICA.equals(that.N_ID_PRATICA));
    equal = equal && (this.T_STATO == null ? that.T_STATO == null : this.T_STATO.equals(that.T_STATO));
    equal = equal && (this.T_CODICE_PDR == null ? that.T_CODICE_PDR == null : this.T_CODICE_PDR.equals(that.T_CODICE_PDR));
    equal = equal && (this.T_CODICE_REMI == null ? that.T_CODICE_REMI == null : this.T_CODICE_REMI.equals(that.T_CODICE_REMI));
    equal = equal && (this.D_DATA_RICHIESTA == null ? that.D_DATA_RICHIESTA == null : this.D_DATA_RICHIESTA.equals(that.D_DATA_RICHIESTA));
    equal = equal && (this.D_DATA_DEC_RICHIESTA == null ? that.D_DATA_DEC_RICHIESTA == null : this.D_DATA_DEC_RICHIESTA.equals(that.D_DATA_DEC_RICHIESTA));
    equal = equal && (this.D_DATA_DEC == null ? that.D_DATA_DEC == null : this.D_DATA_DEC.equals(that.D_DATA_DEC));
    equal = equal && (this.T_TITOLO == null ? that.T_TITOLO == null : this.T_TITOLO.equals(that.T_TITOLO));
    equal = equal && (this.N_ID_NVG1 == null ? that.N_ID_NVG1 == null : this.N_ID_NVG1.equals(that.N_ID_NVG1));
    equal = equal && (this.B_AMMISSIBILE == null ? that.B_AMMISSIBILE == null : this.B_AMMISSIBILE.equals(that.B_AMMISSIBILE));
    equal = equal && (this.T_COD_CAUSALE == null ? that.T_COD_CAUSALE == null : this.T_COD_CAUSALE.equals(that.T_COD_CAUSALE));
    equal = equal && (this.T_MOTIVO == null ? that.T_MOTIVO == null : this.T_MOTIVO.equals(that.T_MOTIVO));
    equal = equal && (this.N_ID_UTENTE == null ? that.N_ID_UTENTE == null : this.N_ID_UTENTE.equals(that.N_ID_UTENTE));
    equal = equal && (this.T_PIVA_UTENTE == null ? that.T_PIVA_UTENTE == null : this.T_PIVA_UTENTE.equals(that.T_PIVA_UTENTE));
    equal = equal && (this.T_RUOLO_UTENTE == null ? that.T_RUOLO_UTENTE == null : this.T_RUOLO_UTENTE.equals(that.T_RUOLO_UTENTE));
    equal = equal && (this.T_CP_UTENTE == null ? that.T_CP_UTENTE == null : this.T_CP_UTENTE.equals(that.T_CP_UTENTE));
    equal = equal && (this.N_ID_DISTR == null ? that.N_ID_DISTR == null : this.N_ID_DISTR.equals(that.N_ID_DISTR));
    equal = equal && (this.T_CP_DISTR == null ? that.T_CP_DISTR == null : this.T_CP_DISTR.equals(that.T_CP_DISTR));
    equal = equal && (this.N_ID_UDD_U == null ? that.N_ID_UDD_U == null : this.N_ID_UDD_U.equals(that.N_ID_UDD_U));
    equal = equal && (this.N_ID_CC_U == null ? that.N_ID_CC_U == null : this.N_ID_CC_U.equals(that.N_ID_CC_U));
    equal = equal && (this.N_ID_UDD_RCU_E == null ? that.N_ID_UDD_RCU_E == null : this.N_ID_UDD_RCU_E.equals(that.N_ID_UDD_RCU_E));
    equal = equal && (this.N_ID_CC_E == null ? that.N_ID_CC_E == null : this.N_ID_CC_E.equals(that.N_ID_CC_E));
    equal = equal && (this.T_RUOLO_CC_E == null ? that.T_RUOLO_CC_E == null : this.T_RUOLO_CC_E.equals(that.T_RUOLO_CC_E));
    equal = equal && (this.T_PIVA_CC == null ? that.T_PIVA_CC == null : this.T_PIVA_CC.equals(that.T_PIVA_CC));
    equal = equal && (this.N_ID_CLIENTE_FINALE == null ? that.N_ID_CLIENTE_FINALE == null : this.N_ID_CLIENTE_FINALE.equals(that.N_ID_CLIENTE_FINALE));
    equal = equal && (this.N_ID_FORNITURA == null ? that.N_ID_FORNITURA == null : this.N_ID_FORNITURA.equals(that.N_ID_FORNITURA));
    equal = equal && (this.N_ID_DISPACCIAMENTO == null ? that.N_ID_DISPACCIAMENTO == null : this.N_ID_DISPACCIAMENTO.equals(that.N_ID_DISPACCIAMENTO));
    equal = equal && (this.N_ID_COD_CAUSALE_DISTR == null ? that.N_ID_COD_CAUSALE_DISTR == null : this.N_ID_COD_CAUSALE_DISTR.equals(that.N_ID_COD_CAUSALE_DISTR));
    equal = equal && (this.T_TIPOLOGIA_VOLTURA == null ? that.T_TIPOLOGIA_VOLTURA == null : this.T_TIPOLOGIA_VOLTURA.equals(that.T_TIPOLOGIA_VOLTURA));
    equal = equal && (this.T_TIPO_FORNITURA == null ? that.T_TIPO_FORNITURA == null : this.T_TIPO_FORNITURA.equals(that.T_TIPO_FORNITURA));
    equal = equal && (this.T_CF_COD_FISC == null ? that.T_CF_COD_FISC == null : this.T_CF_COD_FISC.equals(that.T_CF_COD_FISC));
    equal = equal && (this.T_CF_PIVA == null ? that.T_CF_PIVA == null : this.T_CF_PIVA.equals(that.T_CF_PIVA));
    equal = equal && (this.B_CF_COD_FISC_ESTERO == null ? that.B_CF_COD_FISC_ESTERO == null : this.B_CF_COD_FISC_ESTERO.equals(that.B_CF_COD_FISC_ESTERO));
    equal = equal && (this.B_CF_PERSONA_FISICA == null ? that.B_CF_PERSONA_FISICA == null : this.B_CF_PERSONA_FISICA.equals(that.B_CF_PERSONA_FISICA));
    equal = equal && (this.T_ESITO == null ? that.T_ESITO == null : this.T_ESITO.equals(that.T_ESITO));
    equal = equal && (this.T_COD_ESITO == null ? that.T_COD_ESITO == null : this.T_COD_ESITO.equals(that.T_COD_ESITO));
    equal = equal && (this.T_DETT_ESITO == null ? that.T_DETT_ESITO == null : this.T_DETT_ESITO.equals(that.T_DETT_ESITO));
    equal = equal && (this.N_MAX_ORE_CONG == null ? that.N_MAX_ORE_CONG == null : this.N_MAX_ORE_CONG.equals(that.N_MAX_ORE_CONG));
    equal = equal && (this.D_DATA_STATO == null ? that.D_DATA_STATO == null : this.D_DATA_STATO.equals(that.D_DATA_STATO));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_VTG = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_PRATICA = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_STATO = JdbcWritableBridge.readString(3, __dbResults);
    this.T_CODICE_PDR = JdbcWritableBridge.readString(4, __dbResults);
    this.T_CODICE_REMI = JdbcWritableBridge.readString(5, __dbResults);
    this.D_DATA_RICHIESTA = JdbcWritableBridge.readString(6, __dbResults);
    this.D_DATA_DEC_RICHIESTA = JdbcWritableBridge.readString(7, __dbResults);
    this.D_DATA_DEC = JdbcWritableBridge.readString(8, __dbResults);
    this.T_TITOLO = JdbcWritableBridge.readString(9, __dbResults);
    this.N_ID_NVG1 = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.B_AMMISSIBILE = JdbcWritableBridge.readString(11, __dbResults);
    this.T_COD_CAUSALE = JdbcWritableBridge.readString(12, __dbResults);
    this.T_MOTIVO = JdbcWritableBridge.readString(13, __dbResults);
    this.N_ID_UTENTE = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.T_PIVA_UTENTE = JdbcWritableBridge.readString(15, __dbResults);
    this.T_RUOLO_UTENTE = JdbcWritableBridge.readString(16, __dbResults);
    this.T_CP_UTENTE = JdbcWritableBridge.readString(17, __dbResults);
    this.N_ID_DISTR = JdbcWritableBridge.readBigDecimal(18, __dbResults);
    this.T_CP_DISTR = JdbcWritableBridge.readString(19, __dbResults);
    this.N_ID_UDD_U = JdbcWritableBridge.readBigDecimal(20, __dbResults);
    this.N_ID_CC_U = JdbcWritableBridge.readBigDecimal(21, __dbResults);
    this.N_ID_UDD_RCU_E = JdbcWritableBridge.readBigDecimal(22, __dbResults);
    this.N_ID_CC_E = JdbcWritableBridge.readBigDecimal(23, __dbResults);
    this.T_RUOLO_CC_E = JdbcWritableBridge.readString(24, __dbResults);
    this.T_PIVA_CC = JdbcWritableBridge.readString(25, __dbResults);
    this.N_ID_CLIENTE_FINALE = JdbcWritableBridge.readBigDecimal(26, __dbResults);
    this.N_ID_FORNITURA = JdbcWritableBridge.readBigDecimal(27, __dbResults);
    this.N_ID_DISPACCIAMENTO = JdbcWritableBridge.readBigDecimal(28, __dbResults);
    this.N_ID_COD_CAUSALE_DISTR = JdbcWritableBridge.readBigDecimal(29, __dbResults);
    this.T_TIPOLOGIA_VOLTURA = JdbcWritableBridge.readString(30, __dbResults);
    this.T_TIPO_FORNITURA = JdbcWritableBridge.readString(31, __dbResults);
    this.T_CF_COD_FISC = JdbcWritableBridge.readString(32, __dbResults);
    this.T_CF_PIVA = JdbcWritableBridge.readString(33, __dbResults);
    this.B_CF_COD_FISC_ESTERO = JdbcWritableBridge.readString(34, __dbResults);
    this.B_CF_PERSONA_FISICA = JdbcWritableBridge.readString(35, __dbResults);
    this.T_ESITO = JdbcWritableBridge.readString(36, __dbResults);
    this.T_COD_ESITO = JdbcWritableBridge.readString(37, __dbResults);
    this.T_DETT_ESITO = JdbcWritableBridge.readString(38, __dbResults);
    this.N_MAX_ORE_CONG = JdbcWritableBridge.readBigDecimal(39, __dbResults);
    this.D_DATA_STATO = JdbcWritableBridge.readString(40, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_VTG = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_PRATICA = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_STATO = JdbcWritableBridge.readString(3, __dbResults);
    this.T_CODICE_PDR = JdbcWritableBridge.readString(4, __dbResults);
    this.T_CODICE_REMI = JdbcWritableBridge.readString(5, __dbResults);
    this.D_DATA_RICHIESTA = JdbcWritableBridge.readString(6, __dbResults);
    this.D_DATA_DEC_RICHIESTA = JdbcWritableBridge.readString(7, __dbResults);
    this.D_DATA_DEC = JdbcWritableBridge.readString(8, __dbResults);
    this.T_TITOLO = JdbcWritableBridge.readString(9, __dbResults);
    this.N_ID_NVG1 = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.B_AMMISSIBILE = JdbcWritableBridge.readString(11, __dbResults);
    this.T_COD_CAUSALE = JdbcWritableBridge.readString(12, __dbResults);
    this.T_MOTIVO = JdbcWritableBridge.readString(13, __dbResults);
    this.N_ID_UTENTE = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.T_PIVA_UTENTE = JdbcWritableBridge.readString(15, __dbResults);
    this.T_RUOLO_UTENTE = JdbcWritableBridge.readString(16, __dbResults);
    this.T_CP_UTENTE = JdbcWritableBridge.readString(17, __dbResults);
    this.N_ID_DISTR = JdbcWritableBridge.readBigDecimal(18, __dbResults);
    this.T_CP_DISTR = JdbcWritableBridge.readString(19, __dbResults);
    this.N_ID_UDD_U = JdbcWritableBridge.readBigDecimal(20, __dbResults);
    this.N_ID_CC_U = JdbcWritableBridge.readBigDecimal(21, __dbResults);
    this.N_ID_UDD_RCU_E = JdbcWritableBridge.readBigDecimal(22, __dbResults);
    this.N_ID_CC_E = JdbcWritableBridge.readBigDecimal(23, __dbResults);
    this.T_RUOLO_CC_E = JdbcWritableBridge.readString(24, __dbResults);
    this.T_PIVA_CC = JdbcWritableBridge.readString(25, __dbResults);
    this.N_ID_CLIENTE_FINALE = JdbcWritableBridge.readBigDecimal(26, __dbResults);
    this.N_ID_FORNITURA = JdbcWritableBridge.readBigDecimal(27, __dbResults);
    this.N_ID_DISPACCIAMENTO = JdbcWritableBridge.readBigDecimal(28, __dbResults);
    this.N_ID_COD_CAUSALE_DISTR = JdbcWritableBridge.readBigDecimal(29, __dbResults);
    this.T_TIPOLOGIA_VOLTURA = JdbcWritableBridge.readString(30, __dbResults);
    this.T_TIPO_FORNITURA = JdbcWritableBridge.readString(31, __dbResults);
    this.T_CF_COD_FISC = JdbcWritableBridge.readString(32, __dbResults);
    this.T_CF_PIVA = JdbcWritableBridge.readString(33, __dbResults);
    this.B_CF_COD_FISC_ESTERO = JdbcWritableBridge.readString(34, __dbResults);
    this.B_CF_PERSONA_FISICA = JdbcWritableBridge.readString(35, __dbResults);
    this.T_ESITO = JdbcWritableBridge.readString(36, __dbResults);
    this.T_COD_ESITO = JdbcWritableBridge.readString(37, __dbResults);
    this.T_DETT_ESITO = JdbcWritableBridge.readString(38, __dbResults);
    this.N_MAX_ORE_CONG = JdbcWritableBridge.readBigDecimal(39, __dbResults);
    this.D_DATA_STATO = JdbcWritableBridge.readString(40, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_VTG, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PRATICA, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_PDR, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_REMI, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RICHIESTA, 6 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_DEC_RICHIESTA, 7 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_DEC, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_TITOLO, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_NVG1, 10 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_AMMISSIBILE, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CAUSALE, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MOTIVO, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UTENTE, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA_UTENTE, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_RUOLO_UTENTE, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CP_UTENTE, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTR, 18 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CP_DISTR, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UDD_U, 20 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CC_U, 21 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UDD_RCU_E, 22 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CC_E, 23 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_RUOLO_CC_E, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA_CC, 25 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CLIENTE_FINALE, 26 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_FORNITURA, 27 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DISPACCIAMENTO, 28 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_COD_CAUSALE_DISTR, 29 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPOLOGIA_VOLTURA, 30 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_FORNITURA, 31 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CF_COD_FISC, 32 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CF_PIVA, 33 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_CF_COD_FISC_ESTERO, 34 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_CF_PERSONA_FISICA, 35 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ESITO, 36 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_ESITO, 37 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DETT_ESITO, 38 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_MAX_ORE_CONG, 39 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_STATO, 40 + __off, 93, __dbStmt);
    return 40;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_VTG, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PRATICA, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_PDR, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_REMI, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RICHIESTA, 6 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_DEC_RICHIESTA, 7 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_DEC, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_TITOLO, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_NVG1, 10 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_AMMISSIBILE, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CAUSALE, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MOTIVO, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UTENTE, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA_UTENTE, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_RUOLO_UTENTE, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CP_UTENTE, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTR, 18 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CP_DISTR, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UDD_U, 20 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CC_U, 21 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UDD_RCU_E, 22 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CC_E, 23 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_RUOLO_CC_E, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA_CC, 25 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CLIENTE_FINALE, 26 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_FORNITURA, 27 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DISPACCIAMENTO, 28 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_COD_CAUSALE_DISTR, 29 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPOLOGIA_VOLTURA, 30 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_FORNITURA, 31 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CF_COD_FISC, 32 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CF_PIVA, 33 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_CF_COD_FISC_ESTERO, 34 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_CF_PERSONA_FISICA, 35 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ESITO, 36 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_ESITO, 37 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DETT_ESITO, 38 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_MAX_ORE_CONG, 39 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_STATO, 40 + __off, 93, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_VTG = null;
    } else {
    this.N_ID_VTG = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
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
        this.T_CODICE_PDR = null;
    } else {
    this.T_CODICE_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_REMI = null;
    } else {
    this.T_CODICE_REMI = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_RICHIESTA = null;
    } else {
    this.D_DATA_RICHIESTA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_DEC_RICHIESTA = null;
    } else {
    this.D_DATA_DEC_RICHIESTA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_DEC = null;
    } else {
    this.D_DATA_DEC = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TITOLO = null;
    } else {
    this.T_TITOLO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_NVG1 = null;
    } else {
    this.N_ID_NVG1 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
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
        this.T_MOTIVO = null;
    } else {
    this.T_MOTIVO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_UTENTE = null;
    } else {
    this.N_ID_UTENTE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PIVA_UTENTE = null;
    } else {
    this.T_PIVA_UTENTE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_RUOLO_UTENTE = null;
    } else {
    this.T_RUOLO_UTENTE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CP_UTENTE = null;
    } else {
    this.T_CP_UTENTE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_DISTR = null;
    } else {
    this.N_ID_DISTR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CP_DISTR = null;
    } else {
    this.T_CP_DISTR = Text.readString(__dataIn);
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
        this.N_ID_UDD_RCU_E = null;
    } else {
    this.N_ID_UDD_RCU_E = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_CC_E = null;
    } else {
    this.N_ID_CC_E = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_RUOLO_CC_E = null;
    } else {
    this.T_RUOLO_CC_E = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PIVA_CC = null;
    } else {
    this.T_PIVA_CC = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_CLIENTE_FINALE = null;
    } else {
    this.N_ID_CLIENTE_FINALE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_FORNITURA = null;
    } else {
    this.N_ID_FORNITURA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_DISPACCIAMENTO = null;
    } else {
    this.N_ID_DISPACCIAMENTO = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_COD_CAUSALE_DISTR = null;
    } else {
    this.N_ID_COD_CAUSALE_DISTR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPOLOGIA_VOLTURA = null;
    } else {
    this.T_TIPOLOGIA_VOLTURA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO_FORNITURA = null;
    } else {
    this.T_TIPO_FORNITURA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CF_COD_FISC = null;
    } else {
    this.T_CF_COD_FISC = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CF_PIVA = null;
    } else {
    this.T_CF_PIVA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_CF_COD_FISC_ESTERO = null;
    } else {
    this.B_CF_COD_FISC_ESTERO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_CF_PERSONA_FISICA = null;
    } else {
    this.B_CF_PERSONA_FISICA = Text.readString(__dataIn);
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
        this.N_MAX_ORE_CONG = null;
    } else {
    this.N_MAX_ORE_CONG = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_STATO = null;
    } else {
    this.D_DATA_STATO = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_VTG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_VTG, __dataOut);
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
    if (null == this.T_CODICE_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_PDR);
    }
    if (null == this.T_CODICE_REMI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_REMI);
    }
    if (null == this.D_DATA_RICHIESTA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RICHIESTA);
    }
    if (null == this.D_DATA_DEC_RICHIESTA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_DEC_RICHIESTA);
    }
    if (null == this.D_DATA_DEC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_DEC);
    }
    if (null == this.T_TITOLO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TITOLO);
    }
    if (null == this.N_ID_NVG1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_NVG1, __dataOut);
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
    if (null == this.T_MOTIVO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MOTIVO);
    }
    if (null == this.N_ID_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UTENTE, __dataOut);
    }
    if (null == this.T_PIVA_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA_UTENTE);
    }
    if (null == this.T_RUOLO_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RUOLO_UTENTE);
    }
    if (null == this.T_CP_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CP_UTENTE);
    }
    if (null == this.N_ID_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISTR, __dataOut);
    }
    if (null == this.T_CP_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CP_DISTR);
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
    if (null == this.N_ID_UDD_RCU_E) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UDD_RCU_E, __dataOut);
    }
    if (null == this.N_ID_CC_E) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CC_E, __dataOut);
    }
    if (null == this.T_RUOLO_CC_E) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RUOLO_CC_E);
    }
    if (null == this.T_PIVA_CC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA_CC);
    }
    if (null == this.N_ID_CLIENTE_FINALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CLIENTE_FINALE, __dataOut);
    }
    if (null == this.N_ID_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FORNITURA, __dataOut);
    }
    if (null == this.N_ID_DISPACCIAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISPACCIAMENTO, __dataOut);
    }
    if (null == this.N_ID_COD_CAUSALE_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_COD_CAUSALE_DISTR, __dataOut);
    }
    if (null == this.T_TIPOLOGIA_VOLTURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPOLOGIA_VOLTURA);
    }
    if (null == this.T_TIPO_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_FORNITURA);
    }
    if (null == this.T_CF_COD_FISC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CF_COD_FISC);
    }
    if (null == this.T_CF_PIVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CF_PIVA);
    }
    if (null == this.B_CF_COD_FISC_ESTERO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_CF_COD_FISC_ESTERO);
    }
    if (null == this.B_CF_PERSONA_FISICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_CF_PERSONA_FISICA);
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
    if (null == this.N_MAX_ORE_CONG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_MAX_ORE_CONG, __dataOut);
    }
    if (null == this.D_DATA_STATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_STATO);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_VTG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_VTG, __dataOut);
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
    if (null == this.T_CODICE_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_PDR);
    }
    if (null == this.T_CODICE_REMI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_REMI);
    }
    if (null == this.D_DATA_RICHIESTA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RICHIESTA);
    }
    if (null == this.D_DATA_DEC_RICHIESTA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_DEC_RICHIESTA);
    }
    if (null == this.D_DATA_DEC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_DEC);
    }
    if (null == this.T_TITOLO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TITOLO);
    }
    if (null == this.N_ID_NVG1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_NVG1, __dataOut);
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
    if (null == this.T_MOTIVO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MOTIVO);
    }
    if (null == this.N_ID_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UTENTE, __dataOut);
    }
    if (null == this.T_PIVA_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA_UTENTE);
    }
    if (null == this.T_RUOLO_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RUOLO_UTENTE);
    }
    if (null == this.T_CP_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CP_UTENTE);
    }
    if (null == this.N_ID_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISTR, __dataOut);
    }
    if (null == this.T_CP_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CP_DISTR);
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
    if (null == this.N_ID_UDD_RCU_E) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UDD_RCU_E, __dataOut);
    }
    if (null == this.N_ID_CC_E) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CC_E, __dataOut);
    }
    if (null == this.T_RUOLO_CC_E) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RUOLO_CC_E);
    }
    if (null == this.T_PIVA_CC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA_CC);
    }
    if (null == this.N_ID_CLIENTE_FINALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CLIENTE_FINALE, __dataOut);
    }
    if (null == this.N_ID_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FORNITURA, __dataOut);
    }
    if (null == this.N_ID_DISPACCIAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISPACCIAMENTO, __dataOut);
    }
    if (null == this.N_ID_COD_CAUSALE_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_COD_CAUSALE_DISTR, __dataOut);
    }
    if (null == this.T_TIPOLOGIA_VOLTURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPOLOGIA_VOLTURA);
    }
    if (null == this.T_TIPO_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_FORNITURA);
    }
    if (null == this.T_CF_COD_FISC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CF_COD_FISC);
    }
    if (null == this.T_CF_PIVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CF_PIVA);
    }
    if (null == this.B_CF_COD_FISC_ESTERO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_CF_COD_FISC_ESTERO);
    }
    if (null == this.B_CF_PERSONA_FISICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_CF_PERSONA_FISICA);
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
    if (null == this.N_MAX_ORE_CONG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_MAX_ORE_CONG, __dataOut);
    }
    if (null == this.D_DATA_STATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_STATO);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_VTG==null?"":N_ID_VTG.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PRATICA==null?"":N_ID_PRATICA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO==null?"":T_STATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_PDR==null?"":T_CODICE_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_REMI==null?"":T_CODICE_REMI, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RICHIESTA==null?"":D_DATA_RICHIESTA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_DEC_RICHIESTA==null?"":D_DATA_DEC_RICHIESTA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_DEC==null?"":D_DATA_DEC, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TITOLO==null?"":T_TITOLO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_NVG1==null?"":N_ID_NVG1.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_AMMISSIBILE==null?"":B_AMMISSIBILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CAUSALE==null?"":T_COD_CAUSALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MOTIVO==null?"":T_MOTIVO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UTENTE==null?"":N_ID_UTENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA_UTENTE==null?"":T_PIVA_UTENTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RUOLO_UTENTE==null?"":T_RUOLO_UTENTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CP_UTENTE==null?"":T_CP_UTENTE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISTR==null?"":N_ID_DISTR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CP_DISTR==null?"":T_CP_DISTR, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UDD_U==null?"":N_ID_UDD_U.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CC_U==null?"":N_ID_CC_U.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UDD_RCU_E==null?"":N_ID_UDD_RCU_E.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CC_E==null?"":N_ID_CC_E.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RUOLO_CC_E==null?"":T_RUOLO_CC_E, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA_CC==null?"":T_PIVA_CC, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CLIENTE_FINALE==null?"":N_ID_CLIENTE_FINALE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_FORNITURA==null?"":N_ID_FORNITURA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISPACCIAMENTO==null?"":N_ID_DISPACCIAMENTO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_COD_CAUSALE_DISTR==null?"":N_ID_COD_CAUSALE_DISTR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPOLOGIA_VOLTURA==null?"":T_TIPOLOGIA_VOLTURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_FORNITURA==null?"":T_TIPO_FORNITURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CF_COD_FISC==null?"":T_CF_COD_FISC, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CF_PIVA==null?"":T_CF_PIVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_CF_COD_FISC_ESTERO==null?"":B_CF_COD_FISC_ESTERO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_CF_PERSONA_FISICA==null?"":B_CF_PERSONA_FISICA, " ", delimiters));
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_MAX_ORE_CONG==null?"":N_MAX_ORE_CONG.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_STATO==null?"":D_DATA_STATO, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_VTG==null?"":N_ID_VTG.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PRATICA==null?"":N_ID_PRATICA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO==null?"":T_STATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_PDR==null?"":T_CODICE_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_REMI==null?"":T_CODICE_REMI, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RICHIESTA==null?"":D_DATA_RICHIESTA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_DEC_RICHIESTA==null?"":D_DATA_DEC_RICHIESTA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_DEC==null?"":D_DATA_DEC, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TITOLO==null?"":T_TITOLO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_NVG1==null?"":N_ID_NVG1.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_AMMISSIBILE==null?"":B_AMMISSIBILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CAUSALE==null?"":T_COD_CAUSALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MOTIVO==null?"":T_MOTIVO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UTENTE==null?"":N_ID_UTENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA_UTENTE==null?"":T_PIVA_UTENTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RUOLO_UTENTE==null?"":T_RUOLO_UTENTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CP_UTENTE==null?"":T_CP_UTENTE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISTR==null?"":N_ID_DISTR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CP_DISTR==null?"":T_CP_DISTR, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UDD_U==null?"":N_ID_UDD_U.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CC_U==null?"":N_ID_CC_U.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UDD_RCU_E==null?"":N_ID_UDD_RCU_E.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CC_E==null?"":N_ID_CC_E.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RUOLO_CC_E==null?"":T_RUOLO_CC_E, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA_CC==null?"":T_PIVA_CC, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CLIENTE_FINALE==null?"":N_ID_CLIENTE_FINALE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_FORNITURA==null?"":N_ID_FORNITURA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISPACCIAMENTO==null?"":N_ID_DISPACCIAMENTO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_COD_CAUSALE_DISTR==null?"":N_ID_COD_CAUSALE_DISTR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPOLOGIA_VOLTURA==null?"":T_TIPOLOGIA_VOLTURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_FORNITURA==null?"":T_TIPO_FORNITURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CF_COD_FISC==null?"":T_CF_COD_FISC, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CF_PIVA==null?"":T_CF_PIVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_CF_COD_FISC_ESTERO==null?"":B_CF_COD_FISC_ESTERO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_CF_PERSONA_FISICA==null?"":B_CF_PERSONA_FISICA, " ", delimiters));
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_MAX_ORE_CONG==null?"":N_MAX_ORE_CONG.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_STATO==null?"":D_DATA_STATO, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_VTG = null; } else {
      this.N_ID_VTG = new java.math.BigDecimal(__cur_str);
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
    if (__cur_str.equals("null")) { this.T_CODICE_PDR = null; } else {
      this.T_CODICE_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_REMI = null; } else {
      this.T_CODICE_REMI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RICHIESTA = null; } else {
      this.D_DATA_RICHIESTA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_DEC_RICHIESTA = null; } else {
      this.D_DATA_DEC_RICHIESTA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_DEC = null; } else {
      this.D_DATA_DEC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TITOLO = null; } else {
      this.T_TITOLO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_NVG1 = null; } else {
      this.N_ID_NVG1 = new java.math.BigDecimal(__cur_str);
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
    if (__cur_str.equals("null")) { this.T_MOTIVO = null; } else {
      this.T_MOTIVO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UTENTE = null; } else {
      this.N_ID_UTENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA_UTENTE = null; } else {
      this.T_PIVA_UTENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RUOLO_UTENTE = null; } else {
      this.T_RUOLO_UTENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CP_UTENTE = null; } else {
      this.T_CP_UTENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISTR = null; } else {
      this.N_ID_DISTR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CP_DISTR = null; } else {
      this.T_CP_DISTR = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UDD_RCU_E = null; } else {
      this.N_ID_UDD_RCU_E = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CC_E = null; } else {
      this.N_ID_CC_E = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RUOLO_CC_E = null; } else {
      this.T_RUOLO_CC_E = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA_CC = null; } else {
      this.T_PIVA_CC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CLIENTE_FINALE = null; } else {
      this.N_ID_CLIENTE_FINALE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_FORNITURA = null; } else {
      this.N_ID_FORNITURA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISPACCIAMENTO = null; } else {
      this.N_ID_DISPACCIAMENTO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_COD_CAUSALE_DISTR = null; } else {
      this.N_ID_COD_CAUSALE_DISTR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPOLOGIA_VOLTURA = null; } else {
      this.T_TIPOLOGIA_VOLTURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_FORNITURA = null; } else {
      this.T_TIPO_FORNITURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CF_COD_FISC = null; } else {
      this.T_CF_COD_FISC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CF_PIVA = null; } else {
      this.T_CF_PIVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_CF_COD_FISC_ESTERO = null; } else {
      this.B_CF_COD_FISC_ESTERO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_CF_PERSONA_FISICA = null; } else {
      this.B_CF_PERSONA_FISICA = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_MAX_ORE_CONG = null; } else {
      this.N_MAX_ORE_CONG = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_STATO = null; } else {
      this.D_DATA_STATO = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_VTG = null; } else {
      this.N_ID_VTG = new java.math.BigDecimal(__cur_str);
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
    if (__cur_str.equals("null")) { this.T_CODICE_PDR = null; } else {
      this.T_CODICE_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_REMI = null; } else {
      this.T_CODICE_REMI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RICHIESTA = null; } else {
      this.D_DATA_RICHIESTA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_DEC_RICHIESTA = null; } else {
      this.D_DATA_DEC_RICHIESTA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_DEC = null; } else {
      this.D_DATA_DEC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TITOLO = null; } else {
      this.T_TITOLO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_NVG1 = null; } else {
      this.N_ID_NVG1 = new java.math.BigDecimal(__cur_str);
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
    if (__cur_str.equals("null")) { this.T_MOTIVO = null; } else {
      this.T_MOTIVO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UTENTE = null; } else {
      this.N_ID_UTENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA_UTENTE = null; } else {
      this.T_PIVA_UTENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RUOLO_UTENTE = null; } else {
      this.T_RUOLO_UTENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CP_UTENTE = null; } else {
      this.T_CP_UTENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISTR = null; } else {
      this.N_ID_DISTR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CP_DISTR = null; } else {
      this.T_CP_DISTR = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UDD_RCU_E = null; } else {
      this.N_ID_UDD_RCU_E = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CC_E = null; } else {
      this.N_ID_CC_E = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RUOLO_CC_E = null; } else {
      this.T_RUOLO_CC_E = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA_CC = null; } else {
      this.T_PIVA_CC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CLIENTE_FINALE = null; } else {
      this.N_ID_CLIENTE_FINALE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_FORNITURA = null; } else {
      this.N_ID_FORNITURA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISPACCIAMENTO = null; } else {
      this.N_ID_DISPACCIAMENTO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_COD_CAUSALE_DISTR = null; } else {
      this.N_ID_COD_CAUSALE_DISTR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPOLOGIA_VOLTURA = null; } else {
      this.T_TIPOLOGIA_VOLTURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_FORNITURA = null; } else {
      this.T_TIPO_FORNITURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CF_COD_FISC = null; } else {
      this.T_CF_COD_FISC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CF_PIVA = null; } else {
      this.T_CF_PIVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_CF_COD_FISC_ESTERO = null; } else {
      this.B_CF_COD_FISC_ESTERO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_CF_PERSONA_FISICA = null; } else {
      this.B_CF_PERSONA_FISICA = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_MAX_ORE_CONG = null; } else {
      this.N_MAX_ORE_CONG = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_STATO = null; } else {
      this.D_DATA_STATO = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    switch_gas_prt_vtg o = (switch_gas_prt_vtg) super.clone();
    return o;
  }

  public void clone0(switch_gas_prt_vtg o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_VTG", this.N_ID_VTG);
    __sqoop$field_map.put("N_ID_PRATICA", this.N_ID_PRATICA);
    __sqoop$field_map.put("T_STATO", this.T_STATO);
    __sqoop$field_map.put("T_CODICE_PDR", this.T_CODICE_PDR);
    __sqoop$field_map.put("T_CODICE_REMI", this.T_CODICE_REMI);
    __sqoop$field_map.put("D_DATA_RICHIESTA", this.D_DATA_RICHIESTA);
    __sqoop$field_map.put("D_DATA_DEC_RICHIESTA", this.D_DATA_DEC_RICHIESTA);
    __sqoop$field_map.put("D_DATA_DEC", this.D_DATA_DEC);
    __sqoop$field_map.put("T_TITOLO", this.T_TITOLO);
    __sqoop$field_map.put("N_ID_NVG1", this.N_ID_NVG1);
    __sqoop$field_map.put("B_AMMISSIBILE", this.B_AMMISSIBILE);
    __sqoop$field_map.put("T_COD_CAUSALE", this.T_COD_CAUSALE);
    __sqoop$field_map.put("T_MOTIVO", this.T_MOTIVO);
    __sqoop$field_map.put("N_ID_UTENTE", this.N_ID_UTENTE);
    __sqoop$field_map.put("T_PIVA_UTENTE", this.T_PIVA_UTENTE);
    __sqoop$field_map.put("T_RUOLO_UTENTE", this.T_RUOLO_UTENTE);
    __sqoop$field_map.put("T_CP_UTENTE", this.T_CP_UTENTE);
    __sqoop$field_map.put("N_ID_DISTR", this.N_ID_DISTR);
    __sqoop$field_map.put("T_CP_DISTR", this.T_CP_DISTR);
    __sqoop$field_map.put("N_ID_UDD_U", this.N_ID_UDD_U);
    __sqoop$field_map.put("N_ID_CC_U", this.N_ID_CC_U);
    __sqoop$field_map.put("N_ID_UDD_RCU_E", this.N_ID_UDD_RCU_E);
    __sqoop$field_map.put("N_ID_CC_E", this.N_ID_CC_E);
    __sqoop$field_map.put("T_RUOLO_CC_E", this.T_RUOLO_CC_E);
    __sqoop$field_map.put("T_PIVA_CC", this.T_PIVA_CC);
    __sqoop$field_map.put("N_ID_CLIENTE_FINALE", this.N_ID_CLIENTE_FINALE);
    __sqoop$field_map.put("N_ID_FORNITURA", this.N_ID_FORNITURA);
    __sqoop$field_map.put("N_ID_DISPACCIAMENTO", this.N_ID_DISPACCIAMENTO);
    __sqoop$field_map.put("N_ID_COD_CAUSALE_DISTR", this.N_ID_COD_CAUSALE_DISTR);
    __sqoop$field_map.put("T_TIPOLOGIA_VOLTURA", this.T_TIPOLOGIA_VOLTURA);
    __sqoop$field_map.put("T_TIPO_FORNITURA", this.T_TIPO_FORNITURA);
    __sqoop$field_map.put("T_CF_COD_FISC", this.T_CF_COD_FISC);
    __sqoop$field_map.put("T_CF_PIVA", this.T_CF_PIVA);
    __sqoop$field_map.put("B_CF_COD_FISC_ESTERO", this.B_CF_COD_FISC_ESTERO);
    __sqoop$field_map.put("B_CF_PERSONA_FISICA", this.B_CF_PERSONA_FISICA);
    __sqoop$field_map.put("T_ESITO", this.T_ESITO);
    __sqoop$field_map.put("T_COD_ESITO", this.T_COD_ESITO);
    __sqoop$field_map.put("T_DETT_ESITO", this.T_DETT_ESITO);
    __sqoop$field_map.put("N_MAX_ORE_CONG", this.N_MAX_ORE_CONG);
    __sqoop$field_map.put("D_DATA_STATO", this.D_DATA_STATO);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_VTG", this.N_ID_VTG);
    __sqoop$field_map.put("N_ID_PRATICA", this.N_ID_PRATICA);
    __sqoop$field_map.put("T_STATO", this.T_STATO);
    __sqoop$field_map.put("T_CODICE_PDR", this.T_CODICE_PDR);
    __sqoop$field_map.put("T_CODICE_REMI", this.T_CODICE_REMI);
    __sqoop$field_map.put("D_DATA_RICHIESTA", this.D_DATA_RICHIESTA);
    __sqoop$field_map.put("D_DATA_DEC_RICHIESTA", this.D_DATA_DEC_RICHIESTA);
    __sqoop$field_map.put("D_DATA_DEC", this.D_DATA_DEC);
    __sqoop$field_map.put("T_TITOLO", this.T_TITOLO);
    __sqoop$field_map.put("N_ID_NVG1", this.N_ID_NVG1);
    __sqoop$field_map.put("B_AMMISSIBILE", this.B_AMMISSIBILE);
    __sqoop$field_map.put("T_COD_CAUSALE", this.T_COD_CAUSALE);
    __sqoop$field_map.put("T_MOTIVO", this.T_MOTIVO);
    __sqoop$field_map.put("N_ID_UTENTE", this.N_ID_UTENTE);
    __sqoop$field_map.put("T_PIVA_UTENTE", this.T_PIVA_UTENTE);
    __sqoop$field_map.put("T_RUOLO_UTENTE", this.T_RUOLO_UTENTE);
    __sqoop$field_map.put("T_CP_UTENTE", this.T_CP_UTENTE);
    __sqoop$field_map.put("N_ID_DISTR", this.N_ID_DISTR);
    __sqoop$field_map.put("T_CP_DISTR", this.T_CP_DISTR);
    __sqoop$field_map.put("N_ID_UDD_U", this.N_ID_UDD_U);
    __sqoop$field_map.put("N_ID_CC_U", this.N_ID_CC_U);
    __sqoop$field_map.put("N_ID_UDD_RCU_E", this.N_ID_UDD_RCU_E);
    __sqoop$field_map.put("N_ID_CC_E", this.N_ID_CC_E);
    __sqoop$field_map.put("T_RUOLO_CC_E", this.T_RUOLO_CC_E);
    __sqoop$field_map.put("T_PIVA_CC", this.T_PIVA_CC);
    __sqoop$field_map.put("N_ID_CLIENTE_FINALE", this.N_ID_CLIENTE_FINALE);
    __sqoop$field_map.put("N_ID_FORNITURA", this.N_ID_FORNITURA);
    __sqoop$field_map.put("N_ID_DISPACCIAMENTO", this.N_ID_DISPACCIAMENTO);
    __sqoop$field_map.put("N_ID_COD_CAUSALE_DISTR", this.N_ID_COD_CAUSALE_DISTR);
    __sqoop$field_map.put("T_TIPOLOGIA_VOLTURA", this.T_TIPOLOGIA_VOLTURA);
    __sqoop$field_map.put("T_TIPO_FORNITURA", this.T_TIPO_FORNITURA);
    __sqoop$field_map.put("T_CF_COD_FISC", this.T_CF_COD_FISC);
    __sqoop$field_map.put("T_CF_PIVA", this.T_CF_PIVA);
    __sqoop$field_map.put("B_CF_COD_FISC_ESTERO", this.B_CF_COD_FISC_ESTERO);
    __sqoop$field_map.put("B_CF_PERSONA_FISICA", this.B_CF_PERSONA_FISICA);
    __sqoop$field_map.put("T_ESITO", this.T_ESITO);
    __sqoop$field_map.put("T_COD_ESITO", this.T_COD_ESITO);
    __sqoop$field_map.put("T_DETT_ESITO", this.T_DETT_ESITO);
    __sqoop$field_map.put("N_MAX_ORE_CONG", this.N_MAX_ORE_CONG);
    __sqoop$field_map.put("D_DATA_STATO", this.D_DATA_STATO);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
