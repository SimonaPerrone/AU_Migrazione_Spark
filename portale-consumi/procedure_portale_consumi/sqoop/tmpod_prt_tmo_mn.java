// ORM class for table 'tmpod.prt_tmo_mn'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 15:09:27 CEST 2019
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

public class tmpod_prt_tmo_mn extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_FILE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_FILE = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_UDD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_UDD = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_DISTR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_DISTR = (java.math.BigDecimal)value;
      }
    });
    setters.put("TIPO_PRATICA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TIPO_PRATICA = (String)value;
      }
    });
    setters.put("COD_FLUSSO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        COD_FLUSSO = (String)value;
      }
    });
    setters.put("CODICE_POD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CODICE_POD = (String)value;
      }
    });
    setters.put("ANNOMESE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ANNOMESE = (String)value;
      }
    });
    setters.put("PUNTODISPACCIAMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PUNTODISPACCIAMENTO = (String)value;
      }
    });
    setters.put("DATAMISURA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATAMISURA = (String)value;
      }
    });
    setters.put("MOTIVAZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MOTIVAZIONE = (String)value;
      }
    });
    setters.put("TRATTAMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TRATTAMENTO = (String)value;
      }
    });
    setters.put("TENSIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TENSIONE = (java.math.BigDecimal)value;
      }
    });
    setters.put("POTCONTRIMP", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        POTCONTRIMP = (java.math.BigDecimal)value;
      }
    });
    setters.put("POTDISP", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        POTDISP = (java.math.BigDecimal)value;
      }
    });
    setters.put("CIFREATT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CIFREATT = (java.math.BigDecimal)value;
      }
    });
    setters.put("CIFREREA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CIFREREA = (java.math.BigDecimal)value;
      }
    });
    setters.put("KA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        KA = (java.math.BigDecimal)value;
      }
    });
    setters.put("TIPOMISURA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TIPOMISURA = (String)value;
      }
    });
    setters.put("DATAINIZIOPERIODO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATAINIZIOPERIODO = (String)value;
      }
    });
    setters.put("RACCOLTA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RACCOLTA = (String)value;
      }
    });
    setters.put("TIPODATO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TIPODATO = (String)value;
      }
    });
    setters.put("VALIDATO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        VALIDATO = (String)value;
      }
    });
    setters.put("EAM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        EAM = (java.math.BigDecimal)value;
      }
    });
    setters.put("EAF1", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        EAF1 = (java.math.BigDecimal)value;
      }
    });
    setters.put("EAF2", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        EAF2 = (java.math.BigDecimal)value;
      }
    });
    setters.put("EAF3", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        EAF3 = (java.math.BigDecimal)value;
      }
    });
    setters.put("D_RICEZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_RICEZIONE = (String)value;
      }
    });
    setters.put("PERDITA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PERDITA = (java.math.BigDecimal)value;
      }
    });
    setters.put("ID_PROC", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ID_PROC = (java.math.BigDecimal)value;
      }
    });
    setters.put("GIORNOMISURA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        GIORNOMISURA = (String)value;
      }
    });
    setters.put("GRUPPOMIS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        GRUPPOMIS = (String)value;
      }
    });
    setters.put("FORFAIT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FORFAIT = (String)value;
      }
    });
    setters.put("KR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        KR = (java.math.BigDecimal)value;
      }
    });
    setters.put("KP", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        KP = (java.math.BigDecimal)value;
      }
    });
    setters.put("MATRATT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MATRATT = (String)value;
      }
    });
    setters.put("MATRREA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MATRREA = (String)value;
      }
    });
    setters.put("MATRPOT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MATRPOT = (String)value;
      }
    });
    setters.put("DATAINSTMISATT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATAINSTMISATT = (String)value;
      }
    });
    setters.put("DATAINSTMISREA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATAINSTMISREA = (String)value;
      }
    });
    setters.put("DATAINSTMISPOT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATAINSTMISPOT = (String)value;
      }
    });
    setters.put("CIFREPOT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CIFREPOT = (java.math.BigDecimal)value;
      }
    });
  }
  public tmpod_prt_tmo_mn() {
    init0();
  }
  private java.math.BigDecimal N_ID;
  public java.math.BigDecimal get_N_ID() {
    return N_ID;
  }
  public void set_N_ID(java.math.BigDecimal N_ID) {
    this.N_ID = N_ID;
  }
  public tmpod_prt_tmo_mn with_N_ID(java.math.BigDecimal N_ID) {
    this.N_ID = N_ID;
    return this;
  }
  private java.math.BigDecimal N_ID_FILE;
  public java.math.BigDecimal get_N_ID_FILE() {
    return N_ID_FILE;
  }
  public void set_N_ID_FILE(java.math.BigDecimal N_ID_FILE) {
    this.N_ID_FILE = N_ID_FILE;
  }
  public tmpod_prt_tmo_mn with_N_ID_FILE(java.math.BigDecimal N_ID_FILE) {
    this.N_ID_FILE = N_ID_FILE;
    return this;
  }
  private java.math.BigDecimal N_ID_UDD;
  public java.math.BigDecimal get_N_ID_UDD() {
    return N_ID_UDD;
  }
  public void set_N_ID_UDD(java.math.BigDecimal N_ID_UDD) {
    this.N_ID_UDD = N_ID_UDD;
  }
  public tmpod_prt_tmo_mn with_N_ID_UDD(java.math.BigDecimal N_ID_UDD) {
    this.N_ID_UDD = N_ID_UDD;
    return this;
  }
  private java.math.BigDecimal N_ID_DISTR;
  public java.math.BigDecimal get_N_ID_DISTR() {
    return N_ID_DISTR;
  }
  public void set_N_ID_DISTR(java.math.BigDecimal N_ID_DISTR) {
    this.N_ID_DISTR = N_ID_DISTR;
  }
  public tmpod_prt_tmo_mn with_N_ID_DISTR(java.math.BigDecimal N_ID_DISTR) {
    this.N_ID_DISTR = N_ID_DISTR;
    return this;
  }
  private String TIPO_PRATICA;
  public String get_TIPO_PRATICA() {
    return TIPO_PRATICA;
  }
  public void set_TIPO_PRATICA(String TIPO_PRATICA) {
    this.TIPO_PRATICA = TIPO_PRATICA;
  }
  public tmpod_prt_tmo_mn with_TIPO_PRATICA(String TIPO_PRATICA) {
    this.TIPO_PRATICA = TIPO_PRATICA;
    return this;
  }
  private String COD_FLUSSO;
  public String get_COD_FLUSSO() {
    return COD_FLUSSO;
  }
  public void set_COD_FLUSSO(String COD_FLUSSO) {
    this.COD_FLUSSO = COD_FLUSSO;
  }
  public tmpod_prt_tmo_mn with_COD_FLUSSO(String COD_FLUSSO) {
    this.COD_FLUSSO = COD_FLUSSO;
    return this;
  }
  private String CODICE_POD;
  public String get_CODICE_POD() {
    return CODICE_POD;
  }
  public void set_CODICE_POD(String CODICE_POD) {
    this.CODICE_POD = CODICE_POD;
  }
  public tmpod_prt_tmo_mn with_CODICE_POD(String CODICE_POD) {
    this.CODICE_POD = CODICE_POD;
    return this;
  }
  private String ANNOMESE;
  public String get_ANNOMESE() {
    return ANNOMESE;
  }
  public void set_ANNOMESE(String ANNOMESE) {
    this.ANNOMESE = ANNOMESE;
  }
  public tmpod_prt_tmo_mn with_ANNOMESE(String ANNOMESE) {
    this.ANNOMESE = ANNOMESE;
    return this;
  }
  private String PUNTODISPACCIAMENTO;
  public String get_PUNTODISPACCIAMENTO() {
    return PUNTODISPACCIAMENTO;
  }
  public void set_PUNTODISPACCIAMENTO(String PUNTODISPACCIAMENTO) {
    this.PUNTODISPACCIAMENTO = PUNTODISPACCIAMENTO;
  }
  public tmpod_prt_tmo_mn with_PUNTODISPACCIAMENTO(String PUNTODISPACCIAMENTO) {
    this.PUNTODISPACCIAMENTO = PUNTODISPACCIAMENTO;
    return this;
  }
  private String DATAMISURA;
  public String get_DATAMISURA() {
    return DATAMISURA;
  }
  public void set_DATAMISURA(String DATAMISURA) {
    this.DATAMISURA = DATAMISURA;
  }
  public tmpod_prt_tmo_mn with_DATAMISURA(String DATAMISURA) {
    this.DATAMISURA = DATAMISURA;
    return this;
  }
  private String MOTIVAZIONE;
  public String get_MOTIVAZIONE() {
    return MOTIVAZIONE;
  }
  public void set_MOTIVAZIONE(String MOTIVAZIONE) {
    this.MOTIVAZIONE = MOTIVAZIONE;
  }
  public tmpod_prt_tmo_mn with_MOTIVAZIONE(String MOTIVAZIONE) {
    this.MOTIVAZIONE = MOTIVAZIONE;
    return this;
  }
  private String TRATTAMENTO;
  public String get_TRATTAMENTO() {
    return TRATTAMENTO;
  }
  public void set_TRATTAMENTO(String TRATTAMENTO) {
    this.TRATTAMENTO = TRATTAMENTO;
  }
  public tmpod_prt_tmo_mn with_TRATTAMENTO(String TRATTAMENTO) {
    this.TRATTAMENTO = TRATTAMENTO;
    return this;
  }
  private java.math.BigDecimal TENSIONE;
  public java.math.BigDecimal get_TENSIONE() {
    return TENSIONE;
  }
  public void set_TENSIONE(java.math.BigDecimal TENSIONE) {
    this.TENSIONE = TENSIONE;
  }
  public tmpod_prt_tmo_mn with_TENSIONE(java.math.BigDecimal TENSIONE) {
    this.TENSIONE = TENSIONE;
    return this;
  }
  private java.math.BigDecimal POTCONTRIMP;
  public java.math.BigDecimal get_POTCONTRIMP() {
    return POTCONTRIMP;
  }
  public void set_POTCONTRIMP(java.math.BigDecimal POTCONTRIMP) {
    this.POTCONTRIMP = POTCONTRIMP;
  }
  public tmpod_prt_tmo_mn with_POTCONTRIMP(java.math.BigDecimal POTCONTRIMP) {
    this.POTCONTRIMP = POTCONTRIMP;
    return this;
  }
  private java.math.BigDecimal POTDISP;
  public java.math.BigDecimal get_POTDISP() {
    return POTDISP;
  }
  public void set_POTDISP(java.math.BigDecimal POTDISP) {
    this.POTDISP = POTDISP;
  }
  public tmpod_prt_tmo_mn with_POTDISP(java.math.BigDecimal POTDISP) {
    this.POTDISP = POTDISP;
    return this;
  }
  private java.math.BigDecimal CIFREATT;
  public java.math.BigDecimal get_CIFREATT() {
    return CIFREATT;
  }
  public void set_CIFREATT(java.math.BigDecimal CIFREATT) {
    this.CIFREATT = CIFREATT;
  }
  public tmpod_prt_tmo_mn with_CIFREATT(java.math.BigDecimal CIFREATT) {
    this.CIFREATT = CIFREATT;
    return this;
  }
  private java.math.BigDecimal CIFREREA;
  public java.math.BigDecimal get_CIFREREA() {
    return CIFREREA;
  }
  public void set_CIFREREA(java.math.BigDecimal CIFREREA) {
    this.CIFREREA = CIFREREA;
  }
  public tmpod_prt_tmo_mn with_CIFREREA(java.math.BigDecimal CIFREREA) {
    this.CIFREREA = CIFREREA;
    return this;
  }
  private java.math.BigDecimal KA;
  public java.math.BigDecimal get_KA() {
    return KA;
  }
  public void set_KA(java.math.BigDecimal KA) {
    this.KA = KA;
  }
  public tmpod_prt_tmo_mn with_KA(java.math.BigDecimal KA) {
    this.KA = KA;
    return this;
  }
  private String TIPOMISURA;
  public String get_TIPOMISURA() {
    return TIPOMISURA;
  }
  public void set_TIPOMISURA(String TIPOMISURA) {
    this.TIPOMISURA = TIPOMISURA;
  }
  public tmpod_prt_tmo_mn with_TIPOMISURA(String TIPOMISURA) {
    this.TIPOMISURA = TIPOMISURA;
    return this;
  }
  private String DATAINIZIOPERIODO;
  public String get_DATAINIZIOPERIODO() {
    return DATAINIZIOPERIODO;
  }
  public void set_DATAINIZIOPERIODO(String DATAINIZIOPERIODO) {
    this.DATAINIZIOPERIODO = DATAINIZIOPERIODO;
  }
  public tmpod_prt_tmo_mn with_DATAINIZIOPERIODO(String DATAINIZIOPERIODO) {
    this.DATAINIZIOPERIODO = DATAINIZIOPERIODO;
    return this;
  }
  private String RACCOLTA;
  public String get_RACCOLTA() {
    return RACCOLTA;
  }
  public void set_RACCOLTA(String RACCOLTA) {
    this.RACCOLTA = RACCOLTA;
  }
  public tmpod_prt_tmo_mn with_RACCOLTA(String RACCOLTA) {
    this.RACCOLTA = RACCOLTA;
    return this;
  }
  private String TIPODATO;
  public String get_TIPODATO() {
    return TIPODATO;
  }
  public void set_TIPODATO(String TIPODATO) {
    this.TIPODATO = TIPODATO;
  }
  public tmpod_prt_tmo_mn with_TIPODATO(String TIPODATO) {
    this.TIPODATO = TIPODATO;
    return this;
  }
  private String VALIDATO;
  public String get_VALIDATO() {
    return VALIDATO;
  }
  public void set_VALIDATO(String VALIDATO) {
    this.VALIDATO = VALIDATO;
  }
  public tmpod_prt_tmo_mn with_VALIDATO(String VALIDATO) {
    this.VALIDATO = VALIDATO;
    return this;
  }
  private java.math.BigDecimal EAM;
  public java.math.BigDecimal get_EAM() {
    return EAM;
  }
  public void set_EAM(java.math.BigDecimal EAM) {
    this.EAM = EAM;
  }
  public tmpod_prt_tmo_mn with_EAM(java.math.BigDecimal EAM) {
    this.EAM = EAM;
    return this;
  }
  private java.math.BigDecimal EAF1;
  public java.math.BigDecimal get_EAF1() {
    return EAF1;
  }
  public void set_EAF1(java.math.BigDecimal EAF1) {
    this.EAF1 = EAF1;
  }
  public tmpod_prt_tmo_mn with_EAF1(java.math.BigDecimal EAF1) {
    this.EAF1 = EAF1;
    return this;
  }
  private java.math.BigDecimal EAF2;
  public java.math.BigDecimal get_EAF2() {
    return EAF2;
  }
  public void set_EAF2(java.math.BigDecimal EAF2) {
    this.EAF2 = EAF2;
  }
  public tmpod_prt_tmo_mn with_EAF2(java.math.BigDecimal EAF2) {
    this.EAF2 = EAF2;
    return this;
  }
  private java.math.BigDecimal EAF3;
  public java.math.BigDecimal get_EAF3() {
    return EAF3;
  }
  public void set_EAF3(java.math.BigDecimal EAF3) {
    this.EAF3 = EAF3;
  }
  public tmpod_prt_tmo_mn with_EAF3(java.math.BigDecimal EAF3) {
    this.EAF3 = EAF3;
    return this;
  }
  private String D_RICEZIONE;
  public String get_D_RICEZIONE() {
    return D_RICEZIONE;
  }
  public void set_D_RICEZIONE(String D_RICEZIONE) {
    this.D_RICEZIONE = D_RICEZIONE;
  }
  public tmpod_prt_tmo_mn with_D_RICEZIONE(String D_RICEZIONE) {
    this.D_RICEZIONE = D_RICEZIONE;
    return this;
  }
  private java.math.BigDecimal PERDITA;
  public java.math.BigDecimal get_PERDITA() {
    return PERDITA;
  }
  public void set_PERDITA(java.math.BigDecimal PERDITA) {
    this.PERDITA = PERDITA;
  }
  public tmpod_prt_tmo_mn with_PERDITA(java.math.BigDecimal PERDITA) {
    this.PERDITA = PERDITA;
    return this;
  }
  private java.math.BigDecimal ID_PROC;
  public java.math.BigDecimal get_ID_PROC() {
    return ID_PROC;
  }
  public void set_ID_PROC(java.math.BigDecimal ID_PROC) {
    this.ID_PROC = ID_PROC;
  }
  public tmpod_prt_tmo_mn with_ID_PROC(java.math.BigDecimal ID_PROC) {
    this.ID_PROC = ID_PROC;
    return this;
  }
  private String GIORNOMISURA;
  public String get_GIORNOMISURA() {
    return GIORNOMISURA;
  }
  public void set_GIORNOMISURA(String GIORNOMISURA) {
    this.GIORNOMISURA = GIORNOMISURA;
  }
  public tmpod_prt_tmo_mn with_GIORNOMISURA(String GIORNOMISURA) {
    this.GIORNOMISURA = GIORNOMISURA;
    return this;
  }
  private String GRUPPOMIS;
  public String get_GRUPPOMIS() {
    return GRUPPOMIS;
  }
  public void set_GRUPPOMIS(String GRUPPOMIS) {
    this.GRUPPOMIS = GRUPPOMIS;
  }
  public tmpod_prt_tmo_mn with_GRUPPOMIS(String GRUPPOMIS) {
    this.GRUPPOMIS = GRUPPOMIS;
    return this;
  }
  private String FORFAIT;
  public String get_FORFAIT() {
    return FORFAIT;
  }
  public void set_FORFAIT(String FORFAIT) {
    this.FORFAIT = FORFAIT;
  }
  public tmpod_prt_tmo_mn with_FORFAIT(String FORFAIT) {
    this.FORFAIT = FORFAIT;
    return this;
  }
  private java.math.BigDecimal KR;
  public java.math.BigDecimal get_KR() {
    return KR;
  }
  public void set_KR(java.math.BigDecimal KR) {
    this.KR = KR;
  }
  public tmpod_prt_tmo_mn with_KR(java.math.BigDecimal KR) {
    this.KR = KR;
    return this;
  }
  private java.math.BigDecimal KP;
  public java.math.BigDecimal get_KP() {
    return KP;
  }
  public void set_KP(java.math.BigDecimal KP) {
    this.KP = KP;
  }
  public tmpod_prt_tmo_mn with_KP(java.math.BigDecimal KP) {
    this.KP = KP;
    return this;
  }
  private String MATRATT;
  public String get_MATRATT() {
    return MATRATT;
  }
  public void set_MATRATT(String MATRATT) {
    this.MATRATT = MATRATT;
  }
  public tmpod_prt_tmo_mn with_MATRATT(String MATRATT) {
    this.MATRATT = MATRATT;
    return this;
  }
  private String MATRREA;
  public String get_MATRREA() {
    return MATRREA;
  }
  public void set_MATRREA(String MATRREA) {
    this.MATRREA = MATRREA;
  }
  public tmpod_prt_tmo_mn with_MATRREA(String MATRREA) {
    this.MATRREA = MATRREA;
    return this;
  }
  private String MATRPOT;
  public String get_MATRPOT() {
    return MATRPOT;
  }
  public void set_MATRPOT(String MATRPOT) {
    this.MATRPOT = MATRPOT;
  }
  public tmpod_prt_tmo_mn with_MATRPOT(String MATRPOT) {
    this.MATRPOT = MATRPOT;
    return this;
  }
  private String DATAINSTMISATT;
  public String get_DATAINSTMISATT() {
    return DATAINSTMISATT;
  }
  public void set_DATAINSTMISATT(String DATAINSTMISATT) {
    this.DATAINSTMISATT = DATAINSTMISATT;
  }
  public tmpod_prt_tmo_mn with_DATAINSTMISATT(String DATAINSTMISATT) {
    this.DATAINSTMISATT = DATAINSTMISATT;
    return this;
  }
  private String DATAINSTMISREA;
  public String get_DATAINSTMISREA() {
    return DATAINSTMISREA;
  }
  public void set_DATAINSTMISREA(String DATAINSTMISREA) {
    this.DATAINSTMISREA = DATAINSTMISREA;
  }
  public tmpod_prt_tmo_mn with_DATAINSTMISREA(String DATAINSTMISREA) {
    this.DATAINSTMISREA = DATAINSTMISREA;
    return this;
  }
  private String DATAINSTMISPOT;
  public String get_DATAINSTMISPOT() {
    return DATAINSTMISPOT;
  }
  public void set_DATAINSTMISPOT(String DATAINSTMISPOT) {
    this.DATAINSTMISPOT = DATAINSTMISPOT;
  }
  public tmpod_prt_tmo_mn with_DATAINSTMISPOT(String DATAINSTMISPOT) {
    this.DATAINSTMISPOT = DATAINSTMISPOT;
    return this;
  }
  private java.math.BigDecimal CIFREPOT;
  public java.math.BigDecimal get_CIFREPOT() {
    return CIFREPOT;
  }
  public void set_CIFREPOT(java.math.BigDecimal CIFREPOT) {
    this.CIFREPOT = CIFREPOT;
  }
  public tmpod_prt_tmo_mn with_CIFREPOT(java.math.BigDecimal CIFREPOT) {
    this.CIFREPOT = CIFREPOT;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof tmpod_prt_tmo_mn)) {
      return false;
    }
    tmpod_prt_tmo_mn that = (tmpod_prt_tmo_mn) o;
    boolean equal = true;
    equal = equal && (this.N_ID == null ? that.N_ID == null : this.N_ID.equals(that.N_ID));
    equal = equal && (this.N_ID_FILE == null ? that.N_ID_FILE == null : this.N_ID_FILE.equals(that.N_ID_FILE));
    equal = equal && (this.N_ID_UDD == null ? that.N_ID_UDD == null : this.N_ID_UDD.equals(that.N_ID_UDD));
    equal = equal && (this.N_ID_DISTR == null ? that.N_ID_DISTR == null : this.N_ID_DISTR.equals(that.N_ID_DISTR));
    equal = equal && (this.TIPO_PRATICA == null ? that.TIPO_PRATICA == null : this.TIPO_PRATICA.equals(that.TIPO_PRATICA));
    equal = equal && (this.COD_FLUSSO == null ? that.COD_FLUSSO == null : this.COD_FLUSSO.equals(that.COD_FLUSSO));
    equal = equal && (this.CODICE_POD == null ? that.CODICE_POD == null : this.CODICE_POD.equals(that.CODICE_POD));
    equal = equal && (this.ANNOMESE == null ? that.ANNOMESE == null : this.ANNOMESE.equals(that.ANNOMESE));
    equal = equal && (this.PUNTODISPACCIAMENTO == null ? that.PUNTODISPACCIAMENTO == null : this.PUNTODISPACCIAMENTO.equals(that.PUNTODISPACCIAMENTO));
    equal = equal && (this.DATAMISURA == null ? that.DATAMISURA == null : this.DATAMISURA.equals(that.DATAMISURA));
    equal = equal && (this.MOTIVAZIONE == null ? that.MOTIVAZIONE == null : this.MOTIVAZIONE.equals(that.MOTIVAZIONE));
    equal = equal && (this.TRATTAMENTO == null ? that.TRATTAMENTO == null : this.TRATTAMENTO.equals(that.TRATTAMENTO));
    equal = equal && (this.TENSIONE == null ? that.TENSIONE == null : this.TENSIONE.equals(that.TENSIONE));
    equal = equal && (this.POTCONTRIMP == null ? that.POTCONTRIMP == null : this.POTCONTRIMP.equals(that.POTCONTRIMP));
    equal = equal && (this.POTDISP == null ? that.POTDISP == null : this.POTDISP.equals(that.POTDISP));
    equal = equal && (this.CIFREATT == null ? that.CIFREATT == null : this.CIFREATT.equals(that.CIFREATT));
    equal = equal && (this.CIFREREA == null ? that.CIFREREA == null : this.CIFREREA.equals(that.CIFREREA));
    equal = equal && (this.KA == null ? that.KA == null : this.KA.equals(that.KA));
    equal = equal && (this.TIPOMISURA == null ? that.TIPOMISURA == null : this.TIPOMISURA.equals(that.TIPOMISURA));
    equal = equal && (this.DATAINIZIOPERIODO == null ? that.DATAINIZIOPERIODO == null : this.DATAINIZIOPERIODO.equals(that.DATAINIZIOPERIODO));
    equal = equal && (this.RACCOLTA == null ? that.RACCOLTA == null : this.RACCOLTA.equals(that.RACCOLTA));
    equal = equal && (this.TIPODATO == null ? that.TIPODATO == null : this.TIPODATO.equals(that.TIPODATO));
    equal = equal && (this.VALIDATO == null ? that.VALIDATO == null : this.VALIDATO.equals(that.VALIDATO));
    equal = equal && (this.EAM == null ? that.EAM == null : this.EAM.equals(that.EAM));
    equal = equal && (this.EAF1 == null ? that.EAF1 == null : this.EAF1.equals(that.EAF1));
    equal = equal && (this.EAF2 == null ? that.EAF2 == null : this.EAF2.equals(that.EAF2));
    equal = equal && (this.EAF3 == null ? that.EAF3 == null : this.EAF3.equals(that.EAF3));
    equal = equal && (this.D_RICEZIONE == null ? that.D_RICEZIONE == null : this.D_RICEZIONE.equals(that.D_RICEZIONE));
    equal = equal && (this.PERDITA == null ? that.PERDITA == null : this.PERDITA.equals(that.PERDITA));
    equal = equal && (this.ID_PROC == null ? that.ID_PROC == null : this.ID_PROC.equals(that.ID_PROC));
    equal = equal && (this.GIORNOMISURA == null ? that.GIORNOMISURA == null : this.GIORNOMISURA.equals(that.GIORNOMISURA));
    equal = equal && (this.GRUPPOMIS == null ? that.GRUPPOMIS == null : this.GRUPPOMIS.equals(that.GRUPPOMIS));
    equal = equal && (this.FORFAIT == null ? that.FORFAIT == null : this.FORFAIT.equals(that.FORFAIT));
    equal = equal && (this.KR == null ? that.KR == null : this.KR.equals(that.KR));
    equal = equal && (this.KP == null ? that.KP == null : this.KP.equals(that.KP));
    equal = equal && (this.MATRATT == null ? that.MATRATT == null : this.MATRATT.equals(that.MATRATT));
    equal = equal && (this.MATRREA == null ? that.MATRREA == null : this.MATRREA.equals(that.MATRREA));
    equal = equal && (this.MATRPOT == null ? that.MATRPOT == null : this.MATRPOT.equals(that.MATRPOT));
    equal = equal && (this.DATAINSTMISATT == null ? that.DATAINSTMISATT == null : this.DATAINSTMISATT.equals(that.DATAINSTMISATT));
    equal = equal && (this.DATAINSTMISREA == null ? that.DATAINSTMISREA == null : this.DATAINSTMISREA.equals(that.DATAINSTMISREA));
    equal = equal && (this.DATAINSTMISPOT == null ? that.DATAINSTMISPOT == null : this.DATAINSTMISPOT.equals(that.DATAINSTMISPOT));
    equal = equal && (this.CIFREPOT == null ? that.CIFREPOT == null : this.CIFREPOT.equals(that.CIFREPOT));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof tmpod_prt_tmo_mn)) {
      return false;
    }
    tmpod_prt_tmo_mn that = (tmpod_prt_tmo_mn) o;
    boolean equal = true;
    equal = equal && (this.N_ID == null ? that.N_ID == null : this.N_ID.equals(that.N_ID));
    equal = equal && (this.N_ID_FILE == null ? that.N_ID_FILE == null : this.N_ID_FILE.equals(that.N_ID_FILE));
    equal = equal && (this.N_ID_UDD == null ? that.N_ID_UDD == null : this.N_ID_UDD.equals(that.N_ID_UDD));
    equal = equal && (this.N_ID_DISTR == null ? that.N_ID_DISTR == null : this.N_ID_DISTR.equals(that.N_ID_DISTR));
    equal = equal && (this.TIPO_PRATICA == null ? that.TIPO_PRATICA == null : this.TIPO_PRATICA.equals(that.TIPO_PRATICA));
    equal = equal && (this.COD_FLUSSO == null ? that.COD_FLUSSO == null : this.COD_FLUSSO.equals(that.COD_FLUSSO));
    equal = equal && (this.CODICE_POD == null ? that.CODICE_POD == null : this.CODICE_POD.equals(that.CODICE_POD));
    equal = equal && (this.ANNOMESE == null ? that.ANNOMESE == null : this.ANNOMESE.equals(that.ANNOMESE));
    equal = equal && (this.PUNTODISPACCIAMENTO == null ? that.PUNTODISPACCIAMENTO == null : this.PUNTODISPACCIAMENTO.equals(that.PUNTODISPACCIAMENTO));
    equal = equal && (this.DATAMISURA == null ? that.DATAMISURA == null : this.DATAMISURA.equals(that.DATAMISURA));
    equal = equal && (this.MOTIVAZIONE == null ? that.MOTIVAZIONE == null : this.MOTIVAZIONE.equals(that.MOTIVAZIONE));
    equal = equal && (this.TRATTAMENTO == null ? that.TRATTAMENTO == null : this.TRATTAMENTO.equals(that.TRATTAMENTO));
    equal = equal && (this.TENSIONE == null ? that.TENSIONE == null : this.TENSIONE.equals(that.TENSIONE));
    equal = equal && (this.POTCONTRIMP == null ? that.POTCONTRIMP == null : this.POTCONTRIMP.equals(that.POTCONTRIMP));
    equal = equal && (this.POTDISP == null ? that.POTDISP == null : this.POTDISP.equals(that.POTDISP));
    equal = equal && (this.CIFREATT == null ? that.CIFREATT == null : this.CIFREATT.equals(that.CIFREATT));
    equal = equal && (this.CIFREREA == null ? that.CIFREREA == null : this.CIFREREA.equals(that.CIFREREA));
    equal = equal && (this.KA == null ? that.KA == null : this.KA.equals(that.KA));
    equal = equal && (this.TIPOMISURA == null ? that.TIPOMISURA == null : this.TIPOMISURA.equals(that.TIPOMISURA));
    equal = equal && (this.DATAINIZIOPERIODO == null ? that.DATAINIZIOPERIODO == null : this.DATAINIZIOPERIODO.equals(that.DATAINIZIOPERIODO));
    equal = equal && (this.RACCOLTA == null ? that.RACCOLTA == null : this.RACCOLTA.equals(that.RACCOLTA));
    equal = equal && (this.TIPODATO == null ? that.TIPODATO == null : this.TIPODATO.equals(that.TIPODATO));
    equal = equal && (this.VALIDATO == null ? that.VALIDATO == null : this.VALIDATO.equals(that.VALIDATO));
    equal = equal && (this.EAM == null ? that.EAM == null : this.EAM.equals(that.EAM));
    equal = equal && (this.EAF1 == null ? that.EAF1 == null : this.EAF1.equals(that.EAF1));
    equal = equal && (this.EAF2 == null ? that.EAF2 == null : this.EAF2.equals(that.EAF2));
    equal = equal && (this.EAF3 == null ? that.EAF3 == null : this.EAF3.equals(that.EAF3));
    equal = equal && (this.D_RICEZIONE == null ? that.D_RICEZIONE == null : this.D_RICEZIONE.equals(that.D_RICEZIONE));
    equal = equal && (this.PERDITA == null ? that.PERDITA == null : this.PERDITA.equals(that.PERDITA));
    equal = equal && (this.ID_PROC == null ? that.ID_PROC == null : this.ID_PROC.equals(that.ID_PROC));
    equal = equal && (this.GIORNOMISURA == null ? that.GIORNOMISURA == null : this.GIORNOMISURA.equals(that.GIORNOMISURA));
    equal = equal && (this.GRUPPOMIS == null ? that.GRUPPOMIS == null : this.GRUPPOMIS.equals(that.GRUPPOMIS));
    equal = equal && (this.FORFAIT == null ? that.FORFAIT == null : this.FORFAIT.equals(that.FORFAIT));
    equal = equal && (this.KR == null ? that.KR == null : this.KR.equals(that.KR));
    equal = equal && (this.KP == null ? that.KP == null : this.KP.equals(that.KP));
    equal = equal && (this.MATRATT == null ? that.MATRATT == null : this.MATRATT.equals(that.MATRATT));
    equal = equal && (this.MATRREA == null ? that.MATRREA == null : this.MATRREA.equals(that.MATRREA));
    equal = equal && (this.MATRPOT == null ? that.MATRPOT == null : this.MATRPOT.equals(that.MATRPOT));
    equal = equal && (this.DATAINSTMISATT == null ? that.DATAINSTMISATT == null : this.DATAINSTMISATT.equals(that.DATAINSTMISATT));
    equal = equal && (this.DATAINSTMISREA == null ? that.DATAINSTMISREA == null : this.DATAINSTMISREA.equals(that.DATAINSTMISREA));
    equal = equal && (this.DATAINSTMISPOT == null ? that.DATAINSTMISPOT == null : this.DATAINSTMISPOT.equals(that.DATAINSTMISPOT));
    equal = equal && (this.CIFREPOT == null ? that.CIFREPOT == null : this.CIFREPOT.equals(that.CIFREPOT));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_FILE = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.N_ID_UDD = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.N_ID_DISTR = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.TIPO_PRATICA = JdbcWritableBridge.readString(5, __dbResults);
    this.COD_FLUSSO = JdbcWritableBridge.readString(6, __dbResults);
    this.CODICE_POD = JdbcWritableBridge.readString(7, __dbResults);
    this.ANNOMESE = JdbcWritableBridge.readString(8, __dbResults);
    this.PUNTODISPACCIAMENTO = JdbcWritableBridge.readString(9, __dbResults);
    this.DATAMISURA = JdbcWritableBridge.readString(10, __dbResults);
    this.MOTIVAZIONE = JdbcWritableBridge.readString(11, __dbResults);
    this.TRATTAMENTO = JdbcWritableBridge.readString(12, __dbResults);
    this.TENSIONE = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.POTCONTRIMP = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.POTDISP = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.CIFREATT = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.CIFREREA = JdbcWritableBridge.readBigDecimal(17, __dbResults);
    this.KA = JdbcWritableBridge.readBigDecimal(18, __dbResults);
    this.TIPOMISURA = JdbcWritableBridge.readString(19, __dbResults);
    this.DATAINIZIOPERIODO = JdbcWritableBridge.readString(20, __dbResults);
    this.RACCOLTA = JdbcWritableBridge.readString(21, __dbResults);
    this.TIPODATO = JdbcWritableBridge.readString(22, __dbResults);
    this.VALIDATO = JdbcWritableBridge.readString(23, __dbResults);
    this.EAM = JdbcWritableBridge.readBigDecimal(24, __dbResults);
    this.EAF1 = JdbcWritableBridge.readBigDecimal(25, __dbResults);
    this.EAF2 = JdbcWritableBridge.readBigDecimal(26, __dbResults);
    this.EAF3 = JdbcWritableBridge.readBigDecimal(27, __dbResults);
    this.D_RICEZIONE = JdbcWritableBridge.readString(28, __dbResults);
    this.PERDITA = JdbcWritableBridge.readBigDecimal(29, __dbResults);
    this.ID_PROC = JdbcWritableBridge.readBigDecimal(30, __dbResults);
    this.GIORNOMISURA = JdbcWritableBridge.readString(31, __dbResults);
    this.GRUPPOMIS = JdbcWritableBridge.readString(32, __dbResults);
    this.FORFAIT = JdbcWritableBridge.readString(33, __dbResults);
    this.KR = JdbcWritableBridge.readBigDecimal(34, __dbResults);
    this.KP = JdbcWritableBridge.readBigDecimal(35, __dbResults);
    this.MATRATT = JdbcWritableBridge.readString(36, __dbResults);
    this.MATRREA = JdbcWritableBridge.readString(37, __dbResults);
    this.MATRPOT = JdbcWritableBridge.readString(38, __dbResults);
    this.DATAINSTMISATT = JdbcWritableBridge.readString(39, __dbResults);
    this.DATAINSTMISREA = JdbcWritableBridge.readString(40, __dbResults);
    this.DATAINSTMISPOT = JdbcWritableBridge.readString(41, __dbResults);
    this.CIFREPOT = JdbcWritableBridge.readBigDecimal(42, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_FILE = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.N_ID_UDD = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.N_ID_DISTR = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.TIPO_PRATICA = JdbcWritableBridge.readString(5, __dbResults);
    this.COD_FLUSSO = JdbcWritableBridge.readString(6, __dbResults);
    this.CODICE_POD = JdbcWritableBridge.readString(7, __dbResults);
    this.ANNOMESE = JdbcWritableBridge.readString(8, __dbResults);
    this.PUNTODISPACCIAMENTO = JdbcWritableBridge.readString(9, __dbResults);
    this.DATAMISURA = JdbcWritableBridge.readString(10, __dbResults);
    this.MOTIVAZIONE = JdbcWritableBridge.readString(11, __dbResults);
    this.TRATTAMENTO = JdbcWritableBridge.readString(12, __dbResults);
    this.TENSIONE = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.POTCONTRIMP = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.POTDISP = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.CIFREATT = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.CIFREREA = JdbcWritableBridge.readBigDecimal(17, __dbResults);
    this.KA = JdbcWritableBridge.readBigDecimal(18, __dbResults);
    this.TIPOMISURA = JdbcWritableBridge.readString(19, __dbResults);
    this.DATAINIZIOPERIODO = JdbcWritableBridge.readString(20, __dbResults);
    this.RACCOLTA = JdbcWritableBridge.readString(21, __dbResults);
    this.TIPODATO = JdbcWritableBridge.readString(22, __dbResults);
    this.VALIDATO = JdbcWritableBridge.readString(23, __dbResults);
    this.EAM = JdbcWritableBridge.readBigDecimal(24, __dbResults);
    this.EAF1 = JdbcWritableBridge.readBigDecimal(25, __dbResults);
    this.EAF2 = JdbcWritableBridge.readBigDecimal(26, __dbResults);
    this.EAF3 = JdbcWritableBridge.readBigDecimal(27, __dbResults);
    this.D_RICEZIONE = JdbcWritableBridge.readString(28, __dbResults);
    this.PERDITA = JdbcWritableBridge.readBigDecimal(29, __dbResults);
    this.ID_PROC = JdbcWritableBridge.readBigDecimal(30, __dbResults);
    this.GIORNOMISURA = JdbcWritableBridge.readString(31, __dbResults);
    this.GRUPPOMIS = JdbcWritableBridge.readString(32, __dbResults);
    this.FORFAIT = JdbcWritableBridge.readString(33, __dbResults);
    this.KR = JdbcWritableBridge.readBigDecimal(34, __dbResults);
    this.KP = JdbcWritableBridge.readBigDecimal(35, __dbResults);
    this.MATRATT = JdbcWritableBridge.readString(36, __dbResults);
    this.MATRREA = JdbcWritableBridge.readString(37, __dbResults);
    this.MATRPOT = JdbcWritableBridge.readString(38, __dbResults);
    this.DATAINSTMISATT = JdbcWritableBridge.readString(39, __dbResults);
    this.DATAINSTMISREA = JdbcWritableBridge.readString(40, __dbResults);
    this.DATAINSTMISPOT = JdbcWritableBridge.readString(41, __dbResults);
    this.CIFREPOT = JdbcWritableBridge.readBigDecimal(42, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_FILE, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UDD, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTR, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(TIPO_PRATICA, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COD_FLUSSO, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CODICE_POD, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ANNOMESE, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PUNTODISPACCIAMENTO, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATAMISURA, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(MOTIVAZIONE, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TRATTAMENTO, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(TENSIONE, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(POTCONTRIMP, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(POTDISP, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CIFREATT, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CIFREREA, 17 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(KA, 18 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(TIPOMISURA, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATAINIZIOPERIODO, 20 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(RACCOLTA, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TIPODATO, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(VALIDATO, 23 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(EAM, 24 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(EAF1, 25 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(EAF2, 26 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(EAF3, 27 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_RICEZIONE, 28 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(PERDITA, 29 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(ID_PROC, 30 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(GIORNOMISURA, 31 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(GRUPPOMIS, 32 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FORFAIT, 33 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(KR, 34 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(KP, 35 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(MATRATT, 36 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(MATRREA, 37 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(MATRPOT, 38 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATAINSTMISATT, 39 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(DATAINSTMISREA, 40 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(DATAINSTMISPOT, 41 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CIFREPOT, 42 + __off, 2, __dbStmt);
    return 42;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_FILE, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UDD, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTR, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(TIPO_PRATICA, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COD_FLUSSO, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CODICE_POD, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ANNOMESE, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PUNTODISPACCIAMENTO, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATAMISURA, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(MOTIVAZIONE, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TRATTAMENTO, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(TENSIONE, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(POTCONTRIMP, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(POTDISP, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CIFREATT, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CIFREREA, 17 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(KA, 18 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(TIPOMISURA, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATAINIZIOPERIODO, 20 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(RACCOLTA, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TIPODATO, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(VALIDATO, 23 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(EAM, 24 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(EAF1, 25 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(EAF2, 26 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(EAF3, 27 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_RICEZIONE, 28 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(PERDITA, 29 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(ID_PROC, 30 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(GIORNOMISURA, 31 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(GRUPPOMIS, 32 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FORFAIT, 33 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(KR, 34 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(KP, 35 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(MATRATT, 36 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(MATRREA, 37 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(MATRPOT, 38 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATAINSTMISATT, 39 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(DATAINSTMISREA, 40 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(DATAINSTMISPOT, 41 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CIFREPOT, 42 + __off, 2, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID = null;
    } else {
    this.N_ID = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_FILE = null;
    } else {
    this.N_ID_FILE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_UDD = null;
    } else {
    this.N_ID_UDD = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_DISTR = null;
    } else {
    this.N_ID_DISTR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TIPO_PRATICA = null;
    } else {
    this.TIPO_PRATICA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.COD_FLUSSO = null;
    } else {
    this.COD_FLUSSO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CODICE_POD = null;
    } else {
    this.CODICE_POD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ANNOMESE = null;
    } else {
    this.ANNOMESE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PUNTODISPACCIAMENTO = null;
    } else {
    this.PUNTODISPACCIAMENTO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATAMISURA = null;
    } else {
    this.DATAMISURA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MOTIVAZIONE = null;
    } else {
    this.MOTIVAZIONE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TRATTAMENTO = null;
    } else {
    this.TRATTAMENTO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TENSIONE = null;
    } else {
    this.TENSIONE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.POTCONTRIMP = null;
    } else {
    this.POTCONTRIMP = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.POTDISP = null;
    } else {
    this.POTDISP = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CIFREATT = null;
    } else {
    this.CIFREATT = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CIFREREA = null;
    } else {
    this.CIFREREA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.KA = null;
    } else {
    this.KA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TIPOMISURA = null;
    } else {
    this.TIPOMISURA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATAINIZIOPERIODO = null;
    } else {
    this.DATAINIZIOPERIODO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RACCOLTA = null;
    } else {
    this.RACCOLTA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TIPODATO = null;
    } else {
    this.TIPODATO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.VALIDATO = null;
    } else {
    this.VALIDATO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.EAM = null;
    } else {
    this.EAM = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.EAF1 = null;
    } else {
    this.EAF1 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.EAF2 = null;
    } else {
    this.EAF2 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.EAF3 = null;
    } else {
    this.EAF3 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_RICEZIONE = null;
    } else {
    this.D_RICEZIONE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PERDITA = null;
    } else {
    this.PERDITA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ID_PROC = null;
    } else {
    this.ID_PROC = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.GIORNOMISURA = null;
    } else {
    this.GIORNOMISURA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.GRUPPOMIS = null;
    } else {
    this.GRUPPOMIS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FORFAIT = null;
    } else {
    this.FORFAIT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.KR = null;
    } else {
    this.KR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.KP = null;
    } else {
    this.KP = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MATRATT = null;
    } else {
    this.MATRATT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MATRREA = null;
    } else {
    this.MATRREA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MATRPOT = null;
    } else {
    this.MATRPOT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATAINSTMISATT = null;
    } else {
    this.DATAINSTMISATT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATAINSTMISREA = null;
    } else {
    this.DATAINSTMISREA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATAINSTMISPOT = null;
    } else {
    this.DATAINSTMISPOT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CIFREPOT = null;
    } else {
    this.CIFREPOT = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID, __dataOut);
    }
    if (null == this.N_ID_FILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FILE, __dataOut);
    }
    if (null == this.N_ID_UDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UDD, __dataOut);
    }
    if (null == this.N_ID_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISTR, __dataOut);
    }
    if (null == this.TIPO_PRATICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIPO_PRATICA);
    }
    if (null == this.COD_FLUSSO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_FLUSSO);
    }
    if (null == this.CODICE_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CODICE_POD);
    }
    if (null == this.ANNOMESE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ANNOMESE);
    }
    if (null == this.PUNTODISPACCIAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PUNTODISPACCIAMENTO);
    }
    if (null == this.DATAMISURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATAMISURA);
    }
    if (null == this.MOTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MOTIVAZIONE);
    }
    if (null == this.TRATTAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRATTAMENTO);
    }
    if (null == this.TENSIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.TENSIONE, __dataOut);
    }
    if (null == this.POTCONTRIMP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.POTCONTRIMP, __dataOut);
    }
    if (null == this.POTDISP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.POTDISP, __dataOut);
    }
    if (null == this.CIFREATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CIFREATT, __dataOut);
    }
    if (null == this.CIFREREA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CIFREREA, __dataOut);
    }
    if (null == this.KA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.KA, __dataOut);
    }
    if (null == this.TIPOMISURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIPOMISURA);
    }
    if (null == this.DATAINIZIOPERIODO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATAINIZIOPERIODO);
    }
    if (null == this.RACCOLTA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RACCOLTA);
    }
    if (null == this.TIPODATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIPODATO);
    }
    if (null == this.VALIDATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, VALIDATO);
    }
    if (null == this.EAM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.EAM, __dataOut);
    }
    if (null == this.EAF1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.EAF1, __dataOut);
    }
    if (null == this.EAF2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.EAF2, __dataOut);
    }
    if (null == this.EAF3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.EAF3, __dataOut);
    }
    if (null == this.D_RICEZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_RICEZIONE);
    }
    if (null == this.PERDITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.PERDITA, __dataOut);
    }
    if (null == this.ID_PROC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.ID_PROC, __dataOut);
    }
    if (null == this.GIORNOMISURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, GIORNOMISURA);
    }
    if (null == this.GRUPPOMIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, GRUPPOMIS);
    }
    if (null == this.FORFAIT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FORFAIT);
    }
    if (null == this.KR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.KR, __dataOut);
    }
    if (null == this.KP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.KP, __dataOut);
    }
    if (null == this.MATRATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MATRATT);
    }
    if (null == this.MATRREA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MATRREA);
    }
    if (null == this.MATRPOT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MATRPOT);
    }
    if (null == this.DATAINSTMISATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATAINSTMISATT);
    }
    if (null == this.DATAINSTMISREA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATAINSTMISREA);
    }
    if (null == this.DATAINSTMISPOT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATAINSTMISPOT);
    }
    if (null == this.CIFREPOT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CIFREPOT, __dataOut);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID, __dataOut);
    }
    if (null == this.N_ID_FILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FILE, __dataOut);
    }
    if (null == this.N_ID_UDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UDD, __dataOut);
    }
    if (null == this.N_ID_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISTR, __dataOut);
    }
    if (null == this.TIPO_PRATICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIPO_PRATICA);
    }
    if (null == this.COD_FLUSSO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_FLUSSO);
    }
    if (null == this.CODICE_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CODICE_POD);
    }
    if (null == this.ANNOMESE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ANNOMESE);
    }
    if (null == this.PUNTODISPACCIAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PUNTODISPACCIAMENTO);
    }
    if (null == this.DATAMISURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATAMISURA);
    }
    if (null == this.MOTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MOTIVAZIONE);
    }
    if (null == this.TRATTAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRATTAMENTO);
    }
    if (null == this.TENSIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.TENSIONE, __dataOut);
    }
    if (null == this.POTCONTRIMP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.POTCONTRIMP, __dataOut);
    }
    if (null == this.POTDISP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.POTDISP, __dataOut);
    }
    if (null == this.CIFREATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CIFREATT, __dataOut);
    }
    if (null == this.CIFREREA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CIFREREA, __dataOut);
    }
    if (null == this.KA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.KA, __dataOut);
    }
    if (null == this.TIPOMISURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIPOMISURA);
    }
    if (null == this.DATAINIZIOPERIODO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATAINIZIOPERIODO);
    }
    if (null == this.RACCOLTA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RACCOLTA);
    }
    if (null == this.TIPODATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIPODATO);
    }
    if (null == this.VALIDATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, VALIDATO);
    }
    if (null == this.EAM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.EAM, __dataOut);
    }
    if (null == this.EAF1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.EAF1, __dataOut);
    }
    if (null == this.EAF2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.EAF2, __dataOut);
    }
    if (null == this.EAF3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.EAF3, __dataOut);
    }
    if (null == this.D_RICEZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_RICEZIONE);
    }
    if (null == this.PERDITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.PERDITA, __dataOut);
    }
    if (null == this.ID_PROC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.ID_PROC, __dataOut);
    }
    if (null == this.GIORNOMISURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, GIORNOMISURA);
    }
    if (null == this.GRUPPOMIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, GRUPPOMIS);
    }
    if (null == this.FORFAIT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FORFAIT);
    }
    if (null == this.KR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.KR, __dataOut);
    }
    if (null == this.KP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.KP, __dataOut);
    }
    if (null == this.MATRATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MATRATT);
    }
    if (null == this.MATRREA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MATRREA);
    }
    if (null == this.MATRPOT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MATRPOT);
    }
    if (null == this.DATAINSTMISATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATAINSTMISATT);
    }
    if (null == this.DATAINSTMISREA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATAINSTMISREA);
    }
    if (null == this.DATAINSTMISPOT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATAINSTMISPOT);
    }
    if (null == this.CIFREPOT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CIFREPOT, __dataOut);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID==null?"":N_ID.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_FILE==null?"":N_ID_FILE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UDD==null?"":N_ID_UDD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISTR==null?"":N_ID_DISTR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TIPO_PRATICA==null?"":TIPO_PRATICA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_FLUSSO==null?"":COD_FLUSSO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(CODICE_POD==null?"":CODICE_POD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(ANNOMESE==null?"":ANNOMESE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PUNTODISPACCIAMENTO==null?"":PUNTODISPACCIAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATAMISURA==null?"":DATAMISURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(MOTIVAZIONE==null?"":MOTIVAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TRATTAMENTO==null?"":TRATTAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TENSIONE==null?"":TENSIONE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(POTCONTRIMP==null?"":POTCONTRIMP.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(POTDISP==null?"":POTDISP.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CIFREATT==null?"":CIFREATT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CIFREREA==null?"":CIFREREA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(KA==null?"":KA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TIPOMISURA==null?"":TIPOMISURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATAINIZIOPERIODO==null?"":DATAINIZIOPERIODO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(RACCOLTA==null?"":RACCOLTA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TIPODATO==null?"":TIPODATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(VALIDATO==null?"":VALIDATO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EAM==null?"":EAM.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EAF1==null?"":EAF1.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EAF2==null?"":EAF2.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EAF3==null?"":EAF3.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_RICEZIONE==null?"":D_RICEZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PERDITA==null?"":PERDITA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ID_PROC==null?"":ID_PROC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(GIORNOMISURA==null?"":GIORNOMISURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(GRUPPOMIS==null?"":GRUPPOMIS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(FORFAIT==null?"":FORFAIT, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(KR==null?"":KR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(KP==null?"":KP.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(MATRATT==null?"":MATRATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(MATRREA==null?"":MATRREA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(MATRPOT==null?"":MATRPOT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATAINSTMISATT==null?"":DATAINSTMISATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATAINSTMISREA==null?"":DATAINSTMISREA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATAINSTMISPOT==null?"":DATAINSTMISPOT, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CIFREPOT==null?"":CIFREPOT.toPlainString(), delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID==null?"":N_ID.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_FILE==null?"":N_ID_FILE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UDD==null?"":N_ID_UDD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISTR==null?"":N_ID_DISTR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TIPO_PRATICA==null?"":TIPO_PRATICA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_FLUSSO==null?"":COD_FLUSSO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(CODICE_POD==null?"":CODICE_POD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(ANNOMESE==null?"":ANNOMESE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PUNTODISPACCIAMENTO==null?"":PUNTODISPACCIAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATAMISURA==null?"":DATAMISURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(MOTIVAZIONE==null?"":MOTIVAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TRATTAMENTO==null?"":TRATTAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TENSIONE==null?"":TENSIONE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(POTCONTRIMP==null?"":POTCONTRIMP.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(POTDISP==null?"":POTDISP.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CIFREATT==null?"":CIFREATT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CIFREREA==null?"":CIFREREA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(KA==null?"":KA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TIPOMISURA==null?"":TIPOMISURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATAINIZIOPERIODO==null?"":DATAINIZIOPERIODO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(RACCOLTA==null?"":RACCOLTA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TIPODATO==null?"":TIPODATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(VALIDATO==null?"":VALIDATO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EAM==null?"":EAM.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EAF1==null?"":EAF1.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EAF2==null?"":EAF2.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EAF3==null?"":EAF3.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_RICEZIONE==null?"":D_RICEZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PERDITA==null?"":PERDITA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ID_PROC==null?"":ID_PROC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(GIORNOMISURA==null?"":GIORNOMISURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(GRUPPOMIS==null?"":GRUPPOMIS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(FORFAIT==null?"":FORFAIT, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(KR==null?"":KR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(KP==null?"":KP.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(MATRATT==null?"":MATRATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(MATRREA==null?"":MATRREA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(MATRPOT==null?"":MATRPOT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATAINSTMISATT==null?"":DATAINSTMISATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATAINSTMISREA==null?"":DATAINSTMISREA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATAINSTMISPOT==null?"":DATAINSTMISPOT, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CIFREPOT==null?"":CIFREPOT.toPlainString(), delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID = null; } else {
      this.N_ID = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_FILE = null; } else {
      this.N_ID_FILE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UDD = null; } else {
      this.N_ID_UDD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISTR = null; } else {
      this.N_ID_DISTR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIPO_PRATICA = null; } else {
      this.TIPO_PRATICA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COD_FLUSSO = null; } else {
      this.COD_FLUSSO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CODICE_POD = null; } else {
      this.CODICE_POD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ANNOMESE = null; } else {
      this.ANNOMESE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PUNTODISPACCIAMENTO = null; } else {
      this.PUNTODISPACCIAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATAMISURA = null; } else {
      this.DATAMISURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MOTIVAZIONE = null; } else {
      this.MOTIVAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRATTAMENTO = null; } else {
      this.TRATTAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.TENSIONE = null; } else {
      this.TENSIONE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.POTCONTRIMP = null; } else {
      this.POTCONTRIMP = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.POTDISP = null; } else {
      this.POTDISP = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CIFREATT = null; } else {
      this.CIFREATT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CIFREREA = null; } else {
      this.CIFREREA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.KA = null; } else {
      this.KA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIPOMISURA = null; } else {
      this.TIPOMISURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATAINIZIOPERIODO = null; } else {
      this.DATAINIZIOPERIODO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RACCOLTA = null; } else {
      this.RACCOLTA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIPODATO = null; } else {
      this.TIPODATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.VALIDATO = null; } else {
      this.VALIDATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.EAM = null; } else {
      this.EAM = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.EAF1 = null; } else {
      this.EAF1 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.EAF2 = null; } else {
      this.EAF2 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.EAF3 = null; } else {
      this.EAF3 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_RICEZIONE = null; } else {
      this.D_RICEZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PERDITA = null; } else {
      this.PERDITA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ID_PROC = null; } else {
      this.ID_PROC = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.GIORNOMISURA = null; } else {
      this.GIORNOMISURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.GRUPPOMIS = null; } else {
      this.GRUPPOMIS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FORFAIT = null; } else {
      this.FORFAIT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.KR = null; } else {
      this.KR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.KP = null; } else {
      this.KP = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MATRATT = null; } else {
      this.MATRATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MATRREA = null; } else {
      this.MATRREA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MATRPOT = null; } else {
      this.MATRPOT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATAINSTMISATT = null; } else {
      this.DATAINSTMISATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATAINSTMISREA = null; } else {
      this.DATAINSTMISREA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATAINSTMISPOT = null; } else {
      this.DATAINSTMISPOT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CIFREPOT = null; } else {
      this.CIFREPOT = new java.math.BigDecimal(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID = null; } else {
      this.N_ID = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_FILE = null; } else {
      this.N_ID_FILE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UDD = null; } else {
      this.N_ID_UDD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISTR = null; } else {
      this.N_ID_DISTR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIPO_PRATICA = null; } else {
      this.TIPO_PRATICA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COD_FLUSSO = null; } else {
      this.COD_FLUSSO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CODICE_POD = null; } else {
      this.CODICE_POD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ANNOMESE = null; } else {
      this.ANNOMESE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PUNTODISPACCIAMENTO = null; } else {
      this.PUNTODISPACCIAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATAMISURA = null; } else {
      this.DATAMISURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MOTIVAZIONE = null; } else {
      this.MOTIVAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRATTAMENTO = null; } else {
      this.TRATTAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.TENSIONE = null; } else {
      this.TENSIONE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.POTCONTRIMP = null; } else {
      this.POTCONTRIMP = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.POTDISP = null; } else {
      this.POTDISP = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CIFREATT = null; } else {
      this.CIFREATT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CIFREREA = null; } else {
      this.CIFREREA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.KA = null; } else {
      this.KA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIPOMISURA = null; } else {
      this.TIPOMISURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATAINIZIOPERIODO = null; } else {
      this.DATAINIZIOPERIODO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RACCOLTA = null; } else {
      this.RACCOLTA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIPODATO = null; } else {
      this.TIPODATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.VALIDATO = null; } else {
      this.VALIDATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.EAM = null; } else {
      this.EAM = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.EAF1 = null; } else {
      this.EAF1 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.EAF2 = null; } else {
      this.EAF2 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.EAF3 = null; } else {
      this.EAF3 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_RICEZIONE = null; } else {
      this.D_RICEZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PERDITA = null; } else {
      this.PERDITA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ID_PROC = null; } else {
      this.ID_PROC = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.GIORNOMISURA = null; } else {
      this.GIORNOMISURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.GRUPPOMIS = null; } else {
      this.GRUPPOMIS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FORFAIT = null; } else {
      this.FORFAIT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.KR = null; } else {
      this.KR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.KP = null; } else {
      this.KP = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MATRATT = null; } else {
      this.MATRATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MATRREA = null; } else {
      this.MATRREA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MATRPOT = null; } else {
      this.MATRPOT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATAINSTMISATT = null; } else {
      this.DATAINSTMISATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATAINSTMISREA = null; } else {
      this.DATAINSTMISREA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATAINSTMISPOT = null; } else {
      this.DATAINSTMISPOT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CIFREPOT = null; } else {
      this.CIFREPOT = new java.math.BigDecimal(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    tmpod_prt_tmo_mn o = (tmpod_prt_tmo_mn) super.clone();
    return o;
  }

  public void clone0(tmpod_prt_tmo_mn o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID", this.N_ID);
    __sqoop$field_map.put("N_ID_FILE", this.N_ID_FILE);
    __sqoop$field_map.put("N_ID_UDD", this.N_ID_UDD);
    __sqoop$field_map.put("N_ID_DISTR", this.N_ID_DISTR);
    __sqoop$field_map.put("TIPO_PRATICA", this.TIPO_PRATICA);
    __sqoop$field_map.put("COD_FLUSSO", this.COD_FLUSSO);
    __sqoop$field_map.put("CODICE_POD", this.CODICE_POD);
    __sqoop$field_map.put("ANNOMESE", this.ANNOMESE);
    __sqoop$field_map.put("PUNTODISPACCIAMENTO", this.PUNTODISPACCIAMENTO);
    __sqoop$field_map.put("DATAMISURA", this.DATAMISURA);
    __sqoop$field_map.put("MOTIVAZIONE", this.MOTIVAZIONE);
    __sqoop$field_map.put("TRATTAMENTO", this.TRATTAMENTO);
    __sqoop$field_map.put("TENSIONE", this.TENSIONE);
    __sqoop$field_map.put("POTCONTRIMP", this.POTCONTRIMP);
    __sqoop$field_map.put("POTDISP", this.POTDISP);
    __sqoop$field_map.put("CIFREATT", this.CIFREATT);
    __sqoop$field_map.put("CIFREREA", this.CIFREREA);
    __sqoop$field_map.put("KA", this.KA);
    __sqoop$field_map.put("TIPOMISURA", this.TIPOMISURA);
    __sqoop$field_map.put("DATAINIZIOPERIODO", this.DATAINIZIOPERIODO);
    __sqoop$field_map.put("RACCOLTA", this.RACCOLTA);
    __sqoop$field_map.put("TIPODATO", this.TIPODATO);
    __sqoop$field_map.put("VALIDATO", this.VALIDATO);
    __sqoop$field_map.put("EAM", this.EAM);
    __sqoop$field_map.put("EAF1", this.EAF1);
    __sqoop$field_map.put("EAF2", this.EAF2);
    __sqoop$field_map.put("EAF3", this.EAF3);
    __sqoop$field_map.put("D_RICEZIONE", this.D_RICEZIONE);
    __sqoop$field_map.put("PERDITA", this.PERDITA);
    __sqoop$field_map.put("ID_PROC", this.ID_PROC);
    __sqoop$field_map.put("GIORNOMISURA", this.GIORNOMISURA);
    __sqoop$field_map.put("GRUPPOMIS", this.GRUPPOMIS);
    __sqoop$field_map.put("FORFAIT", this.FORFAIT);
    __sqoop$field_map.put("KR", this.KR);
    __sqoop$field_map.put("KP", this.KP);
    __sqoop$field_map.put("MATRATT", this.MATRATT);
    __sqoop$field_map.put("MATRREA", this.MATRREA);
    __sqoop$field_map.put("MATRPOT", this.MATRPOT);
    __sqoop$field_map.put("DATAINSTMISATT", this.DATAINSTMISATT);
    __sqoop$field_map.put("DATAINSTMISREA", this.DATAINSTMISREA);
    __sqoop$field_map.put("DATAINSTMISPOT", this.DATAINSTMISPOT);
    __sqoop$field_map.put("CIFREPOT", this.CIFREPOT);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID", this.N_ID);
    __sqoop$field_map.put("N_ID_FILE", this.N_ID_FILE);
    __sqoop$field_map.put("N_ID_UDD", this.N_ID_UDD);
    __sqoop$field_map.put("N_ID_DISTR", this.N_ID_DISTR);
    __sqoop$field_map.put("TIPO_PRATICA", this.TIPO_PRATICA);
    __sqoop$field_map.put("COD_FLUSSO", this.COD_FLUSSO);
    __sqoop$field_map.put("CODICE_POD", this.CODICE_POD);
    __sqoop$field_map.put("ANNOMESE", this.ANNOMESE);
    __sqoop$field_map.put("PUNTODISPACCIAMENTO", this.PUNTODISPACCIAMENTO);
    __sqoop$field_map.put("DATAMISURA", this.DATAMISURA);
    __sqoop$field_map.put("MOTIVAZIONE", this.MOTIVAZIONE);
    __sqoop$field_map.put("TRATTAMENTO", this.TRATTAMENTO);
    __sqoop$field_map.put("TENSIONE", this.TENSIONE);
    __sqoop$field_map.put("POTCONTRIMP", this.POTCONTRIMP);
    __sqoop$field_map.put("POTDISP", this.POTDISP);
    __sqoop$field_map.put("CIFREATT", this.CIFREATT);
    __sqoop$field_map.put("CIFREREA", this.CIFREREA);
    __sqoop$field_map.put("KA", this.KA);
    __sqoop$field_map.put("TIPOMISURA", this.TIPOMISURA);
    __sqoop$field_map.put("DATAINIZIOPERIODO", this.DATAINIZIOPERIODO);
    __sqoop$field_map.put("RACCOLTA", this.RACCOLTA);
    __sqoop$field_map.put("TIPODATO", this.TIPODATO);
    __sqoop$field_map.put("VALIDATO", this.VALIDATO);
    __sqoop$field_map.put("EAM", this.EAM);
    __sqoop$field_map.put("EAF1", this.EAF1);
    __sqoop$field_map.put("EAF2", this.EAF2);
    __sqoop$field_map.put("EAF3", this.EAF3);
    __sqoop$field_map.put("D_RICEZIONE", this.D_RICEZIONE);
    __sqoop$field_map.put("PERDITA", this.PERDITA);
    __sqoop$field_map.put("ID_PROC", this.ID_PROC);
    __sqoop$field_map.put("GIORNOMISURA", this.GIORNOMISURA);
    __sqoop$field_map.put("GRUPPOMIS", this.GRUPPOMIS);
    __sqoop$field_map.put("FORFAIT", this.FORFAIT);
    __sqoop$field_map.put("KR", this.KR);
    __sqoop$field_map.put("KP", this.KP);
    __sqoop$field_map.put("MATRATT", this.MATRATT);
    __sqoop$field_map.put("MATRREA", this.MATRREA);
    __sqoop$field_map.put("MATRPOT", this.MATRPOT);
    __sqoop$field_map.put("DATAINSTMISATT", this.DATAINSTMISATT);
    __sqoop$field_map.put("DATAINSTMISREA", this.DATAINSTMISREA);
    __sqoop$field_map.put("DATAINSTMISPOT", this.DATAINSTMISPOT);
    __sqoop$field_map.put("CIFREPOT", this.CIFREPOT);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
