// ORM class for table 'tmpod.prt_tmo_mv'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 16:26:56 CEST 2019
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

public class tmpod_prt_tmo_mv extends SqoopRecord  implements DBWritable, Writable {
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
    setters.put("PIVA_DISTR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PIVA_DISTR = (String)value;
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
    setters.put("DATAVOLTURA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATAVOLTURA = (String)value;
      }
    });
    setters.put("DATARILEVAZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATARILEVAZIONE = (String)value;
      }
    });
    setters.put("TRATTAMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TRATTAMENTO = (String)value;
      }
    });
    setters.put("PUNTODISPACCIAMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PUNTODISPACCIAMENTO = (String)value;
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
    setters.put("CODPRATATT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CODPRATATT = (String)value;
      }
    });
    setters.put("KR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        KR = (java.math.BigDecimal)value;
      }
    });
    setters.put("ERM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ERM = (java.math.BigDecimal)value;
      }
    });
    setters.put("ERF1", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ERF1 = (java.math.BigDecimal)value;
      }
    });
    setters.put("ERF2", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ERF2 = (java.math.BigDecimal)value;
      }
    });
    setters.put("ERF3", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ERF3 = (java.math.BigDecimal)value;
      }
    });
  }
  public tmpod_prt_tmo_mv() {
    init0();
  }
  private java.math.BigDecimal N_ID;
  public java.math.BigDecimal get_N_ID() {
    return N_ID;
  }
  public void set_N_ID(java.math.BigDecimal N_ID) {
    this.N_ID = N_ID;
  }
  public tmpod_prt_tmo_mv with_N_ID(java.math.BigDecimal N_ID) {
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
  public tmpod_prt_tmo_mv with_N_ID_FILE(java.math.BigDecimal N_ID_FILE) {
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
  public tmpod_prt_tmo_mv with_N_ID_UDD(java.math.BigDecimal N_ID_UDD) {
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
  public tmpod_prt_tmo_mv with_N_ID_DISTR(java.math.BigDecimal N_ID_DISTR) {
    this.N_ID_DISTR = N_ID_DISTR;
    return this;
  }
  private String PIVA_DISTR;
  public String get_PIVA_DISTR() {
    return PIVA_DISTR;
  }
  public void set_PIVA_DISTR(String PIVA_DISTR) {
    this.PIVA_DISTR = PIVA_DISTR;
  }
  public tmpod_prt_tmo_mv with_PIVA_DISTR(String PIVA_DISTR) {
    this.PIVA_DISTR = PIVA_DISTR;
    return this;
  }
  private String TIPO_PRATICA;
  public String get_TIPO_PRATICA() {
    return TIPO_PRATICA;
  }
  public void set_TIPO_PRATICA(String TIPO_PRATICA) {
    this.TIPO_PRATICA = TIPO_PRATICA;
  }
  public tmpod_prt_tmo_mv with_TIPO_PRATICA(String TIPO_PRATICA) {
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
  public tmpod_prt_tmo_mv with_COD_FLUSSO(String COD_FLUSSO) {
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
  public tmpod_prt_tmo_mv with_CODICE_POD(String CODICE_POD) {
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
  public tmpod_prt_tmo_mv with_ANNOMESE(String ANNOMESE) {
    this.ANNOMESE = ANNOMESE;
    return this;
  }
  private String DATAVOLTURA;
  public String get_DATAVOLTURA() {
    return DATAVOLTURA;
  }
  public void set_DATAVOLTURA(String DATAVOLTURA) {
    this.DATAVOLTURA = DATAVOLTURA;
  }
  public tmpod_prt_tmo_mv with_DATAVOLTURA(String DATAVOLTURA) {
    this.DATAVOLTURA = DATAVOLTURA;
    return this;
  }
  private String DATARILEVAZIONE;
  public String get_DATARILEVAZIONE() {
    return DATARILEVAZIONE;
  }
  public void set_DATARILEVAZIONE(String DATARILEVAZIONE) {
    this.DATARILEVAZIONE = DATARILEVAZIONE;
  }
  public tmpod_prt_tmo_mv with_DATARILEVAZIONE(String DATARILEVAZIONE) {
    this.DATARILEVAZIONE = DATARILEVAZIONE;
    return this;
  }
  private String TRATTAMENTO;
  public String get_TRATTAMENTO() {
    return TRATTAMENTO;
  }
  public void set_TRATTAMENTO(String TRATTAMENTO) {
    this.TRATTAMENTO = TRATTAMENTO;
  }
  public tmpod_prt_tmo_mv with_TRATTAMENTO(String TRATTAMENTO) {
    this.TRATTAMENTO = TRATTAMENTO;
    return this;
  }
  private String PUNTODISPACCIAMENTO;
  public String get_PUNTODISPACCIAMENTO() {
    return PUNTODISPACCIAMENTO;
  }
  public void set_PUNTODISPACCIAMENTO(String PUNTODISPACCIAMENTO) {
    this.PUNTODISPACCIAMENTO = PUNTODISPACCIAMENTO;
  }
  public tmpod_prt_tmo_mv with_PUNTODISPACCIAMENTO(String PUNTODISPACCIAMENTO) {
    this.PUNTODISPACCIAMENTO = PUNTODISPACCIAMENTO;
    return this;
  }
  private java.math.BigDecimal TENSIONE;
  public java.math.BigDecimal get_TENSIONE() {
    return TENSIONE;
  }
  public void set_TENSIONE(java.math.BigDecimal TENSIONE) {
    this.TENSIONE = TENSIONE;
  }
  public tmpod_prt_tmo_mv with_TENSIONE(java.math.BigDecimal TENSIONE) {
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
  public tmpod_prt_tmo_mv with_POTCONTRIMP(java.math.BigDecimal POTCONTRIMP) {
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
  public tmpod_prt_tmo_mv with_POTDISP(java.math.BigDecimal POTDISP) {
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
  public tmpod_prt_tmo_mv with_CIFREATT(java.math.BigDecimal CIFREATT) {
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
  public tmpod_prt_tmo_mv with_CIFREREA(java.math.BigDecimal CIFREREA) {
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
  public tmpod_prt_tmo_mv with_KA(java.math.BigDecimal KA) {
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
  public tmpod_prt_tmo_mv with_TIPOMISURA(String TIPOMISURA) {
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
  public tmpod_prt_tmo_mv with_DATAINIZIOPERIODO(String DATAINIZIOPERIODO) {
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
  public tmpod_prt_tmo_mv with_RACCOLTA(String RACCOLTA) {
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
  public tmpod_prt_tmo_mv with_TIPODATO(String TIPODATO) {
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
  public tmpod_prt_tmo_mv with_VALIDATO(String VALIDATO) {
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
  public tmpod_prt_tmo_mv with_EAM(java.math.BigDecimal EAM) {
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
  public tmpod_prt_tmo_mv with_EAF1(java.math.BigDecimal EAF1) {
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
  public tmpod_prt_tmo_mv with_EAF2(java.math.BigDecimal EAF2) {
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
  public tmpod_prt_tmo_mv with_EAF3(java.math.BigDecimal EAF3) {
    this.EAF3 = EAF3;
    return this;
  }
  private String CODPRATATT;
  public String get_CODPRATATT() {
    return CODPRATATT;
  }
  public void set_CODPRATATT(String CODPRATATT) {
    this.CODPRATATT = CODPRATATT;
  }
  public tmpod_prt_tmo_mv with_CODPRATATT(String CODPRATATT) {
    this.CODPRATATT = CODPRATATT;
    return this;
  }
  private java.math.BigDecimal KR;
  public java.math.BigDecimal get_KR() {
    return KR;
  }
  public void set_KR(java.math.BigDecimal KR) {
    this.KR = KR;
  }
  public tmpod_prt_tmo_mv with_KR(java.math.BigDecimal KR) {
    this.KR = KR;
    return this;
  }
  private java.math.BigDecimal ERM;
  public java.math.BigDecimal get_ERM() {
    return ERM;
  }
  public void set_ERM(java.math.BigDecimal ERM) {
    this.ERM = ERM;
  }
  public tmpod_prt_tmo_mv with_ERM(java.math.BigDecimal ERM) {
    this.ERM = ERM;
    return this;
  }
  private java.math.BigDecimal ERF1;
  public java.math.BigDecimal get_ERF1() {
    return ERF1;
  }
  public void set_ERF1(java.math.BigDecimal ERF1) {
    this.ERF1 = ERF1;
  }
  public tmpod_prt_tmo_mv with_ERF1(java.math.BigDecimal ERF1) {
    this.ERF1 = ERF1;
    return this;
  }
  private java.math.BigDecimal ERF2;
  public java.math.BigDecimal get_ERF2() {
    return ERF2;
  }
  public void set_ERF2(java.math.BigDecimal ERF2) {
    this.ERF2 = ERF2;
  }
  public tmpod_prt_tmo_mv with_ERF2(java.math.BigDecimal ERF2) {
    this.ERF2 = ERF2;
    return this;
  }
  private java.math.BigDecimal ERF3;
  public java.math.BigDecimal get_ERF3() {
    return ERF3;
  }
  public void set_ERF3(java.math.BigDecimal ERF3) {
    this.ERF3 = ERF3;
  }
  public tmpod_prt_tmo_mv with_ERF3(java.math.BigDecimal ERF3) {
    this.ERF3 = ERF3;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof tmpod_prt_tmo_mv)) {
      return false;
    }
    tmpod_prt_tmo_mv that = (tmpod_prt_tmo_mv) o;
    boolean equal = true;
    equal = equal && (this.N_ID == null ? that.N_ID == null : this.N_ID.equals(that.N_ID));
    equal = equal && (this.N_ID_FILE == null ? that.N_ID_FILE == null : this.N_ID_FILE.equals(that.N_ID_FILE));
    equal = equal && (this.N_ID_UDD == null ? that.N_ID_UDD == null : this.N_ID_UDD.equals(that.N_ID_UDD));
    equal = equal && (this.N_ID_DISTR == null ? that.N_ID_DISTR == null : this.N_ID_DISTR.equals(that.N_ID_DISTR));
    equal = equal && (this.PIVA_DISTR == null ? that.PIVA_DISTR == null : this.PIVA_DISTR.equals(that.PIVA_DISTR));
    equal = equal && (this.TIPO_PRATICA == null ? that.TIPO_PRATICA == null : this.TIPO_PRATICA.equals(that.TIPO_PRATICA));
    equal = equal && (this.COD_FLUSSO == null ? that.COD_FLUSSO == null : this.COD_FLUSSO.equals(that.COD_FLUSSO));
    equal = equal && (this.CODICE_POD == null ? that.CODICE_POD == null : this.CODICE_POD.equals(that.CODICE_POD));
    equal = equal && (this.ANNOMESE == null ? that.ANNOMESE == null : this.ANNOMESE.equals(that.ANNOMESE));
    equal = equal && (this.DATAVOLTURA == null ? that.DATAVOLTURA == null : this.DATAVOLTURA.equals(that.DATAVOLTURA));
    equal = equal && (this.DATARILEVAZIONE == null ? that.DATARILEVAZIONE == null : this.DATARILEVAZIONE.equals(that.DATARILEVAZIONE));
    equal = equal && (this.TRATTAMENTO == null ? that.TRATTAMENTO == null : this.TRATTAMENTO.equals(that.TRATTAMENTO));
    equal = equal && (this.PUNTODISPACCIAMENTO == null ? that.PUNTODISPACCIAMENTO == null : this.PUNTODISPACCIAMENTO.equals(that.PUNTODISPACCIAMENTO));
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
    equal = equal && (this.CODPRATATT == null ? that.CODPRATATT == null : this.CODPRATATT.equals(that.CODPRATATT));
    equal = equal && (this.KR == null ? that.KR == null : this.KR.equals(that.KR));
    equal = equal && (this.ERM == null ? that.ERM == null : this.ERM.equals(that.ERM));
    equal = equal && (this.ERF1 == null ? that.ERF1 == null : this.ERF1.equals(that.ERF1));
    equal = equal && (this.ERF2 == null ? that.ERF2 == null : this.ERF2.equals(that.ERF2));
    equal = equal && (this.ERF3 == null ? that.ERF3 == null : this.ERF3.equals(that.ERF3));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof tmpod_prt_tmo_mv)) {
      return false;
    }
    tmpod_prt_tmo_mv that = (tmpod_prt_tmo_mv) o;
    boolean equal = true;
    equal = equal && (this.N_ID == null ? that.N_ID == null : this.N_ID.equals(that.N_ID));
    equal = equal && (this.N_ID_FILE == null ? that.N_ID_FILE == null : this.N_ID_FILE.equals(that.N_ID_FILE));
    equal = equal && (this.N_ID_UDD == null ? that.N_ID_UDD == null : this.N_ID_UDD.equals(that.N_ID_UDD));
    equal = equal && (this.N_ID_DISTR == null ? that.N_ID_DISTR == null : this.N_ID_DISTR.equals(that.N_ID_DISTR));
    equal = equal && (this.PIVA_DISTR == null ? that.PIVA_DISTR == null : this.PIVA_DISTR.equals(that.PIVA_DISTR));
    equal = equal && (this.TIPO_PRATICA == null ? that.TIPO_PRATICA == null : this.TIPO_PRATICA.equals(that.TIPO_PRATICA));
    equal = equal && (this.COD_FLUSSO == null ? that.COD_FLUSSO == null : this.COD_FLUSSO.equals(that.COD_FLUSSO));
    equal = equal && (this.CODICE_POD == null ? that.CODICE_POD == null : this.CODICE_POD.equals(that.CODICE_POD));
    equal = equal && (this.ANNOMESE == null ? that.ANNOMESE == null : this.ANNOMESE.equals(that.ANNOMESE));
    equal = equal && (this.DATAVOLTURA == null ? that.DATAVOLTURA == null : this.DATAVOLTURA.equals(that.DATAVOLTURA));
    equal = equal && (this.DATARILEVAZIONE == null ? that.DATARILEVAZIONE == null : this.DATARILEVAZIONE.equals(that.DATARILEVAZIONE));
    equal = equal && (this.TRATTAMENTO == null ? that.TRATTAMENTO == null : this.TRATTAMENTO.equals(that.TRATTAMENTO));
    equal = equal && (this.PUNTODISPACCIAMENTO == null ? that.PUNTODISPACCIAMENTO == null : this.PUNTODISPACCIAMENTO.equals(that.PUNTODISPACCIAMENTO));
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
    equal = equal && (this.CODPRATATT == null ? that.CODPRATATT == null : this.CODPRATATT.equals(that.CODPRATATT));
    equal = equal && (this.KR == null ? that.KR == null : this.KR.equals(that.KR));
    equal = equal && (this.ERM == null ? that.ERM == null : this.ERM.equals(that.ERM));
    equal = equal && (this.ERF1 == null ? that.ERF1 == null : this.ERF1.equals(that.ERF1));
    equal = equal && (this.ERF2 == null ? that.ERF2 == null : this.ERF2.equals(that.ERF2));
    equal = equal && (this.ERF3 == null ? that.ERF3 == null : this.ERF3.equals(that.ERF3));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_FILE = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.N_ID_UDD = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.N_ID_DISTR = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.PIVA_DISTR = JdbcWritableBridge.readString(5, __dbResults);
    this.TIPO_PRATICA = JdbcWritableBridge.readString(6, __dbResults);
    this.COD_FLUSSO = JdbcWritableBridge.readString(7, __dbResults);
    this.CODICE_POD = JdbcWritableBridge.readString(8, __dbResults);
    this.ANNOMESE = JdbcWritableBridge.readString(9, __dbResults);
    this.DATAVOLTURA = JdbcWritableBridge.readString(10, __dbResults);
    this.DATARILEVAZIONE = JdbcWritableBridge.readString(11, __dbResults);
    this.TRATTAMENTO = JdbcWritableBridge.readString(12, __dbResults);
    this.PUNTODISPACCIAMENTO = JdbcWritableBridge.readString(13, __dbResults);
    this.TENSIONE = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.POTCONTRIMP = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.POTDISP = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.CIFREATT = JdbcWritableBridge.readBigDecimal(17, __dbResults);
    this.CIFREREA = JdbcWritableBridge.readBigDecimal(18, __dbResults);
    this.KA = JdbcWritableBridge.readBigDecimal(19, __dbResults);
    this.TIPOMISURA = JdbcWritableBridge.readString(20, __dbResults);
    this.DATAINIZIOPERIODO = JdbcWritableBridge.readString(21, __dbResults);
    this.RACCOLTA = JdbcWritableBridge.readString(22, __dbResults);
    this.TIPODATO = JdbcWritableBridge.readString(23, __dbResults);
    this.VALIDATO = JdbcWritableBridge.readString(24, __dbResults);
    this.EAM = JdbcWritableBridge.readBigDecimal(25, __dbResults);
    this.EAF1 = JdbcWritableBridge.readBigDecimal(26, __dbResults);
    this.EAF2 = JdbcWritableBridge.readBigDecimal(27, __dbResults);
    this.EAF3 = JdbcWritableBridge.readBigDecimal(28, __dbResults);
    this.CODPRATATT = JdbcWritableBridge.readString(29, __dbResults);
    this.KR = JdbcWritableBridge.readBigDecimal(30, __dbResults);
    this.ERM = JdbcWritableBridge.readBigDecimal(31, __dbResults);
    this.ERF1 = JdbcWritableBridge.readBigDecimal(32, __dbResults);
    this.ERF2 = JdbcWritableBridge.readBigDecimal(33, __dbResults);
    this.ERF3 = JdbcWritableBridge.readBigDecimal(34, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_FILE = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.N_ID_UDD = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.N_ID_DISTR = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.PIVA_DISTR = JdbcWritableBridge.readString(5, __dbResults);
    this.TIPO_PRATICA = JdbcWritableBridge.readString(6, __dbResults);
    this.COD_FLUSSO = JdbcWritableBridge.readString(7, __dbResults);
    this.CODICE_POD = JdbcWritableBridge.readString(8, __dbResults);
    this.ANNOMESE = JdbcWritableBridge.readString(9, __dbResults);
    this.DATAVOLTURA = JdbcWritableBridge.readString(10, __dbResults);
    this.DATARILEVAZIONE = JdbcWritableBridge.readString(11, __dbResults);
    this.TRATTAMENTO = JdbcWritableBridge.readString(12, __dbResults);
    this.PUNTODISPACCIAMENTO = JdbcWritableBridge.readString(13, __dbResults);
    this.TENSIONE = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.POTCONTRIMP = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.POTDISP = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.CIFREATT = JdbcWritableBridge.readBigDecimal(17, __dbResults);
    this.CIFREREA = JdbcWritableBridge.readBigDecimal(18, __dbResults);
    this.KA = JdbcWritableBridge.readBigDecimal(19, __dbResults);
    this.TIPOMISURA = JdbcWritableBridge.readString(20, __dbResults);
    this.DATAINIZIOPERIODO = JdbcWritableBridge.readString(21, __dbResults);
    this.RACCOLTA = JdbcWritableBridge.readString(22, __dbResults);
    this.TIPODATO = JdbcWritableBridge.readString(23, __dbResults);
    this.VALIDATO = JdbcWritableBridge.readString(24, __dbResults);
    this.EAM = JdbcWritableBridge.readBigDecimal(25, __dbResults);
    this.EAF1 = JdbcWritableBridge.readBigDecimal(26, __dbResults);
    this.EAF2 = JdbcWritableBridge.readBigDecimal(27, __dbResults);
    this.EAF3 = JdbcWritableBridge.readBigDecimal(28, __dbResults);
    this.CODPRATATT = JdbcWritableBridge.readString(29, __dbResults);
    this.KR = JdbcWritableBridge.readBigDecimal(30, __dbResults);
    this.ERM = JdbcWritableBridge.readBigDecimal(31, __dbResults);
    this.ERF1 = JdbcWritableBridge.readBigDecimal(32, __dbResults);
    this.ERF2 = JdbcWritableBridge.readBigDecimal(33, __dbResults);
    this.ERF3 = JdbcWritableBridge.readBigDecimal(34, __dbResults);
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
    JdbcWritableBridge.writeString(PIVA_DISTR, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TIPO_PRATICA, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COD_FLUSSO, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CODICE_POD, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ANNOMESE, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATAVOLTURA, 10 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(DATARILEVAZIONE, 11 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(TRATTAMENTO, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PUNTODISPACCIAMENTO, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(TENSIONE, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(POTCONTRIMP, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(POTDISP, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CIFREATT, 17 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CIFREREA, 18 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(KA, 19 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(TIPOMISURA, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATAINIZIOPERIODO, 21 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(RACCOLTA, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TIPODATO, 23 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(VALIDATO, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(EAM, 25 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(EAF1, 26 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(EAF2, 27 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(EAF3, 28 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(CODPRATATT, 29 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(KR, 30 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(ERM, 31 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(ERF1, 32 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(ERF2, 33 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(ERF3, 34 + __off, 2, __dbStmt);
    return 34;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_FILE, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UDD, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTR, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PIVA_DISTR, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TIPO_PRATICA, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COD_FLUSSO, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CODICE_POD, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ANNOMESE, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATAVOLTURA, 10 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(DATARILEVAZIONE, 11 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(TRATTAMENTO, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PUNTODISPACCIAMENTO, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(TENSIONE, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(POTCONTRIMP, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(POTDISP, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CIFREATT, 17 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CIFREREA, 18 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(KA, 19 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(TIPOMISURA, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATAINIZIOPERIODO, 21 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(RACCOLTA, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TIPODATO, 23 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(VALIDATO, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(EAM, 25 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(EAF1, 26 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(EAF2, 27 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(EAF3, 28 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(CODPRATATT, 29 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(KR, 30 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(ERM, 31 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(ERF1, 32 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(ERF2, 33 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(ERF3, 34 + __off, 2, __dbStmt);
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
        this.PIVA_DISTR = null;
    } else {
    this.PIVA_DISTR = Text.readString(__dataIn);
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
        this.DATAVOLTURA = null;
    } else {
    this.DATAVOLTURA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATARILEVAZIONE = null;
    } else {
    this.DATARILEVAZIONE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TRATTAMENTO = null;
    } else {
    this.TRATTAMENTO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PUNTODISPACCIAMENTO = null;
    } else {
    this.PUNTODISPACCIAMENTO = Text.readString(__dataIn);
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
        this.CODPRATATT = null;
    } else {
    this.CODPRATATT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.KR = null;
    } else {
    this.KR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ERM = null;
    } else {
    this.ERM = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ERF1 = null;
    } else {
    this.ERF1 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ERF2 = null;
    } else {
    this.ERF2 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ERF3 = null;
    } else {
    this.ERF3 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
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
    if (null == this.PIVA_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PIVA_DISTR);
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
    if (null == this.DATAVOLTURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATAVOLTURA);
    }
    if (null == this.DATARILEVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATARILEVAZIONE);
    }
    if (null == this.TRATTAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRATTAMENTO);
    }
    if (null == this.PUNTODISPACCIAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PUNTODISPACCIAMENTO);
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
    if (null == this.CODPRATATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CODPRATATT);
    }
    if (null == this.KR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.KR, __dataOut);
    }
    if (null == this.ERM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.ERM, __dataOut);
    }
    if (null == this.ERF1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.ERF1, __dataOut);
    }
    if (null == this.ERF2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.ERF2, __dataOut);
    }
    if (null == this.ERF3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.ERF3, __dataOut);
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
    if (null == this.PIVA_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PIVA_DISTR);
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
    if (null == this.DATAVOLTURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATAVOLTURA);
    }
    if (null == this.DATARILEVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATARILEVAZIONE);
    }
    if (null == this.TRATTAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRATTAMENTO);
    }
    if (null == this.PUNTODISPACCIAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PUNTODISPACCIAMENTO);
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
    if (null == this.CODPRATATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CODPRATATT);
    }
    if (null == this.KR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.KR, __dataOut);
    }
    if (null == this.ERM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.ERM, __dataOut);
    }
    if (null == this.ERF1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.ERF1, __dataOut);
    }
    if (null == this.ERF2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.ERF2, __dataOut);
    }
    if (null == this.ERF3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.ERF3, __dataOut);
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PIVA_DISTR==null?"":PIVA_DISTR, " ", delimiters));
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATAVOLTURA==null?"":DATAVOLTURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATARILEVAZIONE==null?"":DATARILEVAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TRATTAMENTO==null?"":TRATTAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PUNTODISPACCIAMENTO==null?"":PUNTODISPACCIAMENTO, " ", delimiters));
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(CODPRATATT==null?"":CODPRATATT, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(KR==null?"":KR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ERM==null?"":ERM.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ERF1==null?"":ERF1.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ERF2==null?"":ERF2.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ERF3==null?"":ERF3.toPlainString(), delimiters));
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PIVA_DISTR==null?"":PIVA_DISTR, " ", delimiters));
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATAVOLTURA==null?"":DATAVOLTURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATARILEVAZIONE==null?"":DATARILEVAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TRATTAMENTO==null?"":TRATTAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PUNTODISPACCIAMENTO==null?"":PUNTODISPACCIAMENTO, " ", delimiters));
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(CODPRATATT==null?"":CODPRATATT, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(KR==null?"":KR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ERM==null?"":ERM.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ERF1==null?"":ERF1.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ERF2==null?"":ERF2.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ERF3==null?"":ERF3.toPlainString(), delimiters));
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
    if (__cur_str.equals("null")) { this.PIVA_DISTR = null; } else {
      this.PIVA_DISTR = __cur_str;
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
    if (__cur_str.equals("null")) { this.DATAVOLTURA = null; } else {
      this.DATAVOLTURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATARILEVAZIONE = null; } else {
      this.DATARILEVAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRATTAMENTO = null; } else {
      this.TRATTAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PUNTODISPACCIAMENTO = null; } else {
      this.PUNTODISPACCIAMENTO = __cur_str;
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
    if (__cur_str.equals("null")) { this.CODPRATATT = null; } else {
      this.CODPRATATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.KR = null; } else {
      this.KR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ERM = null; } else {
      this.ERM = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ERF1 = null; } else {
      this.ERF1 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ERF2 = null; } else {
      this.ERF2 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ERF3 = null; } else {
      this.ERF3 = new java.math.BigDecimal(__cur_str);
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
    if (__cur_str.equals("null")) { this.PIVA_DISTR = null; } else {
      this.PIVA_DISTR = __cur_str;
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
    if (__cur_str.equals("null")) { this.DATAVOLTURA = null; } else {
      this.DATAVOLTURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATARILEVAZIONE = null; } else {
      this.DATARILEVAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRATTAMENTO = null; } else {
      this.TRATTAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PUNTODISPACCIAMENTO = null; } else {
      this.PUNTODISPACCIAMENTO = __cur_str;
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
    if (__cur_str.equals("null")) { this.CODPRATATT = null; } else {
      this.CODPRATATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.KR = null; } else {
      this.KR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ERM = null; } else {
      this.ERM = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ERF1 = null; } else {
      this.ERF1 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ERF2 = null; } else {
      this.ERF2 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ERF3 = null; } else {
      this.ERF3 = new java.math.BigDecimal(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    tmpod_prt_tmo_mv o = (tmpod_prt_tmo_mv) super.clone();
    return o;
  }

  public void clone0(tmpod_prt_tmo_mv o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID", this.N_ID);
    __sqoop$field_map.put("N_ID_FILE", this.N_ID_FILE);
    __sqoop$field_map.put("N_ID_UDD", this.N_ID_UDD);
    __sqoop$field_map.put("N_ID_DISTR", this.N_ID_DISTR);
    __sqoop$field_map.put("PIVA_DISTR", this.PIVA_DISTR);
    __sqoop$field_map.put("TIPO_PRATICA", this.TIPO_PRATICA);
    __sqoop$field_map.put("COD_FLUSSO", this.COD_FLUSSO);
    __sqoop$field_map.put("CODICE_POD", this.CODICE_POD);
    __sqoop$field_map.put("ANNOMESE", this.ANNOMESE);
    __sqoop$field_map.put("DATAVOLTURA", this.DATAVOLTURA);
    __sqoop$field_map.put("DATARILEVAZIONE", this.DATARILEVAZIONE);
    __sqoop$field_map.put("TRATTAMENTO", this.TRATTAMENTO);
    __sqoop$field_map.put("PUNTODISPACCIAMENTO", this.PUNTODISPACCIAMENTO);
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
    __sqoop$field_map.put("CODPRATATT", this.CODPRATATT);
    __sqoop$field_map.put("KR", this.KR);
    __sqoop$field_map.put("ERM", this.ERM);
    __sqoop$field_map.put("ERF1", this.ERF1);
    __sqoop$field_map.put("ERF2", this.ERF2);
    __sqoop$field_map.put("ERF3", this.ERF3);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID", this.N_ID);
    __sqoop$field_map.put("N_ID_FILE", this.N_ID_FILE);
    __sqoop$field_map.put("N_ID_UDD", this.N_ID_UDD);
    __sqoop$field_map.put("N_ID_DISTR", this.N_ID_DISTR);
    __sqoop$field_map.put("PIVA_DISTR", this.PIVA_DISTR);
    __sqoop$field_map.put("TIPO_PRATICA", this.TIPO_PRATICA);
    __sqoop$field_map.put("COD_FLUSSO", this.COD_FLUSSO);
    __sqoop$field_map.put("CODICE_POD", this.CODICE_POD);
    __sqoop$field_map.put("ANNOMESE", this.ANNOMESE);
    __sqoop$field_map.put("DATAVOLTURA", this.DATAVOLTURA);
    __sqoop$field_map.put("DATARILEVAZIONE", this.DATARILEVAZIONE);
    __sqoop$field_map.put("TRATTAMENTO", this.TRATTAMENTO);
    __sqoop$field_map.put("PUNTODISPACCIAMENTO", this.PUNTODISPACCIAMENTO);
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
    __sqoop$field_map.put("CODPRATATT", this.CODPRATATT);
    __sqoop$field_map.put("KR", this.KR);
    __sqoop$field_map.put("ERM", this.ERM);
    __sqoop$field_map.put("ERF1", this.ERF1);
    __sqoop$field_map.put("ERF2", this.ERF2);
    __sqoop$field_map.put("ERF3", this.ERF3);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
