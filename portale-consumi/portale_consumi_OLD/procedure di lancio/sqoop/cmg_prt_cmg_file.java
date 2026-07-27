// ORM class for table 'cmg.prt_cmg_file'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 08:09:09 CEST 2019
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

public class cmg_prt_cmg_file extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_FILE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_FILE = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_CMG", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_CMG = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_NOME_FILE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_NOME_FILE = (String)value;
      }
    });
    setters.put("T_TIPO_FILE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO_FILE = (String)value;
      }
    });
    setters.put("T_STATO_FILE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_STATO_FILE = (String)value;
      }
    });
    setters.put("D_DATA_CARICAMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_CARICAMENTO = (String)value;
      }
    });
    setters.put("T_ANNO_CARICAMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ANNO_CARICAMENTO = (String)value;
      }
    });
    setters.put("T_MESE_CARICAMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MESE_CARICAMENTO = (String)value;
      }
    });
    setters.put("T_GIORNO_CARICAMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_GIORNO_CARICAMENTO = (String)value;
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
    setters.put("T_DIGEST", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_DIGEST = (String)value;
      }
    });
    setters.put("N_DIMENSIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_DIMENSIONE = (java.math.BigDecimal)value;
      }
    });
    setters.put("B_INVIATO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_INVIATO = (String)value;
      }
    });
    setters.put("T_PIVA_DISTRIBUTORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PIVA_DISTRIBUTORE = (String)value;
      }
    });
    setters.put("T_PIVA_UDD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PIVA_UDD = (String)value;
      }
    });
    setters.put("T_TIPO_SERVIZIO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO_SERVIZIO = (String)value;
      }
    });
    setters.put("T_TIPO_FLUSSO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO_FLUSSO = (String)value;
      }
    });
    setters.put("PRESENTE_DB", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PRESENTE_DB = (String)value;
      }
    });
    setters.put("T_ANNOMESE_RIF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ANNOMESE_RIF = (String)value;
      }
    });
    setters.put("N_ID_PADRE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_PADRE = (java.math.BigDecimal)value;
      }
    });
    setters.put("B_VERIFICATO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_VERIFICATO = (String)value;
      }
    });
  }
  public cmg_prt_cmg_file() {
    init0();
  }
  private java.math.BigDecimal N_ID_FILE;
  public java.math.BigDecimal get_N_ID_FILE() {
    return N_ID_FILE;
  }
  public void set_N_ID_FILE(java.math.BigDecimal N_ID_FILE) {
    this.N_ID_FILE = N_ID_FILE;
  }
  public cmg_prt_cmg_file with_N_ID_FILE(java.math.BigDecimal N_ID_FILE) {
    this.N_ID_FILE = N_ID_FILE;
    return this;
  }
  private java.math.BigDecimal N_ID_CMG;
  public java.math.BigDecimal get_N_ID_CMG() {
    return N_ID_CMG;
  }
  public void set_N_ID_CMG(java.math.BigDecimal N_ID_CMG) {
    this.N_ID_CMG = N_ID_CMG;
  }
  public cmg_prt_cmg_file with_N_ID_CMG(java.math.BigDecimal N_ID_CMG) {
    this.N_ID_CMG = N_ID_CMG;
    return this;
  }
  private String T_NOME_FILE;
  public String get_T_NOME_FILE() {
    return T_NOME_FILE;
  }
  public void set_T_NOME_FILE(String T_NOME_FILE) {
    this.T_NOME_FILE = T_NOME_FILE;
  }
  public cmg_prt_cmg_file with_T_NOME_FILE(String T_NOME_FILE) {
    this.T_NOME_FILE = T_NOME_FILE;
    return this;
  }
  private String T_TIPO_FILE;
  public String get_T_TIPO_FILE() {
    return T_TIPO_FILE;
  }
  public void set_T_TIPO_FILE(String T_TIPO_FILE) {
    this.T_TIPO_FILE = T_TIPO_FILE;
  }
  public cmg_prt_cmg_file with_T_TIPO_FILE(String T_TIPO_FILE) {
    this.T_TIPO_FILE = T_TIPO_FILE;
    return this;
  }
  private String T_STATO_FILE;
  public String get_T_STATO_FILE() {
    return T_STATO_FILE;
  }
  public void set_T_STATO_FILE(String T_STATO_FILE) {
    this.T_STATO_FILE = T_STATO_FILE;
  }
  public cmg_prt_cmg_file with_T_STATO_FILE(String T_STATO_FILE) {
    this.T_STATO_FILE = T_STATO_FILE;
    return this;
  }
  private String D_DATA_CARICAMENTO;
  public String get_D_DATA_CARICAMENTO() {
    return D_DATA_CARICAMENTO;
  }
  public void set_D_DATA_CARICAMENTO(String D_DATA_CARICAMENTO) {
    this.D_DATA_CARICAMENTO = D_DATA_CARICAMENTO;
  }
  public cmg_prt_cmg_file with_D_DATA_CARICAMENTO(String D_DATA_CARICAMENTO) {
    this.D_DATA_CARICAMENTO = D_DATA_CARICAMENTO;
    return this;
  }
  private String T_ANNO_CARICAMENTO;
  public String get_T_ANNO_CARICAMENTO() {
    return T_ANNO_CARICAMENTO;
  }
  public void set_T_ANNO_CARICAMENTO(String T_ANNO_CARICAMENTO) {
    this.T_ANNO_CARICAMENTO = T_ANNO_CARICAMENTO;
  }
  public cmg_prt_cmg_file with_T_ANNO_CARICAMENTO(String T_ANNO_CARICAMENTO) {
    this.T_ANNO_CARICAMENTO = T_ANNO_CARICAMENTO;
    return this;
  }
  private String T_MESE_CARICAMENTO;
  public String get_T_MESE_CARICAMENTO() {
    return T_MESE_CARICAMENTO;
  }
  public void set_T_MESE_CARICAMENTO(String T_MESE_CARICAMENTO) {
    this.T_MESE_CARICAMENTO = T_MESE_CARICAMENTO;
  }
  public cmg_prt_cmg_file with_T_MESE_CARICAMENTO(String T_MESE_CARICAMENTO) {
    this.T_MESE_CARICAMENTO = T_MESE_CARICAMENTO;
    return this;
  }
  private String T_GIORNO_CARICAMENTO;
  public String get_T_GIORNO_CARICAMENTO() {
    return T_GIORNO_CARICAMENTO;
  }
  public void set_T_GIORNO_CARICAMENTO(String T_GIORNO_CARICAMENTO) {
    this.T_GIORNO_CARICAMENTO = T_GIORNO_CARICAMENTO;
  }
  public cmg_prt_cmg_file with_T_GIORNO_CARICAMENTO(String T_GIORNO_CARICAMENTO) {
    this.T_GIORNO_CARICAMENTO = T_GIORNO_CARICAMENTO;
    return this;
  }
  private String B_AMMISSIBILE;
  public String get_B_AMMISSIBILE() {
    return B_AMMISSIBILE;
  }
  public void set_B_AMMISSIBILE(String B_AMMISSIBILE) {
    this.B_AMMISSIBILE = B_AMMISSIBILE;
  }
  public cmg_prt_cmg_file with_B_AMMISSIBILE(String B_AMMISSIBILE) {
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
  public cmg_prt_cmg_file with_T_COD_CAUSALE(String T_COD_CAUSALE) {
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
  public cmg_prt_cmg_file with_T_MOTIVAZIONE(String T_MOTIVAZIONE) {
    this.T_MOTIVAZIONE = T_MOTIVAZIONE;
    return this;
  }
  private String T_DIGEST;
  public String get_T_DIGEST() {
    return T_DIGEST;
  }
  public void set_T_DIGEST(String T_DIGEST) {
    this.T_DIGEST = T_DIGEST;
  }
  public cmg_prt_cmg_file with_T_DIGEST(String T_DIGEST) {
    this.T_DIGEST = T_DIGEST;
    return this;
  }
  private java.math.BigDecimal N_DIMENSIONE;
  public java.math.BigDecimal get_N_DIMENSIONE() {
    return N_DIMENSIONE;
  }
  public void set_N_DIMENSIONE(java.math.BigDecimal N_DIMENSIONE) {
    this.N_DIMENSIONE = N_DIMENSIONE;
  }
  public cmg_prt_cmg_file with_N_DIMENSIONE(java.math.BigDecimal N_DIMENSIONE) {
    this.N_DIMENSIONE = N_DIMENSIONE;
    return this;
  }
  private String B_INVIATO;
  public String get_B_INVIATO() {
    return B_INVIATO;
  }
  public void set_B_INVIATO(String B_INVIATO) {
    this.B_INVIATO = B_INVIATO;
  }
  public cmg_prt_cmg_file with_B_INVIATO(String B_INVIATO) {
    this.B_INVIATO = B_INVIATO;
    return this;
  }
  private String T_PIVA_DISTRIBUTORE;
  public String get_T_PIVA_DISTRIBUTORE() {
    return T_PIVA_DISTRIBUTORE;
  }
  public void set_T_PIVA_DISTRIBUTORE(String T_PIVA_DISTRIBUTORE) {
    this.T_PIVA_DISTRIBUTORE = T_PIVA_DISTRIBUTORE;
  }
  public cmg_prt_cmg_file with_T_PIVA_DISTRIBUTORE(String T_PIVA_DISTRIBUTORE) {
    this.T_PIVA_DISTRIBUTORE = T_PIVA_DISTRIBUTORE;
    return this;
  }
  private String T_PIVA_UDD;
  public String get_T_PIVA_UDD() {
    return T_PIVA_UDD;
  }
  public void set_T_PIVA_UDD(String T_PIVA_UDD) {
    this.T_PIVA_UDD = T_PIVA_UDD;
  }
  public cmg_prt_cmg_file with_T_PIVA_UDD(String T_PIVA_UDD) {
    this.T_PIVA_UDD = T_PIVA_UDD;
    return this;
  }
  private String T_TIPO_SERVIZIO;
  public String get_T_TIPO_SERVIZIO() {
    return T_TIPO_SERVIZIO;
  }
  public void set_T_TIPO_SERVIZIO(String T_TIPO_SERVIZIO) {
    this.T_TIPO_SERVIZIO = T_TIPO_SERVIZIO;
  }
  public cmg_prt_cmg_file with_T_TIPO_SERVIZIO(String T_TIPO_SERVIZIO) {
    this.T_TIPO_SERVIZIO = T_TIPO_SERVIZIO;
    return this;
  }
  private String T_TIPO_FLUSSO;
  public String get_T_TIPO_FLUSSO() {
    return T_TIPO_FLUSSO;
  }
  public void set_T_TIPO_FLUSSO(String T_TIPO_FLUSSO) {
    this.T_TIPO_FLUSSO = T_TIPO_FLUSSO;
  }
  public cmg_prt_cmg_file with_T_TIPO_FLUSSO(String T_TIPO_FLUSSO) {
    this.T_TIPO_FLUSSO = T_TIPO_FLUSSO;
    return this;
  }
  private String PRESENTE_DB;
  public String get_PRESENTE_DB() {
    return PRESENTE_DB;
  }
  public void set_PRESENTE_DB(String PRESENTE_DB) {
    this.PRESENTE_DB = PRESENTE_DB;
  }
  public cmg_prt_cmg_file with_PRESENTE_DB(String PRESENTE_DB) {
    this.PRESENTE_DB = PRESENTE_DB;
    return this;
  }
  private String T_ANNOMESE_RIF;
  public String get_T_ANNOMESE_RIF() {
    return T_ANNOMESE_RIF;
  }
  public void set_T_ANNOMESE_RIF(String T_ANNOMESE_RIF) {
    this.T_ANNOMESE_RIF = T_ANNOMESE_RIF;
  }
  public cmg_prt_cmg_file with_T_ANNOMESE_RIF(String T_ANNOMESE_RIF) {
    this.T_ANNOMESE_RIF = T_ANNOMESE_RIF;
    return this;
  }
  private java.math.BigDecimal N_ID_PADRE;
  public java.math.BigDecimal get_N_ID_PADRE() {
    return N_ID_PADRE;
  }
  public void set_N_ID_PADRE(java.math.BigDecimal N_ID_PADRE) {
    this.N_ID_PADRE = N_ID_PADRE;
  }
  public cmg_prt_cmg_file with_N_ID_PADRE(java.math.BigDecimal N_ID_PADRE) {
    this.N_ID_PADRE = N_ID_PADRE;
    return this;
  }
  private String B_VERIFICATO;
  public String get_B_VERIFICATO() {
    return B_VERIFICATO;
  }
  public void set_B_VERIFICATO(String B_VERIFICATO) {
    this.B_VERIFICATO = B_VERIFICATO;
  }
  public cmg_prt_cmg_file with_B_VERIFICATO(String B_VERIFICATO) {
    this.B_VERIFICATO = B_VERIFICATO;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof cmg_prt_cmg_file)) {
      return false;
    }
    cmg_prt_cmg_file that = (cmg_prt_cmg_file) o;
    boolean equal = true;
    equal = equal && (this.N_ID_FILE == null ? that.N_ID_FILE == null : this.N_ID_FILE.equals(that.N_ID_FILE));
    equal = equal && (this.N_ID_CMG == null ? that.N_ID_CMG == null : this.N_ID_CMG.equals(that.N_ID_CMG));
    equal = equal && (this.T_NOME_FILE == null ? that.T_NOME_FILE == null : this.T_NOME_FILE.equals(that.T_NOME_FILE));
    equal = equal && (this.T_TIPO_FILE == null ? that.T_TIPO_FILE == null : this.T_TIPO_FILE.equals(that.T_TIPO_FILE));
    equal = equal && (this.T_STATO_FILE == null ? that.T_STATO_FILE == null : this.T_STATO_FILE.equals(that.T_STATO_FILE));
    equal = equal && (this.D_DATA_CARICAMENTO == null ? that.D_DATA_CARICAMENTO == null : this.D_DATA_CARICAMENTO.equals(that.D_DATA_CARICAMENTO));
    equal = equal && (this.T_ANNO_CARICAMENTO == null ? that.T_ANNO_CARICAMENTO == null : this.T_ANNO_CARICAMENTO.equals(that.T_ANNO_CARICAMENTO));
    equal = equal && (this.T_MESE_CARICAMENTO == null ? that.T_MESE_CARICAMENTO == null : this.T_MESE_CARICAMENTO.equals(that.T_MESE_CARICAMENTO));
    equal = equal && (this.T_GIORNO_CARICAMENTO == null ? that.T_GIORNO_CARICAMENTO == null : this.T_GIORNO_CARICAMENTO.equals(that.T_GIORNO_CARICAMENTO));
    equal = equal && (this.B_AMMISSIBILE == null ? that.B_AMMISSIBILE == null : this.B_AMMISSIBILE.equals(that.B_AMMISSIBILE));
    equal = equal && (this.T_COD_CAUSALE == null ? that.T_COD_CAUSALE == null : this.T_COD_CAUSALE.equals(that.T_COD_CAUSALE));
    equal = equal && (this.T_MOTIVAZIONE == null ? that.T_MOTIVAZIONE == null : this.T_MOTIVAZIONE.equals(that.T_MOTIVAZIONE));
    equal = equal && (this.T_DIGEST == null ? that.T_DIGEST == null : this.T_DIGEST.equals(that.T_DIGEST));
    equal = equal && (this.N_DIMENSIONE == null ? that.N_DIMENSIONE == null : this.N_DIMENSIONE.equals(that.N_DIMENSIONE));
    equal = equal && (this.B_INVIATO == null ? that.B_INVIATO == null : this.B_INVIATO.equals(that.B_INVIATO));
    equal = equal && (this.T_PIVA_DISTRIBUTORE == null ? that.T_PIVA_DISTRIBUTORE == null : this.T_PIVA_DISTRIBUTORE.equals(that.T_PIVA_DISTRIBUTORE));
    equal = equal && (this.T_PIVA_UDD == null ? that.T_PIVA_UDD == null : this.T_PIVA_UDD.equals(that.T_PIVA_UDD));
    equal = equal && (this.T_TIPO_SERVIZIO == null ? that.T_TIPO_SERVIZIO == null : this.T_TIPO_SERVIZIO.equals(that.T_TIPO_SERVIZIO));
    equal = equal && (this.T_TIPO_FLUSSO == null ? that.T_TIPO_FLUSSO == null : this.T_TIPO_FLUSSO.equals(that.T_TIPO_FLUSSO));
    equal = equal && (this.PRESENTE_DB == null ? that.PRESENTE_DB == null : this.PRESENTE_DB.equals(that.PRESENTE_DB));
    equal = equal && (this.T_ANNOMESE_RIF == null ? that.T_ANNOMESE_RIF == null : this.T_ANNOMESE_RIF.equals(that.T_ANNOMESE_RIF));
    equal = equal && (this.N_ID_PADRE == null ? that.N_ID_PADRE == null : this.N_ID_PADRE.equals(that.N_ID_PADRE));
    equal = equal && (this.B_VERIFICATO == null ? that.B_VERIFICATO == null : this.B_VERIFICATO.equals(that.B_VERIFICATO));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof cmg_prt_cmg_file)) {
      return false;
    }
    cmg_prt_cmg_file that = (cmg_prt_cmg_file) o;
    boolean equal = true;
    equal = equal && (this.N_ID_FILE == null ? that.N_ID_FILE == null : this.N_ID_FILE.equals(that.N_ID_FILE));
    equal = equal && (this.N_ID_CMG == null ? that.N_ID_CMG == null : this.N_ID_CMG.equals(that.N_ID_CMG));
    equal = equal && (this.T_NOME_FILE == null ? that.T_NOME_FILE == null : this.T_NOME_FILE.equals(that.T_NOME_FILE));
    equal = equal && (this.T_TIPO_FILE == null ? that.T_TIPO_FILE == null : this.T_TIPO_FILE.equals(that.T_TIPO_FILE));
    equal = equal && (this.T_STATO_FILE == null ? that.T_STATO_FILE == null : this.T_STATO_FILE.equals(that.T_STATO_FILE));
    equal = equal && (this.D_DATA_CARICAMENTO == null ? that.D_DATA_CARICAMENTO == null : this.D_DATA_CARICAMENTO.equals(that.D_DATA_CARICAMENTO));
    equal = equal && (this.T_ANNO_CARICAMENTO == null ? that.T_ANNO_CARICAMENTO == null : this.T_ANNO_CARICAMENTO.equals(that.T_ANNO_CARICAMENTO));
    equal = equal && (this.T_MESE_CARICAMENTO == null ? that.T_MESE_CARICAMENTO == null : this.T_MESE_CARICAMENTO.equals(that.T_MESE_CARICAMENTO));
    equal = equal && (this.T_GIORNO_CARICAMENTO == null ? that.T_GIORNO_CARICAMENTO == null : this.T_GIORNO_CARICAMENTO.equals(that.T_GIORNO_CARICAMENTO));
    equal = equal && (this.B_AMMISSIBILE == null ? that.B_AMMISSIBILE == null : this.B_AMMISSIBILE.equals(that.B_AMMISSIBILE));
    equal = equal && (this.T_COD_CAUSALE == null ? that.T_COD_CAUSALE == null : this.T_COD_CAUSALE.equals(that.T_COD_CAUSALE));
    equal = equal && (this.T_MOTIVAZIONE == null ? that.T_MOTIVAZIONE == null : this.T_MOTIVAZIONE.equals(that.T_MOTIVAZIONE));
    equal = equal && (this.T_DIGEST == null ? that.T_DIGEST == null : this.T_DIGEST.equals(that.T_DIGEST));
    equal = equal && (this.N_DIMENSIONE == null ? that.N_DIMENSIONE == null : this.N_DIMENSIONE.equals(that.N_DIMENSIONE));
    equal = equal && (this.B_INVIATO == null ? that.B_INVIATO == null : this.B_INVIATO.equals(that.B_INVIATO));
    equal = equal && (this.T_PIVA_DISTRIBUTORE == null ? that.T_PIVA_DISTRIBUTORE == null : this.T_PIVA_DISTRIBUTORE.equals(that.T_PIVA_DISTRIBUTORE));
    equal = equal && (this.T_PIVA_UDD == null ? that.T_PIVA_UDD == null : this.T_PIVA_UDD.equals(that.T_PIVA_UDD));
    equal = equal && (this.T_TIPO_SERVIZIO == null ? that.T_TIPO_SERVIZIO == null : this.T_TIPO_SERVIZIO.equals(that.T_TIPO_SERVIZIO));
    equal = equal && (this.T_TIPO_FLUSSO == null ? that.T_TIPO_FLUSSO == null : this.T_TIPO_FLUSSO.equals(that.T_TIPO_FLUSSO));
    equal = equal && (this.PRESENTE_DB == null ? that.PRESENTE_DB == null : this.PRESENTE_DB.equals(that.PRESENTE_DB));
    equal = equal && (this.T_ANNOMESE_RIF == null ? that.T_ANNOMESE_RIF == null : this.T_ANNOMESE_RIF.equals(that.T_ANNOMESE_RIF));
    equal = equal && (this.N_ID_PADRE == null ? that.N_ID_PADRE == null : this.N_ID_PADRE.equals(that.N_ID_PADRE));
    equal = equal && (this.B_VERIFICATO == null ? that.B_VERIFICATO == null : this.B_VERIFICATO.equals(that.B_VERIFICATO));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_FILE = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_CMG = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_NOME_FILE = JdbcWritableBridge.readString(3, __dbResults);
    this.T_TIPO_FILE = JdbcWritableBridge.readString(4, __dbResults);
    this.T_STATO_FILE = JdbcWritableBridge.readString(5, __dbResults);
    this.D_DATA_CARICAMENTO = JdbcWritableBridge.readString(6, __dbResults);
    this.T_ANNO_CARICAMENTO = JdbcWritableBridge.readString(7, __dbResults);
    this.T_MESE_CARICAMENTO = JdbcWritableBridge.readString(8, __dbResults);
    this.T_GIORNO_CARICAMENTO = JdbcWritableBridge.readString(9, __dbResults);
    this.B_AMMISSIBILE = JdbcWritableBridge.readString(10, __dbResults);
    this.T_COD_CAUSALE = JdbcWritableBridge.readString(11, __dbResults);
    this.T_MOTIVAZIONE = JdbcWritableBridge.readString(12, __dbResults);
    this.T_DIGEST = JdbcWritableBridge.readString(13, __dbResults);
    this.N_DIMENSIONE = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.B_INVIATO = JdbcWritableBridge.readString(15, __dbResults);
    this.T_PIVA_DISTRIBUTORE = JdbcWritableBridge.readString(16, __dbResults);
    this.T_PIVA_UDD = JdbcWritableBridge.readString(17, __dbResults);
    this.T_TIPO_SERVIZIO = JdbcWritableBridge.readString(18, __dbResults);
    this.T_TIPO_FLUSSO = JdbcWritableBridge.readString(19, __dbResults);
    this.PRESENTE_DB = JdbcWritableBridge.readString(20, __dbResults);
    this.T_ANNOMESE_RIF = JdbcWritableBridge.readString(21, __dbResults);
    this.N_ID_PADRE = JdbcWritableBridge.readBigDecimal(22, __dbResults);
    this.B_VERIFICATO = JdbcWritableBridge.readString(23, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_FILE = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_CMG = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_NOME_FILE = JdbcWritableBridge.readString(3, __dbResults);
    this.T_TIPO_FILE = JdbcWritableBridge.readString(4, __dbResults);
    this.T_STATO_FILE = JdbcWritableBridge.readString(5, __dbResults);
    this.D_DATA_CARICAMENTO = JdbcWritableBridge.readString(6, __dbResults);
    this.T_ANNO_CARICAMENTO = JdbcWritableBridge.readString(7, __dbResults);
    this.T_MESE_CARICAMENTO = JdbcWritableBridge.readString(8, __dbResults);
    this.T_GIORNO_CARICAMENTO = JdbcWritableBridge.readString(9, __dbResults);
    this.B_AMMISSIBILE = JdbcWritableBridge.readString(10, __dbResults);
    this.T_COD_CAUSALE = JdbcWritableBridge.readString(11, __dbResults);
    this.T_MOTIVAZIONE = JdbcWritableBridge.readString(12, __dbResults);
    this.T_DIGEST = JdbcWritableBridge.readString(13, __dbResults);
    this.N_DIMENSIONE = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.B_INVIATO = JdbcWritableBridge.readString(15, __dbResults);
    this.T_PIVA_DISTRIBUTORE = JdbcWritableBridge.readString(16, __dbResults);
    this.T_PIVA_UDD = JdbcWritableBridge.readString(17, __dbResults);
    this.T_TIPO_SERVIZIO = JdbcWritableBridge.readString(18, __dbResults);
    this.T_TIPO_FLUSSO = JdbcWritableBridge.readString(19, __dbResults);
    this.PRESENTE_DB = JdbcWritableBridge.readString(20, __dbResults);
    this.T_ANNOMESE_RIF = JdbcWritableBridge.readString(21, __dbResults);
    this.N_ID_PADRE = JdbcWritableBridge.readBigDecimal(22, __dbResults);
    this.B_VERIFICATO = JdbcWritableBridge.readString(23, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_FILE, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CMG, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_NOME_FILE, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_FILE, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO_FILE, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_CARICAMENTO, 6 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO_CARICAMENTO, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MESE_CARICAMENTO, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_GIORNO_CARICAMENTO, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_AMMISSIBILE, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CAUSALE, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MOTIVAZIONE, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DIGEST, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_DIMENSIONE, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_INVIATO, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA_DISTRIBUTORE, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA_UDD, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_SERVIZIO, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_FLUSSO, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PRESENTE_DB, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNOMESE_RIF, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PADRE, 22 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_VERIFICATO, 23 + __off, 12, __dbStmt);
    return 23;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_FILE, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CMG, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_NOME_FILE, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_FILE, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO_FILE, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_CARICAMENTO, 6 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO_CARICAMENTO, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MESE_CARICAMENTO, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_GIORNO_CARICAMENTO, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_AMMISSIBILE, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CAUSALE, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MOTIVAZIONE, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DIGEST, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_DIMENSIONE, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_INVIATO, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA_DISTRIBUTORE, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA_UDD, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_SERVIZIO, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_FLUSSO, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PRESENTE_DB, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNOMESE_RIF, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PADRE, 22 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_VERIFICATO, 23 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_FILE = null;
    } else {
    this.N_ID_FILE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_CMG = null;
    } else {
    this.N_ID_CMG = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_NOME_FILE = null;
    } else {
    this.T_NOME_FILE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO_FILE = null;
    } else {
    this.T_TIPO_FILE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_STATO_FILE = null;
    } else {
    this.T_STATO_FILE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_CARICAMENTO = null;
    } else {
    this.D_DATA_CARICAMENTO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ANNO_CARICAMENTO = null;
    } else {
    this.T_ANNO_CARICAMENTO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MESE_CARICAMENTO = null;
    } else {
    this.T_MESE_CARICAMENTO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_GIORNO_CARICAMENTO = null;
    } else {
    this.T_GIORNO_CARICAMENTO = Text.readString(__dataIn);
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
        this.T_DIGEST = null;
    } else {
    this.T_DIGEST = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_DIMENSIONE = null;
    } else {
    this.N_DIMENSIONE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_INVIATO = null;
    } else {
    this.B_INVIATO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PIVA_DISTRIBUTORE = null;
    } else {
    this.T_PIVA_DISTRIBUTORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PIVA_UDD = null;
    } else {
    this.T_PIVA_UDD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO_SERVIZIO = null;
    } else {
    this.T_TIPO_SERVIZIO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO_FLUSSO = null;
    } else {
    this.T_TIPO_FLUSSO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PRESENTE_DB = null;
    } else {
    this.PRESENTE_DB = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ANNOMESE_RIF = null;
    } else {
    this.T_ANNOMESE_RIF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_PADRE = null;
    } else {
    this.N_ID_PADRE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_VERIFICATO = null;
    } else {
    this.B_VERIFICATO = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_FILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FILE, __dataOut);
    }
    if (null == this.N_ID_CMG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CMG, __dataOut);
    }
    if (null == this.T_NOME_FILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOME_FILE);
    }
    if (null == this.T_TIPO_FILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_FILE);
    }
    if (null == this.T_STATO_FILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_STATO_FILE);
    }
    if (null == this.D_DATA_CARICAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_CARICAMENTO);
    }
    if (null == this.T_ANNO_CARICAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO_CARICAMENTO);
    }
    if (null == this.T_MESE_CARICAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MESE_CARICAMENTO);
    }
    if (null == this.T_GIORNO_CARICAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_GIORNO_CARICAMENTO);
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
    if (null == this.T_DIGEST) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DIGEST);
    }
    if (null == this.N_DIMENSIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_DIMENSIONE, __dataOut);
    }
    if (null == this.B_INVIATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_INVIATO);
    }
    if (null == this.T_PIVA_DISTRIBUTORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA_DISTRIBUTORE);
    }
    if (null == this.T_PIVA_UDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA_UDD);
    }
    if (null == this.T_TIPO_SERVIZIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_SERVIZIO);
    }
    if (null == this.T_TIPO_FLUSSO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_FLUSSO);
    }
    if (null == this.PRESENTE_DB) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PRESENTE_DB);
    }
    if (null == this.T_ANNOMESE_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNOMESE_RIF);
    }
    if (null == this.N_ID_PADRE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PADRE, __dataOut);
    }
    if (null == this.B_VERIFICATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_VERIFICATO);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_FILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FILE, __dataOut);
    }
    if (null == this.N_ID_CMG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CMG, __dataOut);
    }
    if (null == this.T_NOME_FILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOME_FILE);
    }
    if (null == this.T_TIPO_FILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_FILE);
    }
    if (null == this.T_STATO_FILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_STATO_FILE);
    }
    if (null == this.D_DATA_CARICAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_CARICAMENTO);
    }
    if (null == this.T_ANNO_CARICAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO_CARICAMENTO);
    }
    if (null == this.T_MESE_CARICAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MESE_CARICAMENTO);
    }
    if (null == this.T_GIORNO_CARICAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_GIORNO_CARICAMENTO);
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
    if (null == this.T_DIGEST) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DIGEST);
    }
    if (null == this.N_DIMENSIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_DIMENSIONE, __dataOut);
    }
    if (null == this.B_INVIATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_INVIATO);
    }
    if (null == this.T_PIVA_DISTRIBUTORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA_DISTRIBUTORE);
    }
    if (null == this.T_PIVA_UDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA_UDD);
    }
    if (null == this.T_TIPO_SERVIZIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_SERVIZIO);
    }
    if (null == this.T_TIPO_FLUSSO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_FLUSSO);
    }
    if (null == this.PRESENTE_DB) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PRESENTE_DB);
    }
    if (null == this.T_ANNOMESE_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNOMESE_RIF);
    }
    if (null == this.N_ID_PADRE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PADRE, __dataOut);
    }
    if (null == this.B_VERIFICATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_VERIFICATO);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_FILE==null?"":N_ID_FILE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CMG==null?"":N_ID_CMG.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOME_FILE==null?"":T_NOME_FILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_FILE==null?"":T_TIPO_FILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO_FILE==null?"":T_STATO_FILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_CARICAMENTO==null?"":D_DATA_CARICAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO_CARICAMENTO==null?"":T_ANNO_CARICAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MESE_CARICAMENTO==null?"":T_MESE_CARICAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_GIORNO_CARICAMENTO==null?"":T_GIORNO_CARICAMENTO, " ", delimiters));
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DIGEST==null?"":T_DIGEST, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_DIMENSIONE==null?"":N_DIMENSIONE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_INVIATO==null?"":B_INVIATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA_DISTRIBUTORE==null?"":T_PIVA_DISTRIBUTORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA_UDD==null?"":T_PIVA_UDD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_SERVIZIO==null?"":T_TIPO_SERVIZIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_FLUSSO==null?"":T_TIPO_FLUSSO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PRESENTE_DB==null?"":PRESENTE_DB, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNOMESE_RIF==null?"":T_ANNOMESE_RIF, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PADRE==null?"":N_ID_PADRE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_VERIFICATO==null?"":B_VERIFICATO, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_FILE==null?"":N_ID_FILE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CMG==null?"":N_ID_CMG.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOME_FILE==null?"":T_NOME_FILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_FILE==null?"":T_TIPO_FILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO_FILE==null?"":T_STATO_FILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_CARICAMENTO==null?"":D_DATA_CARICAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO_CARICAMENTO==null?"":T_ANNO_CARICAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MESE_CARICAMENTO==null?"":T_MESE_CARICAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_GIORNO_CARICAMENTO==null?"":T_GIORNO_CARICAMENTO, " ", delimiters));
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DIGEST==null?"":T_DIGEST, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_DIMENSIONE==null?"":N_DIMENSIONE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_INVIATO==null?"":B_INVIATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA_DISTRIBUTORE==null?"":T_PIVA_DISTRIBUTORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA_UDD==null?"":T_PIVA_UDD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_SERVIZIO==null?"":T_TIPO_SERVIZIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_FLUSSO==null?"":T_TIPO_FLUSSO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PRESENTE_DB==null?"":PRESENTE_DB, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNOMESE_RIF==null?"":T_ANNOMESE_RIF, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PADRE==null?"":N_ID_PADRE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_VERIFICATO==null?"":B_VERIFICATO, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_FILE = null; } else {
      this.N_ID_FILE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CMG = null; } else {
      this.N_ID_CMG = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOME_FILE = null; } else {
      this.T_NOME_FILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_FILE = null; } else {
      this.T_TIPO_FILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_STATO_FILE = null; } else {
      this.T_STATO_FILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_CARICAMENTO = null; } else {
      this.D_DATA_CARICAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO_CARICAMENTO = null; } else {
      this.T_ANNO_CARICAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MESE_CARICAMENTO = null; } else {
      this.T_MESE_CARICAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_GIORNO_CARICAMENTO = null; } else {
      this.T_GIORNO_CARICAMENTO = __cur_str;
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
    if (__cur_str.equals("null")) { this.T_DIGEST = null; } else {
      this.T_DIGEST = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_DIMENSIONE = null; } else {
      this.N_DIMENSIONE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_INVIATO = null; } else {
      this.B_INVIATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA_DISTRIBUTORE = null; } else {
      this.T_PIVA_DISTRIBUTORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA_UDD = null; } else {
      this.T_PIVA_UDD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_SERVIZIO = null; } else {
      this.T_TIPO_SERVIZIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_FLUSSO = null; } else {
      this.T_TIPO_FLUSSO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PRESENTE_DB = null; } else {
      this.PRESENTE_DB = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNOMESE_RIF = null; } else {
      this.T_ANNOMESE_RIF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PADRE = null; } else {
      this.N_ID_PADRE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_VERIFICATO = null; } else {
      this.B_VERIFICATO = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_FILE = null; } else {
      this.N_ID_FILE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CMG = null; } else {
      this.N_ID_CMG = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOME_FILE = null; } else {
      this.T_NOME_FILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_FILE = null; } else {
      this.T_TIPO_FILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_STATO_FILE = null; } else {
      this.T_STATO_FILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_CARICAMENTO = null; } else {
      this.D_DATA_CARICAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO_CARICAMENTO = null; } else {
      this.T_ANNO_CARICAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MESE_CARICAMENTO = null; } else {
      this.T_MESE_CARICAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_GIORNO_CARICAMENTO = null; } else {
      this.T_GIORNO_CARICAMENTO = __cur_str;
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
    if (__cur_str.equals("null")) { this.T_DIGEST = null; } else {
      this.T_DIGEST = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_DIMENSIONE = null; } else {
      this.N_DIMENSIONE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_INVIATO = null; } else {
      this.B_INVIATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA_DISTRIBUTORE = null; } else {
      this.T_PIVA_DISTRIBUTORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA_UDD = null; } else {
      this.T_PIVA_UDD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_SERVIZIO = null; } else {
      this.T_TIPO_SERVIZIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_FLUSSO = null; } else {
      this.T_TIPO_FLUSSO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PRESENTE_DB = null; } else {
      this.PRESENTE_DB = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNOMESE_RIF = null; } else {
      this.T_ANNOMESE_RIF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PADRE = null; } else {
      this.N_ID_PADRE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_VERIFICATO = null; } else {
      this.B_VERIFICATO = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    cmg_prt_cmg_file o = (cmg_prt_cmg_file) super.clone();
    return o;
  }

  public void clone0(cmg_prt_cmg_file o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_FILE", this.N_ID_FILE);
    __sqoop$field_map.put("N_ID_CMG", this.N_ID_CMG);
    __sqoop$field_map.put("T_NOME_FILE", this.T_NOME_FILE);
    __sqoop$field_map.put("T_TIPO_FILE", this.T_TIPO_FILE);
    __sqoop$field_map.put("T_STATO_FILE", this.T_STATO_FILE);
    __sqoop$field_map.put("D_DATA_CARICAMENTO", this.D_DATA_CARICAMENTO);
    __sqoop$field_map.put("T_ANNO_CARICAMENTO", this.T_ANNO_CARICAMENTO);
    __sqoop$field_map.put("T_MESE_CARICAMENTO", this.T_MESE_CARICAMENTO);
    __sqoop$field_map.put("T_GIORNO_CARICAMENTO", this.T_GIORNO_CARICAMENTO);
    __sqoop$field_map.put("B_AMMISSIBILE", this.B_AMMISSIBILE);
    __sqoop$field_map.put("T_COD_CAUSALE", this.T_COD_CAUSALE);
    __sqoop$field_map.put("T_MOTIVAZIONE", this.T_MOTIVAZIONE);
    __sqoop$field_map.put("T_DIGEST", this.T_DIGEST);
    __sqoop$field_map.put("N_DIMENSIONE", this.N_DIMENSIONE);
    __sqoop$field_map.put("B_INVIATO", this.B_INVIATO);
    __sqoop$field_map.put("T_PIVA_DISTRIBUTORE", this.T_PIVA_DISTRIBUTORE);
    __sqoop$field_map.put("T_PIVA_UDD", this.T_PIVA_UDD);
    __sqoop$field_map.put("T_TIPO_SERVIZIO", this.T_TIPO_SERVIZIO);
    __sqoop$field_map.put("T_TIPO_FLUSSO", this.T_TIPO_FLUSSO);
    __sqoop$field_map.put("PRESENTE_DB", this.PRESENTE_DB);
    __sqoop$field_map.put("T_ANNOMESE_RIF", this.T_ANNOMESE_RIF);
    __sqoop$field_map.put("N_ID_PADRE", this.N_ID_PADRE);
    __sqoop$field_map.put("B_VERIFICATO", this.B_VERIFICATO);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_FILE", this.N_ID_FILE);
    __sqoop$field_map.put("N_ID_CMG", this.N_ID_CMG);
    __sqoop$field_map.put("T_NOME_FILE", this.T_NOME_FILE);
    __sqoop$field_map.put("T_TIPO_FILE", this.T_TIPO_FILE);
    __sqoop$field_map.put("T_STATO_FILE", this.T_STATO_FILE);
    __sqoop$field_map.put("D_DATA_CARICAMENTO", this.D_DATA_CARICAMENTO);
    __sqoop$field_map.put("T_ANNO_CARICAMENTO", this.T_ANNO_CARICAMENTO);
    __sqoop$field_map.put("T_MESE_CARICAMENTO", this.T_MESE_CARICAMENTO);
    __sqoop$field_map.put("T_GIORNO_CARICAMENTO", this.T_GIORNO_CARICAMENTO);
    __sqoop$field_map.put("B_AMMISSIBILE", this.B_AMMISSIBILE);
    __sqoop$field_map.put("T_COD_CAUSALE", this.T_COD_CAUSALE);
    __sqoop$field_map.put("T_MOTIVAZIONE", this.T_MOTIVAZIONE);
    __sqoop$field_map.put("T_DIGEST", this.T_DIGEST);
    __sqoop$field_map.put("N_DIMENSIONE", this.N_DIMENSIONE);
    __sqoop$field_map.put("B_INVIATO", this.B_INVIATO);
    __sqoop$field_map.put("T_PIVA_DISTRIBUTORE", this.T_PIVA_DISTRIBUTORE);
    __sqoop$field_map.put("T_PIVA_UDD", this.T_PIVA_UDD);
    __sqoop$field_map.put("T_TIPO_SERVIZIO", this.T_TIPO_SERVIZIO);
    __sqoop$field_map.put("T_TIPO_FLUSSO", this.T_TIPO_FLUSSO);
    __sqoop$field_map.put("PRESENTE_DB", this.PRESENTE_DB);
    __sqoop$field_map.put("T_ANNOMESE_RIF", this.T_ANNOMESE_RIF);
    __sqoop$field_map.put("N_ID_PADRE", this.N_ID_PADRE);
    __sqoop$field_map.put("B_VERIFICATO", this.B_VERIFICATO);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
