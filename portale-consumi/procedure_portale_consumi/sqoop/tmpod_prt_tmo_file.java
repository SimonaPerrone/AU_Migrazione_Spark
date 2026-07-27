// ORM class for table 'tmpod.prt_tmo_file'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 15:00:50 CEST 2019
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

public class tmpod_prt_tmo_file extends SqoopRecord  implements DBWritable, Writable {
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
    setters.put("N_ID_TMO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_TMO = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_NOME", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_NOME = (String)value;
      }
    });
    setters.put("T_TIPO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO = (String)value;
      }
    });
    setters.put("T_TIPO_TRACCIATO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO_TRACCIATO = (String)value;
      }
    });
    setters.put("N_ID_PADRE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_PADRE = (java.math.BigDecimal)value;
      }
    });
    setters.put("B_AMMISSIBILE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_AMMISSIBILE = (String)value;
      }
    });
    setters.put("T_STATO_FILE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_STATO_FILE = (String)value;
      }
    });
    setters.put("N_DIMENSIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_DIMENSIONE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_DIGEST", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_DIGEST = (String)value;
      }
    });
    setters.put("N_ID_OPERAZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_OPERAZIONE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_IDENT_FILE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_IDENT_FILE = (String)value;
      }
    });
    setters.put("D_UPLOAD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_UPLOAD = (String)value;
      }
    });
    setters.put("D_TRASMISSIONE_UDD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_TRASMISSIONE_UDD = (String)value;
      }
    });
    setters.put("N_ANNO_RIF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ANNO_RIF = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_MESE_RIF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_MESE_RIF = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_NUM_FILE_CONTENUTI", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_NUM_FILE_CONTENUTI = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_NUM_POD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_NUM_POD = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_CODICE_DP", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_DP = (String)value;
      }
    });
    setters.put("N_ID_CODICE_INAMMISSIBILITA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_CODICE_INAMMISSIBILITA = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_MOTIVAZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MOTIVAZIONE = (String)value;
      }
    });
    setters.put("PRESENTE_DB", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PRESENTE_DB = (String)value;
      }
    });
    setters.put("B_COPIATO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_COPIATO = (String)value;
      }
    });
  }
  public tmpod_prt_tmo_file() {
    init0();
  }
  private java.math.BigDecimal N_ID_FILE;
  public java.math.BigDecimal get_N_ID_FILE() {
    return N_ID_FILE;
  }
  public void set_N_ID_FILE(java.math.BigDecimal N_ID_FILE) {
    this.N_ID_FILE = N_ID_FILE;
  }
  public tmpod_prt_tmo_file with_N_ID_FILE(java.math.BigDecimal N_ID_FILE) {
    this.N_ID_FILE = N_ID_FILE;
    return this;
  }
  private java.math.BigDecimal N_ID_TMO;
  public java.math.BigDecimal get_N_ID_TMO() {
    return N_ID_TMO;
  }
  public void set_N_ID_TMO(java.math.BigDecimal N_ID_TMO) {
    this.N_ID_TMO = N_ID_TMO;
  }
  public tmpod_prt_tmo_file with_N_ID_TMO(java.math.BigDecimal N_ID_TMO) {
    this.N_ID_TMO = N_ID_TMO;
    return this;
  }
  private String T_NOME;
  public String get_T_NOME() {
    return T_NOME;
  }
  public void set_T_NOME(String T_NOME) {
    this.T_NOME = T_NOME;
  }
  public tmpod_prt_tmo_file with_T_NOME(String T_NOME) {
    this.T_NOME = T_NOME;
    return this;
  }
  private String T_TIPO;
  public String get_T_TIPO() {
    return T_TIPO;
  }
  public void set_T_TIPO(String T_TIPO) {
    this.T_TIPO = T_TIPO;
  }
  public tmpod_prt_tmo_file with_T_TIPO(String T_TIPO) {
    this.T_TIPO = T_TIPO;
    return this;
  }
  private String T_TIPO_TRACCIATO;
  public String get_T_TIPO_TRACCIATO() {
    return T_TIPO_TRACCIATO;
  }
  public void set_T_TIPO_TRACCIATO(String T_TIPO_TRACCIATO) {
    this.T_TIPO_TRACCIATO = T_TIPO_TRACCIATO;
  }
  public tmpod_prt_tmo_file with_T_TIPO_TRACCIATO(String T_TIPO_TRACCIATO) {
    this.T_TIPO_TRACCIATO = T_TIPO_TRACCIATO;
    return this;
  }
  private java.math.BigDecimal N_ID_PADRE;
  public java.math.BigDecimal get_N_ID_PADRE() {
    return N_ID_PADRE;
  }
  public void set_N_ID_PADRE(java.math.BigDecimal N_ID_PADRE) {
    this.N_ID_PADRE = N_ID_PADRE;
  }
  public tmpod_prt_tmo_file with_N_ID_PADRE(java.math.BigDecimal N_ID_PADRE) {
    this.N_ID_PADRE = N_ID_PADRE;
    return this;
  }
  private String B_AMMISSIBILE;
  public String get_B_AMMISSIBILE() {
    return B_AMMISSIBILE;
  }
  public void set_B_AMMISSIBILE(String B_AMMISSIBILE) {
    this.B_AMMISSIBILE = B_AMMISSIBILE;
  }
  public tmpod_prt_tmo_file with_B_AMMISSIBILE(String B_AMMISSIBILE) {
    this.B_AMMISSIBILE = B_AMMISSIBILE;
    return this;
  }
  private String T_STATO_FILE;
  public String get_T_STATO_FILE() {
    return T_STATO_FILE;
  }
  public void set_T_STATO_FILE(String T_STATO_FILE) {
    this.T_STATO_FILE = T_STATO_FILE;
  }
  public tmpod_prt_tmo_file with_T_STATO_FILE(String T_STATO_FILE) {
    this.T_STATO_FILE = T_STATO_FILE;
    return this;
  }
  private java.math.BigDecimal N_DIMENSIONE;
  public java.math.BigDecimal get_N_DIMENSIONE() {
    return N_DIMENSIONE;
  }
  public void set_N_DIMENSIONE(java.math.BigDecimal N_DIMENSIONE) {
    this.N_DIMENSIONE = N_DIMENSIONE;
  }
  public tmpod_prt_tmo_file with_N_DIMENSIONE(java.math.BigDecimal N_DIMENSIONE) {
    this.N_DIMENSIONE = N_DIMENSIONE;
    return this;
  }
  private String T_DIGEST;
  public String get_T_DIGEST() {
    return T_DIGEST;
  }
  public void set_T_DIGEST(String T_DIGEST) {
    this.T_DIGEST = T_DIGEST;
  }
  public tmpod_prt_tmo_file with_T_DIGEST(String T_DIGEST) {
    this.T_DIGEST = T_DIGEST;
    return this;
  }
  private java.math.BigDecimal N_ID_OPERAZIONE;
  public java.math.BigDecimal get_N_ID_OPERAZIONE() {
    return N_ID_OPERAZIONE;
  }
  public void set_N_ID_OPERAZIONE(java.math.BigDecimal N_ID_OPERAZIONE) {
    this.N_ID_OPERAZIONE = N_ID_OPERAZIONE;
  }
  public tmpod_prt_tmo_file with_N_ID_OPERAZIONE(java.math.BigDecimal N_ID_OPERAZIONE) {
    this.N_ID_OPERAZIONE = N_ID_OPERAZIONE;
    return this;
  }
  private String T_IDENT_FILE;
  public String get_T_IDENT_FILE() {
    return T_IDENT_FILE;
  }
  public void set_T_IDENT_FILE(String T_IDENT_FILE) {
    this.T_IDENT_FILE = T_IDENT_FILE;
  }
  public tmpod_prt_tmo_file with_T_IDENT_FILE(String T_IDENT_FILE) {
    this.T_IDENT_FILE = T_IDENT_FILE;
    return this;
  }
  private String D_UPLOAD;
  public String get_D_UPLOAD() {
    return D_UPLOAD;
  }
  public void set_D_UPLOAD(String D_UPLOAD) {
    this.D_UPLOAD = D_UPLOAD;
  }
  public tmpod_prt_tmo_file with_D_UPLOAD(String D_UPLOAD) {
    this.D_UPLOAD = D_UPLOAD;
    return this;
  }
  private String D_TRASMISSIONE_UDD;
  public String get_D_TRASMISSIONE_UDD() {
    return D_TRASMISSIONE_UDD;
  }
  public void set_D_TRASMISSIONE_UDD(String D_TRASMISSIONE_UDD) {
    this.D_TRASMISSIONE_UDD = D_TRASMISSIONE_UDD;
  }
  public tmpod_prt_tmo_file with_D_TRASMISSIONE_UDD(String D_TRASMISSIONE_UDD) {
    this.D_TRASMISSIONE_UDD = D_TRASMISSIONE_UDD;
    return this;
  }
  private java.math.BigDecimal N_ANNO_RIF;
  public java.math.BigDecimal get_N_ANNO_RIF() {
    return N_ANNO_RIF;
  }
  public void set_N_ANNO_RIF(java.math.BigDecimal N_ANNO_RIF) {
    this.N_ANNO_RIF = N_ANNO_RIF;
  }
  public tmpod_prt_tmo_file with_N_ANNO_RIF(java.math.BigDecimal N_ANNO_RIF) {
    this.N_ANNO_RIF = N_ANNO_RIF;
    return this;
  }
  private java.math.BigDecimal N_MESE_RIF;
  public java.math.BigDecimal get_N_MESE_RIF() {
    return N_MESE_RIF;
  }
  public void set_N_MESE_RIF(java.math.BigDecimal N_MESE_RIF) {
    this.N_MESE_RIF = N_MESE_RIF;
  }
  public tmpod_prt_tmo_file with_N_MESE_RIF(java.math.BigDecimal N_MESE_RIF) {
    this.N_MESE_RIF = N_MESE_RIF;
    return this;
  }
  private java.math.BigDecimal N_NUM_FILE_CONTENUTI;
  public java.math.BigDecimal get_N_NUM_FILE_CONTENUTI() {
    return N_NUM_FILE_CONTENUTI;
  }
  public void set_N_NUM_FILE_CONTENUTI(java.math.BigDecimal N_NUM_FILE_CONTENUTI) {
    this.N_NUM_FILE_CONTENUTI = N_NUM_FILE_CONTENUTI;
  }
  public tmpod_prt_tmo_file with_N_NUM_FILE_CONTENUTI(java.math.BigDecimal N_NUM_FILE_CONTENUTI) {
    this.N_NUM_FILE_CONTENUTI = N_NUM_FILE_CONTENUTI;
    return this;
  }
  private java.math.BigDecimal N_NUM_POD;
  public java.math.BigDecimal get_N_NUM_POD() {
    return N_NUM_POD;
  }
  public void set_N_NUM_POD(java.math.BigDecimal N_NUM_POD) {
    this.N_NUM_POD = N_NUM_POD;
  }
  public tmpod_prt_tmo_file with_N_NUM_POD(java.math.BigDecimal N_NUM_POD) {
    this.N_NUM_POD = N_NUM_POD;
    return this;
  }
  private String T_CODICE_DP;
  public String get_T_CODICE_DP() {
    return T_CODICE_DP;
  }
  public void set_T_CODICE_DP(String T_CODICE_DP) {
    this.T_CODICE_DP = T_CODICE_DP;
  }
  public tmpod_prt_tmo_file with_T_CODICE_DP(String T_CODICE_DP) {
    this.T_CODICE_DP = T_CODICE_DP;
    return this;
  }
  private java.math.BigDecimal N_ID_CODICE_INAMMISSIBILITA;
  public java.math.BigDecimal get_N_ID_CODICE_INAMMISSIBILITA() {
    return N_ID_CODICE_INAMMISSIBILITA;
  }
  public void set_N_ID_CODICE_INAMMISSIBILITA(java.math.BigDecimal N_ID_CODICE_INAMMISSIBILITA) {
    this.N_ID_CODICE_INAMMISSIBILITA = N_ID_CODICE_INAMMISSIBILITA;
  }
  public tmpod_prt_tmo_file with_N_ID_CODICE_INAMMISSIBILITA(java.math.BigDecimal N_ID_CODICE_INAMMISSIBILITA) {
    this.N_ID_CODICE_INAMMISSIBILITA = N_ID_CODICE_INAMMISSIBILITA;
    return this;
  }
  private String T_MOTIVAZIONE;
  public String get_T_MOTIVAZIONE() {
    return T_MOTIVAZIONE;
  }
  public void set_T_MOTIVAZIONE(String T_MOTIVAZIONE) {
    this.T_MOTIVAZIONE = T_MOTIVAZIONE;
  }
  public tmpod_prt_tmo_file with_T_MOTIVAZIONE(String T_MOTIVAZIONE) {
    this.T_MOTIVAZIONE = T_MOTIVAZIONE;
    return this;
  }
  private String PRESENTE_DB;
  public String get_PRESENTE_DB() {
    return PRESENTE_DB;
  }
  public void set_PRESENTE_DB(String PRESENTE_DB) {
    this.PRESENTE_DB = PRESENTE_DB;
  }
  public tmpod_prt_tmo_file with_PRESENTE_DB(String PRESENTE_DB) {
    this.PRESENTE_DB = PRESENTE_DB;
    return this;
  }
  private String B_COPIATO;
  public String get_B_COPIATO() {
    return B_COPIATO;
  }
  public void set_B_COPIATO(String B_COPIATO) {
    this.B_COPIATO = B_COPIATO;
  }
  public tmpod_prt_tmo_file with_B_COPIATO(String B_COPIATO) {
    this.B_COPIATO = B_COPIATO;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof tmpod_prt_tmo_file)) {
      return false;
    }
    tmpod_prt_tmo_file that = (tmpod_prt_tmo_file) o;
    boolean equal = true;
    equal = equal && (this.N_ID_FILE == null ? that.N_ID_FILE == null : this.N_ID_FILE.equals(that.N_ID_FILE));
    equal = equal && (this.N_ID_TMO == null ? that.N_ID_TMO == null : this.N_ID_TMO.equals(that.N_ID_TMO));
    equal = equal && (this.T_NOME == null ? that.T_NOME == null : this.T_NOME.equals(that.T_NOME));
    equal = equal && (this.T_TIPO == null ? that.T_TIPO == null : this.T_TIPO.equals(that.T_TIPO));
    equal = equal && (this.T_TIPO_TRACCIATO == null ? that.T_TIPO_TRACCIATO == null : this.T_TIPO_TRACCIATO.equals(that.T_TIPO_TRACCIATO));
    equal = equal && (this.N_ID_PADRE == null ? that.N_ID_PADRE == null : this.N_ID_PADRE.equals(that.N_ID_PADRE));
    equal = equal && (this.B_AMMISSIBILE == null ? that.B_AMMISSIBILE == null : this.B_AMMISSIBILE.equals(that.B_AMMISSIBILE));
    equal = equal && (this.T_STATO_FILE == null ? that.T_STATO_FILE == null : this.T_STATO_FILE.equals(that.T_STATO_FILE));
    equal = equal && (this.N_DIMENSIONE == null ? that.N_DIMENSIONE == null : this.N_DIMENSIONE.equals(that.N_DIMENSIONE));
    equal = equal && (this.T_DIGEST == null ? that.T_DIGEST == null : this.T_DIGEST.equals(that.T_DIGEST));
    equal = equal && (this.N_ID_OPERAZIONE == null ? that.N_ID_OPERAZIONE == null : this.N_ID_OPERAZIONE.equals(that.N_ID_OPERAZIONE));
    equal = equal && (this.T_IDENT_FILE == null ? that.T_IDENT_FILE == null : this.T_IDENT_FILE.equals(that.T_IDENT_FILE));
    equal = equal && (this.D_UPLOAD == null ? that.D_UPLOAD == null : this.D_UPLOAD.equals(that.D_UPLOAD));
    equal = equal && (this.D_TRASMISSIONE_UDD == null ? that.D_TRASMISSIONE_UDD == null : this.D_TRASMISSIONE_UDD.equals(that.D_TRASMISSIONE_UDD));
    equal = equal && (this.N_ANNO_RIF == null ? that.N_ANNO_RIF == null : this.N_ANNO_RIF.equals(that.N_ANNO_RIF));
    equal = equal && (this.N_MESE_RIF == null ? that.N_MESE_RIF == null : this.N_MESE_RIF.equals(that.N_MESE_RIF));
    equal = equal && (this.N_NUM_FILE_CONTENUTI == null ? that.N_NUM_FILE_CONTENUTI == null : this.N_NUM_FILE_CONTENUTI.equals(that.N_NUM_FILE_CONTENUTI));
    equal = equal && (this.N_NUM_POD == null ? that.N_NUM_POD == null : this.N_NUM_POD.equals(that.N_NUM_POD));
    equal = equal && (this.T_CODICE_DP == null ? that.T_CODICE_DP == null : this.T_CODICE_DP.equals(that.T_CODICE_DP));
    equal = equal && (this.N_ID_CODICE_INAMMISSIBILITA == null ? that.N_ID_CODICE_INAMMISSIBILITA == null : this.N_ID_CODICE_INAMMISSIBILITA.equals(that.N_ID_CODICE_INAMMISSIBILITA));
    equal = equal && (this.T_MOTIVAZIONE == null ? that.T_MOTIVAZIONE == null : this.T_MOTIVAZIONE.equals(that.T_MOTIVAZIONE));
    equal = equal && (this.PRESENTE_DB == null ? that.PRESENTE_DB == null : this.PRESENTE_DB.equals(that.PRESENTE_DB));
    equal = equal && (this.B_COPIATO == null ? that.B_COPIATO == null : this.B_COPIATO.equals(that.B_COPIATO));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof tmpod_prt_tmo_file)) {
      return false;
    }
    tmpod_prt_tmo_file that = (tmpod_prt_tmo_file) o;
    boolean equal = true;
    equal = equal && (this.N_ID_FILE == null ? that.N_ID_FILE == null : this.N_ID_FILE.equals(that.N_ID_FILE));
    equal = equal && (this.N_ID_TMO == null ? that.N_ID_TMO == null : this.N_ID_TMO.equals(that.N_ID_TMO));
    equal = equal && (this.T_NOME == null ? that.T_NOME == null : this.T_NOME.equals(that.T_NOME));
    equal = equal && (this.T_TIPO == null ? that.T_TIPO == null : this.T_TIPO.equals(that.T_TIPO));
    equal = equal && (this.T_TIPO_TRACCIATO == null ? that.T_TIPO_TRACCIATO == null : this.T_TIPO_TRACCIATO.equals(that.T_TIPO_TRACCIATO));
    equal = equal && (this.N_ID_PADRE == null ? that.N_ID_PADRE == null : this.N_ID_PADRE.equals(that.N_ID_PADRE));
    equal = equal && (this.B_AMMISSIBILE == null ? that.B_AMMISSIBILE == null : this.B_AMMISSIBILE.equals(that.B_AMMISSIBILE));
    equal = equal && (this.T_STATO_FILE == null ? that.T_STATO_FILE == null : this.T_STATO_FILE.equals(that.T_STATO_FILE));
    equal = equal && (this.N_DIMENSIONE == null ? that.N_DIMENSIONE == null : this.N_DIMENSIONE.equals(that.N_DIMENSIONE));
    equal = equal && (this.T_DIGEST == null ? that.T_DIGEST == null : this.T_DIGEST.equals(that.T_DIGEST));
    equal = equal && (this.N_ID_OPERAZIONE == null ? that.N_ID_OPERAZIONE == null : this.N_ID_OPERAZIONE.equals(that.N_ID_OPERAZIONE));
    equal = equal && (this.T_IDENT_FILE == null ? that.T_IDENT_FILE == null : this.T_IDENT_FILE.equals(that.T_IDENT_FILE));
    equal = equal && (this.D_UPLOAD == null ? that.D_UPLOAD == null : this.D_UPLOAD.equals(that.D_UPLOAD));
    equal = equal && (this.D_TRASMISSIONE_UDD == null ? that.D_TRASMISSIONE_UDD == null : this.D_TRASMISSIONE_UDD.equals(that.D_TRASMISSIONE_UDD));
    equal = equal && (this.N_ANNO_RIF == null ? that.N_ANNO_RIF == null : this.N_ANNO_RIF.equals(that.N_ANNO_RIF));
    equal = equal && (this.N_MESE_RIF == null ? that.N_MESE_RIF == null : this.N_MESE_RIF.equals(that.N_MESE_RIF));
    equal = equal && (this.N_NUM_FILE_CONTENUTI == null ? that.N_NUM_FILE_CONTENUTI == null : this.N_NUM_FILE_CONTENUTI.equals(that.N_NUM_FILE_CONTENUTI));
    equal = equal && (this.N_NUM_POD == null ? that.N_NUM_POD == null : this.N_NUM_POD.equals(that.N_NUM_POD));
    equal = equal && (this.T_CODICE_DP == null ? that.T_CODICE_DP == null : this.T_CODICE_DP.equals(that.T_CODICE_DP));
    equal = equal && (this.N_ID_CODICE_INAMMISSIBILITA == null ? that.N_ID_CODICE_INAMMISSIBILITA == null : this.N_ID_CODICE_INAMMISSIBILITA.equals(that.N_ID_CODICE_INAMMISSIBILITA));
    equal = equal && (this.T_MOTIVAZIONE == null ? that.T_MOTIVAZIONE == null : this.T_MOTIVAZIONE.equals(that.T_MOTIVAZIONE));
    equal = equal && (this.PRESENTE_DB == null ? that.PRESENTE_DB == null : this.PRESENTE_DB.equals(that.PRESENTE_DB));
    equal = equal && (this.B_COPIATO == null ? that.B_COPIATO == null : this.B_COPIATO.equals(that.B_COPIATO));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_FILE = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_TMO = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_NOME = JdbcWritableBridge.readString(3, __dbResults);
    this.T_TIPO = JdbcWritableBridge.readString(4, __dbResults);
    this.T_TIPO_TRACCIATO = JdbcWritableBridge.readString(5, __dbResults);
    this.N_ID_PADRE = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.B_AMMISSIBILE = JdbcWritableBridge.readString(7, __dbResults);
    this.T_STATO_FILE = JdbcWritableBridge.readString(8, __dbResults);
    this.N_DIMENSIONE = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.T_DIGEST = JdbcWritableBridge.readString(10, __dbResults);
    this.N_ID_OPERAZIONE = JdbcWritableBridge.readBigDecimal(11, __dbResults);
    this.T_IDENT_FILE = JdbcWritableBridge.readString(12, __dbResults);
    this.D_UPLOAD = JdbcWritableBridge.readString(13, __dbResults);
    this.D_TRASMISSIONE_UDD = JdbcWritableBridge.readString(14, __dbResults);
    this.N_ANNO_RIF = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.N_MESE_RIF = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.N_NUM_FILE_CONTENUTI = JdbcWritableBridge.readBigDecimal(17, __dbResults);
    this.N_NUM_POD = JdbcWritableBridge.readBigDecimal(18, __dbResults);
    this.T_CODICE_DP = JdbcWritableBridge.readString(19, __dbResults);
    this.N_ID_CODICE_INAMMISSIBILITA = JdbcWritableBridge.readBigDecimal(20, __dbResults);
    this.T_MOTIVAZIONE = JdbcWritableBridge.readString(21, __dbResults);
    this.PRESENTE_DB = JdbcWritableBridge.readString(22, __dbResults);
    this.B_COPIATO = JdbcWritableBridge.readString(23, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_FILE = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_TMO = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_NOME = JdbcWritableBridge.readString(3, __dbResults);
    this.T_TIPO = JdbcWritableBridge.readString(4, __dbResults);
    this.T_TIPO_TRACCIATO = JdbcWritableBridge.readString(5, __dbResults);
    this.N_ID_PADRE = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.B_AMMISSIBILE = JdbcWritableBridge.readString(7, __dbResults);
    this.T_STATO_FILE = JdbcWritableBridge.readString(8, __dbResults);
    this.N_DIMENSIONE = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.T_DIGEST = JdbcWritableBridge.readString(10, __dbResults);
    this.N_ID_OPERAZIONE = JdbcWritableBridge.readBigDecimal(11, __dbResults);
    this.T_IDENT_FILE = JdbcWritableBridge.readString(12, __dbResults);
    this.D_UPLOAD = JdbcWritableBridge.readString(13, __dbResults);
    this.D_TRASMISSIONE_UDD = JdbcWritableBridge.readString(14, __dbResults);
    this.N_ANNO_RIF = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.N_MESE_RIF = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.N_NUM_FILE_CONTENUTI = JdbcWritableBridge.readBigDecimal(17, __dbResults);
    this.N_NUM_POD = JdbcWritableBridge.readBigDecimal(18, __dbResults);
    this.T_CODICE_DP = JdbcWritableBridge.readString(19, __dbResults);
    this.N_ID_CODICE_INAMMISSIBILITA = JdbcWritableBridge.readBigDecimal(20, __dbResults);
    this.T_MOTIVAZIONE = JdbcWritableBridge.readString(21, __dbResults);
    this.PRESENTE_DB = JdbcWritableBridge.readString(22, __dbResults);
    this.B_COPIATO = JdbcWritableBridge.readString(23, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_TMO, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_NOME, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_TRACCIATO, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PADRE, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_AMMISSIBILE, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO_FILE, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_DIMENSIONE, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_DIGEST, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_OPERAZIONE, 11 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_IDENT_FILE, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_UPLOAD, 13 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_TRASMISSIONE_UDD, 14 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ANNO_RIF, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_MESE_RIF, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_FILE_CONTENUTI, 17 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_POD, 18 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_DP, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CODICE_INAMMISSIBILITA, 20 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_MOTIVAZIONE, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PRESENTE_DB, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_COPIATO, 23 + __off, 12, __dbStmt);
    return 23;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_FILE, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TMO, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_NOME, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_TRACCIATO, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PADRE, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_AMMISSIBILE, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO_FILE, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_DIMENSIONE, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_DIGEST, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_OPERAZIONE, 11 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_IDENT_FILE, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_UPLOAD, 13 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_TRASMISSIONE_UDD, 14 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ANNO_RIF, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_MESE_RIF, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_FILE_CONTENUTI, 17 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_POD, 18 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_DP, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CODICE_INAMMISSIBILITA, 20 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_MOTIVAZIONE, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PRESENTE_DB, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_COPIATO, 23 + __off, 12, __dbStmt);
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
        this.N_ID_TMO = null;
    } else {
    this.N_ID_TMO = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_NOME = null;
    } else {
    this.T_NOME = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO = null;
    } else {
    this.T_TIPO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO_TRACCIATO = null;
    } else {
    this.T_TIPO_TRACCIATO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_PADRE = null;
    } else {
    this.N_ID_PADRE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_AMMISSIBILE = null;
    } else {
    this.B_AMMISSIBILE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_STATO_FILE = null;
    } else {
    this.T_STATO_FILE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_DIMENSIONE = null;
    } else {
    this.N_DIMENSIONE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_DIGEST = null;
    } else {
    this.T_DIGEST = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_OPERAZIONE = null;
    } else {
    this.N_ID_OPERAZIONE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_IDENT_FILE = null;
    } else {
    this.T_IDENT_FILE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_UPLOAD = null;
    } else {
    this.D_UPLOAD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_TRASMISSIONE_UDD = null;
    } else {
    this.D_TRASMISSIONE_UDD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ANNO_RIF = null;
    } else {
    this.N_ANNO_RIF = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_MESE_RIF = null;
    } else {
    this.N_MESE_RIF = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_NUM_FILE_CONTENUTI = null;
    } else {
    this.N_NUM_FILE_CONTENUTI = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_NUM_POD = null;
    } else {
    this.N_NUM_POD = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_DP = null;
    } else {
    this.T_CODICE_DP = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_CODICE_INAMMISSIBILITA = null;
    } else {
    this.N_ID_CODICE_INAMMISSIBILITA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MOTIVAZIONE = null;
    } else {
    this.T_MOTIVAZIONE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PRESENTE_DB = null;
    } else {
    this.PRESENTE_DB = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_COPIATO = null;
    } else {
    this.B_COPIATO = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_FILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FILE, __dataOut);
    }
    if (null == this.N_ID_TMO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_TMO, __dataOut);
    }
    if (null == this.T_NOME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOME);
    }
    if (null == this.T_TIPO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO);
    }
    if (null == this.T_TIPO_TRACCIATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_TRACCIATO);
    }
    if (null == this.N_ID_PADRE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PADRE, __dataOut);
    }
    if (null == this.B_AMMISSIBILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_AMMISSIBILE);
    }
    if (null == this.T_STATO_FILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_STATO_FILE);
    }
    if (null == this.N_DIMENSIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_DIMENSIONE, __dataOut);
    }
    if (null == this.T_DIGEST) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DIGEST);
    }
    if (null == this.N_ID_OPERAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_OPERAZIONE, __dataOut);
    }
    if (null == this.T_IDENT_FILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_IDENT_FILE);
    }
    if (null == this.D_UPLOAD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_UPLOAD);
    }
    if (null == this.D_TRASMISSIONE_UDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_TRASMISSIONE_UDD);
    }
    if (null == this.N_ANNO_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ANNO_RIF, __dataOut);
    }
    if (null == this.N_MESE_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_MESE_RIF, __dataOut);
    }
    if (null == this.N_NUM_FILE_CONTENUTI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_FILE_CONTENUTI, __dataOut);
    }
    if (null == this.N_NUM_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_POD, __dataOut);
    }
    if (null == this.T_CODICE_DP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_DP);
    }
    if (null == this.N_ID_CODICE_INAMMISSIBILITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CODICE_INAMMISSIBILITA, __dataOut);
    }
    if (null == this.T_MOTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MOTIVAZIONE);
    }
    if (null == this.PRESENTE_DB) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PRESENTE_DB);
    }
    if (null == this.B_COPIATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_COPIATO);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_FILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FILE, __dataOut);
    }
    if (null == this.N_ID_TMO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_TMO, __dataOut);
    }
    if (null == this.T_NOME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOME);
    }
    if (null == this.T_TIPO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO);
    }
    if (null == this.T_TIPO_TRACCIATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_TRACCIATO);
    }
    if (null == this.N_ID_PADRE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PADRE, __dataOut);
    }
    if (null == this.B_AMMISSIBILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_AMMISSIBILE);
    }
    if (null == this.T_STATO_FILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_STATO_FILE);
    }
    if (null == this.N_DIMENSIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_DIMENSIONE, __dataOut);
    }
    if (null == this.T_DIGEST) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DIGEST);
    }
    if (null == this.N_ID_OPERAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_OPERAZIONE, __dataOut);
    }
    if (null == this.T_IDENT_FILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_IDENT_FILE);
    }
    if (null == this.D_UPLOAD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_UPLOAD);
    }
    if (null == this.D_TRASMISSIONE_UDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_TRASMISSIONE_UDD);
    }
    if (null == this.N_ANNO_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ANNO_RIF, __dataOut);
    }
    if (null == this.N_MESE_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_MESE_RIF, __dataOut);
    }
    if (null == this.N_NUM_FILE_CONTENUTI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_FILE_CONTENUTI, __dataOut);
    }
    if (null == this.N_NUM_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_POD, __dataOut);
    }
    if (null == this.T_CODICE_DP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_DP);
    }
    if (null == this.N_ID_CODICE_INAMMISSIBILITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CODICE_INAMMISSIBILITA, __dataOut);
    }
    if (null == this.T_MOTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MOTIVAZIONE);
    }
    if (null == this.PRESENTE_DB) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PRESENTE_DB);
    }
    if (null == this.B_COPIATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_COPIATO);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TMO==null?"":N_ID_TMO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOME==null?"":T_NOME, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO==null?"":T_TIPO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_TRACCIATO==null?"":T_TIPO_TRACCIATO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PADRE==null?"":N_ID_PADRE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_AMMISSIBILE==null?"":B_AMMISSIBILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO_FILE==null?"":T_STATO_FILE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_DIMENSIONE==null?"":N_DIMENSIONE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DIGEST==null?"":T_DIGEST, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_OPERAZIONE==null?"":N_ID_OPERAZIONE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_IDENT_FILE==null?"":T_IDENT_FILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_UPLOAD==null?"":D_UPLOAD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_TRASMISSIONE_UDD==null?"":D_TRASMISSIONE_UDD, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ANNO_RIF==null?"":N_ANNO_RIF.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_MESE_RIF==null?"":N_MESE_RIF.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_FILE_CONTENUTI==null?"":N_NUM_FILE_CONTENUTI.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_POD==null?"":N_NUM_POD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_DP==null?"":T_CODICE_DP, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CODICE_INAMMISSIBILITA==null?"":N_ID_CODICE_INAMMISSIBILITA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MOTIVAZIONE==null?"":T_MOTIVAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PRESENTE_DB==null?"":PRESENTE_DB, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_COPIATO==null?"":B_COPIATO, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_FILE==null?"":N_ID_FILE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TMO==null?"":N_ID_TMO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOME==null?"":T_NOME, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO==null?"":T_TIPO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_TRACCIATO==null?"":T_TIPO_TRACCIATO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PADRE==null?"":N_ID_PADRE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_AMMISSIBILE==null?"":B_AMMISSIBILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO_FILE==null?"":T_STATO_FILE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_DIMENSIONE==null?"":N_DIMENSIONE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DIGEST==null?"":T_DIGEST, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_OPERAZIONE==null?"":N_ID_OPERAZIONE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_IDENT_FILE==null?"":T_IDENT_FILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_UPLOAD==null?"":D_UPLOAD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_TRASMISSIONE_UDD==null?"":D_TRASMISSIONE_UDD, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ANNO_RIF==null?"":N_ANNO_RIF.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_MESE_RIF==null?"":N_MESE_RIF.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_FILE_CONTENUTI==null?"":N_NUM_FILE_CONTENUTI.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_POD==null?"":N_NUM_POD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_DP==null?"":T_CODICE_DP, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CODICE_INAMMISSIBILITA==null?"":N_ID_CODICE_INAMMISSIBILITA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MOTIVAZIONE==null?"":T_MOTIVAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PRESENTE_DB==null?"":PRESENTE_DB, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_COPIATO==null?"":B_COPIATO, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_TMO = null; } else {
      this.N_ID_TMO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOME = null; } else {
      this.T_NOME = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO = null; } else {
      this.T_TIPO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_TRACCIATO = null; } else {
      this.T_TIPO_TRACCIATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PADRE = null; } else {
      this.N_ID_PADRE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_AMMISSIBILE = null; } else {
      this.B_AMMISSIBILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_STATO_FILE = null; } else {
      this.T_STATO_FILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_DIMENSIONE = null; } else {
      this.N_DIMENSIONE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DIGEST = null; } else {
      this.T_DIGEST = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_OPERAZIONE = null; } else {
      this.N_ID_OPERAZIONE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_IDENT_FILE = null; } else {
      this.T_IDENT_FILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_UPLOAD = null; } else {
      this.D_UPLOAD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_TRASMISSIONE_UDD = null; } else {
      this.D_TRASMISSIONE_UDD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ANNO_RIF = null; } else {
      this.N_ANNO_RIF = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_MESE_RIF = null; } else {
      this.N_MESE_RIF = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_FILE_CONTENUTI = null; } else {
      this.N_NUM_FILE_CONTENUTI = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_POD = null; } else {
      this.N_NUM_POD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_DP = null; } else {
      this.T_CODICE_DP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CODICE_INAMMISSIBILITA = null; } else {
      this.N_ID_CODICE_INAMMISSIBILITA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MOTIVAZIONE = null; } else {
      this.T_MOTIVAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PRESENTE_DB = null; } else {
      this.PRESENTE_DB = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_COPIATO = null; } else {
      this.B_COPIATO = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_TMO = null; } else {
      this.N_ID_TMO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOME = null; } else {
      this.T_NOME = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO = null; } else {
      this.T_TIPO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_TRACCIATO = null; } else {
      this.T_TIPO_TRACCIATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PADRE = null; } else {
      this.N_ID_PADRE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_AMMISSIBILE = null; } else {
      this.B_AMMISSIBILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_STATO_FILE = null; } else {
      this.T_STATO_FILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_DIMENSIONE = null; } else {
      this.N_DIMENSIONE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DIGEST = null; } else {
      this.T_DIGEST = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_OPERAZIONE = null; } else {
      this.N_ID_OPERAZIONE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_IDENT_FILE = null; } else {
      this.T_IDENT_FILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_UPLOAD = null; } else {
      this.D_UPLOAD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_TRASMISSIONE_UDD = null; } else {
      this.D_TRASMISSIONE_UDD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ANNO_RIF = null; } else {
      this.N_ANNO_RIF = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_MESE_RIF = null; } else {
      this.N_MESE_RIF = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_FILE_CONTENUTI = null; } else {
      this.N_NUM_FILE_CONTENUTI = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_POD = null; } else {
      this.N_NUM_POD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_DP = null; } else {
      this.T_CODICE_DP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CODICE_INAMMISSIBILITA = null; } else {
      this.N_ID_CODICE_INAMMISSIBILITA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MOTIVAZIONE = null; } else {
      this.T_MOTIVAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PRESENTE_DB = null; } else {
      this.PRESENTE_DB = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_COPIATO = null; } else {
      this.B_COPIATO = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    tmpod_prt_tmo_file o = (tmpod_prt_tmo_file) super.clone();
    return o;
  }

  public void clone0(tmpod_prt_tmo_file o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_FILE", this.N_ID_FILE);
    __sqoop$field_map.put("N_ID_TMO", this.N_ID_TMO);
    __sqoop$field_map.put("T_NOME", this.T_NOME);
    __sqoop$field_map.put("T_TIPO", this.T_TIPO);
    __sqoop$field_map.put("T_TIPO_TRACCIATO", this.T_TIPO_TRACCIATO);
    __sqoop$field_map.put("N_ID_PADRE", this.N_ID_PADRE);
    __sqoop$field_map.put("B_AMMISSIBILE", this.B_AMMISSIBILE);
    __sqoop$field_map.put("T_STATO_FILE", this.T_STATO_FILE);
    __sqoop$field_map.put("N_DIMENSIONE", this.N_DIMENSIONE);
    __sqoop$field_map.put("T_DIGEST", this.T_DIGEST);
    __sqoop$field_map.put("N_ID_OPERAZIONE", this.N_ID_OPERAZIONE);
    __sqoop$field_map.put("T_IDENT_FILE", this.T_IDENT_FILE);
    __sqoop$field_map.put("D_UPLOAD", this.D_UPLOAD);
    __sqoop$field_map.put("D_TRASMISSIONE_UDD", this.D_TRASMISSIONE_UDD);
    __sqoop$field_map.put("N_ANNO_RIF", this.N_ANNO_RIF);
    __sqoop$field_map.put("N_MESE_RIF", this.N_MESE_RIF);
    __sqoop$field_map.put("N_NUM_FILE_CONTENUTI", this.N_NUM_FILE_CONTENUTI);
    __sqoop$field_map.put("N_NUM_POD", this.N_NUM_POD);
    __sqoop$field_map.put("T_CODICE_DP", this.T_CODICE_DP);
    __sqoop$field_map.put("N_ID_CODICE_INAMMISSIBILITA", this.N_ID_CODICE_INAMMISSIBILITA);
    __sqoop$field_map.put("T_MOTIVAZIONE", this.T_MOTIVAZIONE);
    __sqoop$field_map.put("PRESENTE_DB", this.PRESENTE_DB);
    __sqoop$field_map.put("B_COPIATO", this.B_COPIATO);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_FILE", this.N_ID_FILE);
    __sqoop$field_map.put("N_ID_TMO", this.N_ID_TMO);
    __sqoop$field_map.put("T_NOME", this.T_NOME);
    __sqoop$field_map.put("T_TIPO", this.T_TIPO);
    __sqoop$field_map.put("T_TIPO_TRACCIATO", this.T_TIPO_TRACCIATO);
    __sqoop$field_map.put("N_ID_PADRE", this.N_ID_PADRE);
    __sqoop$field_map.put("B_AMMISSIBILE", this.B_AMMISSIBILE);
    __sqoop$field_map.put("T_STATO_FILE", this.T_STATO_FILE);
    __sqoop$field_map.put("N_DIMENSIONE", this.N_DIMENSIONE);
    __sqoop$field_map.put("T_DIGEST", this.T_DIGEST);
    __sqoop$field_map.put("N_ID_OPERAZIONE", this.N_ID_OPERAZIONE);
    __sqoop$field_map.put("T_IDENT_FILE", this.T_IDENT_FILE);
    __sqoop$field_map.put("D_UPLOAD", this.D_UPLOAD);
    __sqoop$field_map.put("D_TRASMISSIONE_UDD", this.D_TRASMISSIONE_UDD);
    __sqoop$field_map.put("N_ANNO_RIF", this.N_ANNO_RIF);
    __sqoop$field_map.put("N_MESE_RIF", this.N_MESE_RIF);
    __sqoop$field_map.put("N_NUM_FILE_CONTENUTI", this.N_NUM_FILE_CONTENUTI);
    __sqoop$field_map.put("N_NUM_POD", this.N_NUM_POD);
    __sqoop$field_map.put("T_CODICE_DP", this.T_CODICE_DP);
    __sqoop$field_map.put("N_ID_CODICE_INAMMISSIBILITA", this.N_ID_CODICE_INAMMISSIBILITA);
    __sqoop$field_map.put("T_MOTIVAZIONE", this.T_MOTIVAZIONE);
    __sqoop$field_map.put("PRESENTE_DB", this.PRESENTE_DB);
    __sqoop$field_map.put("B_COPIATO", this.B_COPIATO);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
