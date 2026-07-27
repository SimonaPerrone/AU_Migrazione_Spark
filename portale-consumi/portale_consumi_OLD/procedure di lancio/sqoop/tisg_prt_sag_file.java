// ORM class for table 'tisg.prt_sag_file'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 14:39:03 CEST 2019
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

public class tisg_prt_sag_file extends SqoopRecord  implements DBWritable, Writable {
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
    setters.put("N_ID_SAG", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_SAG = (java.math.BigDecimal)value;
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
    setters.put("D_TRASMISSIONE_UTENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_TRASMISSIONE_UTENTE = (String)value;
      }
    });
    setters.put("N_NUM_FILE_CONTENUTI", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_NUM_FILE_CONTENUTI = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_RIGHE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_RIGHE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_MOTIVAZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MOTIVAZIONE = (String)value;
      }
    });
    setters.put("D_DOWNLOAD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DOWNLOAD = (String)value;
      }
    });
    setters.put("T_COD_INAMMISSIBILITA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COD_INAMMISSIBILITA = (String)value;
      }
    });
    setters.put("T_ANNO_RIF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ANNO_RIF = (String)value;
      }
    });
    setters.put("T_MESE_RIF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MESE_RIF = (String)value;
      }
    });
    setters.put("T_PIVA_UTENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PIVA_UTENTE = (String)value;
      }
    });
    setters.put("D_ELABORAZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_ELABORAZIONE = (String)value;
      }
    });
    setters.put("B_FILE_LOADED", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_FILE_LOADED = (String)value;
      }
    });
    setters.put("B_FILE_ELABORATO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_FILE_ELABORATO = (String)value;
      }
    });
  }
  public tisg_prt_sag_file() {
    init0();
  }
  private java.math.BigDecimal N_ID_FILE;
  public java.math.BigDecimal get_N_ID_FILE() {
    return N_ID_FILE;
  }
  public void set_N_ID_FILE(java.math.BigDecimal N_ID_FILE) {
    this.N_ID_FILE = N_ID_FILE;
  }
  public tisg_prt_sag_file with_N_ID_FILE(java.math.BigDecimal N_ID_FILE) {
    this.N_ID_FILE = N_ID_FILE;
    return this;
  }
  private java.math.BigDecimal N_ID_SAG;
  public java.math.BigDecimal get_N_ID_SAG() {
    return N_ID_SAG;
  }
  public void set_N_ID_SAG(java.math.BigDecimal N_ID_SAG) {
    this.N_ID_SAG = N_ID_SAG;
  }
  public tisg_prt_sag_file with_N_ID_SAG(java.math.BigDecimal N_ID_SAG) {
    this.N_ID_SAG = N_ID_SAG;
    return this;
  }
  private String T_NOME;
  public String get_T_NOME() {
    return T_NOME;
  }
  public void set_T_NOME(String T_NOME) {
    this.T_NOME = T_NOME;
  }
  public tisg_prt_sag_file with_T_NOME(String T_NOME) {
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
  public tisg_prt_sag_file with_T_TIPO(String T_TIPO) {
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
  public tisg_prt_sag_file with_T_TIPO_TRACCIATO(String T_TIPO_TRACCIATO) {
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
  public tisg_prt_sag_file with_N_ID_PADRE(java.math.BigDecimal N_ID_PADRE) {
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
  public tisg_prt_sag_file with_B_AMMISSIBILE(String B_AMMISSIBILE) {
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
  public tisg_prt_sag_file with_T_STATO_FILE(String T_STATO_FILE) {
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
  public tisg_prt_sag_file with_N_DIMENSIONE(java.math.BigDecimal N_DIMENSIONE) {
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
  public tisg_prt_sag_file with_T_DIGEST(String T_DIGEST) {
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
  public tisg_prt_sag_file with_N_ID_OPERAZIONE(java.math.BigDecimal N_ID_OPERAZIONE) {
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
  public tisg_prt_sag_file with_T_IDENT_FILE(String T_IDENT_FILE) {
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
  public tisg_prt_sag_file with_D_UPLOAD(String D_UPLOAD) {
    this.D_UPLOAD = D_UPLOAD;
    return this;
  }
  private String D_TRASMISSIONE_UTENTE;
  public String get_D_TRASMISSIONE_UTENTE() {
    return D_TRASMISSIONE_UTENTE;
  }
  public void set_D_TRASMISSIONE_UTENTE(String D_TRASMISSIONE_UTENTE) {
    this.D_TRASMISSIONE_UTENTE = D_TRASMISSIONE_UTENTE;
  }
  public tisg_prt_sag_file with_D_TRASMISSIONE_UTENTE(String D_TRASMISSIONE_UTENTE) {
    this.D_TRASMISSIONE_UTENTE = D_TRASMISSIONE_UTENTE;
    return this;
  }
  private java.math.BigDecimal N_NUM_FILE_CONTENUTI;
  public java.math.BigDecimal get_N_NUM_FILE_CONTENUTI() {
    return N_NUM_FILE_CONTENUTI;
  }
  public void set_N_NUM_FILE_CONTENUTI(java.math.BigDecimal N_NUM_FILE_CONTENUTI) {
    this.N_NUM_FILE_CONTENUTI = N_NUM_FILE_CONTENUTI;
  }
  public tisg_prt_sag_file with_N_NUM_FILE_CONTENUTI(java.math.BigDecimal N_NUM_FILE_CONTENUTI) {
    this.N_NUM_FILE_CONTENUTI = N_NUM_FILE_CONTENUTI;
    return this;
  }
  private java.math.BigDecimal N_RIGHE;
  public java.math.BigDecimal get_N_RIGHE() {
    return N_RIGHE;
  }
  public void set_N_RIGHE(java.math.BigDecimal N_RIGHE) {
    this.N_RIGHE = N_RIGHE;
  }
  public tisg_prt_sag_file with_N_RIGHE(java.math.BigDecimal N_RIGHE) {
    this.N_RIGHE = N_RIGHE;
    return this;
  }
  private String T_MOTIVAZIONE;
  public String get_T_MOTIVAZIONE() {
    return T_MOTIVAZIONE;
  }
  public void set_T_MOTIVAZIONE(String T_MOTIVAZIONE) {
    this.T_MOTIVAZIONE = T_MOTIVAZIONE;
  }
  public tisg_prt_sag_file with_T_MOTIVAZIONE(String T_MOTIVAZIONE) {
    this.T_MOTIVAZIONE = T_MOTIVAZIONE;
    return this;
  }
  private String D_DOWNLOAD;
  public String get_D_DOWNLOAD() {
    return D_DOWNLOAD;
  }
  public void set_D_DOWNLOAD(String D_DOWNLOAD) {
    this.D_DOWNLOAD = D_DOWNLOAD;
  }
  public tisg_prt_sag_file with_D_DOWNLOAD(String D_DOWNLOAD) {
    this.D_DOWNLOAD = D_DOWNLOAD;
    return this;
  }
  private String T_COD_INAMMISSIBILITA;
  public String get_T_COD_INAMMISSIBILITA() {
    return T_COD_INAMMISSIBILITA;
  }
  public void set_T_COD_INAMMISSIBILITA(String T_COD_INAMMISSIBILITA) {
    this.T_COD_INAMMISSIBILITA = T_COD_INAMMISSIBILITA;
  }
  public tisg_prt_sag_file with_T_COD_INAMMISSIBILITA(String T_COD_INAMMISSIBILITA) {
    this.T_COD_INAMMISSIBILITA = T_COD_INAMMISSIBILITA;
    return this;
  }
  private String T_ANNO_RIF;
  public String get_T_ANNO_RIF() {
    return T_ANNO_RIF;
  }
  public void set_T_ANNO_RIF(String T_ANNO_RIF) {
    this.T_ANNO_RIF = T_ANNO_RIF;
  }
  public tisg_prt_sag_file with_T_ANNO_RIF(String T_ANNO_RIF) {
    this.T_ANNO_RIF = T_ANNO_RIF;
    return this;
  }
  private String T_MESE_RIF;
  public String get_T_MESE_RIF() {
    return T_MESE_RIF;
  }
  public void set_T_MESE_RIF(String T_MESE_RIF) {
    this.T_MESE_RIF = T_MESE_RIF;
  }
  public tisg_prt_sag_file with_T_MESE_RIF(String T_MESE_RIF) {
    this.T_MESE_RIF = T_MESE_RIF;
    return this;
  }
  private String T_PIVA_UTENTE;
  public String get_T_PIVA_UTENTE() {
    return T_PIVA_UTENTE;
  }
  public void set_T_PIVA_UTENTE(String T_PIVA_UTENTE) {
    this.T_PIVA_UTENTE = T_PIVA_UTENTE;
  }
  public tisg_prt_sag_file with_T_PIVA_UTENTE(String T_PIVA_UTENTE) {
    this.T_PIVA_UTENTE = T_PIVA_UTENTE;
    return this;
  }
  private String D_ELABORAZIONE;
  public String get_D_ELABORAZIONE() {
    return D_ELABORAZIONE;
  }
  public void set_D_ELABORAZIONE(String D_ELABORAZIONE) {
    this.D_ELABORAZIONE = D_ELABORAZIONE;
  }
  public tisg_prt_sag_file with_D_ELABORAZIONE(String D_ELABORAZIONE) {
    this.D_ELABORAZIONE = D_ELABORAZIONE;
    return this;
  }
  private String B_FILE_LOADED;
  public String get_B_FILE_LOADED() {
    return B_FILE_LOADED;
  }
  public void set_B_FILE_LOADED(String B_FILE_LOADED) {
    this.B_FILE_LOADED = B_FILE_LOADED;
  }
  public tisg_prt_sag_file with_B_FILE_LOADED(String B_FILE_LOADED) {
    this.B_FILE_LOADED = B_FILE_LOADED;
    return this;
  }
  private String B_FILE_ELABORATO;
  public String get_B_FILE_ELABORATO() {
    return B_FILE_ELABORATO;
  }
  public void set_B_FILE_ELABORATO(String B_FILE_ELABORATO) {
    this.B_FILE_ELABORATO = B_FILE_ELABORATO;
  }
  public tisg_prt_sag_file with_B_FILE_ELABORATO(String B_FILE_ELABORATO) {
    this.B_FILE_ELABORATO = B_FILE_ELABORATO;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof tisg_prt_sag_file)) {
      return false;
    }
    tisg_prt_sag_file that = (tisg_prt_sag_file) o;
    boolean equal = true;
    equal = equal && (this.N_ID_FILE == null ? that.N_ID_FILE == null : this.N_ID_FILE.equals(that.N_ID_FILE));
    equal = equal && (this.N_ID_SAG == null ? that.N_ID_SAG == null : this.N_ID_SAG.equals(that.N_ID_SAG));
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
    equal = equal && (this.D_TRASMISSIONE_UTENTE == null ? that.D_TRASMISSIONE_UTENTE == null : this.D_TRASMISSIONE_UTENTE.equals(that.D_TRASMISSIONE_UTENTE));
    equal = equal && (this.N_NUM_FILE_CONTENUTI == null ? that.N_NUM_FILE_CONTENUTI == null : this.N_NUM_FILE_CONTENUTI.equals(that.N_NUM_FILE_CONTENUTI));
    equal = equal && (this.N_RIGHE == null ? that.N_RIGHE == null : this.N_RIGHE.equals(that.N_RIGHE));
    equal = equal && (this.T_MOTIVAZIONE == null ? that.T_MOTIVAZIONE == null : this.T_MOTIVAZIONE.equals(that.T_MOTIVAZIONE));
    equal = equal && (this.D_DOWNLOAD == null ? that.D_DOWNLOAD == null : this.D_DOWNLOAD.equals(that.D_DOWNLOAD));
    equal = equal && (this.T_COD_INAMMISSIBILITA == null ? that.T_COD_INAMMISSIBILITA == null : this.T_COD_INAMMISSIBILITA.equals(that.T_COD_INAMMISSIBILITA));
    equal = equal && (this.T_ANNO_RIF == null ? that.T_ANNO_RIF == null : this.T_ANNO_RIF.equals(that.T_ANNO_RIF));
    equal = equal && (this.T_MESE_RIF == null ? that.T_MESE_RIF == null : this.T_MESE_RIF.equals(that.T_MESE_RIF));
    equal = equal && (this.T_PIVA_UTENTE == null ? that.T_PIVA_UTENTE == null : this.T_PIVA_UTENTE.equals(that.T_PIVA_UTENTE));
    equal = equal && (this.D_ELABORAZIONE == null ? that.D_ELABORAZIONE == null : this.D_ELABORAZIONE.equals(that.D_ELABORAZIONE));
    equal = equal && (this.B_FILE_LOADED == null ? that.B_FILE_LOADED == null : this.B_FILE_LOADED.equals(that.B_FILE_LOADED));
    equal = equal && (this.B_FILE_ELABORATO == null ? that.B_FILE_ELABORATO == null : this.B_FILE_ELABORATO.equals(that.B_FILE_ELABORATO));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof tisg_prt_sag_file)) {
      return false;
    }
    tisg_prt_sag_file that = (tisg_prt_sag_file) o;
    boolean equal = true;
    equal = equal && (this.N_ID_FILE == null ? that.N_ID_FILE == null : this.N_ID_FILE.equals(that.N_ID_FILE));
    equal = equal && (this.N_ID_SAG == null ? that.N_ID_SAG == null : this.N_ID_SAG.equals(that.N_ID_SAG));
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
    equal = equal && (this.D_TRASMISSIONE_UTENTE == null ? that.D_TRASMISSIONE_UTENTE == null : this.D_TRASMISSIONE_UTENTE.equals(that.D_TRASMISSIONE_UTENTE));
    equal = equal && (this.N_NUM_FILE_CONTENUTI == null ? that.N_NUM_FILE_CONTENUTI == null : this.N_NUM_FILE_CONTENUTI.equals(that.N_NUM_FILE_CONTENUTI));
    equal = equal && (this.N_RIGHE == null ? that.N_RIGHE == null : this.N_RIGHE.equals(that.N_RIGHE));
    equal = equal && (this.T_MOTIVAZIONE == null ? that.T_MOTIVAZIONE == null : this.T_MOTIVAZIONE.equals(that.T_MOTIVAZIONE));
    equal = equal && (this.D_DOWNLOAD == null ? that.D_DOWNLOAD == null : this.D_DOWNLOAD.equals(that.D_DOWNLOAD));
    equal = equal && (this.T_COD_INAMMISSIBILITA == null ? that.T_COD_INAMMISSIBILITA == null : this.T_COD_INAMMISSIBILITA.equals(that.T_COD_INAMMISSIBILITA));
    equal = equal && (this.T_ANNO_RIF == null ? that.T_ANNO_RIF == null : this.T_ANNO_RIF.equals(that.T_ANNO_RIF));
    equal = equal && (this.T_MESE_RIF == null ? that.T_MESE_RIF == null : this.T_MESE_RIF.equals(that.T_MESE_RIF));
    equal = equal && (this.T_PIVA_UTENTE == null ? that.T_PIVA_UTENTE == null : this.T_PIVA_UTENTE.equals(that.T_PIVA_UTENTE));
    equal = equal && (this.D_ELABORAZIONE == null ? that.D_ELABORAZIONE == null : this.D_ELABORAZIONE.equals(that.D_ELABORAZIONE));
    equal = equal && (this.B_FILE_LOADED == null ? that.B_FILE_LOADED == null : this.B_FILE_LOADED.equals(that.B_FILE_LOADED));
    equal = equal && (this.B_FILE_ELABORATO == null ? that.B_FILE_ELABORATO == null : this.B_FILE_ELABORATO.equals(that.B_FILE_ELABORATO));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_FILE = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_SAG = JdbcWritableBridge.readBigDecimal(2, __dbResults);
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
    this.D_TRASMISSIONE_UTENTE = JdbcWritableBridge.readString(14, __dbResults);
    this.N_NUM_FILE_CONTENUTI = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.N_RIGHE = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.T_MOTIVAZIONE = JdbcWritableBridge.readString(17, __dbResults);
    this.D_DOWNLOAD = JdbcWritableBridge.readString(18, __dbResults);
    this.T_COD_INAMMISSIBILITA = JdbcWritableBridge.readString(19, __dbResults);
    this.T_ANNO_RIF = JdbcWritableBridge.readString(20, __dbResults);
    this.T_MESE_RIF = JdbcWritableBridge.readString(21, __dbResults);
    this.T_PIVA_UTENTE = JdbcWritableBridge.readString(22, __dbResults);
    this.D_ELABORAZIONE = JdbcWritableBridge.readString(23, __dbResults);
    this.B_FILE_LOADED = JdbcWritableBridge.readString(24, __dbResults);
    this.B_FILE_ELABORATO = JdbcWritableBridge.readString(25, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_FILE = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_SAG = JdbcWritableBridge.readBigDecimal(2, __dbResults);
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
    this.D_TRASMISSIONE_UTENTE = JdbcWritableBridge.readString(14, __dbResults);
    this.N_NUM_FILE_CONTENUTI = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.N_RIGHE = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.T_MOTIVAZIONE = JdbcWritableBridge.readString(17, __dbResults);
    this.D_DOWNLOAD = JdbcWritableBridge.readString(18, __dbResults);
    this.T_COD_INAMMISSIBILITA = JdbcWritableBridge.readString(19, __dbResults);
    this.T_ANNO_RIF = JdbcWritableBridge.readString(20, __dbResults);
    this.T_MESE_RIF = JdbcWritableBridge.readString(21, __dbResults);
    this.T_PIVA_UTENTE = JdbcWritableBridge.readString(22, __dbResults);
    this.D_ELABORAZIONE = JdbcWritableBridge.readString(23, __dbResults);
    this.B_FILE_LOADED = JdbcWritableBridge.readString(24, __dbResults);
    this.B_FILE_ELABORATO = JdbcWritableBridge.readString(25, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_SAG, 2 + __off, 2, __dbStmt);
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
    JdbcWritableBridge.writeString(D_TRASMISSIONE_UTENTE, 14 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_FILE_CONTENUTI, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_RIGHE, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_MOTIVAZIONE, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DOWNLOAD, 18 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_INAMMISSIBILITA, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO_RIF, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MESE_RIF, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA_UTENTE, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_ELABORAZIONE, 23 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(B_FILE_LOADED, 24 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_FILE_ELABORATO, 25 + __off, 1, __dbStmt);
    return 25;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_FILE, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_SAG, 2 + __off, 2, __dbStmt);
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
    JdbcWritableBridge.writeString(D_TRASMISSIONE_UTENTE, 14 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_FILE_CONTENUTI, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_RIGHE, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_MOTIVAZIONE, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DOWNLOAD, 18 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_INAMMISSIBILITA, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO_RIF, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MESE_RIF, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA_UTENTE, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_ELABORAZIONE, 23 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(B_FILE_LOADED, 24 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_FILE_ELABORATO, 25 + __off, 1, __dbStmt);
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
        this.N_ID_SAG = null;
    } else {
    this.N_ID_SAG = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
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
        this.D_TRASMISSIONE_UTENTE = null;
    } else {
    this.D_TRASMISSIONE_UTENTE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_NUM_FILE_CONTENUTI = null;
    } else {
    this.N_NUM_FILE_CONTENUTI = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_RIGHE = null;
    } else {
    this.N_RIGHE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MOTIVAZIONE = null;
    } else {
    this.T_MOTIVAZIONE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DOWNLOAD = null;
    } else {
    this.D_DOWNLOAD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COD_INAMMISSIBILITA = null;
    } else {
    this.T_COD_INAMMISSIBILITA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ANNO_RIF = null;
    } else {
    this.T_ANNO_RIF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MESE_RIF = null;
    } else {
    this.T_MESE_RIF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PIVA_UTENTE = null;
    } else {
    this.T_PIVA_UTENTE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_ELABORAZIONE = null;
    } else {
    this.D_ELABORAZIONE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_FILE_LOADED = null;
    } else {
    this.B_FILE_LOADED = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_FILE_ELABORATO = null;
    } else {
    this.B_FILE_ELABORATO = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_FILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FILE, __dataOut);
    }
    if (null == this.N_ID_SAG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_SAG, __dataOut);
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
    if (null == this.D_TRASMISSIONE_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_TRASMISSIONE_UTENTE);
    }
    if (null == this.N_NUM_FILE_CONTENUTI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_FILE_CONTENUTI, __dataOut);
    }
    if (null == this.N_RIGHE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_RIGHE, __dataOut);
    }
    if (null == this.T_MOTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MOTIVAZIONE);
    }
    if (null == this.D_DOWNLOAD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DOWNLOAD);
    }
    if (null == this.T_COD_INAMMISSIBILITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_INAMMISSIBILITA);
    }
    if (null == this.T_ANNO_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO_RIF);
    }
    if (null == this.T_MESE_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MESE_RIF);
    }
    if (null == this.T_PIVA_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA_UTENTE);
    }
    if (null == this.D_ELABORAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_ELABORAZIONE);
    }
    if (null == this.B_FILE_LOADED) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_FILE_LOADED);
    }
    if (null == this.B_FILE_ELABORATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_FILE_ELABORATO);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_FILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FILE, __dataOut);
    }
    if (null == this.N_ID_SAG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_SAG, __dataOut);
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
    if (null == this.D_TRASMISSIONE_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_TRASMISSIONE_UTENTE);
    }
    if (null == this.N_NUM_FILE_CONTENUTI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_FILE_CONTENUTI, __dataOut);
    }
    if (null == this.N_RIGHE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_RIGHE, __dataOut);
    }
    if (null == this.T_MOTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MOTIVAZIONE);
    }
    if (null == this.D_DOWNLOAD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DOWNLOAD);
    }
    if (null == this.T_COD_INAMMISSIBILITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_INAMMISSIBILITA);
    }
    if (null == this.T_ANNO_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO_RIF);
    }
    if (null == this.T_MESE_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MESE_RIF);
    }
    if (null == this.T_PIVA_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA_UTENTE);
    }
    if (null == this.D_ELABORAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_ELABORAZIONE);
    }
    if (null == this.B_FILE_LOADED) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_FILE_LOADED);
    }
    if (null == this.B_FILE_ELABORATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_FILE_ELABORATO);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_SAG==null?"":N_ID_SAG.toPlainString(), delimiters));
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_TRASMISSIONE_UTENTE==null?"":D_TRASMISSIONE_UTENTE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_FILE_CONTENUTI==null?"":N_NUM_FILE_CONTENUTI.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_RIGHE==null?"":N_RIGHE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MOTIVAZIONE==null?"":T_MOTIVAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DOWNLOAD==null?"":D_DOWNLOAD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_INAMMISSIBILITA==null?"":T_COD_INAMMISSIBILITA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO_RIF==null?"":T_ANNO_RIF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MESE_RIF==null?"":T_MESE_RIF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA_UTENTE==null?"":T_PIVA_UTENTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_ELABORAZIONE==null?"":D_ELABORAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_FILE_LOADED==null?"":B_FILE_LOADED, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_FILE_ELABORATO==null?"":B_FILE_ELABORATO, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_FILE==null?"":N_ID_FILE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_SAG==null?"":N_ID_SAG.toPlainString(), delimiters));
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_TRASMISSIONE_UTENTE==null?"":D_TRASMISSIONE_UTENTE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_FILE_CONTENUTI==null?"":N_NUM_FILE_CONTENUTI.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_RIGHE==null?"":N_RIGHE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MOTIVAZIONE==null?"":T_MOTIVAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DOWNLOAD==null?"":D_DOWNLOAD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_INAMMISSIBILITA==null?"":T_COD_INAMMISSIBILITA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO_RIF==null?"":T_ANNO_RIF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MESE_RIF==null?"":T_MESE_RIF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA_UTENTE==null?"":T_PIVA_UTENTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_ELABORAZIONE==null?"":D_ELABORAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_FILE_LOADED==null?"":B_FILE_LOADED, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_FILE_ELABORATO==null?"":B_FILE_ELABORATO, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_SAG = null; } else {
      this.N_ID_SAG = new java.math.BigDecimal(__cur_str);
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
    if (__cur_str.equals("null")) { this.D_TRASMISSIONE_UTENTE = null; } else {
      this.D_TRASMISSIONE_UTENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_FILE_CONTENUTI = null; } else {
      this.N_NUM_FILE_CONTENUTI = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_RIGHE = null; } else {
      this.N_RIGHE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MOTIVAZIONE = null; } else {
      this.T_MOTIVAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DOWNLOAD = null; } else {
      this.D_DOWNLOAD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_INAMMISSIBILITA = null; } else {
      this.T_COD_INAMMISSIBILITA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO_RIF = null; } else {
      this.T_ANNO_RIF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MESE_RIF = null; } else {
      this.T_MESE_RIF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA_UTENTE = null; } else {
      this.T_PIVA_UTENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_ELABORAZIONE = null; } else {
      this.D_ELABORAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_FILE_LOADED = null; } else {
      this.B_FILE_LOADED = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_FILE_ELABORATO = null; } else {
      this.B_FILE_ELABORATO = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_SAG = null; } else {
      this.N_ID_SAG = new java.math.BigDecimal(__cur_str);
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
    if (__cur_str.equals("null")) { this.D_TRASMISSIONE_UTENTE = null; } else {
      this.D_TRASMISSIONE_UTENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_FILE_CONTENUTI = null; } else {
      this.N_NUM_FILE_CONTENUTI = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_RIGHE = null; } else {
      this.N_RIGHE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MOTIVAZIONE = null; } else {
      this.T_MOTIVAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DOWNLOAD = null; } else {
      this.D_DOWNLOAD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_INAMMISSIBILITA = null; } else {
      this.T_COD_INAMMISSIBILITA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO_RIF = null; } else {
      this.T_ANNO_RIF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MESE_RIF = null; } else {
      this.T_MESE_RIF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA_UTENTE = null; } else {
      this.T_PIVA_UTENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_ELABORAZIONE = null; } else {
      this.D_ELABORAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_FILE_LOADED = null; } else {
      this.B_FILE_LOADED = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_FILE_ELABORATO = null; } else {
      this.B_FILE_ELABORATO = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    tisg_prt_sag_file o = (tisg_prt_sag_file) super.clone();
    return o;
  }

  public void clone0(tisg_prt_sag_file o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_FILE", this.N_ID_FILE);
    __sqoop$field_map.put("N_ID_SAG", this.N_ID_SAG);
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
    __sqoop$field_map.put("D_TRASMISSIONE_UTENTE", this.D_TRASMISSIONE_UTENTE);
    __sqoop$field_map.put("N_NUM_FILE_CONTENUTI", this.N_NUM_FILE_CONTENUTI);
    __sqoop$field_map.put("N_RIGHE", this.N_RIGHE);
    __sqoop$field_map.put("T_MOTIVAZIONE", this.T_MOTIVAZIONE);
    __sqoop$field_map.put("D_DOWNLOAD", this.D_DOWNLOAD);
    __sqoop$field_map.put("T_COD_INAMMISSIBILITA", this.T_COD_INAMMISSIBILITA);
    __sqoop$field_map.put("T_ANNO_RIF", this.T_ANNO_RIF);
    __sqoop$field_map.put("T_MESE_RIF", this.T_MESE_RIF);
    __sqoop$field_map.put("T_PIVA_UTENTE", this.T_PIVA_UTENTE);
    __sqoop$field_map.put("D_ELABORAZIONE", this.D_ELABORAZIONE);
    __sqoop$field_map.put("B_FILE_LOADED", this.B_FILE_LOADED);
    __sqoop$field_map.put("B_FILE_ELABORATO", this.B_FILE_ELABORATO);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_FILE", this.N_ID_FILE);
    __sqoop$field_map.put("N_ID_SAG", this.N_ID_SAG);
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
    __sqoop$field_map.put("D_TRASMISSIONE_UTENTE", this.D_TRASMISSIONE_UTENTE);
    __sqoop$field_map.put("N_NUM_FILE_CONTENUTI", this.N_NUM_FILE_CONTENUTI);
    __sqoop$field_map.put("N_RIGHE", this.N_RIGHE);
    __sqoop$field_map.put("T_MOTIVAZIONE", this.T_MOTIVAZIONE);
    __sqoop$field_map.put("D_DOWNLOAD", this.D_DOWNLOAD);
    __sqoop$field_map.put("T_COD_INAMMISSIBILITA", this.T_COD_INAMMISSIBILITA);
    __sqoop$field_map.put("T_ANNO_RIF", this.T_ANNO_RIF);
    __sqoop$field_map.put("T_MESE_RIF", this.T_MESE_RIF);
    __sqoop$field_map.put("T_PIVA_UTENTE", this.T_PIVA_UTENTE);
    __sqoop$field_map.put("D_ELABORAZIONE", this.D_ELABORAZIONE);
    __sqoop$field_map.put("B_FILE_LOADED", this.B_FILE_LOADED);
    __sqoop$field_map.put("B_FILE_ELABORATO", this.B_FILE_ELABORATO);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
