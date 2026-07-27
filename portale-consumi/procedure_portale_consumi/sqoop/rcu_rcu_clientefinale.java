// ORM class for table 'rcu.rcu_clientefinale'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 09:37:24 CEST 2019
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

public class rcu_rcu_clientefinale extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_CLIENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_CLIENTE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_NOME", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_NOME = (String)value;
      }
    });
    setters.put("T_COGNOME", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COGNOME = (String)value;
      }
    });
    setters.put("T_RAGSOC", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_RAGSOC = (String)value;
      }
    });
    setters.put("B_PERSONA_FISICA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_PERSONA_FISICA = (String)value;
      }
    });
    setters.put("T_CF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CF = (String)value;
      }
    });
    setters.put("T_PIVA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PIVA = (String)value;
      }
    });
    setters.put("N_ID_SEDELEGALE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_SEDELEGALE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_EMAIL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_EMAIL = (String)value;
      }
    });
    setters.put("T_CODICE_ATECO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_ATECO = (String)value;
      }
    });
    setters.put("B_DIRITTO_MT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_DIRITTO_MT = (String)value;
      }
    });
    setters.put("D_AUTOCERT_MT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_AUTOCERT_MT = (String)value;
      }
    });
    setters.put("T_NOTA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_NOTA = (String)value;
      }
    });
    setters.put("D_AGGIORNAMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_AGGIORNAMENTO = (String)value;
      }
    });
    setters.put("N_ID_TRACCIA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_TRACCIA = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_S_PREC", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_S_PREC = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_DENOM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_DENOM = (String)value;
      }
    });
    setters.put("T_DETTAGLIO_CF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_DETTAGLIO_CF = (String)value;
      }
    });
    setters.put("T_DETTAGLIO_PIVA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_DETTAGLIO_PIVA = (String)value;
      }
    });
  }
  public rcu_rcu_clientefinale() {
    init0();
  }
  private java.math.BigDecimal N_ID_CLIENTE;
  public java.math.BigDecimal get_N_ID_CLIENTE() {
    return N_ID_CLIENTE;
  }
  public void set_N_ID_CLIENTE(java.math.BigDecimal N_ID_CLIENTE) {
    this.N_ID_CLIENTE = N_ID_CLIENTE;
  }
  public rcu_rcu_clientefinale with_N_ID_CLIENTE(java.math.BigDecimal N_ID_CLIENTE) {
    this.N_ID_CLIENTE = N_ID_CLIENTE;
    return this;
  }
  private String T_NOME;
  public String get_T_NOME() {
    return T_NOME;
  }
  public void set_T_NOME(String T_NOME) {
    this.T_NOME = T_NOME;
  }
  public rcu_rcu_clientefinale with_T_NOME(String T_NOME) {
    this.T_NOME = T_NOME;
    return this;
  }
  private String T_COGNOME;
  public String get_T_COGNOME() {
    return T_COGNOME;
  }
  public void set_T_COGNOME(String T_COGNOME) {
    this.T_COGNOME = T_COGNOME;
  }
  public rcu_rcu_clientefinale with_T_COGNOME(String T_COGNOME) {
    this.T_COGNOME = T_COGNOME;
    return this;
  }
  private String T_RAGSOC;
  public String get_T_RAGSOC() {
    return T_RAGSOC;
  }
  public void set_T_RAGSOC(String T_RAGSOC) {
    this.T_RAGSOC = T_RAGSOC;
  }
  public rcu_rcu_clientefinale with_T_RAGSOC(String T_RAGSOC) {
    this.T_RAGSOC = T_RAGSOC;
    return this;
  }
  private String B_PERSONA_FISICA;
  public String get_B_PERSONA_FISICA() {
    return B_PERSONA_FISICA;
  }
  public void set_B_PERSONA_FISICA(String B_PERSONA_FISICA) {
    this.B_PERSONA_FISICA = B_PERSONA_FISICA;
  }
  public rcu_rcu_clientefinale with_B_PERSONA_FISICA(String B_PERSONA_FISICA) {
    this.B_PERSONA_FISICA = B_PERSONA_FISICA;
    return this;
  }
  private String T_CF;
  public String get_T_CF() {
    return T_CF;
  }
  public void set_T_CF(String T_CF) {
    this.T_CF = T_CF;
  }
  public rcu_rcu_clientefinale with_T_CF(String T_CF) {
    this.T_CF = T_CF;
    return this;
  }
  private String T_PIVA;
  public String get_T_PIVA() {
    return T_PIVA;
  }
  public void set_T_PIVA(String T_PIVA) {
    this.T_PIVA = T_PIVA;
  }
  public rcu_rcu_clientefinale with_T_PIVA(String T_PIVA) {
    this.T_PIVA = T_PIVA;
    return this;
  }
  private java.math.BigDecimal N_ID_SEDELEGALE;
  public java.math.BigDecimal get_N_ID_SEDELEGALE() {
    return N_ID_SEDELEGALE;
  }
  public void set_N_ID_SEDELEGALE(java.math.BigDecimal N_ID_SEDELEGALE) {
    this.N_ID_SEDELEGALE = N_ID_SEDELEGALE;
  }
  public rcu_rcu_clientefinale with_N_ID_SEDELEGALE(java.math.BigDecimal N_ID_SEDELEGALE) {
    this.N_ID_SEDELEGALE = N_ID_SEDELEGALE;
    return this;
  }
  private String T_EMAIL;
  public String get_T_EMAIL() {
    return T_EMAIL;
  }
  public void set_T_EMAIL(String T_EMAIL) {
    this.T_EMAIL = T_EMAIL;
  }
  public rcu_rcu_clientefinale with_T_EMAIL(String T_EMAIL) {
    this.T_EMAIL = T_EMAIL;
    return this;
  }
  private String T_CODICE_ATECO;
  public String get_T_CODICE_ATECO() {
    return T_CODICE_ATECO;
  }
  public void set_T_CODICE_ATECO(String T_CODICE_ATECO) {
    this.T_CODICE_ATECO = T_CODICE_ATECO;
  }
  public rcu_rcu_clientefinale with_T_CODICE_ATECO(String T_CODICE_ATECO) {
    this.T_CODICE_ATECO = T_CODICE_ATECO;
    return this;
  }
  private String B_DIRITTO_MT;
  public String get_B_DIRITTO_MT() {
    return B_DIRITTO_MT;
  }
  public void set_B_DIRITTO_MT(String B_DIRITTO_MT) {
    this.B_DIRITTO_MT = B_DIRITTO_MT;
  }
  public rcu_rcu_clientefinale with_B_DIRITTO_MT(String B_DIRITTO_MT) {
    this.B_DIRITTO_MT = B_DIRITTO_MT;
    return this;
  }
  private String D_AUTOCERT_MT;
  public String get_D_AUTOCERT_MT() {
    return D_AUTOCERT_MT;
  }
  public void set_D_AUTOCERT_MT(String D_AUTOCERT_MT) {
    this.D_AUTOCERT_MT = D_AUTOCERT_MT;
  }
  public rcu_rcu_clientefinale with_D_AUTOCERT_MT(String D_AUTOCERT_MT) {
    this.D_AUTOCERT_MT = D_AUTOCERT_MT;
    return this;
  }
  private String T_NOTA;
  public String get_T_NOTA() {
    return T_NOTA;
  }
  public void set_T_NOTA(String T_NOTA) {
    this.T_NOTA = T_NOTA;
  }
  public rcu_rcu_clientefinale with_T_NOTA(String T_NOTA) {
    this.T_NOTA = T_NOTA;
    return this;
  }
  private String D_AGGIORNAMENTO;
  public String get_D_AGGIORNAMENTO() {
    return D_AGGIORNAMENTO;
  }
  public void set_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
    this.D_AGGIORNAMENTO = D_AGGIORNAMENTO;
  }
  public rcu_rcu_clientefinale with_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
    this.D_AGGIORNAMENTO = D_AGGIORNAMENTO;
    return this;
  }
  private java.math.BigDecimal N_ID_TRACCIA;
  public java.math.BigDecimal get_N_ID_TRACCIA() {
    return N_ID_TRACCIA;
  }
  public void set_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
    this.N_ID_TRACCIA = N_ID_TRACCIA;
  }
  public rcu_rcu_clientefinale with_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
    this.N_ID_TRACCIA = N_ID_TRACCIA;
    return this;
  }
  private java.math.BigDecimal N_ID_S_PREC;
  public java.math.BigDecimal get_N_ID_S_PREC() {
    return N_ID_S_PREC;
  }
  public void set_N_ID_S_PREC(java.math.BigDecimal N_ID_S_PREC) {
    this.N_ID_S_PREC = N_ID_S_PREC;
  }
  public rcu_rcu_clientefinale with_N_ID_S_PREC(java.math.BigDecimal N_ID_S_PREC) {
    this.N_ID_S_PREC = N_ID_S_PREC;
    return this;
  }
  private String T_DENOM;
  public String get_T_DENOM() {
    return T_DENOM;
  }
  public void set_T_DENOM(String T_DENOM) {
    this.T_DENOM = T_DENOM;
  }
  public rcu_rcu_clientefinale with_T_DENOM(String T_DENOM) {
    this.T_DENOM = T_DENOM;
    return this;
  }
  private String T_DETTAGLIO_CF;
  public String get_T_DETTAGLIO_CF() {
    return T_DETTAGLIO_CF;
  }
  public void set_T_DETTAGLIO_CF(String T_DETTAGLIO_CF) {
    this.T_DETTAGLIO_CF = T_DETTAGLIO_CF;
  }
  public rcu_rcu_clientefinale with_T_DETTAGLIO_CF(String T_DETTAGLIO_CF) {
    this.T_DETTAGLIO_CF = T_DETTAGLIO_CF;
    return this;
  }
  private String T_DETTAGLIO_PIVA;
  public String get_T_DETTAGLIO_PIVA() {
    return T_DETTAGLIO_PIVA;
  }
  public void set_T_DETTAGLIO_PIVA(String T_DETTAGLIO_PIVA) {
    this.T_DETTAGLIO_PIVA = T_DETTAGLIO_PIVA;
  }
  public rcu_rcu_clientefinale with_T_DETTAGLIO_PIVA(String T_DETTAGLIO_PIVA) {
    this.T_DETTAGLIO_PIVA = T_DETTAGLIO_PIVA;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcu_rcu_clientefinale)) {
      return false;
    }
    rcu_rcu_clientefinale that = (rcu_rcu_clientefinale) o;
    boolean equal = true;
    equal = equal && (this.N_ID_CLIENTE == null ? that.N_ID_CLIENTE == null : this.N_ID_CLIENTE.equals(that.N_ID_CLIENTE));
    equal = equal && (this.T_NOME == null ? that.T_NOME == null : this.T_NOME.equals(that.T_NOME));
    equal = equal && (this.T_COGNOME == null ? that.T_COGNOME == null : this.T_COGNOME.equals(that.T_COGNOME));
    equal = equal && (this.T_RAGSOC == null ? that.T_RAGSOC == null : this.T_RAGSOC.equals(that.T_RAGSOC));
    equal = equal && (this.B_PERSONA_FISICA == null ? that.B_PERSONA_FISICA == null : this.B_PERSONA_FISICA.equals(that.B_PERSONA_FISICA));
    equal = equal && (this.T_CF == null ? that.T_CF == null : this.T_CF.equals(that.T_CF));
    equal = equal && (this.T_PIVA == null ? that.T_PIVA == null : this.T_PIVA.equals(that.T_PIVA));
    equal = equal && (this.N_ID_SEDELEGALE == null ? that.N_ID_SEDELEGALE == null : this.N_ID_SEDELEGALE.equals(that.N_ID_SEDELEGALE));
    equal = equal && (this.T_EMAIL == null ? that.T_EMAIL == null : this.T_EMAIL.equals(that.T_EMAIL));
    equal = equal && (this.T_CODICE_ATECO == null ? that.T_CODICE_ATECO == null : this.T_CODICE_ATECO.equals(that.T_CODICE_ATECO));
    equal = equal && (this.B_DIRITTO_MT == null ? that.B_DIRITTO_MT == null : this.B_DIRITTO_MT.equals(that.B_DIRITTO_MT));
    equal = equal && (this.D_AUTOCERT_MT == null ? that.D_AUTOCERT_MT == null : this.D_AUTOCERT_MT.equals(that.D_AUTOCERT_MT));
    equal = equal && (this.T_NOTA == null ? that.T_NOTA == null : this.T_NOTA.equals(that.T_NOTA));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.T_DENOM == null ? that.T_DENOM == null : this.T_DENOM.equals(that.T_DENOM));
    equal = equal && (this.T_DETTAGLIO_CF == null ? that.T_DETTAGLIO_CF == null : this.T_DETTAGLIO_CF.equals(that.T_DETTAGLIO_CF));
    equal = equal && (this.T_DETTAGLIO_PIVA == null ? that.T_DETTAGLIO_PIVA == null : this.T_DETTAGLIO_PIVA.equals(that.T_DETTAGLIO_PIVA));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcu_rcu_clientefinale)) {
      return false;
    }
    rcu_rcu_clientefinale that = (rcu_rcu_clientefinale) o;
    boolean equal = true;
    equal = equal && (this.N_ID_CLIENTE == null ? that.N_ID_CLIENTE == null : this.N_ID_CLIENTE.equals(that.N_ID_CLIENTE));
    equal = equal && (this.T_NOME == null ? that.T_NOME == null : this.T_NOME.equals(that.T_NOME));
    equal = equal && (this.T_COGNOME == null ? that.T_COGNOME == null : this.T_COGNOME.equals(that.T_COGNOME));
    equal = equal && (this.T_RAGSOC == null ? that.T_RAGSOC == null : this.T_RAGSOC.equals(that.T_RAGSOC));
    equal = equal && (this.B_PERSONA_FISICA == null ? that.B_PERSONA_FISICA == null : this.B_PERSONA_FISICA.equals(that.B_PERSONA_FISICA));
    equal = equal && (this.T_CF == null ? that.T_CF == null : this.T_CF.equals(that.T_CF));
    equal = equal && (this.T_PIVA == null ? that.T_PIVA == null : this.T_PIVA.equals(that.T_PIVA));
    equal = equal && (this.N_ID_SEDELEGALE == null ? that.N_ID_SEDELEGALE == null : this.N_ID_SEDELEGALE.equals(that.N_ID_SEDELEGALE));
    equal = equal && (this.T_EMAIL == null ? that.T_EMAIL == null : this.T_EMAIL.equals(that.T_EMAIL));
    equal = equal && (this.T_CODICE_ATECO == null ? that.T_CODICE_ATECO == null : this.T_CODICE_ATECO.equals(that.T_CODICE_ATECO));
    equal = equal && (this.B_DIRITTO_MT == null ? that.B_DIRITTO_MT == null : this.B_DIRITTO_MT.equals(that.B_DIRITTO_MT));
    equal = equal && (this.D_AUTOCERT_MT == null ? that.D_AUTOCERT_MT == null : this.D_AUTOCERT_MT.equals(that.D_AUTOCERT_MT));
    equal = equal && (this.T_NOTA == null ? that.T_NOTA == null : this.T_NOTA.equals(that.T_NOTA));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.T_DENOM == null ? that.T_DENOM == null : this.T_DENOM.equals(that.T_DENOM));
    equal = equal && (this.T_DETTAGLIO_CF == null ? that.T_DETTAGLIO_CF == null : this.T_DETTAGLIO_CF.equals(that.T_DETTAGLIO_CF));
    equal = equal && (this.T_DETTAGLIO_PIVA == null ? that.T_DETTAGLIO_PIVA == null : this.T_DETTAGLIO_PIVA.equals(that.T_DETTAGLIO_PIVA));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_CLIENTE = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.T_NOME = JdbcWritableBridge.readString(2, __dbResults);
    this.T_COGNOME = JdbcWritableBridge.readString(3, __dbResults);
    this.T_RAGSOC = JdbcWritableBridge.readString(4, __dbResults);
    this.B_PERSONA_FISICA = JdbcWritableBridge.readString(5, __dbResults);
    this.T_CF = JdbcWritableBridge.readString(6, __dbResults);
    this.T_PIVA = JdbcWritableBridge.readString(7, __dbResults);
    this.N_ID_SEDELEGALE = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.T_EMAIL = JdbcWritableBridge.readString(9, __dbResults);
    this.T_CODICE_ATECO = JdbcWritableBridge.readString(10, __dbResults);
    this.B_DIRITTO_MT = JdbcWritableBridge.readString(11, __dbResults);
    this.D_AUTOCERT_MT = JdbcWritableBridge.readString(12, __dbResults);
    this.T_NOTA = JdbcWritableBridge.readString(13, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(14, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.T_DENOM = JdbcWritableBridge.readString(17, __dbResults);
    this.T_DETTAGLIO_CF = JdbcWritableBridge.readString(18, __dbResults);
    this.T_DETTAGLIO_PIVA = JdbcWritableBridge.readString(19, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_CLIENTE = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.T_NOME = JdbcWritableBridge.readString(2, __dbResults);
    this.T_COGNOME = JdbcWritableBridge.readString(3, __dbResults);
    this.T_RAGSOC = JdbcWritableBridge.readString(4, __dbResults);
    this.B_PERSONA_FISICA = JdbcWritableBridge.readString(5, __dbResults);
    this.T_CF = JdbcWritableBridge.readString(6, __dbResults);
    this.T_PIVA = JdbcWritableBridge.readString(7, __dbResults);
    this.N_ID_SEDELEGALE = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.T_EMAIL = JdbcWritableBridge.readString(9, __dbResults);
    this.T_CODICE_ATECO = JdbcWritableBridge.readString(10, __dbResults);
    this.B_DIRITTO_MT = JdbcWritableBridge.readString(11, __dbResults);
    this.D_AUTOCERT_MT = JdbcWritableBridge.readString(12, __dbResults);
    this.T_NOTA = JdbcWritableBridge.readString(13, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(14, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.T_DENOM = JdbcWritableBridge.readString(17, __dbResults);
    this.T_DETTAGLIO_CF = JdbcWritableBridge.readString(18, __dbResults);
    this.T_DETTAGLIO_PIVA = JdbcWritableBridge.readString(19, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_CLIENTE, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_NOME, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COGNOME, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_RAGSOC, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_PERSONA_FISICA, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CF, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_SEDELEGALE, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_EMAIL, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_ATECO, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_DIRITTO_MT, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AUTOCERT_MT, 12 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTA, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 14 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_DENOM, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DETTAGLIO_CF, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DETTAGLIO_PIVA, 19 + __off, 12, __dbStmt);
    return 19;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_CLIENTE, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_NOME, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COGNOME, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_RAGSOC, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_PERSONA_FISICA, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CF, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_SEDELEGALE, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_EMAIL, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_ATECO, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_DIRITTO_MT, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AUTOCERT_MT, 12 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTA, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 14 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_DENOM, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DETTAGLIO_CF, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DETTAGLIO_PIVA, 19 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_CLIENTE = null;
    } else {
    this.N_ID_CLIENTE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_NOME = null;
    } else {
    this.T_NOME = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COGNOME = null;
    } else {
    this.T_COGNOME = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_RAGSOC = null;
    } else {
    this.T_RAGSOC = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_PERSONA_FISICA = null;
    } else {
    this.B_PERSONA_FISICA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CF = null;
    } else {
    this.T_CF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PIVA = null;
    } else {
    this.T_PIVA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_SEDELEGALE = null;
    } else {
    this.N_ID_SEDELEGALE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_EMAIL = null;
    } else {
    this.T_EMAIL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_ATECO = null;
    } else {
    this.T_CODICE_ATECO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_DIRITTO_MT = null;
    } else {
    this.B_DIRITTO_MT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_AUTOCERT_MT = null;
    } else {
    this.D_AUTOCERT_MT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_NOTA = null;
    } else {
    this.T_NOTA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_AGGIORNAMENTO = null;
    } else {
    this.D_AGGIORNAMENTO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_TRACCIA = null;
    } else {
    this.N_ID_TRACCIA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_S_PREC = null;
    } else {
    this.N_ID_S_PREC = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_DENOM = null;
    } else {
    this.T_DENOM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_DETTAGLIO_CF = null;
    } else {
    this.T_DETTAGLIO_CF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_DETTAGLIO_PIVA = null;
    } else {
    this.T_DETTAGLIO_PIVA = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_CLIENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CLIENTE, __dataOut);
    }
    if (null == this.T_NOME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOME);
    }
    if (null == this.T_COGNOME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COGNOME);
    }
    if (null == this.T_RAGSOC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RAGSOC);
    }
    if (null == this.B_PERSONA_FISICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_PERSONA_FISICA);
    }
    if (null == this.T_CF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CF);
    }
    if (null == this.T_PIVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA);
    }
    if (null == this.N_ID_SEDELEGALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_SEDELEGALE, __dataOut);
    }
    if (null == this.T_EMAIL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_EMAIL);
    }
    if (null == this.T_CODICE_ATECO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_ATECO);
    }
    if (null == this.B_DIRITTO_MT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_DIRITTO_MT);
    }
    if (null == this.D_AUTOCERT_MT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_AUTOCERT_MT);
    }
    if (null == this.T_NOTA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOTA);
    }
    if (null == this.D_AGGIORNAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_AGGIORNAMENTO);
    }
    if (null == this.N_ID_TRACCIA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_TRACCIA, __dataOut);
    }
    if (null == this.N_ID_S_PREC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_S_PREC, __dataOut);
    }
    if (null == this.T_DENOM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DENOM);
    }
    if (null == this.T_DETTAGLIO_CF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DETTAGLIO_CF);
    }
    if (null == this.T_DETTAGLIO_PIVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DETTAGLIO_PIVA);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_CLIENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CLIENTE, __dataOut);
    }
    if (null == this.T_NOME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOME);
    }
    if (null == this.T_COGNOME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COGNOME);
    }
    if (null == this.T_RAGSOC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RAGSOC);
    }
    if (null == this.B_PERSONA_FISICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_PERSONA_FISICA);
    }
    if (null == this.T_CF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CF);
    }
    if (null == this.T_PIVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA);
    }
    if (null == this.N_ID_SEDELEGALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_SEDELEGALE, __dataOut);
    }
    if (null == this.T_EMAIL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_EMAIL);
    }
    if (null == this.T_CODICE_ATECO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_ATECO);
    }
    if (null == this.B_DIRITTO_MT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_DIRITTO_MT);
    }
    if (null == this.D_AUTOCERT_MT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_AUTOCERT_MT);
    }
    if (null == this.T_NOTA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOTA);
    }
    if (null == this.D_AGGIORNAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_AGGIORNAMENTO);
    }
    if (null == this.N_ID_TRACCIA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_TRACCIA, __dataOut);
    }
    if (null == this.N_ID_S_PREC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_S_PREC, __dataOut);
    }
    if (null == this.T_DENOM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DENOM);
    }
    if (null == this.T_DETTAGLIO_CF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DETTAGLIO_CF);
    }
    if (null == this.T_DETTAGLIO_PIVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DETTAGLIO_PIVA);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CLIENTE==null?"":N_ID_CLIENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOME==null?"":T_NOME, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COGNOME==null?"":T_COGNOME, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RAGSOC==null?"":T_RAGSOC, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_PERSONA_FISICA==null?"":B_PERSONA_FISICA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CF==null?"":T_CF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA==null?"":T_PIVA, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_SEDELEGALE==null?"":N_ID_SEDELEGALE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_EMAIL==null?"":T_EMAIL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_ATECO==null?"":T_CODICE_ATECO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_DIRITTO_MT==null?"":B_DIRITTO_MT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AUTOCERT_MT==null?"":D_AUTOCERT_MT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOTA==null?"":T_NOTA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO==null?"":D_AGGIORNAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TRACCIA==null?"":N_ID_TRACCIA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_PREC==null?"":N_ID_S_PREC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DENOM==null?"":T_DENOM, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DETTAGLIO_CF==null?"":T_DETTAGLIO_CF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DETTAGLIO_PIVA==null?"":T_DETTAGLIO_PIVA, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CLIENTE==null?"":N_ID_CLIENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOME==null?"":T_NOME, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COGNOME==null?"":T_COGNOME, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RAGSOC==null?"":T_RAGSOC, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_PERSONA_FISICA==null?"":B_PERSONA_FISICA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CF==null?"":T_CF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA==null?"":T_PIVA, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_SEDELEGALE==null?"":N_ID_SEDELEGALE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_EMAIL==null?"":T_EMAIL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_ATECO==null?"":T_CODICE_ATECO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_DIRITTO_MT==null?"":B_DIRITTO_MT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AUTOCERT_MT==null?"":D_AUTOCERT_MT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOTA==null?"":T_NOTA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO==null?"":D_AGGIORNAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TRACCIA==null?"":N_ID_TRACCIA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_PREC==null?"":N_ID_S_PREC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DENOM==null?"":T_DENOM, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DETTAGLIO_CF==null?"":T_DETTAGLIO_CF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DETTAGLIO_PIVA==null?"":T_DETTAGLIO_PIVA, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CLIENTE = null; } else {
      this.N_ID_CLIENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOME = null; } else {
      this.T_NOME = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COGNOME = null; } else {
      this.T_COGNOME = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RAGSOC = null; } else {
      this.T_RAGSOC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_PERSONA_FISICA = null; } else {
      this.B_PERSONA_FISICA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CF = null; } else {
      this.T_CF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA = null; } else {
      this.T_PIVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_SEDELEGALE = null; } else {
      this.N_ID_SEDELEGALE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_EMAIL = null; } else {
      this.T_EMAIL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_ATECO = null; } else {
      this.T_CODICE_ATECO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_DIRITTO_MT = null; } else {
      this.B_DIRITTO_MT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_AUTOCERT_MT = null; } else {
      this.D_AUTOCERT_MT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOTA = null; } else {
      this.T_NOTA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_AGGIORNAMENTO = null; } else {
      this.D_AGGIORNAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_TRACCIA = null; } else {
      this.N_ID_TRACCIA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_S_PREC = null; } else {
      this.N_ID_S_PREC = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DENOM = null; } else {
      this.T_DENOM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DETTAGLIO_CF = null; } else {
      this.T_DETTAGLIO_CF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DETTAGLIO_PIVA = null; } else {
      this.T_DETTAGLIO_PIVA = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CLIENTE = null; } else {
      this.N_ID_CLIENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOME = null; } else {
      this.T_NOME = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COGNOME = null; } else {
      this.T_COGNOME = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RAGSOC = null; } else {
      this.T_RAGSOC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_PERSONA_FISICA = null; } else {
      this.B_PERSONA_FISICA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CF = null; } else {
      this.T_CF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA = null; } else {
      this.T_PIVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_SEDELEGALE = null; } else {
      this.N_ID_SEDELEGALE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_EMAIL = null; } else {
      this.T_EMAIL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_ATECO = null; } else {
      this.T_CODICE_ATECO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_DIRITTO_MT = null; } else {
      this.B_DIRITTO_MT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_AUTOCERT_MT = null; } else {
      this.D_AUTOCERT_MT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOTA = null; } else {
      this.T_NOTA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_AGGIORNAMENTO = null; } else {
      this.D_AGGIORNAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_TRACCIA = null; } else {
      this.N_ID_TRACCIA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_S_PREC = null; } else {
      this.N_ID_S_PREC = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DENOM = null; } else {
      this.T_DENOM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DETTAGLIO_CF = null; } else {
      this.T_DETTAGLIO_CF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DETTAGLIO_PIVA = null; } else {
      this.T_DETTAGLIO_PIVA = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    rcu_rcu_clientefinale o = (rcu_rcu_clientefinale) super.clone();
    return o;
  }

  public void clone0(rcu_rcu_clientefinale o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_CLIENTE", this.N_ID_CLIENTE);
    __sqoop$field_map.put("T_NOME", this.T_NOME);
    __sqoop$field_map.put("T_COGNOME", this.T_COGNOME);
    __sqoop$field_map.put("T_RAGSOC", this.T_RAGSOC);
    __sqoop$field_map.put("B_PERSONA_FISICA", this.B_PERSONA_FISICA);
    __sqoop$field_map.put("T_CF", this.T_CF);
    __sqoop$field_map.put("T_PIVA", this.T_PIVA);
    __sqoop$field_map.put("N_ID_SEDELEGALE", this.N_ID_SEDELEGALE);
    __sqoop$field_map.put("T_EMAIL", this.T_EMAIL);
    __sqoop$field_map.put("T_CODICE_ATECO", this.T_CODICE_ATECO);
    __sqoop$field_map.put("B_DIRITTO_MT", this.B_DIRITTO_MT);
    __sqoop$field_map.put("D_AUTOCERT_MT", this.D_AUTOCERT_MT);
    __sqoop$field_map.put("T_NOTA", this.T_NOTA);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("T_DENOM", this.T_DENOM);
    __sqoop$field_map.put("T_DETTAGLIO_CF", this.T_DETTAGLIO_CF);
    __sqoop$field_map.put("T_DETTAGLIO_PIVA", this.T_DETTAGLIO_PIVA);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_CLIENTE", this.N_ID_CLIENTE);
    __sqoop$field_map.put("T_NOME", this.T_NOME);
    __sqoop$field_map.put("T_COGNOME", this.T_COGNOME);
    __sqoop$field_map.put("T_RAGSOC", this.T_RAGSOC);
    __sqoop$field_map.put("B_PERSONA_FISICA", this.B_PERSONA_FISICA);
    __sqoop$field_map.put("T_CF", this.T_CF);
    __sqoop$field_map.put("T_PIVA", this.T_PIVA);
    __sqoop$field_map.put("N_ID_SEDELEGALE", this.N_ID_SEDELEGALE);
    __sqoop$field_map.put("T_EMAIL", this.T_EMAIL);
    __sqoop$field_map.put("T_CODICE_ATECO", this.T_CODICE_ATECO);
    __sqoop$field_map.put("B_DIRITTO_MT", this.B_DIRITTO_MT);
    __sqoop$field_map.put("D_AUTOCERT_MT", this.D_AUTOCERT_MT);
    __sqoop$field_map.put("T_NOTA", this.T_NOTA);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("T_DENOM", this.T_DENOM);
    __sqoop$field_map.put("T_DETTAGLIO_CF", this.T_DETTAGLIO_CF);
    __sqoop$field_map.put("T_DETTAGLIO_PIVA", this.T_DETTAGLIO_PIVA);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
