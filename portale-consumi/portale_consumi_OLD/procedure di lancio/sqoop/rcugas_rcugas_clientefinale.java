// ORM class for table 'rcugas.rcugas_clientefinale'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 11:25:02 CEST 2019
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

public class rcugas_rcugas_clientefinale extends SqoopRecord  implements DBWritable, Writable {
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
    setters.put("T_CODICE_FISCALE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_FISCALE = (String)value;
      }
    });
    setters.put("T_PARTITA_IVA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PARTITA_IVA = (String)value;
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
    setters.put("T_RAGIONE_SOCIALE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_RAGIONE_SOCIALE = (String)value;
      }
    });
    setters.put("T_NOTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_NOTE = (String)value;
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
    setters.put("T_SEDE_LEGALE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_SEDE_LEGALE = (String)value;
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
    setters.put("T_DETTAGLIO_ANACLI", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_DETTAGLIO_ANACLI = (String)value;
      }
    });
    setters.put("D_DATA_RIF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_RIF = (String)value;
      }
    });
    setters.put("T_CODICE_ATECO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_ATECO = (String)value;
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
    setters.put("T_TELEFONO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TELEFONO = (String)value;
      }
    });
    setters.put("T_EMAIL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_EMAIL = (String)value;
      }
    });
  }
  public rcugas_rcugas_clientefinale() {
    init0();
  }
  private java.math.BigDecimal N_ID_CLIENTE;
  public java.math.BigDecimal get_N_ID_CLIENTE() {
    return N_ID_CLIENTE;
  }
  public void set_N_ID_CLIENTE(java.math.BigDecimal N_ID_CLIENTE) {
    this.N_ID_CLIENTE = N_ID_CLIENTE;
  }
  public rcugas_rcugas_clientefinale with_N_ID_CLIENTE(java.math.BigDecimal N_ID_CLIENTE) {
    this.N_ID_CLIENTE = N_ID_CLIENTE;
    return this;
  }
  private String T_CODICE_FISCALE;
  public String get_T_CODICE_FISCALE() {
    return T_CODICE_FISCALE;
  }
  public void set_T_CODICE_FISCALE(String T_CODICE_FISCALE) {
    this.T_CODICE_FISCALE = T_CODICE_FISCALE;
  }
  public rcugas_rcugas_clientefinale with_T_CODICE_FISCALE(String T_CODICE_FISCALE) {
    this.T_CODICE_FISCALE = T_CODICE_FISCALE;
    return this;
  }
  private String T_PARTITA_IVA;
  public String get_T_PARTITA_IVA() {
    return T_PARTITA_IVA;
  }
  public void set_T_PARTITA_IVA(String T_PARTITA_IVA) {
    this.T_PARTITA_IVA = T_PARTITA_IVA;
  }
  public rcugas_rcugas_clientefinale with_T_PARTITA_IVA(String T_PARTITA_IVA) {
    this.T_PARTITA_IVA = T_PARTITA_IVA;
    return this;
  }
  private String T_NOME;
  public String get_T_NOME() {
    return T_NOME;
  }
  public void set_T_NOME(String T_NOME) {
    this.T_NOME = T_NOME;
  }
  public rcugas_rcugas_clientefinale with_T_NOME(String T_NOME) {
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
  public rcugas_rcugas_clientefinale with_T_COGNOME(String T_COGNOME) {
    this.T_COGNOME = T_COGNOME;
    return this;
  }
  private String T_RAGIONE_SOCIALE;
  public String get_T_RAGIONE_SOCIALE() {
    return T_RAGIONE_SOCIALE;
  }
  public void set_T_RAGIONE_SOCIALE(String T_RAGIONE_SOCIALE) {
    this.T_RAGIONE_SOCIALE = T_RAGIONE_SOCIALE;
  }
  public rcugas_rcugas_clientefinale with_T_RAGIONE_SOCIALE(String T_RAGIONE_SOCIALE) {
    this.T_RAGIONE_SOCIALE = T_RAGIONE_SOCIALE;
    return this;
  }
  private String T_NOTE;
  public String get_T_NOTE() {
    return T_NOTE;
  }
  public void set_T_NOTE(String T_NOTE) {
    this.T_NOTE = T_NOTE;
  }
  public rcugas_rcugas_clientefinale with_T_NOTE(String T_NOTE) {
    this.T_NOTE = T_NOTE;
    return this;
  }
  private String T_DETTAGLIO_CF;
  public String get_T_DETTAGLIO_CF() {
    return T_DETTAGLIO_CF;
  }
  public void set_T_DETTAGLIO_CF(String T_DETTAGLIO_CF) {
    this.T_DETTAGLIO_CF = T_DETTAGLIO_CF;
  }
  public rcugas_rcugas_clientefinale with_T_DETTAGLIO_CF(String T_DETTAGLIO_CF) {
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
  public rcugas_rcugas_clientefinale with_T_DETTAGLIO_PIVA(String T_DETTAGLIO_PIVA) {
    this.T_DETTAGLIO_PIVA = T_DETTAGLIO_PIVA;
    return this;
  }
  private String T_SEDE_LEGALE;
  public String get_T_SEDE_LEGALE() {
    return T_SEDE_LEGALE;
  }
  public void set_T_SEDE_LEGALE(String T_SEDE_LEGALE) {
    this.T_SEDE_LEGALE = T_SEDE_LEGALE;
  }
  public rcugas_rcugas_clientefinale with_T_SEDE_LEGALE(String T_SEDE_LEGALE) {
    this.T_SEDE_LEGALE = T_SEDE_LEGALE;
    return this;
  }
  private String D_AGGIORNAMENTO;
  public String get_D_AGGIORNAMENTO() {
    return D_AGGIORNAMENTO;
  }
  public void set_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
    this.D_AGGIORNAMENTO = D_AGGIORNAMENTO;
  }
  public rcugas_rcugas_clientefinale with_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
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
  public rcugas_rcugas_clientefinale with_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
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
  public rcugas_rcugas_clientefinale with_N_ID_S_PREC(java.math.BigDecimal N_ID_S_PREC) {
    this.N_ID_S_PREC = N_ID_S_PREC;
    return this;
  }
  private String T_DETTAGLIO_ANACLI;
  public String get_T_DETTAGLIO_ANACLI() {
    return T_DETTAGLIO_ANACLI;
  }
  public void set_T_DETTAGLIO_ANACLI(String T_DETTAGLIO_ANACLI) {
    this.T_DETTAGLIO_ANACLI = T_DETTAGLIO_ANACLI;
  }
  public rcugas_rcugas_clientefinale with_T_DETTAGLIO_ANACLI(String T_DETTAGLIO_ANACLI) {
    this.T_DETTAGLIO_ANACLI = T_DETTAGLIO_ANACLI;
    return this;
  }
  private String D_DATA_RIF;
  public String get_D_DATA_RIF() {
    return D_DATA_RIF;
  }
  public void set_D_DATA_RIF(String D_DATA_RIF) {
    this.D_DATA_RIF = D_DATA_RIF;
  }
  public rcugas_rcugas_clientefinale with_D_DATA_RIF(String D_DATA_RIF) {
    this.D_DATA_RIF = D_DATA_RIF;
    return this;
  }
  private String T_CODICE_ATECO;
  public String get_T_CODICE_ATECO() {
    return T_CODICE_ATECO;
  }
  public void set_T_CODICE_ATECO(String T_CODICE_ATECO) {
    this.T_CODICE_ATECO = T_CODICE_ATECO;
  }
  public rcugas_rcugas_clientefinale with_T_CODICE_ATECO(String T_CODICE_ATECO) {
    this.T_CODICE_ATECO = T_CODICE_ATECO;
    return this;
  }
  private String B_CF_STRANIERO;
  public String get_B_CF_STRANIERO() {
    return B_CF_STRANIERO;
  }
  public void set_B_CF_STRANIERO(String B_CF_STRANIERO) {
    this.B_CF_STRANIERO = B_CF_STRANIERO;
  }
  public rcugas_rcugas_clientefinale with_B_CF_STRANIERO(String B_CF_STRANIERO) {
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
  public rcugas_rcugas_clientefinale with_B_PERSONA_FISICA(String B_PERSONA_FISICA) {
    this.B_PERSONA_FISICA = B_PERSONA_FISICA;
    return this;
  }
  private String T_TELEFONO;
  public String get_T_TELEFONO() {
    return T_TELEFONO;
  }
  public void set_T_TELEFONO(String T_TELEFONO) {
    this.T_TELEFONO = T_TELEFONO;
  }
  public rcugas_rcugas_clientefinale with_T_TELEFONO(String T_TELEFONO) {
    this.T_TELEFONO = T_TELEFONO;
    return this;
  }
  private String T_EMAIL;
  public String get_T_EMAIL() {
    return T_EMAIL;
  }
  public void set_T_EMAIL(String T_EMAIL) {
    this.T_EMAIL = T_EMAIL;
  }
  public rcugas_rcugas_clientefinale with_T_EMAIL(String T_EMAIL) {
    this.T_EMAIL = T_EMAIL;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcugas_rcugas_clientefinale)) {
      return false;
    }
    rcugas_rcugas_clientefinale that = (rcugas_rcugas_clientefinale) o;
    boolean equal = true;
    equal = equal && (this.N_ID_CLIENTE == null ? that.N_ID_CLIENTE == null : this.N_ID_CLIENTE.equals(that.N_ID_CLIENTE));
    equal = equal && (this.T_CODICE_FISCALE == null ? that.T_CODICE_FISCALE == null : this.T_CODICE_FISCALE.equals(that.T_CODICE_FISCALE));
    equal = equal && (this.T_PARTITA_IVA == null ? that.T_PARTITA_IVA == null : this.T_PARTITA_IVA.equals(that.T_PARTITA_IVA));
    equal = equal && (this.T_NOME == null ? that.T_NOME == null : this.T_NOME.equals(that.T_NOME));
    equal = equal && (this.T_COGNOME == null ? that.T_COGNOME == null : this.T_COGNOME.equals(that.T_COGNOME));
    equal = equal && (this.T_RAGIONE_SOCIALE == null ? that.T_RAGIONE_SOCIALE == null : this.T_RAGIONE_SOCIALE.equals(that.T_RAGIONE_SOCIALE));
    equal = equal && (this.T_NOTE == null ? that.T_NOTE == null : this.T_NOTE.equals(that.T_NOTE));
    equal = equal && (this.T_DETTAGLIO_CF == null ? that.T_DETTAGLIO_CF == null : this.T_DETTAGLIO_CF.equals(that.T_DETTAGLIO_CF));
    equal = equal && (this.T_DETTAGLIO_PIVA == null ? that.T_DETTAGLIO_PIVA == null : this.T_DETTAGLIO_PIVA.equals(that.T_DETTAGLIO_PIVA));
    equal = equal && (this.T_SEDE_LEGALE == null ? that.T_SEDE_LEGALE == null : this.T_SEDE_LEGALE.equals(that.T_SEDE_LEGALE));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.T_DETTAGLIO_ANACLI == null ? that.T_DETTAGLIO_ANACLI == null : this.T_DETTAGLIO_ANACLI.equals(that.T_DETTAGLIO_ANACLI));
    equal = equal && (this.D_DATA_RIF == null ? that.D_DATA_RIF == null : this.D_DATA_RIF.equals(that.D_DATA_RIF));
    equal = equal && (this.T_CODICE_ATECO == null ? that.T_CODICE_ATECO == null : this.T_CODICE_ATECO.equals(that.T_CODICE_ATECO));
    equal = equal && (this.B_CF_STRANIERO == null ? that.B_CF_STRANIERO == null : this.B_CF_STRANIERO.equals(that.B_CF_STRANIERO));
    equal = equal && (this.B_PERSONA_FISICA == null ? that.B_PERSONA_FISICA == null : this.B_PERSONA_FISICA.equals(that.B_PERSONA_FISICA));
    equal = equal && (this.T_TELEFONO == null ? that.T_TELEFONO == null : this.T_TELEFONO.equals(that.T_TELEFONO));
    equal = equal && (this.T_EMAIL == null ? that.T_EMAIL == null : this.T_EMAIL.equals(that.T_EMAIL));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcugas_rcugas_clientefinale)) {
      return false;
    }
    rcugas_rcugas_clientefinale that = (rcugas_rcugas_clientefinale) o;
    boolean equal = true;
    equal = equal && (this.N_ID_CLIENTE == null ? that.N_ID_CLIENTE == null : this.N_ID_CLIENTE.equals(that.N_ID_CLIENTE));
    equal = equal && (this.T_CODICE_FISCALE == null ? that.T_CODICE_FISCALE == null : this.T_CODICE_FISCALE.equals(that.T_CODICE_FISCALE));
    equal = equal && (this.T_PARTITA_IVA == null ? that.T_PARTITA_IVA == null : this.T_PARTITA_IVA.equals(that.T_PARTITA_IVA));
    equal = equal && (this.T_NOME == null ? that.T_NOME == null : this.T_NOME.equals(that.T_NOME));
    equal = equal && (this.T_COGNOME == null ? that.T_COGNOME == null : this.T_COGNOME.equals(that.T_COGNOME));
    equal = equal && (this.T_RAGIONE_SOCIALE == null ? that.T_RAGIONE_SOCIALE == null : this.T_RAGIONE_SOCIALE.equals(that.T_RAGIONE_SOCIALE));
    equal = equal && (this.T_NOTE == null ? that.T_NOTE == null : this.T_NOTE.equals(that.T_NOTE));
    equal = equal && (this.T_DETTAGLIO_CF == null ? that.T_DETTAGLIO_CF == null : this.T_DETTAGLIO_CF.equals(that.T_DETTAGLIO_CF));
    equal = equal && (this.T_DETTAGLIO_PIVA == null ? that.T_DETTAGLIO_PIVA == null : this.T_DETTAGLIO_PIVA.equals(that.T_DETTAGLIO_PIVA));
    equal = equal && (this.T_SEDE_LEGALE == null ? that.T_SEDE_LEGALE == null : this.T_SEDE_LEGALE.equals(that.T_SEDE_LEGALE));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.T_DETTAGLIO_ANACLI == null ? that.T_DETTAGLIO_ANACLI == null : this.T_DETTAGLIO_ANACLI.equals(that.T_DETTAGLIO_ANACLI));
    equal = equal && (this.D_DATA_RIF == null ? that.D_DATA_RIF == null : this.D_DATA_RIF.equals(that.D_DATA_RIF));
    equal = equal && (this.T_CODICE_ATECO == null ? that.T_CODICE_ATECO == null : this.T_CODICE_ATECO.equals(that.T_CODICE_ATECO));
    equal = equal && (this.B_CF_STRANIERO == null ? that.B_CF_STRANIERO == null : this.B_CF_STRANIERO.equals(that.B_CF_STRANIERO));
    equal = equal && (this.B_PERSONA_FISICA == null ? that.B_PERSONA_FISICA == null : this.B_PERSONA_FISICA.equals(that.B_PERSONA_FISICA));
    equal = equal && (this.T_TELEFONO == null ? that.T_TELEFONO == null : this.T_TELEFONO.equals(that.T_TELEFONO));
    equal = equal && (this.T_EMAIL == null ? that.T_EMAIL == null : this.T_EMAIL.equals(that.T_EMAIL));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_CLIENTE = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.T_CODICE_FISCALE = JdbcWritableBridge.readString(2, __dbResults);
    this.T_PARTITA_IVA = JdbcWritableBridge.readString(3, __dbResults);
    this.T_NOME = JdbcWritableBridge.readString(4, __dbResults);
    this.T_COGNOME = JdbcWritableBridge.readString(5, __dbResults);
    this.T_RAGIONE_SOCIALE = JdbcWritableBridge.readString(6, __dbResults);
    this.T_NOTE = JdbcWritableBridge.readString(7, __dbResults);
    this.T_DETTAGLIO_CF = JdbcWritableBridge.readString(8, __dbResults);
    this.T_DETTAGLIO_PIVA = JdbcWritableBridge.readString(9, __dbResults);
    this.T_SEDE_LEGALE = JdbcWritableBridge.readString(10, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(11, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.T_DETTAGLIO_ANACLI = JdbcWritableBridge.readString(14, __dbResults);
    this.D_DATA_RIF = JdbcWritableBridge.readString(15, __dbResults);
    this.T_CODICE_ATECO = JdbcWritableBridge.readString(16, __dbResults);
    this.B_CF_STRANIERO = JdbcWritableBridge.readString(17, __dbResults);
    this.B_PERSONA_FISICA = JdbcWritableBridge.readString(18, __dbResults);
    this.T_TELEFONO = JdbcWritableBridge.readString(19, __dbResults);
    this.T_EMAIL = JdbcWritableBridge.readString(20, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_CLIENTE = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.T_CODICE_FISCALE = JdbcWritableBridge.readString(2, __dbResults);
    this.T_PARTITA_IVA = JdbcWritableBridge.readString(3, __dbResults);
    this.T_NOME = JdbcWritableBridge.readString(4, __dbResults);
    this.T_COGNOME = JdbcWritableBridge.readString(5, __dbResults);
    this.T_RAGIONE_SOCIALE = JdbcWritableBridge.readString(6, __dbResults);
    this.T_NOTE = JdbcWritableBridge.readString(7, __dbResults);
    this.T_DETTAGLIO_CF = JdbcWritableBridge.readString(8, __dbResults);
    this.T_DETTAGLIO_PIVA = JdbcWritableBridge.readString(9, __dbResults);
    this.T_SEDE_LEGALE = JdbcWritableBridge.readString(10, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(11, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.T_DETTAGLIO_ANACLI = JdbcWritableBridge.readString(14, __dbResults);
    this.D_DATA_RIF = JdbcWritableBridge.readString(15, __dbResults);
    this.T_CODICE_ATECO = JdbcWritableBridge.readString(16, __dbResults);
    this.B_CF_STRANIERO = JdbcWritableBridge.readString(17, __dbResults);
    this.B_PERSONA_FISICA = JdbcWritableBridge.readString(18, __dbResults);
    this.T_TELEFONO = JdbcWritableBridge.readString(19, __dbResults);
    this.T_EMAIL = JdbcWritableBridge.readString(20, __dbResults);
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
    JdbcWritableBridge.writeString(T_CODICE_FISCALE, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PARTITA_IVA, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOME, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COGNOME, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_RAGIONE_SOCIALE, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTE, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DETTAGLIO_CF, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DETTAGLIO_PIVA, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_SEDE_LEGALE, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 11 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 12 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_DETTAGLIO_ANACLI, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF, 15 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_ATECO, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_CF_STRANIERO, 17 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_PERSONA_FISICA, 18 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_TELEFONO, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_EMAIL, 20 + __off, 12, __dbStmt);
    return 20;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_CLIENTE, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_FISCALE, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PARTITA_IVA, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOME, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COGNOME, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_RAGIONE_SOCIALE, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTE, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DETTAGLIO_CF, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DETTAGLIO_PIVA, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_SEDE_LEGALE, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 11 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 12 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_DETTAGLIO_ANACLI, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF, 15 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_ATECO, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_CF_STRANIERO, 17 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_PERSONA_FISICA, 18 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_TELEFONO, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_EMAIL, 20 + __off, 12, __dbStmt);
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
        this.T_CODICE_FISCALE = null;
    } else {
    this.T_CODICE_FISCALE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PARTITA_IVA = null;
    } else {
    this.T_PARTITA_IVA = Text.readString(__dataIn);
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
        this.T_RAGIONE_SOCIALE = null;
    } else {
    this.T_RAGIONE_SOCIALE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_NOTE = null;
    } else {
    this.T_NOTE = Text.readString(__dataIn);
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
    if (__dataIn.readBoolean()) { 
        this.T_SEDE_LEGALE = null;
    } else {
    this.T_SEDE_LEGALE = Text.readString(__dataIn);
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
        this.T_DETTAGLIO_ANACLI = null;
    } else {
    this.T_DETTAGLIO_ANACLI = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_RIF = null;
    } else {
    this.D_DATA_RIF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_ATECO = null;
    } else {
    this.T_CODICE_ATECO = Text.readString(__dataIn);
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
        this.T_TELEFONO = null;
    } else {
    this.T_TELEFONO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_EMAIL = null;
    } else {
    this.T_EMAIL = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_CLIENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CLIENTE, __dataOut);
    }
    if (null == this.T_CODICE_FISCALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_FISCALE);
    }
    if (null == this.T_PARTITA_IVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PARTITA_IVA);
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
    if (null == this.T_RAGIONE_SOCIALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RAGIONE_SOCIALE);
    }
    if (null == this.T_NOTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOTE);
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
    if (null == this.T_SEDE_LEGALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_SEDE_LEGALE);
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
    if (null == this.T_DETTAGLIO_ANACLI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DETTAGLIO_ANACLI);
    }
    if (null == this.D_DATA_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF);
    }
    if (null == this.T_CODICE_ATECO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_ATECO);
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
    if (null == this.T_TELEFONO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TELEFONO);
    }
    if (null == this.T_EMAIL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_EMAIL);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_CLIENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CLIENTE, __dataOut);
    }
    if (null == this.T_CODICE_FISCALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_FISCALE);
    }
    if (null == this.T_PARTITA_IVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PARTITA_IVA);
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
    if (null == this.T_RAGIONE_SOCIALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RAGIONE_SOCIALE);
    }
    if (null == this.T_NOTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOTE);
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
    if (null == this.T_SEDE_LEGALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_SEDE_LEGALE);
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
    if (null == this.T_DETTAGLIO_ANACLI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DETTAGLIO_ANACLI);
    }
    if (null == this.D_DATA_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF);
    }
    if (null == this.T_CODICE_ATECO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_ATECO);
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
    if (null == this.T_TELEFONO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TELEFONO);
    }
    if (null == this.T_EMAIL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_EMAIL);
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_FISCALE==null?"":T_CODICE_FISCALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PARTITA_IVA==null?"":T_PARTITA_IVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOME==null?"":T_NOME, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COGNOME==null?"":T_COGNOME, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RAGIONE_SOCIALE==null?"":T_RAGIONE_SOCIALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOTE==null?"":T_NOTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DETTAGLIO_CF==null?"":T_DETTAGLIO_CF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DETTAGLIO_PIVA==null?"":T_DETTAGLIO_PIVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_SEDE_LEGALE==null?"":T_SEDE_LEGALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO==null?"":D_AGGIORNAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TRACCIA==null?"":N_ID_TRACCIA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_PREC==null?"":N_ID_S_PREC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DETTAGLIO_ANACLI==null?"":T_DETTAGLIO_ANACLI, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF==null?"":D_DATA_RIF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_ATECO==null?"":T_CODICE_ATECO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_CF_STRANIERO==null?"":B_CF_STRANIERO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_PERSONA_FISICA==null?"":B_PERSONA_FISICA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TELEFONO==null?"":T_TELEFONO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_EMAIL==null?"":T_EMAIL, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CLIENTE==null?"":N_ID_CLIENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_FISCALE==null?"":T_CODICE_FISCALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PARTITA_IVA==null?"":T_PARTITA_IVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOME==null?"":T_NOME, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COGNOME==null?"":T_COGNOME, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RAGIONE_SOCIALE==null?"":T_RAGIONE_SOCIALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOTE==null?"":T_NOTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DETTAGLIO_CF==null?"":T_DETTAGLIO_CF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DETTAGLIO_PIVA==null?"":T_DETTAGLIO_PIVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_SEDE_LEGALE==null?"":T_SEDE_LEGALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO==null?"":D_AGGIORNAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TRACCIA==null?"":N_ID_TRACCIA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_PREC==null?"":N_ID_S_PREC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DETTAGLIO_ANACLI==null?"":T_DETTAGLIO_ANACLI, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF==null?"":D_DATA_RIF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_ATECO==null?"":T_CODICE_ATECO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_CF_STRANIERO==null?"":B_CF_STRANIERO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_PERSONA_FISICA==null?"":B_PERSONA_FISICA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TELEFONO==null?"":T_TELEFONO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_EMAIL==null?"":T_EMAIL, " ", delimiters));
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
    if (__cur_str.equals("null")) { this.T_CODICE_FISCALE = null; } else {
      this.T_CODICE_FISCALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PARTITA_IVA = null; } else {
      this.T_PARTITA_IVA = __cur_str;
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
    if (__cur_str.equals("null")) { this.T_RAGIONE_SOCIALE = null; } else {
      this.T_RAGIONE_SOCIALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOTE = null; } else {
      this.T_NOTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DETTAGLIO_CF = null; } else {
      this.T_DETTAGLIO_CF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DETTAGLIO_PIVA = null; } else {
      this.T_DETTAGLIO_PIVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_SEDE_LEGALE = null; } else {
      this.T_SEDE_LEGALE = __cur_str;
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
    if (__cur_str.equals("null")) { this.T_DETTAGLIO_ANACLI = null; } else {
      this.T_DETTAGLIO_ANACLI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RIF = null; } else {
      this.D_DATA_RIF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_ATECO = null; } else {
      this.T_CODICE_ATECO = __cur_str;
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
    if (__cur_str.equals("null")) { this.T_TELEFONO = null; } else {
      this.T_TELEFONO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_EMAIL = null; } else {
      this.T_EMAIL = __cur_str;
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
    if (__cur_str.equals("null")) { this.T_CODICE_FISCALE = null; } else {
      this.T_CODICE_FISCALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PARTITA_IVA = null; } else {
      this.T_PARTITA_IVA = __cur_str;
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
    if (__cur_str.equals("null")) { this.T_RAGIONE_SOCIALE = null; } else {
      this.T_RAGIONE_SOCIALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOTE = null; } else {
      this.T_NOTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DETTAGLIO_CF = null; } else {
      this.T_DETTAGLIO_CF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DETTAGLIO_PIVA = null; } else {
      this.T_DETTAGLIO_PIVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_SEDE_LEGALE = null; } else {
      this.T_SEDE_LEGALE = __cur_str;
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
    if (__cur_str.equals("null")) { this.T_DETTAGLIO_ANACLI = null; } else {
      this.T_DETTAGLIO_ANACLI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RIF = null; } else {
      this.D_DATA_RIF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_ATECO = null; } else {
      this.T_CODICE_ATECO = __cur_str;
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
    if (__cur_str.equals("null")) { this.T_TELEFONO = null; } else {
      this.T_TELEFONO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_EMAIL = null; } else {
      this.T_EMAIL = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    rcugas_rcugas_clientefinale o = (rcugas_rcugas_clientefinale) super.clone();
    return o;
  }

  public void clone0(rcugas_rcugas_clientefinale o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_CLIENTE", this.N_ID_CLIENTE);
    __sqoop$field_map.put("T_CODICE_FISCALE", this.T_CODICE_FISCALE);
    __sqoop$field_map.put("T_PARTITA_IVA", this.T_PARTITA_IVA);
    __sqoop$field_map.put("T_NOME", this.T_NOME);
    __sqoop$field_map.put("T_COGNOME", this.T_COGNOME);
    __sqoop$field_map.put("T_RAGIONE_SOCIALE", this.T_RAGIONE_SOCIALE);
    __sqoop$field_map.put("T_NOTE", this.T_NOTE);
    __sqoop$field_map.put("T_DETTAGLIO_CF", this.T_DETTAGLIO_CF);
    __sqoop$field_map.put("T_DETTAGLIO_PIVA", this.T_DETTAGLIO_PIVA);
    __sqoop$field_map.put("T_SEDE_LEGALE", this.T_SEDE_LEGALE);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("T_DETTAGLIO_ANACLI", this.T_DETTAGLIO_ANACLI);
    __sqoop$field_map.put("D_DATA_RIF", this.D_DATA_RIF);
    __sqoop$field_map.put("T_CODICE_ATECO", this.T_CODICE_ATECO);
    __sqoop$field_map.put("B_CF_STRANIERO", this.B_CF_STRANIERO);
    __sqoop$field_map.put("B_PERSONA_FISICA", this.B_PERSONA_FISICA);
    __sqoop$field_map.put("T_TELEFONO", this.T_TELEFONO);
    __sqoop$field_map.put("T_EMAIL", this.T_EMAIL);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_CLIENTE", this.N_ID_CLIENTE);
    __sqoop$field_map.put("T_CODICE_FISCALE", this.T_CODICE_FISCALE);
    __sqoop$field_map.put("T_PARTITA_IVA", this.T_PARTITA_IVA);
    __sqoop$field_map.put("T_NOME", this.T_NOME);
    __sqoop$field_map.put("T_COGNOME", this.T_COGNOME);
    __sqoop$field_map.put("T_RAGIONE_SOCIALE", this.T_RAGIONE_SOCIALE);
    __sqoop$field_map.put("T_NOTE", this.T_NOTE);
    __sqoop$field_map.put("T_DETTAGLIO_CF", this.T_DETTAGLIO_CF);
    __sqoop$field_map.put("T_DETTAGLIO_PIVA", this.T_DETTAGLIO_PIVA);
    __sqoop$field_map.put("T_SEDE_LEGALE", this.T_SEDE_LEGALE);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("T_DETTAGLIO_ANACLI", this.T_DETTAGLIO_ANACLI);
    __sqoop$field_map.put("D_DATA_RIF", this.D_DATA_RIF);
    __sqoop$field_map.put("T_CODICE_ATECO", this.T_CODICE_ATECO);
    __sqoop$field_map.put("B_CF_STRANIERO", this.B_CF_STRANIERO);
    __sqoop$field_map.put("B_PERSONA_FISICA", this.B_PERSONA_FISICA);
    __sqoop$field_map.put("T_TELEFONO", this.T_TELEFONO);
    __sqoop$field_map.put("T_EMAIL", this.T_EMAIL);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
