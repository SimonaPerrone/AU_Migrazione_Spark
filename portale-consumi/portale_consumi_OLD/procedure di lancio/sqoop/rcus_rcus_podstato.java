// ORM class for table 'rcus.rcus_podstato'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Aug 10 10:59:12 CEST 2019
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

public class rcus_rcus_podstato extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_SCHEDA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_SCHEDA = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_POD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_POD = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_STATO_ATTIVAZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_STATO_ATTIVAZIONE = (String)value;
      }
    });
    setters.put("D_ATTIVAZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_ATTIVAZIONE = (String)value;
      }
    });
    setters.put("D_DISATTIVAZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DISATTIVAZIONE = (String)value;
      }
    });
    setters.put("T_CAUSALE_NO_RIATTIV", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CAUSALE_NO_RIATTIV = (String)value;
      }
    });
    setters.put("T_CAUSALE_NO_DISATTIV", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CAUSALE_NO_DISATTIV = (String)value;
      }
    });
    setters.put("T_STATO_SOSP", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_STATO_SOSP = (String)value;
      }
    });
    setters.put("D_SOSPENSIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_SOSPENSIONE = (String)value;
      }
    });
    setters.put("D_REVOCA_SOSP", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_REVOCA_SOSP = (String)value;
      }
    });
    setters.put("T_CAUSALE_NO_SOSP", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CAUSALE_NO_SOSP = (String)value;
      }
    });
    setters.put("T_SWITCHING", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_SWITCHING = (String)value;
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
    setters.put("D_ARCHIVIAZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_ARCHIVIAZIONE = (String)value;
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
    setters.put("N_ID_S_SUCC", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_S_SUCC = (java.math.BigDecimal)value;
      }
    });
    setters.put("B_VALIDO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_VALIDO = (String)value;
      }
    });
    setters.put("T_COD_DISATTIVAZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COD_DISATTIVAZIONE = (String)value;
      }
    });
  }
  public rcus_rcus_podstato() {
    init0();
  }
  private java.math.BigDecimal N_ID_SCHEDA;
  public java.math.BigDecimal get_N_ID_SCHEDA() {
    return N_ID_SCHEDA;
  }
  public void set_N_ID_SCHEDA(java.math.BigDecimal N_ID_SCHEDA) {
    this.N_ID_SCHEDA = N_ID_SCHEDA;
  }
  public rcus_rcus_podstato with_N_ID_SCHEDA(java.math.BigDecimal N_ID_SCHEDA) {
    this.N_ID_SCHEDA = N_ID_SCHEDA;
    return this;
  }
  private java.math.BigDecimal N_ID_POD;
  public java.math.BigDecimal get_N_ID_POD() {
    return N_ID_POD;
  }
  public void set_N_ID_POD(java.math.BigDecimal N_ID_POD) {
    this.N_ID_POD = N_ID_POD;
  }
  public rcus_rcus_podstato with_N_ID_POD(java.math.BigDecimal N_ID_POD) {
    this.N_ID_POD = N_ID_POD;
    return this;
  }
  private String T_STATO_ATTIVAZIONE;
  public String get_T_STATO_ATTIVAZIONE() {
    return T_STATO_ATTIVAZIONE;
  }
  public void set_T_STATO_ATTIVAZIONE(String T_STATO_ATTIVAZIONE) {
    this.T_STATO_ATTIVAZIONE = T_STATO_ATTIVAZIONE;
  }
  public rcus_rcus_podstato with_T_STATO_ATTIVAZIONE(String T_STATO_ATTIVAZIONE) {
    this.T_STATO_ATTIVAZIONE = T_STATO_ATTIVAZIONE;
    return this;
  }
  private String D_ATTIVAZIONE;
  public String get_D_ATTIVAZIONE() {
    return D_ATTIVAZIONE;
  }
  public void set_D_ATTIVAZIONE(String D_ATTIVAZIONE) {
    this.D_ATTIVAZIONE = D_ATTIVAZIONE;
  }
  public rcus_rcus_podstato with_D_ATTIVAZIONE(String D_ATTIVAZIONE) {
    this.D_ATTIVAZIONE = D_ATTIVAZIONE;
    return this;
  }
  private String D_DISATTIVAZIONE;
  public String get_D_DISATTIVAZIONE() {
    return D_DISATTIVAZIONE;
  }
  public void set_D_DISATTIVAZIONE(String D_DISATTIVAZIONE) {
    this.D_DISATTIVAZIONE = D_DISATTIVAZIONE;
  }
  public rcus_rcus_podstato with_D_DISATTIVAZIONE(String D_DISATTIVAZIONE) {
    this.D_DISATTIVAZIONE = D_DISATTIVAZIONE;
    return this;
  }
  private String T_CAUSALE_NO_RIATTIV;
  public String get_T_CAUSALE_NO_RIATTIV() {
    return T_CAUSALE_NO_RIATTIV;
  }
  public void set_T_CAUSALE_NO_RIATTIV(String T_CAUSALE_NO_RIATTIV) {
    this.T_CAUSALE_NO_RIATTIV = T_CAUSALE_NO_RIATTIV;
  }
  public rcus_rcus_podstato with_T_CAUSALE_NO_RIATTIV(String T_CAUSALE_NO_RIATTIV) {
    this.T_CAUSALE_NO_RIATTIV = T_CAUSALE_NO_RIATTIV;
    return this;
  }
  private String T_CAUSALE_NO_DISATTIV;
  public String get_T_CAUSALE_NO_DISATTIV() {
    return T_CAUSALE_NO_DISATTIV;
  }
  public void set_T_CAUSALE_NO_DISATTIV(String T_CAUSALE_NO_DISATTIV) {
    this.T_CAUSALE_NO_DISATTIV = T_CAUSALE_NO_DISATTIV;
  }
  public rcus_rcus_podstato with_T_CAUSALE_NO_DISATTIV(String T_CAUSALE_NO_DISATTIV) {
    this.T_CAUSALE_NO_DISATTIV = T_CAUSALE_NO_DISATTIV;
    return this;
  }
  private String T_STATO_SOSP;
  public String get_T_STATO_SOSP() {
    return T_STATO_SOSP;
  }
  public void set_T_STATO_SOSP(String T_STATO_SOSP) {
    this.T_STATO_SOSP = T_STATO_SOSP;
  }
  public rcus_rcus_podstato with_T_STATO_SOSP(String T_STATO_SOSP) {
    this.T_STATO_SOSP = T_STATO_SOSP;
    return this;
  }
  private String D_SOSPENSIONE;
  public String get_D_SOSPENSIONE() {
    return D_SOSPENSIONE;
  }
  public void set_D_SOSPENSIONE(String D_SOSPENSIONE) {
    this.D_SOSPENSIONE = D_SOSPENSIONE;
  }
  public rcus_rcus_podstato with_D_SOSPENSIONE(String D_SOSPENSIONE) {
    this.D_SOSPENSIONE = D_SOSPENSIONE;
    return this;
  }
  private String D_REVOCA_SOSP;
  public String get_D_REVOCA_SOSP() {
    return D_REVOCA_SOSP;
  }
  public void set_D_REVOCA_SOSP(String D_REVOCA_SOSP) {
    this.D_REVOCA_SOSP = D_REVOCA_SOSP;
  }
  public rcus_rcus_podstato with_D_REVOCA_SOSP(String D_REVOCA_SOSP) {
    this.D_REVOCA_SOSP = D_REVOCA_SOSP;
    return this;
  }
  private String T_CAUSALE_NO_SOSP;
  public String get_T_CAUSALE_NO_SOSP() {
    return T_CAUSALE_NO_SOSP;
  }
  public void set_T_CAUSALE_NO_SOSP(String T_CAUSALE_NO_SOSP) {
    this.T_CAUSALE_NO_SOSP = T_CAUSALE_NO_SOSP;
  }
  public rcus_rcus_podstato with_T_CAUSALE_NO_SOSP(String T_CAUSALE_NO_SOSP) {
    this.T_CAUSALE_NO_SOSP = T_CAUSALE_NO_SOSP;
    return this;
  }
  private String T_SWITCHING;
  public String get_T_SWITCHING() {
    return T_SWITCHING;
  }
  public void set_T_SWITCHING(String T_SWITCHING) {
    this.T_SWITCHING = T_SWITCHING;
  }
  public rcus_rcus_podstato with_T_SWITCHING(String T_SWITCHING) {
    this.T_SWITCHING = T_SWITCHING;
    return this;
  }
  private String T_NOTA;
  public String get_T_NOTA() {
    return T_NOTA;
  }
  public void set_T_NOTA(String T_NOTA) {
    this.T_NOTA = T_NOTA;
  }
  public rcus_rcus_podstato with_T_NOTA(String T_NOTA) {
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
  public rcus_rcus_podstato with_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
    this.D_AGGIORNAMENTO = D_AGGIORNAMENTO;
    return this;
  }
  private String D_ARCHIVIAZIONE;
  public String get_D_ARCHIVIAZIONE() {
    return D_ARCHIVIAZIONE;
  }
  public void set_D_ARCHIVIAZIONE(String D_ARCHIVIAZIONE) {
    this.D_ARCHIVIAZIONE = D_ARCHIVIAZIONE;
  }
  public rcus_rcus_podstato with_D_ARCHIVIAZIONE(String D_ARCHIVIAZIONE) {
    this.D_ARCHIVIAZIONE = D_ARCHIVIAZIONE;
    return this;
  }
  private java.math.BigDecimal N_ID_TRACCIA;
  public java.math.BigDecimal get_N_ID_TRACCIA() {
    return N_ID_TRACCIA;
  }
  public void set_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
    this.N_ID_TRACCIA = N_ID_TRACCIA;
  }
  public rcus_rcus_podstato with_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
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
  public rcus_rcus_podstato with_N_ID_S_PREC(java.math.BigDecimal N_ID_S_PREC) {
    this.N_ID_S_PREC = N_ID_S_PREC;
    return this;
  }
  private java.math.BigDecimal N_ID_S_SUCC;
  public java.math.BigDecimal get_N_ID_S_SUCC() {
    return N_ID_S_SUCC;
  }
  public void set_N_ID_S_SUCC(java.math.BigDecimal N_ID_S_SUCC) {
    this.N_ID_S_SUCC = N_ID_S_SUCC;
  }
  public rcus_rcus_podstato with_N_ID_S_SUCC(java.math.BigDecimal N_ID_S_SUCC) {
    this.N_ID_S_SUCC = N_ID_S_SUCC;
    return this;
  }
  private String B_VALIDO;
  public String get_B_VALIDO() {
    return B_VALIDO;
  }
  public void set_B_VALIDO(String B_VALIDO) {
    this.B_VALIDO = B_VALIDO;
  }
  public rcus_rcus_podstato with_B_VALIDO(String B_VALIDO) {
    this.B_VALIDO = B_VALIDO;
    return this;
  }
  private String T_COD_DISATTIVAZIONE;
  public String get_T_COD_DISATTIVAZIONE() {
    return T_COD_DISATTIVAZIONE;
  }
  public void set_T_COD_DISATTIVAZIONE(String T_COD_DISATTIVAZIONE) {
    this.T_COD_DISATTIVAZIONE = T_COD_DISATTIVAZIONE;
  }
  public rcus_rcus_podstato with_T_COD_DISATTIVAZIONE(String T_COD_DISATTIVAZIONE) {
    this.T_COD_DISATTIVAZIONE = T_COD_DISATTIVAZIONE;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcus_rcus_podstato)) {
      return false;
    }
    rcus_rcus_podstato that = (rcus_rcus_podstato) o;
    boolean equal = true;
    equal = equal && (this.N_ID_SCHEDA == null ? that.N_ID_SCHEDA == null : this.N_ID_SCHEDA.equals(that.N_ID_SCHEDA));
    equal = equal && (this.N_ID_POD == null ? that.N_ID_POD == null : this.N_ID_POD.equals(that.N_ID_POD));
    equal = equal && (this.T_STATO_ATTIVAZIONE == null ? that.T_STATO_ATTIVAZIONE == null : this.T_STATO_ATTIVAZIONE.equals(that.T_STATO_ATTIVAZIONE));
    equal = equal && (this.D_ATTIVAZIONE == null ? that.D_ATTIVAZIONE == null : this.D_ATTIVAZIONE.equals(that.D_ATTIVAZIONE));
    equal = equal && (this.D_DISATTIVAZIONE == null ? that.D_DISATTIVAZIONE == null : this.D_DISATTIVAZIONE.equals(that.D_DISATTIVAZIONE));
    equal = equal && (this.T_CAUSALE_NO_RIATTIV == null ? that.T_CAUSALE_NO_RIATTIV == null : this.T_CAUSALE_NO_RIATTIV.equals(that.T_CAUSALE_NO_RIATTIV));
    equal = equal && (this.T_CAUSALE_NO_DISATTIV == null ? that.T_CAUSALE_NO_DISATTIV == null : this.T_CAUSALE_NO_DISATTIV.equals(that.T_CAUSALE_NO_DISATTIV));
    equal = equal && (this.T_STATO_SOSP == null ? that.T_STATO_SOSP == null : this.T_STATO_SOSP.equals(that.T_STATO_SOSP));
    equal = equal && (this.D_SOSPENSIONE == null ? that.D_SOSPENSIONE == null : this.D_SOSPENSIONE.equals(that.D_SOSPENSIONE));
    equal = equal && (this.D_REVOCA_SOSP == null ? that.D_REVOCA_SOSP == null : this.D_REVOCA_SOSP.equals(that.D_REVOCA_SOSP));
    equal = equal && (this.T_CAUSALE_NO_SOSP == null ? that.T_CAUSALE_NO_SOSP == null : this.T_CAUSALE_NO_SOSP.equals(that.T_CAUSALE_NO_SOSP));
    equal = equal && (this.T_SWITCHING == null ? that.T_SWITCHING == null : this.T_SWITCHING.equals(that.T_SWITCHING));
    equal = equal && (this.T_NOTA == null ? that.T_NOTA == null : this.T_NOTA.equals(that.T_NOTA));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.D_ARCHIVIAZIONE == null ? that.D_ARCHIVIAZIONE == null : this.D_ARCHIVIAZIONE.equals(that.D_ARCHIVIAZIONE));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.N_ID_S_SUCC == null ? that.N_ID_S_SUCC == null : this.N_ID_S_SUCC.equals(that.N_ID_S_SUCC));
    equal = equal && (this.B_VALIDO == null ? that.B_VALIDO == null : this.B_VALIDO.equals(that.B_VALIDO));
    equal = equal && (this.T_COD_DISATTIVAZIONE == null ? that.T_COD_DISATTIVAZIONE == null : this.T_COD_DISATTIVAZIONE.equals(that.T_COD_DISATTIVAZIONE));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcus_rcus_podstato)) {
      return false;
    }
    rcus_rcus_podstato that = (rcus_rcus_podstato) o;
    boolean equal = true;
    equal = equal && (this.N_ID_SCHEDA == null ? that.N_ID_SCHEDA == null : this.N_ID_SCHEDA.equals(that.N_ID_SCHEDA));
    equal = equal && (this.N_ID_POD == null ? that.N_ID_POD == null : this.N_ID_POD.equals(that.N_ID_POD));
    equal = equal && (this.T_STATO_ATTIVAZIONE == null ? that.T_STATO_ATTIVAZIONE == null : this.T_STATO_ATTIVAZIONE.equals(that.T_STATO_ATTIVAZIONE));
    equal = equal && (this.D_ATTIVAZIONE == null ? that.D_ATTIVAZIONE == null : this.D_ATTIVAZIONE.equals(that.D_ATTIVAZIONE));
    equal = equal && (this.D_DISATTIVAZIONE == null ? that.D_DISATTIVAZIONE == null : this.D_DISATTIVAZIONE.equals(that.D_DISATTIVAZIONE));
    equal = equal && (this.T_CAUSALE_NO_RIATTIV == null ? that.T_CAUSALE_NO_RIATTIV == null : this.T_CAUSALE_NO_RIATTIV.equals(that.T_CAUSALE_NO_RIATTIV));
    equal = equal && (this.T_CAUSALE_NO_DISATTIV == null ? that.T_CAUSALE_NO_DISATTIV == null : this.T_CAUSALE_NO_DISATTIV.equals(that.T_CAUSALE_NO_DISATTIV));
    equal = equal && (this.T_STATO_SOSP == null ? that.T_STATO_SOSP == null : this.T_STATO_SOSP.equals(that.T_STATO_SOSP));
    equal = equal && (this.D_SOSPENSIONE == null ? that.D_SOSPENSIONE == null : this.D_SOSPENSIONE.equals(that.D_SOSPENSIONE));
    equal = equal && (this.D_REVOCA_SOSP == null ? that.D_REVOCA_SOSP == null : this.D_REVOCA_SOSP.equals(that.D_REVOCA_SOSP));
    equal = equal && (this.T_CAUSALE_NO_SOSP == null ? that.T_CAUSALE_NO_SOSP == null : this.T_CAUSALE_NO_SOSP.equals(that.T_CAUSALE_NO_SOSP));
    equal = equal && (this.T_SWITCHING == null ? that.T_SWITCHING == null : this.T_SWITCHING.equals(that.T_SWITCHING));
    equal = equal && (this.T_NOTA == null ? that.T_NOTA == null : this.T_NOTA.equals(that.T_NOTA));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.D_ARCHIVIAZIONE == null ? that.D_ARCHIVIAZIONE == null : this.D_ARCHIVIAZIONE.equals(that.D_ARCHIVIAZIONE));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.N_ID_S_SUCC == null ? that.N_ID_S_SUCC == null : this.N_ID_S_SUCC.equals(that.N_ID_S_SUCC));
    equal = equal && (this.B_VALIDO == null ? that.B_VALIDO == null : this.B_VALIDO.equals(that.B_VALIDO));
    equal = equal && (this.T_COD_DISATTIVAZIONE == null ? that.T_COD_DISATTIVAZIONE == null : this.T_COD_DISATTIVAZIONE.equals(that.T_COD_DISATTIVAZIONE));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_SCHEDA = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_POD = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_STATO_ATTIVAZIONE = JdbcWritableBridge.readString(3, __dbResults);
    this.D_ATTIVAZIONE = JdbcWritableBridge.readString(4, __dbResults);
    this.D_DISATTIVAZIONE = JdbcWritableBridge.readString(5, __dbResults);
    this.T_CAUSALE_NO_RIATTIV = JdbcWritableBridge.readString(6, __dbResults);
    this.T_CAUSALE_NO_DISATTIV = JdbcWritableBridge.readString(7, __dbResults);
    this.T_STATO_SOSP = JdbcWritableBridge.readString(8, __dbResults);
    this.D_SOSPENSIONE = JdbcWritableBridge.readString(9, __dbResults);
    this.D_REVOCA_SOSP = JdbcWritableBridge.readString(10, __dbResults);
    this.T_CAUSALE_NO_SOSP = JdbcWritableBridge.readString(11, __dbResults);
    this.T_SWITCHING = JdbcWritableBridge.readString(12, __dbResults);
    this.T_NOTA = JdbcWritableBridge.readString(13, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(14, __dbResults);
    this.D_ARCHIVIAZIONE = JdbcWritableBridge.readString(15, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(17, __dbResults);
    this.N_ID_S_SUCC = JdbcWritableBridge.readBigDecimal(18, __dbResults);
    this.B_VALIDO = JdbcWritableBridge.readString(19, __dbResults);
    this.T_COD_DISATTIVAZIONE = JdbcWritableBridge.readString(20, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_SCHEDA = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_POD = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_STATO_ATTIVAZIONE = JdbcWritableBridge.readString(3, __dbResults);
    this.D_ATTIVAZIONE = JdbcWritableBridge.readString(4, __dbResults);
    this.D_DISATTIVAZIONE = JdbcWritableBridge.readString(5, __dbResults);
    this.T_CAUSALE_NO_RIATTIV = JdbcWritableBridge.readString(6, __dbResults);
    this.T_CAUSALE_NO_DISATTIV = JdbcWritableBridge.readString(7, __dbResults);
    this.T_STATO_SOSP = JdbcWritableBridge.readString(8, __dbResults);
    this.D_SOSPENSIONE = JdbcWritableBridge.readString(9, __dbResults);
    this.D_REVOCA_SOSP = JdbcWritableBridge.readString(10, __dbResults);
    this.T_CAUSALE_NO_SOSP = JdbcWritableBridge.readString(11, __dbResults);
    this.T_SWITCHING = JdbcWritableBridge.readString(12, __dbResults);
    this.T_NOTA = JdbcWritableBridge.readString(13, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(14, __dbResults);
    this.D_ARCHIVIAZIONE = JdbcWritableBridge.readString(15, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(17, __dbResults);
    this.N_ID_S_SUCC = JdbcWritableBridge.readBigDecimal(18, __dbResults);
    this.B_VALIDO = JdbcWritableBridge.readString(19, __dbResults);
    this.T_COD_DISATTIVAZIONE = JdbcWritableBridge.readString(20, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_SCHEDA, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_POD, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO_ATTIVAZIONE, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_ATTIVAZIONE, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DISATTIVAZIONE, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_CAUSALE_NO_RIATTIV, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CAUSALE_NO_DISATTIV, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO_SOSP, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_SOSPENSIONE, 9 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_REVOCA_SOSP, 10 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_CAUSALE_NO_SOSP, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_SWITCHING, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTA, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 14 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_ARCHIVIAZIONE, 15 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 17 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_SUCC, 18 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_VALIDO, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_DISATTIVAZIONE, 20 + __off, 12, __dbStmt);
    return 20;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_SCHEDA, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_POD, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO_ATTIVAZIONE, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_ATTIVAZIONE, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DISATTIVAZIONE, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_CAUSALE_NO_RIATTIV, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CAUSALE_NO_DISATTIV, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO_SOSP, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_SOSPENSIONE, 9 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_REVOCA_SOSP, 10 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_CAUSALE_NO_SOSP, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_SWITCHING, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTA, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 14 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_ARCHIVIAZIONE, 15 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 17 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_SUCC, 18 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_VALIDO, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_DISATTIVAZIONE, 20 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_SCHEDA = null;
    } else {
    this.N_ID_SCHEDA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_POD = null;
    } else {
    this.N_ID_POD = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_STATO_ATTIVAZIONE = null;
    } else {
    this.T_STATO_ATTIVAZIONE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_ATTIVAZIONE = null;
    } else {
    this.D_ATTIVAZIONE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DISATTIVAZIONE = null;
    } else {
    this.D_DISATTIVAZIONE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CAUSALE_NO_RIATTIV = null;
    } else {
    this.T_CAUSALE_NO_RIATTIV = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CAUSALE_NO_DISATTIV = null;
    } else {
    this.T_CAUSALE_NO_DISATTIV = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_STATO_SOSP = null;
    } else {
    this.T_STATO_SOSP = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_SOSPENSIONE = null;
    } else {
    this.D_SOSPENSIONE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_REVOCA_SOSP = null;
    } else {
    this.D_REVOCA_SOSP = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CAUSALE_NO_SOSP = null;
    } else {
    this.T_CAUSALE_NO_SOSP = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_SWITCHING = null;
    } else {
    this.T_SWITCHING = Text.readString(__dataIn);
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
        this.D_ARCHIVIAZIONE = null;
    } else {
    this.D_ARCHIVIAZIONE = Text.readString(__dataIn);
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
        this.N_ID_S_SUCC = null;
    } else {
    this.N_ID_S_SUCC = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_VALIDO = null;
    } else {
    this.B_VALIDO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COD_DISATTIVAZIONE = null;
    } else {
    this.T_COD_DISATTIVAZIONE = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_SCHEDA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_SCHEDA, __dataOut);
    }
    if (null == this.N_ID_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_POD, __dataOut);
    }
    if (null == this.T_STATO_ATTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_STATO_ATTIVAZIONE);
    }
    if (null == this.D_ATTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_ATTIVAZIONE);
    }
    if (null == this.D_DISATTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DISATTIVAZIONE);
    }
    if (null == this.T_CAUSALE_NO_RIATTIV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CAUSALE_NO_RIATTIV);
    }
    if (null == this.T_CAUSALE_NO_DISATTIV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CAUSALE_NO_DISATTIV);
    }
    if (null == this.T_STATO_SOSP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_STATO_SOSP);
    }
    if (null == this.D_SOSPENSIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_SOSPENSIONE);
    }
    if (null == this.D_REVOCA_SOSP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_REVOCA_SOSP);
    }
    if (null == this.T_CAUSALE_NO_SOSP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CAUSALE_NO_SOSP);
    }
    if (null == this.T_SWITCHING) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_SWITCHING);
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
    if (null == this.D_ARCHIVIAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_ARCHIVIAZIONE);
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
    if (null == this.N_ID_S_SUCC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_S_SUCC, __dataOut);
    }
    if (null == this.B_VALIDO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_VALIDO);
    }
    if (null == this.T_COD_DISATTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_DISATTIVAZIONE);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_SCHEDA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_SCHEDA, __dataOut);
    }
    if (null == this.N_ID_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_POD, __dataOut);
    }
    if (null == this.T_STATO_ATTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_STATO_ATTIVAZIONE);
    }
    if (null == this.D_ATTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_ATTIVAZIONE);
    }
    if (null == this.D_DISATTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DISATTIVAZIONE);
    }
    if (null == this.T_CAUSALE_NO_RIATTIV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CAUSALE_NO_RIATTIV);
    }
    if (null == this.T_CAUSALE_NO_DISATTIV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CAUSALE_NO_DISATTIV);
    }
    if (null == this.T_STATO_SOSP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_STATO_SOSP);
    }
    if (null == this.D_SOSPENSIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_SOSPENSIONE);
    }
    if (null == this.D_REVOCA_SOSP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_REVOCA_SOSP);
    }
    if (null == this.T_CAUSALE_NO_SOSP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CAUSALE_NO_SOSP);
    }
    if (null == this.T_SWITCHING) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_SWITCHING);
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
    if (null == this.D_ARCHIVIAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_ARCHIVIAZIONE);
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
    if (null == this.N_ID_S_SUCC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_S_SUCC, __dataOut);
    }
    if (null == this.B_VALIDO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_VALIDO);
    }
    if (null == this.T_COD_DISATTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_DISATTIVAZIONE);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_SCHEDA==null?"":N_ID_SCHEDA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_POD==null?"":N_ID_POD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO_ATTIVAZIONE==null?"":T_STATO_ATTIVAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_ATTIVAZIONE==null?"":D_ATTIVAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DISATTIVAZIONE==null?"":D_DISATTIVAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CAUSALE_NO_RIATTIV==null?"":T_CAUSALE_NO_RIATTIV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CAUSALE_NO_DISATTIV==null?"":T_CAUSALE_NO_DISATTIV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO_SOSP==null?"":T_STATO_SOSP, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_SOSPENSIONE==null?"":D_SOSPENSIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_REVOCA_SOSP==null?"":D_REVOCA_SOSP, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CAUSALE_NO_SOSP==null?"":T_CAUSALE_NO_SOSP, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_SWITCHING==null?"":T_SWITCHING, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOTA==null?"":T_NOTA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO==null?"":D_AGGIORNAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_ARCHIVIAZIONE==null?"":D_ARCHIVIAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TRACCIA==null?"":N_ID_TRACCIA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_PREC==null?"":N_ID_S_PREC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_SUCC==null?"":N_ID_S_SUCC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_VALIDO==null?"":B_VALIDO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_DISATTIVAZIONE==null?"":T_COD_DISATTIVAZIONE, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_SCHEDA==null?"":N_ID_SCHEDA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_POD==null?"":N_ID_POD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO_ATTIVAZIONE==null?"":T_STATO_ATTIVAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_ATTIVAZIONE==null?"":D_ATTIVAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DISATTIVAZIONE==null?"":D_DISATTIVAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CAUSALE_NO_RIATTIV==null?"":T_CAUSALE_NO_RIATTIV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CAUSALE_NO_DISATTIV==null?"":T_CAUSALE_NO_DISATTIV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO_SOSP==null?"":T_STATO_SOSP, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_SOSPENSIONE==null?"":D_SOSPENSIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_REVOCA_SOSP==null?"":D_REVOCA_SOSP, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CAUSALE_NO_SOSP==null?"":T_CAUSALE_NO_SOSP, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_SWITCHING==null?"":T_SWITCHING, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOTA==null?"":T_NOTA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO==null?"":D_AGGIORNAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_ARCHIVIAZIONE==null?"":D_ARCHIVIAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TRACCIA==null?"":N_ID_TRACCIA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_PREC==null?"":N_ID_S_PREC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_SUCC==null?"":N_ID_S_SUCC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_VALIDO==null?"":B_VALIDO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_DISATTIVAZIONE==null?"":T_COD_DISATTIVAZIONE, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_SCHEDA = null; } else {
      this.N_ID_SCHEDA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_POD = null; } else {
      this.N_ID_POD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_STATO_ATTIVAZIONE = null; } else {
      this.T_STATO_ATTIVAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_ATTIVAZIONE = null; } else {
      this.D_ATTIVAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DISATTIVAZIONE = null; } else {
      this.D_DISATTIVAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CAUSALE_NO_RIATTIV = null; } else {
      this.T_CAUSALE_NO_RIATTIV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CAUSALE_NO_DISATTIV = null; } else {
      this.T_CAUSALE_NO_DISATTIV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_STATO_SOSP = null; } else {
      this.T_STATO_SOSP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_SOSPENSIONE = null; } else {
      this.D_SOSPENSIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_REVOCA_SOSP = null; } else {
      this.D_REVOCA_SOSP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CAUSALE_NO_SOSP = null; } else {
      this.T_CAUSALE_NO_SOSP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_SWITCHING = null; } else {
      this.T_SWITCHING = __cur_str;
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
    if (__cur_str.equals("null")) { this.D_ARCHIVIAZIONE = null; } else {
      this.D_ARCHIVIAZIONE = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_S_SUCC = null; } else {
      this.N_ID_S_SUCC = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_VALIDO = null; } else {
      this.B_VALIDO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_DISATTIVAZIONE = null; } else {
      this.T_COD_DISATTIVAZIONE = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_SCHEDA = null; } else {
      this.N_ID_SCHEDA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_POD = null; } else {
      this.N_ID_POD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_STATO_ATTIVAZIONE = null; } else {
      this.T_STATO_ATTIVAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_ATTIVAZIONE = null; } else {
      this.D_ATTIVAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DISATTIVAZIONE = null; } else {
      this.D_DISATTIVAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CAUSALE_NO_RIATTIV = null; } else {
      this.T_CAUSALE_NO_RIATTIV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CAUSALE_NO_DISATTIV = null; } else {
      this.T_CAUSALE_NO_DISATTIV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_STATO_SOSP = null; } else {
      this.T_STATO_SOSP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_SOSPENSIONE = null; } else {
      this.D_SOSPENSIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_REVOCA_SOSP = null; } else {
      this.D_REVOCA_SOSP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CAUSALE_NO_SOSP = null; } else {
      this.T_CAUSALE_NO_SOSP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_SWITCHING = null; } else {
      this.T_SWITCHING = __cur_str;
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
    if (__cur_str.equals("null")) { this.D_ARCHIVIAZIONE = null; } else {
      this.D_ARCHIVIAZIONE = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_S_SUCC = null; } else {
      this.N_ID_S_SUCC = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_VALIDO = null; } else {
      this.B_VALIDO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_DISATTIVAZIONE = null; } else {
      this.T_COD_DISATTIVAZIONE = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    rcus_rcus_podstato o = (rcus_rcus_podstato) super.clone();
    return o;
  }

  public void clone0(rcus_rcus_podstato o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_SCHEDA", this.N_ID_SCHEDA);
    __sqoop$field_map.put("N_ID_POD", this.N_ID_POD);
    __sqoop$field_map.put("T_STATO_ATTIVAZIONE", this.T_STATO_ATTIVAZIONE);
    __sqoop$field_map.put("D_ATTIVAZIONE", this.D_ATTIVAZIONE);
    __sqoop$field_map.put("D_DISATTIVAZIONE", this.D_DISATTIVAZIONE);
    __sqoop$field_map.put("T_CAUSALE_NO_RIATTIV", this.T_CAUSALE_NO_RIATTIV);
    __sqoop$field_map.put("T_CAUSALE_NO_DISATTIV", this.T_CAUSALE_NO_DISATTIV);
    __sqoop$field_map.put("T_STATO_SOSP", this.T_STATO_SOSP);
    __sqoop$field_map.put("D_SOSPENSIONE", this.D_SOSPENSIONE);
    __sqoop$field_map.put("D_REVOCA_SOSP", this.D_REVOCA_SOSP);
    __sqoop$field_map.put("T_CAUSALE_NO_SOSP", this.T_CAUSALE_NO_SOSP);
    __sqoop$field_map.put("T_SWITCHING", this.T_SWITCHING);
    __sqoop$field_map.put("T_NOTA", this.T_NOTA);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("D_ARCHIVIAZIONE", this.D_ARCHIVIAZIONE);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("N_ID_S_SUCC", this.N_ID_S_SUCC);
    __sqoop$field_map.put("B_VALIDO", this.B_VALIDO);
    __sqoop$field_map.put("T_COD_DISATTIVAZIONE", this.T_COD_DISATTIVAZIONE);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_SCHEDA", this.N_ID_SCHEDA);
    __sqoop$field_map.put("N_ID_POD", this.N_ID_POD);
    __sqoop$field_map.put("T_STATO_ATTIVAZIONE", this.T_STATO_ATTIVAZIONE);
    __sqoop$field_map.put("D_ATTIVAZIONE", this.D_ATTIVAZIONE);
    __sqoop$field_map.put("D_DISATTIVAZIONE", this.D_DISATTIVAZIONE);
    __sqoop$field_map.put("T_CAUSALE_NO_RIATTIV", this.T_CAUSALE_NO_RIATTIV);
    __sqoop$field_map.put("T_CAUSALE_NO_DISATTIV", this.T_CAUSALE_NO_DISATTIV);
    __sqoop$field_map.put("T_STATO_SOSP", this.T_STATO_SOSP);
    __sqoop$field_map.put("D_SOSPENSIONE", this.D_SOSPENSIONE);
    __sqoop$field_map.put("D_REVOCA_SOSP", this.D_REVOCA_SOSP);
    __sqoop$field_map.put("T_CAUSALE_NO_SOSP", this.T_CAUSALE_NO_SOSP);
    __sqoop$field_map.put("T_SWITCHING", this.T_SWITCHING);
    __sqoop$field_map.put("T_NOTA", this.T_NOTA);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("D_ARCHIVIAZIONE", this.D_ARCHIVIAZIONE);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("N_ID_S_SUCC", this.N_ID_S_SUCC);
    __sqoop$field_map.put("B_VALIDO", this.B_VALIDO);
    __sqoop$field_map.put("T_COD_DISATTIVAZIONE", this.T_COD_DISATTIVAZIONE);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
