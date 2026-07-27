// ORM class for table 'rcus.rcus_pod_udd'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 13:39:31 CEST 2019
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

public class rcus_rcus_pod_udd extends SqoopRecord  implements DBWritable, Writable {
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
    setters.put("N_ID_UDD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_UDD = (java.math.BigDecimal)value;
      }
    });
    setters.put("D_INIZIO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_INIZIO = (String)value;
      }
    });
    setters.put("D_FINE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_FINE = (String)value;
      }
    });
    setters.put("D_STIPULA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_STIPULA = (String)value;
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
  }
  public rcus_rcus_pod_udd() {
    init0();
  }
  private java.math.BigDecimal N_ID_SCHEDA;
  public java.math.BigDecimal get_N_ID_SCHEDA() {
    return N_ID_SCHEDA;
  }
  public void set_N_ID_SCHEDA(java.math.BigDecimal N_ID_SCHEDA) {
    this.N_ID_SCHEDA = N_ID_SCHEDA;
  }
  public rcus_rcus_pod_udd with_N_ID_SCHEDA(java.math.BigDecimal N_ID_SCHEDA) {
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
  public rcus_rcus_pod_udd with_N_ID_POD(java.math.BigDecimal N_ID_POD) {
    this.N_ID_POD = N_ID_POD;
    return this;
  }
  private java.math.BigDecimal N_ID_UDD;
  public java.math.BigDecimal get_N_ID_UDD() {
    return N_ID_UDD;
  }
  public void set_N_ID_UDD(java.math.BigDecimal N_ID_UDD) {
    this.N_ID_UDD = N_ID_UDD;
  }
  public rcus_rcus_pod_udd with_N_ID_UDD(java.math.BigDecimal N_ID_UDD) {
    this.N_ID_UDD = N_ID_UDD;
    return this;
  }
  private String D_INIZIO;
  public String get_D_INIZIO() {
    return D_INIZIO;
  }
  public void set_D_INIZIO(String D_INIZIO) {
    this.D_INIZIO = D_INIZIO;
  }
  public rcus_rcus_pod_udd with_D_INIZIO(String D_INIZIO) {
    this.D_INIZIO = D_INIZIO;
    return this;
  }
  private String D_FINE;
  public String get_D_FINE() {
    return D_FINE;
  }
  public void set_D_FINE(String D_FINE) {
    this.D_FINE = D_FINE;
  }
  public rcus_rcus_pod_udd with_D_FINE(String D_FINE) {
    this.D_FINE = D_FINE;
    return this;
  }
  private String D_STIPULA;
  public String get_D_STIPULA() {
    return D_STIPULA;
  }
  public void set_D_STIPULA(String D_STIPULA) {
    this.D_STIPULA = D_STIPULA;
  }
  public rcus_rcus_pod_udd with_D_STIPULA(String D_STIPULA) {
    this.D_STIPULA = D_STIPULA;
    return this;
  }
  private String T_NOTA;
  public String get_T_NOTA() {
    return T_NOTA;
  }
  public void set_T_NOTA(String T_NOTA) {
    this.T_NOTA = T_NOTA;
  }
  public rcus_rcus_pod_udd with_T_NOTA(String T_NOTA) {
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
  public rcus_rcus_pod_udd with_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
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
  public rcus_rcus_pod_udd with_D_ARCHIVIAZIONE(String D_ARCHIVIAZIONE) {
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
  public rcus_rcus_pod_udd with_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
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
  public rcus_rcus_pod_udd with_N_ID_S_PREC(java.math.BigDecimal N_ID_S_PREC) {
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
  public rcus_rcus_pod_udd with_N_ID_S_SUCC(java.math.BigDecimal N_ID_S_SUCC) {
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
  public rcus_rcus_pod_udd with_B_VALIDO(String B_VALIDO) {
    this.B_VALIDO = B_VALIDO;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcus_rcus_pod_udd)) {
      return false;
    }
    rcus_rcus_pod_udd that = (rcus_rcus_pod_udd) o;
    boolean equal = true;
    equal = equal && (this.N_ID_SCHEDA == null ? that.N_ID_SCHEDA == null : this.N_ID_SCHEDA.equals(that.N_ID_SCHEDA));
    equal = equal && (this.N_ID_POD == null ? that.N_ID_POD == null : this.N_ID_POD.equals(that.N_ID_POD));
    equal = equal && (this.N_ID_UDD == null ? that.N_ID_UDD == null : this.N_ID_UDD.equals(that.N_ID_UDD));
    equal = equal && (this.D_INIZIO == null ? that.D_INIZIO == null : this.D_INIZIO.equals(that.D_INIZIO));
    equal = equal && (this.D_FINE == null ? that.D_FINE == null : this.D_FINE.equals(that.D_FINE));
    equal = equal && (this.D_STIPULA == null ? that.D_STIPULA == null : this.D_STIPULA.equals(that.D_STIPULA));
    equal = equal && (this.T_NOTA == null ? that.T_NOTA == null : this.T_NOTA.equals(that.T_NOTA));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.D_ARCHIVIAZIONE == null ? that.D_ARCHIVIAZIONE == null : this.D_ARCHIVIAZIONE.equals(that.D_ARCHIVIAZIONE));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.N_ID_S_SUCC == null ? that.N_ID_S_SUCC == null : this.N_ID_S_SUCC.equals(that.N_ID_S_SUCC));
    equal = equal && (this.B_VALIDO == null ? that.B_VALIDO == null : this.B_VALIDO.equals(that.B_VALIDO));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcus_rcus_pod_udd)) {
      return false;
    }
    rcus_rcus_pod_udd that = (rcus_rcus_pod_udd) o;
    boolean equal = true;
    equal = equal && (this.N_ID_SCHEDA == null ? that.N_ID_SCHEDA == null : this.N_ID_SCHEDA.equals(that.N_ID_SCHEDA));
    equal = equal && (this.N_ID_POD == null ? that.N_ID_POD == null : this.N_ID_POD.equals(that.N_ID_POD));
    equal = equal && (this.N_ID_UDD == null ? that.N_ID_UDD == null : this.N_ID_UDD.equals(that.N_ID_UDD));
    equal = equal && (this.D_INIZIO == null ? that.D_INIZIO == null : this.D_INIZIO.equals(that.D_INIZIO));
    equal = equal && (this.D_FINE == null ? that.D_FINE == null : this.D_FINE.equals(that.D_FINE));
    equal = equal && (this.D_STIPULA == null ? that.D_STIPULA == null : this.D_STIPULA.equals(that.D_STIPULA));
    equal = equal && (this.T_NOTA == null ? that.T_NOTA == null : this.T_NOTA.equals(that.T_NOTA));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.D_ARCHIVIAZIONE == null ? that.D_ARCHIVIAZIONE == null : this.D_ARCHIVIAZIONE.equals(that.D_ARCHIVIAZIONE));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.N_ID_S_SUCC == null ? that.N_ID_S_SUCC == null : this.N_ID_S_SUCC.equals(that.N_ID_S_SUCC));
    equal = equal && (this.B_VALIDO == null ? that.B_VALIDO == null : this.B_VALIDO.equals(that.B_VALIDO));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_SCHEDA = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_POD = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.N_ID_UDD = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.D_INIZIO = JdbcWritableBridge.readString(4, __dbResults);
    this.D_FINE = JdbcWritableBridge.readString(5, __dbResults);
    this.D_STIPULA = JdbcWritableBridge.readString(6, __dbResults);
    this.T_NOTA = JdbcWritableBridge.readString(7, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(8, __dbResults);
    this.D_ARCHIVIAZIONE = JdbcWritableBridge.readString(9, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(11, __dbResults);
    this.N_ID_S_SUCC = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.B_VALIDO = JdbcWritableBridge.readString(13, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_SCHEDA = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_POD = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.N_ID_UDD = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.D_INIZIO = JdbcWritableBridge.readString(4, __dbResults);
    this.D_FINE = JdbcWritableBridge.readString(5, __dbResults);
    this.D_STIPULA = JdbcWritableBridge.readString(6, __dbResults);
    this.T_NOTA = JdbcWritableBridge.readString(7, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(8, __dbResults);
    this.D_ARCHIVIAZIONE = JdbcWritableBridge.readString(9, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(11, __dbResults);
    this.N_ID_S_SUCC = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.B_VALIDO = JdbcWritableBridge.readString(13, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_UDD, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_INIZIO, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_FINE, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_STIPULA, 6 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTA, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_ARCHIVIAZIONE, 9 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 10 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 11 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_SUCC, 12 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_VALIDO, 13 + __off, 12, __dbStmt);
    return 13;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_SCHEDA, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_POD, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UDD, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_INIZIO, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_FINE, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_STIPULA, 6 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTA, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_ARCHIVIAZIONE, 9 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 10 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 11 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_SUCC, 12 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_VALIDO, 13 + __off, 12, __dbStmt);
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
        this.N_ID_UDD = null;
    } else {
    this.N_ID_UDD = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_INIZIO = null;
    } else {
    this.D_INIZIO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_FINE = null;
    } else {
    this.D_FINE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_STIPULA = null;
    } else {
    this.D_STIPULA = Text.readString(__dataIn);
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
    if (null == this.N_ID_UDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UDD, __dataOut);
    }
    if (null == this.D_INIZIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_INIZIO);
    }
    if (null == this.D_FINE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_FINE);
    }
    if (null == this.D_STIPULA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_STIPULA);
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
    if (null == this.N_ID_UDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UDD, __dataOut);
    }
    if (null == this.D_INIZIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_INIZIO);
    }
    if (null == this.D_FINE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_FINE);
    }
    if (null == this.D_STIPULA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_STIPULA);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UDD==null?"":N_ID_UDD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_INIZIO==null?"":D_INIZIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_FINE==null?"":D_FINE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_STIPULA==null?"":D_STIPULA, " ", delimiters));
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UDD==null?"":N_ID_UDD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_INIZIO==null?"":D_INIZIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_FINE==null?"":D_FINE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_STIPULA==null?"":D_STIPULA, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UDD = null; } else {
      this.N_ID_UDD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_INIZIO = null; } else {
      this.D_INIZIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_FINE = null; } else {
      this.D_FINE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_STIPULA = null; } else {
      this.D_STIPULA = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UDD = null; } else {
      this.N_ID_UDD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_INIZIO = null; } else {
      this.D_INIZIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_FINE = null; } else {
      this.D_FINE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_STIPULA = null; } else {
      this.D_STIPULA = __cur_str;
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

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    rcus_rcus_pod_udd o = (rcus_rcus_pod_udd) super.clone();
    return o;
  }

  public void clone0(rcus_rcus_pod_udd o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_SCHEDA", this.N_ID_SCHEDA);
    __sqoop$field_map.put("N_ID_POD", this.N_ID_POD);
    __sqoop$field_map.put("N_ID_UDD", this.N_ID_UDD);
    __sqoop$field_map.put("D_INIZIO", this.D_INIZIO);
    __sqoop$field_map.put("D_FINE", this.D_FINE);
    __sqoop$field_map.put("D_STIPULA", this.D_STIPULA);
    __sqoop$field_map.put("T_NOTA", this.T_NOTA);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("D_ARCHIVIAZIONE", this.D_ARCHIVIAZIONE);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("N_ID_S_SUCC", this.N_ID_S_SUCC);
    __sqoop$field_map.put("B_VALIDO", this.B_VALIDO);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_SCHEDA", this.N_ID_SCHEDA);
    __sqoop$field_map.put("N_ID_POD", this.N_ID_POD);
    __sqoop$field_map.put("N_ID_UDD", this.N_ID_UDD);
    __sqoop$field_map.put("D_INIZIO", this.D_INIZIO);
    __sqoop$field_map.put("D_FINE", this.D_FINE);
    __sqoop$field_map.put("D_STIPULA", this.D_STIPULA);
    __sqoop$field_map.put("T_NOTA", this.T_NOTA);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("D_ARCHIVIAZIONE", this.D_ARCHIVIAZIONE);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("N_ID_S_SUCC", this.N_ID_S_SUCC);
    __sqoop$field_map.put("B_VALIDO", this.B_VALIDO);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
