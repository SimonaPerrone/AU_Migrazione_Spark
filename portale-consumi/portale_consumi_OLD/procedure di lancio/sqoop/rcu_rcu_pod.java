// ORM class for table 'rcu.rcu_pod'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 10:09:10 CEST 2019
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

public class rcu_rcu_pod extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_POD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_POD = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_CODICE_POD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_POD = (String)value;
      }
    });
    setters.put("T_AREA_RIF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_AREA_RIF = (String)value;
      }
    });
    setters.put("B_RICH_INDENNIZZO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_RICH_INDENNIZZO = (String)value;
      }
    });
    setters.put("B_RICH_PREST_DISTR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_RICH_PREST_DISTR = (String)value;
      }
    });
    setters.put("N_ID_INDIRIZZO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_INDIRIZZO = (java.math.BigDecimal)value;
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
    setters.put("N_ID_IND_FORN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_IND_FORN = (java.math.BigDecimal)value;
      }
    });
  }
  public rcu_rcu_pod() {
    init0();
  }
  private java.math.BigDecimal N_ID_POD;
  public java.math.BigDecimal get_N_ID_POD() {
    return N_ID_POD;
  }
  public void set_N_ID_POD(java.math.BigDecimal N_ID_POD) {
    this.N_ID_POD = N_ID_POD;
  }
  public rcu_rcu_pod with_N_ID_POD(java.math.BigDecimal N_ID_POD) {
    this.N_ID_POD = N_ID_POD;
    return this;
  }
  private String T_CODICE_POD;
  public String get_T_CODICE_POD() {
    return T_CODICE_POD;
  }
  public void set_T_CODICE_POD(String T_CODICE_POD) {
    this.T_CODICE_POD = T_CODICE_POD;
  }
  public rcu_rcu_pod with_T_CODICE_POD(String T_CODICE_POD) {
    this.T_CODICE_POD = T_CODICE_POD;
    return this;
  }
  private String T_AREA_RIF;
  public String get_T_AREA_RIF() {
    return T_AREA_RIF;
  }
  public void set_T_AREA_RIF(String T_AREA_RIF) {
    this.T_AREA_RIF = T_AREA_RIF;
  }
  public rcu_rcu_pod with_T_AREA_RIF(String T_AREA_RIF) {
    this.T_AREA_RIF = T_AREA_RIF;
    return this;
  }
  private String B_RICH_INDENNIZZO;
  public String get_B_RICH_INDENNIZZO() {
    return B_RICH_INDENNIZZO;
  }
  public void set_B_RICH_INDENNIZZO(String B_RICH_INDENNIZZO) {
    this.B_RICH_INDENNIZZO = B_RICH_INDENNIZZO;
  }
  public rcu_rcu_pod with_B_RICH_INDENNIZZO(String B_RICH_INDENNIZZO) {
    this.B_RICH_INDENNIZZO = B_RICH_INDENNIZZO;
    return this;
  }
  private String B_RICH_PREST_DISTR;
  public String get_B_RICH_PREST_DISTR() {
    return B_RICH_PREST_DISTR;
  }
  public void set_B_RICH_PREST_DISTR(String B_RICH_PREST_DISTR) {
    this.B_RICH_PREST_DISTR = B_RICH_PREST_DISTR;
  }
  public rcu_rcu_pod with_B_RICH_PREST_DISTR(String B_RICH_PREST_DISTR) {
    this.B_RICH_PREST_DISTR = B_RICH_PREST_DISTR;
    return this;
  }
  private java.math.BigDecimal N_ID_INDIRIZZO;
  public java.math.BigDecimal get_N_ID_INDIRIZZO() {
    return N_ID_INDIRIZZO;
  }
  public void set_N_ID_INDIRIZZO(java.math.BigDecimal N_ID_INDIRIZZO) {
    this.N_ID_INDIRIZZO = N_ID_INDIRIZZO;
  }
  public rcu_rcu_pod with_N_ID_INDIRIZZO(java.math.BigDecimal N_ID_INDIRIZZO) {
    this.N_ID_INDIRIZZO = N_ID_INDIRIZZO;
    return this;
  }
  private String T_NOTA;
  public String get_T_NOTA() {
    return T_NOTA;
  }
  public void set_T_NOTA(String T_NOTA) {
    this.T_NOTA = T_NOTA;
  }
  public rcu_rcu_pod with_T_NOTA(String T_NOTA) {
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
  public rcu_rcu_pod with_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
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
  public rcu_rcu_pod with_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
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
  public rcu_rcu_pod with_N_ID_S_PREC(java.math.BigDecimal N_ID_S_PREC) {
    this.N_ID_S_PREC = N_ID_S_PREC;
    return this;
  }
  private java.math.BigDecimal N_ID_IND_FORN;
  public java.math.BigDecimal get_N_ID_IND_FORN() {
    return N_ID_IND_FORN;
  }
  public void set_N_ID_IND_FORN(java.math.BigDecimal N_ID_IND_FORN) {
    this.N_ID_IND_FORN = N_ID_IND_FORN;
  }
  public rcu_rcu_pod with_N_ID_IND_FORN(java.math.BigDecimal N_ID_IND_FORN) {
    this.N_ID_IND_FORN = N_ID_IND_FORN;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcu_rcu_pod)) {
      return false;
    }
    rcu_rcu_pod that = (rcu_rcu_pod) o;
    boolean equal = true;
    equal = equal && (this.N_ID_POD == null ? that.N_ID_POD == null : this.N_ID_POD.equals(that.N_ID_POD));
    equal = equal && (this.T_CODICE_POD == null ? that.T_CODICE_POD == null : this.T_CODICE_POD.equals(that.T_CODICE_POD));
    equal = equal && (this.T_AREA_RIF == null ? that.T_AREA_RIF == null : this.T_AREA_RIF.equals(that.T_AREA_RIF));
    equal = equal && (this.B_RICH_INDENNIZZO == null ? that.B_RICH_INDENNIZZO == null : this.B_RICH_INDENNIZZO.equals(that.B_RICH_INDENNIZZO));
    equal = equal && (this.B_RICH_PREST_DISTR == null ? that.B_RICH_PREST_DISTR == null : this.B_RICH_PREST_DISTR.equals(that.B_RICH_PREST_DISTR));
    equal = equal && (this.N_ID_INDIRIZZO == null ? that.N_ID_INDIRIZZO == null : this.N_ID_INDIRIZZO.equals(that.N_ID_INDIRIZZO));
    equal = equal && (this.T_NOTA == null ? that.T_NOTA == null : this.T_NOTA.equals(that.T_NOTA));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.N_ID_IND_FORN == null ? that.N_ID_IND_FORN == null : this.N_ID_IND_FORN.equals(that.N_ID_IND_FORN));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcu_rcu_pod)) {
      return false;
    }
    rcu_rcu_pod that = (rcu_rcu_pod) o;
    boolean equal = true;
    equal = equal && (this.N_ID_POD == null ? that.N_ID_POD == null : this.N_ID_POD.equals(that.N_ID_POD));
    equal = equal && (this.T_CODICE_POD == null ? that.T_CODICE_POD == null : this.T_CODICE_POD.equals(that.T_CODICE_POD));
    equal = equal && (this.T_AREA_RIF == null ? that.T_AREA_RIF == null : this.T_AREA_RIF.equals(that.T_AREA_RIF));
    equal = equal && (this.B_RICH_INDENNIZZO == null ? that.B_RICH_INDENNIZZO == null : this.B_RICH_INDENNIZZO.equals(that.B_RICH_INDENNIZZO));
    equal = equal && (this.B_RICH_PREST_DISTR == null ? that.B_RICH_PREST_DISTR == null : this.B_RICH_PREST_DISTR.equals(that.B_RICH_PREST_DISTR));
    equal = equal && (this.N_ID_INDIRIZZO == null ? that.N_ID_INDIRIZZO == null : this.N_ID_INDIRIZZO.equals(that.N_ID_INDIRIZZO));
    equal = equal && (this.T_NOTA == null ? that.T_NOTA == null : this.T_NOTA.equals(that.T_NOTA));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.N_ID_IND_FORN == null ? that.N_ID_IND_FORN == null : this.N_ID_IND_FORN.equals(that.N_ID_IND_FORN));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_POD = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.T_CODICE_POD = JdbcWritableBridge.readString(2, __dbResults);
    this.T_AREA_RIF = JdbcWritableBridge.readString(3, __dbResults);
    this.B_RICH_INDENNIZZO = JdbcWritableBridge.readString(4, __dbResults);
    this.B_RICH_PREST_DISTR = JdbcWritableBridge.readString(5, __dbResults);
    this.N_ID_INDIRIZZO = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.T_NOTA = JdbcWritableBridge.readString(7, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(8, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.N_ID_IND_FORN = JdbcWritableBridge.readBigDecimal(11, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_POD = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.T_CODICE_POD = JdbcWritableBridge.readString(2, __dbResults);
    this.T_AREA_RIF = JdbcWritableBridge.readString(3, __dbResults);
    this.B_RICH_INDENNIZZO = JdbcWritableBridge.readString(4, __dbResults);
    this.B_RICH_PREST_DISTR = JdbcWritableBridge.readString(5, __dbResults);
    this.N_ID_INDIRIZZO = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.T_NOTA = JdbcWritableBridge.readString(7, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(8, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.N_ID_IND_FORN = JdbcWritableBridge.readBigDecimal(11, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_POD, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_POD, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_AREA_RIF, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_RICH_INDENNIZZO, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_RICH_PREST_DISTR, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_INDIRIZZO, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTA, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 10 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_IND_FORN, 11 + __off, 2, __dbStmt);
    return 11;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_POD, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_POD, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_AREA_RIF, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_RICH_INDENNIZZO, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_RICH_PREST_DISTR, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_INDIRIZZO, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTA, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 10 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_IND_FORN, 11 + __off, 2, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_POD = null;
    } else {
    this.N_ID_POD = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_POD = null;
    } else {
    this.T_CODICE_POD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_AREA_RIF = null;
    } else {
    this.T_AREA_RIF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_RICH_INDENNIZZO = null;
    } else {
    this.B_RICH_INDENNIZZO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_RICH_PREST_DISTR = null;
    } else {
    this.B_RICH_PREST_DISTR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_INDIRIZZO = null;
    } else {
    this.N_ID_INDIRIZZO = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
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
        this.N_ID_IND_FORN = null;
    } else {
    this.N_ID_IND_FORN = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_POD, __dataOut);
    }
    if (null == this.T_CODICE_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_POD);
    }
    if (null == this.T_AREA_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_AREA_RIF);
    }
    if (null == this.B_RICH_INDENNIZZO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_RICH_INDENNIZZO);
    }
    if (null == this.B_RICH_PREST_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_RICH_PREST_DISTR);
    }
    if (null == this.N_ID_INDIRIZZO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_INDIRIZZO, __dataOut);
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
    if (null == this.N_ID_IND_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_IND_FORN, __dataOut);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_POD, __dataOut);
    }
    if (null == this.T_CODICE_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_POD);
    }
    if (null == this.T_AREA_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_AREA_RIF);
    }
    if (null == this.B_RICH_INDENNIZZO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_RICH_INDENNIZZO);
    }
    if (null == this.B_RICH_PREST_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_RICH_PREST_DISTR);
    }
    if (null == this.N_ID_INDIRIZZO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_INDIRIZZO, __dataOut);
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
    if (null == this.N_ID_IND_FORN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_IND_FORN, __dataOut);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_POD==null?"":N_ID_POD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_POD==null?"":T_CODICE_POD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_AREA_RIF==null?"":T_AREA_RIF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_RICH_INDENNIZZO==null?"":B_RICH_INDENNIZZO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_RICH_PREST_DISTR==null?"":B_RICH_PREST_DISTR, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_INDIRIZZO==null?"":N_ID_INDIRIZZO.toPlainString(), delimiters));
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_IND_FORN==null?"":N_ID_IND_FORN.toPlainString(), delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_POD==null?"":N_ID_POD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_POD==null?"":T_CODICE_POD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_AREA_RIF==null?"":T_AREA_RIF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_RICH_INDENNIZZO==null?"":B_RICH_INDENNIZZO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_RICH_PREST_DISTR==null?"":B_RICH_PREST_DISTR, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_INDIRIZZO==null?"":N_ID_INDIRIZZO.toPlainString(), delimiters));
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_IND_FORN==null?"":N_ID_IND_FORN.toPlainString(), delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_POD = null; } else {
      this.N_ID_POD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_POD = null; } else {
      this.T_CODICE_POD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_AREA_RIF = null; } else {
      this.T_AREA_RIF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_RICH_INDENNIZZO = null; } else {
      this.B_RICH_INDENNIZZO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_RICH_PREST_DISTR = null; } else {
      this.B_RICH_PREST_DISTR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_INDIRIZZO = null; } else {
      this.N_ID_INDIRIZZO = new java.math.BigDecimal(__cur_str);
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_IND_FORN = null; } else {
      this.N_ID_IND_FORN = new java.math.BigDecimal(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_POD = null; } else {
      this.N_ID_POD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_POD = null; } else {
      this.T_CODICE_POD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_AREA_RIF = null; } else {
      this.T_AREA_RIF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_RICH_INDENNIZZO = null; } else {
      this.B_RICH_INDENNIZZO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_RICH_PREST_DISTR = null; } else {
      this.B_RICH_PREST_DISTR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_INDIRIZZO = null; } else {
      this.N_ID_INDIRIZZO = new java.math.BigDecimal(__cur_str);
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_IND_FORN = null; } else {
      this.N_ID_IND_FORN = new java.math.BigDecimal(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    rcu_rcu_pod o = (rcu_rcu_pod) super.clone();
    return o;
  }

  public void clone0(rcu_rcu_pod o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_POD", this.N_ID_POD);
    __sqoop$field_map.put("T_CODICE_POD", this.T_CODICE_POD);
    __sqoop$field_map.put("T_AREA_RIF", this.T_AREA_RIF);
    __sqoop$field_map.put("B_RICH_INDENNIZZO", this.B_RICH_INDENNIZZO);
    __sqoop$field_map.put("B_RICH_PREST_DISTR", this.B_RICH_PREST_DISTR);
    __sqoop$field_map.put("N_ID_INDIRIZZO", this.N_ID_INDIRIZZO);
    __sqoop$field_map.put("T_NOTA", this.T_NOTA);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("N_ID_IND_FORN", this.N_ID_IND_FORN);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_POD", this.N_ID_POD);
    __sqoop$field_map.put("T_CODICE_POD", this.T_CODICE_POD);
    __sqoop$field_map.put("T_AREA_RIF", this.T_AREA_RIF);
    __sqoop$field_map.put("B_RICH_INDENNIZZO", this.B_RICH_INDENNIZZO);
    __sqoop$field_map.put("B_RICH_PREST_DISTR", this.B_RICH_PREST_DISTR);
    __sqoop$field_map.put("N_ID_INDIRIZZO", this.N_ID_INDIRIZZO);
    __sqoop$field_map.put("T_NOTA", this.T_NOTA);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("N_ID_IND_FORN", this.N_ID_IND_FORN);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
