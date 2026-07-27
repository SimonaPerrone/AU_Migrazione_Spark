// ORM class for table 'rcu.rcu_residenza'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 11:05:36 CEST 2019
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

public class rcu_rcu_residenza extends SqoopRecord  implements DBWritable, Writable {
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
    setters.put("N_ID_FORNITURA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_FORNITURA = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_RESIDENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_RESIDENTE = (String)value;
      }
    });
    setters.put("D_INIZIO_RESIDENZA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_INIZIO_RESIDENZA = (String)value;
      }
    });
    setters.put("D_FINE_RESIDENZA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_FINE_RESIDENZA = (String)value;
      }
    });
    setters.put("B_STORICO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_STORICO = (String)value;
      }
    });
    setters.put("B_VALIDO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_VALIDO = (String)value;
      }
    });
    setters.put("B_ULTIMA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_ULTIMA = (String)value;
      }
    });
    setters.put("N_ID_TRACCIA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_TRACCIA = (java.math.BigDecimal)value;
      }
    });
    setters.put("D_AGGIORNAMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_AGGIORNAMENTO = (String)value;
      }
    });
  }
  public rcu_rcu_residenza() {
    init0();
  }
  private java.math.BigDecimal N_ID;
  public java.math.BigDecimal get_N_ID() {
    return N_ID;
  }
  public void set_N_ID(java.math.BigDecimal N_ID) {
    this.N_ID = N_ID;
  }
  public rcu_rcu_residenza with_N_ID(java.math.BigDecimal N_ID) {
    this.N_ID = N_ID;
    return this;
  }
  private java.math.BigDecimal N_ID_FORNITURA;
  public java.math.BigDecimal get_N_ID_FORNITURA() {
    return N_ID_FORNITURA;
  }
  public void set_N_ID_FORNITURA(java.math.BigDecimal N_ID_FORNITURA) {
    this.N_ID_FORNITURA = N_ID_FORNITURA;
  }
  public rcu_rcu_residenza with_N_ID_FORNITURA(java.math.BigDecimal N_ID_FORNITURA) {
    this.N_ID_FORNITURA = N_ID_FORNITURA;
    return this;
  }
  private String T_RESIDENTE;
  public String get_T_RESIDENTE() {
    return T_RESIDENTE;
  }
  public void set_T_RESIDENTE(String T_RESIDENTE) {
    this.T_RESIDENTE = T_RESIDENTE;
  }
  public rcu_rcu_residenza with_T_RESIDENTE(String T_RESIDENTE) {
    this.T_RESIDENTE = T_RESIDENTE;
    return this;
  }
  private String D_INIZIO_RESIDENZA;
  public String get_D_INIZIO_RESIDENZA() {
    return D_INIZIO_RESIDENZA;
  }
  public void set_D_INIZIO_RESIDENZA(String D_INIZIO_RESIDENZA) {
    this.D_INIZIO_RESIDENZA = D_INIZIO_RESIDENZA;
  }
  public rcu_rcu_residenza with_D_INIZIO_RESIDENZA(String D_INIZIO_RESIDENZA) {
    this.D_INIZIO_RESIDENZA = D_INIZIO_RESIDENZA;
    return this;
  }
  private String D_FINE_RESIDENZA;
  public String get_D_FINE_RESIDENZA() {
    return D_FINE_RESIDENZA;
  }
  public void set_D_FINE_RESIDENZA(String D_FINE_RESIDENZA) {
    this.D_FINE_RESIDENZA = D_FINE_RESIDENZA;
  }
  public rcu_rcu_residenza with_D_FINE_RESIDENZA(String D_FINE_RESIDENZA) {
    this.D_FINE_RESIDENZA = D_FINE_RESIDENZA;
    return this;
  }
  private String B_STORICO;
  public String get_B_STORICO() {
    return B_STORICO;
  }
  public void set_B_STORICO(String B_STORICO) {
    this.B_STORICO = B_STORICO;
  }
  public rcu_rcu_residenza with_B_STORICO(String B_STORICO) {
    this.B_STORICO = B_STORICO;
    return this;
  }
  private String B_VALIDO;
  public String get_B_VALIDO() {
    return B_VALIDO;
  }
  public void set_B_VALIDO(String B_VALIDO) {
    this.B_VALIDO = B_VALIDO;
  }
  public rcu_rcu_residenza with_B_VALIDO(String B_VALIDO) {
    this.B_VALIDO = B_VALIDO;
    return this;
  }
  private String B_ULTIMA;
  public String get_B_ULTIMA() {
    return B_ULTIMA;
  }
  public void set_B_ULTIMA(String B_ULTIMA) {
    this.B_ULTIMA = B_ULTIMA;
  }
  public rcu_rcu_residenza with_B_ULTIMA(String B_ULTIMA) {
    this.B_ULTIMA = B_ULTIMA;
    return this;
  }
  private java.math.BigDecimal N_ID_TRACCIA;
  public java.math.BigDecimal get_N_ID_TRACCIA() {
    return N_ID_TRACCIA;
  }
  public void set_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
    this.N_ID_TRACCIA = N_ID_TRACCIA;
  }
  public rcu_rcu_residenza with_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
    this.N_ID_TRACCIA = N_ID_TRACCIA;
    return this;
  }
  private String D_AGGIORNAMENTO;
  public String get_D_AGGIORNAMENTO() {
    return D_AGGIORNAMENTO;
  }
  public void set_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
    this.D_AGGIORNAMENTO = D_AGGIORNAMENTO;
  }
  public rcu_rcu_residenza with_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
    this.D_AGGIORNAMENTO = D_AGGIORNAMENTO;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcu_rcu_residenza)) {
      return false;
    }
    rcu_rcu_residenza that = (rcu_rcu_residenza) o;
    boolean equal = true;
    equal = equal && (this.N_ID == null ? that.N_ID == null : this.N_ID.equals(that.N_ID));
    equal = equal && (this.N_ID_FORNITURA == null ? that.N_ID_FORNITURA == null : this.N_ID_FORNITURA.equals(that.N_ID_FORNITURA));
    equal = equal && (this.T_RESIDENTE == null ? that.T_RESIDENTE == null : this.T_RESIDENTE.equals(that.T_RESIDENTE));
    equal = equal && (this.D_INIZIO_RESIDENZA == null ? that.D_INIZIO_RESIDENZA == null : this.D_INIZIO_RESIDENZA.equals(that.D_INIZIO_RESIDENZA));
    equal = equal && (this.D_FINE_RESIDENZA == null ? that.D_FINE_RESIDENZA == null : this.D_FINE_RESIDENZA.equals(that.D_FINE_RESIDENZA));
    equal = equal && (this.B_STORICO == null ? that.B_STORICO == null : this.B_STORICO.equals(that.B_STORICO));
    equal = equal && (this.B_VALIDO == null ? that.B_VALIDO == null : this.B_VALIDO.equals(that.B_VALIDO));
    equal = equal && (this.B_ULTIMA == null ? that.B_ULTIMA == null : this.B_ULTIMA.equals(that.B_ULTIMA));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcu_rcu_residenza)) {
      return false;
    }
    rcu_rcu_residenza that = (rcu_rcu_residenza) o;
    boolean equal = true;
    equal = equal && (this.N_ID == null ? that.N_ID == null : this.N_ID.equals(that.N_ID));
    equal = equal && (this.N_ID_FORNITURA == null ? that.N_ID_FORNITURA == null : this.N_ID_FORNITURA.equals(that.N_ID_FORNITURA));
    equal = equal && (this.T_RESIDENTE == null ? that.T_RESIDENTE == null : this.T_RESIDENTE.equals(that.T_RESIDENTE));
    equal = equal && (this.D_INIZIO_RESIDENZA == null ? that.D_INIZIO_RESIDENZA == null : this.D_INIZIO_RESIDENZA.equals(that.D_INIZIO_RESIDENZA));
    equal = equal && (this.D_FINE_RESIDENZA == null ? that.D_FINE_RESIDENZA == null : this.D_FINE_RESIDENZA.equals(that.D_FINE_RESIDENZA));
    equal = equal && (this.B_STORICO == null ? that.B_STORICO == null : this.B_STORICO.equals(that.B_STORICO));
    equal = equal && (this.B_VALIDO == null ? that.B_VALIDO == null : this.B_VALIDO.equals(that.B_VALIDO));
    equal = equal && (this.B_ULTIMA == null ? that.B_ULTIMA == null : this.B_ULTIMA.equals(that.B_ULTIMA));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_FORNITURA = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_RESIDENTE = JdbcWritableBridge.readString(3, __dbResults);
    this.D_INIZIO_RESIDENZA = JdbcWritableBridge.readString(4, __dbResults);
    this.D_FINE_RESIDENZA = JdbcWritableBridge.readString(5, __dbResults);
    this.B_STORICO = JdbcWritableBridge.readString(6, __dbResults);
    this.B_VALIDO = JdbcWritableBridge.readString(7, __dbResults);
    this.B_ULTIMA = JdbcWritableBridge.readString(8, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(10, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_FORNITURA = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_RESIDENTE = JdbcWritableBridge.readString(3, __dbResults);
    this.D_INIZIO_RESIDENZA = JdbcWritableBridge.readString(4, __dbResults);
    this.D_FINE_RESIDENZA = JdbcWritableBridge.readString(5, __dbResults);
    this.B_STORICO = JdbcWritableBridge.readString(6, __dbResults);
    this.B_VALIDO = JdbcWritableBridge.readString(7, __dbResults);
    this.B_ULTIMA = JdbcWritableBridge.readString(8, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(10, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_FORNITURA, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_RESIDENTE, 3 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(D_INIZIO_RESIDENZA, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_FINE_RESIDENZA, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(B_STORICO, 6 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_VALIDO, 7 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_ULTIMA, 8 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 10 + __off, 93, __dbStmt);
    return 10;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_FORNITURA, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_RESIDENTE, 3 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(D_INIZIO_RESIDENZA, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_FINE_RESIDENZA, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(B_STORICO, 6 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_VALIDO, 7 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_ULTIMA, 8 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 10 + __off, 93, __dbStmt);
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
        this.N_ID_FORNITURA = null;
    } else {
    this.N_ID_FORNITURA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_RESIDENTE = null;
    } else {
    this.T_RESIDENTE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_INIZIO_RESIDENZA = null;
    } else {
    this.D_INIZIO_RESIDENZA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_FINE_RESIDENZA = null;
    } else {
    this.D_FINE_RESIDENZA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_STORICO = null;
    } else {
    this.B_STORICO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_VALIDO = null;
    } else {
    this.B_VALIDO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_ULTIMA = null;
    } else {
    this.B_ULTIMA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_TRACCIA = null;
    } else {
    this.N_ID_TRACCIA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_AGGIORNAMENTO = null;
    } else {
    this.D_AGGIORNAMENTO = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID, __dataOut);
    }
    if (null == this.N_ID_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FORNITURA, __dataOut);
    }
    if (null == this.T_RESIDENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RESIDENTE);
    }
    if (null == this.D_INIZIO_RESIDENZA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_INIZIO_RESIDENZA);
    }
    if (null == this.D_FINE_RESIDENZA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_FINE_RESIDENZA);
    }
    if (null == this.B_STORICO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_STORICO);
    }
    if (null == this.B_VALIDO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_VALIDO);
    }
    if (null == this.B_ULTIMA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_ULTIMA);
    }
    if (null == this.N_ID_TRACCIA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_TRACCIA, __dataOut);
    }
    if (null == this.D_AGGIORNAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_AGGIORNAMENTO);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID, __dataOut);
    }
    if (null == this.N_ID_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FORNITURA, __dataOut);
    }
    if (null == this.T_RESIDENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RESIDENTE);
    }
    if (null == this.D_INIZIO_RESIDENZA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_INIZIO_RESIDENZA);
    }
    if (null == this.D_FINE_RESIDENZA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_FINE_RESIDENZA);
    }
    if (null == this.B_STORICO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_STORICO);
    }
    if (null == this.B_VALIDO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_VALIDO);
    }
    if (null == this.B_ULTIMA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_ULTIMA);
    }
    if (null == this.N_ID_TRACCIA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_TRACCIA, __dataOut);
    }
    if (null == this.D_AGGIORNAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_AGGIORNAMENTO);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_FORNITURA==null?"":N_ID_FORNITURA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RESIDENTE==null?"":T_RESIDENTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_INIZIO_RESIDENZA==null?"":D_INIZIO_RESIDENZA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_FINE_RESIDENZA==null?"":D_FINE_RESIDENZA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_STORICO==null?"":B_STORICO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_VALIDO==null?"":B_VALIDO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_ULTIMA==null?"":B_ULTIMA, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TRACCIA==null?"":N_ID_TRACCIA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO==null?"":D_AGGIORNAMENTO, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID==null?"":N_ID.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_FORNITURA==null?"":N_ID_FORNITURA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RESIDENTE==null?"":T_RESIDENTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_INIZIO_RESIDENZA==null?"":D_INIZIO_RESIDENZA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_FINE_RESIDENZA==null?"":D_FINE_RESIDENZA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_STORICO==null?"":B_STORICO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_VALIDO==null?"":B_VALIDO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_ULTIMA==null?"":B_ULTIMA, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TRACCIA==null?"":N_ID_TRACCIA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO==null?"":D_AGGIORNAMENTO, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_FORNITURA = null; } else {
      this.N_ID_FORNITURA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RESIDENTE = null; } else {
      this.T_RESIDENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_INIZIO_RESIDENZA = null; } else {
      this.D_INIZIO_RESIDENZA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_FINE_RESIDENZA = null; } else {
      this.D_FINE_RESIDENZA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_STORICO = null; } else {
      this.B_STORICO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_VALIDO = null; } else {
      this.B_VALIDO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_ULTIMA = null; } else {
      this.B_ULTIMA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_TRACCIA = null; } else {
      this.N_ID_TRACCIA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_AGGIORNAMENTO = null; } else {
      this.D_AGGIORNAMENTO = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_FORNITURA = null; } else {
      this.N_ID_FORNITURA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RESIDENTE = null; } else {
      this.T_RESIDENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_INIZIO_RESIDENZA = null; } else {
      this.D_INIZIO_RESIDENZA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_FINE_RESIDENZA = null; } else {
      this.D_FINE_RESIDENZA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_STORICO = null; } else {
      this.B_STORICO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_VALIDO = null; } else {
      this.B_VALIDO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_ULTIMA = null; } else {
      this.B_ULTIMA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_TRACCIA = null; } else {
      this.N_ID_TRACCIA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_AGGIORNAMENTO = null; } else {
      this.D_AGGIORNAMENTO = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    rcu_rcu_residenza o = (rcu_rcu_residenza) super.clone();
    return o;
  }

  public void clone0(rcu_rcu_residenza o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID", this.N_ID);
    __sqoop$field_map.put("N_ID_FORNITURA", this.N_ID_FORNITURA);
    __sqoop$field_map.put("T_RESIDENTE", this.T_RESIDENTE);
    __sqoop$field_map.put("D_INIZIO_RESIDENZA", this.D_INIZIO_RESIDENZA);
    __sqoop$field_map.put("D_FINE_RESIDENZA", this.D_FINE_RESIDENZA);
    __sqoop$field_map.put("B_STORICO", this.B_STORICO);
    __sqoop$field_map.put("B_VALIDO", this.B_VALIDO);
    __sqoop$field_map.put("B_ULTIMA", this.B_ULTIMA);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID", this.N_ID);
    __sqoop$field_map.put("N_ID_FORNITURA", this.N_ID_FORNITURA);
    __sqoop$field_map.put("T_RESIDENTE", this.T_RESIDENTE);
    __sqoop$field_map.put("D_INIZIO_RESIDENZA", this.D_INIZIO_RESIDENZA);
    __sqoop$field_map.put("D_FINE_RESIDENZA", this.D_FINE_RESIDENZA);
    __sqoop$field_map.put("B_STORICO", this.B_STORICO);
    __sqoop$field_map.put("B_VALIDO", this.B_VALIDO);
    __sqoop$field_map.put("B_ULTIMA", this.B_ULTIMA);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
