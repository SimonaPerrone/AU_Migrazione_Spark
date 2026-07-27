// ORM class for table 'RCU.RCU_UDD'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Mon Nov 30 18:14:45 GMT 2020
// For connector: org.apache.sqoop.manager.OracleManager
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

public class RCU_RCU_UDD extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_UDD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_UDD = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_CODICE_TERNA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_TERNA = (String)value;
      }
    });
    setters.put("D_AGGIORNAMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_AGGIORNAMENTO = (java.sql.Timestamp)value;
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
    setters.put("D_INIZIO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_INIZIO = (java.sql.Timestamp)value;
      }
    });
    setters.put("D_FINE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_FINE = (java.sql.Timestamp)value;
      }
    });
    setters.put("N_ID_AZIENDA_RIF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_AZIENDA_RIF = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_TIPO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO = (String)value;
      }
    });
  }
  public RCU_RCU_UDD() {
    init0();
  }
  private java.math.BigDecimal N_ID_UDD;
  public java.math.BigDecimal get_N_ID_UDD() {
    return N_ID_UDD;
  }
  public void set_N_ID_UDD(java.math.BigDecimal N_ID_UDD) {
    this.N_ID_UDD = N_ID_UDD;
  }
  public RCU_RCU_UDD with_N_ID_UDD(java.math.BigDecimal N_ID_UDD) {
    this.N_ID_UDD = N_ID_UDD;
    return this;
  }
  private String T_CODICE_TERNA;
  public String get_T_CODICE_TERNA() {
    return T_CODICE_TERNA;
  }
  public void set_T_CODICE_TERNA(String T_CODICE_TERNA) {
    this.T_CODICE_TERNA = T_CODICE_TERNA;
  }
  public RCU_RCU_UDD with_T_CODICE_TERNA(String T_CODICE_TERNA) {
    this.T_CODICE_TERNA = T_CODICE_TERNA;
    return this;
  }
  private java.sql.Timestamp D_AGGIORNAMENTO;
  public java.sql.Timestamp get_D_AGGIORNAMENTO() {
    return D_AGGIORNAMENTO;
  }
  public void set_D_AGGIORNAMENTO(java.sql.Timestamp D_AGGIORNAMENTO) {
    this.D_AGGIORNAMENTO = D_AGGIORNAMENTO;
  }
  public RCU_RCU_UDD with_D_AGGIORNAMENTO(java.sql.Timestamp D_AGGIORNAMENTO) {
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
  public RCU_RCU_UDD with_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
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
  public RCU_RCU_UDD with_N_ID_S_PREC(java.math.BigDecimal N_ID_S_PREC) {
    this.N_ID_S_PREC = N_ID_S_PREC;
    return this;
  }
  private java.sql.Timestamp D_INIZIO;
  public java.sql.Timestamp get_D_INIZIO() {
    return D_INIZIO;
  }
  public void set_D_INIZIO(java.sql.Timestamp D_INIZIO) {
    this.D_INIZIO = D_INIZIO;
  }
  public RCU_RCU_UDD with_D_INIZIO(java.sql.Timestamp D_INIZIO) {
    this.D_INIZIO = D_INIZIO;
    return this;
  }
  private java.sql.Timestamp D_FINE;
  public java.sql.Timestamp get_D_FINE() {
    return D_FINE;
  }
  public void set_D_FINE(java.sql.Timestamp D_FINE) {
    this.D_FINE = D_FINE;
  }
  public RCU_RCU_UDD with_D_FINE(java.sql.Timestamp D_FINE) {
    this.D_FINE = D_FINE;
    return this;
  }
  private java.math.BigDecimal N_ID_AZIENDA_RIF;
  public java.math.BigDecimal get_N_ID_AZIENDA_RIF() {
    return N_ID_AZIENDA_RIF;
  }
  public void set_N_ID_AZIENDA_RIF(java.math.BigDecimal N_ID_AZIENDA_RIF) {
    this.N_ID_AZIENDA_RIF = N_ID_AZIENDA_RIF;
  }
  public RCU_RCU_UDD with_N_ID_AZIENDA_RIF(java.math.BigDecimal N_ID_AZIENDA_RIF) {
    this.N_ID_AZIENDA_RIF = N_ID_AZIENDA_RIF;
    return this;
  }
  private String T_TIPO;
  public String get_T_TIPO() {
    return T_TIPO;
  }
  public void set_T_TIPO(String T_TIPO) {
    this.T_TIPO = T_TIPO;
  }
  public RCU_RCU_UDD with_T_TIPO(String T_TIPO) {
    this.T_TIPO = T_TIPO;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RCU_RCU_UDD)) {
      return false;
    }
    RCU_RCU_UDD that = (RCU_RCU_UDD) o;
    boolean equal = true;
    equal = equal && (this.N_ID_UDD == null ? that.N_ID_UDD == null : this.N_ID_UDD.equals(that.N_ID_UDD));
    equal = equal && (this.T_CODICE_TERNA == null ? that.T_CODICE_TERNA == null : this.T_CODICE_TERNA.equals(that.T_CODICE_TERNA));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.D_INIZIO == null ? that.D_INIZIO == null : this.D_INIZIO.equals(that.D_INIZIO));
    equal = equal && (this.D_FINE == null ? that.D_FINE == null : this.D_FINE.equals(that.D_FINE));
    equal = equal && (this.N_ID_AZIENDA_RIF == null ? that.N_ID_AZIENDA_RIF == null : this.N_ID_AZIENDA_RIF.equals(that.N_ID_AZIENDA_RIF));
    equal = equal && (this.T_TIPO == null ? that.T_TIPO == null : this.T_TIPO.equals(that.T_TIPO));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RCU_RCU_UDD)) {
      return false;
    }
    RCU_RCU_UDD that = (RCU_RCU_UDD) o;
    boolean equal = true;
    equal = equal && (this.N_ID_UDD == null ? that.N_ID_UDD == null : this.N_ID_UDD.equals(that.N_ID_UDD));
    equal = equal && (this.T_CODICE_TERNA == null ? that.T_CODICE_TERNA == null : this.T_CODICE_TERNA.equals(that.T_CODICE_TERNA));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.D_INIZIO == null ? that.D_INIZIO == null : this.D_INIZIO.equals(that.D_INIZIO));
    equal = equal && (this.D_FINE == null ? that.D_FINE == null : this.D_FINE.equals(that.D_FINE));
    equal = equal && (this.N_ID_AZIENDA_RIF == null ? that.N_ID_AZIENDA_RIF == null : this.N_ID_AZIENDA_RIF.equals(that.N_ID_AZIENDA_RIF));
    equal = equal && (this.T_TIPO == null ? that.T_TIPO == null : this.T_TIPO.equals(that.T_TIPO));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_UDD = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.T_CODICE_TERNA = JdbcWritableBridge.readString(2, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readTimestamp(3, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.D_INIZIO = JdbcWritableBridge.readTimestamp(6, __dbResults);
    this.D_FINE = JdbcWritableBridge.readTimestamp(7, __dbResults);
    this.N_ID_AZIENDA_RIF = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.T_TIPO = JdbcWritableBridge.readString(9, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_UDD = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.T_CODICE_TERNA = JdbcWritableBridge.readString(2, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readTimestamp(3, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.D_INIZIO = JdbcWritableBridge.readTimestamp(6, __dbResults);
    this.D_FINE = JdbcWritableBridge.readTimestamp(7, __dbResults);
    this.N_ID_AZIENDA_RIF = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.T_TIPO = JdbcWritableBridge.readString(9, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_UDD, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_TERNA, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(D_AGGIORNAMENTO, 3 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeTimestamp(D_INIZIO, 6 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(D_FINE, 7 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_AZIENDA_RIF, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO, 9 + __off, 12, __dbStmt);
    return 9;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_UDD, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_TERNA, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(D_AGGIORNAMENTO, 3 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeTimestamp(D_INIZIO, 6 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(D_FINE, 7 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_AZIENDA_RIF, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO, 9 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_UDD = null;
    } else {
    this.N_ID_UDD = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_TERNA = null;
    } else {
    this.T_CODICE_TERNA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_AGGIORNAMENTO = null;
    } else {
    this.D_AGGIORNAMENTO = new Timestamp(__dataIn.readLong());
    this.D_AGGIORNAMENTO.setNanos(__dataIn.readInt());
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
        this.D_INIZIO = null;
    } else {
    this.D_INIZIO = new Timestamp(__dataIn.readLong());
    this.D_INIZIO.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.D_FINE = null;
    } else {
    this.D_FINE = new Timestamp(__dataIn.readLong());
    this.D_FINE.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_AZIENDA_RIF = null;
    } else {
    this.N_ID_AZIENDA_RIF = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO = null;
    } else {
    this.T_TIPO = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_UDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UDD, __dataOut);
    }
    if (null == this.T_CODICE_TERNA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_TERNA);
    }
    if (null == this.D_AGGIORNAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.D_AGGIORNAMENTO.getTime());
    __dataOut.writeInt(this.D_AGGIORNAMENTO.getNanos());
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
    if (null == this.D_INIZIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.D_INIZIO.getTime());
    __dataOut.writeInt(this.D_INIZIO.getNanos());
    }
    if (null == this.D_FINE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.D_FINE.getTime());
    __dataOut.writeInt(this.D_FINE.getNanos());
    }
    if (null == this.N_ID_AZIENDA_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_AZIENDA_RIF, __dataOut);
    }
    if (null == this.T_TIPO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_UDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UDD, __dataOut);
    }
    if (null == this.T_CODICE_TERNA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_TERNA);
    }
    if (null == this.D_AGGIORNAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.D_AGGIORNAMENTO.getTime());
    __dataOut.writeInt(this.D_AGGIORNAMENTO.getNanos());
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
    if (null == this.D_INIZIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.D_INIZIO.getTime());
    __dataOut.writeInt(this.D_INIZIO.getNanos());
    }
    if (null == this.D_FINE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.D_FINE.getTime());
    __dataOut.writeInt(this.D_FINE.getNanos());
    }
    if (null == this.N_ID_AZIENDA_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_AZIENDA_RIF, __dataOut);
    }
    if (null == this.T_TIPO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UDD==null?"\\N":N_ID_UDD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_TERNA==null?"\\N":T_CODICE_TERNA, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(D_AGGIORNAMENTO==null?"\\N":"" + D_AGGIORNAMENTO, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TRACCIA==null?"\\N":N_ID_TRACCIA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_PREC==null?"\\N":N_ID_S_PREC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(D_INIZIO==null?"\\N":"" + D_INIZIO, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(D_FINE==null?"\\N":"" + D_FINE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_AZIENDA_RIF==null?"\\N":N_ID_AZIENDA_RIF.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO==null?"\\N":T_TIPO, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UDD==null?"\\N":N_ID_UDD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_TERNA==null?"\\N":T_CODICE_TERNA, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(D_AGGIORNAMENTO==null?"\\N":"" + D_AGGIORNAMENTO, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TRACCIA==null?"\\N":N_ID_TRACCIA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_PREC==null?"\\N":N_ID_S_PREC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(D_INIZIO==null?"\\N":"" + D_INIZIO, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(D_FINE==null?"\\N":"" + D_FINE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_AZIENDA_RIF==null?"\\N":N_ID_AZIENDA_RIF.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO==null?"\\N":T_TIPO, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UDD = null; } else {
      this.N_ID_UDD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_TERNA = null; } else {
      this.T_CODICE_TERNA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.D_AGGIORNAMENTO = null; } else {
      this.D_AGGIORNAMENTO = java.sql.Timestamp.valueOf(__cur_str);
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.D_INIZIO = null; } else {
      this.D_INIZIO = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.D_FINE = null; } else {
      this.D_FINE = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_AZIENDA_RIF = null; } else {
      this.N_ID_AZIENDA_RIF = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO = null; } else {
      this.T_TIPO = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UDD = null; } else {
      this.N_ID_UDD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_TERNA = null; } else {
      this.T_CODICE_TERNA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.D_AGGIORNAMENTO = null; } else {
      this.D_AGGIORNAMENTO = java.sql.Timestamp.valueOf(__cur_str);
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.D_INIZIO = null; } else {
      this.D_INIZIO = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.D_FINE = null; } else {
      this.D_FINE = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_AZIENDA_RIF = null; } else {
      this.N_ID_AZIENDA_RIF = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO = null; } else {
      this.T_TIPO = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    RCU_RCU_UDD o = (RCU_RCU_UDD) super.clone();
    o.D_AGGIORNAMENTO = (o.D_AGGIORNAMENTO != null) ? (java.sql.Timestamp) o.D_AGGIORNAMENTO.clone() : null;
    o.D_INIZIO = (o.D_INIZIO != null) ? (java.sql.Timestamp) o.D_INIZIO.clone() : null;
    o.D_FINE = (o.D_FINE != null) ? (java.sql.Timestamp) o.D_FINE.clone() : null;
    return o;
  }

  public void clone0(RCU_RCU_UDD o) throws CloneNotSupportedException {
    o.D_AGGIORNAMENTO = (o.D_AGGIORNAMENTO != null) ? (java.sql.Timestamp) o.D_AGGIORNAMENTO.clone() : null;
    o.D_INIZIO = (o.D_INIZIO != null) ? (java.sql.Timestamp) o.D_INIZIO.clone() : null;
    o.D_FINE = (o.D_FINE != null) ? (java.sql.Timestamp) o.D_FINE.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_UDD", this.N_ID_UDD);
    __sqoop$field_map.put("T_CODICE_TERNA", this.T_CODICE_TERNA);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("D_INIZIO", this.D_INIZIO);
    __sqoop$field_map.put("D_FINE", this.D_FINE);
    __sqoop$field_map.put("N_ID_AZIENDA_RIF", this.N_ID_AZIENDA_RIF);
    __sqoop$field_map.put("T_TIPO", this.T_TIPO);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_UDD", this.N_ID_UDD);
    __sqoop$field_map.put("T_CODICE_TERNA", this.T_CODICE_TERNA);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("D_INIZIO", this.D_INIZIO);
    __sqoop$field_map.put("D_FINE", this.D_FINE);
    __sqoop$field_map.put("N_ID_AZIENDA_RIF", this.N_ID_AZIENDA_RIF);
    __sqoop$field_map.put("T_TIPO", this.T_TIPO);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
