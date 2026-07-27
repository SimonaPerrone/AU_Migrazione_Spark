// ORM class for table 'rcugas.rcugas_connessioni_distr'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 11:40:36 CEST 2019
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

public class rcugas_rcugas_connessioni_distr extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("T_CODICE_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_PDR = (String)value;
      }
    });
    setters.put("N_ID_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_PDR = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_REMI", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_REMI = (java.math.BigDecimal)value;
      }
    });
    setters.put("D_DATA_INIZIO_CONN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_INIZIO_CONN = (String)value;
      }
    });
    setters.put("D_DATA_FINE_CONN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_FINE_CONN = (String)value;
      }
    });
    setters.put("T_REMI", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_REMI = (String)value;
      }
    });
    setters.put("N_ID_DISTR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_DISTR = (java.math.BigDecimal)value;
      }
    });
    setters.put("D_DATA_INIZIO_GESTECN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_INIZIO_GESTECN = (String)value;
      }
    });
    setters.put("D_DATA_FINE_GESTECN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_FINE_GESTECN = (String)value;
      }
    });
  }
  public rcugas_rcugas_connessioni_distr() {
    init0();
  }
  private String T_CODICE_PDR;
  public String get_T_CODICE_PDR() {
    return T_CODICE_PDR;
  }
  public void set_T_CODICE_PDR(String T_CODICE_PDR) {
    this.T_CODICE_PDR = T_CODICE_PDR;
  }
  public rcugas_rcugas_connessioni_distr with_T_CODICE_PDR(String T_CODICE_PDR) {
    this.T_CODICE_PDR = T_CODICE_PDR;
    return this;
  }
  private java.math.BigDecimal N_ID_PDR;
  public java.math.BigDecimal get_N_ID_PDR() {
    return N_ID_PDR;
  }
  public void set_N_ID_PDR(java.math.BigDecimal N_ID_PDR) {
    this.N_ID_PDR = N_ID_PDR;
  }
  public rcugas_rcugas_connessioni_distr with_N_ID_PDR(java.math.BigDecimal N_ID_PDR) {
    this.N_ID_PDR = N_ID_PDR;
    return this;
  }
  private java.math.BigDecimal N_ID_REMI;
  public java.math.BigDecimal get_N_ID_REMI() {
    return N_ID_REMI;
  }
  public void set_N_ID_REMI(java.math.BigDecimal N_ID_REMI) {
    this.N_ID_REMI = N_ID_REMI;
  }
  public rcugas_rcugas_connessioni_distr with_N_ID_REMI(java.math.BigDecimal N_ID_REMI) {
    this.N_ID_REMI = N_ID_REMI;
    return this;
  }
  private String D_DATA_INIZIO_CONN;
  public String get_D_DATA_INIZIO_CONN() {
    return D_DATA_INIZIO_CONN;
  }
  public void set_D_DATA_INIZIO_CONN(String D_DATA_INIZIO_CONN) {
    this.D_DATA_INIZIO_CONN = D_DATA_INIZIO_CONN;
  }
  public rcugas_rcugas_connessioni_distr with_D_DATA_INIZIO_CONN(String D_DATA_INIZIO_CONN) {
    this.D_DATA_INIZIO_CONN = D_DATA_INIZIO_CONN;
    return this;
  }
  private String D_DATA_FINE_CONN;
  public String get_D_DATA_FINE_CONN() {
    return D_DATA_FINE_CONN;
  }
  public void set_D_DATA_FINE_CONN(String D_DATA_FINE_CONN) {
    this.D_DATA_FINE_CONN = D_DATA_FINE_CONN;
  }
  public rcugas_rcugas_connessioni_distr with_D_DATA_FINE_CONN(String D_DATA_FINE_CONN) {
    this.D_DATA_FINE_CONN = D_DATA_FINE_CONN;
    return this;
  }
  private String T_REMI;
  public String get_T_REMI() {
    return T_REMI;
  }
  public void set_T_REMI(String T_REMI) {
    this.T_REMI = T_REMI;
  }
  public rcugas_rcugas_connessioni_distr with_T_REMI(String T_REMI) {
    this.T_REMI = T_REMI;
    return this;
  }
  private java.math.BigDecimal N_ID_DISTR;
  public java.math.BigDecimal get_N_ID_DISTR() {
    return N_ID_DISTR;
  }
  public void set_N_ID_DISTR(java.math.BigDecimal N_ID_DISTR) {
    this.N_ID_DISTR = N_ID_DISTR;
  }
  public rcugas_rcugas_connessioni_distr with_N_ID_DISTR(java.math.BigDecimal N_ID_DISTR) {
    this.N_ID_DISTR = N_ID_DISTR;
    return this;
  }
  private String D_DATA_INIZIO_GESTECN;
  public String get_D_DATA_INIZIO_GESTECN() {
    return D_DATA_INIZIO_GESTECN;
  }
  public void set_D_DATA_INIZIO_GESTECN(String D_DATA_INIZIO_GESTECN) {
    this.D_DATA_INIZIO_GESTECN = D_DATA_INIZIO_GESTECN;
  }
  public rcugas_rcugas_connessioni_distr with_D_DATA_INIZIO_GESTECN(String D_DATA_INIZIO_GESTECN) {
    this.D_DATA_INIZIO_GESTECN = D_DATA_INIZIO_GESTECN;
    return this;
  }
  private String D_DATA_FINE_GESTECN;
  public String get_D_DATA_FINE_GESTECN() {
    return D_DATA_FINE_GESTECN;
  }
  public void set_D_DATA_FINE_GESTECN(String D_DATA_FINE_GESTECN) {
    this.D_DATA_FINE_GESTECN = D_DATA_FINE_GESTECN;
  }
  public rcugas_rcugas_connessioni_distr with_D_DATA_FINE_GESTECN(String D_DATA_FINE_GESTECN) {
    this.D_DATA_FINE_GESTECN = D_DATA_FINE_GESTECN;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcugas_rcugas_connessioni_distr)) {
      return false;
    }
    rcugas_rcugas_connessioni_distr that = (rcugas_rcugas_connessioni_distr) o;
    boolean equal = true;
    equal = equal && (this.T_CODICE_PDR == null ? that.T_CODICE_PDR == null : this.T_CODICE_PDR.equals(that.T_CODICE_PDR));
    equal = equal && (this.N_ID_PDR == null ? that.N_ID_PDR == null : this.N_ID_PDR.equals(that.N_ID_PDR));
    equal = equal && (this.N_ID_REMI == null ? that.N_ID_REMI == null : this.N_ID_REMI.equals(that.N_ID_REMI));
    equal = equal && (this.D_DATA_INIZIO_CONN == null ? that.D_DATA_INIZIO_CONN == null : this.D_DATA_INIZIO_CONN.equals(that.D_DATA_INIZIO_CONN));
    equal = equal && (this.D_DATA_FINE_CONN == null ? that.D_DATA_FINE_CONN == null : this.D_DATA_FINE_CONN.equals(that.D_DATA_FINE_CONN));
    equal = equal && (this.T_REMI == null ? that.T_REMI == null : this.T_REMI.equals(that.T_REMI));
    equal = equal && (this.N_ID_DISTR == null ? that.N_ID_DISTR == null : this.N_ID_DISTR.equals(that.N_ID_DISTR));
    equal = equal && (this.D_DATA_INIZIO_GESTECN == null ? that.D_DATA_INIZIO_GESTECN == null : this.D_DATA_INIZIO_GESTECN.equals(that.D_DATA_INIZIO_GESTECN));
    equal = equal && (this.D_DATA_FINE_GESTECN == null ? that.D_DATA_FINE_GESTECN == null : this.D_DATA_FINE_GESTECN.equals(that.D_DATA_FINE_GESTECN));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcugas_rcugas_connessioni_distr)) {
      return false;
    }
    rcugas_rcugas_connessioni_distr that = (rcugas_rcugas_connessioni_distr) o;
    boolean equal = true;
    equal = equal && (this.T_CODICE_PDR == null ? that.T_CODICE_PDR == null : this.T_CODICE_PDR.equals(that.T_CODICE_PDR));
    equal = equal && (this.N_ID_PDR == null ? that.N_ID_PDR == null : this.N_ID_PDR.equals(that.N_ID_PDR));
    equal = equal && (this.N_ID_REMI == null ? that.N_ID_REMI == null : this.N_ID_REMI.equals(that.N_ID_REMI));
    equal = equal && (this.D_DATA_INIZIO_CONN == null ? that.D_DATA_INIZIO_CONN == null : this.D_DATA_INIZIO_CONN.equals(that.D_DATA_INIZIO_CONN));
    equal = equal && (this.D_DATA_FINE_CONN == null ? that.D_DATA_FINE_CONN == null : this.D_DATA_FINE_CONN.equals(that.D_DATA_FINE_CONN));
    equal = equal && (this.T_REMI == null ? that.T_REMI == null : this.T_REMI.equals(that.T_REMI));
    equal = equal && (this.N_ID_DISTR == null ? that.N_ID_DISTR == null : this.N_ID_DISTR.equals(that.N_ID_DISTR));
    equal = equal && (this.D_DATA_INIZIO_GESTECN == null ? that.D_DATA_INIZIO_GESTECN == null : this.D_DATA_INIZIO_GESTECN.equals(that.D_DATA_INIZIO_GESTECN));
    equal = equal && (this.D_DATA_FINE_GESTECN == null ? that.D_DATA_FINE_GESTECN == null : this.D_DATA_FINE_GESTECN.equals(that.D_DATA_FINE_GESTECN));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.T_CODICE_PDR = JdbcWritableBridge.readString(1, __dbResults);
    this.N_ID_PDR = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.N_ID_REMI = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.D_DATA_INIZIO_CONN = JdbcWritableBridge.readString(4, __dbResults);
    this.D_DATA_FINE_CONN = JdbcWritableBridge.readString(5, __dbResults);
    this.T_REMI = JdbcWritableBridge.readString(6, __dbResults);
    this.N_ID_DISTR = JdbcWritableBridge.readBigDecimal(7, __dbResults);
    this.D_DATA_INIZIO_GESTECN = JdbcWritableBridge.readString(8, __dbResults);
    this.D_DATA_FINE_GESTECN = JdbcWritableBridge.readString(9, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.T_CODICE_PDR = JdbcWritableBridge.readString(1, __dbResults);
    this.N_ID_PDR = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.N_ID_REMI = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.D_DATA_INIZIO_CONN = JdbcWritableBridge.readString(4, __dbResults);
    this.D_DATA_FINE_CONN = JdbcWritableBridge.readString(5, __dbResults);
    this.T_REMI = JdbcWritableBridge.readString(6, __dbResults);
    this.N_ID_DISTR = JdbcWritableBridge.readBigDecimal(7, __dbResults);
    this.D_DATA_INIZIO_GESTECN = JdbcWritableBridge.readString(8, __dbResults);
    this.D_DATA_FINE_GESTECN = JdbcWritableBridge.readString(9, __dbResults);
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
    JdbcWritableBridge.writeString(T_CODICE_PDR, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PDR, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_REMI, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INIZIO_CONN, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_FINE_CONN, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_REMI, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTR, 7 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INIZIO_GESTECN, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_FINE_GESTECN, 9 + __off, 93, __dbStmt);
    return 9;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(T_CODICE_PDR, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PDR, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_REMI, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INIZIO_CONN, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_FINE_CONN, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_REMI, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTR, 7 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INIZIO_GESTECN, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_FINE_GESTECN, 9 + __off, 93, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_PDR = null;
    } else {
    this.T_CODICE_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_PDR = null;
    } else {
    this.N_ID_PDR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_REMI = null;
    } else {
    this.N_ID_REMI = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_INIZIO_CONN = null;
    } else {
    this.D_DATA_INIZIO_CONN = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_FINE_CONN = null;
    } else {
    this.D_DATA_FINE_CONN = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_REMI = null;
    } else {
    this.T_REMI = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_DISTR = null;
    } else {
    this.N_ID_DISTR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_INIZIO_GESTECN = null;
    } else {
    this.D_DATA_INIZIO_GESTECN = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_FINE_GESTECN = null;
    } else {
    this.D_DATA_FINE_GESTECN = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.T_CODICE_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_PDR);
    }
    if (null == this.N_ID_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PDR, __dataOut);
    }
    if (null == this.N_ID_REMI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_REMI, __dataOut);
    }
    if (null == this.D_DATA_INIZIO_CONN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INIZIO_CONN);
    }
    if (null == this.D_DATA_FINE_CONN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_FINE_CONN);
    }
    if (null == this.T_REMI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_REMI);
    }
    if (null == this.N_ID_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISTR, __dataOut);
    }
    if (null == this.D_DATA_INIZIO_GESTECN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INIZIO_GESTECN);
    }
    if (null == this.D_DATA_FINE_GESTECN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_FINE_GESTECN);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.T_CODICE_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_PDR);
    }
    if (null == this.N_ID_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PDR, __dataOut);
    }
    if (null == this.N_ID_REMI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_REMI, __dataOut);
    }
    if (null == this.D_DATA_INIZIO_CONN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INIZIO_CONN);
    }
    if (null == this.D_DATA_FINE_CONN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_FINE_CONN);
    }
    if (null == this.T_REMI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_REMI);
    }
    if (null == this.N_ID_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISTR, __dataOut);
    }
    if (null == this.D_DATA_INIZIO_GESTECN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INIZIO_GESTECN);
    }
    if (null == this.D_DATA_FINE_GESTECN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_FINE_GESTECN);
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
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_PDR==null?"":T_CODICE_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PDR==null?"":N_ID_PDR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_REMI==null?"":N_ID_REMI.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INIZIO_CONN==null?"":D_DATA_INIZIO_CONN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_FINE_CONN==null?"":D_DATA_FINE_CONN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_REMI==null?"":T_REMI, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISTR==null?"":N_ID_DISTR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INIZIO_GESTECN==null?"":D_DATA_INIZIO_GESTECN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_FINE_GESTECN==null?"":D_DATA_FINE_GESTECN, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_PDR==null?"":T_CODICE_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PDR==null?"":N_ID_PDR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_REMI==null?"":N_ID_REMI.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INIZIO_CONN==null?"":D_DATA_INIZIO_CONN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_FINE_CONN==null?"":D_DATA_FINE_CONN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_REMI==null?"":T_REMI, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISTR==null?"":N_ID_DISTR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INIZIO_GESTECN==null?"":D_DATA_INIZIO_GESTECN, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_FINE_GESTECN==null?"":D_DATA_FINE_GESTECN, " ", delimiters));
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
    if (__cur_str.equals("null")) { this.T_CODICE_PDR = null; } else {
      this.T_CODICE_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PDR = null; } else {
      this.N_ID_PDR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_REMI = null; } else {
      this.N_ID_REMI = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INIZIO_CONN = null; } else {
      this.D_DATA_INIZIO_CONN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_FINE_CONN = null; } else {
      this.D_DATA_FINE_CONN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_REMI = null; } else {
      this.T_REMI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISTR = null; } else {
      this.N_ID_DISTR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INIZIO_GESTECN = null; } else {
      this.D_DATA_INIZIO_GESTECN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_FINE_GESTECN = null; } else {
      this.D_DATA_FINE_GESTECN = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_PDR = null; } else {
      this.T_CODICE_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PDR = null; } else {
      this.N_ID_PDR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_REMI = null; } else {
      this.N_ID_REMI = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INIZIO_CONN = null; } else {
      this.D_DATA_INIZIO_CONN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_FINE_CONN = null; } else {
      this.D_DATA_FINE_CONN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_REMI = null; } else {
      this.T_REMI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISTR = null; } else {
      this.N_ID_DISTR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INIZIO_GESTECN = null; } else {
      this.D_DATA_INIZIO_GESTECN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_FINE_GESTECN = null; } else {
      this.D_DATA_FINE_GESTECN = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    rcugas_rcugas_connessioni_distr o = (rcugas_rcugas_connessioni_distr) super.clone();
    return o;
  }

  public void clone0(rcugas_rcugas_connessioni_distr o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("T_CODICE_PDR", this.T_CODICE_PDR);
    __sqoop$field_map.put("N_ID_PDR", this.N_ID_PDR);
    __sqoop$field_map.put("N_ID_REMI", this.N_ID_REMI);
    __sqoop$field_map.put("D_DATA_INIZIO_CONN", this.D_DATA_INIZIO_CONN);
    __sqoop$field_map.put("D_DATA_FINE_CONN", this.D_DATA_FINE_CONN);
    __sqoop$field_map.put("T_REMI", this.T_REMI);
    __sqoop$field_map.put("N_ID_DISTR", this.N_ID_DISTR);
    __sqoop$field_map.put("D_DATA_INIZIO_GESTECN", this.D_DATA_INIZIO_GESTECN);
    __sqoop$field_map.put("D_DATA_FINE_GESTECN", this.D_DATA_FINE_GESTECN);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("T_CODICE_PDR", this.T_CODICE_PDR);
    __sqoop$field_map.put("N_ID_PDR", this.N_ID_PDR);
    __sqoop$field_map.put("N_ID_REMI", this.N_ID_REMI);
    __sqoop$field_map.put("D_DATA_INIZIO_CONN", this.D_DATA_INIZIO_CONN);
    __sqoop$field_map.put("D_DATA_FINE_CONN", this.D_DATA_FINE_CONN);
    __sqoop$field_map.put("T_REMI", this.T_REMI);
    __sqoop$field_map.put("N_ID_DISTR", this.N_ID_DISTR);
    __sqoop$field_map.put("D_DATA_INIZIO_GESTECN", this.D_DATA_INIZIO_GESTECN);
    __sqoop$field_map.put("D_DATA_FINE_GESTECN", this.D_DATA_FINE_GESTECN);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
