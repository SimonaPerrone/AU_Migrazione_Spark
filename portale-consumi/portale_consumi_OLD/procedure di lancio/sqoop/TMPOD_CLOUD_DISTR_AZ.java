// ORM class for table 'TMPOD_CLOUD.DISTR_AZ'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sun Nov 24 15:31:30 GMT 2019
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

public class TMPOD_CLOUD_DISTR_AZ extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_DISTR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_DISTR = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_DISTR_RIF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_DISTR_RIF = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_TIPO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO = (String)value;
      }
    });
    setters.put("T_PIVA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PIVA = (String)value;
      }
    });
  }
  public TMPOD_CLOUD_DISTR_AZ() {
    init0();
  }
  private java.math.BigDecimal N_ID_DISTR;
  public java.math.BigDecimal get_N_ID_DISTR() {
    return N_ID_DISTR;
  }
  public void set_N_ID_DISTR(java.math.BigDecimal N_ID_DISTR) {
    this.N_ID_DISTR = N_ID_DISTR;
  }
  public TMPOD_CLOUD_DISTR_AZ with_N_ID_DISTR(java.math.BigDecimal N_ID_DISTR) {
    this.N_ID_DISTR = N_ID_DISTR;
    return this;
  }
  private java.math.BigDecimal N_ID_DISTR_RIF;
  public java.math.BigDecimal get_N_ID_DISTR_RIF() {
    return N_ID_DISTR_RIF;
  }
  public void set_N_ID_DISTR_RIF(java.math.BigDecimal N_ID_DISTR_RIF) {
    this.N_ID_DISTR_RIF = N_ID_DISTR_RIF;
  }
  public TMPOD_CLOUD_DISTR_AZ with_N_ID_DISTR_RIF(java.math.BigDecimal N_ID_DISTR_RIF) {
    this.N_ID_DISTR_RIF = N_ID_DISTR_RIF;
    return this;
  }
  private String T_TIPO;
  public String get_T_TIPO() {
    return T_TIPO;
  }
  public void set_T_TIPO(String T_TIPO) {
    this.T_TIPO = T_TIPO;
  }
  public TMPOD_CLOUD_DISTR_AZ with_T_TIPO(String T_TIPO) {
    this.T_TIPO = T_TIPO;
    return this;
  }
  private String T_PIVA;
  public String get_T_PIVA() {
    return T_PIVA;
  }
  public void set_T_PIVA(String T_PIVA) {
    this.T_PIVA = T_PIVA;
  }
  public TMPOD_CLOUD_DISTR_AZ with_T_PIVA(String T_PIVA) {
    this.T_PIVA = T_PIVA;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TMPOD_CLOUD_DISTR_AZ)) {
      return false;
    }
    TMPOD_CLOUD_DISTR_AZ that = (TMPOD_CLOUD_DISTR_AZ) o;
    boolean equal = true;
    equal = equal && (this.N_ID_DISTR == null ? that.N_ID_DISTR == null : this.N_ID_DISTR.equals(that.N_ID_DISTR));
    equal = equal && (this.N_ID_DISTR_RIF == null ? that.N_ID_DISTR_RIF == null : this.N_ID_DISTR_RIF.equals(that.N_ID_DISTR_RIF));
    equal = equal && (this.T_TIPO == null ? that.T_TIPO == null : this.T_TIPO.equals(that.T_TIPO));
    equal = equal && (this.T_PIVA == null ? that.T_PIVA == null : this.T_PIVA.equals(that.T_PIVA));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TMPOD_CLOUD_DISTR_AZ)) {
      return false;
    }
    TMPOD_CLOUD_DISTR_AZ that = (TMPOD_CLOUD_DISTR_AZ) o;
    boolean equal = true;
    equal = equal && (this.N_ID_DISTR == null ? that.N_ID_DISTR == null : this.N_ID_DISTR.equals(that.N_ID_DISTR));
    equal = equal && (this.N_ID_DISTR_RIF == null ? that.N_ID_DISTR_RIF == null : this.N_ID_DISTR_RIF.equals(that.N_ID_DISTR_RIF));
    equal = equal && (this.T_TIPO == null ? that.T_TIPO == null : this.T_TIPO.equals(that.T_TIPO));
    equal = equal && (this.T_PIVA == null ? that.T_PIVA == null : this.T_PIVA.equals(that.T_PIVA));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_DISTR = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_DISTR_RIF = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_TIPO = JdbcWritableBridge.readString(3, __dbResults);
    this.T_PIVA = JdbcWritableBridge.readString(4, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_DISTR = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_DISTR_RIF = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_TIPO = JdbcWritableBridge.readString(3, __dbResults);
    this.T_PIVA = JdbcWritableBridge.readString(4, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTR, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTR_RIF, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA, 4 + __off, 12, __dbStmt);
    return 4;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTR, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTR_RIF, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA, 4 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_DISTR = null;
    } else {
    this.N_ID_DISTR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_DISTR_RIF = null;
    } else {
    this.N_ID_DISTR_RIF = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO = null;
    } else {
    this.T_TIPO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PIVA = null;
    } else {
    this.T_PIVA = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISTR, __dataOut);
    }
    if (null == this.N_ID_DISTR_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISTR_RIF, __dataOut);
    }
    if (null == this.T_TIPO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO);
    }
    if (null == this.T_PIVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISTR, __dataOut);
    }
    if (null == this.N_ID_DISTR_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISTR_RIF, __dataOut);
    }
    if (null == this.T_TIPO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO);
    }
    if (null == this.T_PIVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISTR==null?"\\N":N_ID_DISTR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISTR_RIF==null?"\\N":N_ID_DISTR_RIF.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO==null?"\\N":T_TIPO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA==null?"\\N":T_PIVA, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISTR==null?"\\N":N_ID_DISTR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISTR_RIF==null?"\\N":N_ID_DISTR_RIF.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO==null?"\\N":T_TIPO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA==null?"\\N":T_PIVA, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISTR = null; } else {
      this.N_ID_DISTR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISTR_RIF = null; } else {
      this.N_ID_DISTR_RIF = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO = null; } else {
      this.T_TIPO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA = null; } else {
      this.T_PIVA = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISTR = null; } else {
      this.N_ID_DISTR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISTR_RIF = null; } else {
      this.N_ID_DISTR_RIF = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO = null; } else {
      this.T_TIPO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA = null; } else {
      this.T_PIVA = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    TMPOD_CLOUD_DISTR_AZ o = (TMPOD_CLOUD_DISTR_AZ) super.clone();
    return o;
  }

  public void clone0(TMPOD_CLOUD_DISTR_AZ o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_DISTR", this.N_ID_DISTR);
    __sqoop$field_map.put("N_ID_DISTR_RIF", this.N_ID_DISTR_RIF);
    __sqoop$field_map.put("T_TIPO", this.T_TIPO);
    __sqoop$field_map.put("T_PIVA", this.T_PIVA);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_DISTR", this.N_ID_DISTR);
    __sqoop$field_map.put("N_ID_DISTR_RIF", this.N_ID_DISTR_RIF);
    __sqoop$field_map.put("T_TIPO", this.T_TIPO);
    __sqoop$field_map.put("T_PIVA", this.T_PIVA);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
