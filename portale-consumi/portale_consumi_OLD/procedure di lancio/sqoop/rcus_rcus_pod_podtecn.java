// ORM class for table 'rcus.rcus_pod_podtecn'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 13:30:55 CEST 2019
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

public class rcus_rcus_pod_podtecn extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_S_POD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_S_POD = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_S_PODTECN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_S_PODTECN = (java.math.BigDecimal)value;
      }
    });
    setters.put("D_AGGIORNAMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_AGGIORNAMENTO = (String)value;
      }
    });
  }
  public rcus_rcus_pod_podtecn() {
    init0();
  }
  private java.math.BigDecimal N_ID_S_POD;
  public java.math.BigDecimal get_N_ID_S_POD() {
    return N_ID_S_POD;
  }
  public void set_N_ID_S_POD(java.math.BigDecimal N_ID_S_POD) {
    this.N_ID_S_POD = N_ID_S_POD;
  }
  public rcus_rcus_pod_podtecn with_N_ID_S_POD(java.math.BigDecimal N_ID_S_POD) {
    this.N_ID_S_POD = N_ID_S_POD;
    return this;
  }
  private java.math.BigDecimal N_ID_S_PODTECN;
  public java.math.BigDecimal get_N_ID_S_PODTECN() {
    return N_ID_S_PODTECN;
  }
  public void set_N_ID_S_PODTECN(java.math.BigDecimal N_ID_S_PODTECN) {
    this.N_ID_S_PODTECN = N_ID_S_PODTECN;
  }
  public rcus_rcus_pod_podtecn with_N_ID_S_PODTECN(java.math.BigDecimal N_ID_S_PODTECN) {
    this.N_ID_S_PODTECN = N_ID_S_PODTECN;
    return this;
  }
  private String D_AGGIORNAMENTO;
  public String get_D_AGGIORNAMENTO() {
    return D_AGGIORNAMENTO;
  }
  public void set_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
    this.D_AGGIORNAMENTO = D_AGGIORNAMENTO;
  }
  public rcus_rcus_pod_podtecn with_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
    this.D_AGGIORNAMENTO = D_AGGIORNAMENTO;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcus_rcus_pod_podtecn)) {
      return false;
    }
    rcus_rcus_pod_podtecn that = (rcus_rcus_pod_podtecn) o;
    boolean equal = true;
    equal = equal && (this.N_ID_S_POD == null ? that.N_ID_S_POD == null : this.N_ID_S_POD.equals(that.N_ID_S_POD));
    equal = equal && (this.N_ID_S_PODTECN == null ? that.N_ID_S_PODTECN == null : this.N_ID_S_PODTECN.equals(that.N_ID_S_PODTECN));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcus_rcus_pod_podtecn)) {
      return false;
    }
    rcus_rcus_pod_podtecn that = (rcus_rcus_pod_podtecn) o;
    boolean equal = true;
    equal = equal && (this.N_ID_S_POD == null ? that.N_ID_S_POD == null : this.N_ID_S_POD.equals(that.N_ID_S_POD));
    equal = equal && (this.N_ID_S_PODTECN == null ? that.N_ID_S_PODTECN == null : this.N_ID_S_PODTECN.equals(that.N_ID_S_PODTECN));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_S_POD = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_S_PODTECN = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(3, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_S_POD = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_S_PODTECN = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(3, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_S_POD, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PODTECN, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 3 + __off, 93, __dbStmt);
    return 3;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_S_POD, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PODTECN, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 3 + __off, 93, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_S_POD = null;
    } else {
    this.N_ID_S_POD = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_S_PODTECN = null;
    } else {
    this.N_ID_S_PODTECN = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_AGGIORNAMENTO = null;
    } else {
    this.D_AGGIORNAMENTO = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_S_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_S_POD, __dataOut);
    }
    if (null == this.N_ID_S_PODTECN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_S_PODTECN, __dataOut);
    }
    if (null == this.D_AGGIORNAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_AGGIORNAMENTO);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_S_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_S_POD, __dataOut);
    }
    if (null == this.N_ID_S_PODTECN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_S_PODTECN, __dataOut);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_POD==null?"":N_ID_S_POD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_PODTECN==null?"":N_ID_S_PODTECN.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO==null?"":D_AGGIORNAMENTO, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_POD==null?"":N_ID_S_POD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_PODTECN==null?"":N_ID_S_PODTECN.toPlainString(), delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_S_POD = null; } else {
      this.N_ID_S_POD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_S_PODTECN = null; } else {
      this.N_ID_S_PODTECN = new java.math.BigDecimal(__cur_str);
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_S_POD = null; } else {
      this.N_ID_S_POD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_S_PODTECN = null; } else {
      this.N_ID_S_PODTECN = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_AGGIORNAMENTO = null; } else {
      this.D_AGGIORNAMENTO = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    rcus_rcus_pod_podtecn o = (rcus_rcus_pod_podtecn) super.clone();
    return o;
  }

  public void clone0(rcus_rcus_pod_podtecn o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_S_POD", this.N_ID_S_POD);
    __sqoop$field_map.put("N_ID_S_PODTECN", this.N_ID_S_PODTECN);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_S_POD", this.N_ID_S_POD);
    __sqoop$field_map.put("N_ID_S_PODTECN", this.N_ID_S_PODTECN);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
