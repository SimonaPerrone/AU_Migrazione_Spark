// ORM class for table 'switch_gas.prt_vtg_operazione'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Sep 14 08:00:16 CEST 2019
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

public class switch_gas_prt_vtg_operazione extends SqoopRecord  implements DBWritable, Writable {
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
    setters.put("N_ID_VTG", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_VTG = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_COD_OP", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COD_OP = (String)value;
      }
    });
    setters.put("N_ID_UTENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_UTENTE = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_OPERATORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_OPERATORE = (java.math.BigDecimal)value;
      }
    });
    setters.put("D_RIC_RICH", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_RIC_RICH = (String)value;
      }
    });
    setters.put("D_ESECUZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_ESECUZIONE = (String)value;
      }
    });
  }
  public switch_gas_prt_vtg_operazione() {
    init0();
  }
  private java.math.BigDecimal N_ID;
  public java.math.BigDecimal get_N_ID() {
    return N_ID;
  }
  public void set_N_ID(java.math.BigDecimal N_ID) {
    this.N_ID = N_ID;
  }
  public switch_gas_prt_vtg_operazione with_N_ID(java.math.BigDecimal N_ID) {
    this.N_ID = N_ID;
    return this;
  }
  private java.math.BigDecimal N_ID_VTG;
  public java.math.BigDecimal get_N_ID_VTG() {
    return N_ID_VTG;
  }
  public void set_N_ID_VTG(java.math.BigDecimal N_ID_VTG) {
    this.N_ID_VTG = N_ID_VTG;
  }
  public switch_gas_prt_vtg_operazione with_N_ID_VTG(java.math.BigDecimal N_ID_VTG) {
    this.N_ID_VTG = N_ID_VTG;
    return this;
  }
  private String T_COD_OP;
  public String get_T_COD_OP() {
    return T_COD_OP;
  }
  public void set_T_COD_OP(String T_COD_OP) {
    this.T_COD_OP = T_COD_OP;
  }
  public switch_gas_prt_vtg_operazione with_T_COD_OP(String T_COD_OP) {
    this.T_COD_OP = T_COD_OP;
    return this;
  }
  private java.math.BigDecimal N_ID_UTENTE;
  public java.math.BigDecimal get_N_ID_UTENTE() {
    return N_ID_UTENTE;
  }
  public void set_N_ID_UTENTE(java.math.BigDecimal N_ID_UTENTE) {
    this.N_ID_UTENTE = N_ID_UTENTE;
  }
  public switch_gas_prt_vtg_operazione with_N_ID_UTENTE(java.math.BigDecimal N_ID_UTENTE) {
    this.N_ID_UTENTE = N_ID_UTENTE;
    return this;
  }
  private java.math.BigDecimal N_ID_OPERATORE;
  public java.math.BigDecimal get_N_ID_OPERATORE() {
    return N_ID_OPERATORE;
  }
  public void set_N_ID_OPERATORE(java.math.BigDecimal N_ID_OPERATORE) {
    this.N_ID_OPERATORE = N_ID_OPERATORE;
  }
  public switch_gas_prt_vtg_operazione with_N_ID_OPERATORE(java.math.BigDecimal N_ID_OPERATORE) {
    this.N_ID_OPERATORE = N_ID_OPERATORE;
    return this;
  }
  private String D_RIC_RICH;
  public String get_D_RIC_RICH() {
    return D_RIC_RICH;
  }
  public void set_D_RIC_RICH(String D_RIC_RICH) {
    this.D_RIC_RICH = D_RIC_RICH;
  }
  public switch_gas_prt_vtg_operazione with_D_RIC_RICH(String D_RIC_RICH) {
    this.D_RIC_RICH = D_RIC_RICH;
    return this;
  }
  private String D_ESECUZIONE;
  public String get_D_ESECUZIONE() {
    return D_ESECUZIONE;
  }
  public void set_D_ESECUZIONE(String D_ESECUZIONE) {
    this.D_ESECUZIONE = D_ESECUZIONE;
  }
  public switch_gas_prt_vtg_operazione with_D_ESECUZIONE(String D_ESECUZIONE) {
    this.D_ESECUZIONE = D_ESECUZIONE;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof switch_gas_prt_vtg_operazione)) {
      return false;
    }
    switch_gas_prt_vtg_operazione that = (switch_gas_prt_vtg_operazione) o;
    boolean equal = true;
    equal = equal && (this.N_ID == null ? that.N_ID == null : this.N_ID.equals(that.N_ID));
    equal = equal && (this.N_ID_VTG == null ? that.N_ID_VTG == null : this.N_ID_VTG.equals(that.N_ID_VTG));
    equal = equal && (this.T_COD_OP == null ? that.T_COD_OP == null : this.T_COD_OP.equals(that.T_COD_OP));
    equal = equal && (this.N_ID_UTENTE == null ? that.N_ID_UTENTE == null : this.N_ID_UTENTE.equals(that.N_ID_UTENTE));
    equal = equal && (this.N_ID_OPERATORE == null ? that.N_ID_OPERATORE == null : this.N_ID_OPERATORE.equals(that.N_ID_OPERATORE));
    equal = equal && (this.D_RIC_RICH == null ? that.D_RIC_RICH == null : this.D_RIC_RICH.equals(that.D_RIC_RICH));
    equal = equal && (this.D_ESECUZIONE == null ? that.D_ESECUZIONE == null : this.D_ESECUZIONE.equals(that.D_ESECUZIONE));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof switch_gas_prt_vtg_operazione)) {
      return false;
    }
    switch_gas_prt_vtg_operazione that = (switch_gas_prt_vtg_operazione) o;
    boolean equal = true;
    equal = equal && (this.N_ID == null ? that.N_ID == null : this.N_ID.equals(that.N_ID));
    equal = equal && (this.N_ID_VTG == null ? that.N_ID_VTG == null : this.N_ID_VTG.equals(that.N_ID_VTG));
    equal = equal && (this.T_COD_OP == null ? that.T_COD_OP == null : this.T_COD_OP.equals(that.T_COD_OP));
    equal = equal && (this.N_ID_UTENTE == null ? that.N_ID_UTENTE == null : this.N_ID_UTENTE.equals(that.N_ID_UTENTE));
    equal = equal && (this.N_ID_OPERATORE == null ? that.N_ID_OPERATORE == null : this.N_ID_OPERATORE.equals(that.N_ID_OPERATORE));
    equal = equal && (this.D_RIC_RICH == null ? that.D_RIC_RICH == null : this.D_RIC_RICH.equals(that.D_RIC_RICH));
    equal = equal && (this.D_ESECUZIONE == null ? that.D_ESECUZIONE == null : this.D_ESECUZIONE.equals(that.D_ESECUZIONE));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_VTG = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_COD_OP = JdbcWritableBridge.readString(3, __dbResults);
    this.N_ID_UTENTE = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.N_ID_OPERATORE = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.D_RIC_RICH = JdbcWritableBridge.readString(6, __dbResults);
    this.D_ESECUZIONE = JdbcWritableBridge.readString(7, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_VTG = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_COD_OP = JdbcWritableBridge.readString(3, __dbResults);
    this.N_ID_UTENTE = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.N_ID_OPERATORE = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.D_RIC_RICH = JdbcWritableBridge.readString(6, __dbResults);
    this.D_ESECUZIONE = JdbcWritableBridge.readString(7, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_VTG, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_OP, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UTENTE, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_OPERATORE, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_RIC_RICH, 6 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_ESECUZIONE, 7 + __off, 93, __dbStmt);
    return 7;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_VTG, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_OP, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UTENTE, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_OPERATORE, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_RIC_RICH, 6 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_ESECUZIONE, 7 + __off, 93, __dbStmt);
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
        this.N_ID_VTG = null;
    } else {
    this.N_ID_VTG = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COD_OP = null;
    } else {
    this.T_COD_OP = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_UTENTE = null;
    } else {
    this.N_ID_UTENTE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_OPERATORE = null;
    } else {
    this.N_ID_OPERATORE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_RIC_RICH = null;
    } else {
    this.D_RIC_RICH = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_ESECUZIONE = null;
    } else {
    this.D_ESECUZIONE = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID, __dataOut);
    }
    if (null == this.N_ID_VTG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_VTG, __dataOut);
    }
    if (null == this.T_COD_OP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_OP);
    }
    if (null == this.N_ID_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UTENTE, __dataOut);
    }
    if (null == this.N_ID_OPERATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_OPERATORE, __dataOut);
    }
    if (null == this.D_RIC_RICH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_RIC_RICH);
    }
    if (null == this.D_ESECUZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_ESECUZIONE);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID, __dataOut);
    }
    if (null == this.N_ID_VTG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_VTG, __dataOut);
    }
    if (null == this.T_COD_OP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_OP);
    }
    if (null == this.N_ID_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UTENTE, __dataOut);
    }
    if (null == this.N_ID_OPERATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_OPERATORE, __dataOut);
    }
    if (null == this.D_RIC_RICH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_RIC_RICH);
    }
    if (null == this.D_ESECUZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_ESECUZIONE);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_VTG==null?"":N_ID_VTG.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_OP==null?"":T_COD_OP, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UTENTE==null?"":N_ID_UTENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_OPERATORE==null?"":N_ID_OPERATORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_RIC_RICH==null?"":D_RIC_RICH, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_ESECUZIONE==null?"":D_ESECUZIONE, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID==null?"":N_ID.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_VTG==null?"":N_ID_VTG.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_OP==null?"":T_COD_OP, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UTENTE==null?"":N_ID_UTENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_OPERATORE==null?"":N_ID_OPERATORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_RIC_RICH==null?"":D_RIC_RICH, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_ESECUZIONE==null?"":D_ESECUZIONE, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_VTG = null; } else {
      this.N_ID_VTG = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_OP = null; } else {
      this.T_COD_OP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UTENTE = null; } else {
      this.N_ID_UTENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_OPERATORE = null; } else {
      this.N_ID_OPERATORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_RIC_RICH = null; } else {
      this.D_RIC_RICH = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_ESECUZIONE = null; } else {
      this.D_ESECUZIONE = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_VTG = null; } else {
      this.N_ID_VTG = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_OP = null; } else {
      this.T_COD_OP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UTENTE = null; } else {
      this.N_ID_UTENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_OPERATORE = null; } else {
      this.N_ID_OPERATORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_RIC_RICH = null; } else {
      this.D_RIC_RICH = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_ESECUZIONE = null; } else {
      this.D_ESECUZIONE = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    switch_gas_prt_vtg_operazione o = (switch_gas_prt_vtg_operazione) super.clone();
    return o;
  }

  public void clone0(switch_gas_prt_vtg_operazione o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID", this.N_ID);
    __sqoop$field_map.put("N_ID_VTG", this.N_ID_VTG);
    __sqoop$field_map.put("T_COD_OP", this.T_COD_OP);
    __sqoop$field_map.put("N_ID_UTENTE", this.N_ID_UTENTE);
    __sqoop$field_map.put("N_ID_OPERATORE", this.N_ID_OPERATORE);
    __sqoop$field_map.put("D_RIC_RICH", this.D_RIC_RICH);
    __sqoop$field_map.put("D_ESECUZIONE", this.D_ESECUZIONE);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID", this.N_ID);
    __sqoop$field_map.put("N_ID_VTG", this.N_ID_VTG);
    __sqoop$field_map.put("T_COD_OP", this.T_COD_OP);
    __sqoop$field_map.put("N_ID_UTENTE", this.N_ID_UTENTE);
    __sqoop$field_map.put("N_ID_OPERATORE", this.N_ID_OPERATORE);
    __sqoop$field_map.put("D_RIC_RICH", this.D_RIC_RICH);
    __sqoop$field_map.put("D_ESECUZIONE", this.D_ESECUZIONE);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
