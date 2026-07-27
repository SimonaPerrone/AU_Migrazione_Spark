// ORM class for table 'RCUGAS.V_RCUGAS_DISTRIBUTORE'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 14:48:42 GMT 2019
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

public class RCUGAS_V_RCUGAS_DISTRIBUTORE extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_DISTRIBUTORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_DISTRIBUTORE = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_AZIENDA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_AZIENDA = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_CODICE_ESERCENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_ESERCENTE = (String)value;
      }
    });
    setters.put("N_ID_UTENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_UTENTE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_CODICE_AEEG", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_AEEG = (String)value;
      }
    });
    setters.put("T_PIVA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PIVA = (String)value;
      }
    });
    setters.put("T_RAG_SOC", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_RAG_SOC = (String)value;
      }
    });
    setters.put("D_DATA_INIZIO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_INIZIO = (java.sql.Timestamp)value;
      }
    });
    setters.put("D_DATA_FINE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_FINE = (java.sql.Timestamp)value;
      }
    });
  }
  public RCUGAS_V_RCUGAS_DISTRIBUTORE() {
    init0();
  }
  private java.math.BigDecimal N_ID_DISTRIBUTORE;
  public java.math.BigDecimal get_N_ID_DISTRIBUTORE() {
    return N_ID_DISTRIBUTORE;
  }
  public void set_N_ID_DISTRIBUTORE(java.math.BigDecimal N_ID_DISTRIBUTORE) {
    this.N_ID_DISTRIBUTORE = N_ID_DISTRIBUTORE;
  }
  public RCUGAS_V_RCUGAS_DISTRIBUTORE with_N_ID_DISTRIBUTORE(java.math.BigDecimal N_ID_DISTRIBUTORE) {
    this.N_ID_DISTRIBUTORE = N_ID_DISTRIBUTORE;
    return this;
  }
  private java.math.BigDecimal N_ID_AZIENDA;
  public java.math.BigDecimal get_N_ID_AZIENDA() {
    return N_ID_AZIENDA;
  }
  public void set_N_ID_AZIENDA(java.math.BigDecimal N_ID_AZIENDA) {
    this.N_ID_AZIENDA = N_ID_AZIENDA;
  }
  public RCUGAS_V_RCUGAS_DISTRIBUTORE with_N_ID_AZIENDA(java.math.BigDecimal N_ID_AZIENDA) {
    this.N_ID_AZIENDA = N_ID_AZIENDA;
    return this;
  }
  private String T_CODICE_ESERCENTE;
  public String get_T_CODICE_ESERCENTE() {
    return T_CODICE_ESERCENTE;
  }
  public void set_T_CODICE_ESERCENTE(String T_CODICE_ESERCENTE) {
    this.T_CODICE_ESERCENTE = T_CODICE_ESERCENTE;
  }
  public RCUGAS_V_RCUGAS_DISTRIBUTORE with_T_CODICE_ESERCENTE(String T_CODICE_ESERCENTE) {
    this.T_CODICE_ESERCENTE = T_CODICE_ESERCENTE;
    return this;
  }
  private java.math.BigDecimal N_ID_UTENTE;
  public java.math.BigDecimal get_N_ID_UTENTE() {
    return N_ID_UTENTE;
  }
  public void set_N_ID_UTENTE(java.math.BigDecimal N_ID_UTENTE) {
    this.N_ID_UTENTE = N_ID_UTENTE;
  }
  public RCUGAS_V_RCUGAS_DISTRIBUTORE with_N_ID_UTENTE(java.math.BigDecimal N_ID_UTENTE) {
    this.N_ID_UTENTE = N_ID_UTENTE;
    return this;
  }
  private String T_CODICE_AEEG;
  public String get_T_CODICE_AEEG() {
    return T_CODICE_AEEG;
  }
  public void set_T_CODICE_AEEG(String T_CODICE_AEEG) {
    this.T_CODICE_AEEG = T_CODICE_AEEG;
  }
  public RCUGAS_V_RCUGAS_DISTRIBUTORE with_T_CODICE_AEEG(String T_CODICE_AEEG) {
    this.T_CODICE_AEEG = T_CODICE_AEEG;
    return this;
  }
  private String T_PIVA;
  public String get_T_PIVA() {
    return T_PIVA;
  }
  public void set_T_PIVA(String T_PIVA) {
    this.T_PIVA = T_PIVA;
  }
  public RCUGAS_V_RCUGAS_DISTRIBUTORE with_T_PIVA(String T_PIVA) {
    this.T_PIVA = T_PIVA;
    return this;
  }
  private String T_RAG_SOC;
  public String get_T_RAG_SOC() {
    return T_RAG_SOC;
  }
  public void set_T_RAG_SOC(String T_RAG_SOC) {
    this.T_RAG_SOC = T_RAG_SOC;
  }
  public RCUGAS_V_RCUGAS_DISTRIBUTORE with_T_RAG_SOC(String T_RAG_SOC) {
    this.T_RAG_SOC = T_RAG_SOC;
    return this;
  }
  private java.sql.Timestamp D_DATA_INIZIO;
  public java.sql.Timestamp get_D_DATA_INIZIO() {
    return D_DATA_INIZIO;
  }
  public void set_D_DATA_INIZIO(java.sql.Timestamp D_DATA_INIZIO) {
    this.D_DATA_INIZIO = D_DATA_INIZIO;
  }
  public RCUGAS_V_RCUGAS_DISTRIBUTORE with_D_DATA_INIZIO(java.sql.Timestamp D_DATA_INIZIO) {
    this.D_DATA_INIZIO = D_DATA_INIZIO;
    return this;
  }
  private java.sql.Timestamp D_DATA_FINE;
  public java.sql.Timestamp get_D_DATA_FINE() {
    return D_DATA_FINE;
  }
  public void set_D_DATA_FINE(java.sql.Timestamp D_DATA_FINE) {
    this.D_DATA_FINE = D_DATA_FINE;
  }
  public RCUGAS_V_RCUGAS_DISTRIBUTORE with_D_DATA_FINE(java.sql.Timestamp D_DATA_FINE) {
    this.D_DATA_FINE = D_DATA_FINE;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RCUGAS_V_RCUGAS_DISTRIBUTORE)) {
      return false;
    }
    RCUGAS_V_RCUGAS_DISTRIBUTORE that = (RCUGAS_V_RCUGAS_DISTRIBUTORE) o;
    boolean equal = true;
    equal = equal && (this.N_ID_DISTRIBUTORE == null ? that.N_ID_DISTRIBUTORE == null : this.N_ID_DISTRIBUTORE.equals(that.N_ID_DISTRIBUTORE));
    equal = equal && (this.N_ID_AZIENDA == null ? that.N_ID_AZIENDA == null : this.N_ID_AZIENDA.equals(that.N_ID_AZIENDA));
    equal = equal && (this.T_CODICE_ESERCENTE == null ? that.T_CODICE_ESERCENTE == null : this.T_CODICE_ESERCENTE.equals(that.T_CODICE_ESERCENTE));
    equal = equal && (this.N_ID_UTENTE == null ? that.N_ID_UTENTE == null : this.N_ID_UTENTE.equals(that.N_ID_UTENTE));
    equal = equal && (this.T_CODICE_AEEG == null ? that.T_CODICE_AEEG == null : this.T_CODICE_AEEG.equals(that.T_CODICE_AEEG));
    equal = equal && (this.T_PIVA == null ? that.T_PIVA == null : this.T_PIVA.equals(that.T_PIVA));
    equal = equal && (this.T_RAG_SOC == null ? that.T_RAG_SOC == null : this.T_RAG_SOC.equals(that.T_RAG_SOC));
    equal = equal && (this.D_DATA_INIZIO == null ? that.D_DATA_INIZIO == null : this.D_DATA_INIZIO.equals(that.D_DATA_INIZIO));
    equal = equal && (this.D_DATA_FINE == null ? that.D_DATA_FINE == null : this.D_DATA_FINE.equals(that.D_DATA_FINE));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RCUGAS_V_RCUGAS_DISTRIBUTORE)) {
      return false;
    }
    RCUGAS_V_RCUGAS_DISTRIBUTORE that = (RCUGAS_V_RCUGAS_DISTRIBUTORE) o;
    boolean equal = true;
    equal = equal && (this.N_ID_DISTRIBUTORE == null ? that.N_ID_DISTRIBUTORE == null : this.N_ID_DISTRIBUTORE.equals(that.N_ID_DISTRIBUTORE));
    equal = equal && (this.N_ID_AZIENDA == null ? that.N_ID_AZIENDA == null : this.N_ID_AZIENDA.equals(that.N_ID_AZIENDA));
    equal = equal && (this.T_CODICE_ESERCENTE == null ? that.T_CODICE_ESERCENTE == null : this.T_CODICE_ESERCENTE.equals(that.T_CODICE_ESERCENTE));
    equal = equal && (this.N_ID_UTENTE == null ? that.N_ID_UTENTE == null : this.N_ID_UTENTE.equals(that.N_ID_UTENTE));
    equal = equal && (this.T_CODICE_AEEG == null ? that.T_CODICE_AEEG == null : this.T_CODICE_AEEG.equals(that.T_CODICE_AEEG));
    equal = equal && (this.T_PIVA == null ? that.T_PIVA == null : this.T_PIVA.equals(that.T_PIVA));
    equal = equal && (this.T_RAG_SOC == null ? that.T_RAG_SOC == null : this.T_RAG_SOC.equals(that.T_RAG_SOC));
    equal = equal && (this.D_DATA_INIZIO == null ? that.D_DATA_INIZIO == null : this.D_DATA_INIZIO.equals(that.D_DATA_INIZIO));
    equal = equal && (this.D_DATA_FINE == null ? that.D_DATA_FINE == null : this.D_DATA_FINE.equals(that.D_DATA_FINE));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_DISTRIBUTORE = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_AZIENDA = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_CODICE_ESERCENTE = JdbcWritableBridge.readString(3, __dbResults);
    this.N_ID_UTENTE = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.T_CODICE_AEEG = JdbcWritableBridge.readString(5, __dbResults);
    this.T_PIVA = JdbcWritableBridge.readString(6, __dbResults);
    this.T_RAG_SOC = JdbcWritableBridge.readString(7, __dbResults);
    this.D_DATA_INIZIO = JdbcWritableBridge.readTimestamp(8, __dbResults);
    this.D_DATA_FINE = JdbcWritableBridge.readTimestamp(9, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_DISTRIBUTORE = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_AZIENDA = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_CODICE_ESERCENTE = JdbcWritableBridge.readString(3, __dbResults);
    this.N_ID_UTENTE = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.T_CODICE_AEEG = JdbcWritableBridge.readString(5, __dbResults);
    this.T_PIVA = JdbcWritableBridge.readString(6, __dbResults);
    this.T_RAG_SOC = JdbcWritableBridge.readString(7, __dbResults);
    this.D_DATA_INIZIO = JdbcWritableBridge.readTimestamp(8, __dbResults);
    this.D_DATA_FINE = JdbcWritableBridge.readTimestamp(9, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTRIBUTORE, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_AZIENDA, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_ESERCENTE, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UTENTE, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_AEEG, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_RAG_SOC, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(D_DATA_INIZIO, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(D_DATA_FINE, 9 + __off, 93, __dbStmt);
    return 9;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTRIBUTORE, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_AZIENDA, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_ESERCENTE, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UTENTE, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_AEEG, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_RAG_SOC, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(D_DATA_INIZIO, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(D_DATA_FINE, 9 + __off, 93, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_DISTRIBUTORE = null;
    } else {
    this.N_ID_DISTRIBUTORE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_AZIENDA = null;
    } else {
    this.N_ID_AZIENDA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_ESERCENTE = null;
    } else {
    this.T_CODICE_ESERCENTE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_UTENTE = null;
    } else {
    this.N_ID_UTENTE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_AEEG = null;
    } else {
    this.T_CODICE_AEEG = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PIVA = null;
    } else {
    this.T_PIVA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_RAG_SOC = null;
    } else {
    this.T_RAG_SOC = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_INIZIO = null;
    } else {
    this.D_DATA_INIZIO = new Timestamp(__dataIn.readLong());
    this.D_DATA_INIZIO.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_FINE = null;
    } else {
    this.D_DATA_FINE = new Timestamp(__dataIn.readLong());
    this.D_DATA_FINE.setNanos(__dataIn.readInt());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_DISTRIBUTORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISTRIBUTORE, __dataOut);
    }
    if (null == this.N_ID_AZIENDA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_AZIENDA, __dataOut);
    }
    if (null == this.T_CODICE_ESERCENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_ESERCENTE);
    }
    if (null == this.N_ID_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UTENTE, __dataOut);
    }
    if (null == this.T_CODICE_AEEG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_AEEG);
    }
    if (null == this.T_PIVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA);
    }
    if (null == this.T_RAG_SOC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RAG_SOC);
    }
    if (null == this.D_DATA_INIZIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.D_DATA_INIZIO.getTime());
    __dataOut.writeInt(this.D_DATA_INIZIO.getNanos());
    }
    if (null == this.D_DATA_FINE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.D_DATA_FINE.getTime());
    __dataOut.writeInt(this.D_DATA_FINE.getNanos());
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_DISTRIBUTORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISTRIBUTORE, __dataOut);
    }
    if (null == this.N_ID_AZIENDA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_AZIENDA, __dataOut);
    }
    if (null == this.T_CODICE_ESERCENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_ESERCENTE);
    }
    if (null == this.N_ID_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UTENTE, __dataOut);
    }
    if (null == this.T_CODICE_AEEG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_AEEG);
    }
    if (null == this.T_PIVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA);
    }
    if (null == this.T_RAG_SOC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RAG_SOC);
    }
    if (null == this.D_DATA_INIZIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.D_DATA_INIZIO.getTime());
    __dataOut.writeInt(this.D_DATA_INIZIO.getNanos());
    }
    if (null == this.D_DATA_FINE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.D_DATA_FINE.getTime());
    __dataOut.writeInt(this.D_DATA_FINE.getNanos());
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISTRIBUTORE==null?"":N_ID_DISTRIBUTORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_AZIENDA==null?"":N_ID_AZIENDA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_ESERCENTE==null?"":T_CODICE_ESERCENTE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UTENTE==null?"":N_ID_UTENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_AEEG==null?"":T_CODICE_AEEG, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA==null?"":T_PIVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RAG_SOC==null?"":T_RAG_SOC, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(D_DATA_INIZIO==null?"":"" + D_DATA_INIZIO, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(D_DATA_FINE==null?"":"" + D_DATA_FINE, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISTRIBUTORE==null?"":N_ID_DISTRIBUTORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_AZIENDA==null?"":N_ID_AZIENDA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_ESERCENTE==null?"":T_CODICE_ESERCENTE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UTENTE==null?"":N_ID_UTENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_AEEG==null?"":T_CODICE_AEEG, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA==null?"":T_PIVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RAG_SOC==null?"":T_RAG_SOC, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(D_DATA_INIZIO==null?"":"" + D_DATA_INIZIO, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(D_DATA_FINE==null?"":"" + D_DATA_FINE, delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISTRIBUTORE = null; } else {
      this.N_ID_DISTRIBUTORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_AZIENDA = null; } else {
      this.N_ID_AZIENDA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_ESERCENTE = null; } else {
      this.T_CODICE_ESERCENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UTENTE = null; } else {
      this.N_ID_UTENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_AEEG = null; } else {
      this.T_CODICE_AEEG = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA = null; } else {
      this.T_PIVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RAG_SOC = null; } else {
      this.T_RAG_SOC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.D_DATA_INIZIO = null; } else {
      this.D_DATA_INIZIO = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.D_DATA_FINE = null; } else {
      this.D_DATA_FINE = java.sql.Timestamp.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISTRIBUTORE = null; } else {
      this.N_ID_DISTRIBUTORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_AZIENDA = null; } else {
      this.N_ID_AZIENDA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_ESERCENTE = null; } else {
      this.T_CODICE_ESERCENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UTENTE = null; } else {
      this.N_ID_UTENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_AEEG = null; } else {
      this.T_CODICE_AEEG = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA = null; } else {
      this.T_PIVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RAG_SOC = null; } else {
      this.T_RAG_SOC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.D_DATA_INIZIO = null; } else {
      this.D_DATA_INIZIO = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.D_DATA_FINE = null; } else {
      this.D_DATA_FINE = java.sql.Timestamp.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    RCUGAS_V_RCUGAS_DISTRIBUTORE o = (RCUGAS_V_RCUGAS_DISTRIBUTORE) super.clone();
    o.D_DATA_INIZIO = (o.D_DATA_INIZIO != null) ? (java.sql.Timestamp) o.D_DATA_INIZIO.clone() : null;
    o.D_DATA_FINE = (o.D_DATA_FINE != null) ? (java.sql.Timestamp) o.D_DATA_FINE.clone() : null;
    return o;
  }

  public void clone0(RCUGAS_V_RCUGAS_DISTRIBUTORE o) throws CloneNotSupportedException {
    o.D_DATA_INIZIO = (o.D_DATA_INIZIO != null) ? (java.sql.Timestamp) o.D_DATA_INIZIO.clone() : null;
    o.D_DATA_FINE = (o.D_DATA_FINE != null) ? (java.sql.Timestamp) o.D_DATA_FINE.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_DISTRIBUTORE", this.N_ID_DISTRIBUTORE);
    __sqoop$field_map.put("N_ID_AZIENDA", this.N_ID_AZIENDA);
    __sqoop$field_map.put("T_CODICE_ESERCENTE", this.T_CODICE_ESERCENTE);
    __sqoop$field_map.put("N_ID_UTENTE", this.N_ID_UTENTE);
    __sqoop$field_map.put("T_CODICE_AEEG", this.T_CODICE_AEEG);
    __sqoop$field_map.put("T_PIVA", this.T_PIVA);
    __sqoop$field_map.put("T_RAG_SOC", this.T_RAG_SOC);
    __sqoop$field_map.put("D_DATA_INIZIO", this.D_DATA_INIZIO);
    __sqoop$field_map.put("D_DATA_FINE", this.D_DATA_FINE);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_DISTRIBUTORE", this.N_ID_DISTRIBUTORE);
    __sqoop$field_map.put("N_ID_AZIENDA", this.N_ID_AZIENDA);
    __sqoop$field_map.put("T_CODICE_ESERCENTE", this.T_CODICE_ESERCENTE);
    __sqoop$field_map.put("N_ID_UTENTE", this.N_ID_UTENTE);
    __sqoop$field_map.put("T_CODICE_AEEG", this.T_CODICE_AEEG);
    __sqoop$field_map.put("T_PIVA", this.T_PIVA);
    __sqoop$field_map.put("T_RAG_SOC", this.T_RAG_SOC);
    __sqoop$field_map.put("D_DATA_INIZIO", this.D_DATA_INIZIO);
    __sqoop$field_map.put("D_DATA_FINE", this.D_DATA_FINE);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
