// ORM class for table 'rcugas.rcugas_distributore'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 11:46:09 CEST 2019
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

public class rcugas_rcugas_distributore extends SqoopRecord  implements DBWritable, Writable {
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
    setters.put("COD_TIPO_DISTRIBUTORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        COD_TIPO_DISTRIBUTORE = (String)value;
      }
    });
    setters.put("D_DATA_INIZIO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_INIZIO = (String)value;
      }
    });
    setters.put("D_DATA_FINE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_FINE = (String)value;
      }
    });
    setters.put("T_NOTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_NOTE = (String)value;
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
    setters.put("D_DATA_RIF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_RIF = (String)value;
      }
    });
  }
  public rcugas_rcugas_distributore() {
    init0();
  }
  private java.math.BigDecimal N_ID_DISTRIBUTORE;
  public java.math.BigDecimal get_N_ID_DISTRIBUTORE() {
    return N_ID_DISTRIBUTORE;
  }
  public void set_N_ID_DISTRIBUTORE(java.math.BigDecimal N_ID_DISTRIBUTORE) {
    this.N_ID_DISTRIBUTORE = N_ID_DISTRIBUTORE;
  }
  public rcugas_rcugas_distributore with_N_ID_DISTRIBUTORE(java.math.BigDecimal N_ID_DISTRIBUTORE) {
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
  public rcugas_rcugas_distributore with_N_ID_AZIENDA(java.math.BigDecimal N_ID_AZIENDA) {
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
  public rcugas_rcugas_distributore with_T_CODICE_ESERCENTE(String T_CODICE_ESERCENTE) {
    this.T_CODICE_ESERCENTE = T_CODICE_ESERCENTE;
    return this;
  }
  private String COD_TIPO_DISTRIBUTORE;
  public String get_COD_TIPO_DISTRIBUTORE() {
    return COD_TIPO_DISTRIBUTORE;
  }
  public void set_COD_TIPO_DISTRIBUTORE(String COD_TIPO_DISTRIBUTORE) {
    this.COD_TIPO_DISTRIBUTORE = COD_TIPO_DISTRIBUTORE;
  }
  public rcugas_rcugas_distributore with_COD_TIPO_DISTRIBUTORE(String COD_TIPO_DISTRIBUTORE) {
    this.COD_TIPO_DISTRIBUTORE = COD_TIPO_DISTRIBUTORE;
    return this;
  }
  private String D_DATA_INIZIO;
  public String get_D_DATA_INIZIO() {
    return D_DATA_INIZIO;
  }
  public void set_D_DATA_INIZIO(String D_DATA_INIZIO) {
    this.D_DATA_INIZIO = D_DATA_INIZIO;
  }
  public rcugas_rcugas_distributore with_D_DATA_INIZIO(String D_DATA_INIZIO) {
    this.D_DATA_INIZIO = D_DATA_INIZIO;
    return this;
  }
  private String D_DATA_FINE;
  public String get_D_DATA_FINE() {
    return D_DATA_FINE;
  }
  public void set_D_DATA_FINE(String D_DATA_FINE) {
    this.D_DATA_FINE = D_DATA_FINE;
  }
  public rcugas_rcugas_distributore with_D_DATA_FINE(String D_DATA_FINE) {
    this.D_DATA_FINE = D_DATA_FINE;
    return this;
  }
  private String T_NOTE;
  public String get_T_NOTE() {
    return T_NOTE;
  }
  public void set_T_NOTE(String T_NOTE) {
    this.T_NOTE = T_NOTE;
  }
  public rcugas_rcugas_distributore with_T_NOTE(String T_NOTE) {
    this.T_NOTE = T_NOTE;
    return this;
  }
  private String D_AGGIORNAMENTO;
  public String get_D_AGGIORNAMENTO() {
    return D_AGGIORNAMENTO;
  }
  public void set_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
    this.D_AGGIORNAMENTO = D_AGGIORNAMENTO;
  }
  public rcugas_rcugas_distributore with_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
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
  public rcugas_rcugas_distributore with_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
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
  public rcugas_rcugas_distributore with_N_ID_S_PREC(java.math.BigDecimal N_ID_S_PREC) {
    this.N_ID_S_PREC = N_ID_S_PREC;
    return this;
  }
  private String D_DATA_RIF;
  public String get_D_DATA_RIF() {
    return D_DATA_RIF;
  }
  public void set_D_DATA_RIF(String D_DATA_RIF) {
    this.D_DATA_RIF = D_DATA_RIF;
  }
  public rcugas_rcugas_distributore with_D_DATA_RIF(String D_DATA_RIF) {
    this.D_DATA_RIF = D_DATA_RIF;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcugas_rcugas_distributore)) {
      return false;
    }
    rcugas_rcugas_distributore that = (rcugas_rcugas_distributore) o;
    boolean equal = true;
    equal = equal && (this.N_ID_DISTRIBUTORE == null ? that.N_ID_DISTRIBUTORE == null : this.N_ID_DISTRIBUTORE.equals(that.N_ID_DISTRIBUTORE));
    equal = equal && (this.N_ID_AZIENDA == null ? that.N_ID_AZIENDA == null : this.N_ID_AZIENDA.equals(that.N_ID_AZIENDA));
    equal = equal && (this.T_CODICE_ESERCENTE == null ? that.T_CODICE_ESERCENTE == null : this.T_CODICE_ESERCENTE.equals(that.T_CODICE_ESERCENTE));
    equal = equal && (this.COD_TIPO_DISTRIBUTORE == null ? that.COD_TIPO_DISTRIBUTORE == null : this.COD_TIPO_DISTRIBUTORE.equals(that.COD_TIPO_DISTRIBUTORE));
    equal = equal && (this.D_DATA_INIZIO == null ? that.D_DATA_INIZIO == null : this.D_DATA_INIZIO.equals(that.D_DATA_INIZIO));
    equal = equal && (this.D_DATA_FINE == null ? that.D_DATA_FINE == null : this.D_DATA_FINE.equals(that.D_DATA_FINE));
    equal = equal && (this.T_NOTE == null ? that.T_NOTE == null : this.T_NOTE.equals(that.T_NOTE));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.D_DATA_RIF == null ? that.D_DATA_RIF == null : this.D_DATA_RIF.equals(that.D_DATA_RIF));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcugas_rcugas_distributore)) {
      return false;
    }
    rcugas_rcugas_distributore that = (rcugas_rcugas_distributore) o;
    boolean equal = true;
    equal = equal && (this.N_ID_DISTRIBUTORE == null ? that.N_ID_DISTRIBUTORE == null : this.N_ID_DISTRIBUTORE.equals(that.N_ID_DISTRIBUTORE));
    equal = equal && (this.N_ID_AZIENDA == null ? that.N_ID_AZIENDA == null : this.N_ID_AZIENDA.equals(that.N_ID_AZIENDA));
    equal = equal && (this.T_CODICE_ESERCENTE == null ? that.T_CODICE_ESERCENTE == null : this.T_CODICE_ESERCENTE.equals(that.T_CODICE_ESERCENTE));
    equal = equal && (this.COD_TIPO_DISTRIBUTORE == null ? that.COD_TIPO_DISTRIBUTORE == null : this.COD_TIPO_DISTRIBUTORE.equals(that.COD_TIPO_DISTRIBUTORE));
    equal = equal && (this.D_DATA_INIZIO == null ? that.D_DATA_INIZIO == null : this.D_DATA_INIZIO.equals(that.D_DATA_INIZIO));
    equal = equal && (this.D_DATA_FINE == null ? that.D_DATA_FINE == null : this.D_DATA_FINE.equals(that.D_DATA_FINE));
    equal = equal && (this.T_NOTE == null ? that.T_NOTE == null : this.T_NOTE.equals(that.T_NOTE));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.D_DATA_RIF == null ? that.D_DATA_RIF == null : this.D_DATA_RIF.equals(that.D_DATA_RIF));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_DISTRIBUTORE = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_AZIENDA = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_CODICE_ESERCENTE = JdbcWritableBridge.readString(3, __dbResults);
    this.COD_TIPO_DISTRIBUTORE = JdbcWritableBridge.readString(4, __dbResults);
    this.D_DATA_INIZIO = JdbcWritableBridge.readString(5, __dbResults);
    this.D_DATA_FINE = JdbcWritableBridge.readString(6, __dbResults);
    this.T_NOTE = JdbcWritableBridge.readString(7, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(8, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.D_DATA_RIF = JdbcWritableBridge.readString(11, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_DISTRIBUTORE = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_AZIENDA = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_CODICE_ESERCENTE = JdbcWritableBridge.readString(3, __dbResults);
    this.COD_TIPO_DISTRIBUTORE = JdbcWritableBridge.readString(4, __dbResults);
    this.D_DATA_INIZIO = JdbcWritableBridge.readString(5, __dbResults);
    this.D_DATA_FINE = JdbcWritableBridge.readString(6, __dbResults);
    this.T_NOTE = JdbcWritableBridge.readString(7, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(8, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.D_DATA_RIF = JdbcWritableBridge.readString(11, __dbResults);
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
    JdbcWritableBridge.writeString(COD_TIPO_DISTRIBUTORE, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INIZIO, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_FINE, 6 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTE, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 10 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF, 11 + __off, 93, __dbStmt);
    return 11;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTRIBUTORE, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_AZIENDA, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_ESERCENTE, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COD_TIPO_DISTRIBUTORE, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INIZIO, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_FINE, 6 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTE, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 10 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF, 11 + __off, 93, __dbStmt);
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
        this.COD_TIPO_DISTRIBUTORE = null;
    } else {
    this.COD_TIPO_DISTRIBUTORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_INIZIO = null;
    } else {
    this.D_DATA_INIZIO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_FINE = null;
    } else {
    this.D_DATA_FINE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_NOTE = null;
    } else {
    this.T_NOTE = Text.readString(__dataIn);
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
        this.D_DATA_RIF = null;
    } else {
    this.D_DATA_RIF = Text.readString(__dataIn);
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
    if (null == this.COD_TIPO_DISTRIBUTORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_TIPO_DISTRIBUTORE);
    }
    if (null == this.D_DATA_INIZIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INIZIO);
    }
    if (null == this.D_DATA_FINE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_FINE);
    }
    if (null == this.T_NOTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOTE);
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
    if (null == this.D_DATA_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF);
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
    if (null == this.COD_TIPO_DISTRIBUTORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_TIPO_DISTRIBUTORE);
    }
    if (null == this.D_DATA_INIZIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INIZIO);
    }
    if (null == this.D_DATA_FINE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_FINE);
    }
    if (null == this.T_NOTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOTE);
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
    if (null == this.D_DATA_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF);
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
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_TIPO_DISTRIBUTORE==null?"":COD_TIPO_DISTRIBUTORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INIZIO==null?"":D_DATA_INIZIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_FINE==null?"":D_DATA_FINE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOTE==null?"":T_NOTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO==null?"":D_AGGIORNAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TRACCIA==null?"":N_ID_TRACCIA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_PREC==null?"":N_ID_S_PREC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF==null?"":D_DATA_RIF, " ", delimiters));
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
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_TIPO_DISTRIBUTORE==null?"":COD_TIPO_DISTRIBUTORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INIZIO==null?"":D_DATA_INIZIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_FINE==null?"":D_DATA_FINE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOTE==null?"":T_NOTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO==null?"":D_AGGIORNAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TRACCIA==null?"":N_ID_TRACCIA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_PREC==null?"":N_ID_S_PREC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF==null?"":D_DATA_RIF, " ", delimiters));
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
    if (__cur_str.equals("null")) { this.COD_TIPO_DISTRIBUTORE = null; } else {
      this.COD_TIPO_DISTRIBUTORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INIZIO = null; } else {
      this.D_DATA_INIZIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_FINE = null; } else {
      this.D_DATA_FINE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOTE = null; } else {
      this.T_NOTE = __cur_str;
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
    if (__cur_str.equals("null")) { this.D_DATA_RIF = null; } else {
      this.D_DATA_RIF = __cur_str;
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
    if (__cur_str.equals("null")) { this.COD_TIPO_DISTRIBUTORE = null; } else {
      this.COD_TIPO_DISTRIBUTORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INIZIO = null; } else {
      this.D_DATA_INIZIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_FINE = null; } else {
      this.D_DATA_FINE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOTE = null; } else {
      this.T_NOTE = __cur_str;
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
    if (__cur_str.equals("null")) { this.D_DATA_RIF = null; } else {
      this.D_DATA_RIF = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    rcugas_rcugas_distributore o = (rcugas_rcugas_distributore) super.clone();
    return o;
  }

  public void clone0(rcugas_rcugas_distributore o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_DISTRIBUTORE", this.N_ID_DISTRIBUTORE);
    __sqoop$field_map.put("N_ID_AZIENDA", this.N_ID_AZIENDA);
    __sqoop$field_map.put("T_CODICE_ESERCENTE", this.T_CODICE_ESERCENTE);
    __sqoop$field_map.put("COD_TIPO_DISTRIBUTORE", this.COD_TIPO_DISTRIBUTORE);
    __sqoop$field_map.put("D_DATA_INIZIO", this.D_DATA_INIZIO);
    __sqoop$field_map.put("D_DATA_FINE", this.D_DATA_FINE);
    __sqoop$field_map.put("T_NOTE", this.T_NOTE);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("D_DATA_RIF", this.D_DATA_RIF);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_DISTRIBUTORE", this.N_ID_DISTRIBUTORE);
    __sqoop$field_map.put("N_ID_AZIENDA", this.N_ID_AZIENDA);
    __sqoop$field_map.put("T_CODICE_ESERCENTE", this.T_CODICE_ESERCENTE);
    __sqoop$field_map.put("COD_TIPO_DISTRIBUTORE", this.COD_TIPO_DISTRIBUTORE);
    __sqoop$field_map.put("D_DATA_INIZIO", this.D_DATA_INIZIO);
    __sqoop$field_map.put("D_DATA_FINE", this.D_DATA_FINE);
    __sqoop$field_map.put("T_NOTE", this.T_NOTE);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("D_DATA_RIF", this.D_DATA_RIF);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
