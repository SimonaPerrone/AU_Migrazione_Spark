// ORM class for table 'rcugas.rcugas_pdr'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 12:17:19 CEST 2019
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

public class rcugas_rcugas_pdr extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_PDR = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_CODICE_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_PDR = (String)value;
      }
    });
    setters.put("T_COD_TIPO_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COD_TIPO_PDR = (String)value;
      }
    });
    setters.put("T_CODICE_ISTAT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_ISTAT = (String)value;
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
    setters.put("N_ID_INDIRIZZO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_INDIRIZZO = (java.math.BigDecimal)value;
      }
    });
    setters.put("D_DATA_RIF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_RIF = (String)value;
      }
    });
    setters.put("T_DISALIMENTABILITA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_DISALIMENTABILITA = (String)value;
      }
    });
    setters.put("T_ACCESSO_UI", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ACCESSO_UI = (String)value;
      }
    });
  }
  public rcugas_rcugas_pdr() {
    init0();
  }
  private java.math.BigDecimal N_ID_PDR;
  public java.math.BigDecimal get_N_ID_PDR() {
    return N_ID_PDR;
  }
  public void set_N_ID_PDR(java.math.BigDecimal N_ID_PDR) {
    this.N_ID_PDR = N_ID_PDR;
  }
  public rcugas_rcugas_pdr with_N_ID_PDR(java.math.BigDecimal N_ID_PDR) {
    this.N_ID_PDR = N_ID_PDR;
    return this;
  }
  private String T_CODICE_PDR;
  public String get_T_CODICE_PDR() {
    return T_CODICE_PDR;
  }
  public void set_T_CODICE_PDR(String T_CODICE_PDR) {
    this.T_CODICE_PDR = T_CODICE_PDR;
  }
  public rcugas_rcugas_pdr with_T_CODICE_PDR(String T_CODICE_PDR) {
    this.T_CODICE_PDR = T_CODICE_PDR;
    return this;
  }
  private String T_COD_TIPO_PDR;
  public String get_T_COD_TIPO_PDR() {
    return T_COD_TIPO_PDR;
  }
  public void set_T_COD_TIPO_PDR(String T_COD_TIPO_PDR) {
    this.T_COD_TIPO_PDR = T_COD_TIPO_PDR;
  }
  public rcugas_rcugas_pdr with_T_COD_TIPO_PDR(String T_COD_TIPO_PDR) {
    this.T_COD_TIPO_PDR = T_COD_TIPO_PDR;
    return this;
  }
  private String T_CODICE_ISTAT;
  public String get_T_CODICE_ISTAT() {
    return T_CODICE_ISTAT;
  }
  public void set_T_CODICE_ISTAT(String T_CODICE_ISTAT) {
    this.T_CODICE_ISTAT = T_CODICE_ISTAT;
  }
  public rcugas_rcugas_pdr with_T_CODICE_ISTAT(String T_CODICE_ISTAT) {
    this.T_CODICE_ISTAT = T_CODICE_ISTAT;
    return this;
  }
  private String T_NOTE;
  public String get_T_NOTE() {
    return T_NOTE;
  }
  public void set_T_NOTE(String T_NOTE) {
    this.T_NOTE = T_NOTE;
  }
  public rcugas_rcugas_pdr with_T_NOTE(String T_NOTE) {
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
  public rcugas_rcugas_pdr with_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
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
  public rcugas_rcugas_pdr with_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
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
  public rcugas_rcugas_pdr with_N_ID_S_PREC(java.math.BigDecimal N_ID_S_PREC) {
    this.N_ID_S_PREC = N_ID_S_PREC;
    return this;
  }
  private java.math.BigDecimal N_ID_INDIRIZZO;
  public java.math.BigDecimal get_N_ID_INDIRIZZO() {
    return N_ID_INDIRIZZO;
  }
  public void set_N_ID_INDIRIZZO(java.math.BigDecimal N_ID_INDIRIZZO) {
    this.N_ID_INDIRIZZO = N_ID_INDIRIZZO;
  }
  public rcugas_rcugas_pdr with_N_ID_INDIRIZZO(java.math.BigDecimal N_ID_INDIRIZZO) {
    this.N_ID_INDIRIZZO = N_ID_INDIRIZZO;
    return this;
  }
  private String D_DATA_RIF;
  public String get_D_DATA_RIF() {
    return D_DATA_RIF;
  }
  public void set_D_DATA_RIF(String D_DATA_RIF) {
    this.D_DATA_RIF = D_DATA_RIF;
  }
  public rcugas_rcugas_pdr with_D_DATA_RIF(String D_DATA_RIF) {
    this.D_DATA_RIF = D_DATA_RIF;
    return this;
  }
  private String T_DISALIMENTABILITA;
  public String get_T_DISALIMENTABILITA() {
    return T_DISALIMENTABILITA;
  }
  public void set_T_DISALIMENTABILITA(String T_DISALIMENTABILITA) {
    this.T_DISALIMENTABILITA = T_DISALIMENTABILITA;
  }
  public rcugas_rcugas_pdr with_T_DISALIMENTABILITA(String T_DISALIMENTABILITA) {
    this.T_DISALIMENTABILITA = T_DISALIMENTABILITA;
    return this;
  }
  private String T_ACCESSO_UI;
  public String get_T_ACCESSO_UI() {
    return T_ACCESSO_UI;
  }
  public void set_T_ACCESSO_UI(String T_ACCESSO_UI) {
    this.T_ACCESSO_UI = T_ACCESSO_UI;
  }
  public rcugas_rcugas_pdr with_T_ACCESSO_UI(String T_ACCESSO_UI) {
    this.T_ACCESSO_UI = T_ACCESSO_UI;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcugas_rcugas_pdr)) {
      return false;
    }
    rcugas_rcugas_pdr that = (rcugas_rcugas_pdr) o;
    boolean equal = true;
    equal = equal && (this.N_ID_PDR == null ? that.N_ID_PDR == null : this.N_ID_PDR.equals(that.N_ID_PDR));
    equal = equal && (this.T_CODICE_PDR == null ? that.T_CODICE_PDR == null : this.T_CODICE_PDR.equals(that.T_CODICE_PDR));
    equal = equal && (this.T_COD_TIPO_PDR == null ? that.T_COD_TIPO_PDR == null : this.T_COD_TIPO_PDR.equals(that.T_COD_TIPO_PDR));
    equal = equal && (this.T_CODICE_ISTAT == null ? that.T_CODICE_ISTAT == null : this.T_CODICE_ISTAT.equals(that.T_CODICE_ISTAT));
    equal = equal && (this.T_NOTE == null ? that.T_NOTE == null : this.T_NOTE.equals(that.T_NOTE));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.N_ID_INDIRIZZO == null ? that.N_ID_INDIRIZZO == null : this.N_ID_INDIRIZZO.equals(that.N_ID_INDIRIZZO));
    equal = equal && (this.D_DATA_RIF == null ? that.D_DATA_RIF == null : this.D_DATA_RIF.equals(that.D_DATA_RIF));
    equal = equal && (this.T_DISALIMENTABILITA == null ? that.T_DISALIMENTABILITA == null : this.T_DISALIMENTABILITA.equals(that.T_DISALIMENTABILITA));
    equal = equal && (this.T_ACCESSO_UI == null ? that.T_ACCESSO_UI == null : this.T_ACCESSO_UI.equals(that.T_ACCESSO_UI));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcugas_rcugas_pdr)) {
      return false;
    }
    rcugas_rcugas_pdr that = (rcugas_rcugas_pdr) o;
    boolean equal = true;
    equal = equal && (this.N_ID_PDR == null ? that.N_ID_PDR == null : this.N_ID_PDR.equals(that.N_ID_PDR));
    equal = equal && (this.T_CODICE_PDR == null ? that.T_CODICE_PDR == null : this.T_CODICE_PDR.equals(that.T_CODICE_PDR));
    equal = equal && (this.T_COD_TIPO_PDR == null ? that.T_COD_TIPO_PDR == null : this.T_COD_TIPO_PDR.equals(that.T_COD_TIPO_PDR));
    equal = equal && (this.T_CODICE_ISTAT == null ? that.T_CODICE_ISTAT == null : this.T_CODICE_ISTAT.equals(that.T_CODICE_ISTAT));
    equal = equal && (this.T_NOTE == null ? that.T_NOTE == null : this.T_NOTE.equals(that.T_NOTE));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.N_ID_INDIRIZZO == null ? that.N_ID_INDIRIZZO == null : this.N_ID_INDIRIZZO.equals(that.N_ID_INDIRIZZO));
    equal = equal && (this.D_DATA_RIF == null ? that.D_DATA_RIF == null : this.D_DATA_RIF.equals(that.D_DATA_RIF));
    equal = equal && (this.T_DISALIMENTABILITA == null ? that.T_DISALIMENTABILITA == null : this.T_DISALIMENTABILITA.equals(that.T_DISALIMENTABILITA));
    equal = equal && (this.T_ACCESSO_UI == null ? that.T_ACCESSO_UI == null : this.T_ACCESSO_UI.equals(that.T_ACCESSO_UI));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_PDR = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.T_CODICE_PDR = JdbcWritableBridge.readString(2, __dbResults);
    this.T_COD_TIPO_PDR = JdbcWritableBridge.readString(3, __dbResults);
    this.T_CODICE_ISTAT = JdbcWritableBridge.readString(4, __dbResults);
    this.T_NOTE = JdbcWritableBridge.readString(5, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(6, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(7, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.N_ID_INDIRIZZO = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.D_DATA_RIF = JdbcWritableBridge.readString(10, __dbResults);
    this.T_DISALIMENTABILITA = JdbcWritableBridge.readString(11, __dbResults);
    this.T_ACCESSO_UI = JdbcWritableBridge.readString(12, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_PDR = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.T_CODICE_PDR = JdbcWritableBridge.readString(2, __dbResults);
    this.T_COD_TIPO_PDR = JdbcWritableBridge.readString(3, __dbResults);
    this.T_CODICE_ISTAT = JdbcWritableBridge.readString(4, __dbResults);
    this.T_NOTE = JdbcWritableBridge.readString(5, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(6, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(7, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.N_ID_INDIRIZZO = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.D_DATA_RIF = JdbcWritableBridge.readString(10, __dbResults);
    this.T_DISALIMENTABILITA = JdbcWritableBridge.readString(11, __dbResults);
    this.T_ACCESSO_UI = JdbcWritableBridge.readString(12, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_PDR, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_PDR, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_TIPO_PDR, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_ISTAT, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTE, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 6 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 7 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_INDIRIZZO, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF, 10 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_DISALIMENTABILITA, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ACCESSO_UI, 12 + __off, 12, __dbStmt);
    return 12;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_PDR, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_PDR, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_TIPO_PDR, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_ISTAT, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTE, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 6 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 7 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_INDIRIZZO, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF, 10 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_DISALIMENTABILITA, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ACCESSO_UI, 12 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_PDR = null;
    } else {
    this.N_ID_PDR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_PDR = null;
    } else {
    this.T_CODICE_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COD_TIPO_PDR = null;
    } else {
    this.T_COD_TIPO_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_ISTAT = null;
    } else {
    this.T_CODICE_ISTAT = Text.readString(__dataIn);
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
        this.N_ID_INDIRIZZO = null;
    } else {
    this.N_ID_INDIRIZZO = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_RIF = null;
    } else {
    this.D_DATA_RIF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_DISALIMENTABILITA = null;
    } else {
    this.T_DISALIMENTABILITA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ACCESSO_UI = null;
    } else {
    this.T_ACCESSO_UI = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PDR, __dataOut);
    }
    if (null == this.T_CODICE_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_PDR);
    }
    if (null == this.T_COD_TIPO_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_TIPO_PDR);
    }
    if (null == this.T_CODICE_ISTAT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_ISTAT);
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
    if (null == this.N_ID_INDIRIZZO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_INDIRIZZO, __dataOut);
    }
    if (null == this.D_DATA_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF);
    }
    if (null == this.T_DISALIMENTABILITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DISALIMENTABILITA);
    }
    if (null == this.T_ACCESSO_UI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ACCESSO_UI);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PDR, __dataOut);
    }
    if (null == this.T_CODICE_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_PDR);
    }
    if (null == this.T_COD_TIPO_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_TIPO_PDR);
    }
    if (null == this.T_CODICE_ISTAT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_ISTAT);
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
    if (null == this.N_ID_INDIRIZZO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_INDIRIZZO, __dataOut);
    }
    if (null == this.D_DATA_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF);
    }
    if (null == this.T_DISALIMENTABILITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DISALIMENTABILITA);
    }
    if (null == this.T_ACCESSO_UI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ACCESSO_UI);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PDR==null?"":N_ID_PDR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_PDR==null?"":T_CODICE_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_TIPO_PDR==null?"":T_COD_TIPO_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_ISTAT==null?"":T_CODICE_ISTAT, " ", delimiters));
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_INDIRIZZO==null?"":N_ID_INDIRIZZO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF==null?"":D_DATA_RIF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DISALIMENTABILITA==null?"":T_DISALIMENTABILITA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ACCESSO_UI==null?"":T_ACCESSO_UI, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PDR==null?"":N_ID_PDR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_PDR==null?"":T_CODICE_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_TIPO_PDR==null?"":T_COD_TIPO_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_ISTAT==null?"":T_CODICE_ISTAT, " ", delimiters));
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_INDIRIZZO==null?"":N_ID_INDIRIZZO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF==null?"":D_DATA_RIF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DISALIMENTABILITA==null?"":T_DISALIMENTABILITA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ACCESSO_UI==null?"":T_ACCESSO_UI, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PDR = null; } else {
      this.N_ID_PDR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_PDR = null; } else {
      this.T_CODICE_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_TIPO_PDR = null; } else {
      this.T_COD_TIPO_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_ISTAT = null; } else {
      this.T_CODICE_ISTAT = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_INDIRIZZO = null; } else {
      this.N_ID_INDIRIZZO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RIF = null; } else {
      this.D_DATA_RIF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DISALIMENTABILITA = null; } else {
      this.T_DISALIMENTABILITA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ACCESSO_UI = null; } else {
      this.T_ACCESSO_UI = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PDR = null; } else {
      this.N_ID_PDR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_PDR = null; } else {
      this.T_CODICE_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_TIPO_PDR = null; } else {
      this.T_COD_TIPO_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_ISTAT = null; } else {
      this.T_CODICE_ISTAT = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_INDIRIZZO = null; } else {
      this.N_ID_INDIRIZZO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RIF = null; } else {
      this.D_DATA_RIF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DISALIMENTABILITA = null; } else {
      this.T_DISALIMENTABILITA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ACCESSO_UI = null; } else {
      this.T_ACCESSO_UI = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    rcugas_rcugas_pdr o = (rcugas_rcugas_pdr) super.clone();
    return o;
  }

  public void clone0(rcugas_rcugas_pdr o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_PDR", this.N_ID_PDR);
    __sqoop$field_map.put("T_CODICE_PDR", this.T_CODICE_PDR);
    __sqoop$field_map.put("T_COD_TIPO_PDR", this.T_COD_TIPO_PDR);
    __sqoop$field_map.put("T_CODICE_ISTAT", this.T_CODICE_ISTAT);
    __sqoop$field_map.put("T_NOTE", this.T_NOTE);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("N_ID_INDIRIZZO", this.N_ID_INDIRIZZO);
    __sqoop$field_map.put("D_DATA_RIF", this.D_DATA_RIF);
    __sqoop$field_map.put("T_DISALIMENTABILITA", this.T_DISALIMENTABILITA);
    __sqoop$field_map.put("T_ACCESSO_UI", this.T_ACCESSO_UI);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_PDR", this.N_ID_PDR);
    __sqoop$field_map.put("T_CODICE_PDR", this.T_CODICE_PDR);
    __sqoop$field_map.put("T_COD_TIPO_PDR", this.T_COD_TIPO_PDR);
    __sqoop$field_map.put("T_CODICE_ISTAT", this.T_CODICE_ISTAT);
    __sqoop$field_map.put("T_NOTE", this.T_NOTE);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("N_ID_INDIRIZZO", this.N_ID_INDIRIZZO);
    __sqoop$field_map.put("D_DATA_RIF", this.D_DATA_RIF);
    __sqoop$field_map.put("T_DISALIMENTABILITA", this.T_DISALIMENTABILITA);
    __sqoop$field_map.put("T_ACCESSO_UI", this.T_ACCESSO_UI);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
