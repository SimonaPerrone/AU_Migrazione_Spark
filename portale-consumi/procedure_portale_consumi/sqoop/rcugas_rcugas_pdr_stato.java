// ORM class for table 'rcugas.rcugas_pdr_stato'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Thu Dec 03 15:52:30 CET 2020
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

public class rcugas_rcugas_pdr_stato extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_PDR_STATO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_PDR_STATO = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_PDR = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_COD_STATO_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COD_STATO_PDR = (String)value;
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
    setters.put("T_COD_CAUSALE_NO_DISAT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COD_CAUSALE_NO_DISAT = (String)value;
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
    setters.put("B_PROCESSO_IN_CORSO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_PROCESSO_IN_CORSO = (String)value;
      }
    });
    setters.put("T_COD_CAUSALE_NO_RIATT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COD_CAUSALE_NO_RIATT = (String)value;
      }
    });
    setters.put("TIPO_DATA_INIZIO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TIPO_DATA_INIZIO = (String)value;
      }
    });
    setters.put("TIPO_DATA_FINE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TIPO_DATA_FINE = (String)value;
      }
    });
    setters.put("T_NOTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_NOTE = (String)value;
      }
    });
    setters.put("D_DATA_RIF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_RIF = (String)value;
      }
    });
  }
  public rcugas_rcugas_pdr_stato() {
    init0();
  }
  private java.math.BigDecimal N_ID_PDR_STATO;
  public java.math.BigDecimal get_N_ID_PDR_STATO() {
    return N_ID_PDR_STATO;
  }
  public void set_N_ID_PDR_STATO(java.math.BigDecimal N_ID_PDR_STATO) {
    this.N_ID_PDR_STATO = N_ID_PDR_STATO;
  }
  public rcugas_rcugas_pdr_stato with_N_ID_PDR_STATO(java.math.BigDecimal N_ID_PDR_STATO) {
    this.N_ID_PDR_STATO = N_ID_PDR_STATO;
    return this;
  }
  private java.math.BigDecimal N_ID_PDR;
  public java.math.BigDecimal get_N_ID_PDR() {
    return N_ID_PDR;
  }
  public void set_N_ID_PDR(java.math.BigDecimal N_ID_PDR) {
    this.N_ID_PDR = N_ID_PDR;
  }
  public rcugas_rcugas_pdr_stato with_N_ID_PDR(java.math.BigDecimal N_ID_PDR) {
    this.N_ID_PDR = N_ID_PDR;
    return this;
  }
  private String T_COD_STATO_PDR;
  public String get_T_COD_STATO_PDR() {
    return T_COD_STATO_PDR;
  }
  public void set_T_COD_STATO_PDR(String T_COD_STATO_PDR) {
    this.T_COD_STATO_PDR = T_COD_STATO_PDR;
  }
  public rcugas_rcugas_pdr_stato with_T_COD_STATO_PDR(String T_COD_STATO_PDR) {
    this.T_COD_STATO_PDR = T_COD_STATO_PDR;
    return this;
  }
  private String D_DATA_INIZIO;
  public String get_D_DATA_INIZIO() {
    return D_DATA_INIZIO;
  }
  public void set_D_DATA_INIZIO(String D_DATA_INIZIO) {
    this.D_DATA_INIZIO = D_DATA_INIZIO;
  }
  public rcugas_rcugas_pdr_stato with_D_DATA_INIZIO(String D_DATA_INIZIO) {
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
  public rcugas_rcugas_pdr_stato with_D_DATA_FINE(String D_DATA_FINE) {
    this.D_DATA_FINE = D_DATA_FINE;
    return this;
  }
  private String T_COD_CAUSALE_NO_DISAT;
  public String get_T_COD_CAUSALE_NO_DISAT() {
    return T_COD_CAUSALE_NO_DISAT;
  }
  public void set_T_COD_CAUSALE_NO_DISAT(String T_COD_CAUSALE_NO_DISAT) {
    this.T_COD_CAUSALE_NO_DISAT = T_COD_CAUSALE_NO_DISAT;
  }
  public rcugas_rcugas_pdr_stato with_T_COD_CAUSALE_NO_DISAT(String T_COD_CAUSALE_NO_DISAT) {
    this.T_COD_CAUSALE_NO_DISAT = T_COD_CAUSALE_NO_DISAT;
    return this;
  }
  private String D_AGGIORNAMENTO;
  public String get_D_AGGIORNAMENTO() {
    return D_AGGIORNAMENTO;
  }
  public void set_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
    this.D_AGGIORNAMENTO = D_AGGIORNAMENTO;
  }
  public rcugas_rcugas_pdr_stato with_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
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
  public rcugas_rcugas_pdr_stato with_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
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
  public rcugas_rcugas_pdr_stato with_N_ID_S_PREC(java.math.BigDecimal N_ID_S_PREC) {
    this.N_ID_S_PREC = N_ID_S_PREC;
    return this;
  }
  private String B_PROCESSO_IN_CORSO;
  public String get_B_PROCESSO_IN_CORSO() {
    return B_PROCESSO_IN_CORSO;
  }
  public void set_B_PROCESSO_IN_CORSO(String B_PROCESSO_IN_CORSO) {
    this.B_PROCESSO_IN_CORSO = B_PROCESSO_IN_CORSO;
  }
  public rcugas_rcugas_pdr_stato with_B_PROCESSO_IN_CORSO(String B_PROCESSO_IN_CORSO) {
    this.B_PROCESSO_IN_CORSO = B_PROCESSO_IN_CORSO;
    return this;
  }
  private String T_COD_CAUSALE_NO_RIATT;
  public String get_T_COD_CAUSALE_NO_RIATT() {
    return T_COD_CAUSALE_NO_RIATT;
  }
  public void set_T_COD_CAUSALE_NO_RIATT(String T_COD_CAUSALE_NO_RIATT) {
    this.T_COD_CAUSALE_NO_RIATT = T_COD_CAUSALE_NO_RIATT;
  }
  public rcugas_rcugas_pdr_stato with_T_COD_CAUSALE_NO_RIATT(String T_COD_CAUSALE_NO_RIATT) {
    this.T_COD_CAUSALE_NO_RIATT = T_COD_CAUSALE_NO_RIATT;
    return this;
  }
  private String TIPO_DATA_INIZIO;
  public String get_TIPO_DATA_INIZIO() {
    return TIPO_DATA_INIZIO;
  }
  public void set_TIPO_DATA_INIZIO(String TIPO_DATA_INIZIO) {
    this.TIPO_DATA_INIZIO = TIPO_DATA_INIZIO;
  }
  public rcugas_rcugas_pdr_stato with_TIPO_DATA_INIZIO(String TIPO_DATA_INIZIO) {
    this.TIPO_DATA_INIZIO = TIPO_DATA_INIZIO;
    return this;
  }
  private String TIPO_DATA_FINE;
  public String get_TIPO_DATA_FINE() {
    return TIPO_DATA_FINE;
  }
  public void set_TIPO_DATA_FINE(String TIPO_DATA_FINE) {
    this.TIPO_DATA_FINE = TIPO_DATA_FINE;
  }
  public rcugas_rcugas_pdr_stato with_TIPO_DATA_FINE(String TIPO_DATA_FINE) {
    this.TIPO_DATA_FINE = TIPO_DATA_FINE;
    return this;
  }
  private String T_NOTE;
  public String get_T_NOTE() {
    return T_NOTE;
  }
  public void set_T_NOTE(String T_NOTE) {
    this.T_NOTE = T_NOTE;
  }
  public rcugas_rcugas_pdr_stato with_T_NOTE(String T_NOTE) {
    this.T_NOTE = T_NOTE;
    return this;
  }
  private String D_DATA_RIF;
  public String get_D_DATA_RIF() {
    return D_DATA_RIF;
  }
  public void set_D_DATA_RIF(String D_DATA_RIF) {
    this.D_DATA_RIF = D_DATA_RIF;
  }
  public rcugas_rcugas_pdr_stato with_D_DATA_RIF(String D_DATA_RIF) {
    this.D_DATA_RIF = D_DATA_RIF;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcugas_rcugas_pdr_stato)) {
      return false;
    }
    rcugas_rcugas_pdr_stato that = (rcugas_rcugas_pdr_stato) o;
    boolean equal = true;
    equal = equal && (this.N_ID_PDR_STATO == null ? that.N_ID_PDR_STATO == null : this.N_ID_PDR_STATO.equals(that.N_ID_PDR_STATO));
    equal = equal && (this.N_ID_PDR == null ? that.N_ID_PDR == null : this.N_ID_PDR.equals(that.N_ID_PDR));
    equal = equal && (this.T_COD_STATO_PDR == null ? that.T_COD_STATO_PDR == null : this.T_COD_STATO_PDR.equals(that.T_COD_STATO_PDR));
    equal = equal && (this.D_DATA_INIZIO == null ? that.D_DATA_INIZIO == null : this.D_DATA_INIZIO.equals(that.D_DATA_INIZIO));
    equal = equal && (this.D_DATA_FINE == null ? that.D_DATA_FINE == null : this.D_DATA_FINE.equals(that.D_DATA_FINE));
    equal = equal && (this.T_COD_CAUSALE_NO_DISAT == null ? that.T_COD_CAUSALE_NO_DISAT == null : this.T_COD_CAUSALE_NO_DISAT.equals(that.T_COD_CAUSALE_NO_DISAT));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.B_PROCESSO_IN_CORSO == null ? that.B_PROCESSO_IN_CORSO == null : this.B_PROCESSO_IN_CORSO.equals(that.B_PROCESSO_IN_CORSO));
    equal = equal && (this.T_COD_CAUSALE_NO_RIATT == null ? that.T_COD_CAUSALE_NO_RIATT == null : this.T_COD_CAUSALE_NO_RIATT.equals(that.T_COD_CAUSALE_NO_RIATT));
    equal = equal && (this.TIPO_DATA_INIZIO == null ? that.TIPO_DATA_INIZIO == null : this.TIPO_DATA_INIZIO.equals(that.TIPO_DATA_INIZIO));
    equal = equal && (this.TIPO_DATA_FINE == null ? that.TIPO_DATA_FINE == null : this.TIPO_DATA_FINE.equals(that.TIPO_DATA_FINE));
    equal = equal && (this.T_NOTE == null ? that.T_NOTE == null : this.T_NOTE.equals(that.T_NOTE));
    equal = equal && (this.D_DATA_RIF == null ? that.D_DATA_RIF == null : this.D_DATA_RIF.equals(that.D_DATA_RIF));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcugas_rcugas_pdr_stato)) {
      return false;
    }
    rcugas_rcugas_pdr_stato that = (rcugas_rcugas_pdr_stato) o;
    boolean equal = true;
    equal = equal && (this.N_ID_PDR_STATO == null ? that.N_ID_PDR_STATO == null : this.N_ID_PDR_STATO.equals(that.N_ID_PDR_STATO));
    equal = equal && (this.N_ID_PDR == null ? that.N_ID_PDR == null : this.N_ID_PDR.equals(that.N_ID_PDR));
    equal = equal && (this.T_COD_STATO_PDR == null ? that.T_COD_STATO_PDR == null : this.T_COD_STATO_PDR.equals(that.T_COD_STATO_PDR));
    equal = equal && (this.D_DATA_INIZIO == null ? that.D_DATA_INIZIO == null : this.D_DATA_INIZIO.equals(that.D_DATA_INIZIO));
    equal = equal && (this.D_DATA_FINE == null ? that.D_DATA_FINE == null : this.D_DATA_FINE.equals(that.D_DATA_FINE));
    equal = equal && (this.T_COD_CAUSALE_NO_DISAT == null ? that.T_COD_CAUSALE_NO_DISAT == null : this.T_COD_CAUSALE_NO_DISAT.equals(that.T_COD_CAUSALE_NO_DISAT));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.B_PROCESSO_IN_CORSO == null ? that.B_PROCESSO_IN_CORSO == null : this.B_PROCESSO_IN_CORSO.equals(that.B_PROCESSO_IN_CORSO));
    equal = equal && (this.T_COD_CAUSALE_NO_RIATT == null ? that.T_COD_CAUSALE_NO_RIATT == null : this.T_COD_CAUSALE_NO_RIATT.equals(that.T_COD_CAUSALE_NO_RIATT));
    equal = equal && (this.TIPO_DATA_INIZIO == null ? that.TIPO_DATA_INIZIO == null : this.TIPO_DATA_INIZIO.equals(that.TIPO_DATA_INIZIO));
    equal = equal && (this.TIPO_DATA_FINE == null ? that.TIPO_DATA_FINE == null : this.TIPO_DATA_FINE.equals(that.TIPO_DATA_FINE));
    equal = equal && (this.T_NOTE == null ? that.T_NOTE == null : this.T_NOTE.equals(that.T_NOTE));
    equal = equal && (this.D_DATA_RIF == null ? that.D_DATA_RIF == null : this.D_DATA_RIF.equals(that.D_DATA_RIF));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_PDR_STATO = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_PDR = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_COD_STATO_PDR = JdbcWritableBridge.readString(3, __dbResults);
    this.D_DATA_INIZIO = JdbcWritableBridge.readString(4, __dbResults);
    this.D_DATA_FINE = JdbcWritableBridge.readString(5, __dbResults);
    this.T_COD_CAUSALE_NO_DISAT = JdbcWritableBridge.readString(6, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(7, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.B_PROCESSO_IN_CORSO = JdbcWritableBridge.readString(10, __dbResults);
    this.T_COD_CAUSALE_NO_RIATT = JdbcWritableBridge.readString(11, __dbResults);
    this.TIPO_DATA_INIZIO = JdbcWritableBridge.readString(12, __dbResults);
    this.TIPO_DATA_FINE = JdbcWritableBridge.readString(13, __dbResults);
    this.T_NOTE = JdbcWritableBridge.readString(14, __dbResults);
    this.D_DATA_RIF = JdbcWritableBridge.readString(15, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_PDR_STATO = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_PDR = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_COD_STATO_PDR = JdbcWritableBridge.readString(3, __dbResults);
    this.D_DATA_INIZIO = JdbcWritableBridge.readString(4, __dbResults);
    this.D_DATA_FINE = JdbcWritableBridge.readString(5, __dbResults);
    this.T_COD_CAUSALE_NO_DISAT = JdbcWritableBridge.readString(6, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(7, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.B_PROCESSO_IN_CORSO = JdbcWritableBridge.readString(10, __dbResults);
    this.T_COD_CAUSALE_NO_RIATT = JdbcWritableBridge.readString(11, __dbResults);
    this.TIPO_DATA_INIZIO = JdbcWritableBridge.readString(12, __dbResults);
    this.TIPO_DATA_FINE = JdbcWritableBridge.readString(13, __dbResults);
    this.T_NOTE = JdbcWritableBridge.readString(14, __dbResults);
    this.D_DATA_RIF = JdbcWritableBridge.readString(15, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_PDR_STATO, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PDR, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_STATO_PDR, 3 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INIZIO, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_FINE, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CAUSALE_NO_DISAT, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 7 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_PROCESSO_IN_CORSO, 10 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CAUSALE_NO_RIATT, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TIPO_DATA_INIZIO, 12 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TIPO_DATA_FINE, 13 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTE, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF, 15 + __off, 93, __dbStmt);
    return 15;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_PDR_STATO, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PDR, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_STATO_PDR, 3 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INIZIO, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_FINE, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CAUSALE_NO_DISAT, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 7 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_PROCESSO_IN_CORSO, 10 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CAUSALE_NO_RIATT, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TIPO_DATA_INIZIO, 12 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TIPO_DATA_FINE, 13 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTE, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF, 15 + __off, 93, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_PDR_STATO = null;
    } else {
    this.N_ID_PDR_STATO = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_PDR = null;
    } else {
    this.N_ID_PDR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COD_STATO_PDR = null;
    } else {
    this.T_COD_STATO_PDR = Text.readString(__dataIn);
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
        this.T_COD_CAUSALE_NO_DISAT = null;
    } else {
    this.T_COD_CAUSALE_NO_DISAT = Text.readString(__dataIn);
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
        this.B_PROCESSO_IN_CORSO = null;
    } else {
    this.B_PROCESSO_IN_CORSO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COD_CAUSALE_NO_RIATT = null;
    } else {
    this.T_COD_CAUSALE_NO_RIATT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TIPO_DATA_INIZIO = null;
    } else {
    this.TIPO_DATA_INIZIO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TIPO_DATA_FINE = null;
    } else {
    this.TIPO_DATA_FINE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_NOTE = null;
    } else {
    this.T_NOTE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_RIF = null;
    } else {
    this.D_DATA_RIF = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_PDR_STATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PDR_STATO, __dataOut);
    }
    if (null == this.N_ID_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PDR, __dataOut);
    }
    if (null == this.T_COD_STATO_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_STATO_PDR);
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
    if (null == this.T_COD_CAUSALE_NO_DISAT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_CAUSALE_NO_DISAT);
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
    if (null == this.B_PROCESSO_IN_CORSO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_PROCESSO_IN_CORSO);
    }
    if (null == this.T_COD_CAUSALE_NO_RIATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_CAUSALE_NO_RIATT);
    }
    if (null == this.TIPO_DATA_INIZIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIPO_DATA_INIZIO);
    }
    if (null == this.TIPO_DATA_FINE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIPO_DATA_FINE);
    }
    if (null == this.T_NOTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOTE);
    }
    if (null == this.D_DATA_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_PDR_STATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PDR_STATO, __dataOut);
    }
    if (null == this.N_ID_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PDR, __dataOut);
    }
    if (null == this.T_COD_STATO_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_STATO_PDR);
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
    if (null == this.T_COD_CAUSALE_NO_DISAT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_CAUSALE_NO_DISAT);
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
    if (null == this.B_PROCESSO_IN_CORSO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_PROCESSO_IN_CORSO);
    }
    if (null == this.T_COD_CAUSALE_NO_RIATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_CAUSALE_NO_RIATT);
    }
    if (null == this.TIPO_DATA_INIZIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIPO_DATA_INIZIO);
    }
    if (null == this.TIPO_DATA_FINE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIPO_DATA_FINE);
    }
    if (null == this.T_NOTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOTE);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PDR_STATO==null?"\\N":N_ID_PDR_STATO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PDR==null?"\\N":N_ID_PDR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_STATO_PDR==null?"\\N":T_COD_STATO_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INIZIO==null?"\\N":D_DATA_INIZIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_FINE==null?"\\N":D_DATA_FINE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CAUSALE_NO_DISAT==null?"\\N":T_COD_CAUSALE_NO_DISAT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO==null?"\\N":D_AGGIORNAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TRACCIA==null?"\\N":N_ID_TRACCIA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_PREC==null?"\\N":N_ID_S_PREC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_PROCESSO_IN_CORSO==null?"\\N":B_PROCESSO_IN_CORSO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CAUSALE_NO_RIATT==null?"\\N":T_COD_CAUSALE_NO_RIATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TIPO_DATA_INIZIO==null?"\\N":TIPO_DATA_INIZIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TIPO_DATA_FINE==null?"\\N":TIPO_DATA_FINE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOTE==null?"\\N":T_NOTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF==null?"\\N":D_DATA_RIF, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PDR_STATO==null?"\\N":N_ID_PDR_STATO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PDR==null?"\\N":N_ID_PDR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_STATO_PDR==null?"\\N":T_COD_STATO_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INIZIO==null?"\\N":D_DATA_INIZIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_FINE==null?"\\N":D_DATA_FINE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CAUSALE_NO_DISAT==null?"\\N":T_COD_CAUSALE_NO_DISAT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO==null?"\\N":D_AGGIORNAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TRACCIA==null?"\\N":N_ID_TRACCIA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_PREC==null?"\\N":N_ID_S_PREC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_PROCESSO_IN_CORSO==null?"\\N":B_PROCESSO_IN_CORSO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CAUSALE_NO_RIATT==null?"\\N":T_COD_CAUSALE_NO_RIATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TIPO_DATA_INIZIO==null?"\\N":TIPO_DATA_INIZIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TIPO_DATA_FINE==null?"\\N":TIPO_DATA_FINE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOTE==null?"\\N":T_NOTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF==null?"\\N":D_DATA_RIF, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PDR_STATO = null; } else {
      this.N_ID_PDR_STATO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PDR = null; } else {
      this.N_ID_PDR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_STATO_PDR = null; } else {
      this.T_COD_STATO_PDR = __cur_str;
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
    if (__cur_str.equals("null")) { this.T_COD_CAUSALE_NO_DISAT = null; } else {
      this.T_COD_CAUSALE_NO_DISAT = __cur_str;
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
    if (__cur_str.equals("null")) { this.B_PROCESSO_IN_CORSO = null; } else {
      this.B_PROCESSO_IN_CORSO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_CAUSALE_NO_RIATT = null; } else {
      this.T_COD_CAUSALE_NO_RIATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIPO_DATA_INIZIO = null; } else {
      this.TIPO_DATA_INIZIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIPO_DATA_FINE = null; } else {
      this.TIPO_DATA_FINE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOTE = null; } else {
      this.T_NOTE = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PDR_STATO = null; } else {
      this.N_ID_PDR_STATO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PDR = null; } else {
      this.N_ID_PDR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_STATO_PDR = null; } else {
      this.T_COD_STATO_PDR = __cur_str;
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
    if (__cur_str.equals("null")) { this.T_COD_CAUSALE_NO_DISAT = null; } else {
      this.T_COD_CAUSALE_NO_DISAT = __cur_str;
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
    if (__cur_str.equals("null")) { this.B_PROCESSO_IN_CORSO = null; } else {
      this.B_PROCESSO_IN_CORSO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_CAUSALE_NO_RIATT = null; } else {
      this.T_COD_CAUSALE_NO_RIATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIPO_DATA_INIZIO = null; } else {
      this.TIPO_DATA_INIZIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIPO_DATA_FINE = null; } else {
      this.TIPO_DATA_FINE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOTE = null; } else {
      this.T_NOTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RIF = null; } else {
      this.D_DATA_RIF = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    rcugas_rcugas_pdr_stato o = (rcugas_rcugas_pdr_stato) super.clone();
    return o;
  }

  public void clone0(rcugas_rcugas_pdr_stato o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_PDR_STATO", this.N_ID_PDR_STATO);
    __sqoop$field_map.put("N_ID_PDR", this.N_ID_PDR);
    __sqoop$field_map.put("T_COD_STATO_PDR", this.T_COD_STATO_PDR);
    __sqoop$field_map.put("D_DATA_INIZIO", this.D_DATA_INIZIO);
    __sqoop$field_map.put("D_DATA_FINE", this.D_DATA_FINE);
    __sqoop$field_map.put("T_COD_CAUSALE_NO_DISAT", this.T_COD_CAUSALE_NO_DISAT);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("B_PROCESSO_IN_CORSO", this.B_PROCESSO_IN_CORSO);
    __sqoop$field_map.put("T_COD_CAUSALE_NO_RIATT", this.T_COD_CAUSALE_NO_RIATT);
    __sqoop$field_map.put("TIPO_DATA_INIZIO", this.TIPO_DATA_INIZIO);
    __sqoop$field_map.put("TIPO_DATA_FINE", this.TIPO_DATA_FINE);
    __sqoop$field_map.put("T_NOTE", this.T_NOTE);
    __sqoop$field_map.put("D_DATA_RIF", this.D_DATA_RIF);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_PDR_STATO", this.N_ID_PDR_STATO);
    __sqoop$field_map.put("N_ID_PDR", this.N_ID_PDR);
    __sqoop$field_map.put("T_COD_STATO_PDR", this.T_COD_STATO_PDR);
    __sqoop$field_map.put("D_DATA_INIZIO", this.D_DATA_INIZIO);
    __sqoop$field_map.put("D_DATA_FINE", this.D_DATA_FINE);
    __sqoop$field_map.put("T_COD_CAUSALE_NO_DISAT", this.T_COD_CAUSALE_NO_DISAT);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("B_PROCESSO_IN_CORSO", this.B_PROCESSO_IN_CORSO);
    __sqoop$field_map.put("T_COD_CAUSALE_NO_RIATT", this.T_COD_CAUSALE_NO_RIATT);
    __sqoop$field_map.put("TIPO_DATA_INIZIO", this.TIPO_DATA_INIZIO);
    __sqoop$field_map.put("TIPO_DATA_FINE", this.TIPO_DATA_FINE);
    __sqoop$field_map.put("T_NOTE", this.T_NOTE);
    __sqoop$field_map.put("D_DATA_RIF", this.D_DATA_RIF);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
