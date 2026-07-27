// ORM class for table 'RCU.V_RCU_AZIENDA'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 14:47:52 GMT 2019
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

public class RCU_V_RCU_AZIENDA extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_AZIENDA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_AZIENDA = (java.math.BigDecimal)value;
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
    setters.put("T_CF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CF = (String)value;
      }
    });
    setters.put("T_RAG_SOC", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_RAG_SOC = (String)value;
      }
    });
    setters.put("N_ID_SEDELEGALE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_SEDELEGALE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_CONTATTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CONTATTO = (String)value;
      }
    });
    setters.put("T_EMAIL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_EMAIL = (String)value;
      }
    });
    setters.put("T_PEC", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PEC = (String)value;
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
    setters.put("T_RUOLI", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_RUOLI = (String)value;
      }
    });
    setters.put("T_DESC_RUOLI", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_DESC_RUOLI = (String)value;
      }
    });
  }
  public RCU_V_RCU_AZIENDA() {
    init0();
  }
  private java.math.BigDecimal N_ID_AZIENDA;
  public java.math.BigDecimal get_N_ID_AZIENDA() {
    return N_ID_AZIENDA;
  }
  public void set_N_ID_AZIENDA(java.math.BigDecimal N_ID_AZIENDA) {
    this.N_ID_AZIENDA = N_ID_AZIENDA;
  }
  public RCU_V_RCU_AZIENDA with_N_ID_AZIENDA(java.math.BigDecimal N_ID_AZIENDA) {
    this.N_ID_AZIENDA = N_ID_AZIENDA;
    return this;
  }
  private java.math.BigDecimal N_ID_UTENTE;
  public java.math.BigDecimal get_N_ID_UTENTE() {
    return N_ID_UTENTE;
  }
  public void set_N_ID_UTENTE(java.math.BigDecimal N_ID_UTENTE) {
    this.N_ID_UTENTE = N_ID_UTENTE;
  }
  public RCU_V_RCU_AZIENDA with_N_ID_UTENTE(java.math.BigDecimal N_ID_UTENTE) {
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
  public RCU_V_RCU_AZIENDA with_T_CODICE_AEEG(String T_CODICE_AEEG) {
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
  public RCU_V_RCU_AZIENDA with_T_PIVA(String T_PIVA) {
    this.T_PIVA = T_PIVA;
    return this;
  }
  private String T_CF;
  public String get_T_CF() {
    return T_CF;
  }
  public void set_T_CF(String T_CF) {
    this.T_CF = T_CF;
  }
  public RCU_V_RCU_AZIENDA with_T_CF(String T_CF) {
    this.T_CF = T_CF;
    return this;
  }
  private String T_RAG_SOC;
  public String get_T_RAG_SOC() {
    return T_RAG_SOC;
  }
  public void set_T_RAG_SOC(String T_RAG_SOC) {
    this.T_RAG_SOC = T_RAG_SOC;
  }
  public RCU_V_RCU_AZIENDA with_T_RAG_SOC(String T_RAG_SOC) {
    this.T_RAG_SOC = T_RAG_SOC;
    return this;
  }
  private java.math.BigDecimal N_ID_SEDELEGALE;
  public java.math.BigDecimal get_N_ID_SEDELEGALE() {
    return N_ID_SEDELEGALE;
  }
  public void set_N_ID_SEDELEGALE(java.math.BigDecimal N_ID_SEDELEGALE) {
    this.N_ID_SEDELEGALE = N_ID_SEDELEGALE;
  }
  public RCU_V_RCU_AZIENDA with_N_ID_SEDELEGALE(java.math.BigDecimal N_ID_SEDELEGALE) {
    this.N_ID_SEDELEGALE = N_ID_SEDELEGALE;
    return this;
  }
  private String T_CONTATTO;
  public String get_T_CONTATTO() {
    return T_CONTATTO;
  }
  public void set_T_CONTATTO(String T_CONTATTO) {
    this.T_CONTATTO = T_CONTATTO;
  }
  public RCU_V_RCU_AZIENDA with_T_CONTATTO(String T_CONTATTO) {
    this.T_CONTATTO = T_CONTATTO;
    return this;
  }
  private String T_EMAIL;
  public String get_T_EMAIL() {
    return T_EMAIL;
  }
  public void set_T_EMAIL(String T_EMAIL) {
    this.T_EMAIL = T_EMAIL;
  }
  public RCU_V_RCU_AZIENDA with_T_EMAIL(String T_EMAIL) {
    this.T_EMAIL = T_EMAIL;
    return this;
  }
  private String T_PEC;
  public String get_T_PEC() {
    return T_PEC;
  }
  public void set_T_PEC(String T_PEC) {
    this.T_PEC = T_PEC;
  }
  public RCU_V_RCU_AZIENDA with_T_PEC(String T_PEC) {
    this.T_PEC = T_PEC;
    return this;
  }
  private java.sql.Timestamp D_AGGIORNAMENTO;
  public java.sql.Timestamp get_D_AGGIORNAMENTO() {
    return D_AGGIORNAMENTO;
  }
  public void set_D_AGGIORNAMENTO(java.sql.Timestamp D_AGGIORNAMENTO) {
    this.D_AGGIORNAMENTO = D_AGGIORNAMENTO;
  }
  public RCU_V_RCU_AZIENDA with_D_AGGIORNAMENTO(java.sql.Timestamp D_AGGIORNAMENTO) {
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
  public RCU_V_RCU_AZIENDA with_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
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
  public RCU_V_RCU_AZIENDA with_N_ID_S_PREC(java.math.BigDecimal N_ID_S_PREC) {
    this.N_ID_S_PREC = N_ID_S_PREC;
    return this;
  }
  private String T_RUOLI;
  public String get_T_RUOLI() {
    return T_RUOLI;
  }
  public void set_T_RUOLI(String T_RUOLI) {
    this.T_RUOLI = T_RUOLI;
  }
  public RCU_V_RCU_AZIENDA with_T_RUOLI(String T_RUOLI) {
    this.T_RUOLI = T_RUOLI;
    return this;
  }
  private String T_DESC_RUOLI;
  public String get_T_DESC_RUOLI() {
    return T_DESC_RUOLI;
  }
  public void set_T_DESC_RUOLI(String T_DESC_RUOLI) {
    this.T_DESC_RUOLI = T_DESC_RUOLI;
  }
  public RCU_V_RCU_AZIENDA with_T_DESC_RUOLI(String T_DESC_RUOLI) {
    this.T_DESC_RUOLI = T_DESC_RUOLI;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RCU_V_RCU_AZIENDA)) {
      return false;
    }
    RCU_V_RCU_AZIENDA that = (RCU_V_RCU_AZIENDA) o;
    boolean equal = true;
    equal = equal && (this.N_ID_AZIENDA == null ? that.N_ID_AZIENDA == null : this.N_ID_AZIENDA.equals(that.N_ID_AZIENDA));
    equal = equal && (this.N_ID_UTENTE == null ? that.N_ID_UTENTE == null : this.N_ID_UTENTE.equals(that.N_ID_UTENTE));
    equal = equal && (this.T_CODICE_AEEG == null ? that.T_CODICE_AEEG == null : this.T_CODICE_AEEG.equals(that.T_CODICE_AEEG));
    equal = equal && (this.T_PIVA == null ? that.T_PIVA == null : this.T_PIVA.equals(that.T_PIVA));
    equal = equal && (this.T_CF == null ? that.T_CF == null : this.T_CF.equals(that.T_CF));
    equal = equal && (this.T_RAG_SOC == null ? that.T_RAG_SOC == null : this.T_RAG_SOC.equals(that.T_RAG_SOC));
    equal = equal && (this.N_ID_SEDELEGALE == null ? that.N_ID_SEDELEGALE == null : this.N_ID_SEDELEGALE.equals(that.N_ID_SEDELEGALE));
    equal = equal && (this.T_CONTATTO == null ? that.T_CONTATTO == null : this.T_CONTATTO.equals(that.T_CONTATTO));
    equal = equal && (this.T_EMAIL == null ? that.T_EMAIL == null : this.T_EMAIL.equals(that.T_EMAIL));
    equal = equal && (this.T_PEC == null ? that.T_PEC == null : this.T_PEC.equals(that.T_PEC));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.T_RUOLI == null ? that.T_RUOLI == null : this.T_RUOLI.equals(that.T_RUOLI));
    equal = equal && (this.T_DESC_RUOLI == null ? that.T_DESC_RUOLI == null : this.T_DESC_RUOLI.equals(that.T_DESC_RUOLI));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RCU_V_RCU_AZIENDA)) {
      return false;
    }
    RCU_V_RCU_AZIENDA that = (RCU_V_RCU_AZIENDA) o;
    boolean equal = true;
    equal = equal && (this.N_ID_AZIENDA == null ? that.N_ID_AZIENDA == null : this.N_ID_AZIENDA.equals(that.N_ID_AZIENDA));
    equal = equal && (this.N_ID_UTENTE == null ? that.N_ID_UTENTE == null : this.N_ID_UTENTE.equals(that.N_ID_UTENTE));
    equal = equal && (this.T_CODICE_AEEG == null ? that.T_CODICE_AEEG == null : this.T_CODICE_AEEG.equals(that.T_CODICE_AEEG));
    equal = equal && (this.T_PIVA == null ? that.T_PIVA == null : this.T_PIVA.equals(that.T_PIVA));
    equal = equal && (this.T_CF == null ? that.T_CF == null : this.T_CF.equals(that.T_CF));
    equal = equal && (this.T_RAG_SOC == null ? that.T_RAG_SOC == null : this.T_RAG_SOC.equals(that.T_RAG_SOC));
    equal = equal && (this.N_ID_SEDELEGALE == null ? that.N_ID_SEDELEGALE == null : this.N_ID_SEDELEGALE.equals(that.N_ID_SEDELEGALE));
    equal = equal && (this.T_CONTATTO == null ? that.T_CONTATTO == null : this.T_CONTATTO.equals(that.T_CONTATTO));
    equal = equal && (this.T_EMAIL == null ? that.T_EMAIL == null : this.T_EMAIL.equals(that.T_EMAIL));
    equal = equal && (this.T_PEC == null ? that.T_PEC == null : this.T_PEC.equals(that.T_PEC));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.T_RUOLI == null ? that.T_RUOLI == null : this.T_RUOLI.equals(that.T_RUOLI));
    equal = equal && (this.T_DESC_RUOLI == null ? that.T_DESC_RUOLI == null : this.T_DESC_RUOLI.equals(that.T_DESC_RUOLI));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_AZIENDA = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_UTENTE = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_CODICE_AEEG = JdbcWritableBridge.readString(3, __dbResults);
    this.T_PIVA = JdbcWritableBridge.readString(4, __dbResults);
    this.T_CF = JdbcWritableBridge.readString(5, __dbResults);
    this.T_RAG_SOC = JdbcWritableBridge.readString(6, __dbResults);
    this.N_ID_SEDELEGALE = JdbcWritableBridge.readBigDecimal(7, __dbResults);
    this.T_CONTATTO = JdbcWritableBridge.readString(8, __dbResults);
    this.T_EMAIL = JdbcWritableBridge.readString(9, __dbResults);
    this.T_PEC = JdbcWritableBridge.readString(10, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readTimestamp(11, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.T_RUOLI = JdbcWritableBridge.readString(14, __dbResults);
    this.T_DESC_RUOLI = JdbcWritableBridge.readString(15, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_AZIENDA = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_UTENTE = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_CODICE_AEEG = JdbcWritableBridge.readString(3, __dbResults);
    this.T_PIVA = JdbcWritableBridge.readString(4, __dbResults);
    this.T_CF = JdbcWritableBridge.readString(5, __dbResults);
    this.T_RAG_SOC = JdbcWritableBridge.readString(6, __dbResults);
    this.N_ID_SEDELEGALE = JdbcWritableBridge.readBigDecimal(7, __dbResults);
    this.T_CONTATTO = JdbcWritableBridge.readString(8, __dbResults);
    this.T_EMAIL = JdbcWritableBridge.readString(9, __dbResults);
    this.T_PEC = JdbcWritableBridge.readString(10, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readTimestamp(11, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.T_RUOLI = JdbcWritableBridge.readString(14, __dbResults);
    this.T_DESC_RUOLI = JdbcWritableBridge.readString(15, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_AZIENDA, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UTENTE, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_AEEG, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CF, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_RAG_SOC, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_SEDELEGALE, 7 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CONTATTO, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_EMAIL, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PEC, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(D_AGGIORNAMENTO, 11 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 12 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_RUOLI, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DESC_RUOLI, 15 + __off, 12, __dbStmt);
    return 15;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_AZIENDA, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UTENTE, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_AEEG, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CF, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_RAG_SOC, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_SEDELEGALE, 7 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CONTATTO, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_EMAIL, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PEC, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(D_AGGIORNAMENTO, 11 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 12 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_RUOLI, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DESC_RUOLI, 15 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_AZIENDA = null;
    } else {
    this.N_ID_AZIENDA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
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
        this.T_CF = null;
    } else {
    this.T_CF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_RAG_SOC = null;
    } else {
    this.T_RAG_SOC = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_SEDELEGALE = null;
    } else {
    this.N_ID_SEDELEGALE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CONTATTO = null;
    } else {
    this.T_CONTATTO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_EMAIL = null;
    } else {
    this.T_EMAIL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PEC = null;
    } else {
    this.T_PEC = Text.readString(__dataIn);
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
        this.T_RUOLI = null;
    } else {
    this.T_RUOLI = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_DESC_RUOLI = null;
    } else {
    this.T_DESC_RUOLI = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_AZIENDA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_AZIENDA, __dataOut);
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
    if (null == this.T_CF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CF);
    }
    if (null == this.T_RAG_SOC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RAG_SOC);
    }
    if (null == this.N_ID_SEDELEGALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_SEDELEGALE, __dataOut);
    }
    if (null == this.T_CONTATTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CONTATTO);
    }
    if (null == this.T_EMAIL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_EMAIL);
    }
    if (null == this.T_PEC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PEC);
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
    if (null == this.T_RUOLI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RUOLI);
    }
    if (null == this.T_DESC_RUOLI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DESC_RUOLI);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_AZIENDA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_AZIENDA, __dataOut);
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
    if (null == this.T_CF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CF);
    }
    if (null == this.T_RAG_SOC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RAG_SOC);
    }
    if (null == this.N_ID_SEDELEGALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_SEDELEGALE, __dataOut);
    }
    if (null == this.T_CONTATTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CONTATTO);
    }
    if (null == this.T_EMAIL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_EMAIL);
    }
    if (null == this.T_PEC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PEC);
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
    if (null == this.T_RUOLI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RUOLI);
    }
    if (null == this.T_DESC_RUOLI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DESC_RUOLI);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_AZIENDA==null?"":N_ID_AZIENDA.toPlainString(), delimiters));
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CF==null?"":T_CF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RAG_SOC==null?"":T_RAG_SOC, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_SEDELEGALE==null?"":N_ID_SEDELEGALE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CONTATTO==null?"":T_CONTATTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_EMAIL==null?"":T_EMAIL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PEC==null?"":T_PEC, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(D_AGGIORNAMENTO==null?"":"" + D_AGGIORNAMENTO, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TRACCIA==null?"":N_ID_TRACCIA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_PREC==null?"":N_ID_S_PREC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RUOLI==null?"":T_RUOLI, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DESC_RUOLI==null?"":T_DESC_RUOLI, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_AZIENDA==null?"":N_ID_AZIENDA.toPlainString(), delimiters));
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CF==null?"":T_CF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RAG_SOC==null?"":T_RAG_SOC, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_SEDELEGALE==null?"":N_ID_SEDELEGALE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CONTATTO==null?"":T_CONTATTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_EMAIL==null?"":T_EMAIL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PEC==null?"":T_PEC, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(D_AGGIORNAMENTO==null?"":"" + D_AGGIORNAMENTO, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TRACCIA==null?"":N_ID_TRACCIA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_PREC==null?"":N_ID_S_PREC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RUOLI==null?"":T_RUOLI, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DESC_RUOLI==null?"":T_DESC_RUOLI, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_AZIENDA = null; } else {
      this.N_ID_AZIENDA = new java.math.BigDecimal(__cur_str);
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
    if (__cur_str.equals("null")) { this.T_CF = null; } else {
      this.T_CF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RAG_SOC = null; } else {
      this.T_RAG_SOC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_SEDELEGALE = null; } else {
      this.N_ID_SEDELEGALE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CONTATTO = null; } else {
      this.T_CONTATTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_EMAIL = null; } else {
      this.T_EMAIL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PEC = null; } else {
      this.T_PEC = __cur_str;
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
    if (__cur_str.equals("null")) { this.T_RUOLI = null; } else {
      this.T_RUOLI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DESC_RUOLI = null; } else {
      this.T_DESC_RUOLI = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_AZIENDA = null; } else {
      this.N_ID_AZIENDA = new java.math.BigDecimal(__cur_str);
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
    if (__cur_str.equals("null")) { this.T_CF = null; } else {
      this.T_CF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RAG_SOC = null; } else {
      this.T_RAG_SOC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_SEDELEGALE = null; } else {
      this.N_ID_SEDELEGALE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CONTATTO = null; } else {
      this.T_CONTATTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_EMAIL = null; } else {
      this.T_EMAIL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PEC = null; } else {
      this.T_PEC = __cur_str;
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
    if (__cur_str.equals("null")) { this.T_RUOLI = null; } else {
      this.T_RUOLI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DESC_RUOLI = null; } else {
      this.T_DESC_RUOLI = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    RCU_V_RCU_AZIENDA o = (RCU_V_RCU_AZIENDA) super.clone();
    o.D_AGGIORNAMENTO = (o.D_AGGIORNAMENTO != null) ? (java.sql.Timestamp) o.D_AGGIORNAMENTO.clone() : null;
    return o;
  }

  public void clone0(RCU_V_RCU_AZIENDA o) throws CloneNotSupportedException {
    o.D_AGGIORNAMENTO = (o.D_AGGIORNAMENTO != null) ? (java.sql.Timestamp) o.D_AGGIORNAMENTO.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_AZIENDA", this.N_ID_AZIENDA);
    __sqoop$field_map.put("N_ID_UTENTE", this.N_ID_UTENTE);
    __sqoop$field_map.put("T_CODICE_AEEG", this.T_CODICE_AEEG);
    __sqoop$field_map.put("T_PIVA", this.T_PIVA);
    __sqoop$field_map.put("T_CF", this.T_CF);
    __sqoop$field_map.put("T_RAG_SOC", this.T_RAG_SOC);
    __sqoop$field_map.put("N_ID_SEDELEGALE", this.N_ID_SEDELEGALE);
    __sqoop$field_map.put("T_CONTATTO", this.T_CONTATTO);
    __sqoop$field_map.put("T_EMAIL", this.T_EMAIL);
    __sqoop$field_map.put("T_PEC", this.T_PEC);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("T_RUOLI", this.T_RUOLI);
    __sqoop$field_map.put("T_DESC_RUOLI", this.T_DESC_RUOLI);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_AZIENDA", this.N_ID_AZIENDA);
    __sqoop$field_map.put("N_ID_UTENTE", this.N_ID_UTENTE);
    __sqoop$field_map.put("T_CODICE_AEEG", this.T_CODICE_AEEG);
    __sqoop$field_map.put("T_PIVA", this.T_PIVA);
    __sqoop$field_map.put("T_CF", this.T_CF);
    __sqoop$field_map.put("T_RAG_SOC", this.T_RAG_SOC);
    __sqoop$field_map.put("N_ID_SEDELEGALE", this.N_ID_SEDELEGALE);
    __sqoop$field_map.put("T_CONTATTO", this.T_CONTATTO);
    __sqoop$field_map.put("T_EMAIL", this.T_EMAIL);
    __sqoop$field_map.put("T_PEC", this.T_PEC);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("T_RUOLI", this.T_RUOLI);
    __sqoop$field_map.put("T_DESC_RUOLI", this.T_DESC_RUOLI);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
