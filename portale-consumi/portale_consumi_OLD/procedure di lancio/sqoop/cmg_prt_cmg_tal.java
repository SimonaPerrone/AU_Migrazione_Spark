// ORM class for table 'cmg.prt_cmg_tal'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 08:57:04 CEST 2019
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

public class cmg_prt_cmg_tal extends SqoopRecord  implements DBWritable, Writable {
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
    setters.put("N_ID_FILE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_FILE = (java.math.BigDecimal)value;
      }
    });
    setters.put("ANNOMESE_RIFERIMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ANNOMESE_RIFERIMENTO = (String)value;
      }
    });
    setters.put("DT_CARICAMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DT_CARICAMENTO = (String)value;
      }
    });
    setters.put("COD_SERVIZIO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        COD_SERVIZIO = (String)value;
      }
    });
    setters.put("COD_FLUSSO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        COD_FLUSSO = (String)value;
      }
    });
    setters.put("PIVA_UTENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PIVA_UTENTE = (String)value;
      }
    });
    setters.put("PIVA_DISTR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PIVA_DISTR = (String)value;
      }
    });
    setters.put("COD_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        COD_PDR = (String)value;
      }
    });
    setters.put("MATR_MIS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MATR_MIS = (String)value;
      }
    });
    setters.put("MATR_CONV", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MATR_CONV = (String)value;
      }
    });
    setters.put("DATA_COM_AUTOLET_CF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATA_COM_AUTOLET_CF = (String)value;
      }
    });
    setters.put("LET_TOT_PREL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        LET_TOT_PREL = (String)value;
      }
    });
    setters.put("LET_TOT_CONV", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        LET_TOT_CONV = (String)value;
      }
    });
    setters.put("ESITO_VAL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ESITO_VAL = (String)value;
      }
    });
    setters.put("NOTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        NOTE = (String)value;
      }
    });
  }
  public cmg_prt_cmg_tal() {
    init0();
  }
  private java.math.BigDecimal N_ID;
  public java.math.BigDecimal get_N_ID() {
    return N_ID;
  }
  public void set_N_ID(java.math.BigDecimal N_ID) {
    this.N_ID = N_ID;
  }
  public cmg_prt_cmg_tal with_N_ID(java.math.BigDecimal N_ID) {
    this.N_ID = N_ID;
    return this;
  }
  private java.math.BigDecimal N_ID_FILE;
  public java.math.BigDecimal get_N_ID_FILE() {
    return N_ID_FILE;
  }
  public void set_N_ID_FILE(java.math.BigDecimal N_ID_FILE) {
    this.N_ID_FILE = N_ID_FILE;
  }
  public cmg_prt_cmg_tal with_N_ID_FILE(java.math.BigDecimal N_ID_FILE) {
    this.N_ID_FILE = N_ID_FILE;
    return this;
  }
  private String ANNOMESE_RIFERIMENTO;
  public String get_ANNOMESE_RIFERIMENTO() {
    return ANNOMESE_RIFERIMENTO;
  }
  public void set_ANNOMESE_RIFERIMENTO(String ANNOMESE_RIFERIMENTO) {
    this.ANNOMESE_RIFERIMENTO = ANNOMESE_RIFERIMENTO;
  }
  public cmg_prt_cmg_tal with_ANNOMESE_RIFERIMENTO(String ANNOMESE_RIFERIMENTO) {
    this.ANNOMESE_RIFERIMENTO = ANNOMESE_RIFERIMENTO;
    return this;
  }
  private String DT_CARICAMENTO;
  public String get_DT_CARICAMENTO() {
    return DT_CARICAMENTO;
  }
  public void set_DT_CARICAMENTO(String DT_CARICAMENTO) {
    this.DT_CARICAMENTO = DT_CARICAMENTO;
  }
  public cmg_prt_cmg_tal with_DT_CARICAMENTO(String DT_CARICAMENTO) {
    this.DT_CARICAMENTO = DT_CARICAMENTO;
    return this;
  }
  private String COD_SERVIZIO;
  public String get_COD_SERVIZIO() {
    return COD_SERVIZIO;
  }
  public void set_COD_SERVIZIO(String COD_SERVIZIO) {
    this.COD_SERVIZIO = COD_SERVIZIO;
  }
  public cmg_prt_cmg_tal with_COD_SERVIZIO(String COD_SERVIZIO) {
    this.COD_SERVIZIO = COD_SERVIZIO;
    return this;
  }
  private String COD_FLUSSO;
  public String get_COD_FLUSSO() {
    return COD_FLUSSO;
  }
  public void set_COD_FLUSSO(String COD_FLUSSO) {
    this.COD_FLUSSO = COD_FLUSSO;
  }
  public cmg_prt_cmg_tal with_COD_FLUSSO(String COD_FLUSSO) {
    this.COD_FLUSSO = COD_FLUSSO;
    return this;
  }
  private String PIVA_UTENTE;
  public String get_PIVA_UTENTE() {
    return PIVA_UTENTE;
  }
  public void set_PIVA_UTENTE(String PIVA_UTENTE) {
    this.PIVA_UTENTE = PIVA_UTENTE;
  }
  public cmg_prt_cmg_tal with_PIVA_UTENTE(String PIVA_UTENTE) {
    this.PIVA_UTENTE = PIVA_UTENTE;
    return this;
  }
  private String PIVA_DISTR;
  public String get_PIVA_DISTR() {
    return PIVA_DISTR;
  }
  public void set_PIVA_DISTR(String PIVA_DISTR) {
    this.PIVA_DISTR = PIVA_DISTR;
  }
  public cmg_prt_cmg_tal with_PIVA_DISTR(String PIVA_DISTR) {
    this.PIVA_DISTR = PIVA_DISTR;
    return this;
  }
  private String COD_PDR;
  public String get_COD_PDR() {
    return COD_PDR;
  }
  public void set_COD_PDR(String COD_PDR) {
    this.COD_PDR = COD_PDR;
  }
  public cmg_prt_cmg_tal with_COD_PDR(String COD_PDR) {
    this.COD_PDR = COD_PDR;
    return this;
  }
  private String MATR_MIS;
  public String get_MATR_MIS() {
    return MATR_MIS;
  }
  public void set_MATR_MIS(String MATR_MIS) {
    this.MATR_MIS = MATR_MIS;
  }
  public cmg_prt_cmg_tal with_MATR_MIS(String MATR_MIS) {
    this.MATR_MIS = MATR_MIS;
    return this;
  }
  private String MATR_CONV;
  public String get_MATR_CONV() {
    return MATR_CONV;
  }
  public void set_MATR_CONV(String MATR_CONV) {
    this.MATR_CONV = MATR_CONV;
  }
  public cmg_prt_cmg_tal with_MATR_CONV(String MATR_CONV) {
    this.MATR_CONV = MATR_CONV;
    return this;
  }
  private String DATA_COM_AUTOLET_CF;
  public String get_DATA_COM_AUTOLET_CF() {
    return DATA_COM_AUTOLET_CF;
  }
  public void set_DATA_COM_AUTOLET_CF(String DATA_COM_AUTOLET_CF) {
    this.DATA_COM_AUTOLET_CF = DATA_COM_AUTOLET_CF;
  }
  public cmg_prt_cmg_tal with_DATA_COM_AUTOLET_CF(String DATA_COM_AUTOLET_CF) {
    this.DATA_COM_AUTOLET_CF = DATA_COM_AUTOLET_CF;
    return this;
  }
  private String LET_TOT_PREL;
  public String get_LET_TOT_PREL() {
    return LET_TOT_PREL;
  }
  public void set_LET_TOT_PREL(String LET_TOT_PREL) {
    this.LET_TOT_PREL = LET_TOT_PREL;
  }
  public cmg_prt_cmg_tal with_LET_TOT_PREL(String LET_TOT_PREL) {
    this.LET_TOT_PREL = LET_TOT_PREL;
    return this;
  }
  private String LET_TOT_CONV;
  public String get_LET_TOT_CONV() {
    return LET_TOT_CONV;
  }
  public void set_LET_TOT_CONV(String LET_TOT_CONV) {
    this.LET_TOT_CONV = LET_TOT_CONV;
  }
  public cmg_prt_cmg_tal with_LET_TOT_CONV(String LET_TOT_CONV) {
    this.LET_TOT_CONV = LET_TOT_CONV;
    return this;
  }
  private String ESITO_VAL;
  public String get_ESITO_VAL() {
    return ESITO_VAL;
  }
  public void set_ESITO_VAL(String ESITO_VAL) {
    this.ESITO_VAL = ESITO_VAL;
  }
  public cmg_prt_cmg_tal with_ESITO_VAL(String ESITO_VAL) {
    this.ESITO_VAL = ESITO_VAL;
    return this;
  }
  private String NOTE;
  public String get_NOTE() {
    return NOTE;
  }
  public void set_NOTE(String NOTE) {
    this.NOTE = NOTE;
  }
  public cmg_prt_cmg_tal with_NOTE(String NOTE) {
    this.NOTE = NOTE;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof cmg_prt_cmg_tal)) {
      return false;
    }
    cmg_prt_cmg_tal that = (cmg_prt_cmg_tal) o;
    boolean equal = true;
    equal = equal && (this.N_ID == null ? that.N_ID == null : this.N_ID.equals(that.N_ID));
    equal = equal && (this.N_ID_FILE == null ? that.N_ID_FILE == null : this.N_ID_FILE.equals(that.N_ID_FILE));
    equal = equal && (this.ANNOMESE_RIFERIMENTO == null ? that.ANNOMESE_RIFERIMENTO == null : this.ANNOMESE_RIFERIMENTO.equals(that.ANNOMESE_RIFERIMENTO));
    equal = equal && (this.DT_CARICAMENTO == null ? that.DT_CARICAMENTO == null : this.DT_CARICAMENTO.equals(that.DT_CARICAMENTO));
    equal = equal && (this.COD_SERVIZIO == null ? that.COD_SERVIZIO == null : this.COD_SERVIZIO.equals(that.COD_SERVIZIO));
    equal = equal && (this.COD_FLUSSO == null ? that.COD_FLUSSO == null : this.COD_FLUSSO.equals(that.COD_FLUSSO));
    equal = equal && (this.PIVA_UTENTE == null ? that.PIVA_UTENTE == null : this.PIVA_UTENTE.equals(that.PIVA_UTENTE));
    equal = equal && (this.PIVA_DISTR == null ? that.PIVA_DISTR == null : this.PIVA_DISTR.equals(that.PIVA_DISTR));
    equal = equal && (this.COD_PDR == null ? that.COD_PDR == null : this.COD_PDR.equals(that.COD_PDR));
    equal = equal && (this.MATR_MIS == null ? that.MATR_MIS == null : this.MATR_MIS.equals(that.MATR_MIS));
    equal = equal && (this.MATR_CONV == null ? that.MATR_CONV == null : this.MATR_CONV.equals(that.MATR_CONV));
    equal = equal && (this.DATA_COM_AUTOLET_CF == null ? that.DATA_COM_AUTOLET_CF == null : this.DATA_COM_AUTOLET_CF.equals(that.DATA_COM_AUTOLET_CF));
    equal = equal && (this.LET_TOT_PREL == null ? that.LET_TOT_PREL == null : this.LET_TOT_PREL.equals(that.LET_TOT_PREL));
    equal = equal && (this.LET_TOT_CONV == null ? that.LET_TOT_CONV == null : this.LET_TOT_CONV.equals(that.LET_TOT_CONV));
    equal = equal && (this.ESITO_VAL == null ? that.ESITO_VAL == null : this.ESITO_VAL.equals(that.ESITO_VAL));
    equal = equal && (this.NOTE == null ? that.NOTE == null : this.NOTE.equals(that.NOTE));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof cmg_prt_cmg_tal)) {
      return false;
    }
    cmg_prt_cmg_tal that = (cmg_prt_cmg_tal) o;
    boolean equal = true;
    equal = equal && (this.N_ID == null ? that.N_ID == null : this.N_ID.equals(that.N_ID));
    equal = equal && (this.N_ID_FILE == null ? that.N_ID_FILE == null : this.N_ID_FILE.equals(that.N_ID_FILE));
    equal = equal && (this.ANNOMESE_RIFERIMENTO == null ? that.ANNOMESE_RIFERIMENTO == null : this.ANNOMESE_RIFERIMENTO.equals(that.ANNOMESE_RIFERIMENTO));
    equal = equal && (this.DT_CARICAMENTO == null ? that.DT_CARICAMENTO == null : this.DT_CARICAMENTO.equals(that.DT_CARICAMENTO));
    equal = equal && (this.COD_SERVIZIO == null ? that.COD_SERVIZIO == null : this.COD_SERVIZIO.equals(that.COD_SERVIZIO));
    equal = equal && (this.COD_FLUSSO == null ? that.COD_FLUSSO == null : this.COD_FLUSSO.equals(that.COD_FLUSSO));
    equal = equal && (this.PIVA_UTENTE == null ? that.PIVA_UTENTE == null : this.PIVA_UTENTE.equals(that.PIVA_UTENTE));
    equal = equal && (this.PIVA_DISTR == null ? that.PIVA_DISTR == null : this.PIVA_DISTR.equals(that.PIVA_DISTR));
    equal = equal && (this.COD_PDR == null ? that.COD_PDR == null : this.COD_PDR.equals(that.COD_PDR));
    equal = equal && (this.MATR_MIS == null ? that.MATR_MIS == null : this.MATR_MIS.equals(that.MATR_MIS));
    equal = equal && (this.MATR_CONV == null ? that.MATR_CONV == null : this.MATR_CONV.equals(that.MATR_CONV));
    equal = equal && (this.DATA_COM_AUTOLET_CF == null ? that.DATA_COM_AUTOLET_CF == null : this.DATA_COM_AUTOLET_CF.equals(that.DATA_COM_AUTOLET_CF));
    equal = equal && (this.LET_TOT_PREL == null ? that.LET_TOT_PREL == null : this.LET_TOT_PREL.equals(that.LET_TOT_PREL));
    equal = equal && (this.LET_TOT_CONV == null ? that.LET_TOT_CONV == null : this.LET_TOT_CONV.equals(that.LET_TOT_CONV));
    equal = equal && (this.ESITO_VAL == null ? that.ESITO_VAL == null : this.ESITO_VAL.equals(that.ESITO_VAL));
    equal = equal && (this.NOTE == null ? that.NOTE == null : this.NOTE.equals(that.NOTE));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_FILE = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.ANNOMESE_RIFERIMENTO = JdbcWritableBridge.readString(3, __dbResults);
    this.DT_CARICAMENTO = JdbcWritableBridge.readString(4, __dbResults);
    this.COD_SERVIZIO = JdbcWritableBridge.readString(5, __dbResults);
    this.COD_FLUSSO = JdbcWritableBridge.readString(6, __dbResults);
    this.PIVA_UTENTE = JdbcWritableBridge.readString(7, __dbResults);
    this.PIVA_DISTR = JdbcWritableBridge.readString(8, __dbResults);
    this.COD_PDR = JdbcWritableBridge.readString(9, __dbResults);
    this.MATR_MIS = JdbcWritableBridge.readString(10, __dbResults);
    this.MATR_CONV = JdbcWritableBridge.readString(11, __dbResults);
    this.DATA_COM_AUTOLET_CF = JdbcWritableBridge.readString(12, __dbResults);
    this.LET_TOT_PREL = JdbcWritableBridge.readString(13, __dbResults);
    this.LET_TOT_CONV = JdbcWritableBridge.readString(14, __dbResults);
    this.ESITO_VAL = JdbcWritableBridge.readString(15, __dbResults);
    this.NOTE = JdbcWritableBridge.readString(16, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_FILE = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.ANNOMESE_RIFERIMENTO = JdbcWritableBridge.readString(3, __dbResults);
    this.DT_CARICAMENTO = JdbcWritableBridge.readString(4, __dbResults);
    this.COD_SERVIZIO = JdbcWritableBridge.readString(5, __dbResults);
    this.COD_FLUSSO = JdbcWritableBridge.readString(6, __dbResults);
    this.PIVA_UTENTE = JdbcWritableBridge.readString(7, __dbResults);
    this.PIVA_DISTR = JdbcWritableBridge.readString(8, __dbResults);
    this.COD_PDR = JdbcWritableBridge.readString(9, __dbResults);
    this.MATR_MIS = JdbcWritableBridge.readString(10, __dbResults);
    this.MATR_CONV = JdbcWritableBridge.readString(11, __dbResults);
    this.DATA_COM_AUTOLET_CF = JdbcWritableBridge.readString(12, __dbResults);
    this.LET_TOT_PREL = JdbcWritableBridge.readString(13, __dbResults);
    this.LET_TOT_CONV = JdbcWritableBridge.readString(14, __dbResults);
    this.ESITO_VAL = JdbcWritableBridge.readString(15, __dbResults);
    this.NOTE = JdbcWritableBridge.readString(16, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_FILE, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(ANNOMESE_RIFERIMENTO, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DT_CARICAMENTO, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(COD_SERVIZIO, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COD_FLUSSO, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PIVA_UTENTE, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PIVA_DISTR, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COD_PDR, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(MATR_MIS, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(MATR_CONV, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATA_COM_AUTOLET_CF, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(LET_TOT_PREL, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(LET_TOT_CONV, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ESITO_VAL, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(NOTE, 16 + __off, 12, __dbStmt);
    return 16;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_FILE, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(ANNOMESE_RIFERIMENTO, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DT_CARICAMENTO, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(COD_SERVIZIO, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COD_FLUSSO, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PIVA_UTENTE, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PIVA_DISTR, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COD_PDR, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(MATR_MIS, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(MATR_CONV, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATA_COM_AUTOLET_CF, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(LET_TOT_PREL, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(LET_TOT_CONV, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ESITO_VAL, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(NOTE, 16 + __off, 12, __dbStmt);
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
        this.N_ID_FILE = null;
    } else {
    this.N_ID_FILE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ANNOMESE_RIFERIMENTO = null;
    } else {
    this.ANNOMESE_RIFERIMENTO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DT_CARICAMENTO = null;
    } else {
    this.DT_CARICAMENTO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.COD_SERVIZIO = null;
    } else {
    this.COD_SERVIZIO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.COD_FLUSSO = null;
    } else {
    this.COD_FLUSSO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PIVA_UTENTE = null;
    } else {
    this.PIVA_UTENTE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PIVA_DISTR = null;
    } else {
    this.PIVA_DISTR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.COD_PDR = null;
    } else {
    this.COD_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MATR_MIS = null;
    } else {
    this.MATR_MIS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MATR_CONV = null;
    } else {
    this.MATR_CONV = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATA_COM_AUTOLET_CF = null;
    } else {
    this.DATA_COM_AUTOLET_CF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.LET_TOT_PREL = null;
    } else {
    this.LET_TOT_PREL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.LET_TOT_CONV = null;
    } else {
    this.LET_TOT_CONV = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ESITO_VAL = null;
    } else {
    this.ESITO_VAL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.NOTE = null;
    } else {
    this.NOTE = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID, __dataOut);
    }
    if (null == this.N_ID_FILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FILE, __dataOut);
    }
    if (null == this.ANNOMESE_RIFERIMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ANNOMESE_RIFERIMENTO);
    }
    if (null == this.DT_CARICAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DT_CARICAMENTO);
    }
    if (null == this.COD_SERVIZIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_SERVIZIO);
    }
    if (null == this.COD_FLUSSO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_FLUSSO);
    }
    if (null == this.PIVA_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PIVA_UTENTE);
    }
    if (null == this.PIVA_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PIVA_DISTR);
    }
    if (null == this.COD_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_PDR);
    }
    if (null == this.MATR_MIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MATR_MIS);
    }
    if (null == this.MATR_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MATR_CONV);
    }
    if (null == this.DATA_COM_AUTOLET_CF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_COM_AUTOLET_CF);
    }
    if (null == this.LET_TOT_PREL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, LET_TOT_PREL);
    }
    if (null == this.LET_TOT_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, LET_TOT_CONV);
    }
    if (null == this.ESITO_VAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ESITO_VAL);
    }
    if (null == this.NOTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, NOTE);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID, __dataOut);
    }
    if (null == this.N_ID_FILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FILE, __dataOut);
    }
    if (null == this.ANNOMESE_RIFERIMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ANNOMESE_RIFERIMENTO);
    }
    if (null == this.DT_CARICAMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DT_CARICAMENTO);
    }
    if (null == this.COD_SERVIZIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_SERVIZIO);
    }
    if (null == this.COD_FLUSSO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_FLUSSO);
    }
    if (null == this.PIVA_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PIVA_UTENTE);
    }
    if (null == this.PIVA_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PIVA_DISTR);
    }
    if (null == this.COD_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_PDR);
    }
    if (null == this.MATR_MIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MATR_MIS);
    }
    if (null == this.MATR_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MATR_CONV);
    }
    if (null == this.DATA_COM_AUTOLET_CF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_COM_AUTOLET_CF);
    }
    if (null == this.LET_TOT_PREL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, LET_TOT_PREL);
    }
    if (null == this.LET_TOT_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, LET_TOT_CONV);
    }
    if (null == this.ESITO_VAL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ESITO_VAL);
    }
    if (null == this.NOTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, NOTE);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_FILE==null?"":N_ID_FILE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(ANNOMESE_RIFERIMENTO==null?"":ANNOMESE_RIFERIMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DT_CARICAMENTO==null?"":DT_CARICAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_SERVIZIO==null?"":COD_SERVIZIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_FLUSSO==null?"":COD_FLUSSO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PIVA_UTENTE==null?"":PIVA_UTENTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PIVA_DISTR==null?"":PIVA_DISTR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_PDR==null?"":COD_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(MATR_MIS==null?"":MATR_MIS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(MATR_CONV==null?"":MATR_CONV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATA_COM_AUTOLET_CF==null?"":DATA_COM_AUTOLET_CF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(LET_TOT_PREL==null?"":LET_TOT_PREL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(LET_TOT_CONV==null?"":LET_TOT_CONV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(ESITO_VAL==null?"":ESITO_VAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(NOTE==null?"":NOTE, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID==null?"":N_ID.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_FILE==null?"":N_ID_FILE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(ANNOMESE_RIFERIMENTO==null?"":ANNOMESE_RIFERIMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DT_CARICAMENTO==null?"":DT_CARICAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_SERVIZIO==null?"":COD_SERVIZIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_FLUSSO==null?"":COD_FLUSSO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PIVA_UTENTE==null?"":PIVA_UTENTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PIVA_DISTR==null?"":PIVA_DISTR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_PDR==null?"":COD_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(MATR_MIS==null?"":MATR_MIS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(MATR_CONV==null?"":MATR_CONV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATA_COM_AUTOLET_CF==null?"":DATA_COM_AUTOLET_CF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(LET_TOT_PREL==null?"":LET_TOT_PREL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(LET_TOT_CONV==null?"":LET_TOT_CONV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(ESITO_VAL==null?"":ESITO_VAL, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(NOTE==null?"":NOTE, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_FILE = null; } else {
      this.N_ID_FILE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ANNOMESE_RIFERIMENTO = null; } else {
      this.ANNOMESE_RIFERIMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DT_CARICAMENTO = null; } else {
      this.DT_CARICAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COD_SERVIZIO = null; } else {
      this.COD_SERVIZIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COD_FLUSSO = null; } else {
      this.COD_FLUSSO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PIVA_UTENTE = null; } else {
      this.PIVA_UTENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PIVA_DISTR = null; } else {
      this.PIVA_DISTR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COD_PDR = null; } else {
      this.COD_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MATR_MIS = null; } else {
      this.MATR_MIS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MATR_CONV = null; } else {
      this.MATR_CONV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_COM_AUTOLET_CF = null; } else {
      this.DATA_COM_AUTOLET_CF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.LET_TOT_PREL = null; } else {
      this.LET_TOT_PREL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.LET_TOT_CONV = null; } else {
      this.LET_TOT_CONV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ESITO_VAL = null; } else {
      this.ESITO_VAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.NOTE = null; } else {
      this.NOTE = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_FILE = null; } else {
      this.N_ID_FILE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ANNOMESE_RIFERIMENTO = null; } else {
      this.ANNOMESE_RIFERIMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DT_CARICAMENTO = null; } else {
      this.DT_CARICAMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COD_SERVIZIO = null; } else {
      this.COD_SERVIZIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COD_FLUSSO = null; } else {
      this.COD_FLUSSO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PIVA_UTENTE = null; } else {
      this.PIVA_UTENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PIVA_DISTR = null; } else {
      this.PIVA_DISTR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COD_PDR = null; } else {
      this.COD_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MATR_MIS = null; } else {
      this.MATR_MIS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MATR_CONV = null; } else {
      this.MATR_CONV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_COM_AUTOLET_CF = null; } else {
      this.DATA_COM_AUTOLET_CF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.LET_TOT_PREL = null; } else {
      this.LET_TOT_PREL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.LET_TOT_CONV = null; } else {
      this.LET_TOT_CONV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ESITO_VAL = null; } else {
      this.ESITO_VAL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.NOTE = null; } else {
      this.NOTE = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    cmg_prt_cmg_tal o = (cmg_prt_cmg_tal) super.clone();
    return o;
  }

  public void clone0(cmg_prt_cmg_tal o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID", this.N_ID);
    __sqoop$field_map.put("N_ID_FILE", this.N_ID_FILE);
    __sqoop$field_map.put("ANNOMESE_RIFERIMENTO", this.ANNOMESE_RIFERIMENTO);
    __sqoop$field_map.put("DT_CARICAMENTO", this.DT_CARICAMENTO);
    __sqoop$field_map.put("COD_SERVIZIO", this.COD_SERVIZIO);
    __sqoop$field_map.put("COD_FLUSSO", this.COD_FLUSSO);
    __sqoop$field_map.put("PIVA_UTENTE", this.PIVA_UTENTE);
    __sqoop$field_map.put("PIVA_DISTR", this.PIVA_DISTR);
    __sqoop$field_map.put("COD_PDR", this.COD_PDR);
    __sqoop$field_map.put("MATR_MIS", this.MATR_MIS);
    __sqoop$field_map.put("MATR_CONV", this.MATR_CONV);
    __sqoop$field_map.put("DATA_COM_AUTOLET_CF", this.DATA_COM_AUTOLET_CF);
    __sqoop$field_map.put("LET_TOT_PREL", this.LET_TOT_PREL);
    __sqoop$field_map.put("LET_TOT_CONV", this.LET_TOT_CONV);
    __sqoop$field_map.put("ESITO_VAL", this.ESITO_VAL);
    __sqoop$field_map.put("NOTE", this.NOTE);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID", this.N_ID);
    __sqoop$field_map.put("N_ID_FILE", this.N_ID_FILE);
    __sqoop$field_map.put("ANNOMESE_RIFERIMENTO", this.ANNOMESE_RIFERIMENTO);
    __sqoop$field_map.put("DT_CARICAMENTO", this.DT_CARICAMENTO);
    __sqoop$field_map.put("COD_SERVIZIO", this.COD_SERVIZIO);
    __sqoop$field_map.put("COD_FLUSSO", this.COD_FLUSSO);
    __sqoop$field_map.put("PIVA_UTENTE", this.PIVA_UTENTE);
    __sqoop$field_map.put("PIVA_DISTR", this.PIVA_DISTR);
    __sqoop$field_map.put("COD_PDR", this.COD_PDR);
    __sqoop$field_map.put("MATR_MIS", this.MATR_MIS);
    __sqoop$field_map.put("MATR_CONV", this.MATR_CONV);
    __sqoop$field_map.put("DATA_COM_AUTOLET_CF", this.DATA_COM_AUTOLET_CF);
    __sqoop$field_map.put("LET_TOT_PREL", this.LET_TOT_PREL);
    __sqoop$field_map.put("LET_TOT_CONV", this.LET_TOT_CONV);
    __sqoop$field_map.put("ESITO_VAL", this.ESITO_VAL);
    __sqoop$field_map.put("NOTE", this.NOTE);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
