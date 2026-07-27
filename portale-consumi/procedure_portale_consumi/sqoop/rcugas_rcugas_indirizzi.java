// ORM class for table 'rcugas.rcugas_indirizzi'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 11:59:52 CEST 2019
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

public class rcugas_rcugas_indirizzi extends SqoopRecord  implements DBWritable, Writable {
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
    setters.put("T_TOPONIMO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TOPONIMO = (String)value;
      }
    });
    setters.put("T_NOMESTRADA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_NOMESTRADA = (String)value;
      }
    });
    setters.put("T_CIVICO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CIVICO = (String)value;
      }
    });
    setters.put("T_COMUNE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COMUNE = (String)value;
      }
    });
    setters.put("T_COMUNE_ISTAT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COMUNE_ISTAT = (String)value;
      }
    });
    setters.put("T_PROVINCIA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PROVINCIA = (String)value;
      }
    });
    setters.put("T_NAZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_NAZIONE = (String)value;
      }
    });
    setters.put("T_INDIRIZZO_COMPLETO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_INDIRIZZO_COMPLETO = (String)value;
      }
    });
    setters.put("T_PRESSO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PRESSO = (String)value;
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
    setters.put("T_CAP", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CAP = (String)value;
      }
    });
  }
  public rcugas_rcugas_indirizzi() {
    init0();
  }
  private java.math.BigDecimal N_ID;
  public java.math.BigDecimal get_N_ID() {
    return N_ID;
  }
  public void set_N_ID(java.math.BigDecimal N_ID) {
    this.N_ID = N_ID;
  }
  public rcugas_rcugas_indirizzi with_N_ID(java.math.BigDecimal N_ID) {
    this.N_ID = N_ID;
    return this;
  }
  private String T_TOPONIMO;
  public String get_T_TOPONIMO() {
    return T_TOPONIMO;
  }
  public void set_T_TOPONIMO(String T_TOPONIMO) {
    this.T_TOPONIMO = T_TOPONIMO;
  }
  public rcugas_rcugas_indirizzi with_T_TOPONIMO(String T_TOPONIMO) {
    this.T_TOPONIMO = T_TOPONIMO;
    return this;
  }
  private String T_NOMESTRADA;
  public String get_T_NOMESTRADA() {
    return T_NOMESTRADA;
  }
  public void set_T_NOMESTRADA(String T_NOMESTRADA) {
    this.T_NOMESTRADA = T_NOMESTRADA;
  }
  public rcugas_rcugas_indirizzi with_T_NOMESTRADA(String T_NOMESTRADA) {
    this.T_NOMESTRADA = T_NOMESTRADA;
    return this;
  }
  private String T_CIVICO;
  public String get_T_CIVICO() {
    return T_CIVICO;
  }
  public void set_T_CIVICO(String T_CIVICO) {
    this.T_CIVICO = T_CIVICO;
  }
  public rcugas_rcugas_indirizzi with_T_CIVICO(String T_CIVICO) {
    this.T_CIVICO = T_CIVICO;
    return this;
  }
  private String T_COMUNE;
  public String get_T_COMUNE() {
    return T_COMUNE;
  }
  public void set_T_COMUNE(String T_COMUNE) {
    this.T_COMUNE = T_COMUNE;
  }
  public rcugas_rcugas_indirizzi with_T_COMUNE(String T_COMUNE) {
    this.T_COMUNE = T_COMUNE;
    return this;
  }
  private String T_COMUNE_ISTAT;
  public String get_T_COMUNE_ISTAT() {
    return T_COMUNE_ISTAT;
  }
  public void set_T_COMUNE_ISTAT(String T_COMUNE_ISTAT) {
    this.T_COMUNE_ISTAT = T_COMUNE_ISTAT;
  }
  public rcugas_rcugas_indirizzi with_T_COMUNE_ISTAT(String T_COMUNE_ISTAT) {
    this.T_COMUNE_ISTAT = T_COMUNE_ISTAT;
    return this;
  }
  private String T_PROVINCIA;
  public String get_T_PROVINCIA() {
    return T_PROVINCIA;
  }
  public void set_T_PROVINCIA(String T_PROVINCIA) {
    this.T_PROVINCIA = T_PROVINCIA;
  }
  public rcugas_rcugas_indirizzi with_T_PROVINCIA(String T_PROVINCIA) {
    this.T_PROVINCIA = T_PROVINCIA;
    return this;
  }
  private String T_NAZIONE;
  public String get_T_NAZIONE() {
    return T_NAZIONE;
  }
  public void set_T_NAZIONE(String T_NAZIONE) {
    this.T_NAZIONE = T_NAZIONE;
  }
  public rcugas_rcugas_indirizzi with_T_NAZIONE(String T_NAZIONE) {
    this.T_NAZIONE = T_NAZIONE;
    return this;
  }
  private String T_INDIRIZZO_COMPLETO;
  public String get_T_INDIRIZZO_COMPLETO() {
    return T_INDIRIZZO_COMPLETO;
  }
  public void set_T_INDIRIZZO_COMPLETO(String T_INDIRIZZO_COMPLETO) {
    this.T_INDIRIZZO_COMPLETO = T_INDIRIZZO_COMPLETO;
  }
  public rcugas_rcugas_indirizzi with_T_INDIRIZZO_COMPLETO(String T_INDIRIZZO_COMPLETO) {
    this.T_INDIRIZZO_COMPLETO = T_INDIRIZZO_COMPLETO;
    return this;
  }
  private String T_PRESSO;
  public String get_T_PRESSO() {
    return T_PRESSO;
  }
  public void set_T_PRESSO(String T_PRESSO) {
    this.T_PRESSO = T_PRESSO;
  }
  public rcugas_rcugas_indirizzi with_T_PRESSO(String T_PRESSO) {
    this.T_PRESSO = T_PRESSO;
    return this;
  }
  private String D_AGGIORNAMENTO;
  public String get_D_AGGIORNAMENTO() {
    return D_AGGIORNAMENTO;
  }
  public void set_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
    this.D_AGGIORNAMENTO = D_AGGIORNAMENTO;
  }
  public rcugas_rcugas_indirizzi with_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
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
  public rcugas_rcugas_indirizzi with_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
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
  public rcugas_rcugas_indirizzi with_N_ID_S_PREC(java.math.BigDecimal N_ID_S_PREC) {
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
  public rcugas_rcugas_indirizzi with_D_DATA_RIF(String D_DATA_RIF) {
    this.D_DATA_RIF = D_DATA_RIF;
    return this;
  }
  private String T_CAP;
  public String get_T_CAP() {
    return T_CAP;
  }
  public void set_T_CAP(String T_CAP) {
    this.T_CAP = T_CAP;
  }
  public rcugas_rcugas_indirizzi with_T_CAP(String T_CAP) {
    this.T_CAP = T_CAP;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcugas_rcugas_indirizzi)) {
      return false;
    }
    rcugas_rcugas_indirizzi that = (rcugas_rcugas_indirizzi) o;
    boolean equal = true;
    equal = equal && (this.N_ID == null ? that.N_ID == null : this.N_ID.equals(that.N_ID));
    equal = equal && (this.T_TOPONIMO == null ? that.T_TOPONIMO == null : this.T_TOPONIMO.equals(that.T_TOPONIMO));
    equal = equal && (this.T_NOMESTRADA == null ? that.T_NOMESTRADA == null : this.T_NOMESTRADA.equals(that.T_NOMESTRADA));
    equal = equal && (this.T_CIVICO == null ? that.T_CIVICO == null : this.T_CIVICO.equals(that.T_CIVICO));
    equal = equal && (this.T_COMUNE == null ? that.T_COMUNE == null : this.T_COMUNE.equals(that.T_COMUNE));
    equal = equal && (this.T_COMUNE_ISTAT == null ? that.T_COMUNE_ISTAT == null : this.T_COMUNE_ISTAT.equals(that.T_COMUNE_ISTAT));
    equal = equal && (this.T_PROVINCIA == null ? that.T_PROVINCIA == null : this.T_PROVINCIA.equals(that.T_PROVINCIA));
    equal = equal && (this.T_NAZIONE == null ? that.T_NAZIONE == null : this.T_NAZIONE.equals(that.T_NAZIONE));
    equal = equal && (this.T_INDIRIZZO_COMPLETO == null ? that.T_INDIRIZZO_COMPLETO == null : this.T_INDIRIZZO_COMPLETO.equals(that.T_INDIRIZZO_COMPLETO));
    equal = equal && (this.T_PRESSO == null ? that.T_PRESSO == null : this.T_PRESSO.equals(that.T_PRESSO));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.D_DATA_RIF == null ? that.D_DATA_RIF == null : this.D_DATA_RIF.equals(that.D_DATA_RIF));
    equal = equal && (this.T_CAP == null ? that.T_CAP == null : this.T_CAP.equals(that.T_CAP));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcugas_rcugas_indirizzi)) {
      return false;
    }
    rcugas_rcugas_indirizzi that = (rcugas_rcugas_indirizzi) o;
    boolean equal = true;
    equal = equal && (this.N_ID == null ? that.N_ID == null : this.N_ID.equals(that.N_ID));
    equal = equal && (this.T_TOPONIMO == null ? that.T_TOPONIMO == null : this.T_TOPONIMO.equals(that.T_TOPONIMO));
    equal = equal && (this.T_NOMESTRADA == null ? that.T_NOMESTRADA == null : this.T_NOMESTRADA.equals(that.T_NOMESTRADA));
    equal = equal && (this.T_CIVICO == null ? that.T_CIVICO == null : this.T_CIVICO.equals(that.T_CIVICO));
    equal = equal && (this.T_COMUNE == null ? that.T_COMUNE == null : this.T_COMUNE.equals(that.T_COMUNE));
    equal = equal && (this.T_COMUNE_ISTAT == null ? that.T_COMUNE_ISTAT == null : this.T_COMUNE_ISTAT.equals(that.T_COMUNE_ISTAT));
    equal = equal && (this.T_PROVINCIA == null ? that.T_PROVINCIA == null : this.T_PROVINCIA.equals(that.T_PROVINCIA));
    equal = equal && (this.T_NAZIONE == null ? that.T_NAZIONE == null : this.T_NAZIONE.equals(that.T_NAZIONE));
    equal = equal && (this.T_INDIRIZZO_COMPLETO == null ? that.T_INDIRIZZO_COMPLETO == null : this.T_INDIRIZZO_COMPLETO.equals(that.T_INDIRIZZO_COMPLETO));
    equal = equal && (this.T_PRESSO == null ? that.T_PRESSO == null : this.T_PRESSO.equals(that.T_PRESSO));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.D_DATA_RIF == null ? that.D_DATA_RIF == null : this.D_DATA_RIF.equals(that.D_DATA_RIF));
    equal = equal && (this.T_CAP == null ? that.T_CAP == null : this.T_CAP.equals(that.T_CAP));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.T_TOPONIMO = JdbcWritableBridge.readString(2, __dbResults);
    this.T_NOMESTRADA = JdbcWritableBridge.readString(3, __dbResults);
    this.T_CIVICO = JdbcWritableBridge.readString(4, __dbResults);
    this.T_COMUNE = JdbcWritableBridge.readString(5, __dbResults);
    this.T_COMUNE_ISTAT = JdbcWritableBridge.readString(6, __dbResults);
    this.T_PROVINCIA = JdbcWritableBridge.readString(7, __dbResults);
    this.T_NAZIONE = JdbcWritableBridge.readString(8, __dbResults);
    this.T_INDIRIZZO_COMPLETO = JdbcWritableBridge.readString(9, __dbResults);
    this.T_PRESSO = JdbcWritableBridge.readString(10, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(11, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.D_DATA_RIF = JdbcWritableBridge.readString(14, __dbResults);
    this.T_CAP = JdbcWritableBridge.readString(15, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.T_TOPONIMO = JdbcWritableBridge.readString(2, __dbResults);
    this.T_NOMESTRADA = JdbcWritableBridge.readString(3, __dbResults);
    this.T_CIVICO = JdbcWritableBridge.readString(4, __dbResults);
    this.T_COMUNE = JdbcWritableBridge.readString(5, __dbResults);
    this.T_COMUNE_ISTAT = JdbcWritableBridge.readString(6, __dbResults);
    this.T_PROVINCIA = JdbcWritableBridge.readString(7, __dbResults);
    this.T_NAZIONE = JdbcWritableBridge.readString(8, __dbResults);
    this.T_INDIRIZZO_COMPLETO = JdbcWritableBridge.readString(9, __dbResults);
    this.T_PRESSO = JdbcWritableBridge.readString(10, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(11, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.D_DATA_RIF = JdbcWritableBridge.readString(14, __dbResults);
    this.T_CAP = JdbcWritableBridge.readString(15, __dbResults);
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
    JdbcWritableBridge.writeString(T_TOPONIMO, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOMESTRADA, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CIVICO, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNE, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNE_ISTAT, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PROVINCIA, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NAZIONE, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_INDIRIZZO_COMPLETO, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PRESSO, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 11 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 12 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF, 14 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_CAP, 15 + __off, 12, __dbStmt);
    return 15;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_TOPONIMO, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOMESTRADA, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CIVICO, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNE, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNE_ISTAT, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PROVINCIA, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NAZIONE, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_INDIRIZZO_COMPLETO, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PRESSO, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 11 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 12 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF, 14 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_CAP, 15 + __off, 12, __dbStmt);
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
        this.T_TOPONIMO = null;
    } else {
    this.T_TOPONIMO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_NOMESTRADA = null;
    } else {
    this.T_NOMESTRADA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CIVICO = null;
    } else {
    this.T_CIVICO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COMUNE = null;
    } else {
    this.T_COMUNE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COMUNE_ISTAT = null;
    } else {
    this.T_COMUNE_ISTAT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PROVINCIA = null;
    } else {
    this.T_PROVINCIA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_NAZIONE = null;
    } else {
    this.T_NAZIONE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_INDIRIZZO_COMPLETO = null;
    } else {
    this.T_INDIRIZZO_COMPLETO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PRESSO = null;
    } else {
    this.T_PRESSO = Text.readString(__dataIn);
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
    if (__dataIn.readBoolean()) { 
        this.T_CAP = null;
    } else {
    this.T_CAP = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID, __dataOut);
    }
    if (null == this.T_TOPONIMO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TOPONIMO);
    }
    if (null == this.T_NOMESTRADA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOMESTRADA);
    }
    if (null == this.T_CIVICO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CIVICO);
    }
    if (null == this.T_COMUNE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNE);
    }
    if (null == this.T_COMUNE_ISTAT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNE_ISTAT);
    }
    if (null == this.T_PROVINCIA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PROVINCIA);
    }
    if (null == this.T_NAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NAZIONE);
    }
    if (null == this.T_INDIRIZZO_COMPLETO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_INDIRIZZO_COMPLETO);
    }
    if (null == this.T_PRESSO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PRESSO);
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
    if (null == this.T_CAP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CAP);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID, __dataOut);
    }
    if (null == this.T_TOPONIMO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TOPONIMO);
    }
    if (null == this.T_NOMESTRADA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOMESTRADA);
    }
    if (null == this.T_CIVICO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CIVICO);
    }
    if (null == this.T_COMUNE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNE);
    }
    if (null == this.T_COMUNE_ISTAT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNE_ISTAT);
    }
    if (null == this.T_PROVINCIA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PROVINCIA);
    }
    if (null == this.T_NAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NAZIONE);
    }
    if (null == this.T_INDIRIZZO_COMPLETO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_INDIRIZZO_COMPLETO);
    }
    if (null == this.T_PRESSO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PRESSO);
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
    if (null == this.T_CAP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CAP);
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
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TOPONIMO==null?"":T_TOPONIMO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOMESTRADA==null?"":T_NOMESTRADA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CIVICO==null?"":T_CIVICO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNE==null?"":T_COMUNE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNE_ISTAT==null?"":T_COMUNE_ISTAT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PROVINCIA==null?"":T_PROVINCIA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NAZIONE==null?"":T_NAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_INDIRIZZO_COMPLETO==null?"":T_INDIRIZZO_COMPLETO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PRESSO==null?"":T_PRESSO, " ", delimiters));
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
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CAP==null?"":T_CAP, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID==null?"":N_ID.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TOPONIMO==null?"":T_TOPONIMO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOMESTRADA==null?"":T_NOMESTRADA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CIVICO==null?"":T_CIVICO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNE==null?"":T_COMUNE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNE_ISTAT==null?"":T_COMUNE_ISTAT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PROVINCIA==null?"":T_PROVINCIA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NAZIONE==null?"":T_NAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_INDIRIZZO_COMPLETO==null?"":T_INDIRIZZO_COMPLETO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PRESSO==null?"":T_PRESSO, " ", delimiters));
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
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CAP==null?"":T_CAP, " ", delimiters));
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
    if (__cur_str.equals("null")) { this.T_TOPONIMO = null; } else {
      this.T_TOPONIMO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOMESTRADA = null; } else {
      this.T_NOMESTRADA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CIVICO = null; } else {
      this.T_CIVICO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNE = null; } else {
      this.T_COMUNE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNE_ISTAT = null; } else {
      this.T_COMUNE_ISTAT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PROVINCIA = null; } else {
      this.T_PROVINCIA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NAZIONE = null; } else {
      this.T_NAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_INDIRIZZO_COMPLETO = null; } else {
      this.T_INDIRIZZO_COMPLETO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PRESSO = null; } else {
      this.T_PRESSO = __cur_str;
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

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CAP = null; } else {
      this.T_CAP = __cur_str;
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
    if (__cur_str.equals("null")) { this.T_TOPONIMO = null; } else {
      this.T_TOPONIMO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOMESTRADA = null; } else {
      this.T_NOMESTRADA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CIVICO = null; } else {
      this.T_CIVICO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNE = null; } else {
      this.T_COMUNE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNE_ISTAT = null; } else {
      this.T_COMUNE_ISTAT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PROVINCIA = null; } else {
      this.T_PROVINCIA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NAZIONE = null; } else {
      this.T_NAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_INDIRIZZO_COMPLETO = null; } else {
      this.T_INDIRIZZO_COMPLETO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PRESSO = null; } else {
      this.T_PRESSO = __cur_str;
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

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CAP = null; } else {
      this.T_CAP = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    rcugas_rcugas_indirizzi o = (rcugas_rcugas_indirizzi) super.clone();
    return o;
  }

  public void clone0(rcugas_rcugas_indirizzi o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID", this.N_ID);
    __sqoop$field_map.put("T_TOPONIMO", this.T_TOPONIMO);
    __sqoop$field_map.put("T_NOMESTRADA", this.T_NOMESTRADA);
    __sqoop$field_map.put("T_CIVICO", this.T_CIVICO);
    __sqoop$field_map.put("T_COMUNE", this.T_COMUNE);
    __sqoop$field_map.put("T_COMUNE_ISTAT", this.T_COMUNE_ISTAT);
    __sqoop$field_map.put("T_PROVINCIA", this.T_PROVINCIA);
    __sqoop$field_map.put("T_NAZIONE", this.T_NAZIONE);
    __sqoop$field_map.put("T_INDIRIZZO_COMPLETO", this.T_INDIRIZZO_COMPLETO);
    __sqoop$field_map.put("T_PRESSO", this.T_PRESSO);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("D_DATA_RIF", this.D_DATA_RIF);
    __sqoop$field_map.put("T_CAP", this.T_CAP);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID", this.N_ID);
    __sqoop$field_map.put("T_TOPONIMO", this.T_TOPONIMO);
    __sqoop$field_map.put("T_NOMESTRADA", this.T_NOMESTRADA);
    __sqoop$field_map.put("T_CIVICO", this.T_CIVICO);
    __sqoop$field_map.put("T_COMUNE", this.T_COMUNE);
    __sqoop$field_map.put("T_COMUNE_ISTAT", this.T_COMUNE_ISTAT);
    __sqoop$field_map.put("T_PROVINCIA", this.T_PROVINCIA);
    __sqoop$field_map.put("T_NAZIONE", this.T_NAZIONE);
    __sqoop$field_map.put("T_INDIRIZZO_COMPLETO", this.T_INDIRIZZO_COMPLETO);
    __sqoop$field_map.put("T_PRESSO", this.T_PRESSO);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("D_DATA_RIF", this.D_DATA_RIF);
    __sqoop$field_map.put("T_CAP", this.T_CAP);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
