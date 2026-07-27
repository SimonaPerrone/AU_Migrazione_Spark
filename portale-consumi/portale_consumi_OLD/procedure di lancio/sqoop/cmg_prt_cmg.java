// ORM class for table 'cmg.prt_cmg'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 08:04:07 CEST 2019
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

public class cmg_prt_cmg extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_CMG", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_CMG = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_PRATICA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_PRATICA = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_TIPO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO = (String)value;
      }
    });
    setters.put("T_STATO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_STATO = (String)value;
      }
    });
    setters.put("D_DATA_RICH", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_RICH = (String)value;
      }
    });
    setters.put("N_ID_DESTINATARIO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_DESTINATARIO = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_RUOLO_DESTINATARIO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_RUOLO_DESTINATARIO = (String)value;
      }
    });
    setters.put("T_PIVA_DESTINATARIO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PIVA_DESTINATARIO = (String)value;
      }
    });
    setters.put("T_RAG_SOC_DESTINATARIO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_RAG_SOC_DESTINATARIO = (String)value;
      }
    });
    setters.put("T_ANNO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ANNO = (String)value;
      }
    });
    setters.put("T_MESE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MESE = (String)value;
      }
    });
    setters.put("B_AMMISSIBILE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_AMMISSIBILE = (String)value;
      }
    });
    setters.put("T_COD_CAUSALE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COD_CAUSALE = (String)value;
      }
    });
    setters.put("T_MOTIVAZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MOTIVAZIONE = (String)value;
      }
    });
    setters.put("D_DATA_STATO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_STATO = (String)value;
      }
    });
    setters.put("D_DATA_DOWNLOAD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_DOWNLOAD = (String)value;
      }
    });
  }
  public cmg_prt_cmg() {
    init0();
  }
  private java.math.BigDecimal N_ID_CMG;
  public java.math.BigDecimal get_N_ID_CMG() {
    return N_ID_CMG;
  }
  public void set_N_ID_CMG(java.math.BigDecimal N_ID_CMG) {
    this.N_ID_CMG = N_ID_CMG;
  }
  public cmg_prt_cmg with_N_ID_CMG(java.math.BigDecimal N_ID_CMG) {
    this.N_ID_CMG = N_ID_CMG;
    return this;
  }
  private java.math.BigDecimal N_ID_PRATICA;
  public java.math.BigDecimal get_N_ID_PRATICA() {
    return N_ID_PRATICA;
  }
  public void set_N_ID_PRATICA(java.math.BigDecimal N_ID_PRATICA) {
    this.N_ID_PRATICA = N_ID_PRATICA;
  }
  public cmg_prt_cmg with_N_ID_PRATICA(java.math.BigDecimal N_ID_PRATICA) {
    this.N_ID_PRATICA = N_ID_PRATICA;
    return this;
  }
  private String T_TIPO;
  public String get_T_TIPO() {
    return T_TIPO;
  }
  public void set_T_TIPO(String T_TIPO) {
    this.T_TIPO = T_TIPO;
  }
  public cmg_prt_cmg with_T_TIPO(String T_TIPO) {
    this.T_TIPO = T_TIPO;
    return this;
  }
  private String T_STATO;
  public String get_T_STATO() {
    return T_STATO;
  }
  public void set_T_STATO(String T_STATO) {
    this.T_STATO = T_STATO;
  }
  public cmg_prt_cmg with_T_STATO(String T_STATO) {
    this.T_STATO = T_STATO;
    return this;
  }
  private String D_DATA_RICH;
  public String get_D_DATA_RICH() {
    return D_DATA_RICH;
  }
  public void set_D_DATA_RICH(String D_DATA_RICH) {
    this.D_DATA_RICH = D_DATA_RICH;
  }
  public cmg_prt_cmg with_D_DATA_RICH(String D_DATA_RICH) {
    this.D_DATA_RICH = D_DATA_RICH;
    return this;
  }
  private java.math.BigDecimal N_ID_DESTINATARIO;
  public java.math.BigDecimal get_N_ID_DESTINATARIO() {
    return N_ID_DESTINATARIO;
  }
  public void set_N_ID_DESTINATARIO(java.math.BigDecimal N_ID_DESTINATARIO) {
    this.N_ID_DESTINATARIO = N_ID_DESTINATARIO;
  }
  public cmg_prt_cmg with_N_ID_DESTINATARIO(java.math.BigDecimal N_ID_DESTINATARIO) {
    this.N_ID_DESTINATARIO = N_ID_DESTINATARIO;
    return this;
  }
  private String T_RUOLO_DESTINATARIO;
  public String get_T_RUOLO_DESTINATARIO() {
    return T_RUOLO_DESTINATARIO;
  }
  public void set_T_RUOLO_DESTINATARIO(String T_RUOLO_DESTINATARIO) {
    this.T_RUOLO_DESTINATARIO = T_RUOLO_DESTINATARIO;
  }
  public cmg_prt_cmg with_T_RUOLO_DESTINATARIO(String T_RUOLO_DESTINATARIO) {
    this.T_RUOLO_DESTINATARIO = T_RUOLO_DESTINATARIO;
    return this;
  }
  private String T_PIVA_DESTINATARIO;
  public String get_T_PIVA_DESTINATARIO() {
    return T_PIVA_DESTINATARIO;
  }
  public void set_T_PIVA_DESTINATARIO(String T_PIVA_DESTINATARIO) {
    this.T_PIVA_DESTINATARIO = T_PIVA_DESTINATARIO;
  }
  public cmg_prt_cmg with_T_PIVA_DESTINATARIO(String T_PIVA_DESTINATARIO) {
    this.T_PIVA_DESTINATARIO = T_PIVA_DESTINATARIO;
    return this;
  }
  private String T_RAG_SOC_DESTINATARIO;
  public String get_T_RAG_SOC_DESTINATARIO() {
    return T_RAG_SOC_DESTINATARIO;
  }
  public void set_T_RAG_SOC_DESTINATARIO(String T_RAG_SOC_DESTINATARIO) {
    this.T_RAG_SOC_DESTINATARIO = T_RAG_SOC_DESTINATARIO;
  }
  public cmg_prt_cmg with_T_RAG_SOC_DESTINATARIO(String T_RAG_SOC_DESTINATARIO) {
    this.T_RAG_SOC_DESTINATARIO = T_RAG_SOC_DESTINATARIO;
    return this;
  }
  private String T_ANNO;
  public String get_T_ANNO() {
    return T_ANNO;
  }
  public void set_T_ANNO(String T_ANNO) {
    this.T_ANNO = T_ANNO;
  }
  public cmg_prt_cmg with_T_ANNO(String T_ANNO) {
    this.T_ANNO = T_ANNO;
    return this;
  }
  private String T_MESE;
  public String get_T_MESE() {
    return T_MESE;
  }
  public void set_T_MESE(String T_MESE) {
    this.T_MESE = T_MESE;
  }
  public cmg_prt_cmg with_T_MESE(String T_MESE) {
    this.T_MESE = T_MESE;
    return this;
  }
  private String B_AMMISSIBILE;
  public String get_B_AMMISSIBILE() {
    return B_AMMISSIBILE;
  }
  public void set_B_AMMISSIBILE(String B_AMMISSIBILE) {
    this.B_AMMISSIBILE = B_AMMISSIBILE;
  }
  public cmg_prt_cmg with_B_AMMISSIBILE(String B_AMMISSIBILE) {
    this.B_AMMISSIBILE = B_AMMISSIBILE;
    return this;
  }
  private String T_COD_CAUSALE;
  public String get_T_COD_CAUSALE() {
    return T_COD_CAUSALE;
  }
  public void set_T_COD_CAUSALE(String T_COD_CAUSALE) {
    this.T_COD_CAUSALE = T_COD_CAUSALE;
  }
  public cmg_prt_cmg with_T_COD_CAUSALE(String T_COD_CAUSALE) {
    this.T_COD_CAUSALE = T_COD_CAUSALE;
    return this;
  }
  private String T_MOTIVAZIONE;
  public String get_T_MOTIVAZIONE() {
    return T_MOTIVAZIONE;
  }
  public void set_T_MOTIVAZIONE(String T_MOTIVAZIONE) {
    this.T_MOTIVAZIONE = T_MOTIVAZIONE;
  }
  public cmg_prt_cmg with_T_MOTIVAZIONE(String T_MOTIVAZIONE) {
    this.T_MOTIVAZIONE = T_MOTIVAZIONE;
    return this;
  }
  private String D_DATA_STATO;
  public String get_D_DATA_STATO() {
    return D_DATA_STATO;
  }
  public void set_D_DATA_STATO(String D_DATA_STATO) {
    this.D_DATA_STATO = D_DATA_STATO;
  }
  public cmg_prt_cmg with_D_DATA_STATO(String D_DATA_STATO) {
    this.D_DATA_STATO = D_DATA_STATO;
    return this;
  }
  private String D_DATA_DOWNLOAD;
  public String get_D_DATA_DOWNLOAD() {
    return D_DATA_DOWNLOAD;
  }
  public void set_D_DATA_DOWNLOAD(String D_DATA_DOWNLOAD) {
    this.D_DATA_DOWNLOAD = D_DATA_DOWNLOAD;
  }
  public cmg_prt_cmg with_D_DATA_DOWNLOAD(String D_DATA_DOWNLOAD) {
    this.D_DATA_DOWNLOAD = D_DATA_DOWNLOAD;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof cmg_prt_cmg)) {
      return false;
    }
    cmg_prt_cmg that = (cmg_prt_cmg) o;
    boolean equal = true;
    equal = equal && (this.N_ID_CMG == null ? that.N_ID_CMG == null : this.N_ID_CMG.equals(that.N_ID_CMG));
    equal = equal && (this.N_ID_PRATICA == null ? that.N_ID_PRATICA == null : this.N_ID_PRATICA.equals(that.N_ID_PRATICA));
    equal = equal && (this.T_TIPO == null ? that.T_TIPO == null : this.T_TIPO.equals(that.T_TIPO));
    equal = equal && (this.T_STATO == null ? that.T_STATO == null : this.T_STATO.equals(that.T_STATO));
    equal = equal && (this.D_DATA_RICH == null ? that.D_DATA_RICH == null : this.D_DATA_RICH.equals(that.D_DATA_RICH));
    equal = equal && (this.N_ID_DESTINATARIO == null ? that.N_ID_DESTINATARIO == null : this.N_ID_DESTINATARIO.equals(that.N_ID_DESTINATARIO));
    equal = equal && (this.T_RUOLO_DESTINATARIO == null ? that.T_RUOLO_DESTINATARIO == null : this.T_RUOLO_DESTINATARIO.equals(that.T_RUOLO_DESTINATARIO));
    equal = equal && (this.T_PIVA_DESTINATARIO == null ? that.T_PIVA_DESTINATARIO == null : this.T_PIVA_DESTINATARIO.equals(that.T_PIVA_DESTINATARIO));
    equal = equal && (this.T_RAG_SOC_DESTINATARIO == null ? that.T_RAG_SOC_DESTINATARIO == null : this.T_RAG_SOC_DESTINATARIO.equals(that.T_RAG_SOC_DESTINATARIO));
    equal = equal && (this.T_ANNO == null ? that.T_ANNO == null : this.T_ANNO.equals(that.T_ANNO));
    equal = equal && (this.T_MESE == null ? that.T_MESE == null : this.T_MESE.equals(that.T_MESE));
    equal = equal && (this.B_AMMISSIBILE == null ? that.B_AMMISSIBILE == null : this.B_AMMISSIBILE.equals(that.B_AMMISSIBILE));
    equal = equal && (this.T_COD_CAUSALE == null ? that.T_COD_CAUSALE == null : this.T_COD_CAUSALE.equals(that.T_COD_CAUSALE));
    equal = equal && (this.T_MOTIVAZIONE == null ? that.T_MOTIVAZIONE == null : this.T_MOTIVAZIONE.equals(that.T_MOTIVAZIONE));
    equal = equal && (this.D_DATA_STATO == null ? that.D_DATA_STATO == null : this.D_DATA_STATO.equals(that.D_DATA_STATO));
    equal = equal && (this.D_DATA_DOWNLOAD == null ? that.D_DATA_DOWNLOAD == null : this.D_DATA_DOWNLOAD.equals(that.D_DATA_DOWNLOAD));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof cmg_prt_cmg)) {
      return false;
    }
    cmg_prt_cmg that = (cmg_prt_cmg) o;
    boolean equal = true;
    equal = equal && (this.N_ID_CMG == null ? that.N_ID_CMG == null : this.N_ID_CMG.equals(that.N_ID_CMG));
    equal = equal && (this.N_ID_PRATICA == null ? that.N_ID_PRATICA == null : this.N_ID_PRATICA.equals(that.N_ID_PRATICA));
    equal = equal && (this.T_TIPO == null ? that.T_TIPO == null : this.T_TIPO.equals(that.T_TIPO));
    equal = equal && (this.T_STATO == null ? that.T_STATO == null : this.T_STATO.equals(that.T_STATO));
    equal = equal && (this.D_DATA_RICH == null ? that.D_DATA_RICH == null : this.D_DATA_RICH.equals(that.D_DATA_RICH));
    equal = equal && (this.N_ID_DESTINATARIO == null ? that.N_ID_DESTINATARIO == null : this.N_ID_DESTINATARIO.equals(that.N_ID_DESTINATARIO));
    equal = equal && (this.T_RUOLO_DESTINATARIO == null ? that.T_RUOLO_DESTINATARIO == null : this.T_RUOLO_DESTINATARIO.equals(that.T_RUOLO_DESTINATARIO));
    equal = equal && (this.T_PIVA_DESTINATARIO == null ? that.T_PIVA_DESTINATARIO == null : this.T_PIVA_DESTINATARIO.equals(that.T_PIVA_DESTINATARIO));
    equal = equal && (this.T_RAG_SOC_DESTINATARIO == null ? that.T_RAG_SOC_DESTINATARIO == null : this.T_RAG_SOC_DESTINATARIO.equals(that.T_RAG_SOC_DESTINATARIO));
    equal = equal && (this.T_ANNO == null ? that.T_ANNO == null : this.T_ANNO.equals(that.T_ANNO));
    equal = equal && (this.T_MESE == null ? that.T_MESE == null : this.T_MESE.equals(that.T_MESE));
    equal = equal && (this.B_AMMISSIBILE == null ? that.B_AMMISSIBILE == null : this.B_AMMISSIBILE.equals(that.B_AMMISSIBILE));
    equal = equal && (this.T_COD_CAUSALE == null ? that.T_COD_CAUSALE == null : this.T_COD_CAUSALE.equals(that.T_COD_CAUSALE));
    equal = equal && (this.T_MOTIVAZIONE == null ? that.T_MOTIVAZIONE == null : this.T_MOTIVAZIONE.equals(that.T_MOTIVAZIONE));
    equal = equal && (this.D_DATA_STATO == null ? that.D_DATA_STATO == null : this.D_DATA_STATO.equals(that.D_DATA_STATO));
    equal = equal && (this.D_DATA_DOWNLOAD == null ? that.D_DATA_DOWNLOAD == null : this.D_DATA_DOWNLOAD.equals(that.D_DATA_DOWNLOAD));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_CMG = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_PRATICA = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_TIPO = JdbcWritableBridge.readString(3, __dbResults);
    this.T_STATO = JdbcWritableBridge.readString(4, __dbResults);
    this.D_DATA_RICH = JdbcWritableBridge.readString(5, __dbResults);
    this.N_ID_DESTINATARIO = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.T_RUOLO_DESTINATARIO = JdbcWritableBridge.readString(7, __dbResults);
    this.T_PIVA_DESTINATARIO = JdbcWritableBridge.readString(8, __dbResults);
    this.T_RAG_SOC_DESTINATARIO = JdbcWritableBridge.readString(9, __dbResults);
    this.T_ANNO = JdbcWritableBridge.readString(10, __dbResults);
    this.T_MESE = JdbcWritableBridge.readString(11, __dbResults);
    this.B_AMMISSIBILE = JdbcWritableBridge.readString(12, __dbResults);
    this.T_COD_CAUSALE = JdbcWritableBridge.readString(13, __dbResults);
    this.T_MOTIVAZIONE = JdbcWritableBridge.readString(14, __dbResults);
    this.D_DATA_STATO = JdbcWritableBridge.readString(15, __dbResults);
    this.D_DATA_DOWNLOAD = JdbcWritableBridge.readString(16, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_CMG = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_PRATICA = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_TIPO = JdbcWritableBridge.readString(3, __dbResults);
    this.T_STATO = JdbcWritableBridge.readString(4, __dbResults);
    this.D_DATA_RICH = JdbcWritableBridge.readString(5, __dbResults);
    this.N_ID_DESTINATARIO = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.T_RUOLO_DESTINATARIO = JdbcWritableBridge.readString(7, __dbResults);
    this.T_PIVA_DESTINATARIO = JdbcWritableBridge.readString(8, __dbResults);
    this.T_RAG_SOC_DESTINATARIO = JdbcWritableBridge.readString(9, __dbResults);
    this.T_ANNO = JdbcWritableBridge.readString(10, __dbResults);
    this.T_MESE = JdbcWritableBridge.readString(11, __dbResults);
    this.B_AMMISSIBILE = JdbcWritableBridge.readString(12, __dbResults);
    this.T_COD_CAUSALE = JdbcWritableBridge.readString(13, __dbResults);
    this.T_MOTIVAZIONE = JdbcWritableBridge.readString(14, __dbResults);
    this.D_DATA_STATO = JdbcWritableBridge.readString(15, __dbResults);
    this.D_DATA_DOWNLOAD = JdbcWritableBridge.readString(16, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_CMG, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PRATICA, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RICH, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DESTINATARIO, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_RUOLO_DESTINATARIO, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA_DESTINATARIO, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_RAG_SOC_DESTINATARIO, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MESE, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_AMMISSIBILE, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CAUSALE, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MOTIVAZIONE, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_STATO, 15 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_DOWNLOAD, 16 + __off, 93, __dbStmt);
    return 16;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_CMG, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PRATICA, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RICH, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DESTINATARIO, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_RUOLO_DESTINATARIO, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PIVA_DESTINATARIO, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_RAG_SOC_DESTINATARIO, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MESE, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_AMMISSIBILE, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CAUSALE, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MOTIVAZIONE, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_STATO, 15 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_DOWNLOAD, 16 + __off, 93, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_CMG = null;
    } else {
    this.N_ID_CMG = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_PRATICA = null;
    } else {
    this.N_ID_PRATICA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO = null;
    } else {
    this.T_TIPO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_STATO = null;
    } else {
    this.T_STATO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_RICH = null;
    } else {
    this.D_DATA_RICH = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_DESTINATARIO = null;
    } else {
    this.N_ID_DESTINATARIO = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_RUOLO_DESTINATARIO = null;
    } else {
    this.T_RUOLO_DESTINATARIO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PIVA_DESTINATARIO = null;
    } else {
    this.T_PIVA_DESTINATARIO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_RAG_SOC_DESTINATARIO = null;
    } else {
    this.T_RAG_SOC_DESTINATARIO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ANNO = null;
    } else {
    this.T_ANNO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MESE = null;
    } else {
    this.T_MESE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_AMMISSIBILE = null;
    } else {
    this.B_AMMISSIBILE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COD_CAUSALE = null;
    } else {
    this.T_COD_CAUSALE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MOTIVAZIONE = null;
    } else {
    this.T_MOTIVAZIONE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_STATO = null;
    } else {
    this.D_DATA_STATO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_DOWNLOAD = null;
    } else {
    this.D_DATA_DOWNLOAD = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_CMG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CMG, __dataOut);
    }
    if (null == this.N_ID_PRATICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PRATICA, __dataOut);
    }
    if (null == this.T_TIPO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO);
    }
    if (null == this.T_STATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_STATO);
    }
    if (null == this.D_DATA_RICH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RICH);
    }
    if (null == this.N_ID_DESTINATARIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DESTINATARIO, __dataOut);
    }
    if (null == this.T_RUOLO_DESTINATARIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RUOLO_DESTINATARIO);
    }
    if (null == this.T_PIVA_DESTINATARIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA_DESTINATARIO);
    }
    if (null == this.T_RAG_SOC_DESTINATARIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RAG_SOC_DESTINATARIO);
    }
    if (null == this.T_ANNO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO);
    }
    if (null == this.T_MESE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MESE);
    }
    if (null == this.B_AMMISSIBILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_AMMISSIBILE);
    }
    if (null == this.T_COD_CAUSALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_CAUSALE);
    }
    if (null == this.T_MOTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MOTIVAZIONE);
    }
    if (null == this.D_DATA_STATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_STATO);
    }
    if (null == this.D_DATA_DOWNLOAD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_DOWNLOAD);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_CMG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CMG, __dataOut);
    }
    if (null == this.N_ID_PRATICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PRATICA, __dataOut);
    }
    if (null == this.T_TIPO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO);
    }
    if (null == this.T_STATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_STATO);
    }
    if (null == this.D_DATA_RICH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RICH);
    }
    if (null == this.N_ID_DESTINATARIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DESTINATARIO, __dataOut);
    }
    if (null == this.T_RUOLO_DESTINATARIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RUOLO_DESTINATARIO);
    }
    if (null == this.T_PIVA_DESTINATARIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PIVA_DESTINATARIO);
    }
    if (null == this.T_RAG_SOC_DESTINATARIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RAG_SOC_DESTINATARIO);
    }
    if (null == this.T_ANNO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO);
    }
    if (null == this.T_MESE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MESE);
    }
    if (null == this.B_AMMISSIBILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_AMMISSIBILE);
    }
    if (null == this.T_COD_CAUSALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_CAUSALE);
    }
    if (null == this.T_MOTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MOTIVAZIONE);
    }
    if (null == this.D_DATA_STATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_STATO);
    }
    if (null == this.D_DATA_DOWNLOAD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_DOWNLOAD);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CMG==null?"":N_ID_CMG.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PRATICA==null?"":N_ID_PRATICA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO==null?"":T_TIPO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO==null?"":T_STATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RICH==null?"":D_DATA_RICH, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DESTINATARIO==null?"":N_ID_DESTINATARIO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RUOLO_DESTINATARIO==null?"":T_RUOLO_DESTINATARIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA_DESTINATARIO==null?"":T_PIVA_DESTINATARIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RAG_SOC_DESTINATARIO==null?"":T_RAG_SOC_DESTINATARIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO==null?"":T_ANNO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MESE==null?"":T_MESE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_AMMISSIBILE==null?"":B_AMMISSIBILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CAUSALE==null?"":T_COD_CAUSALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MOTIVAZIONE==null?"":T_MOTIVAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_STATO==null?"":D_DATA_STATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_DOWNLOAD==null?"":D_DATA_DOWNLOAD, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CMG==null?"":N_ID_CMG.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PRATICA==null?"":N_ID_PRATICA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO==null?"":T_TIPO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO==null?"":T_STATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RICH==null?"":D_DATA_RICH, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DESTINATARIO==null?"":N_ID_DESTINATARIO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RUOLO_DESTINATARIO==null?"":T_RUOLO_DESTINATARIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PIVA_DESTINATARIO==null?"":T_PIVA_DESTINATARIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RAG_SOC_DESTINATARIO==null?"":T_RAG_SOC_DESTINATARIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO==null?"":T_ANNO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MESE==null?"":T_MESE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_AMMISSIBILE==null?"":B_AMMISSIBILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CAUSALE==null?"":T_COD_CAUSALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MOTIVAZIONE==null?"":T_MOTIVAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_STATO==null?"":D_DATA_STATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_DOWNLOAD==null?"":D_DATA_DOWNLOAD, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CMG = null; } else {
      this.N_ID_CMG = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PRATICA = null; } else {
      this.N_ID_PRATICA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO = null; } else {
      this.T_TIPO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_STATO = null; } else {
      this.T_STATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RICH = null; } else {
      this.D_DATA_RICH = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DESTINATARIO = null; } else {
      this.N_ID_DESTINATARIO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RUOLO_DESTINATARIO = null; } else {
      this.T_RUOLO_DESTINATARIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA_DESTINATARIO = null; } else {
      this.T_PIVA_DESTINATARIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RAG_SOC_DESTINATARIO = null; } else {
      this.T_RAG_SOC_DESTINATARIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO = null; } else {
      this.T_ANNO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MESE = null; } else {
      this.T_MESE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_AMMISSIBILE = null; } else {
      this.B_AMMISSIBILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_CAUSALE = null; } else {
      this.T_COD_CAUSALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MOTIVAZIONE = null; } else {
      this.T_MOTIVAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_STATO = null; } else {
      this.D_DATA_STATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_DOWNLOAD = null; } else {
      this.D_DATA_DOWNLOAD = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CMG = null; } else {
      this.N_ID_CMG = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PRATICA = null; } else {
      this.N_ID_PRATICA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO = null; } else {
      this.T_TIPO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_STATO = null; } else {
      this.T_STATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RICH = null; } else {
      this.D_DATA_RICH = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DESTINATARIO = null; } else {
      this.N_ID_DESTINATARIO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RUOLO_DESTINATARIO = null; } else {
      this.T_RUOLO_DESTINATARIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PIVA_DESTINATARIO = null; } else {
      this.T_PIVA_DESTINATARIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RAG_SOC_DESTINATARIO = null; } else {
      this.T_RAG_SOC_DESTINATARIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO = null; } else {
      this.T_ANNO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MESE = null; } else {
      this.T_MESE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_AMMISSIBILE = null; } else {
      this.B_AMMISSIBILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_CAUSALE = null; } else {
      this.T_COD_CAUSALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MOTIVAZIONE = null; } else {
      this.T_MOTIVAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_STATO = null; } else {
      this.D_DATA_STATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_DOWNLOAD = null; } else {
      this.D_DATA_DOWNLOAD = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    cmg_prt_cmg o = (cmg_prt_cmg) super.clone();
    return o;
  }

  public void clone0(cmg_prt_cmg o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_CMG", this.N_ID_CMG);
    __sqoop$field_map.put("N_ID_PRATICA", this.N_ID_PRATICA);
    __sqoop$field_map.put("T_TIPO", this.T_TIPO);
    __sqoop$field_map.put("T_STATO", this.T_STATO);
    __sqoop$field_map.put("D_DATA_RICH", this.D_DATA_RICH);
    __sqoop$field_map.put("N_ID_DESTINATARIO", this.N_ID_DESTINATARIO);
    __sqoop$field_map.put("T_RUOLO_DESTINATARIO", this.T_RUOLO_DESTINATARIO);
    __sqoop$field_map.put("T_PIVA_DESTINATARIO", this.T_PIVA_DESTINATARIO);
    __sqoop$field_map.put("T_RAG_SOC_DESTINATARIO", this.T_RAG_SOC_DESTINATARIO);
    __sqoop$field_map.put("T_ANNO", this.T_ANNO);
    __sqoop$field_map.put("T_MESE", this.T_MESE);
    __sqoop$field_map.put("B_AMMISSIBILE", this.B_AMMISSIBILE);
    __sqoop$field_map.put("T_COD_CAUSALE", this.T_COD_CAUSALE);
    __sqoop$field_map.put("T_MOTIVAZIONE", this.T_MOTIVAZIONE);
    __sqoop$field_map.put("D_DATA_STATO", this.D_DATA_STATO);
    __sqoop$field_map.put("D_DATA_DOWNLOAD", this.D_DATA_DOWNLOAD);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_CMG", this.N_ID_CMG);
    __sqoop$field_map.put("N_ID_PRATICA", this.N_ID_PRATICA);
    __sqoop$field_map.put("T_TIPO", this.T_TIPO);
    __sqoop$field_map.put("T_STATO", this.T_STATO);
    __sqoop$field_map.put("D_DATA_RICH", this.D_DATA_RICH);
    __sqoop$field_map.put("N_ID_DESTINATARIO", this.N_ID_DESTINATARIO);
    __sqoop$field_map.put("T_RUOLO_DESTINATARIO", this.T_RUOLO_DESTINATARIO);
    __sqoop$field_map.put("T_PIVA_DESTINATARIO", this.T_PIVA_DESTINATARIO);
    __sqoop$field_map.put("T_RAG_SOC_DESTINATARIO", this.T_RAG_SOC_DESTINATARIO);
    __sqoop$field_map.put("T_ANNO", this.T_ANNO);
    __sqoop$field_map.put("T_MESE", this.T_MESE);
    __sqoop$field_map.put("B_AMMISSIBILE", this.B_AMMISSIBILE);
    __sqoop$field_map.put("T_COD_CAUSALE", this.T_COD_CAUSALE);
    __sqoop$field_map.put("T_MOTIVAZIONE", this.T_MOTIVAZIONE);
    __sqoop$field_map.put("D_DATA_STATO", this.D_DATA_STATO);
    __sqoop$field_map.put("D_DATA_DOWNLOAD", this.D_DATA_DOWNLOAD);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
