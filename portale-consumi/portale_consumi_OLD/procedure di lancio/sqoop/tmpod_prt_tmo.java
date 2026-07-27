// ORM class for table 'tmpod.prt_tmo'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 14:47:27 CEST 2019
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

public class tmpod_prt_tmo extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_TMO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_TMO = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_PRATICA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_PRATICA = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_TIPO_PRT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO_PRT = (String)value;
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
    setters.put("N_ID_UTENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_UTENTE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_STATO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_STATO = (String)value;
      }
    });
    setters.put("D_RICHIESTA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_RICHIESTA = (String)value;
      }
    });
    setters.put("B_AMMISSIBILE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_AMMISSIBILE = (String)value;
      }
    });
    setters.put("B_FUORI_FINESTRA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_FUORI_FINESTRA = (String)value;
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
    setters.put("FLAG_GESTORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FLAG_GESTORE = (String)value;
      }
    });
  }
  public tmpod_prt_tmo() {
    init0();
  }
  private java.math.BigDecimal N_ID_TMO;
  public java.math.BigDecimal get_N_ID_TMO() {
    return N_ID_TMO;
  }
  public void set_N_ID_TMO(java.math.BigDecimal N_ID_TMO) {
    this.N_ID_TMO = N_ID_TMO;
  }
  public tmpod_prt_tmo with_N_ID_TMO(java.math.BigDecimal N_ID_TMO) {
    this.N_ID_TMO = N_ID_TMO;
    return this;
  }
  private java.math.BigDecimal N_ID_PRATICA;
  public java.math.BigDecimal get_N_ID_PRATICA() {
    return N_ID_PRATICA;
  }
  public void set_N_ID_PRATICA(java.math.BigDecimal N_ID_PRATICA) {
    this.N_ID_PRATICA = N_ID_PRATICA;
  }
  public tmpod_prt_tmo with_N_ID_PRATICA(java.math.BigDecimal N_ID_PRATICA) {
    this.N_ID_PRATICA = N_ID_PRATICA;
    return this;
  }
  private String T_TIPO_PRT;
  public String get_T_TIPO_PRT() {
    return T_TIPO_PRT;
  }
  public void set_T_TIPO_PRT(String T_TIPO_PRT) {
    this.T_TIPO_PRT = T_TIPO_PRT;
  }
  public tmpod_prt_tmo with_T_TIPO_PRT(String T_TIPO_PRT) {
    this.T_TIPO_PRT = T_TIPO_PRT;
    return this;
  }
  private String T_ANNO;
  public String get_T_ANNO() {
    return T_ANNO;
  }
  public void set_T_ANNO(String T_ANNO) {
    this.T_ANNO = T_ANNO;
  }
  public tmpod_prt_tmo with_T_ANNO(String T_ANNO) {
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
  public tmpod_prt_tmo with_T_MESE(String T_MESE) {
    this.T_MESE = T_MESE;
    return this;
  }
  private java.math.BigDecimal N_ID_UTENTE;
  public java.math.BigDecimal get_N_ID_UTENTE() {
    return N_ID_UTENTE;
  }
  public void set_N_ID_UTENTE(java.math.BigDecimal N_ID_UTENTE) {
    this.N_ID_UTENTE = N_ID_UTENTE;
  }
  public tmpod_prt_tmo with_N_ID_UTENTE(java.math.BigDecimal N_ID_UTENTE) {
    this.N_ID_UTENTE = N_ID_UTENTE;
    return this;
  }
  private String T_STATO;
  public String get_T_STATO() {
    return T_STATO;
  }
  public void set_T_STATO(String T_STATO) {
    this.T_STATO = T_STATO;
  }
  public tmpod_prt_tmo with_T_STATO(String T_STATO) {
    this.T_STATO = T_STATO;
    return this;
  }
  private String D_RICHIESTA;
  public String get_D_RICHIESTA() {
    return D_RICHIESTA;
  }
  public void set_D_RICHIESTA(String D_RICHIESTA) {
    this.D_RICHIESTA = D_RICHIESTA;
  }
  public tmpod_prt_tmo with_D_RICHIESTA(String D_RICHIESTA) {
    this.D_RICHIESTA = D_RICHIESTA;
    return this;
  }
  private String B_AMMISSIBILE;
  public String get_B_AMMISSIBILE() {
    return B_AMMISSIBILE;
  }
  public void set_B_AMMISSIBILE(String B_AMMISSIBILE) {
    this.B_AMMISSIBILE = B_AMMISSIBILE;
  }
  public tmpod_prt_tmo with_B_AMMISSIBILE(String B_AMMISSIBILE) {
    this.B_AMMISSIBILE = B_AMMISSIBILE;
    return this;
  }
  private String B_FUORI_FINESTRA;
  public String get_B_FUORI_FINESTRA() {
    return B_FUORI_FINESTRA;
  }
  public void set_B_FUORI_FINESTRA(String B_FUORI_FINESTRA) {
    this.B_FUORI_FINESTRA = B_FUORI_FINESTRA;
  }
  public tmpod_prt_tmo with_B_FUORI_FINESTRA(String B_FUORI_FINESTRA) {
    this.B_FUORI_FINESTRA = B_FUORI_FINESTRA;
    return this;
  }
  private String T_COD_CAUSALE;
  public String get_T_COD_CAUSALE() {
    return T_COD_CAUSALE;
  }
  public void set_T_COD_CAUSALE(String T_COD_CAUSALE) {
    this.T_COD_CAUSALE = T_COD_CAUSALE;
  }
  public tmpod_prt_tmo with_T_COD_CAUSALE(String T_COD_CAUSALE) {
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
  public tmpod_prt_tmo with_T_MOTIVAZIONE(String T_MOTIVAZIONE) {
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
  public tmpod_prt_tmo with_D_DATA_STATO(String D_DATA_STATO) {
    this.D_DATA_STATO = D_DATA_STATO;
    return this;
  }
  private String FLAG_GESTORE;
  public String get_FLAG_GESTORE() {
    return FLAG_GESTORE;
  }
  public void set_FLAG_GESTORE(String FLAG_GESTORE) {
    this.FLAG_GESTORE = FLAG_GESTORE;
  }
  public tmpod_prt_tmo with_FLAG_GESTORE(String FLAG_GESTORE) {
    this.FLAG_GESTORE = FLAG_GESTORE;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof tmpod_prt_tmo)) {
      return false;
    }
    tmpod_prt_tmo that = (tmpod_prt_tmo) o;
    boolean equal = true;
    equal = equal && (this.N_ID_TMO == null ? that.N_ID_TMO == null : this.N_ID_TMO.equals(that.N_ID_TMO));
    equal = equal && (this.N_ID_PRATICA == null ? that.N_ID_PRATICA == null : this.N_ID_PRATICA.equals(that.N_ID_PRATICA));
    equal = equal && (this.T_TIPO_PRT == null ? that.T_TIPO_PRT == null : this.T_TIPO_PRT.equals(that.T_TIPO_PRT));
    equal = equal && (this.T_ANNO == null ? that.T_ANNO == null : this.T_ANNO.equals(that.T_ANNO));
    equal = equal && (this.T_MESE == null ? that.T_MESE == null : this.T_MESE.equals(that.T_MESE));
    equal = equal && (this.N_ID_UTENTE == null ? that.N_ID_UTENTE == null : this.N_ID_UTENTE.equals(that.N_ID_UTENTE));
    equal = equal && (this.T_STATO == null ? that.T_STATO == null : this.T_STATO.equals(that.T_STATO));
    equal = equal && (this.D_RICHIESTA == null ? that.D_RICHIESTA == null : this.D_RICHIESTA.equals(that.D_RICHIESTA));
    equal = equal && (this.B_AMMISSIBILE == null ? that.B_AMMISSIBILE == null : this.B_AMMISSIBILE.equals(that.B_AMMISSIBILE));
    equal = equal && (this.B_FUORI_FINESTRA == null ? that.B_FUORI_FINESTRA == null : this.B_FUORI_FINESTRA.equals(that.B_FUORI_FINESTRA));
    equal = equal && (this.T_COD_CAUSALE == null ? that.T_COD_CAUSALE == null : this.T_COD_CAUSALE.equals(that.T_COD_CAUSALE));
    equal = equal && (this.T_MOTIVAZIONE == null ? that.T_MOTIVAZIONE == null : this.T_MOTIVAZIONE.equals(that.T_MOTIVAZIONE));
    equal = equal && (this.D_DATA_STATO == null ? that.D_DATA_STATO == null : this.D_DATA_STATO.equals(that.D_DATA_STATO));
    equal = equal && (this.FLAG_GESTORE == null ? that.FLAG_GESTORE == null : this.FLAG_GESTORE.equals(that.FLAG_GESTORE));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof tmpod_prt_tmo)) {
      return false;
    }
    tmpod_prt_tmo that = (tmpod_prt_tmo) o;
    boolean equal = true;
    equal = equal && (this.N_ID_TMO == null ? that.N_ID_TMO == null : this.N_ID_TMO.equals(that.N_ID_TMO));
    equal = equal && (this.N_ID_PRATICA == null ? that.N_ID_PRATICA == null : this.N_ID_PRATICA.equals(that.N_ID_PRATICA));
    equal = equal && (this.T_TIPO_PRT == null ? that.T_TIPO_PRT == null : this.T_TIPO_PRT.equals(that.T_TIPO_PRT));
    equal = equal && (this.T_ANNO == null ? that.T_ANNO == null : this.T_ANNO.equals(that.T_ANNO));
    equal = equal && (this.T_MESE == null ? that.T_MESE == null : this.T_MESE.equals(that.T_MESE));
    equal = equal && (this.N_ID_UTENTE == null ? that.N_ID_UTENTE == null : this.N_ID_UTENTE.equals(that.N_ID_UTENTE));
    equal = equal && (this.T_STATO == null ? that.T_STATO == null : this.T_STATO.equals(that.T_STATO));
    equal = equal && (this.D_RICHIESTA == null ? that.D_RICHIESTA == null : this.D_RICHIESTA.equals(that.D_RICHIESTA));
    equal = equal && (this.B_AMMISSIBILE == null ? that.B_AMMISSIBILE == null : this.B_AMMISSIBILE.equals(that.B_AMMISSIBILE));
    equal = equal && (this.B_FUORI_FINESTRA == null ? that.B_FUORI_FINESTRA == null : this.B_FUORI_FINESTRA.equals(that.B_FUORI_FINESTRA));
    equal = equal && (this.T_COD_CAUSALE == null ? that.T_COD_CAUSALE == null : this.T_COD_CAUSALE.equals(that.T_COD_CAUSALE));
    equal = equal && (this.T_MOTIVAZIONE == null ? that.T_MOTIVAZIONE == null : this.T_MOTIVAZIONE.equals(that.T_MOTIVAZIONE));
    equal = equal && (this.D_DATA_STATO == null ? that.D_DATA_STATO == null : this.D_DATA_STATO.equals(that.D_DATA_STATO));
    equal = equal && (this.FLAG_GESTORE == null ? that.FLAG_GESTORE == null : this.FLAG_GESTORE.equals(that.FLAG_GESTORE));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_TMO = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_PRATICA = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_TIPO_PRT = JdbcWritableBridge.readString(3, __dbResults);
    this.T_ANNO = JdbcWritableBridge.readString(4, __dbResults);
    this.T_MESE = JdbcWritableBridge.readString(5, __dbResults);
    this.N_ID_UTENTE = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.T_STATO = JdbcWritableBridge.readString(7, __dbResults);
    this.D_RICHIESTA = JdbcWritableBridge.readString(8, __dbResults);
    this.B_AMMISSIBILE = JdbcWritableBridge.readString(9, __dbResults);
    this.B_FUORI_FINESTRA = JdbcWritableBridge.readString(10, __dbResults);
    this.T_COD_CAUSALE = JdbcWritableBridge.readString(11, __dbResults);
    this.T_MOTIVAZIONE = JdbcWritableBridge.readString(12, __dbResults);
    this.D_DATA_STATO = JdbcWritableBridge.readString(13, __dbResults);
    this.FLAG_GESTORE = JdbcWritableBridge.readString(14, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_TMO = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_PRATICA = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_TIPO_PRT = JdbcWritableBridge.readString(3, __dbResults);
    this.T_ANNO = JdbcWritableBridge.readString(4, __dbResults);
    this.T_MESE = JdbcWritableBridge.readString(5, __dbResults);
    this.N_ID_UTENTE = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.T_STATO = JdbcWritableBridge.readString(7, __dbResults);
    this.D_RICHIESTA = JdbcWritableBridge.readString(8, __dbResults);
    this.B_AMMISSIBILE = JdbcWritableBridge.readString(9, __dbResults);
    this.B_FUORI_FINESTRA = JdbcWritableBridge.readString(10, __dbResults);
    this.T_COD_CAUSALE = JdbcWritableBridge.readString(11, __dbResults);
    this.T_MOTIVAZIONE = JdbcWritableBridge.readString(12, __dbResults);
    this.D_DATA_STATO = JdbcWritableBridge.readString(13, __dbResults);
    this.FLAG_GESTORE = JdbcWritableBridge.readString(14, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_TMO, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PRATICA, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_PRT, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MESE, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UTENTE, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_RICHIESTA, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(B_AMMISSIBILE, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_FUORI_FINESTRA, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CAUSALE, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MOTIVAZIONE, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_STATO, 13 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(FLAG_GESTORE, 14 + __off, 12, __dbStmt);
    return 14;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_TMO, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PRATICA, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_PRT, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MESE, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UTENTE, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_RICHIESTA, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(B_AMMISSIBILE, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_FUORI_FINESTRA, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CAUSALE, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MOTIVAZIONE, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_STATO, 13 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(FLAG_GESTORE, 14 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_TMO = null;
    } else {
    this.N_ID_TMO = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_PRATICA = null;
    } else {
    this.N_ID_PRATICA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO_PRT = null;
    } else {
    this.T_TIPO_PRT = Text.readString(__dataIn);
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
        this.N_ID_UTENTE = null;
    } else {
    this.N_ID_UTENTE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_STATO = null;
    } else {
    this.T_STATO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_RICHIESTA = null;
    } else {
    this.D_RICHIESTA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_AMMISSIBILE = null;
    } else {
    this.B_AMMISSIBILE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_FUORI_FINESTRA = null;
    } else {
    this.B_FUORI_FINESTRA = Text.readString(__dataIn);
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
        this.FLAG_GESTORE = null;
    } else {
    this.FLAG_GESTORE = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_TMO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_TMO, __dataOut);
    }
    if (null == this.N_ID_PRATICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PRATICA, __dataOut);
    }
    if (null == this.T_TIPO_PRT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_PRT);
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
    if (null == this.N_ID_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UTENTE, __dataOut);
    }
    if (null == this.T_STATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_STATO);
    }
    if (null == this.D_RICHIESTA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_RICHIESTA);
    }
    if (null == this.B_AMMISSIBILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_AMMISSIBILE);
    }
    if (null == this.B_FUORI_FINESTRA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_FUORI_FINESTRA);
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
    if (null == this.FLAG_GESTORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FLAG_GESTORE);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_TMO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_TMO, __dataOut);
    }
    if (null == this.N_ID_PRATICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PRATICA, __dataOut);
    }
    if (null == this.T_TIPO_PRT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_PRT);
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
    if (null == this.N_ID_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UTENTE, __dataOut);
    }
    if (null == this.T_STATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_STATO);
    }
    if (null == this.D_RICHIESTA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_RICHIESTA);
    }
    if (null == this.B_AMMISSIBILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_AMMISSIBILE);
    }
    if (null == this.B_FUORI_FINESTRA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_FUORI_FINESTRA);
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
    if (null == this.FLAG_GESTORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FLAG_GESTORE);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TMO==null?"":N_ID_TMO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PRATICA==null?"":N_ID_PRATICA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_PRT==null?"":T_TIPO_PRT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO==null?"":T_ANNO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MESE==null?"":T_MESE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UTENTE==null?"":N_ID_UTENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO==null?"":T_STATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_RICHIESTA==null?"":D_RICHIESTA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_AMMISSIBILE==null?"":B_AMMISSIBILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_FUORI_FINESTRA==null?"":B_FUORI_FINESTRA, " ", delimiters));
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(FLAG_GESTORE==null?"":FLAG_GESTORE, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TMO==null?"":N_ID_TMO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PRATICA==null?"":N_ID_PRATICA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_PRT==null?"":T_TIPO_PRT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO==null?"":T_ANNO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MESE==null?"":T_MESE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UTENTE==null?"":N_ID_UTENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO==null?"":T_STATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_RICHIESTA==null?"":D_RICHIESTA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_AMMISSIBILE==null?"":B_AMMISSIBILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_FUORI_FINESTRA==null?"":B_FUORI_FINESTRA, " ", delimiters));
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(FLAG_GESTORE==null?"":FLAG_GESTORE, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_TMO = null; } else {
      this.N_ID_TMO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PRATICA = null; } else {
      this.N_ID_PRATICA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_PRT = null; } else {
      this.T_TIPO_PRT = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UTENTE = null; } else {
      this.N_ID_UTENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_STATO = null; } else {
      this.T_STATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_RICHIESTA = null; } else {
      this.D_RICHIESTA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_AMMISSIBILE = null; } else {
      this.B_AMMISSIBILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_FUORI_FINESTRA = null; } else {
      this.B_FUORI_FINESTRA = __cur_str;
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
    if (__cur_str.equals("null")) { this.FLAG_GESTORE = null; } else {
      this.FLAG_GESTORE = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_TMO = null; } else {
      this.N_ID_TMO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PRATICA = null; } else {
      this.N_ID_PRATICA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_PRT = null; } else {
      this.T_TIPO_PRT = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UTENTE = null; } else {
      this.N_ID_UTENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_STATO = null; } else {
      this.T_STATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_RICHIESTA = null; } else {
      this.D_RICHIESTA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_AMMISSIBILE = null; } else {
      this.B_AMMISSIBILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_FUORI_FINESTRA = null; } else {
      this.B_FUORI_FINESTRA = __cur_str;
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
    if (__cur_str.equals("null")) { this.FLAG_GESTORE = null; } else {
      this.FLAG_GESTORE = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    tmpod_prt_tmo o = (tmpod_prt_tmo) super.clone();
    return o;
  }

  public void clone0(tmpod_prt_tmo o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_TMO", this.N_ID_TMO);
    __sqoop$field_map.put("N_ID_PRATICA", this.N_ID_PRATICA);
    __sqoop$field_map.put("T_TIPO_PRT", this.T_TIPO_PRT);
    __sqoop$field_map.put("T_ANNO", this.T_ANNO);
    __sqoop$field_map.put("T_MESE", this.T_MESE);
    __sqoop$field_map.put("N_ID_UTENTE", this.N_ID_UTENTE);
    __sqoop$field_map.put("T_STATO", this.T_STATO);
    __sqoop$field_map.put("D_RICHIESTA", this.D_RICHIESTA);
    __sqoop$field_map.put("B_AMMISSIBILE", this.B_AMMISSIBILE);
    __sqoop$field_map.put("B_FUORI_FINESTRA", this.B_FUORI_FINESTRA);
    __sqoop$field_map.put("T_COD_CAUSALE", this.T_COD_CAUSALE);
    __sqoop$field_map.put("T_MOTIVAZIONE", this.T_MOTIVAZIONE);
    __sqoop$field_map.put("D_DATA_STATO", this.D_DATA_STATO);
    __sqoop$field_map.put("FLAG_GESTORE", this.FLAG_GESTORE);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_TMO", this.N_ID_TMO);
    __sqoop$field_map.put("N_ID_PRATICA", this.N_ID_PRATICA);
    __sqoop$field_map.put("T_TIPO_PRT", this.T_TIPO_PRT);
    __sqoop$field_map.put("T_ANNO", this.T_ANNO);
    __sqoop$field_map.put("T_MESE", this.T_MESE);
    __sqoop$field_map.put("N_ID_UTENTE", this.N_ID_UTENTE);
    __sqoop$field_map.put("T_STATO", this.T_STATO);
    __sqoop$field_map.put("D_RICHIESTA", this.D_RICHIESTA);
    __sqoop$field_map.put("B_AMMISSIBILE", this.B_AMMISSIBILE);
    __sqoop$field_map.put("B_FUORI_FINESTRA", this.B_FUORI_FINESTRA);
    __sqoop$field_map.put("T_COD_CAUSALE", this.T_COD_CAUSALE);
    __sqoop$field_map.put("T_MOTIVAZIONE", this.T_MOTIVAZIONE);
    __sqoop$field_map.put("D_DATA_STATO", this.D_DATA_STATO);
    __sqoop$field_map.put("FLAG_GESTORE", this.FLAG_GESTORE);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
