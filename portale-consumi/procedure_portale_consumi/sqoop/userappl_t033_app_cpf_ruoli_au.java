// ORM class for table 'userappl.t033_app_cpf_ruoli_au'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 16:42:01 CEST 2019
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

public class userappl_t033_app_cpf_ruoli_au extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_RUOLO_AU", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_RUOLO_AU = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_COD_RUOLO_AU", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COD_RUOLO_AU = (String)value;
      }
    });
    setters.put("T_DES_RUOLO_AU", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_DES_RUOLO_AU = (String)value;
      }
    });
    setters.put("T_UTENTE_INSERIMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_UTENTE_INSERIMENTO = (String)value;
      }
    });
    setters.put("D_DATA_INSERIMENTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_INSERIMENTO = (String)value;
      }
    });
    setters.put("T_UTENTE_MODIFICA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_UTENTE_MODIFICA = (String)value;
      }
    });
    setters.put("D_DATA_MODIFICA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_MODIFICA = (String)value;
      }
    });
    setters.put("T_CODICE_AU", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_AU = (String)value;
      }
    });
    setters.put("T_TIPO_ATTIVITA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO_ATTIVITA = (String)value;
      }
    });
    setters.put("T_FLG_OBBLIGATORIETA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_FLG_OBBLIGATORIETA = (String)value;
      }
    });
    setters.put("T_COMMODITY", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COMMODITY = (String)value;
      }
    });
  }
  public userappl_t033_app_cpf_ruoli_au() {
    init0();
  }
  private java.math.BigDecimal N_ID_RUOLO_AU;
  public java.math.BigDecimal get_N_ID_RUOLO_AU() {
    return N_ID_RUOLO_AU;
  }
  public void set_N_ID_RUOLO_AU(java.math.BigDecimal N_ID_RUOLO_AU) {
    this.N_ID_RUOLO_AU = N_ID_RUOLO_AU;
  }
  public userappl_t033_app_cpf_ruoli_au with_N_ID_RUOLO_AU(java.math.BigDecimal N_ID_RUOLO_AU) {
    this.N_ID_RUOLO_AU = N_ID_RUOLO_AU;
    return this;
  }
  private String T_COD_RUOLO_AU;
  public String get_T_COD_RUOLO_AU() {
    return T_COD_RUOLO_AU;
  }
  public void set_T_COD_RUOLO_AU(String T_COD_RUOLO_AU) {
    this.T_COD_RUOLO_AU = T_COD_RUOLO_AU;
  }
  public userappl_t033_app_cpf_ruoli_au with_T_COD_RUOLO_AU(String T_COD_RUOLO_AU) {
    this.T_COD_RUOLO_AU = T_COD_RUOLO_AU;
    return this;
  }
  private String T_DES_RUOLO_AU;
  public String get_T_DES_RUOLO_AU() {
    return T_DES_RUOLO_AU;
  }
  public void set_T_DES_RUOLO_AU(String T_DES_RUOLO_AU) {
    this.T_DES_RUOLO_AU = T_DES_RUOLO_AU;
  }
  public userappl_t033_app_cpf_ruoli_au with_T_DES_RUOLO_AU(String T_DES_RUOLO_AU) {
    this.T_DES_RUOLO_AU = T_DES_RUOLO_AU;
    return this;
  }
  private String T_UTENTE_INSERIMENTO;
  public String get_T_UTENTE_INSERIMENTO() {
    return T_UTENTE_INSERIMENTO;
  }
  public void set_T_UTENTE_INSERIMENTO(String T_UTENTE_INSERIMENTO) {
    this.T_UTENTE_INSERIMENTO = T_UTENTE_INSERIMENTO;
  }
  public userappl_t033_app_cpf_ruoli_au with_T_UTENTE_INSERIMENTO(String T_UTENTE_INSERIMENTO) {
    this.T_UTENTE_INSERIMENTO = T_UTENTE_INSERIMENTO;
    return this;
  }
  private String D_DATA_INSERIMENTO;
  public String get_D_DATA_INSERIMENTO() {
    return D_DATA_INSERIMENTO;
  }
  public void set_D_DATA_INSERIMENTO(String D_DATA_INSERIMENTO) {
    this.D_DATA_INSERIMENTO = D_DATA_INSERIMENTO;
  }
  public userappl_t033_app_cpf_ruoli_au with_D_DATA_INSERIMENTO(String D_DATA_INSERIMENTO) {
    this.D_DATA_INSERIMENTO = D_DATA_INSERIMENTO;
    return this;
  }
  private String T_UTENTE_MODIFICA;
  public String get_T_UTENTE_MODIFICA() {
    return T_UTENTE_MODIFICA;
  }
  public void set_T_UTENTE_MODIFICA(String T_UTENTE_MODIFICA) {
    this.T_UTENTE_MODIFICA = T_UTENTE_MODIFICA;
  }
  public userappl_t033_app_cpf_ruoli_au with_T_UTENTE_MODIFICA(String T_UTENTE_MODIFICA) {
    this.T_UTENTE_MODIFICA = T_UTENTE_MODIFICA;
    return this;
  }
  private String D_DATA_MODIFICA;
  public String get_D_DATA_MODIFICA() {
    return D_DATA_MODIFICA;
  }
  public void set_D_DATA_MODIFICA(String D_DATA_MODIFICA) {
    this.D_DATA_MODIFICA = D_DATA_MODIFICA;
  }
  public userappl_t033_app_cpf_ruoli_au with_D_DATA_MODIFICA(String D_DATA_MODIFICA) {
    this.D_DATA_MODIFICA = D_DATA_MODIFICA;
    return this;
  }
  private String T_CODICE_AU;
  public String get_T_CODICE_AU() {
    return T_CODICE_AU;
  }
  public void set_T_CODICE_AU(String T_CODICE_AU) {
    this.T_CODICE_AU = T_CODICE_AU;
  }
  public userappl_t033_app_cpf_ruoli_au with_T_CODICE_AU(String T_CODICE_AU) {
    this.T_CODICE_AU = T_CODICE_AU;
    return this;
  }
  private String T_TIPO_ATTIVITA;
  public String get_T_TIPO_ATTIVITA() {
    return T_TIPO_ATTIVITA;
  }
  public void set_T_TIPO_ATTIVITA(String T_TIPO_ATTIVITA) {
    this.T_TIPO_ATTIVITA = T_TIPO_ATTIVITA;
  }
  public userappl_t033_app_cpf_ruoli_au with_T_TIPO_ATTIVITA(String T_TIPO_ATTIVITA) {
    this.T_TIPO_ATTIVITA = T_TIPO_ATTIVITA;
    return this;
  }
  private String T_FLG_OBBLIGATORIETA;
  public String get_T_FLG_OBBLIGATORIETA() {
    return T_FLG_OBBLIGATORIETA;
  }
  public void set_T_FLG_OBBLIGATORIETA(String T_FLG_OBBLIGATORIETA) {
    this.T_FLG_OBBLIGATORIETA = T_FLG_OBBLIGATORIETA;
  }
  public userappl_t033_app_cpf_ruoli_au with_T_FLG_OBBLIGATORIETA(String T_FLG_OBBLIGATORIETA) {
    this.T_FLG_OBBLIGATORIETA = T_FLG_OBBLIGATORIETA;
    return this;
  }
  private String T_COMMODITY;
  public String get_T_COMMODITY() {
    return T_COMMODITY;
  }
  public void set_T_COMMODITY(String T_COMMODITY) {
    this.T_COMMODITY = T_COMMODITY;
  }
  public userappl_t033_app_cpf_ruoli_au with_T_COMMODITY(String T_COMMODITY) {
    this.T_COMMODITY = T_COMMODITY;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof userappl_t033_app_cpf_ruoli_au)) {
      return false;
    }
    userappl_t033_app_cpf_ruoli_au that = (userappl_t033_app_cpf_ruoli_au) o;
    boolean equal = true;
    equal = equal && (this.N_ID_RUOLO_AU == null ? that.N_ID_RUOLO_AU == null : this.N_ID_RUOLO_AU.equals(that.N_ID_RUOLO_AU));
    equal = equal && (this.T_COD_RUOLO_AU == null ? that.T_COD_RUOLO_AU == null : this.T_COD_RUOLO_AU.equals(that.T_COD_RUOLO_AU));
    equal = equal && (this.T_DES_RUOLO_AU == null ? that.T_DES_RUOLO_AU == null : this.T_DES_RUOLO_AU.equals(that.T_DES_RUOLO_AU));
    equal = equal && (this.T_UTENTE_INSERIMENTO == null ? that.T_UTENTE_INSERIMENTO == null : this.T_UTENTE_INSERIMENTO.equals(that.T_UTENTE_INSERIMENTO));
    equal = equal && (this.D_DATA_INSERIMENTO == null ? that.D_DATA_INSERIMENTO == null : this.D_DATA_INSERIMENTO.equals(that.D_DATA_INSERIMENTO));
    equal = equal && (this.T_UTENTE_MODIFICA == null ? that.T_UTENTE_MODIFICA == null : this.T_UTENTE_MODIFICA.equals(that.T_UTENTE_MODIFICA));
    equal = equal && (this.D_DATA_MODIFICA == null ? that.D_DATA_MODIFICA == null : this.D_DATA_MODIFICA.equals(that.D_DATA_MODIFICA));
    equal = equal && (this.T_CODICE_AU == null ? that.T_CODICE_AU == null : this.T_CODICE_AU.equals(that.T_CODICE_AU));
    equal = equal && (this.T_TIPO_ATTIVITA == null ? that.T_TIPO_ATTIVITA == null : this.T_TIPO_ATTIVITA.equals(that.T_TIPO_ATTIVITA));
    equal = equal && (this.T_FLG_OBBLIGATORIETA == null ? that.T_FLG_OBBLIGATORIETA == null : this.T_FLG_OBBLIGATORIETA.equals(that.T_FLG_OBBLIGATORIETA));
    equal = equal && (this.T_COMMODITY == null ? that.T_COMMODITY == null : this.T_COMMODITY.equals(that.T_COMMODITY));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof userappl_t033_app_cpf_ruoli_au)) {
      return false;
    }
    userappl_t033_app_cpf_ruoli_au that = (userappl_t033_app_cpf_ruoli_au) o;
    boolean equal = true;
    equal = equal && (this.N_ID_RUOLO_AU == null ? that.N_ID_RUOLO_AU == null : this.N_ID_RUOLO_AU.equals(that.N_ID_RUOLO_AU));
    equal = equal && (this.T_COD_RUOLO_AU == null ? that.T_COD_RUOLO_AU == null : this.T_COD_RUOLO_AU.equals(that.T_COD_RUOLO_AU));
    equal = equal && (this.T_DES_RUOLO_AU == null ? that.T_DES_RUOLO_AU == null : this.T_DES_RUOLO_AU.equals(that.T_DES_RUOLO_AU));
    equal = equal && (this.T_UTENTE_INSERIMENTO == null ? that.T_UTENTE_INSERIMENTO == null : this.T_UTENTE_INSERIMENTO.equals(that.T_UTENTE_INSERIMENTO));
    equal = equal && (this.D_DATA_INSERIMENTO == null ? that.D_DATA_INSERIMENTO == null : this.D_DATA_INSERIMENTO.equals(that.D_DATA_INSERIMENTO));
    equal = equal && (this.T_UTENTE_MODIFICA == null ? that.T_UTENTE_MODIFICA == null : this.T_UTENTE_MODIFICA.equals(that.T_UTENTE_MODIFICA));
    equal = equal && (this.D_DATA_MODIFICA == null ? that.D_DATA_MODIFICA == null : this.D_DATA_MODIFICA.equals(that.D_DATA_MODIFICA));
    equal = equal && (this.T_CODICE_AU == null ? that.T_CODICE_AU == null : this.T_CODICE_AU.equals(that.T_CODICE_AU));
    equal = equal && (this.T_TIPO_ATTIVITA == null ? that.T_TIPO_ATTIVITA == null : this.T_TIPO_ATTIVITA.equals(that.T_TIPO_ATTIVITA));
    equal = equal && (this.T_FLG_OBBLIGATORIETA == null ? that.T_FLG_OBBLIGATORIETA == null : this.T_FLG_OBBLIGATORIETA.equals(that.T_FLG_OBBLIGATORIETA));
    equal = equal && (this.T_COMMODITY == null ? that.T_COMMODITY == null : this.T_COMMODITY.equals(that.T_COMMODITY));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_RUOLO_AU = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.T_COD_RUOLO_AU = JdbcWritableBridge.readString(2, __dbResults);
    this.T_DES_RUOLO_AU = JdbcWritableBridge.readString(3, __dbResults);
    this.T_UTENTE_INSERIMENTO = JdbcWritableBridge.readString(4, __dbResults);
    this.D_DATA_INSERIMENTO = JdbcWritableBridge.readString(5, __dbResults);
    this.T_UTENTE_MODIFICA = JdbcWritableBridge.readString(6, __dbResults);
    this.D_DATA_MODIFICA = JdbcWritableBridge.readString(7, __dbResults);
    this.T_CODICE_AU = JdbcWritableBridge.readString(8, __dbResults);
    this.T_TIPO_ATTIVITA = JdbcWritableBridge.readString(9, __dbResults);
    this.T_FLG_OBBLIGATORIETA = JdbcWritableBridge.readString(10, __dbResults);
    this.T_COMMODITY = JdbcWritableBridge.readString(11, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_RUOLO_AU = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.T_COD_RUOLO_AU = JdbcWritableBridge.readString(2, __dbResults);
    this.T_DES_RUOLO_AU = JdbcWritableBridge.readString(3, __dbResults);
    this.T_UTENTE_INSERIMENTO = JdbcWritableBridge.readString(4, __dbResults);
    this.D_DATA_INSERIMENTO = JdbcWritableBridge.readString(5, __dbResults);
    this.T_UTENTE_MODIFICA = JdbcWritableBridge.readString(6, __dbResults);
    this.D_DATA_MODIFICA = JdbcWritableBridge.readString(7, __dbResults);
    this.T_CODICE_AU = JdbcWritableBridge.readString(8, __dbResults);
    this.T_TIPO_ATTIVITA = JdbcWritableBridge.readString(9, __dbResults);
    this.T_FLG_OBBLIGATORIETA = JdbcWritableBridge.readString(10, __dbResults);
    this.T_COMMODITY = JdbcWritableBridge.readString(11, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_RUOLO_AU, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_RUOLO_AU, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DES_RUOLO_AU, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_UTENTE_INSERIMENTO, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INSERIMENTO, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_UTENTE_MODIFICA, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_MODIFICA, 7 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_AU, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_ATTIVITA, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_FLG_OBBLIGATORIETA, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMMODITY, 11 + __off, 1, __dbStmt);
    return 11;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_RUOLO_AU, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_RUOLO_AU, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DES_RUOLO_AU, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_UTENTE_INSERIMENTO, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INSERIMENTO, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_UTENTE_MODIFICA, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_MODIFICA, 7 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_AU, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_ATTIVITA, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_FLG_OBBLIGATORIETA, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COMMODITY, 11 + __off, 1, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_RUOLO_AU = null;
    } else {
    this.N_ID_RUOLO_AU = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COD_RUOLO_AU = null;
    } else {
    this.T_COD_RUOLO_AU = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_DES_RUOLO_AU = null;
    } else {
    this.T_DES_RUOLO_AU = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_UTENTE_INSERIMENTO = null;
    } else {
    this.T_UTENTE_INSERIMENTO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_INSERIMENTO = null;
    } else {
    this.D_DATA_INSERIMENTO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_UTENTE_MODIFICA = null;
    } else {
    this.T_UTENTE_MODIFICA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_MODIFICA = null;
    } else {
    this.D_DATA_MODIFICA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_AU = null;
    } else {
    this.T_CODICE_AU = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO_ATTIVITA = null;
    } else {
    this.T_TIPO_ATTIVITA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_FLG_OBBLIGATORIETA = null;
    } else {
    this.T_FLG_OBBLIGATORIETA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COMMODITY = null;
    } else {
    this.T_COMMODITY = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_RUOLO_AU) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_RUOLO_AU, __dataOut);
    }
    if (null == this.T_COD_RUOLO_AU) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_RUOLO_AU);
    }
    if (null == this.T_DES_RUOLO_AU) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DES_RUOLO_AU);
    }
    if (null == this.T_UTENTE_INSERIMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_UTENTE_INSERIMENTO);
    }
    if (null == this.D_DATA_INSERIMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INSERIMENTO);
    }
    if (null == this.T_UTENTE_MODIFICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_UTENTE_MODIFICA);
    }
    if (null == this.D_DATA_MODIFICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_MODIFICA);
    }
    if (null == this.T_CODICE_AU) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_AU);
    }
    if (null == this.T_TIPO_ATTIVITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_ATTIVITA);
    }
    if (null == this.T_FLG_OBBLIGATORIETA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_FLG_OBBLIGATORIETA);
    }
    if (null == this.T_COMMODITY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMMODITY);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_RUOLO_AU) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_RUOLO_AU, __dataOut);
    }
    if (null == this.T_COD_RUOLO_AU) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_RUOLO_AU);
    }
    if (null == this.T_DES_RUOLO_AU) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DES_RUOLO_AU);
    }
    if (null == this.T_UTENTE_INSERIMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_UTENTE_INSERIMENTO);
    }
    if (null == this.D_DATA_INSERIMENTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INSERIMENTO);
    }
    if (null == this.T_UTENTE_MODIFICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_UTENTE_MODIFICA);
    }
    if (null == this.D_DATA_MODIFICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_MODIFICA);
    }
    if (null == this.T_CODICE_AU) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_AU);
    }
    if (null == this.T_TIPO_ATTIVITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_ATTIVITA);
    }
    if (null == this.T_FLG_OBBLIGATORIETA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_FLG_OBBLIGATORIETA);
    }
    if (null == this.T_COMMODITY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMMODITY);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_RUOLO_AU==null?"":N_ID_RUOLO_AU.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_RUOLO_AU==null?"":T_COD_RUOLO_AU, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DES_RUOLO_AU==null?"":T_DES_RUOLO_AU, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_UTENTE_INSERIMENTO==null?"":T_UTENTE_INSERIMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INSERIMENTO==null?"":D_DATA_INSERIMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_UTENTE_MODIFICA==null?"":T_UTENTE_MODIFICA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_MODIFICA==null?"":D_DATA_MODIFICA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_AU==null?"":T_CODICE_AU, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_ATTIVITA==null?"":T_TIPO_ATTIVITA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_FLG_OBBLIGATORIETA==null?"":T_FLG_OBBLIGATORIETA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMMODITY==null?"":T_COMMODITY, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_RUOLO_AU==null?"":N_ID_RUOLO_AU.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_RUOLO_AU==null?"":T_COD_RUOLO_AU, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DES_RUOLO_AU==null?"":T_DES_RUOLO_AU, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_UTENTE_INSERIMENTO==null?"":T_UTENTE_INSERIMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INSERIMENTO==null?"":D_DATA_INSERIMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_UTENTE_MODIFICA==null?"":T_UTENTE_MODIFICA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_MODIFICA==null?"":D_DATA_MODIFICA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_AU==null?"":T_CODICE_AU, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_ATTIVITA==null?"":T_TIPO_ATTIVITA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_FLG_OBBLIGATORIETA==null?"":T_FLG_OBBLIGATORIETA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMMODITY==null?"":T_COMMODITY, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_RUOLO_AU = null; } else {
      this.N_ID_RUOLO_AU = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_RUOLO_AU = null; } else {
      this.T_COD_RUOLO_AU = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DES_RUOLO_AU = null; } else {
      this.T_DES_RUOLO_AU = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_UTENTE_INSERIMENTO = null; } else {
      this.T_UTENTE_INSERIMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INSERIMENTO = null; } else {
      this.D_DATA_INSERIMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_UTENTE_MODIFICA = null; } else {
      this.T_UTENTE_MODIFICA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_MODIFICA = null; } else {
      this.D_DATA_MODIFICA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_AU = null; } else {
      this.T_CODICE_AU = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_ATTIVITA = null; } else {
      this.T_TIPO_ATTIVITA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_FLG_OBBLIGATORIETA = null; } else {
      this.T_FLG_OBBLIGATORIETA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMMODITY = null; } else {
      this.T_COMMODITY = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_RUOLO_AU = null; } else {
      this.N_ID_RUOLO_AU = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_RUOLO_AU = null; } else {
      this.T_COD_RUOLO_AU = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DES_RUOLO_AU = null; } else {
      this.T_DES_RUOLO_AU = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_UTENTE_INSERIMENTO = null; } else {
      this.T_UTENTE_INSERIMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INSERIMENTO = null; } else {
      this.D_DATA_INSERIMENTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_UTENTE_MODIFICA = null; } else {
      this.T_UTENTE_MODIFICA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_MODIFICA = null; } else {
      this.D_DATA_MODIFICA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_AU = null; } else {
      this.T_CODICE_AU = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_ATTIVITA = null; } else {
      this.T_TIPO_ATTIVITA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_FLG_OBBLIGATORIETA = null; } else {
      this.T_FLG_OBBLIGATORIETA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMMODITY = null; } else {
      this.T_COMMODITY = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    userappl_t033_app_cpf_ruoli_au o = (userappl_t033_app_cpf_ruoli_au) super.clone();
    return o;
  }

  public void clone0(userappl_t033_app_cpf_ruoli_au o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_RUOLO_AU", this.N_ID_RUOLO_AU);
    __sqoop$field_map.put("T_COD_RUOLO_AU", this.T_COD_RUOLO_AU);
    __sqoop$field_map.put("T_DES_RUOLO_AU", this.T_DES_RUOLO_AU);
    __sqoop$field_map.put("T_UTENTE_INSERIMENTO", this.T_UTENTE_INSERIMENTO);
    __sqoop$field_map.put("D_DATA_INSERIMENTO", this.D_DATA_INSERIMENTO);
    __sqoop$field_map.put("T_UTENTE_MODIFICA", this.T_UTENTE_MODIFICA);
    __sqoop$field_map.put("D_DATA_MODIFICA", this.D_DATA_MODIFICA);
    __sqoop$field_map.put("T_CODICE_AU", this.T_CODICE_AU);
    __sqoop$field_map.put("T_TIPO_ATTIVITA", this.T_TIPO_ATTIVITA);
    __sqoop$field_map.put("T_FLG_OBBLIGATORIETA", this.T_FLG_OBBLIGATORIETA);
    __sqoop$field_map.put("T_COMMODITY", this.T_COMMODITY);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_RUOLO_AU", this.N_ID_RUOLO_AU);
    __sqoop$field_map.put("T_COD_RUOLO_AU", this.T_COD_RUOLO_AU);
    __sqoop$field_map.put("T_DES_RUOLO_AU", this.T_DES_RUOLO_AU);
    __sqoop$field_map.put("T_UTENTE_INSERIMENTO", this.T_UTENTE_INSERIMENTO);
    __sqoop$field_map.put("D_DATA_INSERIMENTO", this.D_DATA_INSERIMENTO);
    __sqoop$field_map.put("T_UTENTE_MODIFICA", this.T_UTENTE_MODIFICA);
    __sqoop$field_map.put("D_DATA_MODIFICA", this.D_DATA_MODIFICA);
    __sqoop$field_map.put("T_CODICE_AU", this.T_CODICE_AU);
    __sqoop$field_map.put("T_TIPO_ATTIVITA", this.T_TIPO_ATTIVITA);
    __sqoop$field_map.put("T_FLG_OBBLIGATORIETA", this.T_FLG_OBBLIGATORIETA);
    __sqoop$field_map.put("T_COMMODITY", this.T_COMMODITY);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
