// ORM class for table 'userappl.t001_app_prt_pratiche'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 16:33:19 CEST 2019
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

public class userappl_t001_app_prt_pratiche extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_PRATICA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_PRATICA = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_PROTOCOLLO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PROTOCOLLO = (String)value;
      }
    });
    setters.put("N_ID_DESCRITTORE_PROCESSO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_DESCRITTORE_PROCESSO = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_STATO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_STATO = (String)value;
      }
    });
    setters.put("N_ID_UTENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_UTENTE = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_OPERATORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_OPERATORE = (java.math.BigDecimal)value;
      }
    });
    setters.put("D_DATA_APERTURA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_APERTURA = (String)value;
      }
    });
    setters.put("D_DATA_CHIUSURA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_CHIUSURA = (String)value;
      }
    });
    setters.put("T_POD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_POD = (String)value;
      }
    });
    setters.put("N_CONTATORE_MODIFICA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_CONTATORE_MODIFICA = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_STATO_BUSINESS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_STATO_BUSINESS = (String)value;
      }
    });
    setters.put("T_ARCHIVIATA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ARCHIVIATA = (String)value;
      }
    });
    setters.put("N_ID_UTENTE_MODIFICA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_UTENTE_MODIFICA = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_PRATICA_ORIGINE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_PRATICA_ORIGINE = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_OPERATORE_CHIUSURA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_OPERATORE_CHIUSURA = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_VISIBILE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_VISIBILE = (String)value;
      }
    });
    setters.put("T_URL_ANNULL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_URL_ANNULL = (String)value;
      }
    });
  }
  public userappl_t001_app_prt_pratiche() {
    init0();
  }
  private java.math.BigDecimal N_ID_PRATICA;
  public java.math.BigDecimal get_N_ID_PRATICA() {
    return N_ID_PRATICA;
  }
  public void set_N_ID_PRATICA(java.math.BigDecimal N_ID_PRATICA) {
    this.N_ID_PRATICA = N_ID_PRATICA;
  }
  public userappl_t001_app_prt_pratiche with_N_ID_PRATICA(java.math.BigDecimal N_ID_PRATICA) {
    this.N_ID_PRATICA = N_ID_PRATICA;
    return this;
  }
  private String T_PROTOCOLLO;
  public String get_T_PROTOCOLLO() {
    return T_PROTOCOLLO;
  }
  public void set_T_PROTOCOLLO(String T_PROTOCOLLO) {
    this.T_PROTOCOLLO = T_PROTOCOLLO;
  }
  public userappl_t001_app_prt_pratiche with_T_PROTOCOLLO(String T_PROTOCOLLO) {
    this.T_PROTOCOLLO = T_PROTOCOLLO;
    return this;
  }
  private java.math.BigDecimal N_ID_DESCRITTORE_PROCESSO;
  public java.math.BigDecimal get_N_ID_DESCRITTORE_PROCESSO() {
    return N_ID_DESCRITTORE_PROCESSO;
  }
  public void set_N_ID_DESCRITTORE_PROCESSO(java.math.BigDecimal N_ID_DESCRITTORE_PROCESSO) {
    this.N_ID_DESCRITTORE_PROCESSO = N_ID_DESCRITTORE_PROCESSO;
  }
  public userappl_t001_app_prt_pratiche with_N_ID_DESCRITTORE_PROCESSO(java.math.BigDecimal N_ID_DESCRITTORE_PROCESSO) {
    this.N_ID_DESCRITTORE_PROCESSO = N_ID_DESCRITTORE_PROCESSO;
    return this;
  }
  private String T_STATO;
  public String get_T_STATO() {
    return T_STATO;
  }
  public void set_T_STATO(String T_STATO) {
    this.T_STATO = T_STATO;
  }
  public userappl_t001_app_prt_pratiche with_T_STATO(String T_STATO) {
    this.T_STATO = T_STATO;
    return this;
  }
  private java.math.BigDecimal N_ID_UTENTE;
  public java.math.BigDecimal get_N_ID_UTENTE() {
    return N_ID_UTENTE;
  }
  public void set_N_ID_UTENTE(java.math.BigDecimal N_ID_UTENTE) {
    this.N_ID_UTENTE = N_ID_UTENTE;
  }
  public userappl_t001_app_prt_pratiche with_N_ID_UTENTE(java.math.BigDecimal N_ID_UTENTE) {
    this.N_ID_UTENTE = N_ID_UTENTE;
    return this;
  }
  private java.math.BigDecimal N_ID_OPERATORE;
  public java.math.BigDecimal get_N_ID_OPERATORE() {
    return N_ID_OPERATORE;
  }
  public void set_N_ID_OPERATORE(java.math.BigDecimal N_ID_OPERATORE) {
    this.N_ID_OPERATORE = N_ID_OPERATORE;
  }
  public userappl_t001_app_prt_pratiche with_N_ID_OPERATORE(java.math.BigDecimal N_ID_OPERATORE) {
    this.N_ID_OPERATORE = N_ID_OPERATORE;
    return this;
  }
  private String D_DATA_APERTURA;
  public String get_D_DATA_APERTURA() {
    return D_DATA_APERTURA;
  }
  public void set_D_DATA_APERTURA(String D_DATA_APERTURA) {
    this.D_DATA_APERTURA = D_DATA_APERTURA;
  }
  public userappl_t001_app_prt_pratiche with_D_DATA_APERTURA(String D_DATA_APERTURA) {
    this.D_DATA_APERTURA = D_DATA_APERTURA;
    return this;
  }
  private String D_DATA_CHIUSURA;
  public String get_D_DATA_CHIUSURA() {
    return D_DATA_CHIUSURA;
  }
  public void set_D_DATA_CHIUSURA(String D_DATA_CHIUSURA) {
    this.D_DATA_CHIUSURA = D_DATA_CHIUSURA;
  }
  public userappl_t001_app_prt_pratiche with_D_DATA_CHIUSURA(String D_DATA_CHIUSURA) {
    this.D_DATA_CHIUSURA = D_DATA_CHIUSURA;
    return this;
  }
  private String T_POD;
  public String get_T_POD() {
    return T_POD;
  }
  public void set_T_POD(String T_POD) {
    this.T_POD = T_POD;
  }
  public userappl_t001_app_prt_pratiche with_T_POD(String T_POD) {
    this.T_POD = T_POD;
    return this;
  }
  private java.math.BigDecimal N_CONTATORE_MODIFICA;
  public java.math.BigDecimal get_N_CONTATORE_MODIFICA() {
    return N_CONTATORE_MODIFICA;
  }
  public void set_N_CONTATORE_MODIFICA(java.math.BigDecimal N_CONTATORE_MODIFICA) {
    this.N_CONTATORE_MODIFICA = N_CONTATORE_MODIFICA;
  }
  public userappl_t001_app_prt_pratiche with_N_CONTATORE_MODIFICA(java.math.BigDecimal N_CONTATORE_MODIFICA) {
    this.N_CONTATORE_MODIFICA = N_CONTATORE_MODIFICA;
    return this;
  }
  private String T_STATO_BUSINESS;
  public String get_T_STATO_BUSINESS() {
    return T_STATO_BUSINESS;
  }
  public void set_T_STATO_BUSINESS(String T_STATO_BUSINESS) {
    this.T_STATO_BUSINESS = T_STATO_BUSINESS;
  }
  public userappl_t001_app_prt_pratiche with_T_STATO_BUSINESS(String T_STATO_BUSINESS) {
    this.T_STATO_BUSINESS = T_STATO_BUSINESS;
    return this;
  }
  private String T_ARCHIVIATA;
  public String get_T_ARCHIVIATA() {
    return T_ARCHIVIATA;
  }
  public void set_T_ARCHIVIATA(String T_ARCHIVIATA) {
    this.T_ARCHIVIATA = T_ARCHIVIATA;
  }
  public userappl_t001_app_prt_pratiche with_T_ARCHIVIATA(String T_ARCHIVIATA) {
    this.T_ARCHIVIATA = T_ARCHIVIATA;
    return this;
  }
  private java.math.BigDecimal N_ID_UTENTE_MODIFICA;
  public java.math.BigDecimal get_N_ID_UTENTE_MODIFICA() {
    return N_ID_UTENTE_MODIFICA;
  }
  public void set_N_ID_UTENTE_MODIFICA(java.math.BigDecimal N_ID_UTENTE_MODIFICA) {
    this.N_ID_UTENTE_MODIFICA = N_ID_UTENTE_MODIFICA;
  }
  public userappl_t001_app_prt_pratiche with_N_ID_UTENTE_MODIFICA(java.math.BigDecimal N_ID_UTENTE_MODIFICA) {
    this.N_ID_UTENTE_MODIFICA = N_ID_UTENTE_MODIFICA;
    return this;
  }
  private java.math.BigDecimal N_ID_PRATICA_ORIGINE;
  public java.math.BigDecimal get_N_ID_PRATICA_ORIGINE() {
    return N_ID_PRATICA_ORIGINE;
  }
  public void set_N_ID_PRATICA_ORIGINE(java.math.BigDecimal N_ID_PRATICA_ORIGINE) {
    this.N_ID_PRATICA_ORIGINE = N_ID_PRATICA_ORIGINE;
  }
  public userappl_t001_app_prt_pratiche with_N_ID_PRATICA_ORIGINE(java.math.BigDecimal N_ID_PRATICA_ORIGINE) {
    this.N_ID_PRATICA_ORIGINE = N_ID_PRATICA_ORIGINE;
    return this;
  }
  private java.math.BigDecimal N_ID_OPERATORE_CHIUSURA;
  public java.math.BigDecimal get_N_ID_OPERATORE_CHIUSURA() {
    return N_ID_OPERATORE_CHIUSURA;
  }
  public void set_N_ID_OPERATORE_CHIUSURA(java.math.BigDecimal N_ID_OPERATORE_CHIUSURA) {
    this.N_ID_OPERATORE_CHIUSURA = N_ID_OPERATORE_CHIUSURA;
  }
  public userappl_t001_app_prt_pratiche with_N_ID_OPERATORE_CHIUSURA(java.math.BigDecimal N_ID_OPERATORE_CHIUSURA) {
    this.N_ID_OPERATORE_CHIUSURA = N_ID_OPERATORE_CHIUSURA;
    return this;
  }
  private String T_VISIBILE;
  public String get_T_VISIBILE() {
    return T_VISIBILE;
  }
  public void set_T_VISIBILE(String T_VISIBILE) {
    this.T_VISIBILE = T_VISIBILE;
  }
  public userappl_t001_app_prt_pratiche with_T_VISIBILE(String T_VISIBILE) {
    this.T_VISIBILE = T_VISIBILE;
    return this;
  }
  private String T_URL_ANNULL;
  public String get_T_URL_ANNULL() {
    return T_URL_ANNULL;
  }
  public void set_T_URL_ANNULL(String T_URL_ANNULL) {
    this.T_URL_ANNULL = T_URL_ANNULL;
  }
  public userappl_t001_app_prt_pratiche with_T_URL_ANNULL(String T_URL_ANNULL) {
    this.T_URL_ANNULL = T_URL_ANNULL;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof userappl_t001_app_prt_pratiche)) {
      return false;
    }
    userappl_t001_app_prt_pratiche that = (userappl_t001_app_prt_pratiche) o;
    boolean equal = true;
    equal = equal && (this.N_ID_PRATICA == null ? that.N_ID_PRATICA == null : this.N_ID_PRATICA.equals(that.N_ID_PRATICA));
    equal = equal && (this.T_PROTOCOLLO == null ? that.T_PROTOCOLLO == null : this.T_PROTOCOLLO.equals(that.T_PROTOCOLLO));
    equal = equal && (this.N_ID_DESCRITTORE_PROCESSO == null ? that.N_ID_DESCRITTORE_PROCESSO == null : this.N_ID_DESCRITTORE_PROCESSO.equals(that.N_ID_DESCRITTORE_PROCESSO));
    equal = equal && (this.T_STATO == null ? that.T_STATO == null : this.T_STATO.equals(that.T_STATO));
    equal = equal && (this.N_ID_UTENTE == null ? that.N_ID_UTENTE == null : this.N_ID_UTENTE.equals(that.N_ID_UTENTE));
    equal = equal && (this.N_ID_OPERATORE == null ? that.N_ID_OPERATORE == null : this.N_ID_OPERATORE.equals(that.N_ID_OPERATORE));
    equal = equal && (this.D_DATA_APERTURA == null ? that.D_DATA_APERTURA == null : this.D_DATA_APERTURA.equals(that.D_DATA_APERTURA));
    equal = equal && (this.D_DATA_CHIUSURA == null ? that.D_DATA_CHIUSURA == null : this.D_DATA_CHIUSURA.equals(that.D_DATA_CHIUSURA));
    equal = equal && (this.T_POD == null ? that.T_POD == null : this.T_POD.equals(that.T_POD));
    equal = equal && (this.N_CONTATORE_MODIFICA == null ? that.N_CONTATORE_MODIFICA == null : this.N_CONTATORE_MODIFICA.equals(that.N_CONTATORE_MODIFICA));
    equal = equal && (this.T_STATO_BUSINESS == null ? that.T_STATO_BUSINESS == null : this.T_STATO_BUSINESS.equals(that.T_STATO_BUSINESS));
    equal = equal && (this.T_ARCHIVIATA == null ? that.T_ARCHIVIATA == null : this.T_ARCHIVIATA.equals(that.T_ARCHIVIATA));
    equal = equal && (this.N_ID_UTENTE_MODIFICA == null ? that.N_ID_UTENTE_MODIFICA == null : this.N_ID_UTENTE_MODIFICA.equals(that.N_ID_UTENTE_MODIFICA));
    equal = equal && (this.N_ID_PRATICA_ORIGINE == null ? that.N_ID_PRATICA_ORIGINE == null : this.N_ID_PRATICA_ORIGINE.equals(that.N_ID_PRATICA_ORIGINE));
    equal = equal && (this.N_ID_OPERATORE_CHIUSURA == null ? that.N_ID_OPERATORE_CHIUSURA == null : this.N_ID_OPERATORE_CHIUSURA.equals(that.N_ID_OPERATORE_CHIUSURA));
    equal = equal && (this.T_VISIBILE == null ? that.T_VISIBILE == null : this.T_VISIBILE.equals(that.T_VISIBILE));
    equal = equal && (this.T_URL_ANNULL == null ? that.T_URL_ANNULL == null : this.T_URL_ANNULL.equals(that.T_URL_ANNULL));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof userappl_t001_app_prt_pratiche)) {
      return false;
    }
    userappl_t001_app_prt_pratiche that = (userappl_t001_app_prt_pratiche) o;
    boolean equal = true;
    equal = equal && (this.N_ID_PRATICA == null ? that.N_ID_PRATICA == null : this.N_ID_PRATICA.equals(that.N_ID_PRATICA));
    equal = equal && (this.T_PROTOCOLLO == null ? that.T_PROTOCOLLO == null : this.T_PROTOCOLLO.equals(that.T_PROTOCOLLO));
    equal = equal && (this.N_ID_DESCRITTORE_PROCESSO == null ? that.N_ID_DESCRITTORE_PROCESSO == null : this.N_ID_DESCRITTORE_PROCESSO.equals(that.N_ID_DESCRITTORE_PROCESSO));
    equal = equal && (this.T_STATO == null ? that.T_STATO == null : this.T_STATO.equals(that.T_STATO));
    equal = equal && (this.N_ID_UTENTE == null ? that.N_ID_UTENTE == null : this.N_ID_UTENTE.equals(that.N_ID_UTENTE));
    equal = equal && (this.N_ID_OPERATORE == null ? that.N_ID_OPERATORE == null : this.N_ID_OPERATORE.equals(that.N_ID_OPERATORE));
    equal = equal && (this.D_DATA_APERTURA == null ? that.D_DATA_APERTURA == null : this.D_DATA_APERTURA.equals(that.D_DATA_APERTURA));
    equal = equal && (this.D_DATA_CHIUSURA == null ? that.D_DATA_CHIUSURA == null : this.D_DATA_CHIUSURA.equals(that.D_DATA_CHIUSURA));
    equal = equal && (this.T_POD == null ? that.T_POD == null : this.T_POD.equals(that.T_POD));
    equal = equal && (this.N_CONTATORE_MODIFICA == null ? that.N_CONTATORE_MODIFICA == null : this.N_CONTATORE_MODIFICA.equals(that.N_CONTATORE_MODIFICA));
    equal = equal && (this.T_STATO_BUSINESS == null ? that.T_STATO_BUSINESS == null : this.T_STATO_BUSINESS.equals(that.T_STATO_BUSINESS));
    equal = equal && (this.T_ARCHIVIATA == null ? that.T_ARCHIVIATA == null : this.T_ARCHIVIATA.equals(that.T_ARCHIVIATA));
    equal = equal && (this.N_ID_UTENTE_MODIFICA == null ? that.N_ID_UTENTE_MODIFICA == null : this.N_ID_UTENTE_MODIFICA.equals(that.N_ID_UTENTE_MODIFICA));
    equal = equal && (this.N_ID_PRATICA_ORIGINE == null ? that.N_ID_PRATICA_ORIGINE == null : this.N_ID_PRATICA_ORIGINE.equals(that.N_ID_PRATICA_ORIGINE));
    equal = equal && (this.N_ID_OPERATORE_CHIUSURA == null ? that.N_ID_OPERATORE_CHIUSURA == null : this.N_ID_OPERATORE_CHIUSURA.equals(that.N_ID_OPERATORE_CHIUSURA));
    equal = equal && (this.T_VISIBILE == null ? that.T_VISIBILE == null : this.T_VISIBILE.equals(that.T_VISIBILE));
    equal = equal && (this.T_URL_ANNULL == null ? that.T_URL_ANNULL == null : this.T_URL_ANNULL.equals(that.T_URL_ANNULL));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_PRATICA = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.T_PROTOCOLLO = JdbcWritableBridge.readString(2, __dbResults);
    this.N_ID_DESCRITTORE_PROCESSO = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.T_STATO = JdbcWritableBridge.readString(4, __dbResults);
    this.N_ID_UTENTE = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.N_ID_OPERATORE = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.D_DATA_APERTURA = JdbcWritableBridge.readString(7, __dbResults);
    this.D_DATA_CHIUSURA = JdbcWritableBridge.readString(8, __dbResults);
    this.T_POD = JdbcWritableBridge.readString(9, __dbResults);
    this.N_CONTATORE_MODIFICA = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.T_STATO_BUSINESS = JdbcWritableBridge.readString(11, __dbResults);
    this.T_ARCHIVIATA = JdbcWritableBridge.readString(12, __dbResults);
    this.N_ID_UTENTE_MODIFICA = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.N_ID_PRATICA_ORIGINE = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.N_ID_OPERATORE_CHIUSURA = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.T_VISIBILE = JdbcWritableBridge.readString(16, __dbResults);
    this.T_URL_ANNULL = JdbcWritableBridge.readString(17, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_PRATICA = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.T_PROTOCOLLO = JdbcWritableBridge.readString(2, __dbResults);
    this.N_ID_DESCRITTORE_PROCESSO = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.T_STATO = JdbcWritableBridge.readString(4, __dbResults);
    this.N_ID_UTENTE = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.N_ID_OPERATORE = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.D_DATA_APERTURA = JdbcWritableBridge.readString(7, __dbResults);
    this.D_DATA_CHIUSURA = JdbcWritableBridge.readString(8, __dbResults);
    this.T_POD = JdbcWritableBridge.readString(9, __dbResults);
    this.N_CONTATORE_MODIFICA = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.T_STATO_BUSINESS = JdbcWritableBridge.readString(11, __dbResults);
    this.T_ARCHIVIATA = JdbcWritableBridge.readString(12, __dbResults);
    this.N_ID_UTENTE_MODIFICA = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.N_ID_PRATICA_ORIGINE = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.N_ID_OPERATORE_CHIUSURA = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.T_VISIBILE = JdbcWritableBridge.readString(16, __dbResults);
    this.T_URL_ANNULL = JdbcWritableBridge.readString(17, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_PRATICA, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_PROTOCOLLO, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DESCRITTORE_PROCESSO, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UTENTE, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_OPERATORE, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_APERTURA, 7 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_CHIUSURA, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_POD, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_CONTATORE_MODIFICA, 10 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO_BUSINESS, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ARCHIVIATA, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UTENTE_MODIFICA, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PRATICA_ORIGINE, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_OPERATORE_CHIUSURA, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_VISIBILE, 16 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_URL_ANNULL, 17 + __off, 12, __dbStmt);
    return 17;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_PRATICA, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_PROTOCOLLO, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DESCRITTORE_PROCESSO, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UTENTE, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_OPERATORE, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_APERTURA, 7 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_CHIUSURA, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_POD, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_CONTATORE_MODIFICA, 10 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_STATO_BUSINESS, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ARCHIVIATA, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UTENTE_MODIFICA, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PRATICA_ORIGINE, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_OPERATORE_CHIUSURA, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_VISIBILE, 16 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_URL_ANNULL, 17 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_PRATICA = null;
    } else {
    this.N_ID_PRATICA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PROTOCOLLO = null;
    } else {
    this.T_PROTOCOLLO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_DESCRITTORE_PROCESSO = null;
    } else {
    this.N_ID_DESCRITTORE_PROCESSO = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_STATO = null;
    } else {
    this.T_STATO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_UTENTE = null;
    } else {
    this.N_ID_UTENTE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_OPERATORE = null;
    } else {
    this.N_ID_OPERATORE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_APERTURA = null;
    } else {
    this.D_DATA_APERTURA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_CHIUSURA = null;
    } else {
    this.D_DATA_CHIUSURA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_POD = null;
    } else {
    this.T_POD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_CONTATORE_MODIFICA = null;
    } else {
    this.N_CONTATORE_MODIFICA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_STATO_BUSINESS = null;
    } else {
    this.T_STATO_BUSINESS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ARCHIVIATA = null;
    } else {
    this.T_ARCHIVIATA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_UTENTE_MODIFICA = null;
    } else {
    this.N_ID_UTENTE_MODIFICA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_PRATICA_ORIGINE = null;
    } else {
    this.N_ID_PRATICA_ORIGINE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_OPERATORE_CHIUSURA = null;
    } else {
    this.N_ID_OPERATORE_CHIUSURA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_VISIBILE = null;
    } else {
    this.T_VISIBILE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_URL_ANNULL = null;
    } else {
    this.T_URL_ANNULL = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_PRATICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PRATICA, __dataOut);
    }
    if (null == this.T_PROTOCOLLO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PROTOCOLLO);
    }
    if (null == this.N_ID_DESCRITTORE_PROCESSO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DESCRITTORE_PROCESSO, __dataOut);
    }
    if (null == this.T_STATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_STATO);
    }
    if (null == this.N_ID_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UTENTE, __dataOut);
    }
    if (null == this.N_ID_OPERATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_OPERATORE, __dataOut);
    }
    if (null == this.D_DATA_APERTURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_APERTURA);
    }
    if (null == this.D_DATA_CHIUSURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_CHIUSURA);
    }
    if (null == this.T_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_POD);
    }
    if (null == this.N_CONTATORE_MODIFICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_CONTATORE_MODIFICA, __dataOut);
    }
    if (null == this.T_STATO_BUSINESS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_STATO_BUSINESS);
    }
    if (null == this.T_ARCHIVIATA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ARCHIVIATA);
    }
    if (null == this.N_ID_UTENTE_MODIFICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UTENTE_MODIFICA, __dataOut);
    }
    if (null == this.N_ID_PRATICA_ORIGINE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PRATICA_ORIGINE, __dataOut);
    }
    if (null == this.N_ID_OPERATORE_CHIUSURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_OPERATORE_CHIUSURA, __dataOut);
    }
    if (null == this.T_VISIBILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_VISIBILE);
    }
    if (null == this.T_URL_ANNULL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_URL_ANNULL);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_PRATICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PRATICA, __dataOut);
    }
    if (null == this.T_PROTOCOLLO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PROTOCOLLO);
    }
    if (null == this.N_ID_DESCRITTORE_PROCESSO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DESCRITTORE_PROCESSO, __dataOut);
    }
    if (null == this.T_STATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_STATO);
    }
    if (null == this.N_ID_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UTENTE, __dataOut);
    }
    if (null == this.N_ID_OPERATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_OPERATORE, __dataOut);
    }
    if (null == this.D_DATA_APERTURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_APERTURA);
    }
    if (null == this.D_DATA_CHIUSURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_CHIUSURA);
    }
    if (null == this.T_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_POD);
    }
    if (null == this.N_CONTATORE_MODIFICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_CONTATORE_MODIFICA, __dataOut);
    }
    if (null == this.T_STATO_BUSINESS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_STATO_BUSINESS);
    }
    if (null == this.T_ARCHIVIATA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ARCHIVIATA);
    }
    if (null == this.N_ID_UTENTE_MODIFICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UTENTE_MODIFICA, __dataOut);
    }
    if (null == this.N_ID_PRATICA_ORIGINE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PRATICA_ORIGINE, __dataOut);
    }
    if (null == this.N_ID_OPERATORE_CHIUSURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_OPERATORE_CHIUSURA, __dataOut);
    }
    if (null == this.T_VISIBILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_VISIBILE);
    }
    if (null == this.T_URL_ANNULL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_URL_ANNULL);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PRATICA==null?"":N_ID_PRATICA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PROTOCOLLO==null?"":T_PROTOCOLLO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DESCRITTORE_PROCESSO==null?"":N_ID_DESCRITTORE_PROCESSO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO==null?"":T_STATO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UTENTE==null?"":N_ID_UTENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_OPERATORE==null?"":N_ID_OPERATORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_APERTURA==null?"":D_DATA_APERTURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_CHIUSURA==null?"":D_DATA_CHIUSURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_POD==null?"":T_POD, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_CONTATORE_MODIFICA==null?"":N_CONTATORE_MODIFICA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO_BUSINESS==null?"":T_STATO_BUSINESS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ARCHIVIATA==null?"":T_ARCHIVIATA, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UTENTE_MODIFICA==null?"":N_ID_UTENTE_MODIFICA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PRATICA_ORIGINE==null?"":N_ID_PRATICA_ORIGINE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_OPERATORE_CHIUSURA==null?"":N_ID_OPERATORE_CHIUSURA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_VISIBILE==null?"":T_VISIBILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_URL_ANNULL==null?"":T_URL_ANNULL, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PRATICA==null?"":N_ID_PRATICA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PROTOCOLLO==null?"":T_PROTOCOLLO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DESCRITTORE_PROCESSO==null?"":N_ID_DESCRITTORE_PROCESSO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO==null?"":T_STATO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UTENTE==null?"":N_ID_UTENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_OPERATORE==null?"":N_ID_OPERATORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_APERTURA==null?"":D_DATA_APERTURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_CHIUSURA==null?"":D_DATA_CHIUSURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_POD==null?"":T_POD, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_CONTATORE_MODIFICA==null?"":N_CONTATORE_MODIFICA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_STATO_BUSINESS==null?"":T_STATO_BUSINESS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ARCHIVIATA==null?"":T_ARCHIVIATA, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UTENTE_MODIFICA==null?"":N_ID_UTENTE_MODIFICA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PRATICA_ORIGINE==null?"":N_ID_PRATICA_ORIGINE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_OPERATORE_CHIUSURA==null?"":N_ID_OPERATORE_CHIUSURA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_VISIBILE==null?"":T_VISIBILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_URL_ANNULL==null?"":T_URL_ANNULL, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PRATICA = null; } else {
      this.N_ID_PRATICA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PROTOCOLLO = null; } else {
      this.T_PROTOCOLLO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DESCRITTORE_PROCESSO = null; } else {
      this.N_ID_DESCRITTORE_PROCESSO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_STATO = null; } else {
      this.T_STATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UTENTE = null; } else {
      this.N_ID_UTENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_OPERATORE = null; } else {
      this.N_ID_OPERATORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_APERTURA = null; } else {
      this.D_DATA_APERTURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_CHIUSURA = null; } else {
      this.D_DATA_CHIUSURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_POD = null; } else {
      this.T_POD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_CONTATORE_MODIFICA = null; } else {
      this.N_CONTATORE_MODIFICA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_STATO_BUSINESS = null; } else {
      this.T_STATO_BUSINESS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ARCHIVIATA = null; } else {
      this.T_ARCHIVIATA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UTENTE_MODIFICA = null; } else {
      this.N_ID_UTENTE_MODIFICA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PRATICA_ORIGINE = null; } else {
      this.N_ID_PRATICA_ORIGINE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_OPERATORE_CHIUSURA = null; } else {
      this.N_ID_OPERATORE_CHIUSURA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_VISIBILE = null; } else {
      this.T_VISIBILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_URL_ANNULL = null; } else {
      this.T_URL_ANNULL = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PRATICA = null; } else {
      this.N_ID_PRATICA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PROTOCOLLO = null; } else {
      this.T_PROTOCOLLO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DESCRITTORE_PROCESSO = null; } else {
      this.N_ID_DESCRITTORE_PROCESSO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_STATO = null; } else {
      this.T_STATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UTENTE = null; } else {
      this.N_ID_UTENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_OPERATORE = null; } else {
      this.N_ID_OPERATORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_APERTURA = null; } else {
      this.D_DATA_APERTURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_CHIUSURA = null; } else {
      this.D_DATA_CHIUSURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_POD = null; } else {
      this.T_POD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_CONTATORE_MODIFICA = null; } else {
      this.N_CONTATORE_MODIFICA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_STATO_BUSINESS = null; } else {
      this.T_STATO_BUSINESS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ARCHIVIATA = null; } else {
      this.T_ARCHIVIATA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UTENTE_MODIFICA = null; } else {
      this.N_ID_UTENTE_MODIFICA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PRATICA_ORIGINE = null; } else {
      this.N_ID_PRATICA_ORIGINE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_OPERATORE_CHIUSURA = null; } else {
      this.N_ID_OPERATORE_CHIUSURA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_VISIBILE = null; } else {
      this.T_VISIBILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_URL_ANNULL = null; } else {
      this.T_URL_ANNULL = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    userappl_t001_app_prt_pratiche o = (userappl_t001_app_prt_pratiche) super.clone();
    return o;
  }

  public void clone0(userappl_t001_app_prt_pratiche o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_PRATICA", this.N_ID_PRATICA);
    __sqoop$field_map.put("T_PROTOCOLLO", this.T_PROTOCOLLO);
    __sqoop$field_map.put("N_ID_DESCRITTORE_PROCESSO", this.N_ID_DESCRITTORE_PROCESSO);
    __sqoop$field_map.put("T_STATO", this.T_STATO);
    __sqoop$field_map.put("N_ID_UTENTE", this.N_ID_UTENTE);
    __sqoop$field_map.put("N_ID_OPERATORE", this.N_ID_OPERATORE);
    __sqoop$field_map.put("D_DATA_APERTURA", this.D_DATA_APERTURA);
    __sqoop$field_map.put("D_DATA_CHIUSURA", this.D_DATA_CHIUSURA);
    __sqoop$field_map.put("T_POD", this.T_POD);
    __sqoop$field_map.put("N_CONTATORE_MODIFICA", this.N_CONTATORE_MODIFICA);
    __sqoop$field_map.put("T_STATO_BUSINESS", this.T_STATO_BUSINESS);
    __sqoop$field_map.put("T_ARCHIVIATA", this.T_ARCHIVIATA);
    __sqoop$field_map.put("N_ID_UTENTE_MODIFICA", this.N_ID_UTENTE_MODIFICA);
    __sqoop$field_map.put("N_ID_PRATICA_ORIGINE", this.N_ID_PRATICA_ORIGINE);
    __sqoop$field_map.put("N_ID_OPERATORE_CHIUSURA", this.N_ID_OPERATORE_CHIUSURA);
    __sqoop$field_map.put("T_VISIBILE", this.T_VISIBILE);
    __sqoop$field_map.put("T_URL_ANNULL", this.T_URL_ANNULL);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_PRATICA", this.N_ID_PRATICA);
    __sqoop$field_map.put("T_PROTOCOLLO", this.T_PROTOCOLLO);
    __sqoop$field_map.put("N_ID_DESCRITTORE_PROCESSO", this.N_ID_DESCRITTORE_PROCESSO);
    __sqoop$field_map.put("T_STATO", this.T_STATO);
    __sqoop$field_map.put("N_ID_UTENTE", this.N_ID_UTENTE);
    __sqoop$field_map.put("N_ID_OPERATORE", this.N_ID_OPERATORE);
    __sqoop$field_map.put("D_DATA_APERTURA", this.D_DATA_APERTURA);
    __sqoop$field_map.put("D_DATA_CHIUSURA", this.D_DATA_CHIUSURA);
    __sqoop$field_map.put("T_POD", this.T_POD);
    __sqoop$field_map.put("N_CONTATORE_MODIFICA", this.N_CONTATORE_MODIFICA);
    __sqoop$field_map.put("T_STATO_BUSINESS", this.T_STATO_BUSINESS);
    __sqoop$field_map.put("T_ARCHIVIATA", this.T_ARCHIVIATA);
    __sqoop$field_map.put("N_ID_UTENTE_MODIFICA", this.N_ID_UTENTE_MODIFICA);
    __sqoop$field_map.put("N_ID_PRATICA_ORIGINE", this.N_ID_PRATICA_ORIGINE);
    __sqoop$field_map.put("N_ID_OPERATORE_CHIUSURA", this.N_ID_OPERATORE_CHIUSURA);
    __sqoop$field_map.put("T_VISIBILE", this.T_VISIBILE);
    __sqoop$field_map.put("T_URL_ANNULL", this.T_URL_ANNULL);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
