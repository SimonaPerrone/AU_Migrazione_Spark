// ORM class for table 'rcugas.rcugas_pdr_datiprelievo'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 12:25:08 CEST 2019
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

public class rcugas_rcugas_pdr_datiprelievo extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_PDR_DATIPRELIEVO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_PDR_DATIPRELIEVO = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_PDR = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_ANNO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ANNO = (String)value;
      }
    });
    setters.put("T_COD_PROFILO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COD_PROFILO = (String)value;
      }
    });
    setters.put("N_PRELIEVO_ANNUO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_PRELIEVO_ANNUO = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_LETTURA_CONVERTITORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_LETTURA_CONVERTITORE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_COD_CAT_USO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COD_CAT_USO = (String)value;
      }
    });
    setters.put("T_COD_CLASSE_PRELIEVO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COD_CLASSE_PRELIEVO = (String)value;
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
    setters.put("T_ANNO_MESE_RIF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ANNO_MESE_RIF = (String)value;
      }
    });
    setters.put("T_FATTORE_CORREZ_CLIMATICA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_FATTORE_CORREZ_CLIMATICA = (String)value;
      }
    });
    setters.put("T_TRATTAMENTO_SETTLEMENT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TRATTAMENTO_SETTLEMENT = (String)value;
      }
    });
  }
  public rcugas_rcugas_pdr_datiprelievo() {
    init0();
  }
  private java.math.BigDecimal N_ID_PDR_DATIPRELIEVO;
  public java.math.BigDecimal get_N_ID_PDR_DATIPRELIEVO() {
    return N_ID_PDR_DATIPRELIEVO;
  }
  public void set_N_ID_PDR_DATIPRELIEVO(java.math.BigDecimal N_ID_PDR_DATIPRELIEVO) {
    this.N_ID_PDR_DATIPRELIEVO = N_ID_PDR_DATIPRELIEVO;
  }
  public rcugas_rcugas_pdr_datiprelievo with_N_ID_PDR_DATIPRELIEVO(java.math.BigDecimal N_ID_PDR_DATIPRELIEVO) {
    this.N_ID_PDR_DATIPRELIEVO = N_ID_PDR_DATIPRELIEVO;
    return this;
  }
  private java.math.BigDecimal N_ID_PDR;
  public java.math.BigDecimal get_N_ID_PDR() {
    return N_ID_PDR;
  }
  public void set_N_ID_PDR(java.math.BigDecimal N_ID_PDR) {
    this.N_ID_PDR = N_ID_PDR;
  }
  public rcugas_rcugas_pdr_datiprelievo with_N_ID_PDR(java.math.BigDecimal N_ID_PDR) {
    this.N_ID_PDR = N_ID_PDR;
    return this;
  }
  private String T_ANNO;
  public String get_T_ANNO() {
    return T_ANNO;
  }
  public void set_T_ANNO(String T_ANNO) {
    this.T_ANNO = T_ANNO;
  }
  public rcugas_rcugas_pdr_datiprelievo with_T_ANNO(String T_ANNO) {
    this.T_ANNO = T_ANNO;
    return this;
  }
  private String T_COD_PROFILO;
  public String get_T_COD_PROFILO() {
    return T_COD_PROFILO;
  }
  public void set_T_COD_PROFILO(String T_COD_PROFILO) {
    this.T_COD_PROFILO = T_COD_PROFILO;
  }
  public rcugas_rcugas_pdr_datiprelievo with_T_COD_PROFILO(String T_COD_PROFILO) {
    this.T_COD_PROFILO = T_COD_PROFILO;
    return this;
  }
  private java.math.BigDecimal N_PRELIEVO_ANNUO;
  public java.math.BigDecimal get_N_PRELIEVO_ANNUO() {
    return N_PRELIEVO_ANNUO;
  }
  public void set_N_PRELIEVO_ANNUO(java.math.BigDecimal N_PRELIEVO_ANNUO) {
    this.N_PRELIEVO_ANNUO = N_PRELIEVO_ANNUO;
  }
  public rcugas_rcugas_pdr_datiprelievo with_N_PRELIEVO_ANNUO(java.math.BigDecimal N_PRELIEVO_ANNUO) {
    this.N_PRELIEVO_ANNUO = N_PRELIEVO_ANNUO;
    return this;
  }
  private java.math.BigDecimal N_LETTURA_CONVERTITORE;
  public java.math.BigDecimal get_N_LETTURA_CONVERTITORE() {
    return N_LETTURA_CONVERTITORE;
  }
  public void set_N_LETTURA_CONVERTITORE(java.math.BigDecimal N_LETTURA_CONVERTITORE) {
    this.N_LETTURA_CONVERTITORE = N_LETTURA_CONVERTITORE;
  }
  public rcugas_rcugas_pdr_datiprelievo with_N_LETTURA_CONVERTITORE(java.math.BigDecimal N_LETTURA_CONVERTITORE) {
    this.N_LETTURA_CONVERTITORE = N_LETTURA_CONVERTITORE;
    return this;
  }
  private String T_COD_CAT_USO;
  public String get_T_COD_CAT_USO() {
    return T_COD_CAT_USO;
  }
  public void set_T_COD_CAT_USO(String T_COD_CAT_USO) {
    this.T_COD_CAT_USO = T_COD_CAT_USO;
  }
  public rcugas_rcugas_pdr_datiprelievo with_T_COD_CAT_USO(String T_COD_CAT_USO) {
    this.T_COD_CAT_USO = T_COD_CAT_USO;
    return this;
  }
  private String T_COD_CLASSE_PRELIEVO;
  public String get_T_COD_CLASSE_PRELIEVO() {
    return T_COD_CLASSE_PRELIEVO;
  }
  public void set_T_COD_CLASSE_PRELIEVO(String T_COD_CLASSE_PRELIEVO) {
    this.T_COD_CLASSE_PRELIEVO = T_COD_CLASSE_PRELIEVO;
  }
  public rcugas_rcugas_pdr_datiprelievo with_T_COD_CLASSE_PRELIEVO(String T_COD_CLASSE_PRELIEVO) {
    this.T_COD_CLASSE_PRELIEVO = T_COD_CLASSE_PRELIEVO;
    return this;
  }
  private String T_NOTE;
  public String get_T_NOTE() {
    return T_NOTE;
  }
  public void set_T_NOTE(String T_NOTE) {
    this.T_NOTE = T_NOTE;
  }
  public rcugas_rcugas_pdr_datiprelievo with_T_NOTE(String T_NOTE) {
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
  public rcugas_rcugas_pdr_datiprelievo with_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
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
  public rcugas_rcugas_pdr_datiprelievo with_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
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
  public rcugas_rcugas_pdr_datiprelievo with_N_ID_S_PREC(java.math.BigDecimal N_ID_S_PREC) {
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
  public rcugas_rcugas_pdr_datiprelievo with_D_DATA_RIF(String D_DATA_RIF) {
    this.D_DATA_RIF = D_DATA_RIF;
    return this;
  }
  private String T_ANNO_MESE_RIF;
  public String get_T_ANNO_MESE_RIF() {
    return T_ANNO_MESE_RIF;
  }
  public void set_T_ANNO_MESE_RIF(String T_ANNO_MESE_RIF) {
    this.T_ANNO_MESE_RIF = T_ANNO_MESE_RIF;
  }
  public rcugas_rcugas_pdr_datiprelievo with_T_ANNO_MESE_RIF(String T_ANNO_MESE_RIF) {
    this.T_ANNO_MESE_RIF = T_ANNO_MESE_RIF;
    return this;
  }
  private String T_FATTORE_CORREZ_CLIMATICA;
  public String get_T_FATTORE_CORREZ_CLIMATICA() {
    return T_FATTORE_CORREZ_CLIMATICA;
  }
  public void set_T_FATTORE_CORREZ_CLIMATICA(String T_FATTORE_CORREZ_CLIMATICA) {
    this.T_FATTORE_CORREZ_CLIMATICA = T_FATTORE_CORREZ_CLIMATICA;
  }
  public rcugas_rcugas_pdr_datiprelievo with_T_FATTORE_CORREZ_CLIMATICA(String T_FATTORE_CORREZ_CLIMATICA) {
    this.T_FATTORE_CORREZ_CLIMATICA = T_FATTORE_CORREZ_CLIMATICA;
    return this;
  }
  private String T_TRATTAMENTO_SETTLEMENT;
  public String get_T_TRATTAMENTO_SETTLEMENT() {
    return T_TRATTAMENTO_SETTLEMENT;
  }
  public void set_T_TRATTAMENTO_SETTLEMENT(String T_TRATTAMENTO_SETTLEMENT) {
    this.T_TRATTAMENTO_SETTLEMENT = T_TRATTAMENTO_SETTLEMENT;
  }
  public rcugas_rcugas_pdr_datiprelievo with_T_TRATTAMENTO_SETTLEMENT(String T_TRATTAMENTO_SETTLEMENT) {
    this.T_TRATTAMENTO_SETTLEMENT = T_TRATTAMENTO_SETTLEMENT;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcugas_rcugas_pdr_datiprelievo)) {
      return false;
    }
    rcugas_rcugas_pdr_datiprelievo that = (rcugas_rcugas_pdr_datiprelievo) o;
    boolean equal = true;
    equal = equal && (this.N_ID_PDR_DATIPRELIEVO == null ? that.N_ID_PDR_DATIPRELIEVO == null : this.N_ID_PDR_DATIPRELIEVO.equals(that.N_ID_PDR_DATIPRELIEVO));
    equal = equal && (this.N_ID_PDR == null ? that.N_ID_PDR == null : this.N_ID_PDR.equals(that.N_ID_PDR));
    equal = equal && (this.T_ANNO == null ? that.T_ANNO == null : this.T_ANNO.equals(that.T_ANNO));
    equal = equal && (this.T_COD_PROFILO == null ? that.T_COD_PROFILO == null : this.T_COD_PROFILO.equals(that.T_COD_PROFILO));
    equal = equal && (this.N_PRELIEVO_ANNUO == null ? that.N_PRELIEVO_ANNUO == null : this.N_PRELIEVO_ANNUO.equals(that.N_PRELIEVO_ANNUO));
    equal = equal && (this.N_LETTURA_CONVERTITORE == null ? that.N_LETTURA_CONVERTITORE == null : this.N_LETTURA_CONVERTITORE.equals(that.N_LETTURA_CONVERTITORE));
    equal = equal && (this.T_COD_CAT_USO == null ? that.T_COD_CAT_USO == null : this.T_COD_CAT_USO.equals(that.T_COD_CAT_USO));
    equal = equal && (this.T_COD_CLASSE_PRELIEVO == null ? that.T_COD_CLASSE_PRELIEVO == null : this.T_COD_CLASSE_PRELIEVO.equals(that.T_COD_CLASSE_PRELIEVO));
    equal = equal && (this.T_NOTE == null ? that.T_NOTE == null : this.T_NOTE.equals(that.T_NOTE));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.D_DATA_RIF == null ? that.D_DATA_RIF == null : this.D_DATA_RIF.equals(that.D_DATA_RIF));
    equal = equal && (this.T_ANNO_MESE_RIF == null ? that.T_ANNO_MESE_RIF == null : this.T_ANNO_MESE_RIF.equals(that.T_ANNO_MESE_RIF));
    equal = equal && (this.T_FATTORE_CORREZ_CLIMATICA == null ? that.T_FATTORE_CORREZ_CLIMATICA == null : this.T_FATTORE_CORREZ_CLIMATICA.equals(that.T_FATTORE_CORREZ_CLIMATICA));
    equal = equal && (this.T_TRATTAMENTO_SETTLEMENT == null ? that.T_TRATTAMENTO_SETTLEMENT == null : this.T_TRATTAMENTO_SETTLEMENT.equals(that.T_TRATTAMENTO_SETTLEMENT));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcugas_rcugas_pdr_datiprelievo)) {
      return false;
    }
    rcugas_rcugas_pdr_datiprelievo that = (rcugas_rcugas_pdr_datiprelievo) o;
    boolean equal = true;
    equal = equal && (this.N_ID_PDR_DATIPRELIEVO == null ? that.N_ID_PDR_DATIPRELIEVO == null : this.N_ID_PDR_DATIPRELIEVO.equals(that.N_ID_PDR_DATIPRELIEVO));
    equal = equal && (this.N_ID_PDR == null ? that.N_ID_PDR == null : this.N_ID_PDR.equals(that.N_ID_PDR));
    equal = equal && (this.T_ANNO == null ? that.T_ANNO == null : this.T_ANNO.equals(that.T_ANNO));
    equal = equal && (this.T_COD_PROFILO == null ? that.T_COD_PROFILO == null : this.T_COD_PROFILO.equals(that.T_COD_PROFILO));
    equal = equal && (this.N_PRELIEVO_ANNUO == null ? that.N_PRELIEVO_ANNUO == null : this.N_PRELIEVO_ANNUO.equals(that.N_PRELIEVO_ANNUO));
    equal = equal && (this.N_LETTURA_CONVERTITORE == null ? that.N_LETTURA_CONVERTITORE == null : this.N_LETTURA_CONVERTITORE.equals(that.N_LETTURA_CONVERTITORE));
    equal = equal && (this.T_COD_CAT_USO == null ? that.T_COD_CAT_USO == null : this.T_COD_CAT_USO.equals(that.T_COD_CAT_USO));
    equal = equal && (this.T_COD_CLASSE_PRELIEVO == null ? that.T_COD_CLASSE_PRELIEVO == null : this.T_COD_CLASSE_PRELIEVO.equals(that.T_COD_CLASSE_PRELIEVO));
    equal = equal && (this.T_NOTE == null ? that.T_NOTE == null : this.T_NOTE.equals(that.T_NOTE));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.D_DATA_RIF == null ? that.D_DATA_RIF == null : this.D_DATA_RIF.equals(that.D_DATA_RIF));
    equal = equal && (this.T_ANNO_MESE_RIF == null ? that.T_ANNO_MESE_RIF == null : this.T_ANNO_MESE_RIF.equals(that.T_ANNO_MESE_RIF));
    equal = equal && (this.T_FATTORE_CORREZ_CLIMATICA == null ? that.T_FATTORE_CORREZ_CLIMATICA == null : this.T_FATTORE_CORREZ_CLIMATICA.equals(that.T_FATTORE_CORREZ_CLIMATICA));
    equal = equal && (this.T_TRATTAMENTO_SETTLEMENT == null ? that.T_TRATTAMENTO_SETTLEMENT == null : this.T_TRATTAMENTO_SETTLEMENT.equals(that.T_TRATTAMENTO_SETTLEMENT));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_PDR_DATIPRELIEVO = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_PDR = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_ANNO = JdbcWritableBridge.readString(3, __dbResults);
    this.T_COD_PROFILO = JdbcWritableBridge.readString(4, __dbResults);
    this.N_PRELIEVO_ANNUO = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.N_LETTURA_CONVERTITORE = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.T_COD_CAT_USO = JdbcWritableBridge.readString(7, __dbResults);
    this.T_COD_CLASSE_PRELIEVO = JdbcWritableBridge.readString(8, __dbResults);
    this.T_NOTE = JdbcWritableBridge.readString(9, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(10, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(11, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.D_DATA_RIF = JdbcWritableBridge.readString(13, __dbResults);
    this.T_ANNO_MESE_RIF = JdbcWritableBridge.readString(14, __dbResults);
    this.T_FATTORE_CORREZ_CLIMATICA = JdbcWritableBridge.readString(15, __dbResults);
    this.T_TRATTAMENTO_SETTLEMENT = JdbcWritableBridge.readString(16, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_PDR_DATIPRELIEVO = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_PDR = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_ANNO = JdbcWritableBridge.readString(3, __dbResults);
    this.T_COD_PROFILO = JdbcWritableBridge.readString(4, __dbResults);
    this.N_PRELIEVO_ANNUO = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.N_LETTURA_CONVERTITORE = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.T_COD_CAT_USO = JdbcWritableBridge.readString(7, __dbResults);
    this.T_COD_CLASSE_PRELIEVO = JdbcWritableBridge.readString(8, __dbResults);
    this.T_NOTE = JdbcWritableBridge.readString(9, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(10, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(11, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.D_DATA_RIF = JdbcWritableBridge.readString(13, __dbResults);
    this.T_ANNO_MESE_RIF = JdbcWritableBridge.readString(14, __dbResults);
    this.T_FATTORE_CORREZ_CLIMATICA = JdbcWritableBridge.readString(15, __dbResults);
    this.T_TRATTAMENTO_SETTLEMENT = JdbcWritableBridge.readString(16, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_PDR_DATIPRELIEVO, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PDR, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_PROFILO, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_PRELIEVO_ANNUO, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_LETTURA_CONVERTITORE, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CAT_USO, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CLASSE_PRELIEVO, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTE, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 10 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 11 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 12 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF, 13 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO_MESE_RIF, 14 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_FATTORE_CORREZ_CLIMATICA, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TRATTAMENTO_SETTLEMENT, 16 + __off, 12, __dbStmt);
    return 16;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_PDR_DATIPRELIEVO, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PDR, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_PROFILO, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_PRELIEVO_ANNUO, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_LETTURA_CONVERTITORE, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CAT_USO, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CLASSE_PRELIEVO, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTE, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 10 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 11 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 12 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF, 13 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO_MESE_RIF, 14 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_FATTORE_CORREZ_CLIMATICA, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TRATTAMENTO_SETTLEMENT, 16 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_PDR_DATIPRELIEVO = null;
    } else {
    this.N_ID_PDR_DATIPRELIEVO = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_PDR = null;
    } else {
    this.N_ID_PDR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ANNO = null;
    } else {
    this.T_ANNO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COD_PROFILO = null;
    } else {
    this.T_COD_PROFILO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_PRELIEVO_ANNUO = null;
    } else {
    this.N_PRELIEVO_ANNUO = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_LETTURA_CONVERTITORE = null;
    } else {
    this.N_LETTURA_CONVERTITORE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COD_CAT_USO = null;
    } else {
    this.T_COD_CAT_USO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COD_CLASSE_PRELIEVO = null;
    } else {
    this.T_COD_CLASSE_PRELIEVO = Text.readString(__dataIn);
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
    if (__dataIn.readBoolean()) { 
        this.T_ANNO_MESE_RIF = null;
    } else {
    this.T_ANNO_MESE_RIF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_FATTORE_CORREZ_CLIMATICA = null;
    } else {
    this.T_FATTORE_CORREZ_CLIMATICA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TRATTAMENTO_SETTLEMENT = null;
    } else {
    this.T_TRATTAMENTO_SETTLEMENT = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_PDR_DATIPRELIEVO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PDR_DATIPRELIEVO, __dataOut);
    }
    if (null == this.N_ID_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PDR, __dataOut);
    }
    if (null == this.T_ANNO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO);
    }
    if (null == this.T_COD_PROFILO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_PROFILO);
    }
    if (null == this.N_PRELIEVO_ANNUO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_PRELIEVO_ANNUO, __dataOut);
    }
    if (null == this.N_LETTURA_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_LETTURA_CONVERTITORE, __dataOut);
    }
    if (null == this.T_COD_CAT_USO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_CAT_USO);
    }
    if (null == this.T_COD_CLASSE_PRELIEVO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_CLASSE_PRELIEVO);
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
    if (null == this.T_ANNO_MESE_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO_MESE_RIF);
    }
    if (null == this.T_FATTORE_CORREZ_CLIMATICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_FATTORE_CORREZ_CLIMATICA);
    }
    if (null == this.T_TRATTAMENTO_SETTLEMENT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TRATTAMENTO_SETTLEMENT);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_PDR_DATIPRELIEVO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PDR_DATIPRELIEVO, __dataOut);
    }
    if (null == this.N_ID_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PDR, __dataOut);
    }
    if (null == this.T_ANNO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO);
    }
    if (null == this.T_COD_PROFILO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_PROFILO);
    }
    if (null == this.N_PRELIEVO_ANNUO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_PRELIEVO_ANNUO, __dataOut);
    }
    if (null == this.N_LETTURA_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_LETTURA_CONVERTITORE, __dataOut);
    }
    if (null == this.T_COD_CAT_USO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_CAT_USO);
    }
    if (null == this.T_COD_CLASSE_PRELIEVO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_CLASSE_PRELIEVO);
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
    if (null == this.T_ANNO_MESE_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO_MESE_RIF);
    }
    if (null == this.T_FATTORE_CORREZ_CLIMATICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_FATTORE_CORREZ_CLIMATICA);
    }
    if (null == this.T_TRATTAMENTO_SETTLEMENT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TRATTAMENTO_SETTLEMENT);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PDR_DATIPRELIEVO==null?"":N_ID_PDR_DATIPRELIEVO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PDR==null?"":N_ID_PDR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO==null?"":T_ANNO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_PROFILO==null?"":T_COD_PROFILO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_PRELIEVO_ANNUO==null?"":N_PRELIEVO_ANNUO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_LETTURA_CONVERTITORE==null?"":N_LETTURA_CONVERTITORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CAT_USO==null?"":T_COD_CAT_USO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CLASSE_PRELIEVO==null?"":T_COD_CLASSE_PRELIEVO, " ", delimiters));
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
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO_MESE_RIF==null?"":T_ANNO_MESE_RIF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_FATTORE_CORREZ_CLIMATICA==null?"":T_FATTORE_CORREZ_CLIMATICA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TRATTAMENTO_SETTLEMENT==null?"":T_TRATTAMENTO_SETTLEMENT, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PDR_DATIPRELIEVO==null?"":N_ID_PDR_DATIPRELIEVO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PDR==null?"":N_ID_PDR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO==null?"":T_ANNO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_PROFILO==null?"":T_COD_PROFILO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_PRELIEVO_ANNUO==null?"":N_PRELIEVO_ANNUO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_LETTURA_CONVERTITORE==null?"":N_LETTURA_CONVERTITORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CAT_USO==null?"":T_COD_CAT_USO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CLASSE_PRELIEVO==null?"":T_COD_CLASSE_PRELIEVO, " ", delimiters));
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
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO_MESE_RIF==null?"":T_ANNO_MESE_RIF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_FATTORE_CORREZ_CLIMATICA==null?"":T_FATTORE_CORREZ_CLIMATICA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TRATTAMENTO_SETTLEMENT==null?"":T_TRATTAMENTO_SETTLEMENT, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PDR_DATIPRELIEVO = null; } else {
      this.N_ID_PDR_DATIPRELIEVO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PDR = null; } else {
      this.N_ID_PDR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO = null; } else {
      this.T_ANNO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_PROFILO = null; } else {
      this.T_COD_PROFILO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_PRELIEVO_ANNUO = null; } else {
      this.N_PRELIEVO_ANNUO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_LETTURA_CONVERTITORE = null; } else {
      this.N_LETTURA_CONVERTITORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_CAT_USO = null; } else {
      this.T_COD_CAT_USO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_CLASSE_PRELIEVO = null; } else {
      this.T_COD_CLASSE_PRELIEVO = __cur_str;
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

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO_MESE_RIF = null; } else {
      this.T_ANNO_MESE_RIF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_FATTORE_CORREZ_CLIMATICA = null; } else {
      this.T_FATTORE_CORREZ_CLIMATICA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TRATTAMENTO_SETTLEMENT = null; } else {
      this.T_TRATTAMENTO_SETTLEMENT = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PDR_DATIPRELIEVO = null; } else {
      this.N_ID_PDR_DATIPRELIEVO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PDR = null; } else {
      this.N_ID_PDR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO = null; } else {
      this.T_ANNO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_PROFILO = null; } else {
      this.T_COD_PROFILO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_PRELIEVO_ANNUO = null; } else {
      this.N_PRELIEVO_ANNUO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_LETTURA_CONVERTITORE = null; } else {
      this.N_LETTURA_CONVERTITORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_CAT_USO = null; } else {
      this.T_COD_CAT_USO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_CLASSE_PRELIEVO = null; } else {
      this.T_COD_CLASSE_PRELIEVO = __cur_str;
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

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO_MESE_RIF = null; } else {
      this.T_ANNO_MESE_RIF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_FATTORE_CORREZ_CLIMATICA = null; } else {
      this.T_FATTORE_CORREZ_CLIMATICA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TRATTAMENTO_SETTLEMENT = null; } else {
      this.T_TRATTAMENTO_SETTLEMENT = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    rcugas_rcugas_pdr_datiprelievo o = (rcugas_rcugas_pdr_datiprelievo) super.clone();
    return o;
  }

  public void clone0(rcugas_rcugas_pdr_datiprelievo o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_PDR_DATIPRELIEVO", this.N_ID_PDR_DATIPRELIEVO);
    __sqoop$field_map.put("N_ID_PDR", this.N_ID_PDR);
    __sqoop$field_map.put("T_ANNO", this.T_ANNO);
    __sqoop$field_map.put("T_COD_PROFILO", this.T_COD_PROFILO);
    __sqoop$field_map.put("N_PRELIEVO_ANNUO", this.N_PRELIEVO_ANNUO);
    __sqoop$field_map.put("N_LETTURA_CONVERTITORE", this.N_LETTURA_CONVERTITORE);
    __sqoop$field_map.put("T_COD_CAT_USO", this.T_COD_CAT_USO);
    __sqoop$field_map.put("T_COD_CLASSE_PRELIEVO", this.T_COD_CLASSE_PRELIEVO);
    __sqoop$field_map.put("T_NOTE", this.T_NOTE);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("D_DATA_RIF", this.D_DATA_RIF);
    __sqoop$field_map.put("T_ANNO_MESE_RIF", this.T_ANNO_MESE_RIF);
    __sqoop$field_map.put("T_FATTORE_CORREZ_CLIMATICA", this.T_FATTORE_CORREZ_CLIMATICA);
    __sqoop$field_map.put("T_TRATTAMENTO_SETTLEMENT", this.T_TRATTAMENTO_SETTLEMENT);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_PDR_DATIPRELIEVO", this.N_ID_PDR_DATIPRELIEVO);
    __sqoop$field_map.put("N_ID_PDR", this.N_ID_PDR);
    __sqoop$field_map.put("T_ANNO", this.T_ANNO);
    __sqoop$field_map.put("T_COD_PROFILO", this.T_COD_PROFILO);
    __sqoop$field_map.put("N_PRELIEVO_ANNUO", this.N_PRELIEVO_ANNUO);
    __sqoop$field_map.put("N_LETTURA_CONVERTITORE", this.N_LETTURA_CONVERTITORE);
    __sqoop$field_map.put("T_COD_CAT_USO", this.T_COD_CAT_USO);
    __sqoop$field_map.put("T_COD_CLASSE_PRELIEVO", this.T_COD_CLASSE_PRELIEVO);
    __sqoop$field_map.put("T_NOTE", this.T_NOTE);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("D_DATA_RIF", this.D_DATA_RIF);
    __sqoop$field_map.put("T_ANNO_MESE_RIF", this.T_ANNO_MESE_RIF);
    __sqoop$field_map.put("T_FATTORE_CORREZ_CLIMATICA", this.T_FATTORE_CORREZ_CLIMATICA);
    __sqoop$field_map.put("T_TRATTAMENTO_SETTLEMENT", this.T_TRATTAMENTO_SETTLEMENT);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
