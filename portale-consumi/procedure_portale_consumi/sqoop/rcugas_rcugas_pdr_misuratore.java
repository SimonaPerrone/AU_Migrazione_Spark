// ORM class for table 'rcugas.rcugas_pdr_misuratore'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 12:33:02 CEST 2019
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

public class rcugas_rcugas_pdr_misuratore extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_PDR_MISURATORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_PDR_MISURATORE = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_PDR = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_MATRICOLA_MISURATORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MATRICOLA_MISURATORE = (String)value;
      }
    });
    setters.put("T_TIPO_MISURATORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO_MISURATORE = (String)value;
      }
    });
    setters.put("T_TELEGESTITO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TELEGESTITO = (String)value;
      }
    });
    setters.put("N_COEFF_CORREZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_COEFF_CORREZIONE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_CLASSE_MISURATORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CLASSE_MISURATORE = (String)value;
      }
    });
    setters.put("T_ACCESS_MISURATORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ACCESS_MISURATORE = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_NUM_CIFRE_MISURATORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_NUM_CIFRE_MISURATORE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_ANNO_FABBRIC_MISURATORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ANNO_FABBRIC_MISURATORE = (String)value;
      }
    });
    setters.put("T_DATA_INST_MISURATORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_DATA_INST_MISURATORE = (String)value;
      }
    });
    setters.put("T_MISURATORE_INTEGRATO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MISURATORE_INTEGRATO = (String)value;
      }
    });
    setters.put("T_PRESENZA_CONVERTITORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PRESENZA_CONVERTITORE = (String)value;
      }
    });
    setters.put("T_MATRICOLA_CONVERTITORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MATRICOLA_CONVERTITORE = (String)value;
      }
    });
    setters.put("N_NUM_CIFRE_CONVERTITORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_NUM_CIFRE_CONVERTITORE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_ANNO_FABBRIC_CONVERTITORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ANNO_FABBRIC_CONVERTITORE = (String)value;
      }
    });
    setters.put("T_DATA_INST_CONVERTITORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_DATA_INST_CONVERTITORE = (String)value;
      }
    });
    setters.put("N_LETTURA_CONVERTITORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_LETTURA_CONVERTITORE = (java.math.BigDecimal)value;
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
  }
  public rcugas_rcugas_pdr_misuratore() {
    init0();
  }
  private java.math.BigDecimal N_ID_PDR_MISURATORE;
  public java.math.BigDecimal get_N_ID_PDR_MISURATORE() {
    return N_ID_PDR_MISURATORE;
  }
  public void set_N_ID_PDR_MISURATORE(java.math.BigDecimal N_ID_PDR_MISURATORE) {
    this.N_ID_PDR_MISURATORE = N_ID_PDR_MISURATORE;
  }
  public rcugas_rcugas_pdr_misuratore with_N_ID_PDR_MISURATORE(java.math.BigDecimal N_ID_PDR_MISURATORE) {
    this.N_ID_PDR_MISURATORE = N_ID_PDR_MISURATORE;
    return this;
  }
  private java.math.BigDecimal N_ID_PDR;
  public java.math.BigDecimal get_N_ID_PDR() {
    return N_ID_PDR;
  }
  public void set_N_ID_PDR(java.math.BigDecimal N_ID_PDR) {
    this.N_ID_PDR = N_ID_PDR;
  }
  public rcugas_rcugas_pdr_misuratore with_N_ID_PDR(java.math.BigDecimal N_ID_PDR) {
    this.N_ID_PDR = N_ID_PDR;
    return this;
  }
  private String T_MATRICOLA_MISURATORE;
  public String get_T_MATRICOLA_MISURATORE() {
    return T_MATRICOLA_MISURATORE;
  }
  public void set_T_MATRICOLA_MISURATORE(String T_MATRICOLA_MISURATORE) {
    this.T_MATRICOLA_MISURATORE = T_MATRICOLA_MISURATORE;
  }
  public rcugas_rcugas_pdr_misuratore with_T_MATRICOLA_MISURATORE(String T_MATRICOLA_MISURATORE) {
    this.T_MATRICOLA_MISURATORE = T_MATRICOLA_MISURATORE;
    return this;
  }
  private String T_TIPO_MISURATORE;
  public String get_T_TIPO_MISURATORE() {
    return T_TIPO_MISURATORE;
  }
  public void set_T_TIPO_MISURATORE(String T_TIPO_MISURATORE) {
    this.T_TIPO_MISURATORE = T_TIPO_MISURATORE;
  }
  public rcugas_rcugas_pdr_misuratore with_T_TIPO_MISURATORE(String T_TIPO_MISURATORE) {
    this.T_TIPO_MISURATORE = T_TIPO_MISURATORE;
    return this;
  }
  private String T_TELEGESTITO;
  public String get_T_TELEGESTITO() {
    return T_TELEGESTITO;
  }
  public void set_T_TELEGESTITO(String T_TELEGESTITO) {
    this.T_TELEGESTITO = T_TELEGESTITO;
  }
  public rcugas_rcugas_pdr_misuratore with_T_TELEGESTITO(String T_TELEGESTITO) {
    this.T_TELEGESTITO = T_TELEGESTITO;
    return this;
  }
  private java.math.BigDecimal N_COEFF_CORREZIONE;
  public java.math.BigDecimal get_N_COEFF_CORREZIONE() {
    return N_COEFF_CORREZIONE;
  }
  public void set_N_COEFF_CORREZIONE(java.math.BigDecimal N_COEFF_CORREZIONE) {
    this.N_COEFF_CORREZIONE = N_COEFF_CORREZIONE;
  }
  public rcugas_rcugas_pdr_misuratore with_N_COEFF_CORREZIONE(java.math.BigDecimal N_COEFF_CORREZIONE) {
    this.N_COEFF_CORREZIONE = N_COEFF_CORREZIONE;
    return this;
  }
  private String T_CLASSE_MISURATORE;
  public String get_T_CLASSE_MISURATORE() {
    return T_CLASSE_MISURATORE;
  }
  public void set_T_CLASSE_MISURATORE(String T_CLASSE_MISURATORE) {
    this.T_CLASSE_MISURATORE = T_CLASSE_MISURATORE;
  }
  public rcugas_rcugas_pdr_misuratore with_T_CLASSE_MISURATORE(String T_CLASSE_MISURATORE) {
    this.T_CLASSE_MISURATORE = T_CLASSE_MISURATORE;
    return this;
  }
  private java.math.BigDecimal T_ACCESS_MISURATORE;
  public java.math.BigDecimal get_T_ACCESS_MISURATORE() {
    return T_ACCESS_MISURATORE;
  }
  public void set_T_ACCESS_MISURATORE(java.math.BigDecimal T_ACCESS_MISURATORE) {
    this.T_ACCESS_MISURATORE = T_ACCESS_MISURATORE;
  }
  public rcugas_rcugas_pdr_misuratore with_T_ACCESS_MISURATORE(java.math.BigDecimal T_ACCESS_MISURATORE) {
    this.T_ACCESS_MISURATORE = T_ACCESS_MISURATORE;
    return this;
  }
  private java.math.BigDecimal N_NUM_CIFRE_MISURATORE;
  public java.math.BigDecimal get_N_NUM_CIFRE_MISURATORE() {
    return N_NUM_CIFRE_MISURATORE;
  }
  public void set_N_NUM_CIFRE_MISURATORE(java.math.BigDecimal N_NUM_CIFRE_MISURATORE) {
    this.N_NUM_CIFRE_MISURATORE = N_NUM_CIFRE_MISURATORE;
  }
  public rcugas_rcugas_pdr_misuratore with_N_NUM_CIFRE_MISURATORE(java.math.BigDecimal N_NUM_CIFRE_MISURATORE) {
    this.N_NUM_CIFRE_MISURATORE = N_NUM_CIFRE_MISURATORE;
    return this;
  }
  private String T_ANNO_FABBRIC_MISURATORE;
  public String get_T_ANNO_FABBRIC_MISURATORE() {
    return T_ANNO_FABBRIC_MISURATORE;
  }
  public void set_T_ANNO_FABBRIC_MISURATORE(String T_ANNO_FABBRIC_MISURATORE) {
    this.T_ANNO_FABBRIC_MISURATORE = T_ANNO_FABBRIC_MISURATORE;
  }
  public rcugas_rcugas_pdr_misuratore with_T_ANNO_FABBRIC_MISURATORE(String T_ANNO_FABBRIC_MISURATORE) {
    this.T_ANNO_FABBRIC_MISURATORE = T_ANNO_FABBRIC_MISURATORE;
    return this;
  }
  private String T_DATA_INST_MISURATORE;
  public String get_T_DATA_INST_MISURATORE() {
    return T_DATA_INST_MISURATORE;
  }
  public void set_T_DATA_INST_MISURATORE(String T_DATA_INST_MISURATORE) {
    this.T_DATA_INST_MISURATORE = T_DATA_INST_MISURATORE;
  }
  public rcugas_rcugas_pdr_misuratore with_T_DATA_INST_MISURATORE(String T_DATA_INST_MISURATORE) {
    this.T_DATA_INST_MISURATORE = T_DATA_INST_MISURATORE;
    return this;
  }
  private String T_MISURATORE_INTEGRATO;
  public String get_T_MISURATORE_INTEGRATO() {
    return T_MISURATORE_INTEGRATO;
  }
  public void set_T_MISURATORE_INTEGRATO(String T_MISURATORE_INTEGRATO) {
    this.T_MISURATORE_INTEGRATO = T_MISURATORE_INTEGRATO;
  }
  public rcugas_rcugas_pdr_misuratore with_T_MISURATORE_INTEGRATO(String T_MISURATORE_INTEGRATO) {
    this.T_MISURATORE_INTEGRATO = T_MISURATORE_INTEGRATO;
    return this;
  }
  private String T_PRESENZA_CONVERTITORE;
  public String get_T_PRESENZA_CONVERTITORE() {
    return T_PRESENZA_CONVERTITORE;
  }
  public void set_T_PRESENZA_CONVERTITORE(String T_PRESENZA_CONVERTITORE) {
    this.T_PRESENZA_CONVERTITORE = T_PRESENZA_CONVERTITORE;
  }
  public rcugas_rcugas_pdr_misuratore with_T_PRESENZA_CONVERTITORE(String T_PRESENZA_CONVERTITORE) {
    this.T_PRESENZA_CONVERTITORE = T_PRESENZA_CONVERTITORE;
    return this;
  }
  private String T_MATRICOLA_CONVERTITORE;
  public String get_T_MATRICOLA_CONVERTITORE() {
    return T_MATRICOLA_CONVERTITORE;
  }
  public void set_T_MATRICOLA_CONVERTITORE(String T_MATRICOLA_CONVERTITORE) {
    this.T_MATRICOLA_CONVERTITORE = T_MATRICOLA_CONVERTITORE;
  }
  public rcugas_rcugas_pdr_misuratore with_T_MATRICOLA_CONVERTITORE(String T_MATRICOLA_CONVERTITORE) {
    this.T_MATRICOLA_CONVERTITORE = T_MATRICOLA_CONVERTITORE;
    return this;
  }
  private java.math.BigDecimal N_NUM_CIFRE_CONVERTITORE;
  public java.math.BigDecimal get_N_NUM_CIFRE_CONVERTITORE() {
    return N_NUM_CIFRE_CONVERTITORE;
  }
  public void set_N_NUM_CIFRE_CONVERTITORE(java.math.BigDecimal N_NUM_CIFRE_CONVERTITORE) {
    this.N_NUM_CIFRE_CONVERTITORE = N_NUM_CIFRE_CONVERTITORE;
  }
  public rcugas_rcugas_pdr_misuratore with_N_NUM_CIFRE_CONVERTITORE(java.math.BigDecimal N_NUM_CIFRE_CONVERTITORE) {
    this.N_NUM_CIFRE_CONVERTITORE = N_NUM_CIFRE_CONVERTITORE;
    return this;
  }
  private String T_ANNO_FABBRIC_CONVERTITORE;
  public String get_T_ANNO_FABBRIC_CONVERTITORE() {
    return T_ANNO_FABBRIC_CONVERTITORE;
  }
  public void set_T_ANNO_FABBRIC_CONVERTITORE(String T_ANNO_FABBRIC_CONVERTITORE) {
    this.T_ANNO_FABBRIC_CONVERTITORE = T_ANNO_FABBRIC_CONVERTITORE;
  }
  public rcugas_rcugas_pdr_misuratore with_T_ANNO_FABBRIC_CONVERTITORE(String T_ANNO_FABBRIC_CONVERTITORE) {
    this.T_ANNO_FABBRIC_CONVERTITORE = T_ANNO_FABBRIC_CONVERTITORE;
    return this;
  }
  private String T_DATA_INST_CONVERTITORE;
  public String get_T_DATA_INST_CONVERTITORE() {
    return T_DATA_INST_CONVERTITORE;
  }
  public void set_T_DATA_INST_CONVERTITORE(String T_DATA_INST_CONVERTITORE) {
    this.T_DATA_INST_CONVERTITORE = T_DATA_INST_CONVERTITORE;
  }
  public rcugas_rcugas_pdr_misuratore with_T_DATA_INST_CONVERTITORE(String T_DATA_INST_CONVERTITORE) {
    this.T_DATA_INST_CONVERTITORE = T_DATA_INST_CONVERTITORE;
    return this;
  }
  private java.math.BigDecimal N_LETTURA_CONVERTITORE;
  public java.math.BigDecimal get_N_LETTURA_CONVERTITORE() {
    return N_LETTURA_CONVERTITORE;
  }
  public void set_N_LETTURA_CONVERTITORE(java.math.BigDecimal N_LETTURA_CONVERTITORE) {
    this.N_LETTURA_CONVERTITORE = N_LETTURA_CONVERTITORE;
  }
  public rcugas_rcugas_pdr_misuratore with_N_LETTURA_CONVERTITORE(java.math.BigDecimal N_LETTURA_CONVERTITORE) {
    this.N_LETTURA_CONVERTITORE = N_LETTURA_CONVERTITORE;
    return this;
  }
  private String T_NOTE;
  public String get_T_NOTE() {
    return T_NOTE;
  }
  public void set_T_NOTE(String T_NOTE) {
    this.T_NOTE = T_NOTE;
  }
  public rcugas_rcugas_pdr_misuratore with_T_NOTE(String T_NOTE) {
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
  public rcugas_rcugas_pdr_misuratore with_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
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
  public rcugas_rcugas_pdr_misuratore with_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
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
  public rcugas_rcugas_pdr_misuratore with_N_ID_S_PREC(java.math.BigDecimal N_ID_S_PREC) {
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
  public rcugas_rcugas_pdr_misuratore with_D_DATA_RIF(String D_DATA_RIF) {
    this.D_DATA_RIF = D_DATA_RIF;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcugas_rcugas_pdr_misuratore)) {
      return false;
    }
    rcugas_rcugas_pdr_misuratore that = (rcugas_rcugas_pdr_misuratore) o;
    boolean equal = true;
    equal = equal && (this.N_ID_PDR_MISURATORE == null ? that.N_ID_PDR_MISURATORE == null : this.N_ID_PDR_MISURATORE.equals(that.N_ID_PDR_MISURATORE));
    equal = equal && (this.N_ID_PDR == null ? that.N_ID_PDR == null : this.N_ID_PDR.equals(that.N_ID_PDR));
    equal = equal && (this.T_MATRICOLA_MISURATORE == null ? that.T_MATRICOLA_MISURATORE == null : this.T_MATRICOLA_MISURATORE.equals(that.T_MATRICOLA_MISURATORE));
    equal = equal && (this.T_TIPO_MISURATORE == null ? that.T_TIPO_MISURATORE == null : this.T_TIPO_MISURATORE.equals(that.T_TIPO_MISURATORE));
    equal = equal && (this.T_TELEGESTITO == null ? that.T_TELEGESTITO == null : this.T_TELEGESTITO.equals(that.T_TELEGESTITO));
    equal = equal && (this.N_COEFF_CORREZIONE == null ? that.N_COEFF_CORREZIONE == null : this.N_COEFF_CORREZIONE.equals(that.N_COEFF_CORREZIONE));
    equal = equal && (this.T_CLASSE_MISURATORE == null ? that.T_CLASSE_MISURATORE == null : this.T_CLASSE_MISURATORE.equals(that.T_CLASSE_MISURATORE));
    equal = equal && (this.T_ACCESS_MISURATORE == null ? that.T_ACCESS_MISURATORE == null : this.T_ACCESS_MISURATORE.equals(that.T_ACCESS_MISURATORE));
    equal = equal && (this.N_NUM_CIFRE_MISURATORE == null ? that.N_NUM_CIFRE_MISURATORE == null : this.N_NUM_CIFRE_MISURATORE.equals(that.N_NUM_CIFRE_MISURATORE));
    equal = equal && (this.T_ANNO_FABBRIC_MISURATORE == null ? that.T_ANNO_FABBRIC_MISURATORE == null : this.T_ANNO_FABBRIC_MISURATORE.equals(that.T_ANNO_FABBRIC_MISURATORE));
    equal = equal && (this.T_DATA_INST_MISURATORE == null ? that.T_DATA_INST_MISURATORE == null : this.T_DATA_INST_MISURATORE.equals(that.T_DATA_INST_MISURATORE));
    equal = equal && (this.T_MISURATORE_INTEGRATO == null ? that.T_MISURATORE_INTEGRATO == null : this.T_MISURATORE_INTEGRATO.equals(that.T_MISURATORE_INTEGRATO));
    equal = equal && (this.T_PRESENZA_CONVERTITORE == null ? that.T_PRESENZA_CONVERTITORE == null : this.T_PRESENZA_CONVERTITORE.equals(that.T_PRESENZA_CONVERTITORE));
    equal = equal && (this.T_MATRICOLA_CONVERTITORE == null ? that.T_MATRICOLA_CONVERTITORE == null : this.T_MATRICOLA_CONVERTITORE.equals(that.T_MATRICOLA_CONVERTITORE));
    equal = equal && (this.N_NUM_CIFRE_CONVERTITORE == null ? that.N_NUM_CIFRE_CONVERTITORE == null : this.N_NUM_CIFRE_CONVERTITORE.equals(that.N_NUM_CIFRE_CONVERTITORE));
    equal = equal && (this.T_ANNO_FABBRIC_CONVERTITORE == null ? that.T_ANNO_FABBRIC_CONVERTITORE == null : this.T_ANNO_FABBRIC_CONVERTITORE.equals(that.T_ANNO_FABBRIC_CONVERTITORE));
    equal = equal && (this.T_DATA_INST_CONVERTITORE == null ? that.T_DATA_INST_CONVERTITORE == null : this.T_DATA_INST_CONVERTITORE.equals(that.T_DATA_INST_CONVERTITORE));
    equal = equal && (this.N_LETTURA_CONVERTITORE == null ? that.N_LETTURA_CONVERTITORE == null : this.N_LETTURA_CONVERTITORE.equals(that.N_LETTURA_CONVERTITORE));
    equal = equal && (this.T_NOTE == null ? that.T_NOTE == null : this.T_NOTE.equals(that.T_NOTE));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.D_DATA_RIF == null ? that.D_DATA_RIF == null : this.D_DATA_RIF.equals(that.D_DATA_RIF));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcugas_rcugas_pdr_misuratore)) {
      return false;
    }
    rcugas_rcugas_pdr_misuratore that = (rcugas_rcugas_pdr_misuratore) o;
    boolean equal = true;
    equal = equal && (this.N_ID_PDR_MISURATORE == null ? that.N_ID_PDR_MISURATORE == null : this.N_ID_PDR_MISURATORE.equals(that.N_ID_PDR_MISURATORE));
    equal = equal && (this.N_ID_PDR == null ? that.N_ID_PDR == null : this.N_ID_PDR.equals(that.N_ID_PDR));
    equal = equal && (this.T_MATRICOLA_MISURATORE == null ? that.T_MATRICOLA_MISURATORE == null : this.T_MATRICOLA_MISURATORE.equals(that.T_MATRICOLA_MISURATORE));
    equal = equal && (this.T_TIPO_MISURATORE == null ? that.T_TIPO_MISURATORE == null : this.T_TIPO_MISURATORE.equals(that.T_TIPO_MISURATORE));
    equal = equal && (this.T_TELEGESTITO == null ? that.T_TELEGESTITO == null : this.T_TELEGESTITO.equals(that.T_TELEGESTITO));
    equal = equal && (this.N_COEFF_CORREZIONE == null ? that.N_COEFF_CORREZIONE == null : this.N_COEFF_CORREZIONE.equals(that.N_COEFF_CORREZIONE));
    equal = equal && (this.T_CLASSE_MISURATORE == null ? that.T_CLASSE_MISURATORE == null : this.T_CLASSE_MISURATORE.equals(that.T_CLASSE_MISURATORE));
    equal = equal && (this.T_ACCESS_MISURATORE == null ? that.T_ACCESS_MISURATORE == null : this.T_ACCESS_MISURATORE.equals(that.T_ACCESS_MISURATORE));
    equal = equal && (this.N_NUM_CIFRE_MISURATORE == null ? that.N_NUM_CIFRE_MISURATORE == null : this.N_NUM_CIFRE_MISURATORE.equals(that.N_NUM_CIFRE_MISURATORE));
    equal = equal && (this.T_ANNO_FABBRIC_MISURATORE == null ? that.T_ANNO_FABBRIC_MISURATORE == null : this.T_ANNO_FABBRIC_MISURATORE.equals(that.T_ANNO_FABBRIC_MISURATORE));
    equal = equal && (this.T_DATA_INST_MISURATORE == null ? that.T_DATA_INST_MISURATORE == null : this.T_DATA_INST_MISURATORE.equals(that.T_DATA_INST_MISURATORE));
    equal = equal && (this.T_MISURATORE_INTEGRATO == null ? that.T_MISURATORE_INTEGRATO == null : this.T_MISURATORE_INTEGRATO.equals(that.T_MISURATORE_INTEGRATO));
    equal = equal && (this.T_PRESENZA_CONVERTITORE == null ? that.T_PRESENZA_CONVERTITORE == null : this.T_PRESENZA_CONVERTITORE.equals(that.T_PRESENZA_CONVERTITORE));
    equal = equal && (this.T_MATRICOLA_CONVERTITORE == null ? that.T_MATRICOLA_CONVERTITORE == null : this.T_MATRICOLA_CONVERTITORE.equals(that.T_MATRICOLA_CONVERTITORE));
    equal = equal && (this.N_NUM_CIFRE_CONVERTITORE == null ? that.N_NUM_CIFRE_CONVERTITORE == null : this.N_NUM_CIFRE_CONVERTITORE.equals(that.N_NUM_CIFRE_CONVERTITORE));
    equal = equal && (this.T_ANNO_FABBRIC_CONVERTITORE == null ? that.T_ANNO_FABBRIC_CONVERTITORE == null : this.T_ANNO_FABBRIC_CONVERTITORE.equals(that.T_ANNO_FABBRIC_CONVERTITORE));
    equal = equal && (this.T_DATA_INST_CONVERTITORE == null ? that.T_DATA_INST_CONVERTITORE == null : this.T_DATA_INST_CONVERTITORE.equals(that.T_DATA_INST_CONVERTITORE));
    equal = equal && (this.N_LETTURA_CONVERTITORE == null ? that.N_LETTURA_CONVERTITORE == null : this.N_LETTURA_CONVERTITORE.equals(that.N_LETTURA_CONVERTITORE));
    equal = equal && (this.T_NOTE == null ? that.T_NOTE == null : this.T_NOTE.equals(that.T_NOTE));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.D_DATA_RIF == null ? that.D_DATA_RIF == null : this.D_DATA_RIF.equals(that.D_DATA_RIF));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_PDR_MISURATORE = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_PDR = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_MATRICOLA_MISURATORE = JdbcWritableBridge.readString(3, __dbResults);
    this.T_TIPO_MISURATORE = JdbcWritableBridge.readString(4, __dbResults);
    this.T_TELEGESTITO = JdbcWritableBridge.readString(5, __dbResults);
    this.N_COEFF_CORREZIONE = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.T_CLASSE_MISURATORE = JdbcWritableBridge.readString(7, __dbResults);
    this.T_ACCESS_MISURATORE = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.N_NUM_CIFRE_MISURATORE = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.T_ANNO_FABBRIC_MISURATORE = JdbcWritableBridge.readString(10, __dbResults);
    this.T_DATA_INST_MISURATORE = JdbcWritableBridge.readString(11, __dbResults);
    this.T_MISURATORE_INTEGRATO = JdbcWritableBridge.readString(12, __dbResults);
    this.T_PRESENZA_CONVERTITORE = JdbcWritableBridge.readString(13, __dbResults);
    this.T_MATRICOLA_CONVERTITORE = JdbcWritableBridge.readString(14, __dbResults);
    this.N_NUM_CIFRE_CONVERTITORE = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.T_ANNO_FABBRIC_CONVERTITORE = JdbcWritableBridge.readString(16, __dbResults);
    this.T_DATA_INST_CONVERTITORE = JdbcWritableBridge.readString(17, __dbResults);
    this.N_LETTURA_CONVERTITORE = JdbcWritableBridge.readBigDecimal(18, __dbResults);
    this.T_NOTE = JdbcWritableBridge.readString(19, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(20, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(21, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(22, __dbResults);
    this.D_DATA_RIF = JdbcWritableBridge.readString(23, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_PDR_MISURATORE = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_PDR = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_MATRICOLA_MISURATORE = JdbcWritableBridge.readString(3, __dbResults);
    this.T_TIPO_MISURATORE = JdbcWritableBridge.readString(4, __dbResults);
    this.T_TELEGESTITO = JdbcWritableBridge.readString(5, __dbResults);
    this.N_COEFF_CORREZIONE = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.T_CLASSE_MISURATORE = JdbcWritableBridge.readString(7, __dbResults);
    this.T_ACCESS_MISURATORE = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.N_NUM_CIFRE_MISURATORE = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.T_ANNO_FABBRIC_MISURATORE = JdbcWritableBridge.readString(10, __dbResults);
    this.T_DATA_INST_MISURATORE = JdbcWritableBridge.readString(11, __dbResults);
    this.T_MISURATORE_INTEGRATO = JdbcWritableBridge.readString(12, __dbResults);
    this.T_PRESENZA_CONVERTITORE = JdbcWritableBridge.readString(13, __dbResults);
    this.T_MATRICOLA_CONVERTITORE = JdbcWritableBridge.readString(14, __dbResults);
    this.N_NUM_CIFRE_CONVERTITORE = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.T_ANNO_FABBRIC_CONVERTITORE = JdbcWritableBridge.readString(16, __dbResults);
    this.T_DATA_INST_CONVERTITORE = JdbcWritableBridge.readString(17, __dbResults);
    this.N_LETTURA_CONVERTITORE = JdbcWritableBridge.readBigDecimal(18, __dbResults);
    this.T_NOTE = JdbcWritableBridge.readString(19, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(20, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(21, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(22, __dbResults);
    this.D_DATA_RIF = JdbcWritableBridge.readString(23, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_PDR_MISURATORE, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PDR, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_MATRICOLA_MISURATORE, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_MISURATORE, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TELEGESTITO, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_COEFF_CORREZIONE, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CLASSE_MISURATORE, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(T_ACCESS_MISURATORE, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_CIFRE_MISURATORE, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO_FABBRIC_MISURATORE, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DATA_INST_MISURATORE, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MISURATORE_INTEGRATO, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PRESENZA_CONVERTITORE, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MATRICOLA_CONVERTITORE, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_CIFRE_CONVERTITORE, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO_FABBRIC_CONVERTITORE, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DATA_INST_CONVERTITORE, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_LETTURA_CONVERTITORE, 18 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTE, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 20 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 21 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 22 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF, 23 + __off, 93, __dbStmt);
    return 23;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_PDR_MISURATORE, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PDR, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_MATRICOLA_MISURATORE, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_MISURATORE, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TELEGESTITO, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_COEFF_CORREZIONE, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CLASSE_MISURATORE, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(T_ACCESS_MISURATORE, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_CIFRE_MISURATORE, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO_FABBRIC_MISURATORE, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DATA_INST_MISURATORE, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MISURATORE_INTEGRATO, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PRESENZA_CONVERTITORE, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MATRICOLA_CONVERTITORE, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_CIFRE_CONVERTITORE, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_ANNO_FABBRIC_CONVERTITORE, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_DATA_INST_CONVERTITORE, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_LETTURA_CONVERTITORE, 18 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTE, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 20 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 21 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 22 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF, 23 + __off, 93, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_PDR_MISURATORE = null;
    } else {
    this.N_ID_PDR_MISURATORE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_PDR = null;
    } else {
    this.N_ID_PDR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MATRICOLA_MISURATORE = null;
    } else {
    this.T_MATRICOLA_MISURATORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO_MISURATORE = null;
    } else {
    this.T_TIPO_MISURATORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TELEGESTITO = null;
    } else {
    this.T_TELEGESTITO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_COEFF_CORREZIONE = null;
    } else {
    this.N_COEFF_CORREZIONE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CLASSE_MISURATORE = null;
    } else {
    this.T_CLASSE_MISURATORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ACCESS_MISURATORE = null;
    } else {
    this.T_ACCESS_MISURATORE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_NUM_CIFRE_MISURATORE = null;
    } else {
    this.N_NUM_CIFRE_MISURATORE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ANNO_FABBRIC_MISURATORE = null;
    } else {
    this.T_ANNO_FABBRIC_MISURATORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_DATA_INST_MISURATORE = null;
    } else {
    this.T_DATA_INST_MISURATORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MISURATORE_INTEGRATO = null;
    } else {
    this.T_MISURATORE_INTEGRATO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PRESENZA_CONVERTITORE = null;
    } else {
    this.T_PRESENZA_CONVERTITORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MATRICOLA_CONVERTITORE = null;
    } else {
    this.T_MATRICOLA_CONVERTITORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_NUM_CIFRE_CONVERTITORE = null;
    } else {
    this.N_NUM_CIFRE_CONVERTITORE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ANNO_FABBRIC_CONVERTITORE = null;
    } else {
    this.T_ANNO_FABBRIC_CONVERTITORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_DATA_INST_CONVERTITORE = null;
    } else {
    this.T_DATA_INST_CONVERTITORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_LETTURA_CONVERTITORE = null;
    } else {
    this.N_LETTURA_CONVERTITORE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
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
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_PDR_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PDR_MISURATORE, __dataOut);
    }
    if (null == this.N_ID_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PDR, __dataOut);
    }
    if (null == this.T_MATRICOLA_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MATRICOLA_MISURATORE);
    }
    if (null == this.T_TIPO_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_MISURATORE);
    }
    if (null == this.T_TELEGESTITO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TELEGESTITO);
    }
    if (null == this.N_COEFF_CORREZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_COEFF_CORREZIONE, __dataOut);
    }
    if (null == this.T_CLASSE_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CLASSE_MISURATORE);
    }
    if (null == this.T_ACCESS_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.T_ACCESS_MISURATORE, __dataOut);
    }
    if (null == this.N_NUM_CIFRE_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_CIFRE_MISURATORE, __dataOut);
    }
    if (null == this.T_ANNO_FABBRIC_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO_FABBRIC_MISURATORE);
    }
    if (null == this.T_DATA_INST_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DATA_INST_MISURATORE);
    }
    if (null == this.T_MISURATORE_INTEGRATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MISURATORE_INTEGRATO);
    }
    if (null == this.T_PRESENZA_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PRESENZA_CONVERTITORE);
    }
    if (null == this.T_MATRICOLA_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MATRICOLA_CONVERTITORE);
    }
    if (null == this.N_NUM_CIFRE_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_CIFRE_CONVERTITORE, __dataOut);
    }
    if (null == this.T_ANNO_FABBRIC_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO_FABBRIC_CONVERTITORE);
    }
    if (null == this.T_DATA_INST_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DATA_INST_CONVERTITORE);
    }
    if (null == this.N_LETTURA_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_LETTURA_CONVERTITORE, __dataOut);
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
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_PDR_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PDR_MISURATORE, __dataOut);
    }
    if (null == this.N_ID_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PDR, __dataOut);
    }
    if (null == this.T_MATRICOLA_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MATRICOLA_MISURATORE);
    }
    if (null == this.T_TIPO_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_MISURATORE);
    }
    if (null == this.T_TELEGESTITO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TELEGESTITO);
    }
    if (null == this.N_COEFF_CORREZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_COEFF_CORREZIONE, __dataOut);
    }
    if (null == this.T_CLASSE_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CLASSE_MISURATORE);
    }
    if (null == this.T_ACCESS_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.T_ACCESS_MISURATORE, __dataOut);
    }
    if (null == this.N_NUM_CIFRE_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_CIFRE_MISURATORE, __dataOut);
    }
    if (null == this.T_ANNO_FABBRIC_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO_FABBRIC_MISURATORE);
    }
    if (null == this.T_DATA_INST_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DATA_INST_MISURATORE);
    }
    if (null == this.T_MISURATORE_INTEGRATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MISURATORE_INTEGRATO);
    }
    if (null == this.T_PRESENZA_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PRESENZA_CONVERTITORE);
    }
    if (null == this.T_MATRICOLA_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MATRICOLA_CONVERTITORE);
    }
    if (null == this.N_NUM_CIFRE_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_CIFRE_CONVERTITORE, __dataOut);
    }
    if (null == this.T_ANNO_FABBRIC_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ANNO_FABBRIC_CONVERTITORE);
    }
    if (null == this.T_DATA_INST_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DATA_INST_CONVERTITORE);
    }
    if (null == this.N_LETTURA_CONVERTITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_LETTURA_CONVERTITORE, __dataOut);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PDR_MISURATORE==null?"":N_ID_PDR_MISURATORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PDR==null?"":N_ID_PDR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MATRICOLA_MISURATORE==null?"":T_MATRICOLA_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_MISURATORE==null?"":T_TIPO_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TELEGESTITO==null?"":T_TELEGESTITO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_COEFF_CORREZIONE==null?"":N_COEFF_CORREZIONE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CLASSE_MISURATORE==null?"":T_CLASSE_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(T_ACCESS_MISURATORE==null?"":T_ACCESS_MISURATORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_CIFRE_MISURATORE==null?"":N_NUM_CIFRE_MISURATORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO_FABBRIC_MISURATORE==null?"":T_ANNO_FABBRIC_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DATA_INST_MISURATORE==null?"":T_DATA_INST_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MISURATORE_INTEGRATO==null?"":T_MISURATORE_INTEGRATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PRESENZA_CONVERTITORE==null?"":T_PRESENZA_CONVERTITORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MATRICOLA_CONVERTITORE==null?"":T_MATRICOLA_CONVERTITORE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_CIFRE_CONVERTITORE==null?"":N_NUM_CIFRE_CONVERTITORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO_FABBRIC_CONVERTITORE==null?"":T_ANNO_FABBRIC_CONVERTITORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DATA_INST_CONVERTITORE==null?"":T_DATA_INST_CONVERTITORE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_LETTURA_CONVERTITORE==null?"":N_LETTURA_CONVERTITORE.toPlainString(), delimiters));
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
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PDR_MISURATORE==null?"":N_ID_PDR_MISURATORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PDR==null?"":N_ID_PDR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MATRICOLA_MISURATORE==null?"":T_MATRICOLA_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_MISURATORE==null?"":T_TIPO_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TELEGESTITO==null?"":T_TELEGESTITO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_COEFF_CORREZIONE==null?"":N_COEFF_CORREZIONE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CLASSE_MISURATORE==null?"":T_CLASSE_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(T_ACCESS_MISURATORE==null?"":T_ACCESS_MISURATORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_CIFRE_MISURATORE==null?"":N_NUM_CIFRE_MISURATORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO_FABBRIC_MISURATORE==null?"":T_ANNO_FABBRIC_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DATA_INST_MISURATORE==null?"":T_DATA_INST_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MISURATORE_INTEGRATO==null?"":T_MISURATORE_INTEGRATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PRESENZA_CONVERTITORE==null?"":T_PRESENZA_CONVERTITORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MATRICOLA_CONVERTITORE==null?"":T_MATRICOLA_CONVERTITORE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_CIFRE_CONVERTITORE==null?"":N_NUM_CIFRE_CONVERTITORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ANNO_FABBRIC_CONVERTITORE==null?"":T_ANNO_FABBRIC_CONVERTITORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DATA_INST_CONVERTITORE==null?"":T_DATA_INST_CONVERTITORE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_LETTURA_CONVERTITORE==null?"":N_LETTURA_CONVERTITORE.toPlainString(), delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PDR_MISURATORE = null; } else {
      this.N_ID_PDR_MISURATORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PDR = null; } else {
      this.N_ID_PDR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MATRICOLA_MISURATORE = null; } else {
      this.T_MATRICOLA_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_MISURATORE = null; } else {
      this.T_TIPO_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TELEGESTITO = null; } else {
      this.T_TELEGESTITO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_COEFF_CORREZIONE = null; } else {
      this.N_COEFF_CORREZIONE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CLASSE_MISURATORE = null; } else {
      this.T_CLASSE_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.T_ACCESS_MISURATORE = null; } else {
      this.T_ACCESS_MISURATORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_CIFRE_MISURATORE = null; } else {
      this.N_NUM_CIFRE_MISURATORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO_FABBRIC_MISURATORE = null; } else {
      this.T_ANNO_FABBRIC_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DATA_INST_MISURATORE = null; } else {
      this.T_DATA_INST_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MISURATORE_INTEGRATO = null; } else {
      this.T_MISURATORE_INTEGRATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PRESENZA_CONVERTITORE = null; } else {
      this.T_PRESENZA_CONVERTITORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MATRICOLA_CONVERTITORE = null; } else {
      this.T_MATRICOLA_CONVERTITORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_CIFRE_CONVERTITORE = null; } else {
      this.N_NUM_CIFRE_CONVERTITORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO_FABBRIC_CONVERTITORE = null; } else {
      this.T_ANNO_FABBRIC_CONVERTITORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DATA_INST_CONVERTITORE = null; } else {
      this.T_DATA_INST_CONVERTITORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_LETTURA_CONVERTITORE = null; } else {
      this.N_LETTURA_CONVERTITORE = new java.math.BigDecimal(__cur_str);
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

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PDR_MISURATORE = null; } else {
      this.N_ID_PDR_MISURATORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PDR = null; } else {
      this.N_ID_PDR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MATRICOLA_MISURATORE = null; } else {
      this.T_MATRICOLA_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_MISURATORE = null; } else {
      this.T_TIPO_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TELEGESTITO = null; } else {
      this.T_TELEGESTITO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_COEFF_CORREZIONE = null; } else {
      this.N_COEFF_CORREZIONE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CLASSE_MISURATORE = null; } else {
      this.T_CLASSE_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.T_ACCESS_MISURATORE = null; } else {
      this.T_ACCESS_MISURATORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_CIFRE_MISURATORE = null; } else {
      this.N_NUM_CIFRE_MISURATORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO_FABBRIC_MISURATORE = null; } else {
      this.T_ANNO_FABBRIC_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DATA_INST_MISURATORE = null; } else {
      this.T_DATA_INST_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MISURATORE_INTEGRATO = null; } else {
      this.T_MISURATORE_INTEGRATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PRESENZA_CONVERTITORE = null; } else {
      this.T_PRESENZA_CONVERTITORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MATRICOLA_CONVERTITORE = null; } else {
      this.T_MATRICOLA_CONVERTITORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_CIFRE_CONVERTITORE = null; } else {
      this.N_NUM_CIFRE_CONVERTITORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ANNO_FABBRIC_CONVERTITORE = null; } else {
      this.T_ANNO_FABBRIC_CONVERTITORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_DATA_INST_CONVERTITORE = null; } else {
      this.T_DATA_INST_CONVERTITORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_LETTURA_CONVERTITORE = null; } else {
      this.N_LETTURA_CONVERTITORE = new java.math.BigDecimal(__cur_str);
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

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    rcugas_rcugas_pdr_misuratore o = (rcugas_rcugas_pdr_misuratore) super.clone();
    return o;
  }

  public void clone0(rcugas_rcugas_pdr_misuratore o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_PDR_MISURATORE", this.N_ID_PDR_MISURATORE);
    __sqoop$field_map.put("N_ID_PDR", this.N_ID_PDR);
    __sqoop$field_map.put("T_MATRICOLA_MISURATORE", this.T_MATRICOLA_MISURATORE);
    __sqoop$field_map.put("T_TIPO_MISURATORE", this.T_TIPO_MISURATORE);
    __sqoop$field_map.put("T_TELEGESTITO", this.T_TELEGESTITO);
    __sqoop$field_map.put("N_COEFF_CORREZIONE", this.N_COEFF_CORREZIONE);
    __sqoop$field_map.put("T_CLASSE_MISURATORE", this.T_CLASSE_MISURATORE);
    __sqoop$field_map.put("T_ACCESS_MISURATORE", this.T_ACCESS_MISURATORE);
    __sqoop$field_map.put("N_NUM_CIFRE_MISURATORE", this.N_NUM_CIFRE_MISURATORE);
    __sqoop$field_map.put("T_ANNO_FABBRIC_MISURATORE", this.T_ANNO_FABBRIC_MISURATORE);
    __sqoop$field_map.put("T_DATA_INST_MISURATORE", this.T_DATA_INST_MISURATORE);
    __sqoop$field_map.put("T_MISURATORE_INTEGRATO", this.T_MISURATORE_INTEGRATO);
    __sqoop$field_map.put("T_PRESENZA_CONVERTITORE", this.T_PRESENZA_CONVERTITORE);
    __sqoop$field_map.put("T_MATRICOLA_CONVERTITORE", this.T_MATRICOLA_CONVERTITORE);
    __sqoop$field_map.put("N_NUM_CIFRE_CONVERTITORE", this.N_NUM_CIFRE_CONVERTITORE);
    __sqoop$field_map.put("T_ANNO_FABBRIC_CONVERTITORE", this.T_ANNO_FABBRIC_CONVERTITORE);
    __sqoop$field_map.put("T_DATA_INST_CONVERTITORE", this.T_DATA_INST_CONVERTITORE);
    __sqoop$field_map.put("N_LETTURA_CONVERTITORE", this.N_LETTURA_CONVERTITORE);
    __sqoop$field_map.put("T_NOTE", this.T_NOTE);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("D_DATA_RIF", this.D_DATA_RIF);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_PDR_MISURATORE", this.N_ID_PDR_MISURATORE);
    __sqoop$field_map.put("N_ID_PDR", this.N_ID_PDR);
    __sqoop$field_map.put("T_MATRICOLA_MISURATORE", this.T_MATRICOLA_MISURATORE);
    __sqoop$field_map.put("T_TIPO_MISURATORE", this.T_TIPO_MISURATORE);
    __sqoop$field_map.put("T_TELEGESTITO", this.T_TELEGESTITO);
    __sqoop$field_map.put("N_COEFF_CORREZIONE", this.N_COEFF_CORREZIONE);
    __sqoop$field_map.put("T_CLASSE_MISURATORE", this.T_CLASSE_MISURATORE);
    __sqoop$field_map.put("T_ACCESS_MISURATORE", this.T_ACCESS_MISURATORE);
    __sqoop$field_map.put("N_NUM_CIFRE_MISURATORE", this.N_NUM_CIFRE_MISURATORE);
    __sqoop$field_map.put("T_ANNO_FABBRIC_MISURATORE", this.T_ANNO_FABBRIC_MISURATORE);
    __sqoop$field_map.put("T_DATA_INST_MISURATORE", this.T_DATA_INST_MISURATORE);
    __sqoop$field_map.put("T_MISURATORE_INTEGRATO", this.T_MISURATORE_INTEGRATO);
    __sqoop$field_map.put("T_PRESENZA_CONVERTITORE", this.T_PRESENZA_CONVERTITORE);
    __sqoop$field_map.put("T_MATRICOLA_CONVERTITORE", this.T_MATRICOLA_CONVERTITORE);
    __sqoop$field_map.put("N_NUM_CIFRE_CONVERTITORE", this.N_NUM_CIFRE_CONVERTITORE);
    __sqoop$field_map.put("T_ANNO_FABBRIC_CONVERTITORE", this.T_ANNO_FABBRIC_CONVERTITORE);
    __sqoop$field_map.put("T_DATA_INST_CONVERTITORE", this.T_DATA_INST_CONVERTITORE);
    __sqoop$field_map.put("N_LETTURA_CONVERTITORE", this.N_LETTURA_CONVERTITORE);
    __sqoop$field_map.put("T_NOTE", this.T_NOTE);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("D_DATA_RIF", this.D_DATA_RIF);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
