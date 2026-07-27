// ORM class for table 'cmg.prt_cmg_sw1'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 08:48:54 CEST 2019
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

public class cmg_prt_cmg_sw1 extends SqoopRecord  implements DBWritable, Writable {
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
    setters.put("COD_PRAT_UTENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        COD_PRAT_UTENTE = (String)value;
      }
    });
    setters.put("COD_PRAT_DISTR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        COD_PRAT_DISTR = (String)value;
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
    setters.put("DATA_DECO_SWITCH", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATA_DECO_SWITCH = (String)value;
      }
    });
    setters.put("VOL_ANNUO_SOST", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        VOL_ANNUO_SOST = (java.math.BigDecimal)value;
      }
    });
    setters.put("CLASSE_GRUPPO_MIS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CLASSE_GRUPPO_MIS = (String)value;
      }
    });
    setters.put("N_CIFRE_MIS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_CIFRE_MIS = (java.math.BigDecimal)value;
      }
    });
    setters.put("SEGN_MIS_SOST", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SEGN_MIS_SOST = (String)value;
      }
    });
    setters.put("TIPO_LETTURA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TIPO_LETTURA = (String)value;
      }
    });
    setters.put("PRE_CONV", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PRE_CONV = (String)value;
      }
    });
    setters.put("GRUPPO_MIS_INT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        GRUPPO_MIS_INT = (String)value;
      }
    });
    setters.put("COEFF_CORR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        COEFF_CORR = (java.math.BigDecimal)value;
      }
    });
    setters.put("MATR_CONV", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MATR_CONV = (String)value;
      }
    });
    setters.put("N_CIFRE_CONV", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_CIFRE_CONV = (java.math.BigDecimal)value;
      }
    });
    setters.put("SEGN_CONV", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SEGN_CONV = (String)value;
      }
    });
    setters.put("DATA_MIS_EFF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATA_MIS_EFF = (String)value;
      }
    });
    setters.put("SEGN_MIS_EFF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SEGN_MIS_EFF = (String)value;
      }
    });
    setters.put("SEGN_CONV_EFF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SEGN_CONV_EFF = (String)value;
      }
    });
    setters.put("NOTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        NOTE = (String)value;
      }
    });
  }
  public cmg_prt_cmg_sw1() {
    init0();
  }
  private java.math.BigDecimal N_ID;
  public java.math.BigDecimal get_N_ID() {
    return N_ID;
  }
  public void set_N_ID(java.math.BigDecimal N_ID) {
    this.N_ID = N_ID;
  }
  public cmg_prt_cmg_sw1 with_N_ID(java.math.BigDecimal N_ID) {
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
  public cmg_prt_cmg_sw1 with_N_ID_FILE(java.math.BigDecimal N_ID_FILE) {
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
  public cmg_prt_cmg_sw1 with_ANNOMESE_RIFERIMENTO(String ANNOMESE_RIFERIMENTO) {
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
  public cmg_prt_cmg_sw1 with_DT_CARICAMENTO(String DT_CARICAMENTO) {
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
  public cmg_prt_cmg_sw1 with_COD_SERVIZIO(String COD_SERVIZIO) {
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
  public cmg_prt_cmg_sw1 with_COD_FLUSSO(String COD_FLUSSO) {
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
  public cmg_prt_cmg_sw1 with_PIVA_UTENTE(String PIVA_UTENTE) {
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
  public cmg_prt_cmg_sw1 with_PIVA_DISTR(String PIVA_DISTR) {
    this.PIVA_DISTR = PIVA_DISTR;
    return this;
  }
  private String COD_PRAT_UTENTE;
  public String get_COD_PRAT_UTENTE() {
    return COD_PRAT_UTENTE;
  }
  public void set_COD_PRAT_UTENTE(String COD_PRAT_UTENTE) {
    this.COD_PRAT_UTENTE = COD_PRAT_UTENTE;
  }
  public cmg_prt_cmg_sw1 with_COD_PRAT_UTENTE(String COD_PRAT_UTENTE) {
    this.COD_PRAT_UTENTE = COD_PRAT_UTENTE;
    return this;
  }
  private String COD_PRAT_DISTR;
  public String get_COD_PRAT_DISTR() {
    return COD_PRAT_DISTR;
  }
  public void set_COD_PRAT_DISTR(String COD_PRAT_DISTR) {
    this.COD_PRAT_DISTR = COD_PRAT_DISTR;
  }
  public cmg_prt_cmg_sw1 with_COD_PRAT_DISTR(String COD_PRAT_DISTR) {
    this.COD_PRAT_DISTR = COD_PRAT_DISTR;
    return this;
  }
  private String COD_PDR;
  public String get_COD_PDR() {
    return COD_PDR;
  }
  public void set_COD_PDR(String COD_PDR) {
    this.COD_PDR = COD_PDR;
  }
  public cmg_prt_cmg_sw1 with_COD_PDR(String COD_PDR) {
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
  public cmg_prt_cmg_sw1 with_MATR_MIS(String MATR_MIS) {
    this.MATR_MIS = MATR_MIS;
    return this;
  }
  private String DATA_DECO_SWITCH;
  public String get_DATA_DECO_SWITCH() {
    return DATA_DECO_SWITCH;
  }
  public void set_DATA_DECO_SWITCH(String DATA_DECO_SWITCH) {
    this.DATA_DECO_SWITCH = DATA_DECO_SWITCH;
  }
  public cmg_prt_cmg_sw1 with_DATA_DECO_SWITCH(String DATA_DECO_SWITCH) {
    this.DATA_DECO_SWITCH = DATA_DECO_SWITCH;
    return this;
  }
  private java.math.BigDecimal VOL_ANNUO_SOST;
  public java.math.BigDecimal get_VOL_ANNUO_SOST() {
    return VOL_ANNUO_SOST;
  }
  public void set_VOL_ANNUO_SOST(java.math.BigDecimal VOL_ANNUO_SOST) {
    this.VOL_ANNUO_SOST = VOL_ANNUO_SOST;
  }
  public cmg_prt_cmg_sw1 with_VOL_ANNUO_SOST(java.math.BigDecimal VOL_ANNUO_SOST) {
    this.VOL_ANNUO_SOST = VOL_ANNUO_SOST;
    return this;
  }
  private String CLASSE_GRUPPO_MIS;
  public String get_CLASSE_GRUPPO_MIS() {
    return CLASSE_GRUPPO_MIS;
  }
  public void set_CLASSE_GRUPPO_MIS(String CLASSE_GRUPPO_MIS) {
    this.CLASSE_GRUPPO_MIS = CLASSE_GRUPPO_MIS;
  }
  public cmg_prt_cmg_sw1 with_CLASSE_GRUPPO_MIS(String CLASSE_GRUPPO_MIS) {
    this.CLASSE_GRUPPO_MIS = CLASSE_GRUPPO_MIS;
    return this;
  }
  private java.math.BigDecimal N_CIFRE_MIS;
  public java.math.BigDecimal get_N_CIFRE_MIS() {
    return N_CIFRE_MIS;
  }
  public void set_N_CIFRE_MIS(java.math.BigDecimal N_CIFRE_MIS) {
    this.N_CIFRE_MIS = N_CIFRE_MIS;
  }
  public cmg_prt_cmg_sw1 with_N_CIFRE_MIS(java.math.BigDecimal N_CIFRE_MIS) {
    this.N_CIFRE_MIS = N_CIFRE_MIS;
    return this;
  }
  private String SEGN_MIS_SOST;
  public String get_SEGN_MIS_SOST() {
    return SEGN_MIS_SOST;
  }
  public void set_SEGN_MIS_SOST(String SEGN_MIS_SOST) {
    this.SEGN_MIS_SOST = SEGN_MIS_SOST;
  }
  public cmg_prt_cmg_sw1 with_SEGN_MIS_SOST(String SEGN_MIS_SOST) {
    this.SEGN_MIS_SOST = SEGN_MIS_SOST;
    return this;
  }
  private String TIPO_LETTURA;
  public String get_TIPO_LETTURA() {
    return TIPO_LETTURA;
  }
  public void set_TIPO_LETTURA(String TIPO_LETTURA) {
    this.TIPO_LETTURA = TIPO_LETTURA;
  }
  public cmg_prt_cmg_sw1 with_TIPO_LETTURA(String TIPO_LETTURA) {
    this.TIPO_LETTURA = TIPO_LETTURA;
    return this;
  }
  private String PRE_CONV;
  public String get_PRE_CONV() {
    return PRE_CONV;
  }
  public void set_PRE_CONV(String PRE_CONV) {
    this.PRE_CONV = PRE_CONV;
  }
  public cmg_prt_cmg_sw1 with_PRE_CONV(String PRE_CONV) {
    this.PRE_CONV = PRE_CONV;
    return this;
  }
  private String GRUPPO_MIS_INT;
  public String get_GRUPPO_MIS_INT() {
    return GRUPPO_MIS_INT;
  }
  public void set_GRUPPO_MIS_INT(String GRUPPO_MIS_INT) {
    this.GRUPPO_MIS_INT = GRUPPO_MIS_INT;
  }
  public cmg_prt_cmg_sw1 with_GRUPPO_MIS_INT(String GRUPPO_MIS_INT) {
    this.GRUPPO_MIS_INT = GRUPPO_MIS_INT;
    return this;
  }
  private java.math.BigDecimal COEFF_CORR;
  public java.math.BigDecimal get_COEFF_CORR() {
    return COEFF_CORR;
  }
  public void set_COEFF_CORR(java.math.BigDecimal COEFF_CORR) {
    this.COEFF_CORR = COEFF_CORR;
  }
  public cmg_prt_cmg_sw1 with_COEFF_CORR(java.math.BigDecimal COEFF_CORR) {
    this.COEFF_CORR = COEFF_CORR;
    return this;
  }
  private String MATR_CONV;
  public String get_MATR_CONV() {
    return MATR_CONV;
  }
  public void set_MATR_CONV(String MATR_CONV) {
    this.MATR_CONV = MATR_CONV;
  }
  public cmg_prt_cmg_sw1 with_MATR_CONV(String MATR_CONV) {
    this.MATR_CONV = MATR_CONV;
    return this;
  }
  private java.math.BigDecimal N_CIFRE_CONV;
  public java.math.BigDecimal get_N_CIFRE_CONV() {
    return N_CIFRE_CONV;
  }
  public void set_N_CIFRE_CONV(java.math.BigDecimal N_CIFRE_CONV) {
    this.N_CIFRE_CONV = N_CIFRE_CONV;
  }
  public cmg_prt_cmg_sw1 with_N_CIFRE_CONV(java.math.BigDecimal N_CIFRE_CONV) {
    this.N_CIFRE_CONV = N_CIFRE_CONV;
    return this;
  }
  private String SEGN_CONV;
  public String get_SEGN_CONV() {
    return SEGN_CONV;
  }
  public void set_SEGN_CONV(String SEGN_CONV) {
    this.SEGN_CONV = SEGN_CONV;
  }
  public cmg_prt_cmg_sw1 with_SEGN_CONV(String SEGN_CONV) {
    this.SEGN_CONV = SEGN_CONV;
    return this;
  }
  private String DATA_MIS_EFF;
  public String get_DATA_MIS_EFF() {
    return DATA_MIS_EFF;
  }
  public void set_DATA_MIS_EFF(String DATA_MIS_EFF) {
    this.DATA_MIS_EFF = DATA_MIS_EFF;
  }
  public cmg_prt_cmg_sw1 with_DATA_MIS_EFF(String DATA_MIS_EFF) {
    this.DATA_MIS_EFF = DATA_MIS_EFF;
    return this;
  }
  private String SEGN_MIS_EFF;
  public String get_SEGN_MIS_EFF() {
    return SEGN_MIS_EFF;
  }
  public void set_SEGN_MIS_EFF(String SEGN_MIS_EFF) {
    this.SEGN_MIS_EFF = SEGN_MIS_EFF;
  }
  public cmg_prt_cmg_sw1 with_SEGN_MIS_EFF(String SEGN_MIS_EFF) {
    this.SEGN_MIS_EFF = SEGN_MIS_EFF;
    return this;
  }
  private String SEGN_CONV_EFF;
  public String get_SEGN_CONV_EFF() {
    return SEGN_CONV_EFF;
  }
  public void set_SEGN_CONV_EFF(String SEGN_CONV_EFF) {
    this.SEGN_CONV_EFF = SEGN_CONV_EFF;
  }
  public cmg_prt_cmg_sw1 with_SEGN_CONV_EFF(String SEGN_CONV_EFF) {
    this.SEGN_CONV_EFF = SEGN_CONV_EFF;
    return this;
  }
  private String NOTE;
  public String get_NOTE() {
    return NOTE;
  }
  public void set_NOTE(String NOTE) {
    this.NOTE = NOTE;
  }
  public cmg_prt_cmg_sw1 with_NOTE(String NOTE) {
    this.NOTE = NOTE;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof cmg_prt_cmg_sw1)) {
      return false;
    }
    cmg_prt_cmg_sw1 that = (cmg_prt_cmg_sw1) o;
    boolean equal = true;
    equal = equal && (this.N_ID == null ? that.N_ID == null : this.N_ID.equals(that.N_ID));
    equal = equal && (this.N_ID_FILE == null ? that.N_ID_FILE == null : this.N_ID_FILE.equals(that.N_ID_FILE));
    equal = equal && (this.ANNOMESE_RIFERIMENTO == null ? that.ANNOMESE_RIFERIMENTO == null : this.ANNOMESE_RIFERIMENTO.equals(that.ANNOMESE_RIFERIMENTO));
    equal = equal && (this.DT_CARICAMENTO == null ? that.DT_CARICAMENTO == null : this.DT_CARICAMENTO.equals(that.DT_CARICAMENTO));
    equal = equal && (this.COD_SERVIZIO == null ? that.COD_SERVIZIO == null : this.COD_SERVIZIO.equals(that.COD_SERVIZIO));
    equal = equal && (this.COD_FLUSSO == null ? that.COD_FLUSSO == null : this.COD_FLUSSO.equals(that.COD_FLUSSO));
    equal = equal && (this.PIVA_UTENTE == null ? that.PIVA_UTENTE == null : this.PIVA_UTENTE.equals(that.PIVA_UTENTE));
    equal = equal && (this.PIVA_DISTR == null ? that.PIVA_DISTR == null : this.PIVA_DISTR.equals(that.PIVA_DISTR));
    equal = equal && (this.COD_PRAT_UTENTE == null ? that.COD_PRAT_UTENTE == null : this.COD_PRAT_UTENTE.equals(that.COD_PRAT_UTENTE));
    equal = equal && (this.COD_PRAT_DISTR == null ? that.COD_PRAT_DISTR == null : this.COD_PRAT_DISTR.equals(that.COD_PRAT_DISTR));
    equal = equal && (this.COD_PDR == null ? that.COD_PDR == null : this.COD_PDR.equals(that.COD_PDR));
    equal = equal && (this.MATR_MIS == null ? that.MATR_MIS == null : this.MATR_MIS.equals(that.MATR_MIS));
    equal = equal && (this.DATA_DECO_SWITCH == null ? that.DATA_DECO_SWITCH == null : this.DATA_DECO_SWITCH.equals(that.DATA_DECO_SWITCH));
    equal = equal && (this.VOL_ANNUO_SOST == null ? that.VOL_ANNUO_SOST == null : this.VOL_ANNUO_SOST.equals(that.VOL_ANNUO_SOST));
    equal = equal && (this.CLASSE_GRUPPO_MIS == null ? that.CLASSE_GRUPPO_MIS == null : this.CLASSE_GRUPPO_MIS.equals(that.CLASSE_GRUPPO_MIS));
    equal = equal && (this.N_CIFRE_MIS == null ? that.N_CIFRE_MIS == null : this.N_CIFRE_MIS.equals(that.N_CIFRE_MIS));
    equal = equal && (this.SEGN_MIS_SOST == null ? that.SEGN_MIS_SOST == null : this.SEGN_MIS_SOST.equals(that.SEGN_MIS_SOST));
    equal = equal && (this.TIPO_LETTURA == null ? that.TIPO_LETTURA == null : this.TIPO_LETTURA.equals(that.TIPO_LETTURA));
    equal = equal && (this.PRE_CONV == null ? that.PRE_CONV == null : this.PRE_CONV.equals(that.PRE_CONV));
    equal = equal && (this.GRUPPO_MIS_INT == null ? that.GRUPPO_MIS_INT == null : this.GRUPPO_MIS_INT.equals(that.GRUPPO_MIS_INT));
    equal = equal && (this.COEFF_CORR == null ? that.COEFF_CORR == null : this.COEFF_CORR.equals(that.COEFF_CORR));
    equal = equal && (this.MATR_CONV == null ? that.MATR_CONV == null : this.MATR_CONV.equals(that.MATR_CONV));
    equal = equal && (this.N_CIFRE_CONV == null ? that.N_CIFRE_CONV == null : this.N_CIFRE_CONV.equals(that.N_CIFRE_CONV));
    equal = equal && (this.SEGN_CONV == null ? that.SEGN_CONV == null : this.SEGN_CONV.equals(that.SEGN_CONV));
    equal = equal && (this.DATA_MIS_EFF == null ? that.DATA_MIS_EFF == null : this.DATA_MIS_EFF.equals(that.DATA_MIS_EFF));
    equal = equal && (this.SEGN_MIS_EFF == null ? that.SEGN_MIS_EFF == null : this.SEGN_MIS_EFF.equals(that.SEGN_MIS_EFF));
    equal = equal && (this.SEGN_CONV_EFF == null ? that.SEGN_CONV_EFF == null : this.SEGN_CONV_EFF.equals(that.SEGN_CONV_EFF));
    equal = equal && (this.NOTE == null ? that.NOTE == null : this.NOTE.equals(that.NOTE));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof cmg_prt_cmg_sw1)) {
      return false;
    }
    cmg_prt_cmg_sw1 that = (cmg_prt_cmg_sw1) o;
    boolean equal = true;
    equal = equal && (this.N_ID == null ? that.N_ID == null : this.N_ID.equals(that.N_ID));
    equal = equal && (this.N_ID_FILE == null ? that.N_ID_FILE == null : this.N_ID_FILE.equals(that.N_ID_FILE));
    equal = equal && (this.ANNOMESE_RIFERIMENTO == null ? that.ANNOMESE_RIFERIMENTO == null : this.ANNOMESE_RIFERIMENTO.equals(that.ANNOMESE_RIFERIMENTO));
    equal = equal && (this.DT_CARICAMENTO == null ? that.DT_CARICAMENTO == null : this.DT_CARICAMENTO.equals(that.DT_CARICAMENTO));
    equal = equal && (this.COD_SERVIZIO == null ? that.COD_SERVIZIO == null : this.COD_SERVIZIO.equals(that.COD_SERVIZIO));
    equal = equal && (this.COD_FLUSSO == null ? that.COD_FLUSSO == null : this.COD_FLUSSO.equals(that.COD_FLUSSO));
    equal = equal && (this.PIVA_UTENTE == null ? that.PIVA_UTENTE == null : this.PIVA_UTENTE.equals(that.PIVA_UTENTE));
    equal = equal && (this.PIVA_DISTR == null ? that.PIVA_DISTR == null : this.PIVA_DISTR.equals(that.PIVA_DISTR));
    equal = equal && (this.COD_PRAT_UTENTE == null ? that.COD_PRAT_UTENTE == null : this.COD_PRAT_UTENTE.equals(that.COD_PRAT_UTENTE));
    equal = equal && (this.COD_PRAT_DISTR == null ? that.COD_PRAT_DISTR == null : this.COD_PRAT_DISTR.equals(that.COD_PRAT_DISTR));
    equal = equal && (this.COD_PDR == null ? that.COD_PDR == null : this.COD_PDR.equals(that.COD_PDR));
    equal = equal && (this.MATR_MIS == null ? that.MATR_MIS == null : this.MATR_MIS.equals(that.MATR_MIS));
    equal = equal && (this.DATA_DECO_SWITCH == null ? that.DATA_DECO_SWITCH == null : this.DATA_DECO_SWITCH.equals(that.DATA_DECO_SWITCH));
    equal = equal && (this.VOL_ANNUO_SOST == null ? that.VOL_ANNUO_SOST == null : this.VOL_ANNUO_SOST.equals(that.VOL_ANNUO_SOST));
    equal = equal && (this.CLASSE_GRUPPO_MIS == null ? that.CLASSE_GRUPPO_MIS == null : this.CLASSE_GRUPPO_MIS.equals(that.CLASSE_GRUPPO_MIS));
    equal = equal && (this.N_CIFRE_MIS == null ? that.N_CIFRE_MIS == null : this.N_CIFRE_MIS.equals(that.N_CIFRE_MIS));
    equal = equal && (this.SEGN_MIS_SOST == null ? that.SEGN_MIS_SOST == null : this.SEGN_MIS_SOST.equals(that.SEGN_MIS_SOST));
    equal = equal && (this.TIPO_LETTURA == null ? that.TIPO_LETTURA == null : this.TIPO_LETTURA.equals(that.TIPO_LETTURA));
    equal = equal && (this.PRE_CONV == null ? that.PRE_CONV == null : this.PRE_CONV.equals(that.PRE_CONV));
    equal = equal && (this.GRUPPO_MIS_INT == null ? that.GRUPPO_MIS_INT == null : this.GRUPPO_MIS_INT.equals(that.GRUPPO_MIS_INT));
    equal = equal && (this.COEFF_CORR == null ? that.COEFF_CORR == null : this.COEFF_CORR.equals(that.COEFF_CORR));
    equal = equal && (this.MATR_CONV == null ? that.MATR_CONV == null : this.MATR_CONV.equals(that.MATR_CONV));
    equal = equal && (this.N_CIFRE_CONV == null ? that.N_CIFRE_CONV == null : this.N_CIFRE_CONV.equals(that.N_CIFRE_CONV));
    equal = equal && (this.SEGN_CONV == null ? that.SEGN_CONV == null : this.SEGN_CONV.equals(that.SEGN_CONV));
    equal = equal && (this.DATA_MIS_EFF == null ? that.DATA_MIS_EFF == null : this.DATA_MIS_EFF.equals(that.DATA_MIS_EFF));
    equal = equal && (this.SEGN_MIS_EFF == null ? that.SEGN_MIS_EFF == null : this.SEGN_MIS_EFF.equals(that.SEGN_MIS_EFF));
    equal = equal && (this.SEGN_CONV_EFF == null ? that.SEGN_CONV_EFF == null : this.SEGN_CONV_EFF.equals(that.SEGN_CONV_EFF));
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
    this.COD_PRAT_UTENTE = JdbcWritableBridge.readString(9, __dbResults);
    this.COD_PRAT_DISTR = JdbcWritableBridge.readString(10, __dbResults);
    this.COD_PDR = JdbcWritableBridge.readString(11, __dbResults);
    this.MATR_MIS = JdbcWritableBridge.readString(12, __dbResults);
    this.DATA_DECO_SWITCH = JdbcWritableBridge.readString(13, __dbResults);
    this.VOL_ANNUO_SOST = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.CLASSE_GRUPPO_MIS = JdbcWritableBridge.readString(15, __dbResults);
    this.N_CIFRE_MIS = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.SEGN_MIS_SOST = JdbcWritableBridge.readString(17, __dbResults);
    this.TIPO_LETTURA = JdbcWritableBridge.readString(18, __dbResults);
    this.PRE_CONV = JdbcWritableBridge.readString(19, __dbResults);
    this.GRUPPO_MIS_INT = JdbcWritableBridge.readString(20, __dbResults);
    this.COEFF_CORR = JdbcWritableBridge.readBigDecimal(21, __dbResults);
    this.MATR_CONV = JdbcWritableBridge.readString(22, __dbResults);
    this.N_CIFRE_CONV = JdbcWritableBridge.readBigDecimal(23, __dbResults);
    this.SEGN_CONV = JdbcWritableBridge.readString(24, __dbResults);
    this.DATA_MIS_EFF = JdbcWritableBridge.readString(25, __dbResults);
    this.SEGN_MIS_EFF = JdbcWritableBridge.readString(26, __dbResults);
    this.SEGN_CONV_EFF = JdbcWritableBridge.readString(27, __dbResults);
    this.NOTE = JdbcWritableBridge.readString(28, __dbResults);
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
    this.COD_PRAT_UTENTE = JdbcWritableBridge.readString(9, __dbResults);
    this.COD_PRAT_DISTR = JdbcWritableBridge.readString(10, __dbResults);
    this.COD_PDR = JdbcWritableBridge.readString(11, __dbResults);
    this.MATR_MIS = JdbcWritableBridge.readString(12, __dbResults);
    this.DATA_DECO_SWITCH = JdbcWritableBridge.readString(13, __dbResults);
    this.VOL_ANNUO_SOST = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.CLASSE_GRUPPO_MIS = JdbcWritableBridge.readString(15, __dbResults);
    this.N_CIFRE_MIS = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.SEGN_MIS_SOST = JdbcWritableBridge.readString(17, __dbResults);
    this.TIPO_LETTURA = JdbcWritableBridge.readString(18, __dbResults);
    this.PRE_CONV = JdbcWritableBridge.readString(19, __dbResults);
    this.GRUPPO_MIS_INT = JdbcWritableBridge.readString(20, __dbResults);
    this.COEFF_CORR = JdbcWritableBridge.readBigDecimal(21, __dbResults);
    this.MATR_CONV = JdbcWritableBridge.readString(22, __dbResults);
    this.N_CIFRE_CONV = JdbcWritableBridge.readBigDecimal(23, __dbResults);
    this.SEGN_CONV = JdbcWritableBridge.readString(24, __dbResults);
    this.DATA_MIS_EFF = JdbcWritableBridge.readString(25, __dbResults);
    this.SEGN_MIS_EFF = JdbcWritableBridge.readString(26, __dbResults);
    this.SEGN_CONV_EFF = JdbcWritableBridge.readString(27, __dbResults);
    this.NOTE = JdbcWritableBridge.readString(28, __dbResults);
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
    JdbcWritableBridge.writeString(COD_PRAT_UTENTE, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COD_PRAT_DISTR, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COD_PDR, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(MATR_MIS, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATA_DECO_SWITCH, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(VOL_ANNUO_SOST, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(CLASSE_GRUPPO_MIS, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_CIFRE_MIS, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(SEGN_MIS_SOST, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TIPO_LETTURA, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PRE_CONV, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(GRUPPO_MIS_INT, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(COEFF_CORR, 21 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(MATR_CONV, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_CIFRE_CONV, 23 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(SEGN_CONV, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATA_MIS_EFF, 25 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(SEGN_MIS_EFF, 26 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(SEGN_CONV_EFF, 27 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(NOTE, 28 + __off, 12, __dbStmt);
    return 28;
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
    JdbcWritableBridge.writeString(COD_PRAT_UTENTE, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COD_PRAT_DISTR, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COD_PDR, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(MATR_MIS, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATA_DECO_SWITCH, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(VOL_ANNUO_SOST, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(CLASSE_GRUPPO_MIS, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_CIFRE_MIS, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(SEGN_MIS_SOST, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TIPO_LETTURA, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PRE_CONV, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(GRUPPO_MIS_INT, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(COEFF_CORR, 21 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(MATR_CONV, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_CIFRE_CONV, 23 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(SEGN_CONV, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATA_MIS_EFF, 25 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(SEGN_MIS_EFF, 26 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(SEGN_CONV_EFF, 27 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(NOTE, 28 + __off, 12, __dbStmt);
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
        this.COD_PRAT_UTENTE = null;
    } else {
    this.COD_PRAT_UTENTE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.COD_PRAT_DISTR = null;
    } else {
    this.COD_PRAT_DISTR = Text.readString(__dataIn);
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
        this.DATA_DECO_SWITCH = null;
    } else {
    this.DATA_DECO_SWITCH = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.VOL_ANNUO_SOST = null;
    } else {
    this.VOL_ANNUO_SOST = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CLASSE_GRUPPO_MIS = null;
    } else {
    this.CLASSE_GRUPPO_MIS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_CIFRE_MIS = null;
    } else {
    this.N_CIFRE_MIS = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SEGN_MIS_SOST = null;
    } else {
    this.SEGN_MIS_SOST = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TIPO_LETTURA = null;
    } else {
    this.TIPO_LETTURA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PRE_CONV = null;
    } else {
    this.PRE_CONV = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.GRUPPO_MIS_INT = null;
    } else {
    this.GRUPPO_MIS_INT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.COEFF_CORR = null;
    } else {
    this.COEFF_CORR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MATR_CONV = null;
    } else {
    this.MATR_CONV = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_CIFRE_CONV = null;
    } else {
    this.N_CIFRE_CONV = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SEGN_CONV = null;
    } else {
    this.SEGN_CONV = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATA_MIS_EFF = null;
    } else {
    this.DATA_MIS_EFF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SEGN_MIS_EFF = null;
    } else {
    this.SEGN_MIS_EFF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SEGN_CONV_EFF = null;
    } else {
    this.SEGN_CONV_EFF = Text.readString(__dataIn);
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
    if (null == this.COD_PRAT_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_PRAT_UTENTE);
    }
    if (null == this.COD_PRAT_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_PRAT_DISTR);
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
    if (null == this.DATA_DECO_SWITCH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_DECO_SWITCH);
    }
    if (null == this.VOL_ANNUO_SOST) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.VOL_ANNUO_SOST, __dataOut);
    }
    if (null == this.CLASSE_GRUPPO_MIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLASSE_GRUPPO_MIS);
    }
    if (null == this.N_CIFRE_MIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_CIFRE_MIS, __dataOut);
    }
    if (null == this.SEGN_MIS_SOST) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SEGN_MIS_SOST);
    }
    if (null == this.TIPO_LETTURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIPO_LETTURA);
    }
    if (null == this.PRE_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PRE_CONV);
    }
    if (null == this.GRUPPO_MIS_INT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, GRUPPO_MIS_INT);
    }
    if (null == this.COEFF_CORR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.COEFF_CORR, __dataOut);
    }
    if (null == this.MATR_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MATR_CONV);
    }
    if (null == this.N_CIFRE_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_CIFRE_CONV, __dataOut);
    }
    if (null == this.SEGN_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SEGN_CONV);
    }
    if (null == this.DATA_MIS_EFF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_MIS_EFF);
    }
    if (null == this.SEGN_MIS_EFF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SEGN_MIS_EFF);
    }
    if (null == this.SEGN_CONV_EFF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SEGN_CONV_EFF);
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
    if (null == this.COD_PRAT_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_PRAT_UTENTE);
    }
    if (null == this.COD_PRAT_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_PRAT_DISTR);
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
    if (null == this.DATA_DECO_SWITCH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_DECO_SWITCH);
    }
    if (null == this.VOL_ANNUO_SOST) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.VOL_ANNUO_SOST, __dataOut);
    }
    if (null == this.CLASSE_GRUPPO_MIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CLASSE_GRUPPO_MIS);
    }
    if (null == this.N_CIFRE_MIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_CIFRE_MIS, __dataOut);
    }
    if (null == this.SEGN_MIS_SOST) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SEGN_MIS_SOST);
    }
    if (null == this.TIPO_LETTURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIPO_LETTURA);
    }
    if (null == this.PRE_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PRE_CONV);
    }
    if (null == this.GRUPPO_MIS_INT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, GRUPPO_MIS_INT);
    }
    if (null == this.COEFF_CORR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.COEFF_CORR, __dataOut);
    }
    if (null == this.MATR_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MATR_CONV);
    }
    if (null == this.N_CIFRE_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_CIFRE_CONV, __dataOut);
    }
    if (null == this.SEGN_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SEGN_CONV);
    }
    if (null == this.DATA_MIS_EFF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_MIS_EFF);
    }
    if (null == this.SEGN_MIS_EFF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SEGN_MIS_EFF);
    }
    if (null == this.SEGN_CONV_EFF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SEGN_CONV_EFF);
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_PRAT_UTENTE==null?"":COD_PRAT_UTENTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_PRAT_DISTR==null?"":COD_PRAT_DISTR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_PDR==null?"":COD_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(MATR_MIS==null?"":MATR_MIS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATA_DECO_SWITCH==null?"":DATA_DECO_SWITCH, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(VOL_ANNUO_SOST==null?"":VOL_ANNUO_SOST.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(CLASSE_GRUPPO_MIS==null?"":CLASSE_GRUPPO_MIS, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_CIFRE_MIS==null?"":N_CIFRE_MIS.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(SEGN_MIS_SOST==null?"":SEGN_MIS_SOST, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TIPO_LETTURA==null?"":TIPO_LETTURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PRE_CONV==null?"":PRE_CONV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(GRUPPO_MIS_INT==null?"":GRUPPO_MIS_INT, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(COEFF_CORR==null?"":COEFF_CORR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(MATR_CONV==null?"":MATR_CONV, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_CIFRE_CONV==null?"":N_CIFRE_CONV.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(SEGN_CONV==null?"":SEGN_CONV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATA_MIS_EFF==null?"":DATA_MIS_EFF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(SEGN_MIS_EFF==null?"":SEGN_MIS_EFF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(SEGN_CONV_EFF==null?"":SEGN_CONV_EFF, " ", delimiters));
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_PRAT_UTENTE==null?"":COD_PRAT_UTENTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_PRAT_DISTR==null?"":COD_PRAT_DISTR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_PDR==null?"":COD_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(MATR_MIS==null?"":MATR_MIS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATA_DECO_SWITCH==null?"":DATA_DECO_SWITCH, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(VOL_ANNUO_SOST==null?"":VOL_ANNUO_SOST.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(CLASSE_GRUPPO_MIS==null?"":CLASSE_GRUPPO_MIS, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_CIFRE_MIS==null?"":N_CIFRE_MIS.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(SEGN_MIS_SOST==null?"":SEGN_MIS_SOST, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TIPO_LETTURA==null?"":TIPO_LETTURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PRE_CONV==null?"":PRE_CONV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(GRUPPO_MIS_INT==null?"":GRUPPO_MIS_INT, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(COEFF_CORR==null?"":COEFF_CORR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(MATR_CONV==null?"":MATR_CONV, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_CIFRE_CONV==null?"":N_CIFRE_CONV.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(SEGN_CONV==null?"":SEGN_CONV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATA_MIS_EFF==null?"":DATA_MIS_EFF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(SEGN_MIS_EFF==null?"":SEGN_MIS_EFF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(SEGN_CONV_EFF==null?"":SEGN_CONV_EFF, " ", delimiters));
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
    if (__cur_str.equals("null")) { this.COD_PRAT_UTENTE = null; } else {
      this.COD_PRAT_UTENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COD_PRAT_DISTR = null; } else {
      this.COD_PRAT_DISTR = __cur_str;
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
    if (__cur_str.equals("null")) { this.DATA_DECO_SWITCH = null; } else {
      this.DATA_DECO_SWITCH = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.VOL_ANNUO_SOST = null; } else {
      this.VOL_ANNUO_SOST = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLASSE_GRUPPO_MIS = null; } else {
      this.CLASSE_GRUPPO_MIS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_CIFRE_MIS = null; } else {
      this.N_CIFRE_MIS = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SEGN_MIS_SOST = null; } else {
      this.SEGN_MIS_SOST = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIPO_LETTURA = null; } else {
      this.TIPO_LETTURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PRE_CONV = null; } else {
      this.PRE_CONV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.GRUPPO_MIS_INT = null; } else {
      this.GRUPPO_MIS_INT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.COEFF_CORR = null; } else {
      this.COEFF_CORR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MATR_CONV = null; } else {
      this.MATR_CONV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_CIFRE_CONV = null; } else {
      this.N_CIFRE_CONV = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SEGN_CONV = null; } else {
      this.SEGN_CONV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_MIS_EFF = null; } else {
      this.DATA_MIS_EFF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SEGN_MIS_EFF = null; } else {
      this.SEGN_MIS_EFF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SEGN_CONV_EFF = null; } else {
      this.SEGN_CONV_EFF = __cur_str;
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
    if (__cur_str.equals("null")) { this.COD_PRAT_UTENTE = null; } else {
      this.COD_PRAT_UTENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COD_PRAT_DISTR = null; } else {
      this.COD_PRAT_DISTR = __cur_str;
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
    if (__cur_str.equals("null")) { this.DATA_DECO_SWITCH = null; } else {
      this.DATA_DECO_SWITCH = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.VOL_ANNUO_SOST = null; } else {
      this.VOL_ANNUO_SOST = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CLASSE_GRUPPO_MIS = null; } else {
      this.CLASSE_GRUPPO_MIS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_CIFRE_MIS = null; } else {
      this.N_CIFRE_MIS = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SEGN_MIS_SOST = null; } else {
      this.SEGN_MIS_SOST = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIPO_LETTURA = null; } else {
      this.TIPO_LETTURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PRE_CONV = null; } else {
      this.PRE_CONV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.GRUPPO_MIS_INT = null; } else {
      this.GRUPPO_MIS_INT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.COEFF_CORR = null; } else {
      this.COEFF_CORR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MATR_CONV = null; } else {
      this.MATR_CONV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_CIFRE_CONV = null; } else {
      this.N_CIFRE_CONV = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SEGN_CONV = null; } else {
      this.SEGN_CONV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_MIS_EFF = null; } else {
      this.DATA_MIS_EFF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SEGN_MIS_EFF = null; } else {
      this.SEGN_MIS_EFF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SEGN_CONV_EFF = null; } else {
      this.SEGN_CONV_EFF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.NOTE = null; } else {
      this.NOTE = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    cmg_prt_cmg_sw1 o = (cmg_prt_cmg_sw1) super.clone();
    return o;
  }

  public void clone0(cmg_prt_cmg_sw1 o) throws CloneNotSupportedException {
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
    __sqoop$field_map.put("COD_PRAT_UTENTE", this.COD_PRAT_UTENTE);
    __sqoop$field_map.put("COD_PRAT_DISTR", this.COD_PRAT_DISTR);
    __sqoop$field_map.put("COD_PDR", this.COD_PDR);
    __sqoop$field_map.put("MATR_MIS", this.MATR_MIS);
    __sqoop$field_map.put("DATA_DECO_SWITCH", this.DATA_DECO_SWITCH);
    __sqoop$field_map.put("VOL_ANNUO_SOST", this.VOL_ANNUO_SOST);
    __sqoop$field_map.put("CLASSE_GRUPPO_MIS", this.CLASSE_GRUPPO_MIS);
    __sqoop$field_map.put("N_CIFRE_MIS", this.N_CIFRE_MIS);
    __sqoop$field_map.put("SEGN_MIS_SOST", this.SEGN_MIS_SOST);
    __sqoop$field_map.put("TIPO_LETTURA", this.TIPO_LETTURA);
    __sqoop$field_map.put("PRE_CONV", this.PRE_CONV);
    __sqoop$field_map.put("GRUPPO_MIS_INT", this.GRUPPO_MIS_INT);
    __sqoop$field_map.put("COEFF_CORR", this.COEFF_CORR);
    __sqoop$field_map.put("MATR_CONV", this.MATR_CONV);
    __sqoop$field_map.put("N_CIFRE_CONV", this.N_CIFRE_CONV);
    __sqoop$field_map.put("SEGN_CONV", this.SEGN_CONV);
    __sqoop$field_map.put("DATA_MIS_EFF", this.DATA_MIS_EFF);
    __sqoop$field_map.put("SEGN_MIS_EFF", this.SEGN_MIS_EFF);
    __sqoop$field_map.put("SEGN_CONV_EFF", this.SEGN_CONV_EFF);
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
    __sqoop$field_map.put("COD_PRAT_UTENTE", this.COD_PRAT_UTENTE);
    __sqoop$field_map.put("COD_PRAT_DISTR", this.COD_PRAT_DISTR);
    __sqoop$field_map.put("COD_PDR", this.COD_PDR);
    __sqoop$field_map.put("MATR_MIS", this.MATR_MIS);
    __sqoop$field_map.put("DATA_DECO_SWITCH", this.DATA_DECO_SWITCH);
    __sqoop$field_map.put("VOL_ANNUO_SOST", this.VOL_ANNUO_SOST);
    __sqoop$field_map.put("CLASSE_GRUPPO_MIS", this.CLASSE_GRUPPO_MIS);
    __sqoop$field_map.put("N_CIFRE_MIS", this.N_CIFRE_MIS);
    __sqoop$field_map.put("SEGN_MIS_SOST", this.SEGN_MIS_SOST);
    __sqoop$field_map.put("TIPO_LETTURA", this.TIPO_LETTURA);
    __sqoop$field_map.put("PRE_CONV", this.PRE_CONV);
    __sqoop$field_map.put("GRUPPO_MIS_INT", this.GRUPPO_MIS_INT);
    __sqoop$field_map.put("COEFF_CORR", this.COEFF_CORR);
    __sqoop$field_map.put("MATR_CONV", this.MATR_CONV);
    __sqoop$field_map.put("N_CIFRE_CONV", this.N_CIFRE_CONV);
    __sqoop$field_map.put("SEGN_CONV", this.SEGN_CONV);
    __sqoop$field_map.put("DATA_MIS_EFF", this.DATA_MIS_EFF);
    __sqoop$field_map.put("SEGN_MIS_EFF", this.SEGN_MIS_EFF);
    __sqoop$field_map.put("SEGN_CONV_EFF", this.SEGN_CONV_EFF);
    __sqoop$field_map.put("NOTE", this.NOTE);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
