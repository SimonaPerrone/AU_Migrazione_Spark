// ORM class for table 'switch_gas.prt_vtg6'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 14:10:54 CEST 2019
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

public class switch_gas_prt_vtg6 extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_VTG6", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_VTG6 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_VTG", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_VTG = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_PRATICA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_PRATICA = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_UTENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_UTENTE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_CODICE_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_PDR = (String)value;
      }
    });
    setters.put("T_MATR_MIS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MATR_MIS = (String)value;
      }
    });
    setters.put("D_DATA_ATT_CONTR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_ATT_CONTR = (String)value;
      }
    });
    setters.put("N_VOL_ANNUO_SOST", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_VOL_ANNUO_SOST = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_CLASSE_GRUPPO_MIS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CLASSE_GRUPPO_MIS = (String)value;
      }
    });
    setters.put("T_CIFRE_MIS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CIFRE_MIS = (String)value;
      }
    });
    setters.put("T_SEGN_MIS_SOST", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_SEGN_MIS_SOST = (String)value;
      }
    });
    setters.put("T_PRE_CONV", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PRE_CONV = (String)value;
      }
    });
    setters.put("T_GRUPPO_MIS_INT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_GRUPPO_MIS_INT = (String)value;
      }
    });
    setters.put("N_COEFF_CORR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_COEFF_CORR = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_MATR_CONV", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MATR_CONV = (String)value;
      }
    });
    setters.put("T_CIFRE_CONV", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CIFRE_CONV = (String)value;
      }
    });
    setters.put("T_SEGN_CONV", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_SEGN_CONV = (String)value;
      }
    });
    setters.put("D_DATA_MIS_EFF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_MIS_EFF = (String)value;
      }
    });
    setters.put("T_SEGN_MIS_EFF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_SEGN_MIS_EFF = (String)value;
      }
    });
    setters.put("T_SEGN_CONV_EFF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_SEGN_CONV_EFF = (String)value;
      }
    });
    setters.put("T_NOTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_NOTE = (String)value;
      }
    });
    setters.put("T_TIPO_LETTURA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO_LETTURA = (String)value;
      }
    });
    setters.put("B_COPIATO_TMG_MISURE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_COPIATO_TMG_MISURE = (String)value;
      }
    });
  }
  public switch_gas_prt_vtg6() {
    init0();
  }
  private java.math.BigDecimal N_ID_VTG6;
  public java.math.BigDecimal get_N_ID_VTG6() {
    return N_ID_VTG6;
  }
  public void set_N_ID_VTG6(java.math.BigDecimal N_ID_VTG6) {
    this.N_ID_VTG6 = N_ID_VTG6;
  }
  public switch_gas_prt_vtg6 with_N_ID_VTG6(java.math.BigDecimal N_ID_VTG6) {
    this.N_ID_VTG6 = N_ID_VTG6;
    return this;
  }
  private java.math.BigDecimal N_ID_VTG;
  public java.math.BigDecimal get_N_ID_VTG() {
    return N_ID_VTG;
  }
  public void set_N_ID_VTG(java.math.BigDecimal N_ID_VTG) {
    this.N_ID_VTG = N_ID_VTG;
  }
  public switch_gas_prt_vtg6 with_N_ID_VTG(java.math.BigDecimal N_ID_VTG) {
    this.N_ID_VTG = N_ID_VTG;
    return this;
  }
  private java.math.BigDecimal N_ID_PRATICA;
  public java.math.BigDecimal get_N_ID_PRATICA() {
    return N_ID_PRATICA;
  }
  public void set_N_ID_PRATICA(java.math.BigDecimal N_ID_PRATICA) {
    this.N_ID_PRATICA = N_ID_PRATICA;
  }
  public switch_gas_prt_vtg6 with_N_ID_PRATICA(java.math.BigDecimal N_ID_PRATICA) {
    this.N_ID_PRATICA = N_ID_PRATICA;
    return this;
  }
  private java.math.BigDecimal N_ID_UTENTE;
  public java.math.BigDecimal get_N_ID_UTENTE() {
    return N_ID_UTENTE;
  }
  public void set_N_ID_UTENTE(java.math.BigDecimal N_ID_UTENTE) {
    this.N_ID_UTENTE = N_ID_UTENTE;
  }
  public switch_gas_prt_vtg6 with_N_ID_UTENTE(java.math.BigDecimal N_ID_UTENTE) {
    this.N_ID_UTENTE = N_ID_UTENTE;
    return this;
  }
  private String T_CODICE_PDR;
  public String get_T_CODICE_PDR() {
    return T_CODICE_PDR;
  }
  public void set_T_CODICE_PDR(String T_CODICE_PDR) {
    this.T_CODICE_PDR = T_CODICE_PDR;
  }
  public switch_gas_prt_vtg6 with_T_CODICE_PDR(String T_CODICE_PDR) {
    this.T_CODICE_PDR = T_CODICE_PDR;
    return this;
  }
  private String T_MATR_MIS;
  public String get_T_MATR_MIS() {
    return T_MATR_MIS;
  }
  public void set_T_MATR_MIS(String T_MATR_MIS) {
    this.T_MATR_MIS = T_MATR_MIS;
  }
  public switch_gas_prt_vtg6 with_T_MATR_MIS(String T_MATR_MIS) {
    this.T_MATR_MIS = T_MATR_MIS;
    return this;
  }
  private String D_DATA_ATT_CONTR;
  public String get_D_DATA_ATT_CONTR() {
    return D_DATA_ATT_CONTR;
  }
  public void set_D_DATA_ATT_CONTR(String D_DATA_ATT_CONTR) {
    this.D_DATA_ATT_CONTR = D_DATA_ATT_CONTR;
  }
  public switch_gas_prt_vtg6 with_D_DATA_ATT_CONTR(String D_DATA_ATT_CONTR) {
    this.D_DATA_ATT_CONTR = D_DATA_ATT_CONTR;
    return this;
  }
  private java.math.BigDecimal N_VOL_ANNUO_SOST;
  public java.math.BigDecimal get_N_VOL_ANNUO_SOST() {
    return N_VOL_ANNUO_SOST;
  }
  public void set_N_VOL_ANNUO_SOST(java.math.BigDecimal N_VOL_ANNUO_SOST) {
    this.N_VOL_ANNUO_SOST = N_VOL_ANNUO_SOST;
  }
  public switch_gas_prt_vtg6 with_N_VOL_ANNUO_SOST(java.math.BigDecimal N_VOL_ANNUO_SOST) {
    this.N_VOL_ANNUO_SOST = N_VOL_ANNUO_SOST;
    return this;
  }
  private String T_CLASSE_GRUPPO_MIS;
  public String get_T_CLASSE_GRUPPO_MIS() {
    return T_CLASSE_GRUPPO_MIS;
  }
  public void set_T_CLASSE_GRUPPO_MIS(String T_CLASSE_GRUPPO_MIS) {
    this.T_CLASSE_GRUPPO_MIS = T_CLASSE_GRUPPO_MIS;
  }
  public switch_gas_prt_vtg6 with_T_CLASSE_GRUPPO_MIS(String T_CLASSE_GRUPPO_MIS) {
    this.T_CLASSE_GRUPPO_MIS = T_CLASSE_GRUPPO_MIS;
    return this;
  }
  private String T_CIFRE_MIS;
  public String get_T_CIFRE_MIS() {
    return T_CIFRE_MIS;
  }
  public void set_T_CIFRE_MIS(String T_CIFRE_MIS) {
    this.T_CIFRE_MIS = T_CIFRE_MIS;
  }
  public switch_gas_prt_vtg6 with_T_CIFRE_MIS(String T_CIFRE_MIS) {
    this.T_CIFRE_MIS = T_CIFRE_MIS;
    return this;
  }
  private String T_SEGN_MIS_SOST;
  public String get_T_SEGN_MIS_SOST() {
    return T_SEGN_MIS_SOST;
  }
  public void set_T_SEGN_MIS_SOST(String T_SEGN_MIS_SOST) {
    this.T_SEGN_MIS_SOST = T_SEGN_MIS_SOST;
  }
  public switch_gas_prt_vtg6 with_T_SEGN_MIS_SOST(String T_SEGN_MIS_SOST) {
    this.T_SEGN_MIS_SOST = T_SEGN_MIS_SOST;
    return this;
  }
  private String T_PRE_CONV;
  public String get_T_PRE_CONV() {
    return T_PRE_CONV;
  }
  public void set_T_PRE_CONV(String T_PRE_CONV) {
    this.T_PRE_CONV = T_PRE_CONV;
  }
  public switch_gas_prt_vtg6 with_T_PRE_CONV(String T_PRE_CONV) {
    this.T_PRE_CONV = T_PRE_CONV;
    return this;
  }
  private String T_GRUPPO_MIS_INT;
  public String get_T_GRUPPO_MIS_INT() {
    return T_GRUPPO_MIS_INT;
  }
  public void set_T_GRUPPO_MIS_INT(String T_GRUPPO_MIS_INT) {
    this.T_GRUPPO_MIS_INT = T_GRUPPO_MIS_INT;
  }
  public switch_gas_prt_vtg6 with_T_GRUPPO_MIS_INT(String T_GRUPPO_MIS_INT) {
    this.T_GRUPPO_MIS_INT = T_GRUPPO_MIS_INT;
    return this;
  }
  private java.math.BigDecimal N_COEFF_CORR;
  public java.math.BigDecimal get_N_COEFF_CORR() {
    return N_COEFF_CORR;
  }
  public void set_N_COEFF_CORR(java.math.BigDecimal N_COEFF_CORR) {
    this.N_COEFF_CORR = N_COEFF_CORR;
  }
  public switch_gas_prt_vtg6 with_N_COEFF_CORR(java.math.BigDecimal N_COEFF_CORR) {
    this.N_COEFF_CORR = N_COEFF_CORR;
    return this;
  }
  private String T_MATR_CONV;
  public String get_T_MATR_CONV() {
    return T_MATR_CONV;
  }
  public void set_T_MATR_CONV(String T_MATR_CONV) {
    this.T_MATR_CONV = T_MATR_CONV;
  }
  public switch_gas_prt_vtg6 with_T_MATR_CONV(String T_MATR_CONV) {
    this.T_MATR_CONV = T_MATR_CONV;
    return this;
  }
  private String T_CIFRE_CONV;
  public String get_T_CIFRE_CONV() {
    return T_CIFRE_CONV;
  }
  public void set_T_CIFRE_CONV(String T_CIFRE_CONV) {
    this.T_CIFRE_CONV = T_CIFRE_CONV;
  }
  public switch_gas_prt_vtg6 with_T_CIFRE_CONV(String T_CIFRE_CONV) {
    this.T_CIFRE_CONV = T_CIFRE_CONV;
    return this;
  }
  private String T_SEGN_CONV;
  public String get_T_SEGN_CONV() {
    return T_SEGN_CONV;
  }
  public void set_T_SEGN_CONV(String T_SEGN_CONV) {
    this.T_SEGN_CONV = T_SEGN_CONV;
  }
  public switch_gas_prt_vtg6 with_T_SEGN_CONV(String T_SEGN_CONV) {
    this.T_SEGN_CONV = T_SEGN_CONV;
    return this;
  }
  private String D_DATA_MIS_EFF;
  public String get_D_DATA_MIS_EFF() {
    return D_DATA_MIS_EFF;
  }
  public void set_D_DATA_MIS_EFF(String D_DATA_MIS_EFF) {
    this.D_DATA_MIS_EFF = D_DATA_MIS_EFF;
  }
  public switch_gas_prt_vtg6 with_D_DATA_MIS_EFF(String D_DATA_MIS_EFF) {
    this.D_DATA_MIS_EFF = D_DATA_MIS_EFF;
    return this;
  }
  private String T_SEGN_MIS_EFF;
  public String get_T_SEGN_MIS_EFF() {
    return T_SEGN_MIS_EFF;
  }
  public void set_T_SEGN_MIS_EFF(String T_SEGN_MIS_EFF) {
    this.T_SEGN_MIS_EFF = T_SEGN_MIS_EFF;
  }
  public switch_gas_prt_vtg6 with_T_SEGN_MIS_EFF(String T_SEGN_MIS_EFF) {
    this.T_SEGN_MIS_EFF = T_SEGN_MIS_EFF;
    return this;
  }
  private String T_SEGN_CONV_EFF;
  public String get_T_SEGN_CONV_EFF() {
    return T_SEGN_CONV_EFF;
  }
  public void set_T_SEGN_CONV_EFF(String T_SEGN_CONV_EFF) {
    this.T_SEGN_CONV_EFF = T_SEGN_CONV_EFF;
  }
  public switch_gas_prt_vtg6 with_T_SEGN_CONV_EFF(String T_SEGN_CONV_EFF) {
    this.T_SEGN_CONV_EFF = T_SEGN_CONV_EFF;
    return this;
  }
  private String T_NOTE;
  public String get_T_NOTE() {
    return T_NOTE;
  }
  public void set_T_NOTE(String T_NOTE) {
    this.T_NOTE = T_NOTE;
  }
  public switch_gas_prt_vtg6 with_T_NOTE(String T_NOTE) {
    this.T_NOTE = T_NOTE;
    return this;
  }
  private String T_TIPO_LETTURA;
  public String get_T_TIPO_LETTURA() {
    return T_TIPO_LETTURA;
  }
  public void set_T_TIPO_LETTURA(String T_TIPO_LETTURA) {
    this.T_TIPO_LETTURA = T_TIPO_LETTURA;
  }
  public switch_gas_prt_vtg6 with_T_TIPO_LETTURA(String T_TIPO_LETTURA) {
    this.T_TIPO_LETTURA = T_TIPO_LETTURA;
    return this;
  }
  private String B_COPIATO_TMG_MISURE;
  public String get_B_COPIATO_TMG_MISURE() {
    return B_COPIATO_TMG_MISURE;
  }
  public void set_B_COPIATO_TMG_MISURE(String B_COPIATO_TMG_MISURE) {
    this.B_COPIATO_TMG_MISURE = B_COPIATO_TMG_MISURE;
  }
  public switch_gas_prt_vtg6 with_B_COPIATO_TMG_MISURE(String B_COPIATO_TMG_MISURE) {
    this.B_COPIATO_TMG_MISURE = B_COPIATO_TMG_MISURE;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof switch_gas_prt_vtg6)) {
      return false;
    }
    switch_gas_prt_vtg6 that = (switch_gas_prt_vtg6) o;
    boolean equal = true;
    equal = equal && (this.N_ID_VTG6 == null ? that.N_ID_VTG6 == null : this.N_ID_VTG6.equals(that.N_ID_VTG6));
    equal = equal && (this.N_ID_VTG == null ? that.N_ID_VTG == null : this.N_ID_VTG.equals(that.N_ID_VTG));
    equal = equal && (this.N_ID_PRATICA == null ? that.N_ID_PRATICA == null : this.N_ID_PRATICA.equals(that.N_ID_PRATICA));
    equal = equal && (this.N_ID_UTENTE == null ? that.N_ID_UTENTE == null : this.N_ID_UTENTE.equals(that.N_ID_UTENTE));
    equal = equal && (this.T_CODICE_PDR == null ? that.T_CODICE_PDR == null : this.T_CODICE_PDR.equals(that.T_CODICE_PDR));
    equal = equal && (this.T_MATR_MIS == null ? that.T_MATR_MIS == null : this.T_MATR_MIS.equals(that.T_MATR_MIS));
    equal = equal && (this.D_DATA_ATT_CONTR == null ? that.D_DATA_ATT_CONTR == null : this.D_DATA_ATT_CONTR.equals(that.D_DATA_ATT_CONTR));
    equal = equal && (this.N_VOL_ANNUO_SOST == null ? that.N_VOL_ANNUO_SOST == null : this.N_VOL_ANNUO_SOST.equals(that.N_VOL_ANNUO_SOST));
    equal = equal && (this.T_CLASSE_GRUPPO_MIS == null ? that.T_CLASSE_GRUPPO_MIS == null : this.T_CLASSE_GRUPPO_MIS.equals(that.T_CLASSE_GRUPPO_MIS));
    equal = equal && (this.T_CIFRE_MIS == null ? that.T_CIFRE_MIS == null : this.T_CIFRE_MIS.equals(that.T_CIFRE_MIS));
    equal = equal && (this.T_SEGN_MIS_SOST == null ? that.T_SEGN_MIS_SOST == null : this.T_SEGN_MIS_SOST.equals(that.T_SEGN_MIS_SOST));
    equal = equal && (this.T_PRE_CONV == null ? that.T_PRE_CONV == null : this.T_PRE_CONV.equals(that.T_PRE_CONV));
    equal = equal && (this.T_GRUPPO_MIS_INT == null ? that.T_GRUPPO_MIS_INT == null : this.T_GRUPPO_MIS_INT.equals(that.T_GRUPPO_MIS_INT));
    equal = equal && (this.N_COEFF_CORR == null ? that.N_COEFF_CORR == null : this.N_COEFF_CORR.equals(that.N_COEFF_CORR));
    equal = equal && (this.T_MATR_CONV == null ? that.T_MATR_CONV == null : this.T_MATR_CONV.equals(that.T_MATR_CONV));
    equal = equal && (this.T_CIFRE_CONV == null ? that.T_CIFRE_CONV == null : this.T_CIFRE_CONV.equals(that.T_CIFRE_CONV));
    equal = equal && (this.T_SEGN_CONV == null ? that.T_SEGN_CONV == null : this.T_SEGN_CONV.equals(that.T_SEGN_CONV));
    equal = equal && (this.D_DATA_MIS_EFF == null ? that.D_DATA_MIS_EFF == null : this.D_DATA_MIS_EFF.equals(that.D_DATA_MIS_EFF));
    equal = equal && (this.T_SEGN_MIS_EFF == null ? that.T_SEGN_MIS_EFF == null : this.T_SEGN_MIS_EFF.equals(that.T_SEGN_MIS_EFF));
    equal = equal && (this.T_SEGN_CONV_EFF == null ? that.T_SEGN_CONV_EFF == null : this.T_SEGN_CONV_EFF.equals(that.T_SEGN_CONV_EFF));
    equal = equal && (this.T_NOTE == null ? that.T_NOTE == null : this.T_NOTE.equals(that.T_NOTE));
    equal = equal && (this.T_TIPO_LETTURA == null ? that.T_TIPO_LETTURA == null : this.T_TIPO_LETTURA.equals(that.T_TIPO_LETTURA));
    equal = equal && (this.B_COPIATO_TMG_MISURE == null ? that.B_COPIATO_TMG_MISURE == null : this.B_COPIATO_TMG_MISURE.equals(that.B_COPIATO_TMG_MISURE));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof switch_gas_prt_vtg6)) {
      return false;
    }
    switch_gas_prt_vtg6 that = (switch_gas_prt_vtg6) o;
    boolean equal = true;
    equal = equal && (this.N_ID_VTG6 == null ? that.N_ID_VTG6 == null : this.N_ID_VTG6.equals(that.N_ID_VTG6));
    equal = equal && (this.N_ID_VTG == null ? that.N_ID_VTG == null : this.N_ID_VTG.equals(that.N_ID_VTG));
    equal = equal && (this.N_ID_PRATICA == null ? that.N_ID_PRATICA == null : this.N_ID_PRATICA.equals(that.N_ID_PRATICA));
    equal = equal && (this.N_ID_UTENTE == null ? that.N_ID_UTENTE == null : this.N_ID_UTENTE.equals(that.N_ID_UTENTE));
    equal = equal && (this.T_CODICE_PDR == null ? that.T_CODICE_PDR == null : this.T_CODICE_PDR.equals(that.T_CODICE_PDR));
    equal = equal && (this.T_MATR_MIS == null ? that.T_MATR_MIS == null : this.T_MATR_MIS.equals(that.T_MATR_MIS));
    equal = equal && (this.D_DATA_ATT_CONTR == null ? that.D_DATA_ATT_CONTR == null : this.D_DATA_ATT_CONTR.equals(that.D_DATA_ATT_CONTR));
    equal = equal && (this.N_VOL_ANNUO_SOST == null ? that.N_VOL_ANNUO_SOST == null : this.N_VOL_ANNUO_SOST.equals(that.N_VOL_ANNUO_SOST));
    equal = equal && (this.T_CLASSE_GRUPPO_MIS == null ? that.T_CLASSE_GRUPPO_MIS == null : this.T_CLASSE_GRUPPO_MIS.equals(that.T_CLASSE_GRUPPO_MIS));
    equal = equal && (this.T_CIFRE_MIS == null ? that.T_CIFRE_MIS == null : this.T_CIFRE_MIS.equals(that.T_CIFRE_MIS));
    equal = equal && (this.T_SEGN_MIS_SOST == null ? that.T_SEGN_MIS_SOST == null : this.T_SEGN_MIS_SOST.equals(that.T_SEGN_MIS_SOST));
    equal = equal && (this.T_PRE_CONV == null ? that.T_PRE_CONV == null : this.T_PRE_CONV.equals(that.T_PRE_CONV));
    equal = equal && (this.T_GRUPPO_MIS_INT == null ? that.T_GRUPPO_MIS_INT == null : this.T_GRUPPO_MIS_INT.equals(that.T_GRUPPO_MIS_INT));
    equal = equal && (this.N_COEFF_CORR == null ? that.N_COEFF_CORR == null : this.N_COEFF_CORR.equals(that.N_COEFF_CORR));
    equal = equal && (this.T_MATR_CONV == null ? that.T_MATR_CONV == null : this.T_MATR_CONV.equals(that.T_MATR_CONV));
    equal = equal && (this.T_CIFRE_CONV == null ? that.T_CIFRE_CONV == null : this.T_CIFRE_CONV.equals(that.T_CIFRE_CONV));
    equal = equal && (this.T_SEGN_CONV == null ? that.T_SEGN_CONV == null : this.T_SEGN_CONV.equals(that.T_SEGN_CONV));
    equal = equal && (this.D_DATA_MIS_EFF == null ? that.D_DATA_MIS_EFF == null : this.D_DATA_MIS_EFF.equals(that.D_DATA_MIS_EFF));
    equal = equal && (this.T_SEGN_MIS_EFF == null ? that.T_SEGN_MIS_EFF == null : this.T_SEGN_MIS_EFF.equals(that.T_SEGN_MIS_EFF));
    equal = equal && (this.T_SEGN_CONV_EFF == null ? that.T_SEGN_CONV_EFF == null : this.T_SEGN_CONV_EFF.equals(that.T_SEGN_CONV_EFF));
    equal = equal && (this.T_NOTE == null ? that.T_NOTE == null : this.T_NOTE.equals(that.T_NOTE));
    equal = equal && (this.T_TIPO_LETTURA == null ? that.T_TIPO_LETTURA == null : this.T_TIPO_LETTURA.equals(that.T_TIPO_LETTURA));
    equal = equal && (this.B_COPIATO_TMG_MISURE == null ? that.B_COPIATO_TMG_MISURE == null : this.B_COPIATO_TMG_MISURE.equals(that.B_COPIATO_TMG_MISURE));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_VTG6 = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_VTG = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.N_ID_PRATICA = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.N_ID_UTENTE = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.T_CODICE_PDR = JdbcWritableBridge.readString(5, __dbResults);
    this.T_MATR_MIS = JdbcWritableBridge.readString(6, __dbResults);
    this.D_DATA_ATT_CONTR = JdbcWritableBridge.readString(7, __dbResults);
    this.N_VOL_ANNUO_SOST = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.T_CLASSE_GRUPPO_MIS = JdbcWritableBridge.readString(9, __dbResults);
    this.T_CIFRE_MIS = JdbcWritableBridge.readString(10, __dbResults);
    this.T_SEGN_MIS_SOST = JdbcWritableBridge.readString(11, __dbResults);
    this.T_PRE_CONV = JdbcWritableBridge.readString(12, __dbResults);
    this.T_GRUPPO_MIS_INT = JdbcWritableBridge.readString(13, __dbResults);
    this.N_COEFF_CORR = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.T_MATR_CONV = JdbcWritableBridge.readString(15, __dbResults);
    this.T_CIFRE_CONV = JdbcWritableBridge.readString(16, __dbResults);
    this.T_SEGN_CONV = JdbcWritableBridge.readString(17, __dbResults);
    this.D_DATA_MIS_EFF = JdbcWritableBridge.readString(18, __dbResults);
    this.T_SEGN_MIS_EFF = JdbcWritableBridge.readString(19, __dbResults);
    this.T_SEGN_CONV_EFF = JdbcWritableBridge.readString(20, __dbResults);
    this.T_NOTE = JdbcWritableBridge.readString(21, __dbResults);
    this.T_TIPO_LETTURA = JdbcWritableBridge.readString(22, __dbResults);
    this.B_COPIATO_TMG_MISURE = JdbcWritableBridge.readString(23, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_VTG6 = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_VTG = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.N_ID_PRATICA = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.N_ID_UTENTE = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.T_CODICE_PDR = JdbcWritableBridge.readString(5, __dbResults);
    this.T_MATR_MIS = JdbcWritableBridge.readString(6, __dbResults);
    this.D_DATA_ATT_CONTR = JdbcWritableBridge.readString(7, __dbResults);
    this.N_VOL_ANNUO_SOST = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.T_CLASSE_GRUPPO_MIS = JdbcWritableBridge.readString(9, __dbResults);
    this.T_CIFRE_MIS = JdbcWritableBridge.readString(10, __dbResults);
    this.T_SEGN_MIS_SOST = JdbcWritableBridge.readString(11, __dbResults);
    this.T_PRE_CONV = JdbcWritableBridge.readString(12, __dbResults);
    this.T_GRUPPO_MIS_INT = JdbcWritableBridge.readString(13, __dbResults);
    this.N_COEFF_CORR = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.T_MATR_CONV = JdbcWritableBridge.readString(15, __dbResults);
    this.T_CIFRE_CONV = JdbcWritableBridge.readString(16, __dbResults);
    this.T_SEGN_CONV = JdbcWritableBridge.readString(17, __dbResults);
    this.D_DATA_MIS_EFF = JdbcWritableBridge.readString(18, __dbResults);
    this.T_SEGN_MIS_EFF = JdbcWritableBridge.readString(19, __dbResults);
    this.T_SEGN_CONV_EFF = JdbcWritableBridge.readString(20, __dbResults);
    this.T_NOTE = JdbcWritableBridge.readString(21, __dbResults);
    this.T_TIPO_LETTURA = JdbcWritableBridge.readString(22, __dbResults);
    this.B_COPIATO_TMG_MISURE = JdbcWritableBridge.readString(23, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_VTG6, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_VTG, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PRATICA, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UTENTE, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_PDR, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MATR_MIS, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_ATT_CONTR, 7 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_VOL_ANNUO_SOST, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CLASSE_GRUPPO_MIS, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CIFRE_MIS, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_SEGN_MIS_SOST, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PRE_CONV, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_GRUPPO_MIS_INT, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_COEFF_CORR, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_MATR_CONV, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CIFRE_CONV, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_SEGN_CONV, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_MIS_EFF, 18 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_SEGN_MIS_EFF, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_SEGN_CONV_EFF, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTE, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_LETTURA, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_COPIATO_TMG_MISURE, 23 + __off, 12, __dbStmt);
    return 23;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_VTG6, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_VTG, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PRATICA, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UTENTE, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_PDR, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MATR_MIS, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_ATT_CONTR, 7 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_VOL_ANNUO_SOST, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_CLASSE_GRUPPO_MIS, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CIFRE_MIS, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_SEGN_MIS_SOST, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PRE_CONV, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_GRUPPO_MIS_INT, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_COEFF_CORR, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_MATR_CONV, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CIFRE_CONV, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_SEGN_CONV, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_MIS_EFF, 18 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_SEGN_MIS_EFF, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_SEGN_CONV_EFF, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTE, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_LETTURA, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_COPIATO_TMG_MISURE, 23 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_VTG6 = null;
    } else {
    this.N_ID_VTG6 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_VTG = null;
    } else {
    this.N_ID_VTG = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_PRATICA = null;
    } else {
    this.N_ID_PRATICA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_UTENTE = null;
    } else {
    this.N_ID_UTENTE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_PDR = null;
    } else {
    this.T_CODICE_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MATR_MIS = null;
    } else {
    this.T_MATR_MIS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_ATT_CONTR = null;
    } else {
    this.D_DATA_ATT_CONTR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_VOL_ANNUO_SOST = null;
    } else {
    this.N_VOL_ANNUO_SOST = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CLASSE_GRUPPO_MIS = null;
    } else {
    this.T_CLASSE_GRUPPO_MIS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CIFRE_MIS = null;
    } else {
    this.T_CIFRE_MIS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_SEGN_MIS_SOST = null;
    } else {
    this.T_SEGN_MIS_SOST = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PRE_CONV = null;
    } else {
    this.T_PRE_CONV = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_GRUPPO_MIS_INT = null;
    } else {
    this.T_GRUPPO_MIS_INT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_COEFF_CORR = null;
    } else {
    this.N_COEFF_CORR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MATR_CONV = null;
    } else {
    this.T_MATR_CONV = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CIFRE_CONV = null;
    } else {
    this.T_CIFRE_CONV = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_SEGN_CONV = null;
    } else {
    this.T_SEGN_CONV = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_MIS_EFF = null;
    } else {
    this.D_DATA_MIS_EFF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_SEGN_MIS_EFF = null;
    } else {
    this.T_SEGN_MIS_EFF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_SEGN_CONV_EFF = null;
    } else {
    this.T_SEGN_CONV_EFF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_NOTE = null;
    } else {
    this.T_NOTE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO_LETTURA = null;
    } else {
    this.T_TIPO_LETTURA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_COPIATO_TMG_MISURE = null;
    } else {
    this.B_COPIATO_TMG_MISURE = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_VTG6) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_VTG6, __dataOut);
    }
    if (null == this.N_ID_VTG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_VTG, __dataOut);
    }
    if (null == this.N_ID_PRATICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PRATICA, __dataOut);
    }
    if (null == this.N_ID_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UTENTE, __dataOut);
    }
    if (null == this.T_CODICE_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_PDR);
    }
    if (null == this.T_MATR_MIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MATR_MIS);
    }
    if (null == this.D_DATA_ATT_CONTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_ATT_CONTR);
    }
    if (null == this.N_VOL_ANNUO_SOST) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_VOL_ANNUO_SOST, __dataOut);
    }
    if (null == this.T_CLASSE_GRUPPO_MIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CLASSE_GRUPPO_MIS);
    }
    if (null == this.T_CIFRE_MIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CIFRE_MIS);
    }
    if (null == this.T_SEGN_MIS_SOST) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_SEGN_MIS_SOST);
    }
    if (null == this.T_PRE_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PRE_CONV);
    }
    if (null == this.T_GRUPPO_MIS_INT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_GRUPPO_MIS_INT);
    }
    if (null == this.N_COEFF_CORR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_COEFF_CORR, __dataOut);
    }
    if (null == this.T_MATR_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MATR_CONV);
    }
    if (null == this.T_CIFRE_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CIFRE_CONV);
    }
    if (null == this.T_SEGN_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_SEGN_CONV);
    }
    if (null == this.D_DATA_MIS_EFF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_MIS_EFF);
    }
    if (null == this.T_SEGN_MIS_EFF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_SEGN_MIS_EFF);
    }
    if (null == this.T_SEGN_CONV_EFF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_SEGN_CONV_EFF);
    }
    if (null == this.T_NOTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOTE);
    }
    if (null == this.T_TIPO_LETTURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_LETTURA);
    }
    if (null == this.B_COPIATO_TMG_MISURE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_COPIATO_TMG_MISURE);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_VTG6) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_VTG6, __dataOut);
    }
    if (null == this.N_ID_VTG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_VTG, __dataOut);
    }
    if (null == this.N_ID_PRATICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PRATICA, __dataOut);
    }
    if (null == this.N_ID_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UTENTE, __dataOut);
    }
    if (null == this.T_CODICE_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_PDR);
    }
    if (null == this.T_MATR_MIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MATR_MIS);
    }
    if (null == this.D_DATA_ATT_CONTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_ATT_CONTR);
    }
    if (null == this.N_VOL_ANNUO_SOST) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_VOL_ANNUO_SOST, __dataOut);
    }
    if (null == this.T_CLASSE_GRUPPO_MIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CLASSE_GRUPPO_MIS);
    }
    if (null == this.T_CIFRE_MIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CIFRE_MIS);
    }
    if (null == this.T_SEGN_MIS_SOST) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_SEGN_MIS_SOST);
    }
    if (null == this.T_PRE_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PRE_CONV);
    }
    if (null == this.T_GRUPPO_MIS_INT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_GRUPPO_MIS_INT);
    }
    if (null == this.N_COEFF_CORR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_COEFF_CORR, __dataOut);
    }
    if (null == this.T_MATR_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MATR_CONV);
    }
    if (null == this.T_CIFRE_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CIFRE_CONV);
    }
    if (null == this.T_SEGN_CONV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_SEGN_CONV);
    }
    if (null == this.D_DATA_MIS_EFF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_MIS_EFF);
    }
    if (null == this.T_SEGN_MIS_EFF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_SEGN_MIS_EFF);
    }
    if (null == this.T_SEGN_CONV_EFF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_SEGN_CONV_EFF);
    }
    if (null == this.T_NOTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOTE);
    }
    if (null == this.T_TIPO_LETTURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_LETTURA);
    }
    if (null == this.B_COPIATO_TMG_MISURE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_COPIATO_TMG_MISURE);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_VTG6==null?"":N_ID_VTG6.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_VTG==null?"":N_ID_VTG.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PRATICA==null?"":N_ID_PRATICA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UTENTE==null?"":N_ID_UTENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_PDR==null?"":T_CODICE_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MATR_MIS==null?"":T_MATR_MIS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_ATT_CONTR==null?"":D_DATA_ATT_CONTR, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_VOL_ANNUO_SOST==null?"":N_VOL_ANNUO_SOST.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CLASSE_GRUPPO_MIS==null?"":T_CLASSE_GRUPPO_MIS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CIFRE_MIS==null?"":T_CIFRE_MIS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_SEGN_MIS_SOST==null?"":T_SEGN_MIS_SOST, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PRE_CONV==null?"":T_PRE_CONV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_GRUPPO_MIS_INT==null?"":T_GRUPPO_MIS_INT, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_COEFF_CORR==null?"":N_COEFF_CORR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MATR_CONV==null?"":T_MATR_CONV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CIFRE_CONV==null?"":T_CIFRE_CONV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_SEGN_CONV==null?"":T_SEGN_CONV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_MIS_EFF==null?"":D_DATA_MIS_EFF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_SEGN_MIS_EFF==null?"":T_SEGN_MIS_EFF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_SEGN_CONV_EFF==null?"":T_SEGN_CONV_EFF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOTE==null?"":T_NOTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_LETTURA==null?"":T_TIPO_LETTURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_COPIATO_TMG_MISURE==null?"":B_COPIATO_TMG_MISURE, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_VTG6==null?"":N_ID_VTG6.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_VTG==null?"":N_ID_VTG.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PRATICA==null?"":N_ID_PRATICA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UTENTE==null?"":N_ID_UTENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_PDR==null?"":T_CODICE_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MATR_MIS==null?"":T_MATR_MIS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_ATT_CONTR==null?"":D_DATA_ATT_CONTR, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_VOL_ANNUO_SOST==null?"":N_VOL_ANNUO_SOST.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CLASSE_GRUPPO_MIS==null?"":T_CLASSE_GRUPPO_MIS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CIFRE_MIS==null?"":T_CIFRE_MIS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_SEGN_MIS_SOST==null?"":T_SEGN_MIS_SOST, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PRE_CONV==null?"":T_PRE_CONV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_GRUPPO_MIS_INT==null?"":T_GRUPPO_MIS_INT, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_COEFF_CORR==null?"":N_COEFF_CORR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MATR_CONV==null?"":T_MATR_CONV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CIFRE_CONV==null?"":T_CIFRE_CONV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_SEGN_CONV==null?"":T_SEGN_CONV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_MIS_EFF==null?"":D_DATA_MIS_EFF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_SEGN_MIS_EFF==null?"":T_SEGN_MIS_EFF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_SEGN_CONV_EFF==null?"":T_SEGN_CONV_EFF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOTE==null?"":T_NOTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_LETTURA==null?"":T_TIPO_LETTURA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_COPIATO_TMG_MISURE==null?"":B_COPIATO_TMG_MISURE, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_VTG6 = null; } else {
      this.N_ID_VTG6 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_VTG = null; } else {
      this.N_ID_VTG = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PRATICA = null; } else {
      this.N_ID_PRATICA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UTENTE = null; } else {
      this.N_ID_UTENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_PDR = null; } else {
      this.T_CODICE_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MATR_MIS = null; } else {
      this.T_MATR_MIS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_ATT_CONTR = null; } else {
      this.D_DATA_ATT_CONTR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_VOL_ANNUO_SOST = null; } else {
      this.N_VOL_ANNUO_SOST = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CLASSE_GRUPPO_MIS = null; } else {
      this.T_CLASSE_GRUPPO_MIS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CIFRE_MIS = null; } else {
      this.T_CIFRE_MIS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_SEGN_MIS_SOST = null; } else {
      this.T_SEGN_MIS_SOST = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PRE_CONV = null; } else {
      this.T_PRE_CONV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_GRUPPO_MIS_INT = null; } else {
      this.T_GRUPPO_MIS_INT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_COEFF_CORR = null; } else {
      this.N_COEFF_CORR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MATR_CONV = null; } else {
      this.T_MATR_CONV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CIFRE_CONV = null; } else {
      this.T_CIFRE_CONV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_SEGN_CONV = null; } else {
      this.T_SEGN_CONV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_MIS_EFF = null; } else {
      this.D_DATA_MIS_EFF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_SEGN_MIS_EFF = null; } else {
      this.T_SEGN_MIS_EFF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_SEGN_CONV_EFF = null; } else {
      this.T_SEGN_CONV_EFF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOTE = null; } else {
      this.T_NOTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_LETTURA = null; } else {
      this.T_TIPO_LETTURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_COPIATO_TMG_MISURE = null; } else {
      this.B_COPIATO_TMG_MISURE = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_VTG6 = null; } else {
      this.N_ID_VTG6 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_VTG = null; } else {
      this.N_ID_VTG = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PRATICA = null; } else {
      this.N_ID_PRATICA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UTENTE = null; } else {
      this.N_ID_UTENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_PDR = null; } else {
      this.T_CODICE_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MATR_MIS = null; } else {
      this.T_MATR_MIS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_ATT_CONTR = null; } else {
      this.D_DATA_ATT_CONTR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_VOL_ANNUO_SOST = null; } else {
      this.N_VOL_ANNUO_SOST = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CLASSE_GRUPPO_MIS = null; } else {
      this.T_CLASSE_GRUPPO_MIS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CIFRE_MIS = null; } else {
      this.T_CIFRE_MIS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_SEGN_MIS_SOST = null; } else {
      this.T_SEGN_MIS_SOST = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PRE_CONV = null; } else {
      this.T_PRE_CONV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_GRUPPO_MIS_INT = null; } else {
      this.T_GRUPPO_MIS_INT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_COEFF_CORR = null; } else {
      this.N_COEFF_CORR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MATR_CONV = null; } else {
      this.T_MATR_CONV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CIFRE_CONV = null; } else {
      this.T_CIFRE_CONV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_SEGN_CONV = null; } else {
      this.T_SEGN_CONV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_MIS_EFF = null; } else {
      this.D_DATA_MIS_EFF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_SEGN_MIS_EFF = null; } else {
      this.T_SEGN_MIS_EFF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_SEGN_CONV_EFF = null; } else {
      this.T_SEGN_CONV_EFF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOTE = null; } else {
      this.T_NOTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_LETTURA = null; } else {
      this.T_TIPO_LETTURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_COPIATO_TMG_MISURE = null; } else {
      this.B_COPIATO_TMG_MISURE = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    switch_gas_prt_vtg6 o = (switch_gas_prt_vtg6) super.clone();
    return o;
  }

  public void clone0(switch_gas_prt_vtg6 o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_VTG6", this.N_ID_VTG6);
    __sqoop$field_map.put("N_ID_VTG", this.N_ID_VTG);
    __sqoop$field_map.put("N_ID_PRATICA", this.N_ID_PRATICA);
    __sqoop$field_map.put("N_ID_UTENTE", this.N_ID_UTENTE);
    __sqoop$field_map.put("T_CODICE_PDR", this.T_CODICE_PDR);
    __sqoop$field_map.put("T_MATR_MIS", this.T_MATR_MIS);
    __sqoop$field_map.put("D_DATA_ATT_CONTR", this.D_DATA_ATT_CONTR);
    __sqoop$field_map.put("N_VOL_ANNUO_SOST", this.N_VOL_ANNUO_SOST);
    __sqoop$field_map.put("T_CLASSE_GRUPPO_MIS", this.T_CLASSE_GRUPPO_MIS);
    __sqoop$field_map.put("T_CIFRE_MIS", this.T_CIFRE_MIS);
    __sqoop$field_map.put("T_SEGN_MIS_SOST", this.T_SEGN_MIS_SOST);
    __sqoop$field_map.put("T_PRE_CONV", this.T_PRE_CONV);
    __sqoop$field_map.put("T_GRUPPO_MIS_INT", this.T_GRUPPO_MIS_INT);
    __sqoop$field_map.put("N_COEFF_CORR", this.N_COEFF_CORR);
    __sqoop$field_map.put("T_MATR_CONV", this.T_MATR_CONV);
    __sqoop$field_map.put("T_CIFRE_CONV", this.T_CIFRE_CONV);
    __sqoop$field_map.put("T_SEGN_CONV", this.T_SEGN_CONV);
    __sqoop$field_map.put("D_DATA_MIS_EFF", this.D_DATA_MIS_EFF);
    __sqoop$field_map.put("T_SEGN_MIS_EFF", this.T_SEGN_MIS_EFF);
    __sqoop$field_map.put("T_SEGN_CONV_EFF", this.T_SEGN_CONV_EFF);
    __sqoop$field_map.put("T_NOTE", this.T_NOTE);
    __sqoop$field_map.put("T_TIPO_LETTURA", this.T_TIPO_LETTURA);
    __sqoop$field_map.put("B_COPIATO_TMG_MISURE", this.B_COPIATO_TMG_MISURE);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_VTG6", this.N_ID_VTG6);
    __sqoop$field_map.put("N_ID_VTG", this.N_ID_VTG);
    __sqoop$field_map.put("N_ID_PRATICA", this.N_ID_PRATICA);
    __sqoop$field_map.put("N_ID_UTENTE", this.N_ID_UTENTE);
    __sqoop$field_map.put("T_CODICE_PDR", this.T_CODICE_PDR);
    __sqoop$field_map.put("T_MATR_MIS", this.T_MATR_MIS);
    __sqoop$field_map.put("D_DATA_ATT_CONTR", this.D_DATA_ATT_CONTR);
    __sqoop$field_map.put("N_VOL_ANNUO_SOST", this.N_VOL_ANNUO_SOST);
    __sqoop$field_map.put("T_CLASSE_GRUPPO_MIS", this.T_CLASSE_GRUPPO_MIS);
    __sqoop$field_map.put("T_CIFRE_MIS", this.T_CIFRE_MIS);
    __sqoop$field_map.put("T_SEGN_MIS_SOST", this.T_SEGN_MIS_SOST);
    __sqoop$field_map.put("T_PRE_CONV", this.T_PRE_CONV);
    __sqoop$field_map.put("T_GRUPPO_MIS_INT", this.T_GRUPPO_MIS_INT);
    __sqoop$field_map.put("N_COEFF_CORR", this.N_COEFF_CORR);
    __sqoop$field_map.put("T_MATR_CONV", this.T_MATR_CONV);
    __sqoop$field_map.put("T_CIFRE_CONV", this.T_CIFRE_CONV);
    __sqoop$field_map.put("T_SEGN_CONV", this.T_SEGN_CONV);
    __sqoop$field_map.put("D_DATA_MIS_EFF", this.D_DATA_MIS_EFF);
    __sqoop$field_map.put("T_SEGN_MIS_EFF", this.T_SEGN_MIS_EFF);
    __sqoop$field_map.put("T_SEGN_CONV_EFF", this.T_SEGN_CONV_EFF);
    __sqoop$field_map.put("T_NOTE", this.T_NOTE);
    __sqoop$field_map.put("T_TIPO_LETTURA", this.T_TIPO_LETTURA);
    __sqoop$field_map.put("B_COPIATO_TMG_MISURE", this.B_COPIATO_TMG_MISURE);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
