// ORM class for table 'rcu.rcu_pod_tecn'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 10:45:25 CEST 2019
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

public class rcu_rcu_pod_tecn extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_POD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_POD = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_POTENZA_DISPONIBILE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_POTENZA_DISPONIBILE = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_POTENZA_IMPEGNATA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_POTENZA_IMPEGNATA = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_TENSIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_TENSIONE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_TIPO_MISURATORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO_MISURATORE = (String)value;
      }
    });
    setters.put("N_K_TRASFORMAZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_K_TRASFORMAZIONE = (java.math.BigDecimal)value;
      }
    });
    setters.put("D_INST_MISURATORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_INST_MISURATORE = (String)value;
      }
    });
    setters.put("D_RIMOZ_MISURATORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_RIMOZ_MISURATORE = (String)value;
      }
    });
    setters.put("T_NOTA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_NOTA = (String)value;
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
    setters.put("N_NUM_CIFRE_EA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_NUM_CIFRE_EA = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_NUM_CIFRE_ER", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_NUM_CIFRE_ER = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_K_TRASFOR_ATT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_K_TRASFOR_ATT = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_K_TRASFOR_REA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_K_TRASFOR_REA = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_K_TRASFOR_POT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_K_TRASFOR_POT = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_MAT_MISURATORE_ATT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MAT_MISURATORE_ATT = (String)value;
      }
    });
    setters.put("T_MAT_MISURATORE_REA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MAT_MISURATORE_REA = (String)value;
      }
    });
    setters.put("T_MAT_MISURATORE_POT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MAT_MISURATORE_POT = (String)value;
      }
    });
    setters.put("D_INST_MISURATOR_ATT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_INST_MISURATOR_ATT = (String)value;
      }
    });
    setters.put("D_INST_MISURATOR_REA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_INST_MISURATOR_REA = (String)value;
      }
    });
    setters.put("D_INST_MISURATOR_POT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_INST_MISURATOR_POT = (String)value;
      }
    });
    setters.put("N_NUM_CIFRE_ATT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_NUM_CIFRE_ATT = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_NUM_CIFRE_REA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_NUM_CIFRE_REA = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_NUM_CIFRE_POT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_NUM_CIFRE_POT = (java.math.BigDecimal)value;
      }
    });
    setters.put("B_PRESENZA_MIS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_PRESENZA_MIS = (String)value;
      }
    });
    setters.put("B_GEST_FORFAIT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_GEST_FORFAIT = (String)value;
      }
    });
    setters.put("T_TIPO_POD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO_POD = (String)value;
      }
    });
    setters.put("D_FINE_TIPO_POD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_FINE_TIPO_POD = (String)value;
      }
    });
    setters.put("D_OPER_MISURATOR_ATT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_OPER_MISURATOR_ATT = (String)value;
      }
    });
    setters.put("D_OPER_MISURATOR_REA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_OPER_MISURATOR_REA = (String)value;
      }
    });
    setters.put("D_OPER_MISURATOR_POT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_OPER_MISURATOR_POT = (String)value;
      }
    });
    setters.put("T_MOTIVAZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MOTIVAZIONE = (String)value;
      }
    });
  }
  public rcu_rcu_pod_tecn() {
    init0();
  }
  private java.math.BigDecimal N_ID_POD;
  public java.math.BigDecimal get_N_ID_POD() {
    return N_ID_POD;
  }
  public void set_N_ID_POD(java.math.BigDecimal N_ID_POD) {
    this.N_ID_POD = N_ID_POD;
  }
  public rcu_rcu_pod_tecn with_N_ID_POD(java.math.BigDecimal N_ID_POD) {
    this.N_ID_POD = N_ID_POD;
    return this;
  }
  private java.math.BigDecimal N_POTENZA_DISPONIBILE;
  public java.math.BigDecimal get_N_POTENZA_DISPONIBILE() {
    return N_POTENZA_DISPONIBILE;
  }
  public void set_N_POTENZA_DISPONIBILE(java.math.BigDecimal N_POTENZA_DISPONIBILE) {
    this.N_POTENZA_DISPONIBILE = N_POTENZA_DISPONIBILE;
  }
  public rcu_rcu_pod_tecn with_N_POTENZA_DISPONIBILE(java.math.BigDecimal N_POTENZA_DISPONIBILE) {
    this.N_POTENZA_DISPONIBILE = N_POTENZA_DISPONIBILE;
    return this;
  }
  private java.math.BigDecimal N_POTENZA_IMPEGNATA;
  public java.math.BigDecimal get_N_POTENZA_IMPEGNATA() {
    return N_POTENZA_IMPEGNATA;
  }
  public void set_N_POTENZA_IMPEGNATA(java.math.BigDecimal N_POTENZA_IMPEGNATA) {
    this.N_POTENZA_IMPEGNATA = N_POTENZA_IMPEGNATA;
  }
  public rcu_rcu_pod_tecn with_N_POTENZA_IMPEGNATA(java.math.BigDecimal N_POTENZA_IMPEGNATA) {
    this.N_POTENZA_IMPEGNATA = N_POTENZA_IMPEGNATA;
    return this;
  }
  private java.math.BigDecimal N_TENSIONE;
  public java.math.BigDecimal get_N_TENSIONE() {
    return N_TENSIONE;
  }
  public void set_N_TENSIONE(java.math.BigDecimal N_TENSIONE) {
    this.N_TENSIONE = N_TENSIONE;
  }
  public rcu_rcu_pod_tecn with_N_TENSIONE(java.math.BigDecimal N_TENSIONE) {
    this.N_TENSIONE = N_TENSIONE;
    return this;
  }
  private String T_TIPO_MISURATORE;
  public String get_T_TIPO_MISURATORE() {
    return T_TIPO_MISURATORE;
  }
  public void set_T_TIPO_MISURATORE(String T_TIPO_MISURATORE) {
    this.T_TIPO_MISURATORE = T_TIPO_MISURATORE;
  }
  public rcu_rcu_pod_tecn with_T_TIPO_MISURATORE(String T_TIPO_MISURATORE) {
    this.T_TIPO_MISURATORE = T_TIPO_MISURATORE;
    return this;
  }
  private java.math.BigDecimal N_K_TRASFORMAZIONE;
  public java.math.BigDecimal get_N_K_TRASFORMAZIONE() {
    return N_K_TRASFORMAZIONE;
  }
  public void set_N_K_TRASFORMAZIONE(java.math.BigDecimal N_K_TRASFORMAZIONE) {
    this.N_K_TRASFORMAZIONE = N_K_TRASFORMAZIONE;
  }
  public rcu_rcu_pod_tecn with_N_K_TRASFORMAZIONE(java.math.BigDecimal N_K_TRASFORMAZIONE) {
    this.N_K_TRASFORMAZIONE = N_K_TRASFORMAZIONE;
    return this;
  }
  private String D_INST_MISURATORE;
  public String get_D_INST_MISURATORE() {
    return D_INST_MISURATORE;
  }
  public void set_D_INST_MISURATORE(String D_INST_MISURATORE) {
    this.D_INST_MISURATORE = D_INST_MISURATORE;
  }
  public rcu_rcu_pod_tecn with_D_INST_MISURATORE(String D_INST_MISURATORE) {
    this.D_INST_MISURATORE = D_INST_MISURATORE;
    return this;
  }
  private String D_RIMOZ_MISURATORE;
  public String get_D_RIMOZ_MISURATORE() {
    return D_RIMOZ_MISURATORE;
  }
  public void set_D_RIMOZ_MISURATORE(String D_RIMOZ_MISURATORE) {
    this.D_RIMOZ_MISURATORE = D_RIMOZ_MISURATORE;
  }
  public rcu_rcu_pod_tecn with_D_RIMOZ_MISURATORE(String D_RIMOZ_MISURATORE) {
    this.D_RIMOZ_MISURATORE = D_RIMOZ_MISURATORE;
    return this;
  }
  private String T_NOTA;
  public String get_T_NOTA() {
    return T_NOTA;
  }
  public void set_T_NOTA(String T_NOTA) {
    this.T_NOTA = T_NOTA;
  }
  public rcu_rcu_pod_tecn with_T_NOTA(String T_NOTA) {
    this.T_NOTA = T_NOTA;
    return this;
  }
  private String D_AGGIORNAMENTO;
  public String get_D_AGGIORNAMENTO() {
    return D_AGGIORNAMENTO;
  }
  public void set_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
    this.D_AGGIORNAMENTO = D_AGGIORNAMENTO;
  }
  public rcu_rcu_pod_tecn with_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
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
  public rcu_rcu_pod_tecn with_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
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
  public rcu_rcu_pod_tecn with_N_ID_S_PREC(java.math.BigDecimal N_ID_S_PREC) {
    this.N_ID_S_PREC = N_ID_S_PREC;
    return this;
  }
  private java.math.BigDecimal N_NUM_CIFRE_EA;
  public java.math.BigDecimal get_N_NUM_CIFRE_EA() {
    return N_NUM_CIFRE_EA;
  }
  public void set_N_NUM_CIFRE_EA(java.math.BigDecimal N_NUM_CIFRE_EA) {
    this.N_NUM_CIFRE_EA = N_NUM_CIFRE_EA;
  }
  public rcu_rcu_pod_tecn with_N_NUM_CIFRE_EA(java.math.BigDecimal N_NUM_CIFRE_EA) {
    this.N_NUM_CIFRE_EA = N_NUM_CIFRE_EA;
    return this;
  }
  private java.math.BigDecimal N_NUM_CIFRE_ER;
  public java.math.BigDecimal get_N_NUM_CIFRE_ER() {
    return N_NUM_CIFRE_ER;
  }
  public void set_N_NUM_CIFRE_ER(java.math.BigDecimal N_NUM_CIFRE_ER) {
    this.N_NUM_CIFRE_ER = N_NUM_CIFRE_ER;
  }
  public rcu_rcu_pod_tecn with_N_NUM_CIFRE_ER(java.math.BigDecimal N_NUM_CIFRE_ER) {
    this.N_NUM_CIFRE_ER = N_NUM_CIFRE_ER;
    return this;
  }
  private java.math.BigDecimal N_K_TRASFOR_ATT;
  public java.math.BigDecimal get_N_K_TRASFOR_ATT() {
    return N_K_TRASFOR_ATT;
  }
  public void set_N_K_TRASFOR_ATT(java.math.BigDecimal N_K_TRASFOR_ATT) {
    this.N_K_TRASFOR_ATT = N_K_TRASFOR_ATT;
  }
  public rcu_rcu_pod_tecn with_N_K_TRASFOR_ATT(java.math.BigDecimal N_K_TRASFOR_ATT) {
    this.N_K_TRASFOR_ATT = N_K_TRASFOR_ATT;
    return this;
  }
  private java.math.BigDecimal N_K_TRASFOR_REA;
  public java.math.BigDecimal get_N_K_TRASFOR_REA() {
    return N_K_TRASFOR_REA;
  }
  public void set_N_K_TRASFOR_REA(java.math.BigDecimal N_K_TRASFOR_REA) {
    this.N_K_TRASFOR_REA = N_K_TRASFOR_REA;
  }
  public rcu_rcu_pod_tecn with_N_K_TRASFOR_REA(java.math.BigDecimal N_K_TRASFOR_REA) {
    this.N_K_TRASFOR_REA = N_K_TRASFOR_REA;
    return this;
  }
  private java.math.BigDecimal N_K_TRASFOR_POT;
  public java.math.BigDecimal get_N_K_TRASFOR_POT() {
    return N_K_TRASFOR_POT;
  }
  public void set_N_K_TRASFOR_POT(java.math.BigDecimal N_K_TRASFOR_POT) {
    this.N_K_TRASFOR_POT = N_K_TRASFOR_POT;
  }
  public rcu_rcu_pod_tecn with_N_K_TRASFOR_POT(java.math.BigDecimal N_K_TRASFOR_POT) {
    this.N_K_TRASFOR_POT = N_K_TRASFOR_POT;
    return this;
  }
  private String T_MAT_MISURATORE_ATT;
  public String get_T_MAT_MISURATORE_ATT() {
    return T_MAT_MISURATORE_ATT;
  }
  public void set_T_MAT_MISURATORE_ATT(String T_MAT_MISURATORE_ATT) {
    this.T_MAT_MISURATORE_ATT = T_MAT_MISURATORE_ATT;
  }
  public rcu_rcu_pod_tecn with_T_MAT_MISURATORE_ATT(String T_MAT_MISURATORE_ATT) {
    this.T_MAT_MISURATORE_ATT = T_MAT_MISURATORE_ATT;
    return this;
  }
  private String T_MAT_MISURATORE_REA;
  public String get_T_MAT_MISURATORE_REA() {
    return T_MAT_MISURATORE_REA;
  }
  public void set_T_MAT_MISURATORE_REA(String T_MAT_MISURATORE_REA) {
    this.T_MAT_MISURATORE_REA = T_MAT_MISURATORE_REA;
  }
  public rcu_rcu_pod_tecn with_T_MAT_MISURATORE_REA(String T_MAT_MISURATORE_REA) {
    this.T_MAT_MISURATORE_REA = T_MAT_MISURATORE_REA;
    return this;
  }
  private String T_MAT_MISURATORE_POT;
  public String get_T_MAT_MISURATORE_POT() {
    return T_MAT_MISURATORE_POT;
  }
  public void set_T_MAT_MISURATORE_POT(String T_MAT_MISURATORE_POT) {
    this.T_MAT_MISURATORE_POT = T_MAT_MISURATORE_POT;
  }
  public rcu_rcu_pod_tecn with_T_MAT_MISURATORE_POT(String T_MAT_MISURATORE_POT) {
    this.T_MAT_MISURATORE_POT = T_MAT_MISURATORE_POT;
    return this;
  }
  private String D_INST_MISURATOR_ATT;
  public String get_D_INST_MISURATOR_ATT() {
    return D_INST_MISURATOR_ATT;
  }
  public void set_D_INST_MISURATOR_ATT(String D_INST_MISURATOR_ATT) {
    this.D_INST_MISURATOR_ATT = D_INST_MISURATOR_ATT;
  }
  public rcu_rcu_pod_tecn with_D_INST_MISURATOR_ATT(String D_INST_MISURATOR_ATT) {
    this.D_INST_MISURATOR_ATT = D_INST_MISURATOR_ATT;
    return this;
  }
  private String D_INST_MISURATOR_REA;
  public String get_D_INST_MISURATOR_REA() {
    return D_INST_MISURATOR_REA;
  }
  public void set_D_INST_MISURATOR_REA(String D_INST_MISURATOR_REA) {
    this.D_INST_MISURATOR_REA = D_INST_MISURATOR_REA;
  }
  public rcu_rcu_pod_tecn with_D_INST_MISURATOR_REA(String D_INST_MISURATOR_REA) {
    this.D_INST_MISURATOR_REA = D_INST_MISURATOR_REA;
    return this;
  }
  private String D_INST_MISURATOR_POT;
  public String get_D_INST_MISURATOR_POT() {
    return D_INST_MISURATOR_POT;
  }
  public void set_D_INST_MISURATOR_POT(String D_INST_MISURATOR_POT) {
    this.D_INST_MISURATOR_POT = D_INST_MISURATOR_POT;
  }
  public rcu_rcu_pod_tecn with_D_INST_MISURATOR_POT(String D_INST_MISURATOR_POT) {
    this.D_INST_MISURATOR_POT = D_INST_MISURATOR_POT;
    return this;
  }
  private java.math.BigDecimal N_NUM_CIFRE_ATT;
  public java.math.BigDecimal get_N_NUM_CIFRE_ATT() {
    return N_NUM_CIFRE_ATT;
  }
  public void set_N_NUM_CIFRE_ATT(java.math.BigDecimal N_NUM_CIFRE_ATT) {
    this.N_NUM_CIFRE_ATT = N_NUM_CIFRE_ATT;
  }
  public rcu_rcu_pod_tecn with_N_NUM_CIFRE_ATT(java.math.BigDecimal N_NUM_CIFRE_ATT) {
    this.N_NUM_CIFRE_ATT = N_NUM_CIFRE_ATT;
    return this;
  }
  private java.math.BigDecimal N_NUM_CIFRE_REA;
  public java.math.BigDecimal get_N_NUM_CIFRE_REA() {
    return N_NUM_CIFRE_REA;
  }
  public void set_N_NUM_CIFRE_REA(java.math.BigDecimal N_NUM_CIFRE_REA) {
    this.N_NUM_CIFRE_REA = N_NUM_CIFRE_REA;
  }
  public rcu_rcu_pod_tecn with_N_NUM_CIFRE_REA(java.math.BigDecimal N_NUM_CIFRE_REA) {
    this.N_NUM_CIFRE_REA = N_NUM_CIFRE_REA;
    return this;
  }
  private java.math.BigDecimal N_NUM_CIFRE_POT;
  public java.math.BigDecimal get_N_NUM_CIFRE_POT() {
    return N_NUM_CIFRE_POT;
  }
  public void set_N_NUM_CIFRE_POT(java.math.BigDecimal N_NUM_CIFRE_POT) {
    this.N_NUM_CIFRE_POT = N_NUM_CIFRE_POT;
  }
  public rcu_rcu_pod_tecn with_N_NUM_CIFRE_POT(java.math.BigDecimal N_NUM_CIFRE_POT) {
    this.N_NUM_CIFRE_POT = N_NUM_CIFRE_POT;
    return this;
  }
  private String B_PRESENZA_MIS;
  public String get_B_PRESENZA_MIS() {
    return B_PRESENZA_MIS;
  }
  public void set_B_PRESENZA_MIS(String B_PRESENZA_MIS) {
    this.B_PRESENZA_MIS = B_PRESENZA_MIS;
  }
  public rcu_rcu_pod_tecn with_B_PRESENZA_MIS(String B_PRESENZA_MIS) {
    this.B_PRESENZA_MIS = B_PRESENZA_MIS;
    return this;
  }
  private String B_GEST_FORFAIT;
  public String get_B_GEST_FORFAIT() {
    return B_GEST_FORFAIT;
  }
  public void set_B_GEST_FORFAIT(String B_GEST_FORFAIT) {
    this.B_GEST_FORFAIT = B_GEST_FORFAIT;
  }
  public rcu_rcu_pod_tecn with_B_GEST_FORFAIT(String B_GEST_FORFAIT) {
    this.B_GEST_FORFAIT = B_GEST_FORFAIT;
    return this;
  }
  private String T_TIPO_POD;
  public String get_T_TIPO_POD() {
    return T_TIPO_POD;
  }
  public void set_T_TIPO_POD(String T_TIPO_POD) {
    this.T_TIPO_POD = T_TIPO_POD;
  }
  public rcu_rcu_pod_tecn with_T_TIPO_POD(String T_TIPO_POD) {
    this.T_TIPO_POD = T_TIPO_POD;
    return this;
  }
  private String D_FINE_TIPO_POD;
  public String get_D_FINE_TIPO_POD() {
    return D_FINE_TIPO_POD;
  }
  public void set_D_FINE_TIPO_POD(String D_FINE_TIPO_POD) {
    this.D_FINE_TIPO_POD = D_FINE_TIPO_POD;
  }
  public rcu_rcu_pod_tecn with_D_FINE_TIPO_POD(String D_FINE_TIPO_POD) {
    this.D_FINE_TIPO_POD = D_FINE_TIPO_POD;
    return this;
  }
  private String D_OPER_MISURATOR_ATT;
  public String get_D_OPER_MISURATOR_ATT() {
    return D_OPER_MISURATOR_ATT;
  }
  public void set_D_OPER_MISURATOR_ATT(String D_OPER_MISURATOR_ATT) {
    this.D_OPER_MISURATOR_ATT = D_OPER_MISURATOR_ATT;
  }
  public rcu_rcu_pod_tecn with_D_OPER_MISURATOR_ATT(String D_OPER_MISURATOR_ATT) {
    this.D_OPER_MISURATOR_ATT = D_OPER_MISURATOR_ATT;
    return this;
  }
  private String D_OPER_MISURATOR_REA;
  public String get_D_OPER_MISURATOR_REA() {
    return D_OPER_MISURATOR_REA;
  }
  public void set_D_OPER_MISURATOR_REA(String D_OPER_MISURATOR_REA) {
    this.D_OPER_MISURATOR_REA = D_OPER_MISURATOR_REA;
  }
  public rcu_rcu_pod_tecn with_D_OPER_MISURATOR_REA(String D_OPER_MISURATOR_REA) {
    this.D_OPER_MISURATOR_REA = D_OPER_MISURATOR_REA;
    return this;
  }
  private String D_OPER_MISURATOR_POT;
  public String get_D_OPER_MISURATOR_POT() {
    return D_OPER_MISURATOR_POT;
  }
  public void set_D_OPER_MISURATOR_POT(String D_OPER_MISURATOR_POT) {
    this.D_OPER_MISURATOR_POT = D_OPER_MISURATOR_POT;
  }
  public rcu_rcu_pod_tecn with_D_OPER_MISURATOR_POT(String D_OPER_MISURATOR_POT) {
    this.D_OPER_MISURATOR_POT = D_OPER_MISURATOR_POT;
    return this;
  }
  private String T_MOTIVAZIONE;
  public String get_T_MOTIVAZIONE() {
    return T_MOTIVAZIONE;
  }
  public void set_T_MOTIVAZIONE(String T_MOTIVAZIONE) {
    this.T_MOTIVAZIONE = T_MOTIVAZIONE;
  }
  public rcu_rcu_pod_tecn with_T_MOTIVAZIONE(String T_MOTIVAZIONE) {
    this.T_MOTIVAZIONE = T_MOTIVAZIONE;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcu_rcu_pod_tecn)) {
      return false;
    }
    rcu_rcu_pod_tecn that = (rcu_rcu_pod_tecn) o;
    boolean equal = true;
    equal = equal && (this.N_ID_POD == null ? that.N_ID_POD == null : this.N_ID_POD.equals(that.N_ID_POD));
    equal = equal && (this.N_POTENZA_DISPONIBILE == null ? that.N_POTENZA_DISPONIBILE == null : this.N_POTENZA_DISPONIBILE.equals(that.N_POTENZA_DISPONIBILE));
    equal = equal && (this.N_POTENZA_IMPEGNATA == null ? that.N_POTENZA_IMPEGNATA == null : this.N_POTENZA_IMPEGNATA.equals(that.N_POTENZA_IMPEGNATA));
    equal = equal && (this.N_TENSIONE == null ? that.N_TENSIONE == null : this.N_TENSIONE.equals(that.N_TENSIONE));
    equal = equal && (this.T_TIPO_MISURATORE == null ? that.T_TIPO_MISURATORE == null : this.T_TIPO_MISURATORE.equals(that.T_TIPO_MISURATORE));
    equal = equal && (this.N_K_TRASFORMAZIONE == null ? that.N_K_TRASFORMAZIONE == null : this.N_K_TRASFORMAZIONE.equals(that.N_K_TRASFORMAZIONE));
    equal = equal && (this.D_INST_MISURATORE == null ? that.D_INST_MISURATORE == null : this.D_INST_MISURATORE.equals(that.D_INST_MISURATORE));
    equal = equal && (this.D_RIMOZ_MISURATORE == null ? that.D_RIMOZ_MISURATORE == null : this.D_RIMOZ_MISURATORE.equals(that.D_RIMOZ_MISURATORE));
    equal = equal && (this.T_NOTA == null ? that.T_NOTA == null : this.T_NOTA.equals(that.T_NOTA));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.N_NUM_CIFRE_EA == null ? that.N_NUM_CIFRE_EA == null : this.N_NUM_CIFRE_EA.equals(that.N_NUM_CIFRE_EA));
    equal = equal && (this.N_NUM_CIFRE_ER == null ? that.N_NUM_CIFRE_ER == null : this.N_NUM_CIFRE_ER.equals(that.N_NUM_CIFRE_ER));
    equal = equal && (this.N_K_TRASFOR_ATT == null ? that.N_K_TRASFOR_ATT == null : this.N_K_TRASFOR_ATT.equals(that.N_K_TRASFOR_ATT));
    equal = equal && (this.N_K_TRASFOR_REA == null ? that.N_K_TRASFOR_REA == null : this.N_K_TRASFOR_REA.equals(that.N_K_TRASFOR_REA));
    equal = equal && (this.N_K_TRASFOR_POT == null ? that.N_K_TRASFOR_POT == null : this.N_K_TRASFOR_POT.equals(that.N_K_TRASFOR_POT));
    equal = equal && (this.T_MAT_MISURATORE_ATT == null ? that.T_MAT_MISURATORE_ATT == null : this.T_MAT_MISURATORE_ATT.equals(that.T_MAT_MISURATORE_ATT));
    equal = equal && (this.T_MAT_MISURATORE_REA == null ? that.T_MAT_MISURATORE_REA == null : this.T_MAT_MISURATORE_REA.equals(that.T_MAT_MISURATORE_REA));
    equal = equal && (this.T_MAT_MISURATORE_POT == null ? that.T_MAT_MISURATORE_POT == null : this.T_MAT_MISURATORE_POT.equals(that.T_MAT_MISURATORE_POT));
    equal = equal && (this.D_INST_MISURATOR_ATT == null ? that.D_INST_MISURATOR_ATT == null : this.D_INST_MISURATOR_ATT.equals(that.D_INST_MISURATOR_ATT));
    equal = equal && (this.D_INST_MISURATOR_REA == null ? that.D_INST_MISURATOR_REA == null : this.D_INST_MISURATOR_REA.equals(that.D_INST_MISURATOR_REA));
    equal = equal && (this.D_INST_MISURATOR_POT == null ? that.D_INST_MISURATOR_POT == null : this.D_INST_MISURATOR_POT.equals(that.D_INST_MISURATOR_POT));
    equal = equal && (this.N_NUM_CIFRE_ATT == null ? that.N_NUM_CIFRE_ATT == null : this.N_NUM_CIFRE_ATT.equals(that.N_NUM_CIFRE_ATT));
    equal = equal && (this.N_NUM_CIFRE_REA == null ? that.N_NUM_CIFRE_REA == null : this.N_NUM_CIFRE_REA.equals(that.N_NUM_CIFRE_REA));
    equal = equal && (this.N_NUM_CIFRE_POT == null ? that.N_NUM_CIFRE_POT == null : this.N_NUM_CIFRE_POT.equals(that.N_NUM_CIFRE_POT));
    equal = equal && (this.B_PRESENZA_MIS == null ? that.B_PRESENZA_MIS == null : this.B_PRESENZA_MIS.equals(that.B_PRESENZA_MIS));
    equal = equal && (this.B_GEST_FORFAIT == null ? that.B_GEST_FORFAIT == null : this.B_GEST_FORFAIT.equals(that.B_GEST_FORFAIT));
    equal = equal && (this.T_TIPO_POD == null ? that.T_TIPO_POD == null : this.T_TIPO_POD.equals(that.T_TIPO_POD));
    equal = equal && (this.D_FINE_TIPO_POD == null ? that.D_FINE_TIPO_POD == null : this.D_FINE_TIPO_POD.equals(that.D_FINE_TIPO_POD));
    equal = equal && (this.D_OPER_MISURATOR_ATT == null ? that.D_OPER_MISURATOR_ATT == null : this.D_OPER_MISURATOR_ATT.equals(that.D_OPER_MISURATOR_ATT));
    equal = equal && (this.D_OPER_MISURATOR_REA == null ? that.D_OPER_MISURATOR_REA == null : this.D_OPER_MISURATOR_REA.equals(that.D_OPER_MISURATOR_REA));
    equal = equal && (this.D_OPER_MISURATOR_POT == null ? that.D_OPER_MISURATOR_POT == null : this.D_OPER_MISURATOR_POT.equals(that.D_OPER_MISURATOR_POT));
    equal = equal && (this.T_MOTIVAZIONE == null ? that.T_MOTIVAZIONE == null : this.T_MOTIVAZIONE.equals(that.T_MOTIVAZIONE));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcu_rcu_pod_tecn)) {
      return false;
    }
    rcu_rcu_pod_tecn that = (rcu_rcu_pod_tecn) o;
    boolean equal = true;
    equal = equal && (this.N_ID_POD == null ? that.N_ID_POD == null : this.N_ID_POD.equals(that.N_ID_POD));
    equal = equal && (this.N_POTENZA_DISPONIBILE == null ? that.N_POTENZA_DISPONIBILE == null : this.N_POTENZA_DISPONIBILE.equals(that.N_POTENZA_DISPONIBILE));
    equal = equal && (this.N_POTENZA_IMPEGNATA == null ? that.N_POTENZA_IMPEGNATA == null : this.N_POTENZA_IMPEGNATA.equals(that.N_POTENZA_IMPEGNATA));
    equal = equal && (this.N_TENSIONE == null ? that.N_TENSIONE == null : this.N_TENSIONE.equals(that.N_TENSIONE));
    equal = equal && (this.T_TIPO_MISURATORE == null ? that.T_TIPO_MISURATORE == null : this.T_TIPO_MISURATORE.equals(that.T_TIPO_MISURATORE));
    equal = equal && (this.N_K_TRASFORMAZIONE == null ? that.N_K_TRASFORMAZIONE == null : this.N_K_TRASFORMAZIONE.equals(that.N_K_TRASFORMAZIONE));
    equal = equal && (this.D_INST_MISURATORE == null ? that.D_INST_MISURATORE == null : this.D_INST_MISURATORE.equals(that.D_INST_MISURATORE));
    equal = equal && (this.D_RIMOZ_MISURATORE == null ? that.D_RIMOZ_MISURATORE == null : this.D_RIMOZ_MISURATORE.equals(that.D_RIMOZ_MISURATORE));
    equal = equal && (this.T_NOTA == null ? that.T_NOTA == null : this.T_NOTA.equals(that.T_NOTA));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.N_NUM_CIFRE_EA == null ? that.N_NUM_CIFRE_EA == null : this.N_NUM_CIFRE_EA.equals(that.N_NUM_CIFRE_EA));
    equal = equal && (this.N_NUM_CIFRE_ER == null ? that.N_NUM_CIFRE_ER == null : this.N_NUM_CIFRE_ER.equals(that.N_NUM_CIFRE_ER));
    equal = equal && (this.N_K_TRASFOR_ATT == null ? that.N_K_TRASFOR_ATT == null : this.N_K_TRASFOR_ATT.equals(that.N_K_TRASFOR_ATT));
    equal = equal && (this.N_K_TRASFOR_REA == null ? that.N_K_TRASFOR_REA == null : this.N_K_TRASFOR_REA.equals(that.N_K_TRASFOR_REA));
    equal = equal && (this.N_K_TRASFOR_POT == null ? that.N_K_TRASFOR_POT == null : this.N_K_TRASFOR_POT.equals(that.N_K_TRASFOR_POT));
    equal = equal && (this.T_MAT_MISURATORE_ATT == null ? that.T_MAT_MISURATORE_ATT == null : this.T_MAT_MISURATORE_ATT.equals(that.T_MAT_MISURATORE_ATT));
    equal = equal && (this.T_MAT_MISURATORE_REA == null ? that.T_MAT_MISURATORE_REA == null : this.T_MAT_MISURATORE_REA.equals(that.T_MAT_MISURATORE_REA));
    equal = equal && (this.T_MAT_MISURATORE_POT == null ? that.T_MAT_MISURATORE_POT == null : this.T_MAT_MISURATORE_POT.equals(that.T_MAT_MISURATORE_POT));
    equal = equal && (this.D_INST_MISURATOR_ATT == null ? that.D_INST_MISURATOR_ATT == null : this.D_INST_MISURATOR_ATT.equals(that.D_INST_MISURATOR_ATT));
    equal = equal && (this.D_INST_MISURATOR_REA == null ? that.D_INST_MISURATOR_REA == null : this.D_INST_MISURATOR_REA.equals(that.D_INST_MISURATOR_REA));
    equal = equal && (this.D_INST_MISURATOR_POT == null ? that.D_INST_MISURATOR_POT == null : this.D_INST_MISURATOR_POT.equals(that.D_INST_MISURATOR_POT));
    equal = equal && (this.N_NUM_CIFRE_ATT == null ? that.N_NUM_CIFRE_ATT == null : this.N_NUM_CIFRE_ATT.equals(that.N_NUM_CIFRE_ATT));
    equal = equal && (this.N_NUM_CIFRE_REA == null ? that.N_NUM_CIFRE_REA == null : this.N_NUM_CIFRE_REA.equals(that.N_NUM_CIFRE_REA));
    equal = equal && (this.N_NUM_CIFRE_POT == null ? that.N_NUM_CIFRE_POT == null : this.N_NUM_CIFRE_POT.equals(that.N_NUM_CIFRE_POT));
    equal = equal && (this.B_PRESENZA_MIS == null ? that.B_PRESENZA_MIS == null : this.B_PRESENZA_MIS.equals(that.B_PRESENZA_MIS));
    equal = equal && (this.B_GEST_FORFAIT == null ? that.B_GEST_FORFAIT == null : this.B_GEST_FORFAIT.equals(that.B_GEST_FORFAIT));
    equal = equal && (this.T_TIPO_POD == null ? that.T_TIPO_POD == null : this.T_TIPO_POD.equals(that.T_TIPO_POD));
    equal = equal && (this.D_FINE_TIPO_POD == null ? that.D_FINE_TIPO_POD == null : this.D_FINE_TIPO_POD.equals(that.D_FINE_TIPO_POD));
    equal = equal && (this.D_OPER_MISURATOR_ATT == null ? that.D_OPER_MISURATOR_ATT == null : this.D_OPER_MISURATOR_ATT.equals(that.D_OPER_MISURATOR_ATT));
    equal = equal && (this.D_OPER_MISURATOR_REA == null ? that.D_OPER_MISURATOR_REA == null : this.D_OPER_MISURATOR_REA.equals(that.D_OPER_MISURATOR_REA));
    equal = equal && (this.D_OPER_MISURATOR_POT == null ? that.D_OPER_MISURATOR_POT == null : this.D_OPER_MISURATOR_POT.equals(that.D_OPER_MISURATOR_POT));
    equal = equal && (this.T_MOTIVAZIONE == null ? that.T_MOTIVAZIONE == null : this.T_MOTIVAZIONE.equals(that.T_MOTIVAZIONE));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_POD = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_POTENZA_DISPONIBILE = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.N_POTENZA_IMPEGNATA = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.N_TENSIONE = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.T_TIPO_MISURATORE = JdbcWritableBridge.readString(5, __dbResults);
    this.N_K_TRASFORMAZIONE = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.D_INST_MISURATORE = JdbcWritableBridge.readString(7, __dbResults);
    this.D_RIMOZ_MISURATORE = JdbcWritableBridge.readString(8, __dbResults);
    this.T_NOTA = JdbcWritableBridge.readString(9, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(10, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(11, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.N_NUM_CIFRE_EA = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.N_NUM_CIFRE_ER = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.N_K_TRASFOR_ATT = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.N_K_TRASFOR_REA = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.N_K_TRASFOR_POT = JdbcWritableBridge.readBigDecimal(17, __dbResults);
    this.T_MAT_MISURATORE_ATT = JdbcWritableBridge.readString(18, __dbResults);
    this.T_MAT_MISURATORE_REA = JdbcWritableBridge.readString(19, __dbResults);
    this.T_MAT_MISURATORE_POT = JdbcWritableBridge.readString(20, __dbResults);
    this.D_INST_MISURATOR_ATT = JdbcWritableBridge.readString(21, __dbResults);
    this.D_INST_MISURATOR_REA = JdbcWritableBridge.readString(22, __dbResults);
    this.D_INST_MISURATOR_POT = JdbcWritableBridge.readString(23, __dbResults);
    this.N_NUM_CIFRE_ATT = JdbcWritableBridge.readBigDecimal(24, __dbResults);
    this.N_NUM_CIFRE_REA = JdbcWritableBridge.readBigDecimal(25, __dbResults);
    this.N_NUM_CIFRE_POT = JdbcWritableBridge.readBigDecimal(26, __dbResults);
    this.B_PRESENZA_MIS = JdbcWritableBridge.readString(27, __dbResults);
    this.B_GEST_FORFAIT = JdbcWritableBridge.readString(28, __dbResults);
    this.T_TIPO_POD = JdbcWritableBridge.readString(29, __dbResults);
    this.D_FINE_TIPO_POD = JdbcWritableBridge.readString(30, __dbResults);
    this.D_OPER_MISURATOR_ATT = JdbcWritableBridge.readString(31, __dbResults);
    this.D_OPER_MISURATOR_REA = JdbcWritableBridge.readString(32, __dbResults);
    this.D_OPER_MISURATOR_POT = JdbcWritableBridge.readString(33, __dbResults);
    this.T_MOTIVAZIONE = JdbcWritableBridge.readString(34, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_POD = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_POTENZA_DISPONIBILE = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.N_POTENZA_IMPEGNATA = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.N_TENSIONE = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.T_TIPO_MISURATORE = JdbcWritableBridge.readString(5, __dbResults);
    this.N_K_TRASFORMAZIONE = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.D_INST_MISURATORE = JdbcWritableBridge.readString(7, __dbResults);
    this.D_RIMOZ_MISURATORE = JdbcWritableBridge.readString(8, __dbResults);
    this.T_NOTA = JdbcWritableBridge.readString(9, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(10, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(11, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.N_NUM_CIFRE_EA = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.N_NUM_CIFRE_ER = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.N_K_TRASFOR_ATT = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.N_K_TRASFOR_REA = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.N_K_TRASFOR_POT = JdbcWritableBridge.readBigDecimal(17, __dbResults);
    this.T_MAT_MISURATORE_ATT = JdbcWritableBridge.readString(18, __dbResults);
    this.T_MAT_MISURATORE_REA = JdbcWritableBridge.readString(19, __dbResults);
    this.T_MAT_MISURATORE_POT = JdbcWritableBridge.readString(20, __dbResults);
    this.D_INST_MISURATOR_ATT = JdbcWritableBridge.readString(21, __dbResults);
    this.D_INST_MISURATOR_REA = JdbcWritableBridge.readString(22, __dbResults);
    this.D_INST_MISURATOR_POT = JdbcWritableBridge.readString(23, __dbResults);
    this.N_NUM_CIFRE_ATT = JdbcWritableBridge.readBigDecimal(24, __dbResults);
    this.N_NUM_CIFRE_REA = JdbcWritableBridge.readBigDecimal(25, __dbResults);
    this.N_NUM_CIFRE_POT = JdbcWritableBridge.readBigDecimal(26, __dbResults);
    this.B_PRESENZA_MIS = JdbcWritableBridge.readString(27, __dbResults);
    this.B_GEST_FORFAIT = JdbcWritableBridge.readString(28, __dbResults);
    this.T_TIPO_POD = JdbcWritableBridge.readString(29, __dbResults);
    this.D_FINE_TIPO_POD = JdbcWritableBridge.readString(30, __dbResults);
    this.D_OPER_MISURATOR_ATT = JdbcWritableBridge.readString(31, __dbResults);
    this.D_OPER_MISURATOR_REA = JdbcWritableBridge.readString(32, __dbResults);
    this.D_OPER_MISURATOR_POT = JdbcWritableBridge.readString(33, __dbResults);
    this.T_MOTIVAZIONE = JdbcWritableBridge.readString(34, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_POD, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_POTENZA_DISPONIBILE, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_POTENZA_IMPEGNATA, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_TENSIONE, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_MISURATORE, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_K_TRASFORMAZIONE, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_INST_MISURATORE, 7 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_RIMOZ_MISURATORE, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTA, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 10 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 11 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 12 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_CIFRE_EA, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_CIFRE_ER, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_K_TRASFOR_ATT, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_K_TRASFOR_REA, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_K_TRASFOR_POT, 17 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_MAT_MISURATORE_ATT, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MAT_MISURATORE_REA, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MAT_MISURATORE_POT, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_INST_MISURATOR_ATT, 21 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_INST_MISURATOR_REA, 22 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_INST_MISURATOR_POT, 23 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_CIFRE_ATT, 24 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_CIFRE_REA, 25 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_CIFRE_POT, 26 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_PRESENZA_MIS, 27 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_GEST_FORFAIT, 28 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_POD, 29 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_FINE_TIPO_POD, 30 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_OPER_MISURATOR_ATT, 31 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_OPER_MISURATOR_REA, 32 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_OPER_MISURATOR_POT, 33 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_MOTIVAZIONE, 34 + __off, 12, __dbStmt);
    return 34;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_POD, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_POTENZA_DISPONIBILE, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_POTENZA_IMPEGNATA, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_TENSIONE, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_MISURATORE, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_K_TRASFORMAZIONE, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_INST_MISURATORE, 7 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_RIMOZ_MISURATORE, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTA, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 10 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 11 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 12 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_CIFRE_EA, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_CIFRE_ER, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_K_TRASFOR_ATT, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_K_TRASFOR_REA, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_K_TRASFOR_POT, 17 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_MAT_MISURATORE_ATT, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MAT_MISURATORE_REA, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MAT_MISURATORE_POT, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_INST_MISURATOR_ATT, 21 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_INST_MISURATOR_REA, 22 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_INST_MISURATOR_POT, 23 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_CIFRE_ATT, 24 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_CIFRE_REA, 25 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_CIFRE_POT, 26 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_PRESENZA_MIS, 27 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_GEST_FORFAIT, 28 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_POD, 29 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_FINE_TIPO_POD, 30 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_OPER_MISURATOR_ATT, 31 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_OPER_MISURATOR_REA, 32 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_OPER_MISURATOR_POT, 33 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_MOTIVAZIONE, 34 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_POD = null;
    } else {
    this.N_ID_POD = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_POTENZA_DISPONIBILE = null;
    } else {
    this.N_POTENZA_DISPONIBILE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_POTENZA_IMPEGNATA = null;
    } else {
    this.N_POTENZA_IMPEGNATA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_TENSIONE = null;
    } else {
    this.N_TENSIONE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO_MISURATORE = null;
    } else {
    this.T_TIPO_MISURATORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_K_TRASFORMAZIONE = null;
    } else {
    this.N_K_TRASFORMAZIONE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_INST_MISURATORE = null;
    } else {
    this.D_INST_MISURATORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_RIMOZ_MISURATORE = null;
    } else {
    this.D_RIMOZ_MISURATORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_NOTA = null;
    } else {
    this.T_NOTA = Text.readString(__dataIn);
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
        this.N_NUM_CIFRE_EA = null;
    } else {
    this.N_NUM_CIFRE_EA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_NUM_CIFRE_ER = null;
    } else {
    this.N_NUM_CIFRE_ER = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_K_TRASFOR_ATT = null;
    } else {
    this.N_K_TRASFOR_ATT = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_K_TRASFOR_REA = null;
    } else {
    this.N_K_TRASFOR_REA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_K_TRASFOR_POT = null;
    } else {
    this.N_K_TRASFOR_POT = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MAT_MISURATORE_ATT = null;
    } else {
    this.T_MAT_MISURATORE_ATT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MAT_MISURATORE_REA = null;
    } else {
    this.T_MAT_MISURATORE_REA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MAT_MISURATORE_POT = null;
    } else {
    this.T_MAT_MISURATORE_POT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_INST_MISURATOR_ATT = null;
    } else {
    this.D_INST_MISURATOR_ATT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_INST_MISURATOR_REA = null;
    } else {
    this.D_INST_MISURATOR_REA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_INST_MISURATOR_POT = null;
    } else {
    this.D_INST_MISURATOR_POT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_NUM_CIFRE_ATT = null;
    } else {
    this.N_NUM_CIFRE_ATT = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_NUM_CIFRE_REA = null;
    } else {
    this.N_NUM_CIFRE_REA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_NUM_CIFRE_POT = null;
    } else {
    this.N_NUM_CIFRE_POT = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_PRESENZA_MIS = null;
    } else {
    this.B_PRESENZA_MIS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_GEST_FORFAIT = null;
    } else {
    this.B_GEST_FORFAIT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO_POD = null;
    } else {
    this.T_TIPO_POD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_FINE_TIPO_POD = null;
    } else {
    this.D_FINE_TIPO_POD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_OPER_MISURATOR_ATT = null;
    } else {
    this.D_OPER_MISURATOR_ATT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_OPER_MISURATOR_REA = null;
    } else {
    this.D_OPER_MISURATOR_REA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_OPER_MISURATOR_POT = null;
    } else {
    this.D_OPER_MISURATOR_POT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MOTIVAZIONE = null;
    } else {
    this.T_MOTIVAZIONE = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_POD, __dataOut);
    }
    if (null == this.N_POTENZA_DISPONIBILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_POTENZA_DISPONIBILE, __dataOut);
    }
    if (null == this.N_POTENZA_IMPEGNATA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_POTENZA_IMPEGNATA, __dataOut);
    }
    if (null == this.N_TENSIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_TENSIONE, __dataOut);
    }
    if (null == this.T_TIPO_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_MISURATORE);
    }
    if (null == this.N_K_TRASFORMAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_K_TRASFORMAZIONE, __dataOut);
    }
    if (null == this.D_INST_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_INST_MISURATORE);
    }
    if (null == this.D_RIMOZ_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_RIMOZ_MISURATORE);
    }
    if (null == this.T_NOTA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOTA);
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
    if (null == this.N_NUM_CIFRE_EA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_CIFRE_EA, __dataOut);
    }
    if (null == this.N_NUM_CIFRE_ER) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_CIFRE_ER, __dataOut);
    }
    if (null == this.N_K_TRASFOR_ATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_K_TRASFOR_ATT, __dataOut);
    }
    if (null == this.N_K_TRASFOR_REA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_K_TRASFOR_REA, __dataOut);
    }
    if (null == this.N_K_TRASFOR_POT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_K_TRASFOR_POT, __dataOut);
    }
    if (null == this.T_MAT_MISURATORE_ATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MAT_MISURATORE_ATT);
    }
    if (null == this.T_MAT_MISURATORE_REA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MAT_MISURATORE_REA);
    }
    if (null == this.T_MAT_MISURATORE_POT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MAT_MISURATORE_POT);
    }
    if (null == this.D_INST_MISURATOR_ATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_INST_MISURATOR_ATT);
    }
    if (null == this.D_INST_MISURATOR_REA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_INST_MISURATOR_REA);
    }
    if (null == this.D_INST_MISURATOR_POT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_INST_MISURATOR_POT);
    }
    if (null == this.N_NUM_CIFRE_ATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_CIFRE_ATT, __dataOut);
    }
    if (null == this.N_NUM_CIFRE_REA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_CIFRE_REA, __dataOut);
    }
    if (null == this.N_NUM_CIFRE_POT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_CIFRE_POT, __dataOut);
    }
    if (null == this.B_PRESENZA_MIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_PRESENZA_MIS);
    }
    if (null == this.B_GEST_FORFAIT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_GEST_FORFAIT);
    }
    if (null == this.T_TIPO_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_POD);
    }
    if (null == this.D_FINE_TIPO_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_FINE_TIPO_POD);
    }
    if (null == this.D_OPER_MISURATOR_ATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_OPER_MISURATOR_ATT);
    }
    if (null == this.D_OPER_MISURATOR_REA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_OPER_MISURATOR_REA);
    }
    if (null == this.D_OPER_MISURATOR_POT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_OPER_MISURATOR_POT);
    }
    if (null == this.T_MOTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MOTIVAZIONE);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_POD, __dataOut);
    }
    if (null == this.N_POTENZA_DISPONIBILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_POTENZA_DISPONIBILE, __dataOut);
    }
    if (null == this.N_POTENZA_IMPEGNATA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_POTENZA_IMPEGNATA, __dataOut);
    }
    if (null == this.N_TENSIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_TENSIONE, __dataOut);
    }
    if (null == this.T_TIPO_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_MISURATORE);
    }
    if (null == this.N_K_TRASFORMAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_K_TRASFORMAZIONE, __dataOut);
    }
    if (null == this.D_INST_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_INST_MISURATORE);
    }
    if (null == this.D_RIMOZ_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_RIMOZ_MISURATORE);
    }
    if (null == this.T_NOTA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_NOTA);
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
    if (null == this.N_NUM_CIFRE_EA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_CIFRE_EA, __dataOut);
    }
    if (null == this.N_NUM_CIFRE_ER) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_CIFRE_ER, __dataOut);
    }
    if (null == this.N_K_TRASFOR_ATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_K_TRASFOR_ATT, __dataOut);
    }
    if (null == this.N_K_TRASFOR_REA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_K_TRASFOR_REA, __dataOut);
    }
    if (null == this.N_K_TRASFOR_POT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_K_TRASFOR_POT, __dataOut);
    }
    if (null == this.T_MAT_MISURATORE_ATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MAT_MISURATORE_ATT);
    }
    if (null == this.T_MAT_MISURATORE_REA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MAT_MISURATORE_REA);
    }
    if (null == this.T_MAT_MISURATORE_POT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MAT_MISURATORE_POT);
    }
    if (null == this.D_INST_MISURATOR_ATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_INST_MISURATOR_ATT);
    }
    if (null == this.D_INST_MISURATOR_REA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_INST_MISURATOR_REA);
    }
    if (null == this.D_INST_MISURATOR_POT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_INST_MISURATOR_POT);
    }
    if (null == this.N_NUM_CIFRE_ATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_CIFRE_ATT, __dataOut);
    }
    if (null == this.N_NUM_CIFRE_REA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_CIFRE_REA, __dataOut);
    }
    if (null == this.N_NUM_CIFRE_POT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_CIFRE_POT, __dataOut);
    }
    if (null == this.B_PRESENZA_MIS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_PRESENZA_MIS);
    }
    if (null == this.B_GEST_FORFAIT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_GEST_FORFAIT);
    }
    if (null == this.T_TIPO_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_POD);
    }
    if (null == this.D_FINE_TIPO_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_FINE_TIPO_POD);
    }
    if (null == this.D_OPER_MISURATOR_ATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_OPER_MISURATOR_ATT);
    }
    if (null == this.D_OPER_MISURATOR_REA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_OPER_MISURATOR_REA);
    }
    if (null == this.D_OPER_MISURATOR_POT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_OPER_MISURATOR_POT);
    }
    if (null == this.T_MOTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MOTIVAZIONE);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_POD==null?"":N_ID_POD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_POTENZA_DISPONIBILE==null?"":N_POTENZA_DISPONIBILE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_POTENZA_IMPEGNATA==null?"":N_POTENZA_IMPEGNATA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_TENSIONE==null?"":N_TENSIONE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_MISURATORE==null?"":T_TIPO_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_K_TRASFORMAZIONE==null?"":N_K_TRASFORMAZIONE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_INST_MISURATORE==null?"":D_INST_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_RIMOZ_MISURATORE==null?"":D_RIMOZ_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOTA==null?"":T_NOTA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO==null?"":D_AGGIORNAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TRACCIA==null?"":N_ID_TRACCIA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_PREC==null?"":N_ID_S_PREC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_CIFRE_EA==null?"":N_NUM_CIFRE_EA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_CIFRE_ER==null?"":N_NUM_CIFRE_ER.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_K_TRASFOR_ATT==null?"":N_K_TRASFOR_ATT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_K_TRASFOR_REA==null?"":N_K_TRASFOR_REA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_K_TRASFOR_POT==null?"":N_K_TRASFOR_POT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MAT_MISURATORE_ATT==null?"":T_MAT_MISURATORE_ATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MAT_MISURATORE_REA==null?"":T_MAT_MISURATORE_REA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MAT_MISURATORE_POT==null?"":T_MAT_MISURATORE_POT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_INST_MISURATOR_ATT==null?"":D_INST_MISURATOR_ATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_INST_MISURATOR_REA==null?"":D_INST_MISURATOR_REA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_INST_MISURATOR_POT==null?"":D_INST_MISURATOR_POT, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_CIFRE_ATT==null?"":N_NUM_CIFRE_ATT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_CIFRE_REA==null?"":N_NUM_CIFRE_REA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_CIFRE_POT==null?"":N_NUM_CIFRE_POT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_PRESENZA_MIS==null?"":B_PRESENZA_MIS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_GEST_FORFAIT==null?"":B_GEST_FORFAIT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_POD==null?"":T_TIPO_POD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_FINE_TIPO_POD==null?"":D_FINE_TIPO_POD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_OPER_MISURATOR_ATT==null?"":D_OPER_MISURATOR_ATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_OPER_MISURATOR_REA==null?"":D_OPER_MISURATOR_REA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_OPER_MISURATOR_POT==null?"":D_OPER_MISURATOR_POT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MOTIVAZIONE==null?"":T_MOTIVAZIONE, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_POD==null?"":N_ID_POD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_POTENZA_DISPONIBILE==null?"":N_POTENZA_DISPONIBILE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_POTENZA_IMPEGNATA==null?"":N_POTENZA_IMPEGNATA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_TENSIONE==null?"":N_TENSIONE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_MISURATORE==null?"":T_TIPO_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_K_TRASFORMAZIONE==null?"":N_K_TRASFORMAZIONE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_INST_MISURATORE==null?"":D_INST_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_RIMOZ_MISURATORE==null?"":D_RIMOZ_MISURATORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_NOTA==null?"":T_NOTA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_AGGIORNAMENTO==null?"":D_AGGIORNAMENTO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_TRACCIA==null?"":N_ID_TRACCIA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_S_PREC==null?"":N_ID_S_PREC.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_CIFRE_EA==null?"":N_NUM_CIFRE_EA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_CIFRE_ER==null?"":N_NUM_CIFRE_ER.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_K_TRASFOR_ATT==null?"":N_K_TRASFOR_ATT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_K_TRASFOR_REA==null?"":N_K_TRASFOR_REA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_K_TRASFOR_POT==null?"":N_K_TRASFOR_POT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MAT_MISURATORE_ATT==null?"":T_MAT_MISURATORE_ATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MAT_MISURATORE_REA==null?"":T_MAT_MISURATORE_REA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MAT_MISURATORE_POT==null?"":T_MAT_MISURATORE_POT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_INST_MISURATOR_ATT==null?"":D_INST_MISURATOR_ATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_INST_MISURATOR_REA==null?"":D_INST_MISURATOR_REA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_INST_MISURATOR_POT==null?"":D_INST_MISURATOR_POT, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_CIFRE_ATT==null?"":N_NUM_CIFRE_ATT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_CIFRE_REA==null?"":N_NUM_CIFRE_REA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_CIFRE_POT==null?"":N_NUM_CIFRE_POT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_PRESENZA_MIS==null?"":B_PRESENZA_MIS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_GEST_FORFAIT==null?"":B_GEST_FORFAIT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_POD==null?"":T_TIPO_POD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_FINE_TIPO_POD==null?"":D_FINE_TIPO_POD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_OPER_MISURATOR_ATT==null?"":D_OPER_MISURATOR_ATT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_OPER_MISURATOR_REA==null?"":D_OPER_MISURATOR_REA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_OPER_MISURATOR_POT==null?"":D_OPER_MISURATOR_POT, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MOTIVAZIONE==null?"":T_MOTIVAZIONE, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_POD = null; } else {
      this.N_ID_POD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_POTENZA_DISPONIBILE = null; } else {
      this.N_POTENZA_DISPONIBILE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_POTENZA_IMPEGNATA = null; } else {
      this.N_POTENZA_IMPEGNATA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_TENSIONE = null; } else {
      this.N_TENSIONE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_MISURATORE = null; } else {
      this.T_TIPO_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_K_TRASFORMAZIONE = null; } else {
      this.N_K_TRASFORMAZIONE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_INST_MISURATORE = null; } else {
      this.D_INST_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_RIMOZ_MISURATORE = null; } else {
      this.D_RIMOZ_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOTA = null; } else {
      this.T_NOTA = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_CIFRE_EA = null; } else {
      this.N_NUM_CIFRE_EA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_CIFRE_ER = null; } else {
      this.N_NUM_CIFRE_ER = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_K_TRASFOR_ATT = null; } else {
      this.N_K_TRASFOR_ATT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_K_TRASFOR_REA = null; } else {
      this.N_K_TRASFOR_REA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_K_TRASFOR_POT = null; } else {
      this.N_K_TRASFOR_POT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MAT_MISURATORE_ATT = null; } else {
      this.T_MAT_MISURATORE_ATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MAT_MISURATORE_REA = null; } else {
      this.T_MAT_MISURATORE_REA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MAT_MISURATORE_POT = null; } else {
      this.T_MAT_MISURATORE_POT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_INST_MISURATOR_ATT = null; } else {
      this.D_INST_MISURATOR_ATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_INST_MISURATOR_REA = null; } else {
      this.D_INST_MISURATOR_REA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_INST_MISURATOR_POT = null; } else {
      this.D_INST_MISURATOR_POT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_CIFRE_ATT = null; } else {
      this.N_NUM_CIFRE_ATT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_CIFRE_REA = null; } else {
      this.N_NUM_CIFRE_REA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_CIFRE_POT = null; } else {
      this.N_NUM_CIFRE_POT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_PRESENZA_MIS = null; } else {
      this.B_PRESENZA_MIS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_GEST_FORFAIT = null; } else {
      this.B_GEST_FORFAIT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_POD = null; } else {
      this.T_TIPO_POD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_FINE_TIPO_POD = null; } else {
      this.D_FINE_TIPO_POD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_OPER_MISURATOR_ATT = null; } else {
      this.D_OPER_MISURATOR_ATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_OPER_MISURATOR_REA = null; } else {
      this.D_OPER_MISURATOR_REA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_OPER_MISURATOR_POT = null; } else {
      this.D_OPER_MISURATOR_POT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MOTIVAZIONE = null; } else {
      this.T_MOTIVAZIONE = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_POD = null; } else {
      this.N_ID_POD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_POTENZA_DISPONIBILE = null; } else {
      this.N_POTENZA_DISPONIBILE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_POTENZA_IMPEGNATA = null; } else {
      this.N_POTENZA_IMPEGNATA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_TENSIONE = null; } else {
      this.N_TENSIONE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_MISURATORE = null; } else {
      this.T_TIPO_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_K_TRASFORMAZIONE = null; } else {
      this.N_K_TRASFORMAZIONE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_INST_MISURATORE = null; } else {
      this.D_INST_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_RIMOZ_MISURATORE = null; } else {
      this.D_RIMOZ_MISURATORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_NOTA = null; } else {
      this.T_NOTA = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_CIFRE_EA = null; } else {
      this.N_NUM_CIFRE_EA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_CIFRE_ER = null; } else {
      this.N_NUM_CIFRE_ER = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_K_TRASFOR_ATT = null; } else {
      this.N_K_TRASFOR_ATT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_K_TRASFOR_REA = null; } else {
      this.N_K_TRASFOR_REA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_K_TRASFOR_POT = null; } else {
      this.N_K_TRASFOR_POT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MAT_MISURATORE_ATT = null; } else {
      this.T_MAT_MISURATORE_ATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MAT_MISURATORE_REA = null; } else {
      this.T_MAT_MISURATORE_REA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MAT_MISURATORE_POT = null; } else {
      this.T_MAT_MISURATORE_POT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_INST_MISURATOR_ATT = null; } else {
      this.D_INST_MISURATOR_ATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_INST_MISURATOR_REA = null; } else {
      this.D_INST_MISURATOR_REA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_INST_MISURATOR_POT = null; } else {
      this.D_INST_MISURATOR_POT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_CIFRE_ATT = null; } else {
      this.N_NUM_CIFRE_ATT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_CIFRE_REA = null; } else {
      this.N_NUM_CIFRE_REA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_CIFRE_POT = null; } else {
      this.N_NUM_CIFRE_POT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_PRESENZA_MIS = null; } else {
      this.B_PRESENZA_MIS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_GEST_FORFAIT = null; } else {
      this.B_GEST_FORFAIT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_POD = null; } else {
      this.T_TIPO_POD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_FINE_TIPO_POD = null; } else {
      this.D_FINE_TIPO_POD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_OPER_MISURATOR_ATT = null; } else {
      this.D_OPER_MISURATOR_ATT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_OPER_MISURATOR_REA = null; } else {
      this.D_OPER_MISURATOR_REA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_OPER_MISURATOR_POT = null; } else {
      this.D_OPER_MISURATOR_POT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MOTIVAZIONE = null; } else {
      this.T_MOTIVAZIONE = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    rcu_rcu_pod_tecn o = (rcu_rcu_pod_tecn) super.clone();
    return o;
  }

  public void clone0(rcu_rcu_pod_tecn o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_POD", this.N_ID_POD);
    __sqoop$field_map.put("N_POTENZA_DISPONIBILE", this.N_POTENZA_DISPONIBILE);
    __sqoop$field_map.put("N_POTENZA_IMPEGNATA", this.N_POTENZA_IMPEGNATA);
    __sqoop$field_map.put("N_TENSIONE", this.N_TENSIONE);
    __sqoop$field_map.put("T_TIPO_MISURATORE", this.T_TIPO_MISURATORE);
    __sqoop$field_map.put("N_K_TRASFORMAZIONE", this.N_K_TRASFORMAZIONE);
    __sqoop$field_map.put("D_INST_MISURATORE", this.D_INST_MISURATORE);
    __sqoop$field_map.put("D_RIMOZ_MISURATORE", this.D_RIMOZ_MISURATORE);
    __sqoop$field_map.put("T_NOTA", this.T_NOTA);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("N_NUM_CIFRE_EA", this.N_NUM_CIFRE_EA);
    __sqoop$field_map.put("N_NUM_CIFRE_ER", this.N_NUM_CIFRE_ER);
    __sqoop$field_map.put("N_K_TRASFOR_ATT", this.N_K_TRASFOR_ATT);
    __sqoop$field_map.put("N_K_TRASFOR_REA", this.N_K_TRASFOR_REA);
    __sqoop$field_map.put("N_K_TRASFOR_POT", this.N_K_TRASFOR_POT);
    __sqoop$field_map.put("T_MAT_MISURATORE_ATT", this.T_MAT_MISURATORE_ATT);
    __sqoop$field_map.put("T_MAT_MISURATORE_REA", this.T_MAT_MISURATORE_REA);
    __sqoop$field_map.put("T_MAT_MISURATORE_POT", this.T_MAT_MISURATORE_POT);
    __sqoop$field_map.put("D_INST_MISURATOR_ATT", this.D_INST_MISURATOR_ATT);
    __sqoop$field_map.put("D_INST_MISURATOR_REA", this.D_INST_MISURATOR_REA);
    __sqoop$field_map.put("D_INST_MISURATOR_POT", this.D_INST_MISURATOR_POT);
    __sqoop$field_map.put("N_NUM_CIFRE_ATT", this.N_NUM_CIFRE_ATT);
    __sqoop$field_map.put("N_NUM_CIFRE_REA", this.N_NUM_CIFRE_REA);
    __sqoop$field_map.put("N_NUM_CIFRE_POT", this.N_NUM_CIFRE_POT);
    __sqoop$field_map.put("B_PRESENZA_MIS", this.B_PRESENZA_MIS);
    __sqoop$field_map.put("B_GEST_FORFAIT", this.B_GEST_FORFAIT);
    __sqoop$field_map.put("T_TIPO_POD", this.T_TIPO_POD);
    __sqoop$field_map.put("D_FINE_TIPO_POD", this.D_FINE_TIPO_POD);
    __sqoop$field_map.put("D_OPER_MISURATOR_ATT", this.D_OPER_MISURATOR_ATT);
    __sqoop$field_map.put("D_OPER_MISURATOR_REA", this.D_OPER_MISURATOR_REA);
    __sqoop$field_map.put("D_OPER_MISURATOR_POT", this.D_OPER_MISURATOR_POT);
    __sqoop$field_map.put("T_MOTIVAZIONE", this.T_MOTIVAZIONE);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_POD", this.N_ID_POD);
    __sqoop$field_map.put("N_POTENZA_DISPONIBILE", this.N_POTENZA_DISPONIBILE);
    __sqoop$field_map.put("N_POTENZA_IMPEGNATA", this.N_POTENZA_IMPEGNATA);
    __sqoop$field_map.put("N_TENSIONE", this.N_TENSIONE);
    __sqoop$field_map.put("T_TIPO_MISURATORE", this.T_TIPO_MISURATORE);
    __sqoop$field_map.put("N_K_TRASFORMAZIONE", this.N_K_TRASFORMAZIONE);
    __sqoop$field_map.put("D_INST_MISURATORE", this.D_INST_MISURATORE);
    __sqoop$field_map.put("D_RIMOZ_MISURATORE", this.D_RIMOZ_MISURATORE);
    __sqoop$field_map.put("T_NOTA", this.T_NOTA);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("N_NUM_CIFRE_EA", this.N_NUM_CIFRE_EA);
    __sqoop$field_map.put("N_NUM_CIFRE_ER", this.N_NUM_CIFRE_ER);
    __sqoop$field_map.put("N_K_TRASFOR_ATT", this.N_K_TRASFOR_ATT);
    __sqoop$field_map.put("N_K_TRASFOR_REA", this.N_K_TRASFOR_REA);
    __sqoop$field_map.put("N_K_TRASFOR_POT", this.N_K_TRASFOR_POT);
    __sqoop$field_map.put("T_MAT_MISURATORE_ATT", this.T_MAT_MISURATORE_ATT);
    __sqoop$field_map.put("T_MAT_MISURATORE_REA", this.T_MAT_MISURATORE_REA);
    __sqoop$field_map.put("T_MAT_MISURATORE_POT", this.T_MAT_MISURATORE_POT);
    __sqoop$field_map.put("D_INST_MISURATOR_ATT", this.D_INST_MISURATOR_ATT);
    __sqoop$field_map.put("D_INST_MISURATOR_REA", this.D_INST_MISURATOR_REA);
    __sqoop$field_map.put("D_INST_MISURATOR_POT", this.D_INST_MISURATOR_POT);
    __sqoop$field_map.put("N_NUM_CIFRE_ATT", this.N_NUM_CIFRE_ATT);
    __sqoop$field_map.put("N_NUM_CIFRE_REA", this.N_NUM_CIFRE_REA);
    __sqoop$field_map.put("N_NUM_CIFRE_POT", this.N_NUM_CIFRE_POT);
    __sqoop$field_map.put("B_PRESENZA_MIS", this.B_PRESENZA_MIS);
    __sqoop$field_map.put("B_GEST_FORFAIT", this.B_GEST_FORFAIT);
    __sqoop$field_map.put("T_TIPO_POD", this.T_TIPO_POD);
    __sqoop$field_map.put("D_FINE_TIPO_POD", this.D_FINE_TIPO_POD);
    __sqoop$field_map.put("D_OPER_MISURATOR_ATT", this.D_OPER_MISURATOR_ATT);
    __sqoop$field_map.put("D_OPER_MISURATOR_REA", this.D_OPER_MISURATOR_REA);
    __sqoop$field_map.put("D_OPER_MISURATOR_POT", this.D_OPER_MISURATOR_POT);
    __sqoop$field_map.put("T_MOTIVAZIONE", this.T_MOTIVAZIONE);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
