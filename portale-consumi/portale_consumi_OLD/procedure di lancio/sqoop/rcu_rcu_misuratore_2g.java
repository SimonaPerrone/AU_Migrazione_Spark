// ORM class for table 'rcu.rcu_misuratore_2g'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 10:04:12 CEST 2019
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

public class rcu_rcu_misuratore_2g extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_MISURATORE_2G", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_MISURATORE_2G = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_POD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_POD = (java.math.BigDecimal)value;
      }
    });
    setters.put("B_VIS_FASCE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_VIS_FASCE = (String)value;
      }
    });
    setters.put("B_VIS_VENDITORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_VIS_VENDITORE = (String)value;
      }
    });
    setters.put("B_VIS_TELEFONOV", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_VIS_TELEFONOV = (String)value;
      }
    });
    setters.put("B_VIS_DATAINICONTR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_VIS_DATAINICONTR = (String)value;
      }
    });
    setters.put("B_VIS_DATAINIZIOFREEZING", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_VIS_DATAINIZIOFREEZING = (String)value;
      }
    });
    setters.put("B_VIS_MESSAGGICLIENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_VIS_MESSAGGICLIENTE = (String)value;
      }
    });
    setters.put("B_VIS_CODCLI", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_VIS_CODCLI = (String)value;
      }
    });
    setters.put("T_CODCLI", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODCLI = (String)value;
      }
    });
    setters.put("T_VENDITORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_VENDITORE = (String)value;
      }
    });
    setters.put("T_TELEFONOV", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TELEFONOV = (String)value;
      }
    });
    setters.put("D_DATA_INICONTR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_INICONTR = (String)value;
      }
    });
    setters.put("D_DATA_INIZIOFREEZING", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_INIZIOFREEZING = (String)value;
      }
    });
    setters.put("T_MESSAGGIO_CLIENTE_1", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MESSAGGIO_CLIENTE_1 = (String)value;
      }
    });
    setters.put("T_MESSAGGIO_CLIENTE_2", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MESSAGGIO_CLIENTE_2 = (String)value;
      }
    });
    setters.put("T_MESSAGGIO_CLIENTE_3", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MESSAGGIO_CLIENTE_3 = (String)value;
      }
    });
    setters.put("T_MESSAGGIO_CLIENTE_4", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MESSAGGIO_CLIENTE_4 = (String)value;
      }
    });
    setters.put("T_MESSAGGIO_CLIENTE_5", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_MESSAGGIO_CLIENTE_5 = (String)value;
      }
    });
    setters.put("N_NUM_FASCE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_NUM_FASCE = (java.math.BigDecimal)value;
      }
    });
    setters.put("D_INIZIO_VALIDITA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_INIZIO_VALIDITA = (String)value;
      }
    });
    setters.put("D_FINE_VALIDITA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_FINE_VALIDITA = (String)value;
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
    setters.put("D_DATA_RIF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_RIF = (String)value;
      }
    });
    setters.put("T_TIPO_CONFIGURAZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO_CONFIGURAZIONE = (String)value;
      }
    });
  }
  public rcu_rcu_misuratore_2g() {
    init0();
  }
  private java.math.BigDecimal N_ID_MISURATORE_2G;
  public java.math.BigDecimal get_N_ID_MISURATORE_2G() {
    return N_ID_MISURATORE_2G;
  }
  public void set_N_ID_MISURATORE_2G(java.math.BigDecimal N_ID_MISURATORE_2G) {
    this.N_ID_MISURATORE_2G = N_ID_MISURATORE_2G;
  }
  public rcu_rcu_misuratore_2g with_N_ID_MISURATORE_2G(java.math.BigDecimal N_ID_MISURATORE_2G) {
    this.N_ID_MISURATORE_2G = N_ID_MISURATORE_2G;
    return this;
  }
  private java.math.BigDecimal N_ID_POD;
  public java.math.BigDecimal get_N_ID_POD() {
    return N_ID_POD;
  }
  public void set_N_ID_POD(java.math.BigDecimal N_ID_POD) {
    this.N_ID_POD = N_ID_POD;
  }
  public rcu_rcu_misuratore_2g with_N_ID_POD(java.math.BigDecimal N_ID_POD) {
    this.N_ID_POD = N_ID_POD;
    return this;
  }
  private String B_VIS_FASCE;
  public String get_B_VIS_FASCE() {
    return B_VIS_FASCE;
  }
  public void set_B_VIS_FASCE(String B_VIS_FASCE) {
    this.B_VIS_FASCE = B_VIS_FASCE;
  }
  public rcu_rcu_misuratore_2g with_B_VIS_FASCE(String B_VIS_FASCE) {
    this.B_VIS_FASCE = B_VIS_FASCE;
    return this;
  }
  private String B_VIS_VENDITORE;
  public String get_B_VIS_VENDITORE() {
    return B_VIS_VENDITORE;
  }
  public void set_B_VIS_VENDITORE(String B_VIS_VENDITORE) {
    this.B_VIS_VENDITORE = B_VIS_VENDITORE;
  }
  public rcu_rcu_misuratore_2g with_B_VIS_VENDITORE(String B_VIS_VENDITORE) {
    this.B_VIS_VENDITORE = B_VIS_VENDITORE;
    return this;
  }
  private String B_VIS_TELEFONOV;
  public String get_B_VIS_TELEFONOV() {
    return B_VIS_TELEFONOV;
  }
  public void set_B_VIS_TELEFONOV(String B_VIS_TELEFONOV) {
    this.B_VIS_TELEFONOV = B_VIS_TELEFONOV;
  }
  public rcu_rcu_misuratore_2g with_B_VIS_TELEFONOV(String B_VIS_TELEFONOV) {
    this.B_VIS_TELEFONOV = B_VIS_TELEFONOV;
    return this;
  }
  private String B_VIS_DATAINICONTR;
  public String get_B_VIS_DATAINICONTR() {
    return B_VIS_DATAINICONTR;
  }
  public void set_B_VIS_DATAINICONTR(String B_VIS_DATAINICONTR) {
    this.B_VIS_DATAINICONTR = B_VIS_DATAINICONTR;
  }
  public rcu_rcu_misuratore_2g with_B_VIS_DATAINICONTR(String B_VIS_DATAINICONTR) {
    this.B_VIS_DATAINICONTR = B_VIS_DATAINICONTR;
    return this;
  }
  private String B_VIS_DATAINIZIOFREEZING;
  public String get_B_VIS_DATAINIZIOFREEZING() {
    return B_VIS_DATAINIZIOFREEZING;
  }
  public void set_B_VIS_DATAINIZIOFREEZING(String B_VIS_DATAINIZIOFREEZING) {
    this.B_VIS_DATAINIZIOFREEZING = B_VIS_DATAINIZIOFREEZING;
  }
  public rcu_rcu_misuratore_2g with_B_VIS_DATAINIZIOFREEZING(String B_VIS_DATAINIZIOFREEZING) {
    this.B_VIS_DATAINIZIOFREEZING = B_VIS_DATAINIZIOFREEZING;
    return this;
  }
  private String B_VIS_MESSAGGICLIENTE;
  public String get_B_VIS_MESSAGGICLIENTE() {
    return B_VIS_MESSAGGICLIENTE;
  }
  public void set_B_VIS_MESSAGGICLIENTE(String B_VIS_MESSAGGICLIENTE) {
    this.B_VIS_MESSAGGICLIENTE = B_VIS_MESSAGGICLIENTE;
  }
  public rcu_rcu_misuratore_2g with_B_VIS_MESSAGGICLIENTE(String B_VIS_MESSAGGICLIENTE) {
    this.B_VIS_MESSAGGICLIENTE = B_VIS_MESSAGGICLIENTE;
    return this;
  }
  private String B_VIS_CODCLI;
  public String get_B_VIS_CODCLI() {
    return B_VIS_CODCLI;
  }
  public void set_B_VIS_CODCLI(String B_VIS_CODCLI) {
    this.B_VIS_CODCLI = B_VIS_CODCLI;
  }
  public rcu_rcu_misuratore_2g with_B_VIS_CODCLI(String B_VIS_CODCLI) {
    this.B_VIS_CODCLI = B_VIS_CODCLI;
    return this;
  }
  private String T_CODCLI;
  public String get_T_CODCLI() {
    return T_CODCLI;
  }
  public void set_T_CODCLI(String T_CODCLI) {
    this.T_CODCLI = T_CODCLI;
  }
  public rcu_rcu_misuratore_2g with_T_CODCLI(String T_CODCLI) {
    this.T_CODCLI = T_CODCLI;
    return this;
  }
  private String T_VENDITORE;
  public String get_T_VENDITORE() {
    return T_VENDITORE;
  }
  public void set_T_VENDITORE(String T_VENDITORE) {
    this.T_VENDITORE = T_VENDITORE;
  }
  public rcu_rcu_misuratore_2g with_T_VENDITORE(String T_VENDITORE) {
    this.T_VENDITORE = T_VENDITORE;
    return this;
  }
  private String T_TELEFONOV;
  public String get_T_TELEFONOV() {
    return T_TELEFONOV;
  }
  public void set_T_TELEFONOV(String T_TELEFONOV) {
    this.T_TELEFONOV = T_TELEFONOV;
  }
  public rcu_rcu_misuratore_2g with_T_TELEFONOV(String T_TELEFONOV) {
    this.T_TELEFONOV = T_TELEFONOV;
    return this;
  }
  private String D_DATA_INICONTR;
  public String get_D_DATA_INICONTR() {
    return D_DATA_INICONTR;
  }
  public void set_D_DATA_INICONTR(String D_DATA_INICONTR) {
    this.D_DATA_INICONTR = D_DATA_INICONTR;
  }
  public rcu_rcu_misuratore_2g with_D_DATA_INICONTR(String D_DATA_INICONTR) {
    this.D_DATA_INICONTR = D_DATA_INICONTR;
    return this;
  }
  private String D_DATA_INIZIOFREEZING;
  public String get_D_DATA_INIZIOFREEZING() {
    return D_DATA_INIZIOFREEZING;
  }
  public void set_D_DATA_INIZIOFREEZING(String D_DATA_INIZIOFREEZING) {
    this.D_DATA_INIZIOFREEZING = D_DATA_INIZIOFREEZING;
  }
  public rcu_rcu_misuratore_2g with_D_DATA_INIZIOFREEZING(String D_DATA_INIZIOFREEZING) {
    this.D_DATA_INIZIOFREEZING = D_DATA_INIZIOFREEZING;
    return this;
  }
  private String T_MESSAGGIO_CLIENTE_1;
  public String get_T_MESSAGGIO_CLIENTE_1() {
    return T_MESSAGGIO_CLIENTE_1;
  }
  public void set_T_MESSAGGIO_CLIENTE_1(String T_MESSAGGIO_CLIENTE_1) {
    this.T_MESSAGGIO_CLIENTE_1 = T_MESSAGGIO_CLIENTE_1;
  }
  public rcu_rcu_misuratore_2g with_T_MESSAGGIO_CLIENTE_1(String T_MESSAGGIO_CLIENTE_1) {
    this.T_MESSAGGIO_CLIENTE_1 = T_MESSAGGIO_CLIENTE_1;
    return this;
  }
  private String T_MESSAGGIO_CLIENTE_2;
  public String get_T_MESSAGGIO_CLIENTE_2() {
    return T_MESSAGGIO_CLIENTE_2;
  }
  public void set_T_MESSAGGIO_CLIENTE_2(String T_MESSAGGIO_CLIENTE_2) {
    this.T_MESSAGGIO_CLIENTE_2 = T_MESSAGGIO_CLIENTE_2;
  }
  public rcu_rcu_misuratore_2g with_T_MESSAGGIO_CLIENTE_2(String T_MESSAGGIO_CLIENTE_2) {
    this.T_MESSAGGIO_CLIENTE_2 = T_MESSAGGIO_CLIENTE_2;
    return this;
  }
  private String T_MESSAGGIO_CLIENTE_3;
  public String get_T_MESSAGGIO_CLIENTE_3() {
    return T_MESSAGGIO_CLIENTE_3;
  }
  public void set_T_MESSAGGIO_CLIENTE_3(String T_MESSAGGIO_CLIENTE_3) {
    this.T_MESSAGGIO_CLIENTE_3 = T_MESSAGGIO_CLIENTE_3;
  }
  public rcu_rcu_misuratore_2g with_T_MESSAGGIO_CLIENTE_3(String T_MESSAGGIO_CLIENTE_3) {
    this.T_MESSAGGIO_CLIENTE_3 = T_MESSAGGIO_CLIENTE_3;
    return this;
  }
  private String T_MESSAGGIO_CLIENTE_4;
  public String get_T_MESSAGGIO_CLIENTE_4() {
    return T_MESSAGGIO_CLIENTE_4;
  }
  public void set_T_MESSAGGIO_CLIENTE_4(String T_MESSAGGIO_CLIENTE_4) {
    this.T_MESSAGGIO_CLIENTE_4 = T_MESSAGGIO_CLIENTE_4;
  }
  public rcu_rcu_misuratore_2g with_T_MESSAGGIO_CLIENTE_4(String T_MESSAGGIO_CLIENTE_4) {
    this.T_MESSAGGIO_CLIENTE_4 = T_MESSAGGIO_CLIENTE_4;
    return this;
  }
  private String T_MESSAGGIO_CLIENTE_5;
  public String get_T_MESSAGGIO_CLIENTE_5() {
    return T_MESSAGGIO_CLIENTE_5;
  }
  public void set_T_MESSAGGIO_CLIENTE_5(String T_MESSAGGIO_CLIENTE_5) {
    this.T_MESSAGGIO_CLIENTE_5 = T_MESSAGGIO_CLIENTE_5;
  }
  public rcu_rcu_misuratore_2g with_T_MESSAGGIO_CLIENTE_5(String T_MESSAGGIO_CLIENTE_5) {
    this.T_MESSAGGIO_CLIENTE_5 = T_MESSAGGIO_CLIENTE_5;
    return this;
  }
  private java.math.BigDecimal N_NUM_FASCE;
  public java.math.BigDecimal get_N_NUM_FASCE() {
    return N_NUM_FASCE;
  }
  public void set_N_NUM_FASCE(java.math.BigDecimal N_NUM_FASCE) {
    this.N_NUM_FASCE = N_NUM_FASCE;
  }
  public rcu_rcu_misuratore_2g with_N_NUM_FASCE(java.math.BigDecimal N_NUM_FASCE) {
    this.N_NUM_FASCE = N_NUM_FASCE;
    return this;
  }
  private String D_INIZIO_VALIDITA;
  public String get_D_INIZIO_VALIDITA() {
    return D_INIZIO_VALIDITA;
  }
  public void set_D_INIZIO_VALIDITA(String D_INIZIO_VALIDITA) {
    this.D_INIZIO_VALIDITA = D_INIZIO_VALIDITA;
  }
  public rcu_rcu_misuratore_2g with_D_INIZIO_VALIDITA(String D_INIZIO_VALIDITA) {
    this.D_INIZIO_VALIDITA = D_INIZIO_VALIDITA;
    return this;
  }
  private String D_FINE_VALIDITA;
  public String get_D_FINE_VALIDITA() {
    return D_FINE_VALIDITA;
  }
  public void set_D_FINE_VALIDITA(String D_FINE_VALIDITA) {
    this.D_FINE_VALIDITA = D_FINE_VALIDITA;
  }
  public rcu_rcu_misuratore_2g with_D_FINE_VALIDITA(String D_FINE_VALIDITA) {
    this.D_FINE_VALIDITA = D_FINE_VALIDITA;
    return this;
  }
  private String T_NOTA;
  public String get_T_NOTA() {
    return T_NOTA;
  }
  public void set_T_NOTA(String T_NOTA) {
    this.T_NOTA = T_NOTA;
  }
  public rcu_rcu_misuratore_2g with_T_NOTA(String T_NOTA) {
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
  public rcu_rcu_misuratore_2g with_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
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
  public rcu_rcu_misuratore_2g with_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
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
  public rcu_rcu_misuratore_2g with_N_ID_S_PREC(java.math.BigDecimal N_ID_S_PREC) {
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
  public rcu_rcu_misuratore_2g with_D_DATA_RIF(String D_DATA_RIF) {
    this.D_DATA_RIF = D_DATA_RIF;
    return this;
  }
  private String T_TIPO_CONFIGURAZIONE;
  public String get_T_TIPO_CONFIGURAZIONE() {
    return T_TIPO_CONFIGURAZIONE;
  }
  public void set_T_TIPO_CONFIGURAZIONE(String T_TIPO_CONFIGURAZIONE) {
    this.T_TIPO_CONFIGURAZIONE = T_TIPO_CONFIGURAZIONE;
  }
  public rcu_rcu_misuratore_2g with_T_TIPO_CONFIGURAZIONE(String T_TIPO_CONFIGURAZIONE) {
    this.T_TIPO_CONFIGURAZIONE = T_TIPO_CONFIGURAZIONE;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcu_rcu_misuratore_2g)) {
      return false;
    }
    rcu_rcu_misuratore_2g that = (rcu_rcu_misuratore_2g) o;
    boolean equal = true;
    equal = equal && (this.N_ID_MISURATORE_2G == null ? that.N_ID_MISURATORE_2G == null : this.N_ID_MISURATORE_2G.equals(that.N_ID_MISURATORE_2G));
    equal = equal && (this.N_ID_POD == null ? that.N_ID_POD == null : this.N_ID_POD.equals(that.N_ID_POD));
    equal = equal && (this.B_VIS_FASCE == null ? that.B_VIS_FASCE == null : this.B_VIS_FASCE.equals(that.B_VIS_FASCE));
    equal = equal && (this.B_VIS_VENDITORE == null ? that.B_VIS_VENDITORE == null : this.B_VIS_VENDITORE.equals(that.B_VIS_VENDITORE));
    equal = equal && (this.B_VIS_TELEFONOV == null ? that.B_VIS_TELEFONOV == null : this.B_VIS_TELEFONOV.equals(that.B_VIS_TELEFONOV));
    equal = equal && (this.B_VIS_DATAINICONTR == null ? that.B_VIS_DATAINICONTR == null : this.B_VIS_DATAINICONTR.equals(that.B_VIS_DATAINICONTR));
    equal = equal && (this.B_VIS_DATAINIZIOFREEZING == null ? that.B_VIS_DATAINIZIOFREEZING == null : this.B_VIS_DATAINIZIOFREEZING.equals(that.B_VIS_DATAINIZIOFREEZING));
    equal = equal && (this.B_VIS_MESSAGGICLIENTE == null ? that.B_VIS_MESSAGGICLIENTE == null : this.B_VIS_MESSAGGICLIENTE.equals(that.B_VIS_MESSAGGICLIENTE));
    equal = equal && (this.B_VIS_CODCLI == null ? that.B_VIS_CODCLI == null : this.B_VIS_CODCLI.equals(that.B_VIS_CODCLI));
    equal = equal && (this.T_CODCLI == null ? that.T_CODCLI == null : this.T_CODCLI.equals(that.T_CODCLI));
    equal = equal && (this.T_VENDITORE == null ? that.T_VENDITORE == null : this.T_VENDITORE.equals(that.T_VENDITORE));
    equal = equal && (this.T_TELEFONOV == null ? that.T_TELEFONOV == null : this.T_TELEFONOV.equals(that.T_TELEFONOV));
    equal = equal && (this.D_DATA_INICONTR == null ? that.D_DATA_INICONTR == null : this.D_DATA_INICONTR.equals(that.D_DATA_INICONTR));
    equal = equal && (this.D_DATA_INIZIOFREEZING == null ? that.D_DATA_INIZIOFREEZING == null : this.D_DATA_INIZIOFREEZING.equals(that.D_DATA_INIZIOFREEZING));
    equal = equal && (this.T_MESSAGGIO_CLIENTE_1 == null ? that.T_MESSAGGIO_CLIENTE_1 == null : this.T_MESSAGGIO_CLIENTE_1.equals(that.T_MESSAGGIO_CLIENTE_1));
    equal = equal && (this.T_MESSAGGIO_CLIENTE_2 == null ? that.T_MESSAGGIO_CLIENTE_2 == null : this.T_MESSAGGIO_CLIENTE_2.equals(that.T_MESSAGGIO_CLIENTE_2));
    equal = equal && (this.T_MESSAGGIO_CLIENTE_3 == null ? that.T_MESSAGGIO_CLIENTE_3 == null : this.T_MESSAGGIO_CLIENTE_3.equals(that.T_MESSAGGIO_CLIENTE_3));
    equal = equal && (this.T_MESSAGGIO_CLIENTE_4 == null ? that.T_MESSAGGIO_CLIENTE_4 == null : this.T_MESSAGGIO_CLIENTE_4.equals(that.T_MESSAGGIO_CLIENTE_4));
    equal = equal && (this.T_MESSAGGIO_CLIENTE_5 == null ? that.T_MESSAGGIO_CLIENTE_5 == null : this.T_MESSAGGIO_CLIENTE_5.equals(that.T_MESSAGGIO_CLIENTE_5));
    equal = equal && (this.N_NUM_FASCE == null ? that.N_NUM_FASCE == null : this.N_NUM_FASCE.equals(that.N_NUM_FASCE));
    equal = equal && (this.D_INIZIO_VALIDITA == null ? that.D_INIZIO_VALIDITA == null : this.D_INIZIO_VALIDITA.equals(that.D_INIZIO_VALIDITA));
    equal = equal && (this.D_FINE_VALIDITA == null ? that.D_FINE_VALIDITA == null : this.D_FINE_VALIDITA.equals(that.D_FINE_VALIDITA));
    equal = equal && (this.T_NOTA == null ? that.T_NOTA == null : this.T_NOTA.equals(that.T_NOTA));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.D_DATA_RIF == null ? that.D_DATA_RIF == null : this.D_DATA_RIF.equals(that.D_DATA_RIF));
    equal = equal && (this.T_TIPO_CONFIGURAZIONE == null ? that.T_TIPO_CONFIGURAZIONE == null : this.T_TIPO_CONFIGURAZIONE.equals(that.T_TIPO_CONFIGURAZIONE));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcu_rcu_misuratore_2g)) {
      return false;
    }
    rcu_rcu_misuratore_2g that = (rcu_rcu_misuratore_2g) o;
    boolean equal = true;
    equal = equal && (this.N_ID_MISURATORE_2G == null ? that.N_ID_MISURATORE_2G == null : this.N_ID_MISURATORE_2G.equals(that.N_ID_MISURATORE_2G));
    equal = equal && (this.N_ID_POD == null ? that.N_ID_POD == null : this.N_ID_POD.equals(that.N_ID_POD));
    equal = equal && (this.B_VIS_FASCE == null ? that.B_VIS_FASCE == null : this.B_VIS_FASCE.equals(that.B_VIS_FASCE));
    equal = equal && (this.B_VIS_VENDITORE == null ? that.B_VIS_VENDITORE == null : this.B_VIS_VENDITORE.equals(that.B_VIS_VENDITORE));
    equal = equal && (this.B_VIS_TELEFONOV == null ? that.B_VIS_TELEFONOV == null : this.B_VIS_TELEFONOV.equals(that.B_VIS_TELEFONOV));
    equal = equal && (this.B_VIS_DATAINICONTR == null ? that.B_VIS_DATAINICONTR == null : this.B_VIS_DATAINICONTR.equals(that.B_VIS_DATAINICONTR));
    equal = equal && (this.B_VIS_DATAINIZIOFREEZING == null ? that.B_VIS_DATAINIZIOFREEZING == null : this.B_VIS_DATAINIZIOFREEZING.equals(that.B_VIS_DATAINIZIOFREEZING));
    equal = equal && (this.B_VIS_MESSAGGICLIENTE == null ? that.B_VIS_MESSAGGICLIENTE == null : this.B_VIS_MESSAGGICLIENTE.equals(that.B_VIS_MESSAGGICLIENTE));
    equal = equal && (this.B_VIS_CODCLI == null ? that.B_VIS_CODCLI == null : this.B_VIS_CODCLI.equals(that.B_VIS_CODCLI));
    equal = equal && (this.T_CODCLI == null ? that.T_CODCLI == null : this.T_CODCLI.equals(that.T_CODCLI));
    equal = equal && (this.T_VENDITORE == null ? that.T_VENDITORE == null : this.T_VENDITORE.equals(that.T_VENDITORE));
    equal = equal && (this.T_TELEFONOV == null ? that.T_TELEFONOV == null : this.T_TELEFONOV.equals(that.T_TELEFONOV));
    equal = equal && (this.D_DATA_INICONTR == null ? that.D_DATA_INICONTR == null : this.D_DATA_INICONTR.equals(that.D_DATA_INICONTR));
    equal = equal && (this.D_DATA_INIZIOFREEZING == null ? that.D_DATA_INIZIOFREEZING == null : this.D_DATA_INIZIOFREEZING.equals(that.D_DATA_INIZIOFREEZING));
    equal = equal && (this.T_MESSAGGIO_CLIENTE_1 == null ? that.T_MESSAGGIO_CLIENTE_1 == null : this.T_MESSAGGIO_CLIENTE_1.equals(that.T_MESSAGGIO_CLIENTE_1));
    equal = equal && (this.T_MESSAGGIO_CLIENTE_2 == null ? that.T_MESSAGGIO_CLIENTE_2 == null : this.T_MESSAGGIO_CLIENTE_2.equals(that.T_MESSAGGIO_CLIENTE_2));
    equal = equal && (this.T_MESSAGGIO_CLIENTE_3 == null ? that.T_MESSAGGIO_CLIENTE_3 == null : this.T_MESSAGGIO_CLIENTE_3.equals(that.T_MESSAGGIO_CLIENTE_3));
    equal = equal && (this.T_MESSAGGIO_CLIENTE_4 == null ? that.T_MESSAGGIO_CLIENTE_4 == null : this.T_MESSAGGIO_CLIENTE_4.equals(that.T_MESSAGGIO_CLIENTE_4));
    equal = equal && (this.T_MESSAGGIO_CLIENTE_5 == null ? that.T_MESSAGGIO_CLIENTE_5 == null : this.T_MESSAGGIO_CLIENTE_5.equals(that.T_MESSAGGIO_CLIENTE_5));
    equal = equal && (this.N_NUM_FASCE == null ? that.N_NUM_FASCE == null : this.N_NUM_FASCE.equals(that.N_NUM_FASCE));
    equal = equal && (this.D_INIZIO_VALIDITA == null ? that.D_INIZIO_VALIDITA == null : this.D_INIZIO_VALIDITA.equals(that.D_INIZIO_VALIDITA));
    equal = equal && (this.D_FINE_VALIDITA == null ? that.D_FINE_VALIDITA == null : this.D_FINE_VALIDITA.equals(that.D_FINE_VALIDITA));
    equal = equal && (this.T_NOTA == null ? that.T_NOTA == null : this.T_NOTA.equals(that.T_NOTA));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.D_DATA_RIF == null ? that.D_DATA_RIF == null : this.D_DATA_RIF.equals(that.D_DATA_RIF));
    equal = equal && (this.T_TIPO_CONFIGURAZIONE == null ? that.T_TIPO_CONFIGURAZIONE == null : this.T_TIPO_CONFIGURAZIONE.equals(that.T_TIPO_CONFIGURAZIONE));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_MISURATORE_2G = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_POD = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.B_VIS_FASCE = JdbcWritableBridge.readString(3, __dbResults);
    this.B_VIS_VENDITORE = JdbcWritableBridge.readString(4, __dbResults);
    this.B_VIS_TELEFONOV = JdbcWritableBridge.readString(5, __dbResults);
    this.B_VIS_DATAINICONTR = JdbcWritableBridge.readString(6, __dbResults);
    this.B_VIS_DATAINIZIOFREEZING = JdbcWritableBridge.readString(7, __dbResults);
    this.B_VIS_MESSAGGICLIENTE = JdbcWritableBridge.readString(8, __dbResults);
    this.B_VIS_CODCLI = JdbcWritableBridge.readString(9, __dbResults);
    this.T_CODCLI = JdbcWritableBridge.readString(10, __dbResults);
    this.T_VENDITORE = JdbcWritableBridge.readString(11, __dbResults);
    this.T_TELEFONOV = JdbcWritableBridge.readString(12, __dbResults);
    this.D_DATA_INICONTR = JdbcWritableBridge.readString(13, __dbResults);
    this.D_DATA_INIZIOFREEZING = JdbcWritableBridge.readString(14, __dbResults);
    this.T_MESSAGGIO_CLIENTE_1 = JdbcWritableBridge.readString(15, __dbResults);
    this.T_MESSAGGIO_CLIENTE_2 = JdbcWritableBridge.readString(16, __dbResults);
    this.T_MESSAGGIO_CLIENTE_3 = JdbcWritableBridge.readString(17, __dbResults);
    this.T_MESSAGGIO_CLIENTE_4 = JdbcWritableBridge.readString(18, __dbResults);
    this.T_MESSAGGIO_CLIENTE_5 = JdbcWritableBridge.readString(19, __dbResults);
    this.N_NUM_FASCE = JdbcWritableBridge.readBigDecimal(20, __dbResults);
    this.D_INIZIO_VALIDITA = JdbcWritableBridge.readString(21, __dbResults);
    this.D_FINE_VALIDITA = JdbcWritableBridge.readString(22, __dbResults);
    this.T_NOTA = JdbcWritableBridge.readString(23, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(24, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(25, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(26, __dbResults);
    this.D_DATA_RIF = JdbcWritableBridge.readString(27, __dbResults);
    this.T_TIPO_CONFIGURAZIONE = JdbcWritableBridge.readString(28, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_MISURATORE_2G = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_POD = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.B_VIS_FASCE = JdbcWritableBridge.readString(3, __dbResults);
    this.B_VIS_VENDITORE = JdbcWritableBridge.readString(4, __dbResults);
    this.B_VIS_TELEFONOV = JdbcWritableBridge.readString(5, __dbResults);
    this.B_VIS_DATAINICONTR = JdbcWritableBridge.readString(6, __dbResults);
    this.B_VIS_DATAINIZIOFREEZING = JdbcWritableBridge.readString(7, __dbResults);
    this.B_VIS_MESSAGGICLIENTE = JdbcWritableBridge.readString(8, __dbResults);
    this.B_VIS_CODCLI = JdbcWritableBridge.readString(9, __dbResults);
    this.T_CODCLI = JdbcWritableBridge.readString(10, __dbResults);
    this.T_VENDITORE = JdbcWritableBridge.readString(11, __dbResults);
    this.T_TELEFONOV = JdbcWritableBridge.readString(12, __dbResults);
    this.D_DATA_INICONTR = JdbcWritableBridge.readString(13, __dbResults);
    this.D_DATA_INIZIOFREEZING = JdbcWritableBridge.readString(14, __dbResults);
    this.T_MESSAGGIO_CLIENTE_1 = JdbcWritableBridge.readString(15, __dbResults);
    this.T_MESSAGGIO_CLIENTE_2 = JdbcWritableBridge.readString(16, __dbResults);
    this.T_MESSAGGIO_CLIENTE_3 = JdbcWritableBridge.readString(17, __dbResults);
    this.T_MESSAGGIO_CLIENTE_4 = JdbcWritableBridge.readString(18, __dbResults);
    this.T_MESSAGGIO_CLIENTE_5 = JdbcWritableBridge.readString(19, __dbResults);
    this.N_NUM_FASCE = JdbcWritableBridge.readBigDecimal(20, __dbResults);
    this.D_INIZIO_VALIDITA = JdbcWritableBridge.readString(21, __dbResults);
    this.D_FINE_VALIDITA = JdbcWritableBridge.readString(22, __dbResults);
    this.T_NOTA = JdbcWritableBridge.readString(23, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(24, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(25, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(26, __dbResults);
    this.D_DATA_RIF = JdbcWritableBridge.readString(27, __dbResults);
    this.T_TIPO_CONFIGURAZIONE = JdbcWritableBridge.readString(28, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_MISURATORE_2G, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_POD, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_VIS_FASCE, 3 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_VIS_VENDITORE, 4 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_VIS_TELEFONOV, 5 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_VIS_DATAINICONTR, 6 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_VIS_DATAINIZIOFREEZING, 7 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_VIS_MESSAGGICLIENTE, 8 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_VIS_CODCLI, 9 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_CODCLI, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_VENDITORE, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TELEFONOV, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INICONTR, 13 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INIZIOFREEZING, 14 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_MESSAGGIO_CLIENTE_1, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MESSAGGIO_CLIENTE_2, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MESSAGGIO_CLIENTE_3, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MESSAGGIO_CLIENTE_4, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MESSAGGIO_CLIENTE_5, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_FASCE, 20 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_INIZIO_VALIDITA, 21 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_FINE_VALIDITA, 22 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTA, 23 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 24 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 25 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 26 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF, 27 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_CONFIGURAZIONE, 28 + __off, 1, __dbStmt);
    return 28;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_MISURATORE_2G, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_POD, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_VIS_FASCE, 3 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_VIS_VENDITORE, 4 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_VIS_TELEFONOV, 5 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_VIS_DATAINICONTR, 6 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_VIS_DATAINIZIOFREEZING, 7 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_VIS_MESSAGGICLIENTE, 8 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_VIS_CODCLI, 9 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_CODCLI, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_VENDITORE, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TELEFONOV, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INICONTR, 13 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INIZIOFREEZING, 14 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_MESSAGGIO_CLIENTE_1, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MESSAGGIO_CLIENTE_2, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MESSAGGIO_CLIENTE_3, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MESSAGGIO_CLIENTE_4, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_MESSAGGIO_CLIENTE_5, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_NUM_FASCE, 20 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_INIZIO_VALIDITA, 21 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_FINE_VALIDITA, 22 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTA, 23 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 24 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 25 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 26 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF, 27 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_CONFIGURAZIONE, 28 + __off, 1, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_MISURATORE_2G = null;
    } else {
    this.N_ID_MISURATORE_2G = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_POD = null;
    } else {
    this.N_ID_POD = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_VIS_FASCE = null;
    } else {
    this.B_VIS_FASCE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_VIS_VENDITORE = null;
    } else {
    this.B_VIS_VENDITORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_VIS_TELEFONOV = null;
    } else {
    this.B_VIS_TELEFONOV = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_VIS_DATAINICONTR = null;
    } else {
    this.B_VIS_DATAINICONTR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_VIS_DATAINIZIOFREEZING = null;
    } else {
    this.B_VIS_DATAINIZIOFREEZING = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_VIS_MESSAGGICLIENTE = null;
    } else {
    this.B_VIS_MESSAGGICLIENTE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_VIS_CODCLI = null;
    } else {
    this.B_VIS_CODCLI = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODCLI = null;
    } else {
    this.T_CODCLI = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_VENDITORE = null;
    } else {
    this.T_VENDITORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TELEFONOV = null;
    } else {
    this.T_TELEFONOV = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_INICONTR = null;
    } else {
    this.D_DATA_INICONTR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_INIZIOFREEZING = null;
    } else {
    this.D_DATA_INIZIOFREEZING = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MESSAGGIO_CLIENTE_1 = null;
    } else {
    this.T_MESSAGGIO_CLIENTE_1 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MESSAGGIO_CLIENTE_2 = null;
    } else {
    this.T_MESSAGGIO_CLIENTE_2 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MESSAGGIO_CLIENTE_3 = null;
    } else {
    this.T_MESSAGGIO_CLIENTE_3 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MESSAGGIO_CLIENTE_4 = null;
    } else {
    this.T_MESSAGGIO_CLIENTE_4 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_MESSAGGIO_CLIENTE_5 = null;
    } else {
    this.T_MESSAGGIO_CLIENTE_5 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_NUM_FASCE = null;
    } else {
    this.N_NUM_FASCE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_INIZIO_VALIDITA = null;
    } else {
    this.D_INIZIO_VALIDITA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_FINE_VALIDITA = null;
    } else {
    this.D_FINE_VALIDITA = Text.readString(__dataIn);
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
        this.D_DATA_RIF = null;
    } else {
    this.D_DATA_RIF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO_CONFIGURAZIONE = null;
    } else {
    this.T_TIPO_CONFIGURAZIONE = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_MISURATORE_2G) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_MISURATORE_2G, __dataOut);
    }
    if (null == this.N_ID_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_POD, __dataOut);
    }
    if (null == this.B_VIS_FASCE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_VIS_FASCE);
    }
    if (null == this.B_VIS_VENDITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_VIS_VENDITORE);
    }
    if (null == this.B_VIS_TELEFONOV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_VIS_TELEFONOV);
    }
    if (null == this.B_VIS_DATAINICONTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_VIS_DATAINICONTR);
    }
    if (null == this.B_VIS_DATAINIZIOFREEZING) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_VIS_DATAINIZIOFREEZING);
    }
    if (null == this.B_VIS_MESSAGGICLIENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_VIS_MESSAGGICLIENTE);
    }
    if (null == this.B_VIS_CODCLI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_VIS_CODCLI);
    }
    if (null == this.T_CODCLI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODCLI);
    }
    if (null == this.T_VENDITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_VENDITORE);
    }
    if (null == this.T_TELEFONOV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TELEFONOV);
    }
    if (null == this.D_DATA_INICONTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INICONTR);
    }
    if (null == this.D_DATA_INIZIOFREEZING) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INIZIOFREEZING);
    }
    if (null == this.T_MESSAGGIO_CLIENTE_1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MESSAGGIO_CLIENTE_1);
    }
    if (null == this.T_MESSAGGIO_CLIENTE_2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MESSAGGIO_CLIENTE_2);
    }
    if (null == this.T_MESSAGGIO_CLIENTE_3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MESSAGGIO_CLIENTE_3);
    }
    if (null == this.T_MESSAGGIO_CLIENTE_4) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MESSAGGIO_CLIENTE_4);
    }
    if (null == this.T_MESSAGGIO_CLIENTE_5) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MESSAGGIO_CLIENTE_5);
    }
    if (null == this.N_NUM_FASCE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_FASCE, __dataOut);
    }
    if (null == this.D_INIZIO_VALIDITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_INIZIO_VALIDITA);
    }
    if (null == this.D_FINE_VALIDITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_FINE_VALIDITA);
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
    if (null == this.D_DATA_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF);
    }
    if (null == this.T_TIPO_CONFIGURAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_CONFIGURAZIONE);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_MISURATORE_2G) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_MISURATORE_2G, __dataOut);
    }
    if (null == this.N_ID_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_POD, __dataOut);
    }
    if (null == this.B_VIS_FASCE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_VIS_FASCE);
    }
    if (null == this.B_VIS_VENDITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_VIS_VENDITORE);
    }
    if (null == this.B_VIS_TELEFONOV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_VIS_TELEFONOV);
    }
    if (null == this.B_VIS_DATAINICONTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_VIS_DATAINICONTR);
    }
    if (null == this.B_VIS_DATAINIZIOFREEZING) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_VIS_DATAINIZIOFREEZING);
    }
    if (null == this.B_VIS_MESSAGGICLIENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_VIS_MESSAGGICLIENTE);
    }
    if (null == this.B_VIS_CODCLI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_VIS_CODCLI);
    }
    if (null == this.T_CODCLI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODCLI);
    }
    if (null == this.T_VENDITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_VENDITORE);
    }
    if (null == this.T_TELEFONOV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TELEFONOV);
    }
    if (null == this.D_DATA_INICONTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INICONTR);
    }
    if (null == this.D_DATA_INIZIOFREEZING) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INIZIOFREEZING);
    }
    if (null == this.T_MESSAGGIO_CLIENTE_1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MESSAGGIO_CLIENTE_1);
    }
    if (null == this.T_MESSAGGIO_CLIENTE_2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MESSAGGIO_CLIENTE_2);
    }
    if (null == this.T_MESSAGGIO_CLIENTE_3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MESSAGGIO_CLIENTE_3);
    }
    if (null == this.T_MESSAGGIO_CLIENTE_4) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MESSAGGIO_CLIENTE_4);
    }
    if (null == this.T_MESSAGGIO_CLIENTE_5) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_MESSAGGIO_CLIENTE_5);
    }
    if (null == this.N_NUM_FASCE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_NUM_FASCE, __dataOut);
    }
    if (null == this.D_INIZIO_VALIDITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_INIZIO_VALIDITA);
    }
    if (null == this.D_FINE_VALIDITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_FINE_VALIDITA);
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
    if (null == this.D_DATA_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF);
    }
    if (null == this.T_TIPO_CONFIGURAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_CONFIGURAZIONE);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_MISURATORE_2G==null?"":N_ID_MISURATORE_2G.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_POD==null?"":N_ID_POD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_VIS_FASCE==null?"":B_VIS_FASCE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_VIS_VENDITORE==null?"":B_VIS_VENDITORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_VIS_TELEFONOV==null?"":B_VIS_TELEFONOV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_VIS_DATAINICONTR==null?"":B_VIS_DATAINICONTR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_VIS_DATAINIZIOFREEZING==null?"":B_VIS_DATAINIZIOFREEZING, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_VIS_MESSAGGICLIENTE==null?"":B_VIS_MESSAGGICLIENTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_VIS_CODCLI==null?"":B_VIS_CODCLI, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODCLI==null?"":T_CODCLI, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_VENDITORE==null?"":T_VENDITORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TELEFONOV==null?"":T_TELEFONOV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INICONTR==null?"":D_DATA_INICONTR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INIZIOFREEZING==null?"":D_DATA_INIZIOFREEZING, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MESSAGGIO_CLIENTE_1==null?"":T_MESSAGGIO_CLIENTE_1, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MESSAGGIO_CLIENTE_2==null?"":T_MESSAGGIO_CLIENTE_2, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MESSAGGIO_CLIENTE_3==null?"":T_MESSAGGIO_CLIENTE_3, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MESSAGGIO_CLIENTE_4==null?"":T_MESSAGGIO_CLIENTE_4, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MESSAGGIO_CLIENTE_5==null?"":T_MESSAGGIO_CLIENTE_5, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_FASCE==null?"":N_NUM_FASCE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_INIZIO_VALIDITA==null?"":D_INIZIO_VALIDITA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_FINE_VALIDITA==null?"":D_FINE_VALIDITA, " ", delimiters));
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
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF==null?"":D_DATA_RIF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_CONFIGURAZIONE==null?"":T_TIPO_CONFIGURAZIONE, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_MISURATORE_2G==null?"":N_ID_MISURATORE_2G.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_POD==null?"":N_ID_POD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_VIS_FASCE==null?"":B_VIS_FASCE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_VIS_VENDITORE==null?"":B_VIS_VENDITORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_VIS_TELEFONOV==null?"":B_VIS_TELEFONOV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_VIS_DATAINICONTR==null?"":B_VIS_DATAINICONTR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_VIS_DATAINIZIOFREEZING==null?"":B_VIS_DATAINIZIOFREEZING, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_VIS_MESSAGGICLIENTE==null?"":B_VIS_MESSAGGICLIENTE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_VIS_CODCLI==null?"":B_VIS_CODCLI, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODCLI==null?"":T_CODCLI, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_VENDITORE==null?"":T_VENDITORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TELEFONOV==null?"":T_TELEFONOV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INICONTR==null?"":D_DATA_INICONTR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INIZIOFREEZING==null?"":D_DATA_INIZIOFREEZING, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MESSAGGIO_CLIENTE_1==null?"":T_MESSAGGIO_CLIENTE_1, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MESSAGGIO_CLIENTE_2==null?"":T_MESSAGGIO_CLIENTE_2, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MESSAGGIO_CLIENTE_3==null?"":T_MESSAGGIO_CLIENTE_3, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MESSAGGIO_CLIENTE_4==null?"":T_MESSAGGIO_CLIENTE_4, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_MESSAGGIO_CLIENTE_5==null?"":T_MESSAGGIO_CLIENTE_5, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_NUM_FASCE==null?"":N_NUM_FASCE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_INIZIO_VALIDITA==null?"":D_INIZIO_VALIDITA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_FINE_VALIDITA==null?"":D_FINE_VALIDITA, " ", delimiters));
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
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF==null?"":D_DATA_RIF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_CONFIGURAZIONE==null?"":T_TIPO_CONFIGURAZIONE, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_MISURATORE_2G = null; } else {
      this.N_ID_MISURATORE_2G = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_POD = null; } else {
      this.N_ID_POD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_VIS_FASCE = null; } else {
      this.B_VIS_FASCE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_VIS_VENDITORE = null; } else {
      this.B_VIS_VENDITORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_VIS_TELEFONOV = null; } else {
      this.B_VIS_TELEFONOV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_VIS_DATAINICONTR = null; } else {
      this.B_VIS_DATAINICONTR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_VIS_DATAINIZIOFREEZING = null; } else {
      this.B_VIS_DATAINIZIOFREEZING = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_VIS_MESSAGGICLIENTE = null; } else {
      this.B_VIS_MESSAGGICLIENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_VIS_CODCLI = null; } else {
      this.B_VIS_CODCLI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODCLI = null; } else {
      this.T_CODCLI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_VENDITORE = null; } else {
      this.T_VENDITORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TELEFONOV = null; } else {
      this.T_TELEFONOV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INICONTR = null; } else {
      this.D_DATA_INICONTR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INIZIOFREEZING = null; } else {
      this.D_DATA_INIZIOFREEZING = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MESSAGGIO_CLIENTE_1 = null; } else {
      this.T_MESSAGGIO_CLIENTE_1 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MESSAGGIO_CLIENTE_2 = null; } else {
      this.T_MESSAGGIO_CLIENTE_2 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MESSAGGIO_CLIENTE_3 = null; } else {
      this.T_MESSAGGIO_CLIENTE_3 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MESSAGGIO_CLIENTE_4 = null; } else {
      this.T_MESSAGGIO_CLIENTE_4 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MESSAGGIO_CLIENTE_5 = null; } else {
      this.T_MESSAGGIO_CLIENTE_5 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_FASCE = null; } else {
      this.N_NUM_FASCE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_INIZIO_VALIDITA = null; } else {
      this.D_INIZIO_VALIDITA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_FINE_VALIDITA = null; } else {
      this.D_FINE_VALIDITA = __cur_str;
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
    if (__cur_str.equals("null")) { this.D_DATA_RIF = null; } else {
      this.D_DATA_RIF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_CONFIGURAZIONE = null; } else {
      this.T_TIPO_CONFIGURAZIONE = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_MISURATORE_2G = null; } else {
      this.N_ID_MISURATORE_2G = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_POD = null; } else {
      this.N_ID_POD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_VIS_FASCE = null; } else {
      this.B_VIS_FASCE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_VIS_VENDITORE = null; } else {
      this.B_VIS_VENDITORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_VIS_TELEFONOV = null; } else {
      this.B_VIS_TELEFONOV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_VIS_DATAINICONTR = null; } else {
      this.B_VIS_DATAINICONTR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_VIS_DATAINIZIOFREEZING = null; } else {
      this.B_VIS_DATAINIZIOFREEZING = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_VIS_MESSAGGICLIENTE = null; } else {
      this.B_VIS_MESSAGGICLIENTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_VIS_CODCLI = null; } else {
      this.B_VIS_CODCLI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODCLI = null; } else {
      this.T_CODCLI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_VENDITORE = null; } else {
      this.T_VENDITORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TELEFONOV = null; } else {
      this.T_TELEFONOV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INICONTR = null; } else {
      this.D_DATA_INICONTR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INIZIOFREEZING = null; } else {
      this.D_DATA_INIZIOFREEZING = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MESSAGGIO_CLIENTE_1 = null; } else {
      this.T_MESSAGGIO_CLIENTE_1 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MESSAGGIO_CLIENTE_2 = null; } else {
      this.T_MESSAGGIO_CLIENTE_2 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MESSAGGIO_CLIENTE_3 = null; } else {
      this.T_MESSAGGIO_CLIENTE_3 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MESSAGGIO_CLIENTE_4 = null; } else {
      this.T_MESSAGGIO_CLIENTE_4 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_MESSAGGIO_CLIENTE_5 = null; } else {
      this.T_MESSAGGIO_CLIENTE_5 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_NUM_FASCE = null; } else {
      this.N_NUM_FASCE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_INIZIO_VALIDITA = null; } else {
      this.D_INIZIO_VALIDITA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_FINE_VALIDITA = null; } else {
      this.D_FINE_VALIDITA = __cur_str;
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
    if (__cur_str.equals("null")) { this.D_DATA_RIF = null; } else {
      this.D_DATA_RIF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_CONFIGURAZIONE = null; } else {
      this.T_TIPO_CONFIGURAZIONE = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    rcu_rcu_misuratore_2g o = (rcu_rcu_misuratore_2g) super.clone();
    return o;
  }

  public void clone0(rcu_rcu_misuratore_2g o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_MISURATORE_2G", this.N_ID_MISURATORE_2G);
    __sqoop$field_map.put("N_ID_POD", this.N_ID_POD);
    __sqoop$field_map.put("B_VIS_FASCE", this.B_VIS_FASCE);
    __sqoop$field_map.put("B_VIS_VENDITORE", this.B_VIS_VENDITORE);
    __sqoop$field_map.put("B_VIS_TELEFONOV", this.B_VIS_TELEFONOV);
    __sqoop$field_map.put("B_VIS_DATAINICONTR", this.B_VIS_DATAINICONTR);
    __sqoop$field_map.put("B_VIS_DATAINIZIOFREEZING", this.B_VIS_DATAINIZIOFREEZING);
    __sqoop$field_map.put("B_VIS_MESSAGGICLIENTE", this.B_VIS_MESSAGGICLIENTE);
    __sqoop$field_map.put("B_VIS_CODCLI", this.B_VIS_CODCLI);
    __sqoop$field_map.put("T_CODCLI", this.T_CODCLI);
    __sqoop$field_map.put("T_VENDITORE", this.T_VENDITORE);
    __sqoop$field_map.put("T_TELEFONOV", this.T_TELEFONOV);
    __sqoop$field_map.put("D_DATA_INICONTR", this.D_DATA_INICONTR);
    __sqoop$field_map.put("D_DATA_INIZIOFREEZING", this.D_DATA_INIZIOFREEZING);
    __sqoop$field_map.put("T_MESSAGGIO_CLIENTE_1", this.T_MESSAGGIO_CLIENTE_1);
    __sqoop$field_map.put("T_MESSAGGIO_CLIENTE_2", this.T_MESSAGGIO_CLIENTE_2);
    __sqoop$field_map.put("T_MESSAGGIO_CLIENTE_3", this.T_MESSAGGIO_CLIENTE_3);
    __sqoop$field_map.put("T_MESSAGGIO_CLIENTE_4", this.T_MESSAGGIO_CLIENTE_4);
    __sqoop$field_map.put("T_MESSAGGIO_CLIENTE_5", this.T_MESSAGGIO_CLIENTE_5);
    __sqoop$field_map.put("N_NUM_FASCE", this.N_NUM_FASCE);
    __sqoop$field_map.put("D_INIZIO_VALIDITA", this.D_INIZIO_VALIDITA);
    __sqoop$field_map.put("D_FINE_VALIDITA", this.D_FINE_VALIDITA);
    __sqoop$field_map.put("T_NOTA", this.T_NOTA);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("D_DATA_RIF", this.D_DATA_RIF);
    __sqoop$field_map.put("T_TIPO_CONFIGURAZIONE", this.T_TIPO_CONFIGURAZIONE);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_MISURATORE_2G", this.N_ID_MISURATORE_2G);
    __sqoop$field_map.put("N_ID_POD", this.N_ID_POD);
    __sqoop$field_map.put("B_VIS_FASCE", this.B_VIS_FASCE);
    __sqoop$field_map.put("B_VIS_VENDITORE", this.B_VIS_VENDITORE);
    __sqoop$field_map.put("B_VIS_TELEFONOV", this.B_VIS_TELEFONOV);
    __sqoop$field_map.put("B_VIS_DATAINICONTR", this.B_VIS_DATAINICONTR);
    __sqoop$field_map.put("B_VIS_DATAINIZIOFREEZING", this.B_VIS_DATAINIZIOFREEZING);
    __sqoop$field_map.put("B_VIS_MESSAGGICLIENTE", this.B_VIS_MESSAGGICLIENTE);
    __sqoop$field_map.put("B_VIS_CODCLI", this.B_VIS_CODCLI);
    __sqoop$field_map.put("T_CODCLI", this.T_CODCLI);
    __sqoop$field_map.put("T_VENDITORE", this.T_VENDITORE);
    __sqoop$field_map.put("T_TELEFONOV", this.T_TELEFONOV);
    __sqoop$field_map.put("D_DATA_INICONTR", this.D_DATA_INICONTR);
    __sqoop$field_map.put("D_DATA_INIZIOFREEZING", this.D_DATA_INIZIOFREEZING);
    __sqoop$field_map.put("T_MESSAGGIO_CLIENTE_1", this.T_MESSAGGIO_CLIENTE_1);
    __sqoop$field_map.put("T_MESSAGGIO_CLIENTE_2", this.T_MESSAGGIO_CLIENTE_2);
    __sqoop$field_map.put("T_MESSAGGIO_CLIENTE_3", this.T_MESSAGGIO_CLIENTE_3);
    __sqoop$field_map.put("T_MESSAGGIO_CLIENTE_4", this.T_MESSAGGIO_CLIENTE_4);
    __sqoop$field_map.put("T_MESSAGGIO_CLIENTE_5", this.T_MESSAGGIO_CLIENTE_5);
    __sqoop$field_map.put("N_NUM_FASCE", this.N_NUM_FASCE);
    __sqoop$field_map.put("D_INIZIO_VALIDITA", this.D_INIZIO_VALIDITA);
    __sqoop$field_map.put("D_FINE_VALIDITA", this.D_FINE_VALIDITA);
    __sqoop$field_map.put("T_NOTA", this.T_NOTA);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("D_DATA_RIF", this.D_DATA_RIF);
    __sqoop$field_map.put("T_TIPO_CONFIGURAZIONE", this.T_TIPO_CONFIGURAZIONE);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
