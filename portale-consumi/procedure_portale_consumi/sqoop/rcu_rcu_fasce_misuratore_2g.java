// ORM class for table 'rcu.rcu_fasce_misuratore_2g'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 09:43:38 CEST 2019
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

public class rcu_rcu_fasce_misuratore_2g extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_FASCE_MISURATORE_2G", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_FASCE_MISURATORE_2G = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_MISURATORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_MISURATORE = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_COD_GIORNO_2G", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_COD_GIORNO_2G = (java.math.BigDecimal)value;
      }
    });
    setters.put("D_DATA_GIORNO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_GIORNO = (String)value;
      }
    });
    setters.put("N_FASCIA_1", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_FASCIA_1 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_FINE_FASCIA_1", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_FINE_FASCIA_1 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_FASCIA_2", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_FASCIA_2 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_FINE_FASCIA_2", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_FINE_FASCIA_2 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_FASCIA_3", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_FASCIA_3 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_FINE_FASCIA_3", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_FINE_FASCIA_3 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_FASCIA_4", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_FASCIA_4 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_FINE_FASCIA_4", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_FINE_FASCIA_4 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_FASCIA_5", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_FASCIA_5 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_FINE_FASCIA_5", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_FINE_FASCIA_5 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_FASCIA_6", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_FASCIA_6 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_FINE_FASCIA_6", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_FINE_FASCIA_6 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_FASCIA_7", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_FASCIA_7 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_FINE_FASCIA_7", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_FINE_FASCIA_7 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_FASCIA_8", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_FASCIA_8 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_FINE_FASCIA_8", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_FINE_FASCIA_8 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_FASCIA_9", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_FASCIA_9 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_FINE_FASCIA_9", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_FINE_FASCIA_9 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_FASCIA_10", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_FASCIA_10 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_FINE_FASCIA_10", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_FINE_FASCIA_10 = (java.math.BigDecimal)value;
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
  }
  public rcu_rcu_fasce_misuratore_2g() {
    init0();
  }
  private java.math.BigDecimal N_ID_FASCE_MISURATORE_2G;
  public java.math.BigDecimal get_N_ID_FASCE_MISURATORE_2G() {
    return N_ID_FASCE_MISURATORE_2G;
  }
  public void set_N_ID_FASCE_MISURATORE_2G(java.math.BigDecimal N_ID_FASCE_MISURATORE_2G) {
    this.N_ID_FASCE_MISURATORE_2G = N_ID_FASCE_MISURATORE_2G;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_ID_FASCE_MISURATORE_2G(java.math.BigDecimal N_ID_FASCE_MISURATORE_2G) {
    this.N_ID_FASCE_MISURATORE_2G = N_ID_FASCE_MISURATORE_2G;
    return this;
  }
  private java.math.BigDecimal N_ID_MISURATORE;
  public java.math.BigDecimal get_N_ID_MISURATORE() {
    return N_ID_MISURATORE;
  }
  public void set_N_ID_MISURATORE(java.math.BigDecimal N_ID_MISURATORE) {
    this.N_ID_MISURATORE = N_ID_MISURATORE;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_ID_MISURATORE(java.math.BigDecimal N_ID_MISURATORE) {
    this.N_ID_MISURATORE = N_ID_MISURATORE;
    return this;
  }
  private java.math.BigDecimal N_COD_GIORNO_2G;
  public java.math.BigDecimal get_N_COD_GIORNO_2G() {
    return N_COD_GIORNO_2G;
  }
  public void set_N_COD_GIORNO_2G(java.math.BigDecimal N_COD_GIORNO_2G) {
    this.N_COD_GIORNO_2G = N_COD_GIORNO_2G;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_COD_GIORNO_2G(java.math.BigDecimal N_COD_GIORNO_2G) {
    this.N_COD_GIORNO_2G = N_COD_GIORNO_2G;
    return this;
  }
  private String D_DATA_GIORNO;
  public String get_D_DATA_GIORNO() {
    return D_DATA_GIORNO;
  }
  public void set_D_DATA_GIORNO(String D_DATA_GIORNO) {
    this.D_DATA_GIORNO = D_DATA_GIORNO;
  }
  public rcu_rcu_fasce_misuratore_2g with_D_DATA_GIORNO(String D_DATA_GIORNO) {
    this.D_DATA_GIORNO = D_DATA_GIORNO;
    return this;
  }
  private java.math.BigDecimal N_FASCIA_1;
  public java.math.BigDecimal get_N_FASCIA_1() {
    return N_FASCIA_1;
  }
  public void set_N_FASCIA_1(java.math.BigDecimal N_FASCIA_1) {
    this.N_FASCIA_1 = N_FASCIA_1;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_FASCIA_1(java.math.BigDecimal N_FASCIA_1) {
    this.N_FASCIA_1 = N_FASCIA_1;
    return this;
  }
  private java.math.BigDecimal N_FINE_FASCIA_1;
  public java.math.BigDecimal get_N_FINE_FASCIA_1() {
    return N_FINE_FASCIA_1;
  }
  public void set_N_FINE_FASCIA_1(java.math.BigDecimal N_FINE_FASCIA_1) {
    this.N_FINE_FASCIA_1 = N_FINE_FASCIA_1;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_FINE_FASCIA_1(java.math.BigDecimal N_FINE_FASCIA_1) {
    this.N_FINE_FASCIA_1 = N_FINE_FASCIA_1;
    return this;
  }
  private java.math.BigDecimal N_FASCIA_2;
  public java.math.BigDecimal get_N_FASCIA_2() {
    return N_FASCIA_2;
  }
  public void set_N_FASCIA_2(java.math.BigDecimal N_FASCIA_2) {
    this.N_FASCIA_2 = N_FASCIA_2;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_FASCIA_2(java.math.BigDecimal N_FASCIA_2) {
    this.N_FASCIA_2 = N_FASCIA_2;
    return this;
  }
  private java.math.BigDecimal N_FINE_FASCIA_2;
  public java.math.BigDecimal get_N_FINE_FASCIA_2() {
    return N_FINE_FASCIA_2;
  }
  public void set_N_FINE_FASCIA_2(java.math.BigDecimal N_FINE_FASCIA_2) {
    this.N_FINE_FASCIA_2 = N_FINE_FASCIA_2;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_FINE_FASCIA_2(java.math.BigDecimal N_FINE_FASCIA_2) {
    this.N_FINE_FASCIA_2 = N_FINE_FASCIA_2;
    return this;
  }
  private java.math.BigDecimal N_FASCIA_3;
  public java.math.BigDecimal get_N_FASCIA_3() {
    return N_FASCIA_3;
  }
  public void set_N_FASCIA_3(java.math.BigDecimal N_FASCIA_3) {
    this.N_FASCIA_3 = N_FASCIA_3;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_FASCIA_3(java.math.BigDecimal N_FASCIA_3) {
    this.N_FASCIA_3 = N_FASCIA_3;
    return this;
  }
  private java.math.BigDecimal N_FINE_FASCIA_3;
  public java.math.BigDecimal get_N_FINE_FASCIA_3() {
    return N_FINE_FASCIA_3;
  }
  public void set_N_FINE_FASCIA_3(java.math.BigDecimal N_FINE_FASCIA_3) {
    this.N_FINE_FASCIA_3 = N_FINE_FASCIA_3;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_FINE_FASCIA_3(java.math.BigDecimal N_FINE_FASCIA_3) {
    this.N_FINE_FASCIA_3 = N_FINE_FASCIA_3;
    return this;
  }
  private java.math.BigDecimal N_FASCIA_4;
  public java.math.BigDecimal get_N_FASCIA_4() {
    return N_FASCIA_4;
  }
  public void set_N_FASCIA_4(java.math.BigDecimal N_FASCIA_4) {
    this.N_FASCIA_4 = N_FASCIA_4;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_FASCIA_4(java.math.BigDecimal N_FASCIA_4) {
    this.N_FASCIA_4 = N_FASCIA_4;
    return this;
  }
  private java.math.BigDecimal N_FINE_FASCIA_4;
  public java.math.BigDecimal get_N_FINE_FASCIA_4() {
    return N_FINE_FASCIA_4;
  }
  public void set_N_FINE_FASCIA_4(java.math.BigDecimal N_FINE_FASCIA_4) {
    this.N_FINE_FASCIA_4 = N_FINE_FASCIA_4;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_FINE_FASCIA_4(java.math.BigDecimal N_FINE_FASCIA_4) {
    this.N_FINE_FASCIA_4 = N_FINE_FASCIA_4;
    return this;
  }
  private java.math.BigDecimal N_FASCIA_5;
  public java.math.BigDecimal get_N_FASCIA_5() {
    return N_FASCIA_5;
  }
  public void set_N_FASCIA_5(java.math.BigDecimal N_FASCIA_5) {
    this.N_FASCIA_5 = N_FASCIA_5;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_FASCIA_5(java.math.BigDecimal N_FASCIA_5) {
    this.N_FASCIA_5 = N_FASCIA_5;
    return this;
  }
  private java.math.BigDecimal N_FINE_FASCIA_5;
  public java.math.BigDecimal get_N_FINE_FASCIA_5() {
    return N_FINE_FASCIA_5;
  }
  public void set_N_FINE_FASCIA_5(java.math.BigDecimal N_FINE_FASCIA_5) {
    this.N_FINE_FASCIA_5 = N_FINE_FASCIA_5;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_FINE_FASCIA_5(java.math.BigDecimal N_FINE_FASCIA_5) {
    this.N_FINE_FASCIA_5 = N_FINE_FASCIA_5;
    return this;
  }
  private java.math.BigDecimal N_FASCIA_6;
  public java.math.BigDecimal get_N_FASCIA_6() {
    return N_FASCIA_6;
  }
  public void set_N_FASCIA_6(java.math.BigDecimal N_FASCIA_6) {
    this.N_FASCIA_6 = N_FASCIA_6;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_FASCIA_6(java.math.BigDecimal N_FASCIA_6) {
    this.N_FASCIA_6 = N_FASCIA_6;
    return this;
  }
  private java.math.BigDecimal N_FINE_FASCIA_6;
  public java.math.BigDecimal get_N_FINE_FASCIA_6() {
    return N_FINE_FASCIA_6;
  }
  public void set_N_FINE_FASCIA_6(java.math.BigDecimal N_FINE_FASCIA_6) {
    this.N_FINE_FASCIA_6 = N_FINE_FASCIA_6;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_FINE_FASCIA_6(java.math.BigDecimal N_FINE_FASCIA_6) {
    this.N_FINE_FASCIA_6 = N_FINE_FASCIA_6;
    return this;
  }
  private java.math.BigDecimal N_FASCIA_7;
  public java.math.BigDecimal get_N_FASCIA_7() {
    return N_FASCIA_7;
  }
  public void set_N_FASCIA_7(java.math.BigDecimal N_FASCIA_7) {
    this.N_FASCIA_7 = N_FASCIA_7;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_FASCIA_7(java.math.BigDecimal N_FASCIA_7) {
    this.N_FASCIA_7 = N_FASCIA_7;
    return this;
  }
  private java.math.BigDecimal N_FINE_FASCIA_7;
  public java.math.BigDecimal get_N_FINE_FASCIA_7() {
    return N_FINE_FASCIA_7;
  }
  public void set_N_FINE_FASCIA_7(java.math.BigDecimal N_FINE_FASCIA_7) {
    this.N_FINE_FASCIA_7 = N_FINE_FASCIA_7;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_FINE_FASCIA_7(java.math.BigDecimal N_FINE_FASCIA_7) {
    this.N_FINE_FASCIA_7 = N_FINE_FASCIA_7;
    return this;
  }
  private java.math.BigDecimal N_FASCIA_8;
  public java.math.BigDecimal get_N_FASCIA_8() {
    return N_FASCIA_8;
  }
  public void set_N_FASCIA_8(java.math.BigDecimal N_FASCIA_8) {
    this.N_FASCIA_8 = N_FASCIA_8;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_FASCIA_8(java.math.BigDecimal N_FASCIA_8) {
    this.N_FASCIA_8 = N_FASCIA_8;
    return this;
  }
  private java.math.BigDecimal N_FINE_FASCIA_8;
  public java.math.BigDecimal get_N_FINE_FASCIA_8() {
    return N_FINE_FASCIA_8;
  }
  public void set_N_FINE_FASCIA_8(java.math.BigDecimal N_FINE_FASCIA_8) {
    this.N_FINE_FASCIA_8 = N_FINE_FASCIA_8;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_FINE_FASCIA_8(java.math.BigDecimal N_FINE_FASCIA_8) {
    this.N_FINE_FASCIA_8 = N_FINE_FASCIA_8;
    return this;
  }
  private java.math.BigDecimal N_FASCIA_9;
  public java.math.BigDecimal get_N_FASCIA_9() {
    return N_FASCIA_9;
  }
  public void set_N_FASCIA_9(java.math.BigDecimal N_FASCIA_9) {
    this.N_FASCIA_9 = N_FASCIA_9;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_FASCIA_9(java.math.BigDecimal N_FASCIA_9) {
    this.N_FASCIA_9 = N_FASCIA_9;
    return this;
  }
  private java.math.BigDecimal N_FINE_FASCIA_9;
  public java.math.BigDecimal get_N_FINE_FASCIA_9() {
    return N_FINE_FASCIA_9;
  }
  public void set_N_FINE_FASCIA_9(java.math.BigDecimal N_FINE_FASCIA_9) {
    this.N_FINE_FASCIA_9 = N_FINE_FASCIA_9;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_FINE_FASCIA_9(java.math.BigDecimal N_FINE_FASCIA_9) {
    this.N_FINE_FASCIA_9 = N_FINE_FASCIA_9;
    return this;
  }
  private java.math.BigDecimal N_FASCIA_10;
  public java.math.BigDecimal get_N_FASCIA_10() {
    return N_FASCIA_10;
  }
  public void set_N_FASCIA_10(java.math.BigDecimal N_FASCIA_10) {
    this.N_FASCIA_10 = N_FASCIA_10;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_FASCIA_10(java.math.BigDecimal N_FASCIA_10) {
    this.N_FASCIA_10 = N_FASCIA_10;
    return this;
  }
  private java.math.BigDecimal N_FINE_FASCIA_10;
  public java.math.BigDecimal get_N_FINE_FASCIA_10() {
    return N_FINE_FASCIA_10;
  }
  public void set_N_FINE_FASCIA_10(java.math.BigDecimal N_FINE_FASCIA_10) {
    this.N_FINE_FASCIA_10 = N_FINE_FASCIA_10;
  }
  public rcu_rcu_fasce_misuratore_2g with_N_FINE_FASCIA_10(java.math.BigDecimal N_FINE_FASCIA_10) {
    this.N_FINE_FASCIA_10 = N_FINE_FASCIA_10;
    return this;
  }
  private String T_NOTA;
  public String get_T_NOTA() {
    return T_NOTA;
  }
  public void set_T_NOTA(String T_NOTA) {
    this.T_NOTA = T_NOTA;
  }
  public rcu_rcu_fasce_misuratore_2g with_T_NOTA(String T_NOTA) {
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
  public rcu_rcu_fasce_misuratore_2g with_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
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
  public rcu_rcu_fasce_misuratore_2g with_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
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
  public rcu_rcu_fasce_misuratore_2g with_N_ID_S_PREC(java.math.BigDecimal N_ID_S_PREC) {
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
  public rcu_rcu_fasce_misuratore_2g with_D_DATA_RIF(String D_DATA_RIF) {
    this.D_DATA_RIF = D_DATA_RIF;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcu_rcu_fasce_misuratore_2g)) {
      return false;
    }
    rcu_rcu_fasce_misuratore_2g that = (rcu_rcu_fasce_misuratore_2g) o;
    boolean equal = true;
    equal = equal && (this.N_ID_FASCE_MISURATORE_2G == null ? that.N_ID_FASCE_MISURATORE_2G == null : this.N_ID_FASCE_MISURATORE_2G.equals(that.N_ID_FASCE_MISURATORE_2G));
    equal = equal && (this.N_ID_MISURATORE == null ? that.N_ID_MISURATORE == null : this.N_ID_MISURATORE.equals(that.N_ID_MISURATORE));
    equal = equal && (this.N_COD_GIORNO_2G == null ? that.N_COD_GIORNO_2G == null : this.N_COD_GIORNO_2G.equals(that.N_COD_GIORNO_2G));
    equal = equal && (this.D_DATA_GIORNO == null ? that.D_DATA_GIORNO == null : this.D_DATA_GIORNO.equals(that.D_DATA_GIORNO));
    equal = equal && (this.N_FASCIA_1 == null ? that.N_FASCIA_1 == null : this.N_FASCIA_1.equals(that.N_FASCIA_1));
    equal = equal && (this.N_FINE_FASCIA_1 == null ? that.N_FINE_FASCIA_1 == null : this.N_FINE_FASCIA_1.equals(that.N_FINE_FASCIA_1));
    equal = equal && (this.N_FASCIA_2 == null ? that.N_FASCIA_2 == null : this.N_FASCIA_2.equals(that.N_FASCIA_2));
    equal = equal && (this.N_FINE_FASCIA_2 == null ? that.N_FINE_FASCIA_2 == null : this.N_FINE_FASCIA_2.equals(that.N_FINE_FASCIA_2));
    equal = equal && (this.N_FASCIA_3 == null ? that.N_FASCIA_3 == null : this.N_FASCIA_3.equals(that.N_FASCIA_3));
    equal = equal && (this.N_FINE_FASCIA_3 == null ? that.N_FINE_FASCIA_3 == null : this.N_FINE_FASCIA_3.equals(that.N_FINE_FASCIA_3));
    equal = equal && (this.N_FASCIA_4 == null ? that.N_FASCIA_4 == null : this.N_FASCIA_4.equals(that.N_FASCIA_4));
    equal = equal && (this.N_FINE_FASCIA_4 == null ? that.N_FINE_FASCIA_4 == null : this.N_FINE_FASCIA_4.equals(that.N_FINE_FASCIA_4));
    equal = equal && (this.N_FASCIA_5 == null ? that.N_FASCIA_5 == null : this.N_FASCIA_5.equals(that.N_FASCIA_5));
    equal = equal && (this.N_FINE_FASCIA_5 == null ? that.N_FINE_FASCIA_5 == null : this.N_FINE_FASCIA_5.equals(that.N_FINE_FASCIA_5));
    equal = equal && (this.N_FASCIA_6 == null ? that.N_FASCIA_6 == null : this.N_FASCIA_6.equals(that.N_FASCIA_6));
    equal = equal && (this.N_FINE_FASCIA_6 == null ? that.N_FINE_FASCIA_6 == null : this.N_FINE_FASCIA_6.equals(that.N_FINE_FASCIA_6));
    equal = equal && (this.N_FASCIA_7 == null ? that.N_FASCIA_7 == null : this.N_FASCIA_7.equals(that.N_FASCIA_7));
    equal = equal && (this.N_FINE_FASCIA_7 == null ? that.N_FINE_FASCIA_7 == null : this.N_FINE_FASCIA_7.equals(that.N_FINE_FASCIA_7));
    equal = equal && (this.N_FASCIA_8 == null ? that.N_FASCIA_8 == null : this.N_FASCIA_8.equals(that.N_FASCIA_8));
    equal = equal && (this.N_FINE_FASCIA_8 == null ? that.N_FINE_FASCIA_8 == null : this.N_FINE_FASCIA_8.equals(that.N_FINE_FASCIA_8));
    equal = equal && (this.N_FASCIA_9 == null ? that.N_FASCIA_9 == null : this.N_FASCIA_9.equals(that.N_FASCIA_9));
    equal = equal && (this.N_FINE_FASCIA_9 == null ? that.N_FINE_FASCIA_9 == null : this.N_FINE_FASCIA_9.equals(that.N_FINE_FASCIA_9));
    equal = equal && (this.N_FASCIA_10 == null ? that.N_FASCIA_10 == null : this.N_FASCIA_10.equals(that.N_FASCIA_10));
    equal = equal && (this.N_FINE_FASCIA_10 == null ? that.N_FINE_FASCIA_10 == null : this.N_FINE_FASCIA_10.equals(that.N_FINE_FASCIA_10));
    equal = equal && (this.T_NOTA == null ? that.T_NOTA == null : this.T_NOTA.equals(that.T_NOTA));
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
    if (!(o instanceof rcu_rcu_fasce_misuratore_2g)) {
      return false;
    }
    rcu_rcu_fasce_misuratore_2g that = (rcu_rcu_fasce_misuratore_2g) o;
    boolean equal = true;
    equal = equal && (this.N_ID_FASCE_MISURATORE_2G == null ? that.N_ID_FASCE_MISURATORE_2G == null : this.N_ID_FASCE_MISURATORE_2G.equals(that.N_ID_FASCE_MISURATORE_2G));
    equal = equal && (this.N_ID_MISURATORE == null ? that.N_ID_MISURATORE == null : this.N_ID_MISURATORE.equals(that.N_ID_MISURATORE));
    equal = equal && (this.N_COD_GIORNO_2G == null ? that.N_COD_GIORNO_2G == null : this.N_COD_GIORNO_2G.equals(that.N_COD_GIORNO_2G));
    equal = equal && (this.D_DATA_GIORNO == null ? that.D_DATA_GIORNO == null : this.D_DATA_GIORNO.equals(that.D_DATA_GIORNO));
    equal = equal && (this.N_FASCIA_1 == null ? that.N_FASCIA_1 == null : this.N_FASCIA_1.equals(that.N_FASCIA_1));
    equal = equal && (this.N_FINE_FASCIA_1 == null ? that.N_FINE_FASCIA_1 == null : this.N_FINE_FASCIA_1.equals(that.N_FINE_FASCIA_1));
    equal = equal && (this.N_FASCIA_2 == null ? that.N_FASCIA_2 == null : this.N_FASCIA_2.equals(that.N_FASCIA_2));
    equal = equal && (this.N_FINE_FASCIA_2 == null ? that.N_FINE_FASCIA_2 == null : this.N_FINE_FASCIA_2.equals(that.N_FINE_FASCIA_2));
    equal = equal && (this.N_FASCIA_3 == null ? that.N_FASCIA_3 == null : this.N_FASCIA_3.equals(that.N_FASCIA_3));
    equal = equal && (this.N_FINE_FASCIA_3 == null ? that.N_FINE_FASCIA_3 == null : this.N_FINE_FASCIA_3.equals(that.N_FINE_FASCIA_3));
    equal = equal && (this.N_FASCIA_4 == null ? that.N_FASCIA_4 == null : this.N_FASCIA_4.equals(that.N_FASCIA_4));
    equal = equal && (this.N_FINE_FASCIA_4 == null ? that.N_FINE_FASCIA_4 == null : this.N_FINE_FASCIA_4.equals(that.N_FINE_FASCIA_4));
    equal = equal && (this.N_FASCIA_5 == null ? that.N_FASCIA_5 == null : this.N_FASCIA_5.equals(that.N_FASCIA_5));
    equal = equal && (this.N_FINE_FASCIA_5 == null ? that.N_FINE_FASCIA_5 == null : this.N_FINE_FASCIA_5.equals(that.N_FINE_FASCIA_5));
    equal = equal && (this.N_FASCIA_6 == null ? that.N_FASCIA_6 == null : this.N_FASCIA_6.equals(that.N_FASCIA_6));
    equal = equal && (this.N_FINE_FASCIA_6 == null ? that.N_FINE_FASCIA_6 == null : this.N_FINE_FASCIA_6.equals(that.N_FINE_FASCIA_6));
    equal = equal && (this.N_FASCIA_7 == null ? that.N_FASCIA_7 == null : this.N_FASCIA_7.equals(that.N_FASCIA_7));
    equal = equal && (this.N_FINE_FASCIA_7 == null ? that.N_FINE_FASCIA_7 == null : this.N_FINE_FASCIA_7.equals(that.N_FINE_FASCIA_7));
    equal = equal && (this.N_FASCIA_8 == null ? that.N_FASCIA_8 == null : this.N_FASCIA_8.equals(that.N_FASCIA_8));
    equal = equal && (this.N_FINE_FASCIA_8 == null ? that.N_FINE_FASCIA_8 == null : this.N_FINE_FASCIA_8.equals(that.N_FINE_FASCIA_8));
    equal = equal && (this.N_FASCIA_9 == null ? that.N_FASCIA_9 == null : this.N_FASCIA_9.equals(that.N_FASCIA_9));
    equal = equal && (this.N_FINE_FASCIA_9 == null ? that.N_FINE_FASCIA_9 == null : this.N_FINE_FASCIA_9.equals(that.N_FINE_FASCIA_9));
    equal = equal && (this.N_FASCIA_10 == null ? that.N_FASCIA_10 == null : this.N_FASCIA_10.equals(that.N_FASCIA_10));
    equal = equal && (this.N_FINE_FASCIA_10 == null ? that.N_FINE_FASCIA_10 == null : this.N_FINE_FASCIA_10.equals(that.N_FINE_FASCIA_10));
    equal = equal && (this.T_NOTA == null ? that.T_NOTA == null : this.T_NOTA.equals(that.T_NOTA));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.D_DATA_RIF == null ? that.D_DATA_RIF == null : this.D_DATA_RIF.equals(that.D_DATA_RIF));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_FASCE_MISURATORE_2G = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_MISURATORE = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.N_COD_GIORNO_2G = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.D_DATA_GIORNO = JdbcWritableBridge.readString(4, __dbResults);
    this.N_FASCIA_1 = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.N_FINE_FASCIA_1 = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.N_FASCIA_2 = JdbcWritableBridge.readBigDecimal(7, __dbResults);
    this.N_FINE_FASCIA_2 = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.N_FASCIA_3 = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.N_FINE_FASCIA_3 = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.N_FASCIA_4 = JdbcWritableBridge.readBigDecimal(11, __dbResults);
    this.N_FINE_FASCIA_4 = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.N_FASCIA_5 = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.N_FINE_FASCIA_5 = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.N_FASCIA_6 = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.N_FINE_FASCIA_6 = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.N_FASCIA_7 = JdbcWritableBridge.readBigDecimal(17, __dbResults);
    this.N_FINE_FASCIA_7 = JdbcWritableBridge.readBigDecimal(18, __dbResults);
    this.N_FASCIA_8 = JdbcWritableBridge.readBigDecimal(19, __dbResults);
    this.N_FINE_FASCIA_8 = JdbcWritableBridge.readBigDecimal(20, __dbResults);
    this.N_FASCIA_9 = JdbcWritableBridge.readBigDecimal(21, __dbResults);
    this.N_FINE_FASCIA_9 = JdbcWritableBridge.readBigDecimal(22, __dbResults);
    this.N_FASCIA_10 = JdbcWritableBridge.readBigDecimal(23, __dbResults);
    this.N_FINE_FASCIA_10 = JdbcWritableBridge.readBigDecimal(24, __dbResults);
    this.T_NOTA = JdbcWritableBridge.readString(25, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(26, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(27, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(28, __dbResults);
    this.D_DATA_RIF = JdbcWritableBridge.readString(29, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_FASCE_MISURATORE_2G = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_MISURATORE = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.N_COD_GIORNO_2G = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.D_DATA_GIORNO = JdbcWritableBridge.readString(4, __dbResults);
    this.N_FASCIA_1 = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.N_FINE_FASCIA_1 = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.N_FASCIA_2 = JdbcWritableBridge.readBigDecimal(7, __dbResults);
    this.N_FINE_FASCIA_2 = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.N_FASCIA_3 = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.N_FINE_FASCIA_3 = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.N_FASCIA_4 = JdbcWritableBridge.readBigDecimal(11, __dbResults);
    this.N_FINE_FASCIA_4 = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.N_FASCIA_5 = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.N_FINE_FASCIA_5 = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.N_FASCIA_6 = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.N_FINE_FASCIA_6 = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.N_FASCIA_7 = JdbcWritableBridge.readBigDecimal(17, __dbResults);
    this.N_FINE_FASCIA_7 = JdbcWritableBridge.readBigDecimal(18, __dbResults);
    this.N_FASCIA_8 = JdbcWritableBridge.readBigDecimal(19, __dbResults);
    this.N_FINE_FASCIA_8 = JdbcWritableBridge.readBigDecimal(20, __dbResults);
    this.N_FASCIA_9 = JdbcWritableBridge.readBigDecimal(21, __dbResults);
    this.N_FINE_FASCIA_9 = JdbcWritableBridge.readBigDecimal(22, __dbResults);
    this.N_FASCIA_10 = JdbcWritableBridge.readBigDecimal(23, __dbResults);
    this.N_FINE_FASCIA_10 = JdbcWritableBridge.readBigDecimal(24, __dbResults);
    this.T_NOTA = JdbcWritableBridge.readString(25, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(26, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(27, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(28, __dbResults);
    this.D_DATA_RIF = JdbcWritableBridge.readString(29, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_FASCE_MISURATORE_2G, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_MISURATORE, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_COD_GIORNO_2G, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_GIORNO, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FASCIA_1, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FINE_FASCIA_1, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FASCIA_2, 7 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FINE_FASCIA_2, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FASCIA_3, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FINE_FASCIA_3, 10 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FASCIA_4, 11 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FINE_FASCIA_4, 12 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FASCIA_5, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FINE_FASCIA_5, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FASCIA_6, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FINE_FASCIA_6, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FASCIA_7, 17 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FINE_FASCIA_7, 18 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FASCIA_8, 19 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FINE_FASCIA_8, 20 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FASCIA_9, 21 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FINE_FASCIA_9, 22 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FASCIA_10, 23 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FINE_FASCIA_10, 24 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTA, 25 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 26 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 27 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 28 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF, 29 + __off, 93, __dbStmt);
    return 29;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_FASCE_MISURATORE_2G, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_MISURATORE, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_COD_GIORNO_2G, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_GIORNO, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FASCIA_1, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FINE_FASCIA_1, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FASCIA_2, 7 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FINE_FASCIA_2, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FASCIA_3, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FINE_FASCIA_3, 10 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FASCIA_4, 11 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FINE_FASCIA_4, 12 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FASCIA_5, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FINE_FASCIA_5, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FASCIA_6, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FINE_FASCIA_6, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FASCIA_7, 17 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FINE_FASCIA_7, 18 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FASCIA_8, 19 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FINE_FASCIA_8, 20 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FASCIA_9, 21 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FINE_FASCIA_9, 22 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FASCIA_10, 23 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_FINE_FASCIA_10, 24 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTA, 25 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 26 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 27 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 28 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF, 29 + __off, 93, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_FASCE_MISURATORE_2G = null;
    } else {
    this.N_ID_FASCE_MISURATORE_2G = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_MISURATORE = null;
    } else {
    this.N_ID_MISURATORE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_COD_GIORNO_2G = null;
    } else {
    this.N_COD_GIORNO_2G = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_GIORNO = null;
    } else {
    this.D_DATA_GIORNO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_FASCIA_1 = null;
    } else {
    this.N_FASCIA_1 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_FINE_FASCIA_1 = null;
    } else {
    this.N_FINE_FASCIA_1 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_FASCIA_2 = null;
    } else {
    this.N_FASCIA_2 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_FINE_FASCIA_2 = null;
    } else {
    this.N_FINE_FASCIA_2 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_FASCIA_3 = null;
    } else {
    this.N_FASCIA_3 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_FINE_FASCIA_3 = null;
    } else {
    this.N_FINE_FASCIA_3 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_FASCIA_4 = null;
    } else {
    this.N_FASCIA_4 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_FINE_FASCIA_4 = null;
    } else {
    this.N_FINE_FASCIA_4 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_FASCIA_5 = null;
    } else {
    this.N_FASCIA_5 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_FINE_FASCIA_5 = null;
    } else {
    this.N_FINE_FASCIA_5 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_FASCIA_6 = null;
    } else {
    this.N_FASCIA_6 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_FINE_FASCIA_6 = null;
    } else {
    this.N_FINE_FASCIA_6 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_FASCIA_7 = null;
    } else {
    this.N_FASCIA_7 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_FINE_FASCIA_7 = null;
    } else {
    this.N_FINE_FASCIA_7 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_FASCIA_8 = null;
    } else {
    this.N_FASCIA_8 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_FINE_FASCIA_8 = null;
    } else {
    this.N_FINE_FASCIA_8 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_FASCIA_9 = null;
    } else {
    this.N_FASCIA_9 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_FINE_FASCIA_9 = null;
    } else {
    this.N_FINE_FASCIA_9 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_FASCIA_10 = null;
    } else {
    this.N_FASCIA_10 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_FINE_FASCIA_10 = null;
    } else {
    this.N_FINE_FASCIA_10 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
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
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_FASCE_MISURATORE_2G) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FASCE_MISURATORE_2G, __dataOut);
    }
    if (null == this.N_ID_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_MISURATORE, __dataOut);
    }
    if (null == this.N_COD_GIORNO_2G) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_COD_GIORNO_2G, __dataOut);
    }
    if (null == this.D_DATA_GIORNO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_GIORNO);
    }
    if (null == this.N_FASCIA_1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FASCIA_1, __dataOut);
    }
    if (null == this.N_FINE_FASCIA_1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FINE_FASCIA_1, __dataOut);
    }
    if (null == this.N_FASCIA_2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FASCIA_2, __dataOut);
    }
    if (null == this.N_FINE_FASCIA_2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FINE_FASCIA_2, __dataOut);
    }
    if (null == this.N_FASCIA_3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FASCIA_3, __dataOut);
    }
    if (null == this.N_FINE_FASCIA_3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FINE_FASCIA_3, __dataOut);
    }
    if (null == this.N_FASCIA_4) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FASCIA_4, __dataOut);
    }
    if (null == this.N_FINE_FASCIA_4) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FINE_FASCIA_4, __dataOut);
    }
    if (null == this.N_FASCIA_5) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FASCIA_5, __dataOut);
    }
    if (null == this.N_FINE_FASCIA_5) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FINE_FASCIA_5, __dataOut);
    }
    if (null == this.N_FASCIA_6) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FASCIA_6, __dataOut);
    }
    if (null == this.N_FINE_FASCIA_6) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FINE_FASCIA_6, __dataOut);
    }
    if (null == this.N_FASCIA_7) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FASCIA_7, __dataOut);
    }
    if (null == this.N_FINE_FASCIA_7) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FINE_FASCIA_7, __dataOut);
    }
    if (null == this.N_FASCIA_8) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FASCIA_8, __dataOut);
    }
    if (null == this.N_FINE_FASCIA_8) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FINE_FASCIA_8, __dataOut);
    }
    if (null == this.N_FASCIA_9) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FASCIA_9, __dataOut);
    }
    if (null == this.N_FINE_FASCIA_9) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FINE_FASCIA_9, __dataOut);
    }
    if (null == this.N_FASCIA_10) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FASCIA_10, __dataOut);
    }
    if (null == this.N_FINE_FASCIA_10) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FINE_FASCIA_10, __dataOut);
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
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_FASCE_MISURATORE_2G) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FASCE_MISURATORE_2G, __dataOut);
    }
    if (null == this.N_ID_MISURATORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_MISURATORE, __dataOut);
    }
    if (null == this.N_COD_GIORNO_2G) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_COD_GIORNO_2G, __dataOut);
    }
    if (null == this.D_DATA_GIORNO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_GIORNO);
    }
    if (null == this.N_FASCIA_1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FASCIA_1, __dataOut);
    }
    if (null == this.N_FINE_FASCIA_1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FINE_FASCIA_1, __dataOut);
    }
    if (null == this.N_FASCIA_2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FASCIA_2, __dataOut);
    }
    if (null == this.N_FINE_FASCIA_2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FINE_FASCIA_2, __dataOut);
    }
    if (null == this.N_FASCIA_3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FASCIA_3, __dataOut);
    }
    if (null == this.N_FINE_FASCIA_3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FINE_FASCIA_3, __dataOut);
    }
    if (null == this.N_FASCIA_4) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FASCIA_4, __dataOut);
    }
    if (null == this.N_FINE_FASCIA_4) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FINE_FASCIA_4, __dataOut);
    }
    if (null == this.N_FASCIA_5) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FASCIA_5, __dataOut);
    }
    if (null == this.N_FINE_FASCIA_5) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FINE_FASCIA_5, __dataOut);
    }
    if (null == this.N_FASCIA_6) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FASCIA_6, __dataOut);
    }
    if (null == this.N_FINE_FASCIA_6) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FINE_FASCIA_6, __dataOut);
    }
    if (null == this.N_FASCIA_7) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FASCIA_7, __dataOut);
    }
    if (null == this.N_FINE_FASCIA_7) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FINE_FASCIA_7, __dataOut);
    }
    if (null == this.N_FASCIA_8) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FASCIA_8, __dataOut);
    }
    if (null == this.N_FINE_FASCIA_8) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FINE_FASCIA_8, __dataOut);
    }
    if (null == this.N_FASCIA_9) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FASCIA_9, __dataOut);
    }
    if (null == this.N_FINE_FASCIA_9) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FINE_FASCIA_9, __dataOut);
    }
    if (null == this.N_FASCIA_10) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FASCIA_10, __dataOut);
    }
    if (null == this.N_FINE_FASCIA_10) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_FINE_FASCIA_10, __dataOut);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_FASCE_MISURATORE_2G==null?"":N_ID_FASCE_MISURATORE_2G.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_MISURATORE==null?"":N_ID_MISURATORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_COD_GIORNO_2G==null?"":N_COD_GIORNO_2G.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_GIORNO==null?"":D_DATA_GIORNO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FASCIA_1==null?"":N_FASCIA_1.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FINE_FASCIA_1==null?"":N_FINE_FASCIA_1.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FASCIA_2==null?"":N_FASCIA_2.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FINE_FASCIA_2==null?"":N_FINE_FASCIA_2.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FASCIA_3==null?"":N_FASCIA_3.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FINE_FASCIA_3==null?"":N_FINE_FASCIA_3.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FASCIA_4==null?"":N_FASCIA_4.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FINE_FASCIA_4==null?"":N_FINE_FASCIA_4.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FASCIA_5==null?"":N_FASCIA_5.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FINE_FASCIA_5==null?"":N_FINE_FASCIA_5.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FASCIA_6==null?"":N_FASCIA_6.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FINE_FASCIA_6==null?"":N_FINE_FASCIA_6.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FASCIA_7==null?"":N_FASCIA_7.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FINE_FASCIA_7==null?"":N_FINE_FASCIA_7.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FASCIA_8==null?"":N_FASCIA_8.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FINE_FASCIA_8==null?"":N_FINE_FASCIA_8.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FASCIA_9==null?"":N_FASCIA_9.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FINE_FASCIA_9==null?"":N_FINE_FASCIA_9.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FASCIA_10==null?"":N_FASCIA_10.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FINE_FASCIA_10==null?"":N_FINE_FASCIA_10.toPlainString(), delimiters));
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
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_FASCE_MISURATORE_2G==null?"":N_ID_FASCE_MISURATORE_2G.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_MISURATORE==null?"":N_ID_MISURATORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_COD_GIORNO_2G==null?"":N_COD_GIORNO_2G.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_GIORNO==null?"":D_DATA_GIORNO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FASCIA_1==null?"":N_FASCIA_1.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FINE_FASCIA_1==null?"":N_FINE_FASCIA_1.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FASCIA_2==null?"":N_FASCIA_2.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FINE_FASCIA_2==null?"":N_FINE_FASCIA_2.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FASCIA_3==null?"":N_FASCIA_3.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FINE_FASCIA_3==null?"":N_FINE_FASCIA_3.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FASCIA_4==null?"":N_FASCIA_4.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FINE_FASCIA_4==null?"":N_FINE_FASCIA_4.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FASCIA_5==null?"":N_FASCIA_5.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FINE_FASCIA_5==null?"":N_FINE_FASCIA_5.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FASCIA_6==null?"":N_FASCIA_6.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FINE_FASCIA_6==null?"":N_FINE_FASCIA_6.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FASCIA_7==null?"":N_FASCIA_7.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FINE_FASCIA_7==null?"":N_FINE_FASCIA_7.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FASCIA_8==null?"":N_FASCIA_8.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FINE_FASCIA_8==null?"":N_FINE_FASCIA_8.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FASCIA_9==null?"":N_FASCIA_9.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FINE_FASCIA_9==null?"":N_FINE_FASCIA_9.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FASCIA_10==null?"":N_FASCIA_10.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_FINE_FASCIA_10==null?"":N_FINE_FASCIA_10.toPlainString(), delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_FASCE_MISURATORE_2G = null; } else {
      this.N_ID_FASCE_MISURATORE_2G = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_MISURATORE = null; } else {
      this.N_ID_MISURATORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_COD_GIORNO_2G = null; } else {
      this.N_COD_GIORNO_2G = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_GIORNO = null; } else {
      this.D_DATA_GIORNO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FASCIA_1 = null; } else {
      this.N_FASCIA_1 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FINE_FASCIA_1 = null; } else {
      this.N_FINE_FASCIA_1 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FASCIA_2 = null; } else {
      this.N_FASCIA_2 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FINE_FASCIA_2 = null; } else {
      this.N_FINE_FASCIA_2 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FASCIA_3 = null; } else {
      this.N_FASCIA_3 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FINE_FASCIA_3 = null; } else {
      this.N_FINE_FASCIA_3 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FASCIA_4 = null; } else {
      this.N_FASCIA_4 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FINE_FASCIA_4 = null; } else {
      this.N_FINE_FASCIA_4 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FASCIA_5 = null; } else {
      this.N_FASCIA_5 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FINE_FASCIA_5 = null; } else {
      this.N_FINE_FASCIA_5 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FASCIA_6 = null; } else {
      this.N_FASCIA_6 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FINE_FASCIA_6 = null; } else {
      this.N_FINE_FASCIA_6 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FASCIA_7 = null; } else {
      this.N_FASCIA_7 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FINE_FASCIA_7 = null; } else {
      this.N_FINE_FASCIA_7 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FASCIA_8 = null; } else {
      this.N_FASCIA_8 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FINE_FASCIA_8 = null; } else {
      this.N_FINE_FASCIA_8 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FASCIA_9 = null; } else {
      this.N_FASCIA_9 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FINE_FASCIA_9 = null; } else {
      this.N_FINE_FASCIA_9 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FASCIA_10 = null; } else {
      this.N_FASCIA_10 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FINE_FASCIA_10 = null; } else {
      this.N_FINE_FASCIA_10 = new java.math.BigDecimal(__cur_str);
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

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_FASCE_MISURATORE_2G = null; } else {
      this.N_ID_FASCE_MISURATORE_2G = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_MISURATORE = null; } else {
      this.N_ID_MISURATORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_COD_GIORNO_2G = null; } else {
      this.N_COD_GIORNO_2G = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_GIORNO = null; } else {
      this.D_DATA_GIORNO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FASCIA_1 = null; } else {
      this.N_FASCIA_1 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FINE_FASCIA_1 = null; } else {
      this.N_FINE_FASCIA_1 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FASCIA_2 = null; } else {
      this.N_FASCIA_2 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FINE_FASCIA_2 = null; } else {
      this.N_FINE_FASCIA_2 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FASCIA_3 = null; } else {
      this.N_FASCIA_3 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FINE_FASCIA_3 = null; } else {
      this.N_FINE_FASCIA_3 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FASCIA_4 = null; } else {
      this.N_FASCIA_4 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FINE_FASCIA_4 = null; } else {
      this.N_FINE_FASCIA_4 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FASCIA_5 = null; } else {
      this.N_FASCIA_5 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FINE_FASCIA_5 = null; } else {
      this.N_FINE_FASCIA_5 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FASCIA_6 = null; } else {
      this.N_FASCIA_6 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FINE_FASCIA_6 = null; } else {
      this.N_FINE_FASCIA_6 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FASCIA_7 = null; } else {
      this.N_FASCIA_7 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FINE_FASCIA_7 = null; } else {
      this.N_FINE_FASCIA_7 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FASCIA_8 = null; } else {
      this.N_FASCIA_8 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FINE_FASCIA_8 = null; } else {
      this.N_FINE_FASCIA_8 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FASCIA_9 = null; } else {
      this.N_FASCIA_9 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FINE_FASCIA_9 = null; } else {
      this.N_FINE_FASCIA_9 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FASCIA_10 = null; } else {
      this.N_FASCIA_10 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_FINE_FASCIA_10 = null; } else {
      this.N_FINE_FASCIA_10 = new java.math.BigDecimal(__cur_str);
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

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    rcu_rcu_fasce_misuratore_2g o = (rcu_rcu_fasce_misuratore_2g) super.clone();
    return o;
  }

  public void clone0(rcu_rcu_fasce_misuratore_2g o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_FASCE_MISURATORE_2G", this.N_ID_FASCE_MISURATORE_2G);
    __sqoop$field_map.put("N_ID_MISURATORE", this.N_ID_MISURATORE);
    __sqoop$field_map.put("N_COD_GIORNO_2G", this.N_COD_GIORNO_2G);
    __sqoop$field_map.put("D_DATA_GIORNO", this.D_DATA_GIORNO);
    __sqoop$field_map.put("N_FASCIA_1", this.N_FASCIA_1);
    __sqoop$field_map.put("N_FINE_FASCIA_1", this.N_FINE_FASCIA_1);
    __sqoop$field_map.put("N_FASCIA_2", this.N_FASCIA_2);
    __sqoop$field_map.put("N_FINE_FASCIA_2", this.N_FINE_FASCIA_2);
    __sqoop$field_map.put("N_FASCIA_3", this.N_FASCIA_3);
    __sqoop$field_map.put("N_FINE_FASCIA_3", this.N_FINE_FASCIA_3);
    __sqoop$field_map.put("N_FASCIA_4", this.N_FASCIA_4);
    __sqoop$field_map.put("N_FINE_FASCIA_4", this.N_FINE_FASCIA_4);
    __sqoop$field_map.put("N_FASCIA_5", this.N_FASCIA_5);
    __sqoop$field_map.put("N_FINE_FASCIA_5", this.N_FINE_FASCIA_5);
    __sqoop$field_map.put("N_FASCIA_6", this.N_FASCIA_6);
    __sqoop$field_map.put("N_FINE_FASCIA_6", this.N_FINE_FASCIA_6);
    __sqoop$field_map.put("N_FASCIA_7", this.N_FASCIA_7);
    __sqoop$field_map.put("N_FINE_FASCIA_7", this.N_FINE_FASCIA_7);
    __sqoop$field_map.put("N_FASCIA_8", this.N_FASCIA_8);
    __sqoop$field_map.put("N_FINE_FASCIA_8", this.N_FINE_FASCIA_8);
    __sqoop$field_map.put("N_FASCIA_9", this.N_FASCIA_9);
    __sqoop$field_map.put("N_FINE_FASCIA_9", this.N_FINE_FASCIA_9);
    __sqoop$field_map.put("N_FASCIA_10", this.N_FASCIA_10);
    __sqoop$field_map.put("N_FINE_FASCIA_10", this.N_FINE_FASCIA_10);
    __sqoop$field_map.put("T_NOTA", this.T_NOTA);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("D_DATA_RIF", this.D_DATA_RIF);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_FASCE_MISURATORE_2G", this.N_ID_FASCE_MISURATORE_2G);
    __sqoop$field_map.put("N_ID_MISURATORE", this.N_ID_MISURATORE);
    __sqoop$field_map.put("N_COD_GIORNO_2G", this.N_COD_GIORNO_2G);
    __sqoop$field_map.put("D_DATA_GIORNO", this.D_DATA_GIORNO);
    __sqoop$field_map.put("N_FASCIA_1", this.N_FASCIA_1);
    __sqoop$field_map.put("N_FINE_FASCIA_1", this.N_FINE_FASCIA_1);
    __sqoop$field_map.put("N_FASCIA_2", this.N_FASCIA_2);
    __sqoop$field_map.put("N_FINE_FASCIA_2", this.N_FINE_FASCIA_2);
    __sqoop$field_map.put("N_FASCIA_3", this.N_FASCIA_3);
    __sqoop$field_map.put("N_FINE_FASCIA_3", this.N_FINE_FASCIA_3);
    __sqoop$field_map.put("N_FASCIA_4", this.N_FASCIA_4);
    __sqoop$field_map.put("N_FINE_FASCIA_4", this.N_FINE_FASCIA_4);
    __sqoop$field_map.put("N_FASCIA_5", this.N_FASCIA_5);
    __sqoop$field_map.put("N_FINE_FASCIA_5", this.N_FINE_FASCIA_5);
    __sqoop$field_map.put("N_FASCIA_6", this.N_FASCIA_6);
    __sqoop$field_map.put("N_FINE_FASCIA_6", this.N_FINE_FASCIA_6);
    __sqoop$field_map.put("N_FASCIA_7", this.N_FASCIA_7);
    __sqoop$field_map.put("N_FINE_FASCIA_7", this.N_FINE_FASCIA_7);
    __sqoop$field_map.put("N_FASCIA_8", this.N_FASCIA_8);
    __sqoop$field_map.put("N_FINE_FASCIA_8", this.N_FINE_FASCIA_8);
    __sqoop$field_map.put("N_FASCIA_9", this.N_FASCIA_9);
    __sqoop$field_map.put("N_FINE_FASCIA_9", this.N_FINE_FASCIA_9);
    __sqoop$field_map.put("N_FASCIA_10", this.N_FASCIA_10);
    __sqoop$field_map.put("N_FINE_FASCIA_10", this.N_FINE_FASCIA_10);
    __sqoop$field_map.put("T_NOTA", this.T_NOTA);
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
