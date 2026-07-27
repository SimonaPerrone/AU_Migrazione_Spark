// ORM class for table 'tmpod.prt_tmo_aggregati_calcolati'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 14:52:22 CEST 2019
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

public class tmpod_prt_tmo_aggregati_calcolati extends SqoopRecord  implements DBWritable, Writable {
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
    setters.put("N_ID_DISTR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_DISTR = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_AREA_RIF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_AREA_RIF = (String)value;
      }
    });
    setters.put("ANNOMESE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ANNOMESE = (String)value;
      }
    });
    setters.put("N_ID_UDD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_UDD = (java.math.BigDecimal)value;
      }
    });
    setters.put("GIORNO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        GIORNO = (String)value;
      }
    });
    setters.put("N_H1", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H1 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H2", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H2 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H3", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H3 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H4", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H4 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H5", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H5 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H6", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H6 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H7", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H7 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H8", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H8 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H9", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H9 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H10", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H10 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H11", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H11 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H12", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H12 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H13", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H13 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H14", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H14 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H15", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H15 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H16", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H16 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H17", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H17 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H18", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H18 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H19", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H19 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H20", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H20 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H21", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H21 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H22", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H22 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H23", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H23 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H24", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H24 = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_H25", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_H25 = (java.math.BigDecimal)value;
      }
    });
    setters.put("D_DATA_AGGREGAZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_AGGREGAZIONE = (String)value;
      }
    });
    setters.put("T_AGGR_SOTTESI", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_AGGR_SOTTESI = (String)value;
      }
    });
    setters.put("N_ID_DISTR_RIF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_DISTR_RIF = (java.math.BigDecimal)value;
      }
    });
    setters.put("UID_ELAB", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        UID_ELAB = (java.math.BigDecimal)value;
      }
    });
  }
  public tmpod_prt_tmo_aggregati_calcolati() {
    init0();
  }
  private java.math.BigDecimal N_ID;
  public java.math.BigDecimal get_N_ID() {
    return N_ID;
  }
  public void set_N_ID(java.math.BigDecimal N_ID) {
    this.N_ID = N_ID;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_ID(java.math.BigDecimal N_ID) {
    this.N_ID = N_ID;
    return this;
  }
  private java.math.BigDecimal N_ID_DISTR;
  public java.math.BigDecimal get_N_ID_DISTR() {
    return N_ID_DISTR;
  }
  public void set_N_ID_DISTR(java.math.BigDecimal N_ID_DISTR) {
    this.N_ID_DISTR = N_ID_DISTR;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_ID_DISTR(java.math.BigDecimal N_ID_DISTR) {
    this.N_ID_DISTR = N_ID_DISTR;
    return this;
  }
  private String T_AREA_RIF;
  public String get_T_AREA_RIF() {
    return T_AREA_RIF;
  }
  public void set_T_AREA_RIF(String T_AREA_RIF) {
    this.T_AREA_RIF = T_AREA_RIF;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_T_AREA_RIF(String T_AREA_RIF) {
    this.T_AREA_RIF = T_AREA_RIF;
    return this;
  }
  private String ANNOMESE;
  public String get_ANNOMESE() {
    return ANNOMESE;
  }
  public void set_ANNOMESE(String ANNOMESE) {
    this.ANNOMESE = ANNOMESE;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_ANNOMESE(String ANNOMESE) {
    this.ANNOMESE = ANNOMESE;
    return this;
  }
  private java.math.BigDecimal N_ID_UDD;
  public java.math.BigDecimal get_N_ID_UDD() {
    return N_ID_UDD;
  }
  public void set_N_ID_UDD(java.math.BigDecimal N_ID_UDD) {
    this.N_ID_UDD = N_ID_UDD;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_ID_UDD(java.math.BigDecimal N_ID_UDD) {
    this.N_ID_UDD = N_ID_UDD;
    return this;
  }
  private String GIORNO;
  public String get_GIORNO() {
    return GIORNO;
  }
  public void set_GIORNO(String GIORNO) {
    this.GIORNO = GIORNO;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_GIORNO(String GIORNO) {
    this.GIORNO = GIORNO;
    return this;
  }
  private java.math.BigDecimal N_H1;
  public java.math.BigDecimal get_N_H1() {
    return N_H1;
  }
  public void set_N_H1(java.math.BigDecimal N_H1) {
    this.N_H1 = N_H1;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H1(java.math.BigDecimal N_H1) {
    this.N_H1 = N_H1;
    return this;
  }
  private java.math.BigDecimal N_H2;
  public java.math.BigDecimal get_N_H2() {
    return N_H2;
  }
  public void set_N_H2(java.math.BigDecimal N_H2) {
    this.N_H2 = N_H2;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H2(java.math.BigDecimal N_H2) {
    this.N_H2 = N_H2;
    return this;
  }
  private java.math.BigDecimal N_H3;
  public java.math.BigDecimal get_N_H3() {
    return N_H3;
  }
  public void set_N_H3(java.math.BigDecimal N_H3) {
    this.N_H3 = N_H3;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H3(java.math.BigDecimal N_H3) {
    this.N_H3 = N_H3;
    return this;
  }
  private java.math.BigDecimal N_H4;
  public java.math.BigDecimal get_N_H4() {
    return N_H4;
  }
  public void set_N_H4(java.math.BigDecimal N_H4) {
    this.N_H4 = N_H4;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H4(java.math.BigDecimal N_H4) {
    this.N_H4 = N_H4;
    return this;
  }
  private java.math.BigDecimal N_H5;
  public java.math.BigDecimal get_N_H5() {
    return N_H5;
  }
  public void set_N_H5(java.math.BigDecimal N_H5) {
    this.N_H5 = N_H5;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H5(java.math.BigDecimal N_H5) {
    this.N_H5 = N_H5;
    return this;
  }
  private java.math.BigDecimal N_H6;
  public java.math.BigDecimal get_N_H6() {
    return N_H6;
  }
  public void set_N_H6(java.math.BigDecimal N_H6) {
    this.N_H6 = N_H6;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H6(java.math.BigDecimal N_H6) {
    this.N_H6 = N_H6;
    return this;
  }
  private java.math.BigDecimal N_H7;
  public java.math.BigDecimal get_N_H7() {
    return N_H7;
  }
  public void set_N_H7(java.math.BigDecimal N_H7) {
    this.N_H7 = N_H7;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H7(java.math.BigDecimal N_H7) {
    this.N_H7 = N_H7;
    return this;
  }
  private java.math.BigDecimal N_H8;
  public java.math.BigDecimal get_N_H8() {
    return N_H8;
  }
  public void set_N_H8(java.math.BigDecimal N_H8) {
    this.N_H8 = N_H8;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H8(java.math.BigDecimal N_H8) {
    this.N_H8 = N_H8;
    return this;
  }
  private java.math.BigDecimal N_H9;
  public java.math.BigDecimal get_N_H9() {
    return N_H9;
  }
  public void set_N_H9(java.math.BigDecimal N_H9) {
    this.N_H9 = N_H9;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H9(java.math.BigDecimal N_H9) {
    this.N_H9 = N_H9;
    return this;
  }
  private java.math.BigDecimal N_H10;
  public java.math.BigDecimal get_N_H10() {
    return N_H10;
  }
  public void set_N_H10(java.math.BigDecimal N_H10) {
    this.N_H10 = N_H10;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H10(java.math.BigDecimal N_H10) {
    this.N_H10 = N_H10;
    return this;
  }
  private java.math.BigDecimal N_H11;
  public java.math.BigDecimal get_N_H11() {
    return N_H11;
  }
  public void set_N_H11(java.math.BigDecimal N_H11) {
    this.N_H11 = N_H11;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H11(java.math.BigDecimal N_H11) {
    this.N_H11 = N_H11;
    return this;
  }
  private java.math.BigDecimal N_H12;
  public java.math.BigDecimal get_N_H12() {
    return N_H12;
  }
  public void set_N_H12(java.math.BigDecimal N_H12) {
    this.N_H12 = N_H12;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H12(java.math.BigDecimal N_H12) {
    this.N_H12 = N_H12;
    return this;
  }
  private java.math.BigDecimal N_H13;
  public java.math.BigDecimal get_N_H13() {
    return N_H13;
  }
  public void set_N_H13(java.math.BigDecimal N_H13) {
    this.N_H13 = N_H13;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H13(java.math.BigDecimal N_H13) {
    this.N_H13 = N_H13;
    return this;
  }
  private java.math.BigDecimal N_H14;
  public java.math.BigDecimal get_N_H14() {
    return N_H14;
  }
  public void set_N_H14(java.math.BigDecimal N_H14) {
    this.N_H14 = N_H14;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H14(java.math.BigDecimal N_H14) {
    this.N_H14 = N_H14;
    return this;
  }
  private java.math.BigDecimal N_H15;
  public java.math.BigDecimal get_N_H15() {
    return N_H15;
  }
  public void set_N_H15(java.math.BigDecimal N_H15) {
    this.N_H15 = N_H15;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H15(java.math.BigDecimal N_H15) {
    this.N_H15 = N_H15;
    return this;
  }
  private java.math.BigDecimal N_H16;
  public java.math.BigDecimal get_N_H16() {
    return N_H16;
  }
  public void set_N_H16(java.math.BigDecimal N_H16) {
    this.N_H16 = N_H16;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H16(java.math.BigDecimal N_H16) {
    this.N_H16 = N_H16;
    return this;
  }
  private java.math.BigDecimal N_H17;
  public java.math.BigDecimal get_N_H17() {
    return N_H17;
  }
  public void set_N_H17(java.math.BigDecimal N_H17) {
    this.N_H17 = N_H17;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H17(java.math.BigDecimal N_H17) {
    this.N_H17 = N_H17;
    return this;
  }
  private java.math.BigDecimal N_H18;
  public java.math.BigDecimal get_N_H18() {
    return N_H18;
  }
  public void set_N_H18(java.math.BigDecimal N_H18) {
    this.N_H18 = N_H18;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H18(java.math.BigDecimal N_H18) {
    this.N_H18 = N_H18;
    return this;
  }
  private java.math.BigDecimal N_H19;
  public java.math.BigDecimal get_N_H19() {
    return N_H19;
  }
  public void set_N_H19(java.math.BigDecimal N_H19) {
    this.N_H19 = N_H19;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H19(java.math.BigDecimal N_H19) {
    this.N_H19 = N_H19;
    return this;
  }
  private java.math.BigDecimal N_H20;
  public java.math.BigDecimal get_N_H20() {
    return N_H20;
  }
  public void set_N_H20(java.math.BigDecimal N_H20) {
    this.N_H20 = N_H20;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H20(java.math.BigDecimal N_H20) {
    this.N_H20 = N_H20;
    return this;
  }
  private java.math.BigDecimal N_H21;
  public java.math.BigDecimal get_N_H21() {
    return N_H21;
  }
  public void set_N_H21(java.math.BigDecimal N_H21) {
    this.N_H21 = N_H21;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H21(java.math.BigDecimal N_H21) {
    this.N_H21 = N_H21;
    return this;
  }
  private java.math.BigDecimal N_H22;
  public java.math.BigDecimal get_N_H22() {
    return N_H22;
  }
  public void set_N_H22(java.math.BigDecimal N_H22) {
    this.N_H22 = N_H22;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H22(java.math.BigDecimal N_H22) {
    this.N_H22 = N_H22;
    return this;
  }
  private java.math.BigDecimal N_H23;
  public java.math.BigDecimal get_N_H23() {
    return N_H23;
  }
  public void set_N_H23(java.math.BigDecimal N_H23) {
    this.N_H23 = N_H23;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H23(java.math.BigDecimal N_H23) {
    this.N_H23 = N_H23;
    return this;
  }
  private java.math.BigDecimal N_H24;
  public java.math.BigDecimal get_N_H24() {
    return N_H24;
  }
  public void set_N_H24(java.math.BigDecimal N_H24) {
    this.N_H24 = N_H24;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H24(java.math.BigDecimal N_H24) {
    this.N_H24 = N_H24;
    return this;
  }
  private java.math.BigDecimal N_H25;
  public java.math.BigDecimal get_N_H25() {
    return N_H25;
  }
  public void set_N_H25(java.math.BigDecimal N_H25) {
    this.N_H25 = N_H25;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_H25(java.math.BigDecimal N_H25) {
    this.N_H25 = N_H25;
    return this;
  }
  private String D_DATA_AGGREGAZIONE;
  public String get_D_DATA_AGGREGAZIONE() {
    return D_DATA_AGGREGAZIONE;
  }
  public void set_D_DATA_AGGREGAZIONE(String D_DATA_AGGREGAZIONE) {
    this.D_DATA_AGGREGAZIONE = D_DATA_AGGREGAZIONE;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_D_DATA_AGGREGAZIONE(String D_DATA_AGGREGAZIONE) {
    this.D_DATA_AGGREGAZIONE = D_DATA_AGGREGAZIONE;
    return this;
  }
  private String T_AGGR_SOTTESI;
  public String get_T_AGGR_SOTTESI() {
    return T_AGGR_SOTTESI;
  }
  public void set_T_AGGR_SOTTESI(String T_AGGR_SOTTESI) {
    this.T_AGGR_SOTTESI = T_AGGR_SOTTESI;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_T_AGGR_SOTTESI(String T_AGGR_SOTTESI) {
    this.T_AGGR_SOTTESI = T_AGGR_SOTTESI;
    return this;
  }
  private java.math.BigDecimal N_ID_DISTR_RIF;
  public java.math.BigDecimal get_N_ID_DISTR_RIF() {
    return N_ID_DISTR_RIF;
  }
  public void set_N_ID_DISTR_RIF(java.math.BigDecimal N_ID_DISTR_RIF) {
    this.N_ID_DISTR_RIF = N_ID_DISTR_RIF;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_N_ID_DISTR_RIF(java.math.BigDecimal N_ID_DISTR_RIF) {
    this.N_ID_DISTR_RIF = N_ID_DISTR_RIF;
    return this;
  }
  private java.math.BigDecimal UID_ELAB;
  public java.math.BigDecimal get_UID_ELAB() {
    return UID_ELAB;
  }
  public void set_UID_ELAB(java.math.BigDecimal UID_ELAB) {
    this.UID_ELAB = UID_ELAB;
  }
  public tmpod_prt_tmo_aggregati_calcolati with_UID_ELAB(java.math.BigDecimal UID_ELAB) {
    this.UID_ELAB = UID_ELAB;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof tmpod_prt_tmo_aggregati_calcolati)) {
      return false;
    }
    tmpod_prt_tmo_aggregati_calcolati that = (tmpod_prt_tmo_aggregati_calcolati) o;
    boolean equal = true;
    equal = equal && (this.N_ID == null ? that.N_ID == null : this.N_ID.equals(that.N_ID));
    equal = equal && (this.N_ID_DISTR == null ? that.N_ID_DISTR == null : this.N_ID_DISTR.equals(that.N_ID_DISTR));
    equal = equal && (this.T_AREA_RIF == null ? that.T_AREA_RIF == null : this.T_AREA_RIF.equals(that.T_AREA_RIF));
    equal = equal && (this.ANNOMESE == null ? that.ANNOMESE == null : this.ANNOMESE.equals(that.ANNOMESE));
    equal = equal && (this.N_ID_UDD == null ? that.N_ID_UDD == null : this.N_ID_UDD.equals(that.N_ID_UDD));
    equal = equal && (this.GIORNO == null ? that.GIORNO == null : this.GIORNO.equals(that.GIORNO));
    equal = equal && (this.N_H1 == null ? that.N_H1 == null : this.N_H1.equals(that.N_H1));
    equal = equal && (this.N_H2 == null ? that.N_H2 == null : this.N_H2.equals(that.N_H2));
    equal = equal && (this.N_H3 == null ? that.N_H3 == null : this.N_H3.equals(that.N_H3));
    equal = equal && (this.N_H4 == null ? that.N_H4 == null : this.N_H4.equals(that.N_H4));
    equal = equal && (this.N_H5 == null ? that.N_H5 == null : this.N_H5.equals(that.N_H5));
    equal = equal && (this.N_H6 == null ? that.N_H6 == null : this.N_H6.equals(that.N_H6));
    equal = equal && (this.N_H7 == null ? that.N_H7 == null : this.N_H7.equals(that.N_H7));
    equal = equal && (this.N_H8 == null ? that.N_H8 == null : this.N_H8.equals(that.N_H8));
    equal = equal && (this.N_H9 == null ? that.N_H9 == null : this.N_H9.equals(that.N_H9));
    equal = equal && (this.N_H10 == null ? that.N_H10 == null : this.N_H10.equals(that.N_H10));
    equal = equal && (this.N_H11 == null ? that.N_H11 == null : this.N_H11.equals(that.N_H11));
    equal = equal && (this.N_H12 == null ? that.N_H12 == null : this.N_H12.equals(that.N_H12));
    equal = equal && (this.N_H13 == null ? that.N_H13 == null : this.N_H13.equals(that.N_H13));
    equal = equal && (this.N_H14 == null ? that.N_H14 == null : this.N_H14.equals(that.N_H14));
    equal = equal && (this.N_H15 == null ? that.N_H15 == null : this.N_H15.equals(that.N_H15));
    equal = equal && (this.N_H16 == null ? that.N_H16 == null : this.N_H16.equals(that.N_H16));
    equal = equal && (this.N_H17 == null ? that.N_H17 == null : this.N_H17.equals(that.N_H17));
    equal = equal && (this.N_H18 == null ? that.N_H18 == null : this.N_H18.equals(that.N_H18));
    equal = equal && (this.N_H19 == null ? that.N_H19 == null : this.N_H19.equals(that.N_H19));
    equal = equal && (this.N_H20 == null ? that.N_H20 == null : this.N_H20.equals(that.N_H20));
    equal = equal && (this.N_H21 == null ? that.N_H21 == null : this.N_H21.equals(that.N_H21));
    equal = equal && (this.N_H22 == null ? that.N_H22 == null : this.N_H22.equals(that.N_H22));
    equal = equal && (this.N_H23 == null ? that.N_H23 == null : this.N_H23.equals(that.N_H23));
    equal = equal && (this.N_H24 == null ? that.N_H24 == null : this.N_H24.equals(that.N_H24));
    equal = equal && (this.N_H25 == null ? that.N_H25 == null : this.N_H25.equals(that.N_H25));
    equal = equal && (this.D_DATA_AGGREGAZIONE == null ? that.D_DATA_AGGREGAZIONE == null : this.D_DATA_AGGREGAZIONE.equals(that.D_DATA_AGGREGAZIONE));
    equal = equal && (this.T_AGGR_SOTTESI == null ? that.T_AGGR_SOTTESI == null : this.T_AGGR_SOTTESI.equals(that.T_AGGR_SOTTESI));
    equal = equal && (this.N_ID_DISTR_RIF == null ? that.N_ID_DISTR_RIF == null : this.N_ID_DISTR_RIF.equals(that.N_ID_DISTR_RIF));
    equal = equal && (this.UID_ELAB == null ? that.UID_ELAB == null : this.UID_ELAB.equals(that.UID_ELAB));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof tmpod_prt_tmo_aggregati_calcolati)) {
      return false;
    }
    tmpod_prt_tmo_aggregati_calcolati that = (tmpod_prt_tmo_aggregati_calcolati) o;
    boolean equal = true;
    equal = equal && (this.N_ID == null ? that.N_ID == null : this.N_ID.equals(that.N_ID));
    equal = equal && (this.N_ID_DISTR == null ? that.N_ID_DISTR == null : this.N_ID_DISTR.equals(that.N_ID_DISTR));
    equal = equal && (this.T_AREA_RIF == null ? that.T_AREA_RIF == null : this.T_AREA_RIF.equals(that.T_AREA_RIF));
    equal = equal && (this.ANNOMESE == null ? that.ANNOMESE == null : this.ANNOMESE.equals(that.ANNOMESE));
    equal = equal && (this.N_ID_UDD == null ? that.N_ID_UDD == null : this.N_ID_UDD.equals(that.N_ID_UDD));
    equal = equal && (this.GIORNO == null ? that.GIORNO == null : this.GIORNO.equals(that.GIORNO));
    equal = equal && (this.N_H1 == null ? that.N_H1 == null : this.N_H1.equals(that.N_H1));
    equal = equal && (this.N_H2 == null ? that.N_H2 == null : this.N_H2.equals(that.N_H2));
    equal = equal && (this.N_H3 == null ? that.N_H3 == null : this.N_H3.equals(that.N_H3));
    equal = equal && (this.N_H4 == null ? that.N_H4 == null : this.N_H4.equals(that.N_H4));
    equal = equal && (this.N_H5 == null ? that.N_H5 == null : this.N_H5.equals(that.N_H5));
    equal = equal && (this.N_H6 == null ? that.N_H6 == null : this.N_H6.equals(that.N_H6));
    equal = equal && (this.N_H7 == null ? that.N_H7 == null : this.N_H7.equals(that.N_H7));
    equal = equal && (this.N_H8 == null ? that.N_H8 == null : this.N_H8.equals(that.N_H8));
    equal = equal && (this.N_H9 == null ? that.N_H9 == null : this.N_H9.equals(that.N_H9));
    equal = equal && (this.N_H10 == null ? that.N_H10 == null : this.N_H10.equals(that.N_H10));
    equal = equal && (this.N_H11 == null ? that.N_H11 == null : this.N_H11.equals(that.N_H11));
    equal = equal && (this.N_H12 == null ? that.N_H12 == null : this.N_H12.equals(that.N_H12));
    equal = equal && (this.N_H13 == null ? that.N_H13 == null : this.N_H13.equals(that.N_H13));
    equal = equal && (this.N_H14 == null ? that.N_H14 == null : this.N_H14.equals(that.N_H14));
    equal = equal && (this.N_H15 == null ? that.N_H15 == null : this.N_H15.equals(that.N_H15));
    equal = equal && (this.N_H16 == null ? that.N_H16 == null : this.N_H16.equals(that.N_H16));
    equal = equal && (this.N_H17 == null ? that.N_H17 == null : this.N_H17.equals(that.N_H17));
    equal = equal && (this.N_H18 == null ? that.N_H18 == null : this.N_H18.equals(that.N_H18));
    equal = equal && (this.N_H19 == null ? that.N_H19 == null : this.N_H19.equals(that.N_H19));
    equal = equal && (this.N_H20 == null ? that.N_H20 == null : this.N_H20.equals(that.N_H20));
    equal = equal && (this.N_H21 == null ? that.N_H21 == null : this.N_H21.equals(that.N_H21));
    equal = equal && (this.N_H22 == null ? that.N_H22 == null : this.N_H22.equals(that.N_H22));
    equal = equal && (this.N_H23 == null ? that.N_H23 == null : this.N_H23.equals(that.N_H23));
    equal = equal && (this.N_H24 == null ? that.N_H24 == null : this.N_H24.equals(that.N_H24));
    equal = equal && (this.N_H25 == null ? that.N_H25 == null : this.N_H25.equals(that.N_H25));
    equal = equal && (this.D_DATA_AGGREGAZIONE == null ? that.D_DATA_AGGREGAZIONE == null : this.D_DATA_AGGREGAZIONE.equals(that.D_DATA_AGGREGAZIONE));
    equal = equal && (this.T_AGGR_SOTTESI == null ? that.T_AGGR_SOTTESI == null : this.T_AGGR_SOTTESI.equals(that.T_AGGR_SOTTESI));
    equal = equal && (this.N_ID_DISTR_RIF == null ? that.N_ID_DISTR_RIF == null : this.N_ID_DISTR_RIF.equals(that.N_ID_DISTR_RIF));
    equal = equal && (this.UID_ELAB == null ? that.UID_ELAB == null : this.UID_ELAB.equals(that.UID_ELAB));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_DISTR = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_AREA_RIF = JdbcWritableBridge.readString(3, __dbResults);
    this.ANNOMESE = JdbcWritableBridge.readString(4, __dbResults);
    this.N_ID_UDD = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.GIORNO = JdbcWritableBridge.readString(6, __dbResults);
    this.N_H1 = JdbcWritableBridge.readBigDecimal(7, __dbResults);
    this.N_H2 = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.N_H3 = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.N_H4 = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.N_H5 = JdbcWritableBridge.readBigDecimal(11, __dbResults);
    this.N_H6 = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.N_H7 = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.N_H8 = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.N_H9 = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.N_H10 = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.N_H11 = JdbcWritableBridge.readBigDecimal(17, __dbResults);
    this.N_H12 = JdbcWritableBridge.readBigDecimal(18, __dbResults);
    this.N_H13 = JdbcWritableBridge.readBigDecimal(19, __dbResults);
    this.N_H14 = JdbcWritableBridge.readBigDecimal(20, __dbResults);
    this.N_H15 = JdbcWritableBridge.readBigDecimal(21, __dbResults);
    this.N_H16 = JdbcWritableBridge.readBigDecimal(22, __dbResults);
    this.N_H17 = JdbcWritableBridge.readBigDecimal(23, __dbResults);
    this.N_H18 = JdbcWritableBridge.readBigDecimal(24, __dbResults);
    this.N_H19 = JdbcWritableBridge.readBigDecimal(25, __dbResults);
    this.N_H20 = JdbcWritableBridge.readBigDecimal(26, __dbResults);
    this.N_H21 = JdbcWritableBridge.readBigDecimal(27, __dbResults);
    this.N_H22 = JdbcWritableBridge.readBigDecimal(28, __dbResults);
    this.N_H23 = JdbcWritableBridge.readBigDecimal(29, __dbResults);
    this.N_H24 = JdbcWritableBridge.readBigDecimal(30, __dbResults);
    this.N_H25 = JdbcWritableBridge.readBigDecimal(31, __dbResults);
    this.D_DATA_AGGREGAZIONE = JdbcWritableBridge.readString(32, __dbResults);
    this.T_AGGR_SOTTESI = JdbcWritableBridge.readString(33, __dbResults);
    this.N_ID_DISTR_RIF = JdbcWritableBridge.readBigDecimal(34, __dbResults);
    this.UID_ELAB = JdbcWritableBridge.readBigDecimal(35, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_DISTR = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.T_AREA_RIF = JdbcWritableBridge.readString(3, __dbResults);
    this.ANNOMESE = JdbcWritableBridge.readString(4, __dbResults);
    this.N_ID_UDD = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.GIORNO = JdbcWritableBridge.readString(6, __dbResults);
    this.N_H1 = JdbcWritableBridge.readBigDecimal(7, __dbResults);
    this.N_H2 = JdbcWritableBridge.readBigDecimal(8, __dbResults);
    this.N_H3 = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.N_H4 = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.N_H5 = JdbcWritableBridge.readBigDecimal(11, __dbResults);
    this.N_H6 = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.N_H7 = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.N_H8 = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.N_H9 = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.N_H10 = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.N_H11 = JdbcWritableBridge.readBigDecimal(17, __dbResults);
    this.N_H12 = JdbcWritableBridge.readBigDecimal(18, __dbResults);
    this.N_H13 = JdbcWritableBridge.readBigDecimal(19, __dbResults);
    this.N_H14 = JdbcWritableBridge.readBigDecimal(20, __dbResults);
    this.N_H15 = JdbcWritableBridge.readBigDecimal(21, __dbResults);
    this.N_H16 = JdbcWritableBridge.readBigDecimal(22, __dbResults);
    this.N_H17 = JdbcWritableBridge.readBigDecimal(23, __dbResults);
    this.N_H18 = JdbcWritableBridge.readBigDecimal(24, __dbResults);
    this.N_H19 = JdbcWritableBridge.readBigDecimal(25, __dbResults);
    this.N_H20 = JdbcWritableBridge.readBigDecimal(26, __dbResults);
    this.N_H21 = JdbcWritableBridge.readBigDecimal(27, __dbResults);
    this.N_H22 = JdbcWritableBridge.readBigDecimal(28, __dbResults);
    this.N_H23 = JdbcWritableBridge.readBigDecimal(29, __dbResults);
    this.N_H24 = JdbcWritableBridge.readBigDecimal(30, __dbResults);
    this.N_H25 = JdbcWritableBridge.readBigDecimal(31, __dbResults);
    this.D_DATA_AGGREGAZIONE = JdbcWritableBridge.readString(32, __dbResults);
    this.T_AGGR_SOTTESI = JdbcWritableBridge.readString(33, __dbResults);
    this.N_ID_DISTR_RIF = JdbcWritableBridge.readBigDecimal(34, __dbResults);
    this.UID_ELAB = JdbcWritableBridge.readBigDecimal(35, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTR, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_AREA_RIF, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ANNOMESE, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UDD, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(GIORNO, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H1, 7 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H2, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H3, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H4, 10 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H5, 11 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H6, 12 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H7, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H8, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H9, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H10, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H11, 17 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H12, 18 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H13, 19 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H14, 20 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H15, 21 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H16, 22 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H17, 23 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H18, 24 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H19, 25 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H20, 26 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H21, 27 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H22, 28 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H23, 29 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H24, 30 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H25, 31 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_AGGREGAZIONE, 32 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_AGGR_SOTTESI, 33 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTR_RIF, 34 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(UID_ELAB, 35 + __off, 2, __dbStmt);
    return 35;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTR, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_AREA_RIF, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ANNOMESE, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UDD, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(GIORNO, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H1, 7 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H2, 8 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H3, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H4, 10 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H5, 11 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H6, 12 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H7, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H8, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H9, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H10, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H11, 17 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H12, 18 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H13, 19 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H14, 20 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H15, 21 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H16, 22 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H17, 23 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H18, 24 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H19, 25 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H20, 26 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H21, 27 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H22, 28 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H23, 29 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H24, 30 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_H25, 31 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_AGGREGAZIONE, 32 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_AGGR_SOTTESI, 33 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_DISTR_RIF, 34 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(UID_ELAB, 35 + __off, 2, __dbStmt);
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
        this.N_ID_DISTR = null;
    } else {
    this.N_ID_DISTR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_AREA_RIF = null;
    } else {
    this.T_AREA_RIF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ANNOMESE = null;
    } else {
    this.ANNOMESE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_UDD = null;
    } else {
    this.N_ID_UDD = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.GIORNO = null;
    } else {
    this.GIORNO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H1 = null;
    } else {
    this.N_H1 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H2 = null;
    } else {
    this.N_H2 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H3 = null;
    } else {
    this.N_H3 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H4 = null;
    } else {
    this.N_H4 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H5 = null;
    } else {
    this.N_H5 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H6 = null;
    } else {
    this.N_H6 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H7 = null;
    } else {
    this.N_H7 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H8 = null;
    } else {
    this.N_H8 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H9 = null;
    } else {
    this.N_H9 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H10 = null;
    } else {
    this.N_H10 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H11 = null;
    } else {
    this.N_H11 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H12 = null;
    } else {
    this.N_H12 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H13 = null;
    } else {
    this.N_H13 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H14 = null;
    } else {
    this.N_H14 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H15 = null;
    } else {
    this.N_H15 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H16 = null;
    } else {
    this.N_H16 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H17 = null;
    } else {
    this.N_H17 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H18 = null;
    } else {
    this.N_H18 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H19 = null;
    } else {
    this.N_H19 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H20 = null;
    } else {
    this.N_H20 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H21 = null;
    } else {
    this.N_H21 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H22 = null;
    } else {
    this.N_H22 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H23 = null;
    } else {
    this.N_H23 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H24 = null;
    } else {
    this.N_H24 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_H25 = null;
    } else {
    this.N_H25 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_AGGREGAZIONE = null;
    } else {
    this.D_DATA_AGGREGAZIONE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_AGGR_SOTTESI = null;
    } else {
    this.T_AGGR_SOTTESI = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_DISTR_RIF = null;
    } else {
    this.N_ID_DISTR_RIF = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.UID_ELAB = null;
    } else {
    this.UID_ELAB = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID, __dataOut);
    }
    if (null == this.N_ID_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISTR, __dataOut);
    }
    if (null == this.T_AREA_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_AREA_RIF);
    }
    if (null == this.ANNOMESE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ANNOMESE);
    }
    if (null == this.N_ID_UDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UDD, __dataOut);
    }
    if (null == this.GIORNO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, GIORNO);
    }
    if (null == this.N_H1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H1, __dataOut);
    }
    if (null == this.N_H2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H2, __dataOut);
    }
    if (null == this.N_H3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H3, __dataOut);
    }
    if (null == this.N_H4) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H4, __dataOut);
    }
    if (null == this.N_H5) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H5, __dataOut);
    }
    if (null == this.N_H6) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H6, __dataOut);
    }
    if (null == this.N_H7) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H7, __dataOut);
    }
    if (null == this.N_H8) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H8, __dataOut);
    }
    if (null == this.N_H9) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H9, __dataOut);
    }
    if (null == this.N_H10) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H10, __dataOut);
    }
    if (null == this.N_H11) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H11, __dataOut);
    }
    if (null == this.N_H12) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H12, __dataOut);
    }
    if (null == this.N_H13) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H13, __dataOut);
    }
    if (null == this.N_H14) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H14, __dataOut);
    }
    if (null == this.N_H15) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H15, __dataOut);
    }
    if (null == this.N_H16) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H16, __dataOut);
    }
    if (null == this.N_H17) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H17, __dataOut);
    }
    if (null == this.N_H18) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H18, __dataOut);
    }
    if (null == this.N_H19) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H19, __dataOut);
    }
    if (null == this.N_H20) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H20, __dataOut);
    }
    if (null == this.N_H21) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H21, __dataOut);
    }
    if (null == this.N_H22) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H22, __dataOut);
    }
    if (null == this.N_H23) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H23, __dataOut);
    }
    if (null == this.N_H24) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H24, __dataOut);
    }
    if (null == this.N_H25) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H25, __dataOut);
    }
    if (null == this.D_DATA_AGGREGAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_AGGREGAZIONE);
    }
    if (null == this.T_AGGR_SOTTESI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_AGGR_SOTTESI);
    }
    if (null == this.N_ID_DISTR_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISTR_RIF, __dataOut);
    }
    if (null == this.UID_ELAB) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.UID_ELAB, __dataOut);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID, __dataOut);
    }
    if (null == this.N_ID_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISTR, __dataOut);
    }
    if (null == this.T_AREA_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_AREA_RIF);
    }
    if (null == this.ANNOMESE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ANNOMESE);
    }
    if (null == this.N_ID_UDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UDD, __dataOut);
    }
    if (null == this.GIORNO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, GIORNO);
    }
    if (null == this.N_H1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H1, __dataOut);
    }
    if (null == this.N_H2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H2, __dataOut);
    }
    if (null == this.N_H3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H3, __dataOut);
    }
    if (null == this.N_H4) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H4, __dataOut);
    }
    if (null == this.N_H5) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H5, __dataOut);
    }
    if (null == this.N_H6) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H6, __dataOut);
    }
    if (null == this.N_H7) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H7, __dataOut);
    }
    if (null == this.N_H8) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H8, __dataOut);
    }
    if (null == this.N_H9) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H9, __dataOut);
    }
    if (null == this.N_H10) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H10, __dataOut);
    }
    if (null == this.N_H11) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H11, __dataOut);
    }
    if (null == this.N_H12) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H12, __dataOut);
    }
    if (null == this.N_H13) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H13, __dataOut);
    }
    if (null == this.N_H14) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H14, __dataOut);
    }
    if (null == this.N_H15) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H15, __dataOut);
    }
    if (null == this.N_H16) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H16, __dataOut);
    }
    if (null == this.N_H17) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H17, __dataOut);
    }
    if (null == this.N_H18) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H18, __dataOut);
    }
    if (null == this.N_H19) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H19, __dataOut);
    }
    if (null == this.N_H20) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H20, __dataOut);
    }
    if (null == this.N_H21) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H21, __dataOut);
    }
    if (null == this.N_H22) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H22, __dataOut);
    }
    if (null == this.N_H23) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H23, __dataOut);
    }
    if (null == this.N_H24) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H24, __dataOut);
    }
    if (null == this.N_H25) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_H25, __dataOut);
    }
    if (null == this.D_DATA_AGGREGAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_AGGREGAZIONE);
    }
    if (null == this.T_AGGR_SOTTESI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_AGGR_SOTTESI);
    }
    if (null == this.N_ID_DISTR_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_DISTR_RIF, __dataOut);
    }
    if (null == this.UID_ELAB) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.UID_ELAB, __dataOut);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISTR==null?"":N_ID_DISTR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_AREA_RIF==null?"":T_AREA_RIF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(ANNOMESE==null?"":ANNOMESE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UDD==null?"":N_ID_UDD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(GIORNO==null?"":GIORNO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H1==null?"":N_H1.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H2==null?"":N_H2.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H3==null?"":N_H3.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H4==null?"":N_H4.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H5==null?"":N_H5.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H6==null?"":N_H6.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H7==null?"":N_H7.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H8==null?"":N_H8.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H9==null?"":N_H9.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H10==null?"":N_H10.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H11==null?"":N_H11.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H12==null?"":N_H12.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H13==null?"":N_H13.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H14==null?"":N_H14.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H15==null?"":N_H15.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H16==null?"":N_H16.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H17==null?"":N_H17.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H18==null?"":N_H18.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H19==null?"":N_H19.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H20==null?"":N_H20.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H21==null?"":N_H21.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H22==null?"":N_H22.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H23==null?"":N_H23.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H24==null?"":N_H24.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H25==null?"":N_H25.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_AGGREGAZIONE==null?"":D_DATA_AGGREGAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_AGGR_SOTTESI==null?"":T_AGGR_SOTTESI, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISTR_RIF==null?"":N_ID_DISTR_RIF.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(UID_ELAB==null?"":UID_ELAB.toPlainString(), delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID==null?"":N_ID.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISTR==null?"":N_ID_DISTR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_AREA_RIF==null?"":T_AREA_RIF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(ANNOMESE==null?"":ANNOMESE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UDD==null?"":N_ID_UDD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(GIORNO==null?"":GIORNO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H1==null?"":N_H1.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H2==null?"":N_H2.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H3==null?"":N_H3.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H4==null?"":N_H4.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H5==null?"":N_H5.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H6==null?"":N_H6.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H7==null?"":N_H7.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H8==null?"":N_H8.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H9==null?"":N_H9.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H10==null?"":N_H10.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H11==null?"":N_H11.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H12==null?"":N_H12.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H13==null?"":N_H13.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H14==null?"":N_H14.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H15==null?"":N_H15.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H16==null?"":N_H16.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H17==null?"":N_H17.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H18==null?"":N_H18.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H19==null?"":N_H19.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H20==null?"":N_H20.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H21==null?"":N_H21.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H22==null?"":N_H22.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H23==null?"":N_H23.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H24==null?"":N_H24.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_H25==null?"":N_H25.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_AGGREGAZIONE==null?"":D_DATA_AGGREGAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_AGGR_SOTTESI==null?"":T_AGGR_SOTTESI, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_DISTR_RIF==null?"":N_ID_DISTR_RIF.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(UID_ELAB==null?"":UID_ELAB.toPlainString(), delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISTR = null; } else {
      this.N_ID_DISTR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_AREA_RIF = null; } else {
      this.T_AREA_RIF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ANNOMESE = null; } else {
      this.ANNOMESE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UDD = null; } else {
      this.N_ID_UDD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.GIORNO = null; } else {
      this.GIORNO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H1 = null; } else {
      this.N_H1 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H2 = null; } else {
      this.N_H2 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H3 = null; } else {
      this.N_H3 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H4 = null; } else {
      this.N_H4 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H5 = null; } else {
      this.N_H5 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H6 = null; } else {
      this.N_H6 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H7 = null; } else {
      this.N_H7 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H8 = null; } else {
      this.N_H8 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H9 = null; } else {
      this.N_H9 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H10 = null; } else {
      this.N_H10 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H11 = null; } else {
      this.N_H11 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H12 = null; } else {
      this.N_H12 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H13 = null; } else {
      this.N_H13 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H14 = null; } else {
      this.N_H14 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H15 = null; } else {
      this.N_H15 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H16 = null; } else {
      this.N_H16 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H17 = null; } else {
      this.N_H17 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H18 = null; } else {
      this.N_H18 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H19 = null; } else {
      this.N_H19 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H20 = null; } else {
      this.N_H20 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H21 = null; } else {
      this.N_H21 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H22 = null; } else {
      this.N_H22 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H23 = null; } else {
      this.N_H23 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H24 = null; } else {
      this.N_H24 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H25 = null; } else {
      this.N_H25 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_AGGREGAZIONE = null; } else {
      this.D_DATA_AGGREGAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_AGGR_SOTTESI = null; } else {
      this.T_AGGR_SOTTESI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISTR_RIF = null; } else {
      this.N_ID_DISTR_RIF = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.UID_ELAB = null; } else {
      this.UID_ELAB = new java.math.BigDecimal(__cur_str);
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISTR = null; } else {
      this.N_ID_DISTR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_AREA_RIF = null; } else {
      this.T_AREA_RIF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ANNOMESE = null; } else {
      this.ANNOMESE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UDD = null; } else {
      this.N_ID_UDD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.GIORNO = null; } else {
      this.GIORNO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H1 = null; } else {
      this.N_H1 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H2 = null; } else {
      this.N_H2 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H3 = null; } else {
      this.N_H3 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H4 = null; } else {
      this.N_H4 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H5 = null; } else {
      this.N_H5 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H6 = null; } else {
      this.N_H6 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H7 = null; } else {
      this.N_H7 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H8 = null; } else {
      this.N_H8 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H9 = null; } else {
      this.N_H9 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H10 = null; } else {
      this.N_H10 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H11 = null; } else {
      this.N_H11 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H12 = null; } else {
      this.N_H12 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H13 = null; } else {
      this.N_H13 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H14 = null; } else {
      this.N_H14 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H15 = null; } else {
      this.N_H15 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H16 = null; } else {
      this.N_H16 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H17 = null; } else {
      this.N_H17 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H18 = null; } else {
      this.N_H18 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H19 = null; } else {
      this.N_H19 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H20 = null; } else {
      this.N_H20 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H21 = null; } else {
      this.N_H21 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H22 = null; } else {
      this.N_H22 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H23 = null; } else {
      this.N_H23 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H24 = null; } else {
      this.N_H24 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_H25 = null; } else {
      this.N_H25 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_AGGREGAZIONE = null; } else {
      this.D_DATA_AGGREGAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_AGGR_SOTTESI = null; } else {
      this.T_AGGR_SOTTESI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_DISTR_RIF = null; } else {
      this.N_ID_DISTR_RIF = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.UID_ELAB = null; } else {
      this.UID_ELAB = new java.math.BigDecimal(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    tmpod_prt_tmo_aggregati_calcolati o = (tmpod_prt_tmo_aggregati_calcolati) super.clone();
    return o;
  }

  public void clone0(tmpod_prt_tmo_aggregati_calcolati o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID", this.N_ID);
    __sqoop$field_map.put("N_ID_DISTR", this.N_ID_DISTR);
    __sqoop$field_map.put("T_AREA_RIF", this.T_AREA_RIF);
    __sqoop$field_map.put("ANNOMESE", this.ANNOMESE);
    __sqoop$field_map.put("N_ID_UDD", this.N_ID_UDD);
    __sqoop$field_map.put("GIORNO", this.GIORNO);
    __sqoop$field_map.put("N_H1", this.N_H1);
    __sqoop$field_map.put("N_H2", this.N_H2);
    __sqoop$field_map.put("N_H3", this.N_H3);
    __sqoop$field_map.put("N_H4", this.N_H4);
    __sqoop$field_map.put("N_H5", this.N_H5);
    __sqoop$field_map.put("N_H6", this.N_H6);
    __sqoop$field_map.put("N_H7", this.N_H7);
    __sqoop$field_map.put("N_H8", this.N_H8);
    __sqoop$field_map.put("N_H9", this.N_H9);
    __sqoop$field_map.put("N_H10", this.N_H10);
    __sqoop$field_map.put("N_H11", this.N_H11);
    __sqoop$field_map.put("N_H12", this.N_H12);
    __sqoop$field_map.put("N_H13", this.N_H13);
    __sqoop$field_map.put("N_H14", this.N_H14);
    __sqoop$field_map.put("N_H15", this.N_H15);
    __sqoop$field_map.put("N_H16", this.N_H16);
    __sqoop$field_map.put("N_H17", this.N_H17);
    __sqoop$field_map.put("N_H18", this.N_H18);
    __sqoop$field_map.put("N_H19", this.N_H19);
    __sqoop$field_map.put("N_H20", this.N_H20);
    __sqoop$field_map.put("N_H21", this.N_H21);
    __sqoop$field_map.put("N_H22", this.N_H22);
    __sqoop$field_map.put("N_H23", this.N_H23);
    __sqoop$field_map.put("N_H24", this.N_H24);
    __sqoop$field_map.put("N_H25", this.N_H25);
    __sqoop$field_map.put("D_DATA_AGGREGAZIONE", this.D_DATA_AGGREGAZIONE);
    __sqoop$field_map.put("T_AGGR_SOTTESI", this.T_AGGR_SOTTESI);
    __sqoop$field_map.put("N_ID_DISTR_RIF", this.N_ID_DISTR_RIF);
    __sqoop$field_map.put("UID_ELAB", this.UID_ELAB);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID", this.N_ID);
    __sqoop$field_map.put("N_ID_DISTR", this.N_ID_DISTR);
    __sqoop$field_map.put("T_AREA_RIF", this.T_AREA_RIF);
    __sqoop$field_map.put("ANNOMESE", this.ANNOMESE);
    __sqoop$field_map.put("N_ID_UDD", this.N_ID_UDD);
    __sqoop$field_map.put("GIORNO", this.GIORNO);
    __sqoop$field_map.put("N_H1", this.N_H1);
    __sqoop$field_map.put("N_H2", this.N_H2);
    __sqoop$field_map.put("N_H3", this.N_H3);
    __sqoop$field_map.put("N_H4", this.N_H4);
    __sqoop$field_map.put("N_H5", this.N_H5);
    __sqoop$field_map.put("N_H6", this.N_H6);
    __sqoop$field_map.put("N_H7", this.N_H7);
    __sqoop$field_map.put("N_H8", this.N_H8);
    __sqoop$field_map.put("N_H9", this.N_H9);
    __sqoop$field_map.put("N_H10", this.N_H10);
    __sqoop$field_map.put("N_H11", this.N_H11);
    __sqoop$field_map.put("N_H12", this.N_H12);
    __sqoop$field_map.put("N_H13", this.N_H13);
    __sqoop$field_map.put("N_H14", this.N_H14);
    __sqoop$field_map.put("N_H15", this.N_H15);
    __sqoop$field_map.put("N_H16", this.N_H16);
    __sqoop$field_map.put("N_H17", this.N_H17);
    __sqoop$field_map.put("N_H18", this.N_H18);
    __sqoop$field_map.put("N_H19", this.N_H19);
    __sqoop$field_map.put("N_H20", this.N_H20);
    __sqoop$field_map.put("N_H21", this.N_H21);
    __sqoop$field_map.put("N_H22", this.N_H22);
    __sqoop$field_map.put("N_H23", this.N_H23);
    __sqoop$field_map.put("N_H24", this.N_H24);
    __sqoop$field_map.put("N_H25", this.N_H25);
    __sqoop$field_map.put("D_DATA_AGGREGAZIONE", this.D_DATA_AGGREGAZIONE);
    __sqoop$field_map.put("T_AGGR_SOTTESI", this.T_AGGR_SOTTESI);
    __sqoop$field_map.put("N_ID_DISTR_RIF", this.N_ID_DISTR_RIF);
    __sqoop$field_map.put("UID_ELAB", this.UID_ELAB);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
