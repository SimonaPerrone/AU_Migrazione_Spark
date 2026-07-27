// ORM class for table 'rcugas.rcugas_fornitura'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 11:51:07 CEST 2019
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

public class rcugas_rcugas_fornitura extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_FORNITURA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_FORNITURA = (java.math.BigDecimal)value;
      }
    });
    setters.put("D_DATA_INIZIO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_INIZIO = (String)value;
      }
    });
    setters.put("D_DATA_FINE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_FINE = (String)value;
      }
    });
    setters.put("N_ID_CLIENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_CLIENTE = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_PDR = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_VEND", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_VEND = (java.math.BigDecimal)value;
      }
    });
    setters.put("B_TARIFFA_TM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_TARIFFA_TM = (String)value;
      }
    });
    setters.put("T_CODICE_ATECO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_ATECO = (String)value;
      }
    });
    setters.put("N_LETTURA_ATTIVAZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_LETTURA_ATTIVAZIONE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_ALIQUOTA_IVA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ALIQUOTA_IVA = (String)value;
      }
    });
    setters.put("T_IMPOSTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_IMPOSTE = (String)value;
      }
    });
    setters.put("N_INDIRIZZO_FORNITURA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_INDIRIZZO_FORNITURA = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_INDIRIZZO_RECAP", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_INDIRIZZO_RECAP = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_BONUS_GAS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_BONUS_GAS = (String)value;
      }
    });
    setters.put("D_DATA_INIZIO_BONUS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_INIZIO_BONUS = (String)value;
      }
    });
    setters.put("D_DATA_FINE_BONUS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_FINE_BONUS = (String)value;
      }
    });
    setters.put("B_PRESTAZIONI_NON_CONCLUSE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_PRESTAZIONI_NON_CONCLUSE = (String)value;
      }
    });
    setters.put("B_DISALIMENTABILITA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_DISALIMENTABILITA = (String)value;
      }
    });
    setters.put("T_CODICE_CONTRATTO_VENDITA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_CONTRATTO_VENDITA = (String)value;
      }
    });
    setters.put("T_ID_CONTRATTO_VEND", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ID_CONTRATTO_VEND = (String)value;
      }
    });
    setters.put("D_DATA_STIPULA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_STIPULA = (String)value;
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
    setters.put("TIPO_DATA_INIZIO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TIPO_DATA_INIZIO = (String)value;
      }
    });
    setters.put("TIPO_DATA_FINE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TIPO_DATA_FINE = (String)value;
      }
    });
    setters.put("D_DATA_RIF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_DATA_RIF = (String)value;
      }
    });
    setters.put("T_TIPO_FORNITURA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO_FORNITURA = (String)value;
      }
    });
    setters.put("N_INDIRIZZO_FATT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_INDIRIZZO_FATT = (java.math.BigDecimal)value;
      }
    });
  }
  public rcugas_rcugas_fornitura() {
    init0();
  }
  private java.math.BigDecimal N_ID_FORNITURA;
  public java.math.BigDecimal get_N_ID_FORNITURA() {
    return N_ID_FORNITURA;
  }
  public void set_N_ID_FORNITURA(java.math.BigDecimal N_ID_FORNITURA) {
    this.N_ID_FORNITURA = N_ID_FORNITURA;
  }
  public rcugas_rcugas_fornitura with_N_ID_FORNITURA(java.math.BigDecimal N_ID_FORNITURA) {
    this.N_ID_FORNITURA = N_ID_FORNITURA;
    return this;
  }
  private String D_DATA_INIZIO;
  public String get_D_DATA_INIZIO() {
    return D_DATA_INIZIO;
  }
  public void set_D_DATA_INIZIO(String D_DATA_INIZIO) {
    this.D_DATA_INIZIO = D_DATA_INIZIO;
  }
  public rcugas_rcugas_fornitura with_D_DATA_INIZIO(String D_DATA_INIZIO) {
    this.D_DATA_INIZIO = D_DATA_INIZIO;
    return this;
  }
  private String D_DATA_FINE;
  public String get_D_DATA_FINE() {
    return D_DATA_FINE;
  }
  public void set_D_DATA_FINE(String D_DATA_FINE) {
    this.D_DATA_FINE = D_DATA_FINE;
  }
  public rcugas_rcugas_fornitura with_D_DATA_FINE(String D_DATA_FINE) {
    this.D_DATA_FINE = D_DATA_FINE;
    return this;
  }
  private java.math.BigDecimal N_ID_CLIENTE;
  public java.math.BigDecimal get_N_ID_CLIENTE() {
    return N_ID_CLIENTE;
  }
  public void set_N_ID_CLIENTE(java.math.BigDecimal N_ID_CLIENTE) {
    this.N_ID_CLIENTE = N_ID_CLIENTE;
  }
  public rcugas_rcugas_fornitura with_N_ID_CLIENTE(java.math.BigDecimal N_ID_CLIENTE) {
    this.N_ID_CLIENTE = N_ID_CLIENTE;
    return this;
  }
  private java.math.BigDecimal N_ID_PDR;
  public java.math.BigDecimal get_N_ID_PDR() {
    return N_ID_PDR;
  }
  public void set_N_ID_PDR(java.math.BigDecimal N_ID_PDR) {
    this.N_ID_PDR = N_ID_PDR;
  }
  public rcugas_rcugas_fornitura with_N_ID_PDR(java.math.BigDecimal N_ID_PDR) {
    this.N_ID_PDR = N_ID_PDR;
    return this;
  }
  private java.math.BigDecimal N_ID_VEND;
  public java.math.BigDecimal get_N_ID_VEND() {
    return N_ID_VEND;
  }
  public void set_N_ID_VEND(java.math.BigDecimal N_ID_VEND) {
    this.N_ID_VEND = N_ID_VEND;
  }
  public rcugas_rcugas_fornitura with_N_ID_VEND(java.math.BigDecimal N_ID_VEND) {
    this.N_ID_VEND = N_ID_VEND;
    return this;
  }
  private String B_TARIFFA_TM;
  public String get_B_TARIFFA_TM() {
    return B_TARIFFA_TM;
  }
  public void set_B_TARIFFA_TM(String B_TARIFFA_TM) {
    this.B_TARIFFA_TM = B_TARIFFA_TM;
  }
  public rcugas_rcugas_fornitura with_B_TARIFFA_TM(String B_TARIFFA_TM) {
    this.B_TARIFFA_TM = B_TARIFFA_TM;
    return this;
  }
  private String T_CODICE_ATECO;
  public String get_T_CODICE_ATECO() {
    return T_CODICE_ATECO;
  }
  public void set_T_CODICE_ATECO(String T_CODICE_ATECO) {
    this.T_CODICE_ATECO = T_CODICE_ATECO;
  }
  public rcugas_rcugas_fornitura with_T_CODICE_ATECO(String T_CODICE_ATECO) {
    this.T_CODICE_ATECO = T_CODICE_ATECO;
    return this;
  }
  private java.math.BigDecimal N_LETTURA_ATTIVAZIONE;
  public java.math.BigDecimal get_N_LETTURA_ATTIVAZIONE() {
    return N_LETTURA_ATTIVAZIONE;
  }
  public void set_N_LETTURA_ATTIVAZIONE(java.math.BigDecimal N_LETTURA_ATTIVAZIONE) {
    this.N_LETTURA_ATTIVAZIONE = N_LETTURA_ATTIVAZIONE;
  }
  public rcugas_rcugas_fornitura with_N_LETTURA_ATTIVAZIONE(java.math.BigDecimal N_LETTURA_ATTIVAZIONE) {
    this.N_LETTURA_ATTIVAZIONE = N_LETTURA_ATTIVAZIONE;
    return this;
  }
  private String T_ALIQUOTA_IVA;
  public String get_T_ALIQUOTA_IVA() {
    return T_ALIQUOTA_IVA;
  }
  public void set_T_ALIQUOTA_IVA(String T_ALIQUOTA_IVA) {
    this.T_ALIQUOTA_IVA = T_ALIQUOTA_IVA;
  }
  public rcugas_rcugas_fornitura with_T_ALIQUOTA_IVA(String T_ALIQUOTA_IVA) {
    this.T_ALIQUOTA_IVA = T_ALIQUOTA_IVA;
    return this;
  }
  private String T_IMPOSTE;
  public String get_T_IMPOSTE() {
    return T_IMPOSTE;
  }
  public void set_T_IMPOSTE(String T_IMPOSTE) {
    this.T_IMPOSTE = T_IMPOSTE;
  }
  public rcugas_rcugas_fornitura with_T_IMPOSTE(String T_IMPOSTE) {
    this.T_IMPOSTE = T_IMPOSTE;
    return this;
  }
  private java.math.BigDecimal N_INDIRIZZO_FORNITURA;
  public java.math.BigDecimal get_N_INDIRIZZO_FORNITURA() {
    return N_INDIRIZZO_FORNITURA;
  }
  public void set_N_INDIRIZZO_FORNITURA(java.math.BigDecimal N_INDIRIZZO_FORNITURA) {
    this.N_INDIRIZZO_FORNITURA = N_INDIRIZZO_FORNITURA;
  }
  public rcugas_rcugas_fornitura with_N_INDIRIZZO_FORNITURA(java.math.BigDecimal N_INDIRIZZO_FORNITURA) {
    this.N_INDIRIZZO_FORNITURA = N_INDIRIZZO_FORNITURA;
    return this;
  }
  private java.math.BigDecimal N_INDIRIZZO_RECAP;
  public java.math.BigDecimal get_N_INDIRIZZO_RECAP() {
    return N_INDIRIZZO_RECAP;
  }
  public void set_N_INDIRIZZO_RECAP(java.math.BigDecimal N_INDIRIZZO_RECAP) {
    this.N_INDIRIZZO_RECAP = N_INDIRIZZO_RECAP;
  }
  public rcugas_rcugas_fornitura with_N_INDIRIZZO_RECAP(java.math.BigDecimal N_INDIRIZZO_RECAP) {
    this.N_INDIRIZZO_RECAP = N_INDIRIZZO_RECAP;
    return this;
  }
  private String T_BONUS_GAS;
  public String get_T_BONUS_GAS() {
    return T_BONUS_GAS;
  }
  public void set_T_BONUS_GAS(String T_BONUS_GAS) {
    this.T_BONUS_GAS = T_BONUS_GAS;
  }
  public rcugas_rcugas_fornitura with_T_BONUS_GAS(String T_BONUS_GAS) {
    this.T_BONUS_GAS = T_BONUS_GAS;
    return this;
  }
  private String D_DATA_INIZIO_BONUS;
  public String get_D_DATA_INIZIO_BONUS() {
    return D_DATA_INIZIO_BONUS;
  }
  public void set_D_DATA_INIZIO_BONUS(String D_DATA_INIZIO_BONUS) {
    this.D_DATA_INIZIO_BONUS = D_DATA_INIZIO_BONUS;
  }
  public rcugas_rcugas_fornitura with_D_DATA_INIZIO_BONUS(String D_DATA_INIZIO_BONUS) {
    this.D_DATA_INIZIO_BONUS = D_DATA_INIZIO_BONUS;
    return this;
  }
  private String D_DATA_FINE_BONUS;
  public String get_D_DATA_FINE_BONUS() {
    return D_DATA_FINE_BONUS;
  }
  public void set_D_DATA_FINE_BONUS(String D_DATA_FINE_BONUS) {
    this.D_DATA_FINE_BONUS = D_DATA_FINE_BONUS;
  }
  public rcugas_rcugas_fornitura with_D_DATA_FINE_BONUS(String D_DATA_FINE_BONUS) {
    this.D_DATA_FINE_BONUS = D_DATA_FINE_BONUS;
    return this;
  }
  private String B_PRESTAZIONI_NON_CONCLUSE;
  public String get_B_PRESTAZIONI_NON_CONCLUSE() {
    return B_PRESTAZIONI_NON_CONCLUSE;
  }
  public void set_B_PRESTAZIONI_NON_CONCLUSE(String B_PRESTAZIONI_NON_CONCLUSE) {
    this.B_PRESTAZIONI_NON_CONCLUSE = B_PRESTAZIONI_NON_CONCLUSE;
  }
  public rcugas_rcugas_fornitura with_B_PRESTAZIONI_NON_CONCLUSE(String B_PRESTAZIONI_NON_CONCLUSE) {
    this.B_PRESTAZIONI_NON_CONCLUSE = B_PRESTAZIONI_NON_CONCLUSE;
    return this;
  }
  private String B_DISALIMENTABILITA;
  public String get_B_DISALIMENTABILITA() {
    return B_DISALIMENTABILITA;
  }
  public void set_B_DISALIMENTABILITA(String B_DISALIMENTABILITA) {
    this.B_DISALIMENTABILITA = B_DISALIMENTABILITA;
  }
  public rcugas_rcugas_fornitura with_B_DISALIMENTABILITA(String B_DISALIMENTABILITA) {
    this.B_DISALIMENTABILITA = B_DISALIMENTABILITA;
    return this;
  }
  private String T_CODICE_CONTRATTO_VENDITA;
  public String get_T_CODICE_CONTRATTO_VENDITA() {
    return T_CODICE_CONTRATTO_VENDITA;
  }
  public void set_T_CODICE_CONTRATTO_VENDITA(String T_CODICE_CONTRATTO_VENDITA) {
    this.T_CODICE_CONTRATTO_VENDITA = T_CODICE_CONTRATTO_VENDITA;
  }
  public rcugas_rcugas_fornitura with_T_CODICE_CONTRATTO_VENDITA(String T_CODICE_CONTRATTO_VENDITA) {
    this.T_CODICE_CONTRATTO_VENDITA = T_CODICE_CONTRATTO_VENDITA;
    return this;
  }
  private String T_ID_CONTRATTO_VEND;
  public String get_T_ID_CONTRATTO_VEND() {
    return T_ID_CONTRATTO_VEND;
  }
  public void set_T_ID_CONTRATTO_VEND(String T_ID_CONTRATTO_VEND) {
    this.T_ID_CONTRATTO_VEND = T_ID_CONTRATTO_VEND;
  }
  public rcugas_rcugas_fornitura with_T_ID_CONTRATTO_VEND(String T_ID_CONTRATTO_VEND) {
    this.T_ID_CONTRATTO_VEND = T_ID_CONTRATTO_VEND;
    return this;
  }
  private String D_DATA_STIPULA;
  public String get_D_DATA_STIPULA() {
    return D_DATA_STIPULA;
  }
  public void set_D_DATA_STIPULA(String D_DATA_STIPULA) {
    this.D_DATA_STIPULA = D_DATA_STIPULA;
  }
  public rcugas_rcugas_fornitura with_D_DATA_STIPULA(String D_DATA_STIPULA) {
    this.D_DATA_STIPULA = D_DATA_STIPULA;
    return this;
  }
  private String T_NOTE;
  public String get_T_NOTE() {
    return T_NOTE;
  }
  public void set_T_NOTE(String T_NOTE) {
    this.T_NOTE = T_NOTE;
  }
  public rcugas_rcugas_fornitura with_T_NOTE(String T_NOTE) {
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
  public rcugas_rcugas_fornitura with_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
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
  public rcugas_rcugas_fornitura with_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
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
  public rcugas_rcugas_fornitura with_N_ID_S_PREC(java.math.BigDecimal N_ID_S_PREC) {
    this.N_ID_S_PREC = N_ID_S_PREC;
    return this;
  }
  private String TIPO_DATA_INIZIO;
  public String get_TIPO_DATA_INIZIO() {
    return TIPO_DATA_INIZIO;
  }
  public void set_TIPO_DATA_INIZIO(String TIPO_DATA_INIZIO) {
    this.TIPO_DATA_INIZIO = TIPO_DATA_INIZIO;
  }
  public rcugas_rcugas_fornitura with_TIPO_DATA_INIZIO(String TIPO_DATA_INIZIO) {
    this.TIPO_DATA_INIZIO = TIPO_DATA_INIZIO;
    return this;
  }
  private String TIPO_DATA_FINE;
  public String get_TIPO_DATA_FINE() {
    return TIPO_DATA_FINE;
  }
  public void set_TIPO_DATA_FINE(String TIPO_DATA_FINE) {
    this.TIPO_DATA_FINE = TIPO_DATA_FINE;
  }
  public rcugas_rcugas_fornitura with_TIPO_DATA_FINE(String TIPO_DATA_FINE) {
    this.TIPO_DATA_FINE = TIPO_DATA_FINE;
    return this;
  }
  private String D_DATA_RIF;
  public String get_D_DATA_RIF() {
    return D_DATA_RIF;
  }
  public void set_D_DATA_RIF(String D_DATA_RIF) {
    this.D_DATA_RIF = D_DATA_RIF;
  }
  public rcugas_rcugas_fornitura with_D_DATA_RIF(String D_DATA_RIF) {
    this.D_DATA_RIF = D_DATA_RIF;
    return this;
  }
  private String T_TIPO_FORNITURA;
  public String get_T_TIPO_FORNITURA() {
    return T_TIPO_FORNITURA;
  }
  public void set_T_TIPO_FORNITURA(String T_TIPO_FORNITURA) {
    this.T_TIPO_FORNITURA = T_TIPO_FORNITURA;
  }
  public rcugas_rcugas_fornitura with_T_TIPO_FORNITURA(String T_TIPO_FORNITURA) {
    this.T_TIPO_FORNITURA = T_TIPO_FORNITURA;
    return this;
  }
  private java.math.BigDecimal N_INDIRIZZO_FATT;
  public java.math.BigDecimal get_N_INDIRIZZO_FATT() {
    return N_INDIRIZZO_FATT;
  }
  public void set_N_INDIRIZZO_FATT(java.math.BigDecimal N_INDIRIZZO_FATT) {
    this.N_INDIRIZZO_FATT = N_INDIRIZZO_FATT;
  }
  public rcugas_rcugas_fornitura with_N_INDIRIZZO_FATT(java.math.BigDecimal N_INDIRIZZO_FATT) {
    this.N_INDIRIZZO_FATT = N_INDIRIZZO_FATT;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcugas_rcugas_fornitura)) {
      return false;
    }
    rcugas_rcugas_fornitura that = (rcugas_rcugas_fornitura) o;
    boolean equal = true;
    equal = equal && (this.N_ID_FORNITURA == null ? that.N_ID_FORNITURA == null : this.N_ID_FORNITURA.equals(that.N_ID_FORNITURA));
    equal = equal && (this.D_DATA_INIZIO == null ? that.D_DATA_INIZIO == null : this.D_DATA_INIZIO.equals(that.D_DATA_INIZIO));
    equal = equal && (this.D_DATA_FINE == null ? that.D_DATA_FINE == null : this.D_DATA_FINE.equals(that.D_DATA_FINE));
    equal = equal && (this.N_ID_CLIENTE == null ? that.N_ID_CLIENTE == null : this.N_ID_CLIENTE.equals(that.N_ID_CLIENTE));
    equal = equal && (this.N_ID_PDR == null ? that.N_ID_PDR == null : this.N_ID_PDR.equals(that.N_ID_PDR));
    equal = equal && (this.N_ID_VEND == null ? that.N_ID_VEND == null : this.N_ID_VEND.equals(that.N_ID_VEND));
    equal = equal && (this.B_TARIFFA_TM == null ? that.B_TARIFFA_TM == null : this.B_TARIFFA_TM.equals(that.B_TARIFFA_TM));
    equal = equal && (this.T_CODICE_ATECO == null ? that.T_CODICE_ATECO == null : this.T_CODICE_ATECO.equals(that.T_CODICE_ATECO));
    equal = equal && (this.N_LETTURA_ATTIVAZIONE == null ? that.N_LETTURA_ATTIVAZIONE == null : this.N_LETTURA_ATTIVAZIONE.equals(that.N_LETTURA_ATTIVAZIONE));
    equal = equal && (this.T_ALIQUOTA_IVA == null ? that.T_ALIQUOTA_IVA == null : this.T_ALIQUOTA_IVA.equals(that.T_ALIQUOTA_IVA));
    equal = equal && (this.T_IMPOSTE == null ? that.T_IMPOSTE == null : this.T_IMPOSTE.equals(that.T_IMPOSTE));
    equal = equal && (this.N_INDIRIZZO_FORNITURA == null ? that.N_INDIRIZZO_FORNITURA == null : this.N_INDIRIZZO_FORNITURA.equals(that.N_INDIRIZZO_FORNITURA));
    equal = equal && (this.N_INDIRIZZO_RECAP == null ? that.N_INDIRIZZO_RECAP == null : this.N_INDIRIZZO_RECAP.equals(that.N_INDIRIZZO_RECAP));
    equal = equal && (this.T_BONUS_GAS == null ? that.T_BONUS_GAS == null : this.T_BONUS_GAS.equals(that.T_BONUS_GAS));
    equal = equal && (this.D_DATA_INIZIO_BONUS == null ? that.D_DATA_INIZIO_BONUS == null : this.D_DATA_INIZIO_BONUS.equals(that.D_DATA_INIZIO_BONUS));
    equal = equal && (this.D_DATA_FINE_BONUS == null ? that.D_DATA_FINE_BONUS == null : this.D_DATA_FINE_BONUS.equals(that.D_DATA_FINE_BONUS));
    equal = equal && (this.B_PRESTAZIONI_NON_CONCLUSE == null ? that.B_PRESTAZIONI_NON_CONCLUSE == null : this.B_PRESTAZIONI_NON_CONCLUSE.equals(that.B_PRESTAZIONI_NON_CONCLUSE));
    equal = equal && (this.B_DISALIMENTABILITA == null ? that.B_DISALIMENTABILITA == null : this.B_DISALIMENTABILITA.equals(that.B_DISALIMENTABILITA));
    equal = equal && (this.T_CODICE_CONTRATTO_VENDITA == null ? that.T_CODICE_CONTRATTO_VENDITA == null : this.T_CODICE_CONTRATTO_VENDITA.equals(that.T_CODICE_CONTRATTO_VENDITA));
    equal = equal && (this.T_ID_CONTRATTO_VEND == null ? that.T_ID_CONTRATTO_VEND == null : this.T_ID_CONTRATTO_VEND.equals(that.T_ID_CONTRATTO_VEND));
    equal = equal && (this.D_DATA_STIPULA == null ? that.D_DATA_STIPULA == null : this.D_DATA_STIPULA.equals(that.D_DATA_STIPULA));
    equal = equal && (this.T_NOTE == null ? that.T_NOTE == null : this.T_NOTE.equals(that.T_NOTE));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.TIPO_DATA_INIZIO == null ? that.TIPO_DATA_INIZIO == null : this.TIPO_DATA_INIZIO.equals(that.TIPO_DATA_INIZIO));
    equal = equal && (this.TIPO_DATA_FINE == null ? that.TIPO_DATA_FINE == null : this.TIPO_DATA_FINE.equals(that.TIPO_DATA_FINE));
    equal = equal && (this.D_DATA_RIF == null ? that.D_DATA_RIF == null : this.D_DATA_RIF.equals(that.D_DATA_RIF));
    equal = equal && (this.T_TIPO_FORNITURA == null ? that.T_TIPO_FORNITURA == null : this.T_TIPO_FORNITURA.equals(that.T_TIPO_FORNITURA));
    equal = equal && (this.N_INDIRIZZO_FATT == null ? that.N_INDIRIZZO_FATT == null : this.N_INDIRIZZO_FATT.equals(that.N_INDIRIZZO_FATT));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcugas_rcugas_fornitura)) {
      return false;
    }
    rcugas_rcugas_fornitura that = (rcugas_rcugas_fornitura) o;
    boolean equal = true;
    equal = equal && (this.N_ID_FORNITURA == null ? that.N_ID_FORNITURA == null : this.N_ID_FORNITURA.equals(that.N_ID_FORNITURA));
    equal = equal && (this.D_DATA_INIZIO == null ? that.D_DATA_INIZIO == null : this.D_DATA_INIZIO.equals(that.D_DATA_INIZIO));
    equal = equal && (this.D_DATA_FINE == null ? that.D_DATA_FINE == null : this.D_DATA_FINE.equals(that.D_DATA_FINE));
    equal = equal && (this.N_ID_CLIENTE == null ? that.N_ID_CLIENTE == null : this.N_ID_CLIENTE.equals(that.N_ID_CLIENTE));
    equal = equal && (this.N_ID_PDR == null ? that.N_ID_PDR == null : this.N_ID_PDR.equals(that.N_ID_PDR));
    equal = equal && (this.N_ID_VEND == null ? that.N_ID_VEND == null : this.N_ID_VEND.equals(that.N_ID_VEND));
    equal = equal && (this.B_TARIFFA_TM == null ? that.B_TARIFFA_TM == null : this.B_TARIFFA_TM.equals(that.B_TARIFFA_TM));
    equal = equal && (this.T_CODICE_ATECO == null ? that.T_CODICE_ATECO == null : this.T_CODICE_ATECO.equals(that.T_CODICE_ATECO));
    equal = equal && (this.N_LETTURA_ATTIVAZIONE == null ? that.N_LETTURA_ATTIVAZIONE == null : this.N_LETTURA_ATTIVAZIONE.equals(that.N_LETTURA_ATTIVAZIONE));
    equal = equal && (this.T_ALIQUOTA_IVA == null ? that.T_ALIQUOTA_IVA == null : this.T_ALIQUOTA_IVA.equals(that.T_ALIQUOTA_IVA));
    equal = equal && (this.T_IMPOSTE == null ? that.T_IMPOSTE == null : this.T_IMPOSTE.equals(that.T_IMPOSTE));
    equal = equal && (this.N_INDIRIZZO_FORNITURA == null ? that.N_INDIRIZZO_FORNITURA == null : this.N_INDIRIZZO_FORNITURA.equals(that.N_INDIRIZZO_FORNITURA));
    equal = equal && (this.N_INDIRIZZO_RECAP == null ? that.N_INDIRIZZO_RECAP == null : this.N_INDIRIZZO_RECAP.equals(that.N_INDIRIZZO_RECAP));
    equal = equal && (this.T_BONUS_GAS == null ? that.T_BONUS_GAS == null : this.T_BONUS_GAS.equals(that.T_BONUS_GAS));
    equal = equal && (this.D_DATA_INIZIO_BONUS == null ? that.D_DATA_INIZIO_BONUS == null : this.D_DATA_INIZIO_BONUS.equals(that.D_DATA_INIZIO_BONUS));
    equal = equal && (this.D_DATA_FINE_BONUS == null ? that.D_DATA_FINE_BONUS == null : this.D_DATA_FINE_BONUS.equals(that.D_DATA_FINE_BONUS));
    equal = equal && (this.B_PRESTAZIONI_NON_CONCLUSE == null ? that.B_PRESTAZIONI_NON_CONCLUSE == null : this.B_PRESTAZIONI_NON_CONCLUSE.equals(that.B_PRESTAZIONI_NON_CONCLUSE));
    equal = equal && (this.B_DISALIMENTABILITA == null ? that.B_DISALIMENTABILITA == null : this.B_DISALIMENTABILITA.equals(that.B_DISALIMENTABILITA));
    equal = equal && (this.T_CODICE_CONTRATTO_VENDITA == null ? that.T_CODICE_CONTRATTO_VENDITA == null : this.T_CODICE_CONTRATTO_VENDITA.equals(that.T_CODICE_CONTRATTO_VENDITA));
    equal = equal && (this.T_ID_CONTRATTO_VEND == null ? that.T_ID_CONTRATTO_VEND == null : this.T_ID_CONTRATTO_VEND.equals(that.T_ID_CONTRATTO_VEND));
    equal = equal && (this.D_DATA_STIPULA == null ? that.D_DATA_STIPULA == null : this.D_DATA_STIPULA.equals(that.D_DATA_STIPULA));
    equal = equal && (this.T_NOTE == null ? that.T_NOTE == null : this.T_NOTE.equals(that.T_NOTE));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.TIPO_DATA_INIZIO == null ? that.TIPO_DATA_INIZIO == null : this.TIPO_DATA_INIZIO.equals(that.TIPO_DATA_INIZIO));
    equal = equal && (this.TIPO_DATA_FINE == null ? that.TIPO_DATA_FINE == null : this.TIPO_DATA_FINE.equals(that.TIPO_DATA_FINE));
    equal = equal && (this.D_DATA_RIF == null ? that.D_DATA_RIF == null : this.D_DATA_RIF.equals(that.D_DATA_RIF));
    equal = equal && (this.T_TIPO_FORNITURA == null ? that.T_TIPO_FORNITURA == null : this.T_TIPO_FORNITURA.equals(that.T_TIPO_FORNITURA));
    equal = equal && (this.N_INDIRIZZO_FATT == null ? that.N_INDIRIZZO_FATT == null : this.N_INDIRIZZO_FATT.equals(that.N_INDIRIZZO_FATT));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_FORNITURA = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.D_DATA_INIZIO = JdbcWritableBridge.readString(2, __dbResults);
    this.D_DATA_FINE = JdbcWritableBridge.readString(3, __dbResults);
    this.N_ID_CLIENTE = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.N_ID_PDR = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.N_ID_VEND = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.B_TARIFFA_TM = JdbcWritableBridge.readString(7, __dbResults);
    this.T_CODICE_ATECO = JdbcWritableBridge.readString(8, __dbResults);
    this.N_LETTURA_ATTIVAZIONE = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.T_ALIQUOTA_IVA = JdbcWritableBridge.readString(10, __dbResults);
    this.T_IMPOSTE = JdbcWritableBridge.readString(11, __dbResults);
    this.N_INDIRIZZO_FORNITURA = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.N_INDIRIZZO_RECAP = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.T_BONUS_GAS = JdbcWritableBridge.readString(14, __dbResults);
    this.D_DATA_INIZIO_BONUS = JdbcWritableBridge.readString(15, __dbResults);
    this.D_DATA_FINE_BONUS = JdbcWritableBridge.readString(16, __dbResults);
    this.B_PRESTAZIONI_NON_CONCLUSE = JdbcWritableBridge.readString(17, __dbResults);
    this.B_DISALIMENTABILITA = JdbcWritableBridge.readString(18, __dbResults);
    this.T_CODICE_CONTRATTO_VENDITA = JdbcWritableBridge.readString(19, __dbResults);
    this.T_ID_CONTRATTO_VEND = JdbcWritableBridge.readString(20, __dbResults);
    this.D_DATA_STIPULA = JdbcWritableBridge.readString(21, __dbResults);
    this.T_NOTE = JdbcWritableBridge.readString(22, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(23, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(24, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(25, __dbResults);
    this.TIPO_DATA_INIZIO = JdbcWritableBridge.readString(26, __dbResults);
    this.TIPO_DATA_FINE = JdbcWritableBridge.readString(27, __dbResults);
    this.D_DATA_RIF = JdbcWritableBridge.readString(28, __dbResults);
    this.T_TIPO_FORNITURA = JdbcWritableBridge.readString(29, __dbResults);
    this.N_INDIRIZZO_FATT = JdbcWritableBridge.readBigDecimal(30, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_FORNITURA = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.D_DATA_INIZIO = JdbcWritableBridge.readString(2, __dbResults);
    this.D_DATA_FINE = JdbcWritableBridge.readString(3, __dbResults);
    this.N_ID_CLIENTE = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.N_ID_PDR = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.N_ID_VEND = JdbcWritableBridge.readBigDecimal(6, __dbResults);
    this.B_TARIFFA_TM = JdbcWritableBridge.readString(7, __dbResults);
    this.T_CODICE_ATECO = JdbcWritableBridge.readString(8, __dbResults);
    this.N_LETTURA_ATTIVAZIONE = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.T_ALIQUOTA_IVA = JdbcWritableBridge.readString(10, __dbResults);
    this.T_IMPOSTE = JdbcWritableBridge.readString(11, __dbResults);
    this.N_INDIRIZZO_FORNITURA = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.N_INDIRIZZO_RECAP = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.T_BONUS_GAS = JdbcWritableBridge.readString(14, __dbResults);
    this.D_DATA_INIZIO_BONUS = JdbcWritableBridge.readString(15, __dbResults);
    this.D_DATA_FINE_BONUS = JdbcWritableBridge.readString(16, __dbResults);
    this.B_PRESTAZIONI_NON_CONCLUSE = JdbcWritableBridge.readString(17, __dbResults);
    this.B_DISALIMENTABILITA = JdbcWritableBridge.readString(18, __dbResults);
    this.T_CODICE_CONTRATTO_VENDITA = JdbcWritableBridge.readString(19, __dbResults);
    this.T_ID_CONTRATTO_VEND = JdbcWritableBridge.readString(20, __dbResults);
    this.D_DATA_STIPULA = JdbcWritableBridge.readString(21, __dbResults);
    this.T_NOTE = JdbcWritableBridge.readString(22, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(23, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(24, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(25, __dbResults);
    this.TIPO_DATA_INIZIO = JdbcWritableBridge.readString(26, __dbResults);
    this.TIPO_DATA_FINE = JdbcWritableBridge.readString(27, __dbResults);
    this.D_DATA_RIF = JdbcWritableBridge.readString(28, __dbResults);
    this.T_TIPO_FORNITURA = JdbcWritableBridge.readString(29, __dbResults);
    this.N_INDIRIZZO_FATT = JdbcWritableBridge.readBigDecimal(30, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_FORNITURA, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INIZIO, 2 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_FINE, 3 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CLIENTE, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PDR, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_VEND, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_TARIFFA_TM, 7 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_ATECO, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_LETTURA_ATTIVAZIONE, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_ALIQUOTA_IVA, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_IMPOSTE, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_INDIRIZZO_FORNITURA, 12 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_INDIRIZZO_RECAP, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_BONUS_GAS, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INIZIO_BONUS, 15 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_FINE_BONUS, 16 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(B_PRESTAZIONI_NON_CONCLUSE, 17 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_DISALIMENTABILITA, 18 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_CONTRATTO_VENDITA, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ID_CONTRATTO_VEND, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_STIPULA, 21 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTE, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 23 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 24 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 25 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(TIPO_DATA_INIZIO, 26 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TIPO_DATA_FINE, 27 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF, 28 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_FORNITURA, 29 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_INDIRIZZO_FATT, 30 + __off, 2, __dbStmt);
    return 30;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_FORNITURA, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INIZIO, 2 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_FINE, 3 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CLIENTE, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_PDR, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_VEND, 6 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_TARIFFA_TM, 7 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_ATECO, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_LETTURA_ATTIVAZIONE, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_ALIQUOTA_IVA, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_IMPOSTE, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_INDIRIZZO_FORNITURA, 12 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_INDIRIZZO_RECAP, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_BONUS_GAS, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_INIZIO_BONUS, 15 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_FINE_BONUS, 16 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(B_PRESTAZIONI_NON_CONCLUSE, 17 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(B_DISALIMENTABILITA, 18 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_CONTRATTO_VENDITA, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ID_CONTRATTO_VEND, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_STIPULA, 21 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTE, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 23 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 24 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 25 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(TIPO_DATA_INIZIO, 26 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TIPO_DATA_FINE, 27 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(D_DATA_RIF, 28 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_FORNITURA, 29 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_INDIRIZZO_FATT, 30 + __off, 2, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_FORNITURA = null;
    } else {
    this.N_ID_FORNITURA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_INIZIO = null;
    } else {
    this.D_DATA_INIZIO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_FINE = null;
    } else {
    this.D_DATA_FINE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_CLIENTE = null;
    } else {
    this.N_ID_CLIENTE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_PDR = null;
    } else {
    this.N_ID_PDR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_VEND = null;
    } else {
    this.N_ID_VEND = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_TARIFFA_TM = null;
    } else {
    this.B_TARIFFA_TM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_ATECO = null;
    } else {
    this.T_CODICE_ATECO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_LETTURA_ATTIVAZIONE = null;
    } else {
    this.N_LETTURA_ATTIVAZIONE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ALIQUOTA_IVA = null;
    } else {
    this.T_ALIQUOTA_IVA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_IMPOSTE = null;
    } else {
    this.T_IMPOSTE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_INDIRIZZO_FORNITURA = null;
    } else {
    this.N_INDIRIZZO_FORNITURA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_INDIRIZZO_RECAP = null;
    } else {
    this.N_INDIRIZZO_RECAP = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_BONUS_GAS = null;
    } else {
    this.T_BONUS_GAS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_INIZIO_BONUS = null;
    } else {
    this.D_DATA_INIZIO_BONUS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_FINE_BONUS = null;
    } else {
    this.D_DATA_FINE_BONUS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_PRESTAZIONI_NON_CONCLUSE = null;
    } else {
    this.B_PRESTAZIONI_NON_CONCLUSE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_DISALIMENTABILITA = null;
    } else {
    this.B_DISALIMENTABILITA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_CONTRATTO_VENDITA = null;
    } else {
    this.T_CODICE_CONTRATTO_VENDITA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ID_CONTRATTO_VEND = null;
    } else {
    this.T_ID_CONTRATTO_VEND = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_STIPULA = null;
    } else {
    this.D_DATA_STIPULA = Text.readString(__dataIn);
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
        this.TIPO_DATA_INIZIO = null;
    } else {
    this.TIPO_DATA_INIZIO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TIPO_DATA_FINE = null;
    } else {
    this.TIPO_DATA_FINE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_DATA_RIF = null;
    } else {
    this.D_DATA_RIF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO_FORNITURA = null;
    } else {
    this.T_TIPO_FORNITURA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_INDIRIZZO_FATT = null;
    } else {
    this.N_INDIRIZZO_FATT = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FORNITURA, __dataOut);
    }
    if (null == this.D_DATA_INIZIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INIZIO);
    }
    if (null == this.D_DATA_FINE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_FINE);
    }
    if (null == this.N_ID_CLIENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CLIENTE, __dataOut);
    }
    if (null == this.N_ID_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PDR, __dataOut);
    }
    if (null == this.N_ID_VEND) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_VEND, __dataOut);
    }
    if (null == this.B_TARIFFA_TM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_TARIFFA_TM);
    }
    if (null == this.T_CODICE_ATECO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_ATECO);
    }
    if (null == this.N_LETTURA_ATTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_LETTURA_ATTIVAZIONE, __dataOut);
    }
    if (null == this.T_ALIQUOTA_IVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ALIQUOTA_IVA);
    }
    if (null == this.T_IMPOSTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_IMPOSTE);
    }
    if (null == this.N_INDIRIZZO_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_INDIRIZZO_FORNITURA, __dataOut);
    }
    if (null == this.N_INDIRIZZO_RECAP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_INDIRIZZO_RECAP, __dataOut);
    }
    if (null == this.T_BONUS_GAS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_BONUS_GAS);
    }
    if (null == this.D_DATA_INIZIO_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INIZIO_BONUS);
    }
    if (null == this.D_DATA_FINE_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_FINE_BONUS);
    }
    if (null == this.B_PRESTAZIONI_NON_CONCLUSE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_PRESTAZIONI_NON_CONCLUSE);
    }
    if (null == this.B_DISALIMENTABILITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_DISALIMENTABILITA);
    }
    if (null == this.T_CODICE_CONTRATTO_VENDITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_CONTRATTO_VENDITA);
    }
    if (null == this.T_ID_CONTRATTO_VEND) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ID_CONTRATTO_VEND);
    }
    if (null == this.D_DATA_STIPULA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_STIPULA);
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
    if (null == this.TIPO_DATA_INIZIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIPO_DATA_INIZIO);
    }
    if (null == this.TIPO_DATA_FINE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIPO_DATA_FINE);
    }
    if (null == this.D_DATA_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF);
    }
    if (null == this.T_TIPO_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_FORNITURA);
    }
    if (null == this.N_INDIRIZZO_FATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_INDIRIZZO_FATT, __dataOut);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FORNITURA, __dataOut);
    }
    if (null == this.D_DATA_INIZIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INIZIO);
    }
    if (null == this.D_DATA_FINE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_FINE);
    }
    if (null == this.N_ID_CLIENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CLIENTE, __dataOut);
    }
    if (null == this.N_ID_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PDR, __dataOut);
    }
    if (null == this.N_ID_VEND) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_VEND, __dataOut);
    }
    if (null == this.B_TARIFFA_TM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_TARIFFA_TM);
    }
    if (null == this.T_CODICE_ATECO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_ATECO);
    }
    if (null == this.N_LETTURA_ATTIVAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_LETTURA_ATTIVAZIONE, __dataOut);
    }
    if (null == this.T_ALIQUOTA_IVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ALIQUOTA_IVA);
    }
    if (null == this.T_IMPOSTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_IMPOSTE);
    }
    if (null == this.N_INDIRIZZO_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_INDIRIZZO_FORNITURA, __dataOut);
    }
    if (null == this.N_INDIRIZZO_RECAP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_INDIRIZZO_RECAP, __dataOut);
    }
    if (null == this.T_BONUS_GAS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_BONUS_GAS);
    }
    if (null == this.D_DATA_INIZIO_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_INIZIO_BONUS);
    }
    if (null == this.D_DATA_FINE_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_FINE_BONUS);
    }
    if (null == this.B_PRESTAZIONI_NON_CONCLUSE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_PRESTAZIONI_NON_CONCLUSE);
    }
    if (null == this.B_DISALIMENTABILITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_DISALIMENTABILITA);
    }
    if (null == this.T_CODICE_CONTRATTO_VENDITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_CONTRATTO_VENDITA);
    }
    if (null == this.T_ID_CONTRATTO_VEND) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ID_CONTRATTO_VEND);
    }
    if (null == this.D_DATA_STIPULA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_STIPULA);
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
    if (null == this.TIPO_DATA_INIZIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIPO_DATA_INIZIO);
    }
    if (null == this.TIPO_DATA_FINE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIPO_DATA_FINE);
    }
    if (null == this.D_DATA_RIF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_DATA_RIF);
    }
    if (null == this.T_TIPO_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_FORNITURA);
    }
    if (null == this.N_INDIRIZZO_FATT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_INDIRIZZO_FATT, __dataOut);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_FORNITURA==null?"":N_ID_FORNITURA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INIZIO==null?"":D_DATA_INIZIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_FINE==null?"":D_DATA_FINE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CLIENTE==null?"":N_ID_CLIENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PDR==null?"":N_ID_PDR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_VEND==null?"":N_ID_VEND.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_TARIFFA_TM==null?"":B_TARIFFA_TM, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_ATECO==null?"":T_CODICE_ATECO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_LETTURA_ATTIVAZIONE==null?"":N_LETTURA_ATTIVAZIONE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ALIQUOTA_IVA==null?"":T_ALIQUOTA_IVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_IMPOSTE==null?"":T_IMPOSTE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_INDIRIZZO_FORNITURA==null?"":N_INDIRIZZO_FORNITURA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_INDIRIZZO_RECAP==null?"":N_INDIRIZZO_RECAP.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_BONUS_GAS==null?"":T_BONUS_GAS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INIZIO_BONUS==null?"":D_DATA_INIZIO_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_FINE_BONUS==null?"":D_DATA_FINE_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_PRESTAZIONI_NON_CONCLUSE==null?"":B_PRESTAZIONI_NON_CONCLUSE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_DISALIMENTABILITA==null?"":B_DISALIMENTABILITA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_CONTRATTO_VENDITA==null?"":T_CODICE_CONTRATTO_VENDITA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ID_CONTRATTO_VEND==null?"":T_ID_CONTRATTO_VEND, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_STIPULA==null?"":D_DATA_STIPULA, " ", delimiters));
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TIPO_DATA_INIZIO==null?"":TIPO_DATA_INIZIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TIPO_DATA_FINE==null?"":TIPO_DATA_FINE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF==null?"":D_DATA_RIF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_FORNITURA==null?"":T_TIPO_FORNITURA, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_INDIRIZZO_FATT==null?"":N_INDIRIZZO_FATT.toPlainString(), delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_FORNITURA==null?"":N_ID_FORNITURA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INIZIO==null?"":D_DATA_INIZIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_FINE==null?"":D_DATA_FINE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CLIENTE==null?"":N_ID_CLIENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PDR==null?"":N_ID_PDR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_VEND==null?"":N_ID_VEND.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_TARIFFA_TM==null?"":B_TARIFFA_TM, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_ATECO==null?"":T_CODICE_ATECO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_LETTURA_ATTIVAZIONE==null?"":N_LETTURA_ATTIVAZIONE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ALIQUOTA_IVA==null?"":T_ALIQUOTA_IVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_IMPOSTE==null?"":T_IMPOSTE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_INDIRIZZO_FORNITURA==null?"":N_INDIRIZZO_FORNITURA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_INDIRIZZO_RECAP==null?"":N_INDIRIZZO_RECAP.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_BONUS_GAS==null?"":T_BONUS_GAS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_INIZIO_BONUS==null?"":D_DATA_INIZIO_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_FINE_BONUS==null?"":D_DATA_FINE_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_PRESTAZIONI_NON_CONCLUSE==null?"":B_PRESTAZIONI_NON_CONCLUSE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_DISALIMENTABILITA==null?"":B_DISALIMENTABILITA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_CONTRATTO_VENDITA==null?"":T_CODICE_CONTRATTO_VENDITA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ID_CONTRATTO_VEND==null?"":T_ID_CONTRATTO_VEND, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_STIPULA==null?"":D_DATA_STIPULA, " ", delimiters));
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TIPO_DATA_INIZIO==null?"":TIPO_DATA_INIZIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TIPO_DATA_FINE==null?"":TIPO_DATA_FINE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_DATA_RIF==null?"":D_DATA_RIF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_FORNITURA==null?"":T_TIPO_FORNITURA, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_INDIRIZZO_FATT==null?"":N_INDIRIZZO_FATT.toPlainString(), delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_FORNITURA = null; } else {
      this.N_ID_FORNITURA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INIZIO = null; } else {
      this.D_DATA_INIZIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_FINE = null; } else {
      this.D_DATA_FINE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CLIENTE = null; } else {
      this.N_ID_CLIENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PDR = null; } else {
      this.N_ID_PDR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_VEND = null; } else {
      this.N_ID_VEND = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_TARIFFA_TM = null; } else {
      this.B_TARIFFA_TM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_ATECO = null; } else {
      this.T_CODICE_ATECO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_LETTURA_ATTIVAZIONE = null; } else {
      this.N_LETTURA_ATTIVAZIONE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ALIQUOTA_IVA = null; } else {
      this.T_ALIQUOTA_IVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_IMPOSTE = null; } else {
      this.T_IMPOSTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_INDIRIZZO_FORNITURA = null; } else {
      this.N_INDIRIZZO_FORNITURA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_INDIRIZZO_RECAP = null; } else {
      this.N_INDIRIZZO_RECAP = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_BONUS_GAS = null; } else {
      this.T_BONUS_GAS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INIZIO_BONUS = null; } else {
      this.D_DATA_INIZIO_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_FINE_BONUS = null; } else {
      this.D_DATA_FINE_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_PRESTAZIONI_NON_CONCLUSE = null; } else {
      this.B_PRESTAZIONI_NON_CONCLUSE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_DISALIMENTABILITA = null; } else {
      this.B_DISALIMENTABILITA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_CONTRATTO_VENDITA = null; } else {
      this.T_CODICE_CONTRATTO_VENDITA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ID_CONTRATTO_VEND = null; } else {
      this.T_ID_CONTRATTO_VEND = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_STIPULA = null; } else {
      this.D_DATA_STIPULA = __cur_str;
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
    if (__cur_str.equals("null")) { this.TIPO_DATA_INIZIO = null; } else {
      this.TIPO_DATA_INIZIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIPO_DATA_FINE = null; } else {
      this.TIPO_DATA_FINE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RIF = null; } else {
      this.D_DATA_RIF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_FORNITURA = null; } else {
      this.T_TIPO_FORNITURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_INDIRIZZO_FATT = null; } else {
      this.N_INDIRIZZO_FATT = new java.math.BigDecimal(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_FORNITURA = null; } else {
      this.N_ID_FORNITURA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INIZIO = null; } else {
      this.D_DATA_INIZIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_FINE = null; } else {
      this.D_DATA_FINE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CLIENTE = null; } else {
      this.N_ID_CLIENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PDR = null; } else {
      this.N_ID_PDR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_VEND = null; } else {
      this.N_ID_VEND = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_TARIFFA_TM = null; } else {
      this.B_TARIFFA_TM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_ATECO = null; } else {
      this.T_CODICE_ATECO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_LETTURA_ATTIVAZIONE = null; } else {
      this.N_LETTURA_ATTIVAZIONE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ALIQUOTA_IVA = null; } else {
      this.T_ALIQUOTA_IVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_IMPOSTE = null; } else {
      this.T_IMPOSTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_INDIRIZZO_FORNITURA = null; } else {
      this.N_INDIRIZZO_FORNITURA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_INDIRIZZO_RECAP = null; } else {
      this.N_INDIRIZZO_RECAP = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_BONUS_GAS = null; } else {
      this.T_BONUS_GAS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_INIZIO_BONUS = null; } else {
      this.D_DATA_INIZIO_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_FINE_BONUS = null; } else {
      this.D_DATA_FINE_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_PRESTAZIONI_NON_CONCLUSE = null; } else {
      this.B_PRESTAZIONI_NON_CONCLUSE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_DISALIMENTABILITA = null; } else {
      this.B_DISALIMENTABILITA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_CONTRATTO_VENDITA = null; } else {
      this.T_CODICE_CONTRATTO_VENDITA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ID_CONTRATTO_VEND = null; } else {
      this.T_ID_CONTRATTO_VEND = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_STIPULA = null; } else {
      this.D_DATA_STIPULA = __cur_str;
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
    if (__cur_str.equals("null")) { this.TIPO_DATA_INIZIO = null; } else {
      this.TIPO_DATA_INIZIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIPO_DATA_FINE = null; } else {
      this.TIPO_DATA_FINE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_DATA_RIF = null; } else {
      this.D_DATA_RIF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_FORNITURA = null; } else {
      this.T_TIPO_FORNITURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_INDIRIZZO_FATT = null; } else {
      this.N_INDIRIZZO_FATT = new java.math.BigDecimal(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    rcugas_rcugas_fornitura o = (rcugas_rcugas_fornitura) super.clone();
    return o;
  }

  public void clone0(rcugas_rcugas_fornitura o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_FORNITURA", this.N_ID_FORNITURA);
    __sqoop$field_map.put("D_DATA_INIZIO", this.D_DATA_INIZIO);
    __sqoop$field_map.put("D_DATA_FINE", this.D_DATA_FINE);
    __sqoop$field_map.put("N_ID_CLIENTE", this.N_ID_CLIENTE);
    __sqoop$field_map.put("N_ID_PDR", this.N_ID_PDR);
    __sqoop$field_map.put("N_ID_VEND", this.N_ID_VEND);
    __sqoop$field_map.put("B_TARIFFA_TM", this.B_TARIFFA_TM);
    __sqoop$field_map.put("T_CODICE_ATECO", this.T_CODICE_ATECO);
    __sqoop$field_map.put("N_LETTURA_ATTIVAZIONE", this.N_LETTURA_ATTIVAZIONE);
    __sqoop$field_map.put("T_ALIQUOTA_IVA", this.T_ALIQUOTA_IVA);
    __sqoop$field_map.put("T_IMPOSTE", this.T_IMPOSTE);
    __sqoop$field_map.put("N_INDIRIZZO_FORNITURA", this.N_INDIRIZZO_FORNITURA);
    __sqoop$field_map.put("N_INDIRIZZO_RECAP", this.N_INDIRIZZO_RECAP);
    __sqoop$field_map.put("T_BONUS_GAS", this.T_BONUS_GAS);
    __sqoop$field_map.put("D_DATA_INIZIO_BONUS", this.D_DATA_INIZIO_BONUS);
    __sqoop$field_map.put("D_DATA_FINE_BONUS", this.D_DATA_FINE_BONUS);
    __sqoop$field_map.put("B_PRESTAZIONI_NON_CONCLUSE", this.B_PRESTAZIONI_NON_CONCLUSE);
    __sqoop$field_map.put("B_DISALIMENTABILITA", this.B_DISALIMENTABILITA);
    __sqoop$field_map.put("T_CODICE_CONTRATTO_VENDITA", this.T_CODICE_CONTRATTO_VENDITA);
    __sqoop$field_map.put("T_ID_CONTRATTO_VEND", this.T_ID_CONTRATTO_VEND);
    __sqoop$field_map.put("D_DATA_STIPULA", this.D_DATA_STIPULA);
    __sqoop$field_map.put("T_NOTE", this.T_NOTE);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("TIPO_DATA_INIZIO", this.TIPO_DATA_INIZIO);
    __sqoop$field_map.put("TIPO_DATA_FINE", this.TIPO_DATA_FINE);
    __sqoop$field_map.put("D_DATA_RIF", this.D_DATA_RIF);
    __sqoop$field_map.put("T_TIPO_FORNITURA", this.T_TIPO_FORNITURA);
    __sqoop$field_map.put("N_INDIRIZZO_FATT", this.N_INDIRIZZO_FATT);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_FORNITURA", this.N_ID_FORNITURA);
    __sqoop$field_map.put("D_DATA_INIZIO", this.D_DATA_INIZIO);
    __sqoop$field_map.put("D_DATA_FINE", this.D_DATA_FINE);
    __sqoop$field_map.put("N_ID_CLIENTE", this.N_ID_CLIENTE);
    __sqoop$field_map.put("N_ID_PDR", this.N_ID_PDR);
    __sqoop$field_map.put("N_ID_VEND", this.N_ID_VEND);
    __sqoop$field_map.put("B_TARIFFA_TM", this.B_TARIFFA_TM);
    __sqoop$field_map.put("T_CODICE_ATECO", this.T_CODICE_ATECO);
    __sqoop$field_map.put("N_LETTURA_ATTIVAZIONE", this.N_LETTURA_ATTIVAZIONE);
    __sqoop$field_map.put("T_ALIQUOTA_IVA", this.T_ALIQUOTA_IVA);
    __sqoop$field_map.put("T_IMPOSTE", this.T_IMPOSTE);
    __sqoop$field_map.put("N_INDIRIZZO_FORNITURA", this.N_INDIRIZZO_FORNITURA);
    __sqoop$field_map.put("N_INDIRIZZO_RECAP", this.N_INDIRIZZO_RECAP);
    __sqoop$field_map.put("T_BONUS_GAS", this.T_BONUS_GAS);
    __sqoop$field_map.put("D_DATA_INIZIO_BONUS", this.D_DATA_INIZIO_BONUS);
    __sqoop$field_map.put("D_DATA_FINE_BONUS", this.D_DATA_FINE_BONUS);
    __sqoop$field_map.put("B_PRESTAZIONI_NON_CONCLUSE", this.B_PRESTAZIONI_NON_CONCLUSE);
    __sqoop$field_map.put("B_DISALIMENTABILITA", this.B_DISALIMENTABILITA);
    __sqoop$field_map.put("T_CODICE_CONTRATTO_VENDITA", this.T_CODICE_CONTRATTO_VENDITA);
    __sqoop$field_map.put("T_ID_CONTRATTO_VEND", this.T_ID_CONTRATTO_VEND);
    __sqoop$field_map.put("D_DATA_STIPULA", this.D_DATA_STIPULA);
    __sqoop$field_map.put("T_NOTE", this.T_NOTE);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("TIPO_DATA_INIZIO", this.TIPO_DATA_INIZIO);
    __sqoop$field_map.put("TIPO_DATA_FINE", this.TIPO_DATA_FINE);
    __sqoop$field_map.put("D_DATA_RIF", this.D_DATA_RIF);
    __sqoop$field_map.put("T_TIPO_FORNITURA", this.T_TIPO_FORNITURA);
    __sqoop$field_map.put("N_INDIRIZZO_FATT", this.N_INDIRIZZO_FATT);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
