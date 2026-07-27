// ORM class for table 'rcu.rcu_fornitura'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 09:48:34 CEST 2019
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

public class rcu_rcu_fornitura extends SqoopRecord  implements DBWritable, Writable {
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
    setters.put("N_ID_POD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_POD = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_CLIENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_CLIENTE = (java.math.BigDecimal)value;
      }
    });
    setters.put("D_INIZIO_TITOLARITA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_INIZIO_TITOLARITA = (String)value;
      }
    });
    setters.put("D_FINE_TITOLARITA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_FINE_TITOLARITA = (String)value;
      }
    });
    setters.put("T_COD_CONTRATTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COD_CONTRATTO = (String)value;
      }
    });
    setters.put("D_STIPULA_CONTRATTO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_STIPULA_CONTRATTO = (String)value;
      }
    });
    setters.put("D_MESE_RINNOVO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_MESE_RINNOVO = (String)value;
      }
    });
    setters.put("N_IVA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_IVA = (java.math.BigDecimal)value;
      }
    });
    setters.put("B_DISALIMENTABILITA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_DISALIMENTABILITA = (String)value;
      }
    });
    setters.put("T_TARIFFA_DISTR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TARIFFA_DISTR = (String)value;
      }
    });
    setters.put("T_CODICE_ATECO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_ATECO = (String)value;
      }
    });
    setters.put("N_ID_FORNITORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_FORNITORE = (java.math.BigDecimal)value;
      }
    });
    setters.put("T_RUOLO_FORNITORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_RUOLO_FORNITORE = (String)value;
      }
    });
    setters.put("T_TIPO_MERCATO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TIPO_MERCATO = (String)value;
      }
    });
    setters.put("B_SALVAGUARDIA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        B_SALVAGUARDIA = (String)value;
      }
    });
    setters.put("T_BONUS_SOCIALE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_BONUS_SOCIALE = (String)value;
      }
    });
    setters.put("D_INIZIO_BONUS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_INIZIO_BONUS = (String)value;
      }
    });
    setters.put("D_FINE_BONUS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        D_FINE_BONUS = (String)value;
      }
    });
    setters.put("T_COMUNIC_BONUS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_COMUNIC_BONUS = (String)value;
      }
    });
    setters.put("N_IMPOSTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_IMPOSTE = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_INDIR_ESAZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_INDIR_ESAZIONE = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_INDIR_COMUNIC", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_INDIR_COMUNIC = (java.math.BigDecimal)value;
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
    setters.put("T_DIRITTO_TUTELA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_DIRITTO_TUTELA = (String)value;
      }
    });
    setters.put("T_CODICE_UFFICIO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_CODICE_UFFICIO = (String)value;
      }
    });
    setters.put("T_PAGAMENTO_IVA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_PAGAMENTO_IVA = (String)value;
      }
    });
    setters.put("T_ADDIZ_PROVINCIALE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ADDIZ_PROVINCIALE = (String)value;
      }
    });
    setters.put("T_ADDIZ_COMUNALE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_ADDIZ_COMUNALE = (String)value;
      }
    });
    setters.put("T_TELEFONO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_TELEFONO = (String)value;
      }
    });
    setters.put("T_IVA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_IVA = (String)value;
      }
    });
    setters.put("T_IMPOSTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        T_IMPOSTE = (String)value;
      }
    });
  }
  public rcu_rcu_fornitura() {
    init0();
  }
  private java.math.BigDecimal N_ID_FORNITURA;
  public java.math.BigDecimal get_N_ID_FORNITURA() {
    return N_ID_FORNITURA;
  }
  public void set_N_ID_FORNITURA(java.math.BigDecimal N_ID_FORNITURA) {
    this.N_ID_FORNITURA = N_ID_FORNITURA;
  }
  public rcu_rcu_fornitura with_N_ID_FORNITURA(java.math.BigDecimal N_ID_FORNITURA) {
    this.N_ID_FORNITURA = N_ID_FORNITURA;
    return this;
  }
  private java.math.BigDecimal N_ID_POD;
  public java.math.BigDecimal get_N_ID_POD() {
    return N_ID_POD;
  }
  public void set_N_ID_POD(java.math.BigDecimal N_ID_POD) {
    this.N_ID_POD = N_ID_POD;
  }
  public rcu_rcu_fornitura with_N_ID_POD(java.math.BigDecimal N_ID_POD) {
    this.N_ID_POD = N_ID_POD;
    return this;
  }
  private java.math.BigDecimal N_ID_CLIENTE;
  public java.math.BigDecimal get_N_ID_CLIENTE() {
    return N_ID_CLIENTE;
  }
  public void set_N_ID_CLIENTE(java.math.BigDecimal N_ID_CLIENTE) {
    this.N_ID_CLIENTE = N_ID_CLIENTE;
  }
  public rcu_rcu_fornitura with_N_ID_CLIENTE(java.math.BigDecimal N_ID_CLIENTE) {
    this.N_ID_CLIENTE = N_ID_CLIENTE;
    return this;
  }
  private String D_INIZIO_TITOLARITA;
  public String get_D_INIZIO_TITOLARITA() {
    return D_INIZIO_TITOLARITA;
  }
  public void set_D_INIZIO_TITOLARITA(String D_INIZIO_TITOLARITA) {
    this.D_INIZIO_TITOLARITA = D_INIZIO_TITOLARITA;
  }
  public rcu_rcu_fornitura with_D_INIZIO_TITOLARITA(String D_INIZIO_TITOLARITA) {
    this.D_INIZIO_TITOLARITA = D_INIZIO_TITOLARITA;
    return this;
  }
  private String D_FINE_TITOLARITA;
  public String get_D_FINE_TITOLARITA() {
    return D_FINE_TITOLARITA;
  }
  public void set_D_FINE_TITOLARITA(String D_FINE_TITOLARITA) {
    this.D_FINE_TITOLARITA = D_FINE_TITOLARITA;
  }
  public rcu_rcu_fornitura with_D_FINE_TITOLARITA(String D_FINE_TITOLARITA) {
    this.D_FINE_TITOLARITA = D_FINE_TITOLARITA;
    return this;
  }
  private String T_COD_CONTRATTO;
  public String get_T_COD_CONTRATTO() {
    return T_COD_CONTRATTO;
  }
  public void set_T_COD_CONTRATTO(String T_COD_CONTRATTO) {
    this.T_COD_CONTRATTO = T_COD_CONTRATTO;
  }
  public rcu_rcu_fornitura with_T_COD_CONTRATTO(String T_COD_CONTRATTO) {
    this.T_COD_CONTRATTO = T_COD_CONTRATTO;
    return this;
  }
  private String D_STIPULA_CONTRATTO;
  public String get_D_STIPULA_CONTRATTO() {
    return D_STIPULA_CONTRATTO;
  }
  public void set_D_STIPULA_CONTRATTO(String D_STIPULA_CONTRATTO) {
    this.D_STIPULA_CONTRATTO = D_STIPULA_CONTRATTO;
  }
  public rcu_rcu_fornitura with_D_STIPULA_CONTRATTO(String D_STIPULA_CONTRATTO) {
    this.D_STIPULA_CONTRATTO = D_STIPULA_CONTRATTO;
    return this;
  }
  private String D_MESE_RINNOVO;
  public String get_D_MESE_RINNOVO() {
    return D_MESE_RINNOVO;
  }
  public void set_D_MESE_RINNOVO(String D_MESE_RINNOVO) {
    this.D_MESE_RINNOVO = D_MESE_RINNOVO;
  }
  public rcu_rcu_fornitura with_D_MESE_RINNOVO(String D_MESE_RINNOVO) {
    this.D_MESE_RINNOVO = D_MESE_RINNOVO;
    return this;
  }
  private java.math.BigDecimal N_IVA;
  public java.math.BigDecimal get_N_IVA() {
    return N_IVA;
  }
  public void set_N_IVA(java.math.BigDecimal N_IVA) {
    this.N_IVA = N_IVA;
  }
  public rcu_rcu_fornitura with_N_IVA(java.math.BigDecimal N_IVA) {
    this.N_IVA = N_IVA;
    return this;
  }
  private String B_DISALIMENTABILITA;
  public String get_B_DISALIMENTABILITA() {
    return B_DISALIMENTABILITA;
  }
  public void set_B_DISALIMENTABILITA(String B_DISALIMENTABILITA) {
    this.B_DISALIMENTABILITA = B_DISALIMENTABILITA;
  }
  public rcu_rcu_fornitura with_B_DISALIMENTABILITA(String B_DISALIMENTABILITA) {
    this.B_DISALIMENTABILITA = B_DISALIMENTABILITA;
    return this;
  }
  private String T_TARIFFA_DISTR;
  public String get_T_TARIFFA_DISTR() {
    return T_TARIFFA_DISTR;
  }
  public void set_T_TARIFFA_DISTR(String T_TARIFFA_DISTR) {
    this.T_TARIFFA_DISTR = T_TARIFFA_DISTR;
  }
  public rcu_rcu_fornitura with_T_TARIFFA_DISTR(String T_TARIFFA_DISTR) {
    this.T_TARIFFA_DISTR = T_TARIFFA_DISTR;
    return this;
  }
  private String T_CODICE_ATECO;
  public String get_T_CODICE_ATECO() {
    return T_CODICE_ATECO;
  }
  public void set_T_CODICE_ATECO(String T_CODICE_ATECO) {
    this.T_CODICE_ATECO = T_CODICE_ATECO;
  }
  public rcu_rcu_fornitura with_T_CODICE_ATECO(String T_CODICE_ATECO) {
    this.T_CODICE_ATECO = T_CODICE_ATECO;
    return this;
  }
  private java.math.BigDecimal N_ID_FORNITORE;
  public java.math.BigDecimal get_N_ID_FORNITORE() {
    return N_ID_FORNITORE;
  }
  public void set_N_ID_FORNITORE(java.math.BigDecimal N_ID_FORNITORE) {
    this.N_ID_FORNITORE = N_ID_FORNITORE;
  }
  public rcu_rcu_fornitura with_N_ID_FORNITORE(java.math.BigDecimal N_ID_FORNITORE) {
    this.N_ID_FORNITORE = N_ID_FORNITORE;
    return this;
  }
  private String T_RUOLO_FORNITORE;
  public String get_T_RUOLO_FORNITORE() {
    return T_RUOLO_FORNITORE;
  }
  public void set_T_RUOLO_FORNITORE(String T_RUOLO_FORNITORE) {
    this.T_RUOLO_FORNITORE = T_RUOLO_FORNITORE;
  }
  public rcu_rcu_fornitura with_T_RUOLO_FORNITORE(String T_RUOLO_FORNITORE) {
    this.T_RUOLO_FORNITORE = T_RUOLO_FORNITORE;
    return this;
  }
  private String T_TIPO_MERCATO;
  public String get_T_TIPO_MERCATO() {
    return T_TIPO_MERCATO;
  }
  public void set_T_TIPO_MERCATO(String T_TIPO_MERCATO) {
    this.T_TIPO_MERCATO = T_TIPO_MERCATO;
  }
  public rcu_rcu_fornitura with_T_TIPO_MERCATO(String T_TIPO_MERCATO) {
    this.T_TIPO_MERCATO = T_TIPO_MERCATO;
    return this;
  }
  private String B_SALVAGUARDIA;
  public String get_B_SALVAGUARDIA() {
    return B_SALVAGUARDIA;
  }
  public void set_B_SALVAGUARDIA(String B_SALVAGUARDIA) {
    this.B_SALVAGUARDIA = B_SALVAGUARDIA;
  }
  public rcu_rcu_fornitura with_B_SALVAGUARDIA(String B_SALVAGUARDIA) {
    this.B_SALVAGUARDIA = B_SALVAGUARDIA;
    return this;
  }
  private String T_BONUS_SOCIALE;
  public String get_T_BONUS_SOCIALE() {
    return T_BONUS_SOCIALE;
  }
  public void set_T_BONUS_SOCIALE(String T_BONUS_SOCIALE) {
    this.T_BONUS_SOCIALE = T_BONUS_SOCIALE;
  }
  public rcu_rcu_fornitura with_T_BONUS_SOCIALE(String T_BONUS_SOCIALE) {
    this.T_BONUS_SOCIALE = T_BONUS_SOCIALE;
    return this;
  }
  private String D_INIZIO_BONUS;
  public String get_D_INIZIO_BONUS() {
    return D_INIZIO_BONUS;
  }
  public void set_D_INIZIO_BONUS(String D_INIZIO_BONUS) {
    this.D_INIZIO_BONUS = D_INIZIO_BONUS;
  }
  public rcu_rcu_fornitura with_D_INIZIO_BONUS(String D_INIZIO_BONUS) {
    this.D_INIZIO_BONUS = D_INIZIO_BONUS;
    return this;
  }
  private String D_FINE_BONUS;
  public String get_D_FINE_BONUS() {
    return D_FINE_BONUS;
  }
  public void set_D_FINE_BONUS(String D_FINE_BONUS) {
    this.D_FINE_BONUS = D_FINE_BONUS;
  }
  public rcu_rcu_fornitura with_D_FINE_BONUS(String D_FINE_BONUS) {
    this.D_FINE_BONUS = D_FINE_BONUS;
    return this;
  }
  private String T_COMUNIC_BONUS;
  public String get_T_COMUNIC_BONUS() {
    return T_COMUNIC_BONUS;
  }
  public void set_T_COMUNIC_BONUS(String T_COMUNIC_BONUS) {
    this.T_COMUNIC_BONUS = T_COMUNIC_BONUS;
  }
  public rcu_rcu_fornitura with_T_COMUNIC_BONUS(String T_COMUNIC_BONUS) {
    this.T_COMUNIC_BONUS = T_COMUNIC_BONUS;
    return this;
  }
  private java.math.BigDecimal N_IMPOSTE;
  public java.math.BigDecimal get_N_IMPOSTE() {
    return N_IMPOSTE;
  }
  public void set_N_IMPOSTE(java.math.BigDecimal N_IMPOSTE) {
    this.N_IMPOSTE = N_IMPOSTE;
  }
  public rcu_rcu_fornitura with_N_IMPOSTE(java.math.BigDecimal N_IMPOSTE) {
    this.N_IMPOSTE = N_IMPOSTE;
    return this;
  }
  private java.math.BigDecimal N_ID_INDIR_ESAZIONE;
  public java.math.BigDecimal get_N_ID_INDIR_ESAZIONE() {
    return N_ID_INDIR_ESAZIONE;
  }
  public void set_N_ID_INDIR_ESAZIONE(java.math.BigDecimal N_ID_INDIR_ESAZIONE) {
    this.N_ID_INDIR_ESAZIONE = N_ID_INDIR_ESAZIONE;
  }
  public rcu_rcu_fornitura with_N_ID_INDIR_ESAZIONE(java.math.BigDecimal N_ID_INDIR_ESAZIONE) {
    this.N_ID_INDIR_ESAZIONE = N_ID_INDIR_ESAZIONE;
    return this;
  }
  private java.math.BigDecimal N_ID_INDIR_COMUNIC;
  public java.math.BigDecimal get_N_ID_INDIR_COMUNIC() {
    return N_ID_INDIR_COMUNIC;
  }
  public void set_N_ID_INDIR_COMUNIC(java.math.BigDecimal N_ID_INDIR_COMUNIC) {
    this.N_ID_INDIR_COMUNIC = N_ID_INDIR_COMUNIC;
  }
  public rcu_rcu_fornitura with_N_ID_INDIR_COMUNIC(java.math.BigDecimal N_ID_INDIR_COMUNIC) {
    this.N_ID_INDIR_COMUNIC = N_ID_INDIR_COMUNIC;
    return this;
  }
  private String T_NOTA;
  public String get_T_NOTA() {
    return T_NOTA;
  }
  public void set_T_NOTA(String T_NOTA) {
    this.T_NOTA = T_NOTA;
  }
  public rcu_rcu_fornitura with_T_NOTA(String T_NOTA) {
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
  public rcu_rcu_fornitura with_D_AGGIORNAMENTO(String D_AGGIORNAMENTO) {
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
  public rcu_rcu_fornitura with_N_ID_TRACCIA(java.math.BigDecimal N_ID_TRACCIA) {
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
  public rcu_rcu_fornitura with_N_ID_S_PREC(java.math.BigDecimal N_ID_S_PREC) {
    this.N_ID_S_PREC = N_ID_S_PREC;
    return this;
  }
  private String T_DIRITTO_TUTELA;
  public String get_T_DIRITTO_TUTELA() {
    return T_DIRITTO_TUTELA;
  }
  public void set_T_DIRITTO_TUTELA(String T_DIRITTO_TUTELA) {
    this.T_DIRITTO_TUTELA = T_DIRITTO_TUTELA;
  }
  public rcu_rcu_fornitura with_T_DIRITTO_TUTELA(String T_DIRITTO_TUTELA) {
    this.T_DIRITTO_TUTELA = T_DIRITTO_TUTELA;
    return this;
  }
  private String T_CODICE_UFFICIO;
  public String get_T_CODICE_UFFICIO() {
    return T_CODICE_UFFICIO;
  }
  public void set_T_CODICE_UFFICIO(String T_CODICE_UFFICIO) {
    this.T_CODICE_UFFICIO = T_CODICE_UFFICIO;
  }
  public rcu_rcu_fornitura with_T_CODICE_UFFICIO(String T_CODICE_UFFICIO) {
    this.T_CODICE_UFFICIO = T_CODICE_UFFICIO;
    return this;
  }
  private String T_PAGAMENTO_IVA;
  public String get_T_PAGAMENTO_IVA() {
    return T_PAGAMENTO_IVA;
  }
  public void set_T_PAGAMENTO_IVA(String T_PAGAMENTO_IVA) {
    this.T_PAGAMENTO_IVA = T_PAGAMENTO_IVA;
  }
  public rcu_rcu_fornitura with_T_PAGAMENTO_IVA(String T_PAGAMENTO_IVA) {
    this.T_PAGAMENTO_IVA = T_PAGAMENTO_IVA;
    return this;
  }
  private String T_ADDIZ_PROVINCIALE;
  public String get_T_ADDIZ_PROVINCIALE() {
    return T_ADDIZ_PROVINCIALE;
  }
  public void set_T_ADDIZ_PROVINCIALE(String T_ADDIZ_PROVINCIALE) {
    this.T_ADDIZ_PROVINCIALE = T_ADDIZ_PROVINCIALE;
  }
  public rcu_rcu_fornitura with_T_ADDIZ_PROVINCIALE(String T_ADDIZ_PROVINCIALE) {
    this.T_ADDIZ_PROVINCIALE = T_ADDIZ_PROVINCIALE;
    return this;
  }
  private String T_ADDIZ_COMUNALE;
  public String get_T_ADDIZ_COMUNALE() {
    return T_ADDIZ_COMUNALE;
  }
  public void set_T_ADDIZ_COMUNALE(String T_ADDIZ_COMUNALE) {
    this.T_ADDIZ_COMUNALE = T_ADDIZ_COMUNALE;
  }
  public rcu_rcu_fornitura with_T_ADDIZ_COMUNALE(String T_ADDIZ_COMUNALE) {
    this.T_ADDIZ_COMUNALE = T_ADDIZ_COMUNALE;
    return this;
  }
  private String T_TELEFONO;
  public String get_T_TELEFONO() {
    return T_TELEFONO;
  }
  public void set_T_TELEFONO(String T_TELEFONO) {
    this.T_TELEFONO = T_TELEFONO;
  }
  public rcu_rcu_fornitura with_T_TELEFONO(String T_TELEFONO) {
    this.T_TELEFONO = T_TELEFONO;
    return this;
  }
  private String T_IVA;
  public String get_T_IVA() {
    return T_IVA;
  }
  public void set_T_IVA(String T_IVA) {
    this.T_IVA = T_IVA;
  }
  public rcu_rcu_fornitura with_T_IVA(String T_IVA) {
    this.T_IVA = T_IVA;
    return this;
  }
  private String T_IMPOSTE;
  public String get_T_IMPOSTE() {
    return T_IMPOSTE;
  }
  public void set_T_IMPOSTE(String T_IMPOSTE) {
    this.T_IMPOSTE = T_IMPOSTE;
  }
  public rcu_rcu_fornitura with_T_IMPOSTE(String T_IMPOSTE) {
    this.T_IMPOSTE = T_IMPOSTE;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcu_rcu_fornitura)) {
      return false;
    }
    rcu_rcu_fornitura that = (rcu_rcu_fornitura) o;
    boolean equal = true;
    equal = equal && (this.N_ID_FORNITURA == null ? that.N_ID_FORNITURA == null : this.N_ID_FORNITURA.equals(that.N_ID_FORNITURA));
    equal = equal && (this.N_ID_POD == null ? that.N_ID_POD == null : this.N_ID_POD.equals(that.N_ID_POD));
    equal = equal && (this.N_ID_CLIENTE == null ? that.N_ID_CLIENTE == null : this.N_ID_CLIENTE.equals(that.N_ID_CLIENTE));
    equal = equal && (this.D_INIZIO_TITOLARITA == null ? that.D_INIZIO_TITOLARITA == null : this.D_INIZIO_TITOLARITA.equals(that.D_INIZIO_TITOLARITA));
    equal = equal && (this.D_FINE_TITOLARITA == null ? that.D_FINE_TITOLARITA == null : this.D_FINE_TITOLARITA.equals(that.D_FINE_TITOLARITA));
    equal = equal && (this.T_COD_CONTRATTO == null ? that.T_COD_CONTRATTO == null : this.T_COD_CONTRATTO.equals(that.T_COD_CONTRATTO));
    equal = equal && (this.D_STIPULA_CONTRATTO == null ? that.D_STIPULA_CONTRATTO == null : this.D_STIPULA_CONTRATTO.equals(that.D_STIPULA_CONTRATTO));
    equal = equal && (this.D_MESE_RINNOVO == null ? that.D_MESE_RINNOVO == null : this.D_MESE_RINNOVO.equals(that.D_MESE_RINNOVO));
    equal = equal && (this.N_IVA == null ? that.N_IVA == null : this.N_IVA.equals(that.N_IVA));
    equal = equal && (this.B_DISALIMENTABILITA == null ? that.B_DISALIMENTABILITA == null : this.B_DISALIMENTABILITA.equals(that.B_DISALIMENTABILITA));
    equal = equal && (this.T_TARIFFA_DISTR == null ? that.T_TARIFFA_DISTR == null : this.T_TARIFFA_DISTR.equals(that.T_TARIFFA_DISTR));
    equal = equal && (this.T_CODICE_ATECO == null ? that.T_CODICE_ATECO == null : this.T_CODICE_ATECO.equals(that.T_CODICE_ATECO));
    equal = equal && (this.N_ID_FORNITORE == null ? that.N_ID_FORNITORE == null : this.N_ID_FORNITORE.equals(that.N_ID_FORNITORE));
    equal = equal && (this.T_RUOLO_FORNITORE == null ? that.T_RUOLO_FORNITORE == null : this.T_RUOLO_FORNITORE.equals(that.T_RUOLO_FORNITORE));
    equal = equal && (this.T_TIPO_MERCATO == null ? that.T_TIPO_MERCATO == null : this.T_TIPO_MERCATO.equals(that.T_TIPO_MERCATO));
    equal = equal && (this.B_SALVAGUARDIA == null ? that.B_SALVAGUARDIA == null : this.B_SALVAGUARDIA.equals(that.B_SALVAGUARDIA));
    equal = equal && (this.T_BONUS_SOCIALE == null ? that.T_BONUS_SOCIALE == null : this.T_BONUS_SOCIALE.equals(that.T_BONUS_SOCIALE));
    equal = equal && (this.D_INIZIO_BONUS == null ? that.D_INIZIO_BONUS == null : this.D_INIZIO_BONUS.equals(that.D_INIZIO_BONUS));
    equal = equal && (this.D_FINE_BONUS == null ? that.D_FINE_BONUS == null : this.D_FINE_BONUS.equals(that.D_FINE_BONUS));
    equal = equal && (this.T_COMUNIC_BONUS == null ? that.T_COMUNIC_BONUS == null : this.T_COMUNIC_BONUS.equals(that.T_COMUNIC_BONUS));
    equal = equal && (this.N_IMPOSTE == null ? that.N_IMPOSTE == null : this.N_IMPOSTE.equals(that.N_IMPOSTE));
    equal = equal && (this.N_ID_INDIR_ESAZIONE == null ? that.N_ID_INDIR_ESAZIONE == null : this.N_ID_INDIR_ESAZIONE.equals(that.N_ID_INDIR_ESAZIONE));
    equal = equal && (this.N_ID_INDIR_COMUNIC == null ? that.N_ID_INDIR_COMUNIC == null : this.N_ID_INDIR_COMUNIC.equals(that.N_ID_INDIR_COMUNIC));
    equal = equal && (this.T_NOTA == null ? that.T_NOTA == null : this.T_NOTA.equals(that.T_NOTA));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.T_DIRITTO_TUTELA == null ? that.T_DIRITTO_TUTELA == null : this.T_DIRITTO_TUTELA.equals(that.T_DIRITTO_TUTELA));
    equal = equal && (this.T_CODICE_UFFICIO == null ? that.T_CODICE_UFFICIO == null : this.T_CODICE_UFFICIO.equals(that.T_CODICE_UFFICIO));
    equal = equal && (this.T_PAGAMENTO_IVA == null ? that.T_PAGAMENTO_IVA == null : this.T_PAGAMENTO_IVA.equals(that.T_PAGAMENTO_IVA));
    equal = equal && (this.T_ADDIZ_PROVINCIALE == null ? that.T_ADDIZ_PROVINCIALE == null : this.T_ADDIZ_PROVINCIALE.equals(that.T_ADDIZ_PROVINCIALE));
    equal = equal && (this.T_ADDIZ_COMUNALE == null ? that.T_ADDIZ_COMUNALE == null : this.T_ADDIZ_COMUNALE.equals(that.T_ADDIZ_COMUNALE));
    equal = equal && (this.T_TELEFONO == null ? that.T_TELEFONO == null : this.T_TELEFONO.equals(that.T_TELEFONO));
    equal = equal && (this.T_IVA == null ? that.T_IVA == null : this.T_IVA.equals(that.T_IVA));
    equal = equal && (this.T_IMPOSTE == null ? that.T_IMPOSTE == null : this.T_IMPOSTE.equals(that.T_IMPOSTE));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof rcu_rcu_fornitura)) {
      return false;
    }
    rcu_rcu_fornitura that = (rcu_rcu_fornitura) o;
    boolean equal = true;
    equal = equal && (this.N_ID_FORNITURA == null ? that.N_ID_FORNITURA == null : this.N_ID_FORNITURA.equals(that.N_ID_FORNITURA));
    equal = equal && (this.N_ID_POD == null ? that.N_ID_POD == null : this.N_ID_POD.equals(that.N_ID_POD));
    equal = equal && (this.N_ID_CLIENTE == null ? that.N_ID_CLIENTE == null : this.N_ID_CLIENTE.equals(that.N_ID_CLIENTE));
    equal = equal && (this.D_INIZIO_TITOLARITA == null ? that.D_INIZIO_TITOLARITA == null : this.D_INIZIO_TITOLARITA.equals(that.D_INIZIO_TITOLARITA));
    equal = equal && (this.D_FINE_TITOLARITA == null ? that.D_FINE_TITOLARITA == null : this.D_FINE_TITOLARITA.equals(that.D_FINE_TITOLARITA));
    equal = equal && (this.T_COD_CONTRATTO == null ? that.T_COD_CONTRATTO == null : this.T_COD_CONTRATTO.equals(that.T_COD_CONTRATTO));
    equal = equal && (this.D_STIPULA_CONTRATTO == null ? that.D_STIPULA_CONTRATTO == null : this.D_STIPULA_CONTRATTO.equals(that.D_STIPULA_CONTRATTO));
    equal = equal && (this.D_MESE_RINNOVO == null ? that.D_MESE_RINNOVO == null : this.D_MESE_RINNOVO.equals(that.D_MESE_RINNOVO));
    equal = equal && (this.N_IVA == null ? that.N_IVA == null : this.N_IVA.equals(that.N_IVA));
    equal = equal && (this.B_DISALIMENTABILITA == null ? that.B_DISALIMENTABILITA == null : this.B_DISALIMENTABILITA.equals(that.B_DISALIMENTABILITA));
    equal = equal && (this.T_TARIFFA_DISTR == null ? that.T_TARIFFA_DISTR == null : this.T_TARIFFA_DISTR.equals(that.T_TARIFFA_DISTR));
    equal = equal && (this.T_CODICE_ATECO == null ? that.T_CODICE_ATECO == null : this.T_CODICE_ATECO.equals(that.T_CODICE_ATECO));
    equal = equal && (this.N_ID_FORNITORE == null ? that.N_ID_FORNITORE == null : this.N_ID_FORNITORE.equals(that.N_ID_FORNITORE));
    equal = equal && (this.T_RUOLO_FORNITORE == null ? that.T_RUOLO_FORNITORE == null : this.T_RUOLO_FORNITORE.equals(that.T_RUOLO_FORNITORE));
    equal = equal && (this.T_TIPO_MERCATO == null ? that.T_TIPO_MERCATO == null : this.T_TIPO_MERCATO.equals(that.T_TIPO_MERCATO));
    equal = equal && (this.B_SALVAGUARDIA == null ? that.B_SALVAGUARDIA == null : this.B_SALVAGUARDIA.equals(that.B_SALVAGUARDIA));
    equal = equal && (this.T_BONUS_SOCIALE == null ? that.T_BONUS_SOCIALE == null : this.T_BONUS_SOCIALE.equals(that.T_BONUS_SOCIALE));
    equal = equal && (this.D_INIZIO_BONUS == null ? that.D_INIZIO_BONUS == null : this.D_INIZIO_BONUS.equals(that.D_INIZIO_BONUS));
    equal = equal && (this.D_FINE_BONUS == null ? that.D_FINE_BONUS == null : this.D_FINE_BONUS.equals(that.D_FINE_BONUS));
    equal = equal && (this.T_COMUNIC_BONUS == null ? that.T_COMUNIC_BONUS == null : this.T_COMUNIC_BONUS.equals(that.T_COMUNIC_BONUS));
    equal = equal && (this.N_IMPOSTE == null ? that.N_IMPOSTE == null : this.N_IMPOSTE.equals(that.N_IMPOSTE));
    equal = equal && (this.N_ID_INDIR_ESAZIONE == null ? that.N_ID_INDIR_ESAZIONE == null : this.N_ID_INDIR_ESAZIONE.equals(that.N_ID_INDIR_ESAZIONE));
    equal = equal && (this.N_ID_INDIR_COMUNIC == null ? that.N_ID_INDIR_COMUNIC == null : this.N_ID_INDIR_COMUNIC.equals(that.N_ID_INDIR_COMUNIC));
    equal = equal && (this.T_NOTA == null ? that.T_NOTA == null : this.T_NOTA.equals(that.T_NOTA));
    equal = equal && (this.D_AGGIORNAMENTO == null ? that.D_AGGIORNAMENTO == null : this.D_AGGIORNAMENTO.equals(that.D_AGGIORNAMENTO));
    equal = equal && (this.N_ID_TRACCIA == null ? that.N_ID_TRACCIA == null : this.N_ID_TRACCIA.equals(that.N_ID_TRACCIA));
    equal = equal && (this.N_ID_S_PREC == null ? that.N_ID_S_PREC == null : this.N_ID_S_PREC.equals(that.N_ID_S_PREC));
    equal = equal && (this.T_DIRITTO_TUTELA == null ? that.T_DIRITTO_TUTELA == null : this.T_DIRITTO_TUTELA.equals(that.T_DIRITTO_TUTELA));
    equal = equal && (this.T_CODICE_UFFICIO == null ? that.T_CODICE_UFFICIO == null : this.T_CODICE_UFFICIO.equals(that.T_CODICE_UFFICIO));
    equal = equal && (this.T_PAGAMENTO_IVA == null ? that.T_PAGAMENTO_IVA == null : this.T_PAGAMENTO_IVA.equals(that.T_PAGAMENTO_IVA));
    equal = equal && (this.T_ADDIZ_PROVINCIALE == null ? that.T_ADDIZ_PROVINCIALE == null : this.T_ADDIZ_PROVINCIALE.equals(that.T_ADDIZ_PROVINCIALE));
    equal = equal && (this.T_ADDIZ_COMUNALE == null ? that.T_ADDIZ_COMUNALE == null : this.T_ADDIZ_COMUNALE.equals(that.T_ADDIZ_COMUNALE));
    equal = equal && (this.T_TELEFONO == null ? that.T_TELEFONO == null : this.T_TELEFONO.equals(that.T_TELEFONO));
    equal = equal && (this.T_IVA == null ? that.T_IVA == null : this.T_IVA.equals(that.T_IVA));
    equal = equal && (this.T_IMPOSTE == null ? that.T_IMPOSTE == null : this.T_IMPOSTE.equals(that.T_IMPOSTE));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_FORNITURA = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_POD = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.N_ID_CLIENTE = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.D_INIZIO_TITOLARITA = JdbcWritableBridge.readString(4, __dbResults);
    this.D_FINE_TITOLARITA = JdbcWritableBridge.readString(5, __dbResults);
    this.T_COD_CONTRATTO = JdbcWritableBridge.readString(6, __dbResults);
    this.D_STIPULA_CONTRATTO = JdbcWritableBridge.readString(7, __dbResults);
    this.D_MESE_RINNOVO = JdbcWritableBridge.readString(8, __dbResults);
    this.N_IVA = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.B_DISALIMENTABILITA = JdbcWritableBridge.readString(10, __dbResults);
    this.T_TARIFFA_DISTR = JdbcWritableBridge.readString(11, __dbResults);
    this.T_CODICE_ATECO = JdbcWritableBridge.readString(12, __dbResults);
    this.N_ID_FORNITORE = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.T_RUOLO_FORNITORE = JdbcWritableBridge.readString(14, __dbResults);
    this.T_TIPO_MERCATO = JdbcWritableBridge.readString(15, __dbResults);
    this.B_SALVAGUARDIA = JdbcWritableBridge.readString(16, __dbResults);
    this.T_BONUS_SOCIALE = JdbcWritableBridge.readString(17, __dbResults);
    this.D_INIZIO_BONUS = JdbcWritableBridge.readString(18, __dbResults);
    this.D_FINE_BONUS = JdbcWritableBridge.readString(19, __dbResults);
    this.T_COMUNIC_BONUS = JdbcWritableBridge.readString(20, __dbResults);
    this.N_IMPOSTE = JdbcWritableBridge.readBigDecimal(21, __dbResults);
    this.N_ID_INDIR_ESAZIONE = JdbcWritableBridge.readBigDecimal(22, __dbResults);
    this.N_ID_INDIR_COMUNIC = JdbcWritableBridge.readBigDecimal(23, __dbResults);
    this.T_NOTA = JdbcWritableBridge.readString(24, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(25, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(26, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(27, __dbResults);
    this.T_DIRITTO_TUTELA = JdbcWritableBridge.readString(28, __dbResults);
    this.T_CODICE_UFFICIO = JdbcWritableBridge.readString(29, __dbResults);
    this.T_PAGAMENTO_IVA = JdbcWritableBridge.readString(30, __dbResults);
    this.T_ADDIZ_PROVINCIALE = JdbcWritableBridge.readString(31, __dbResults);
    this.T_ADDIZ_COMUNALE = JdbcWritableBridge.readString(32, __dbResults);
    this.T_TELEFONO = JdbcWritableBridge.readString(33, __dbResults);
    this.T_IVA = JdbcWritableBridge.readString(34, __dbResults);
    this.T_IMPOSTE = JdbcWritableBridge.readString(35, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_FORNITURA = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.N_ID_POD = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.N_ID_CLIENTE = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.D_INIZIO_TITOLARITA = JdbcWritableBridge.readString(4, __dbResults);
    this.D_FINE_TITOLARITA = JdbcWritableBridge.readString(5, __dbResults);
    this.T_COD_CONTRATTO = JdbcWritableBridge.readString(6, __dbResults);
    this.D_STIPULA_CONTRATTO = JdbcWritableBridge.readString(7, __dbResults);
    this.D_MESE_RINNOVO = JdbcWritableBridge.readString(8, __dbResults);
    this.N_IVA = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.B_DISALIMENTABILITA = JdbcWritableBridge.readString(10, __dbResults);
    this.T_TARIFFA_DISTR = JdbcWritableBridge.readString(11, __dbResults);
    this.T_CODICE_ATECO = JdbcWritableBridge.readString(12, __dbResults);
    this.N_ID_FORNITORE = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.T_RUOLO_FORNITORE = JdbcWritableBridge.readString(14, __dbResults);
    this.T_TIPO_MERCATO = JdbcWritableBridge.readString(15, __dbResults);
    this.B_SALVAGUARDIA = JdbcWritableBridge.readString(16, __dbResults);
    this.T_BONUS_SOCIALE = JdbcWritableBridge.readString(17, __dbResults);
    this.D_INIZIO_BONUS = JdbcWritableBridge.readString(18, __dbResults);
    this.D_FINE_BONUS = JdbcWritableBridge.readString(19, __dbResults);
    this.T_COMUNIC_BONUS = JdbcWritableBridge.readString(20, __dbResults);
    this.N_IMPOSTE = JdbcWritableBridge.readBigDecimal(21, __dbResults);
    this.N_ID_INDIR_ESAZIONE = JdbcWritableBridge.readBigDecimal(22, __dbResults);
    this.N_ID_INDIR_COMUNIC = JdbcWritableBridge.readBigDecimal(23, __dbResults);
    this.T_NOTA = JdbcWritableBridge.readString(24, __dbResults);
    this.D_AGGIORNAMENTO = JdbcWritableBridge.readString(25, __dbResults);
    this.N_ID_TRACCIA = JdbcWritableBridge.readBigDecimal(26, __dbResults);
    this.N_ID_S_PREC = JdbcWritableBridge.readBigDecimal(27, __dbResults);
    this.T_DIRITTO_TUTELA = JdbcWritableBridge.readString(28, __dbResults);
    this.T_CODICE_UFFICIO = JdbcWritableBridge.readString(29, __dbResults);
    this.T_PAGAMENTO_IVA = JdbcWritableBridge.readString(30, __dbResults);
    this.T_ADDIZ_PROVINCIALE = JdbcWritableBridge.readString(31, __dbResults);
    this.T_ADDIZ_COMUNALE = JdbcWritableBridge.readString(32, __dbResults);
    this.T_TELEFONO = JdbcWritableBridge.readString(33, __dbResults);
    this.T_IVA = JdbcWritableBridge.readString(34, __dbResults);
    this.T_IMPOSTE = JdbcWritableBridge.readString(35, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_POD, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CLIENTE, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_INIZIO_TITOLARITA, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_FINE_TITOLARITA, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CONTRATTO, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_STIPULA_CONTRATTO, 7 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_MESE_RINNOVO, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_IVA, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_DISALIMENTABILITA, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TARIFFA_DISTR, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_ATECO, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_FORNITORE, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_RUOLO_FORNITORE, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_MERCATO, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_SALVAGUARDIA, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_BONUS_SOCIALE, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_INIZIO_BONUS, 18 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_FINE_BONUS, 19 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNIC_BONUS, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_IMPOSTE, 21 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_INDIR_ESAZIONE, 22 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_INDIR_COMUNIC, 23 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTA, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 25 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 26 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 27 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_DIRITTO_TUTELA, 28 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_UFFICIO, 29 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PAGAMENTO_IVA, 30 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ADDIZ_PROVINCIALE, 31 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ADDIZ_COMUNALE, 32 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TELEFONO, 33 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_IVA, 34 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_IMPOSTE, 35 + __off, 12, __dbStmt);
    return 35;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_FORNITURA, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_POD, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CLIENTE, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(D_INIZIO_TITOLARITA, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_FINE_TITOLARITA, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_COD_CONTRATTO, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_STIPULA_CONTRATTO, 7 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_MESE_RINNOVO, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_IVA, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(B_DISALIMENTABILITA, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TARIFFA_DISTR, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_ATECO, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_FORNITORE, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_RUOLO_FORNITORE, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TIPO_MERCATO, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(B_SALVAGUARDIA, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_BONUS_SOCIALE, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_INIZIO_BONUS, 18 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(D_FINE_BONUS, 19 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(T_COMUNIC_BONUS, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_IMPOSTE, 21 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_INDIR_ESAZIONE, 22 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_INDIR_COMUNIC, 23 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_NOTA, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(D_AGGIORNAMENTO, 25 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_TRACCIA, 26 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_S_PREC, 27 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(T_DIRITTO_TUTELA, 28 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_CODICE_UFFICIO, 29 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_PAGAMENTO_IVA, 30 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ADDIZ_PROVINCIALE, 31 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_ADDIZ_COMUNALE, 32 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_TELEFONO, 33 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_IVA, 34 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(T_IMPOSTE, 35 + __off, 12, __dbStmt);
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
        this.N_ID_POD = null;
    } else {
    this.N_ID_POD = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_CLIENTE = null;
    } else {
    this.N_ID_CLIENTE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_INIZIO_TITOLARITA = null;
    } else {
    this.D_INIZIO_TITOLARITA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_FINE_TITOLARITA = null;
    } else {
    this.D_FINE_TITOLARITA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COD_CONTRATTO = null;
    } else {
    this.T_COD_CONTRATTO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_STIPULA_CONTRATTO = null;
    } else {
    this.D_STIPULA_CONTRATTO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_MESE_RINNOVO = null;
    } else {
    this.D_MESE_RINNOVO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_IVA = null;
    } else {
    this.N_IVA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_DISALIMENTABILITA = null;
    } else {
    this.B_DISALIMENTABILITA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TARIFFA_DISTR = null;
    } else {
    this.T_TARIFFA_DISTR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_ATECO = null;
    } else {
    this.T_CODICE_ATECO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_FORNITORE = null;
    } else {
    this.N_ID_FORNITORE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_RUOLO_FORNITORE = null;
    } else {
    this.T_RUOLO_FORNITORE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TIPO_MERCATO = null;
    } else {
    this.T_TIPO_MERCATO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.B_SALVAGUARDIA = null;
    } else {
    this.B_SALVAGUARDIA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_BONUS_SOCIALE = null;
    } else {
    this.T_BONUS_SOCIALE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_INIZIO_BONUS = null;
    } else {
    this.D_INIZIO_BONUS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.D_FINE_BONUS = null;
    } else {
    this.D_FINE_BONUS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_COMUNIC_BONUS = null;
    } else {
    this.T_COMUNIC_BONUS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_IMPOSTE = null;
    } else {
    this.N_IMPOSTE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_INDIR_ESAZIONE = null;
    } else {
    this.N_ID_INDIR_ESAZIONE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_INDIR_COMUNIC = null;
    } else {
    this.N_ID_INDIR_COMUNIC = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
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
        this.T_DIRITTO_TUTELA = null;
    } else {
    this.T_DIRITTO_TUTELA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_CODICE_UFFICIO = null;
    } else {
    this.T_CODICE_UFFICIO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_PAGAMENTO_IVA = null;
    } else {
    this.T_PAGAMENTO_IVA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ADDIZ_PROVINCIALE = null;
    } else {
    this.T_ADDIZ_PROVINCIALE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_ADDIZ_COMUNALE = null;
    } else {
    this.T_ADDIZ_COMUNALE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_TELEFONO = null;
    } else {
    this.T_TELEFONO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_IVA = null;
    } else {
    this.T_IVA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.T_IMPOSTE = null;
    } else {
    this.T_IMPOSTE = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FORNITURA, __dataOut);
    }
    if (null == this.N_ID_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_POD, __dataOut);
    }
    if (null == this.N_ID_CLIENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CLIENTE, __dataOut);
    }
    if (null == this.D_INIZIO_TITOLARITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_INIZIO_TITOLARITA);
    }
    if (null == this.D_FINE_TITOLARITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_FINE_TITOLARITA);
    }
    if (null == this.T_COD_CONTRATTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_CONTRATTO);
    }
    if (null == this.D_STIPULA_CONTRATTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_STIPULA_CONTRATTO);
    }
    if (null == this.D_MESE_RINNOVO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_MESE_RINNOVO);
    }
    if (null == this.N_IVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_IVA, __dataOut);
    }
    if (null == this.B_DISALIMENTABILITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_DISALIMENTABILITA);
    }
    if (null == this.T_TARIFFA_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TARIFFA_DISTR);
    }
    if (null == this.T_CODICE_ATECO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_ATECO);
    }
    if (null == this.N_ID_FORNITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FORNITORE, __dataOut);
    }
    if (null == this.T_RUOLO_FORNITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RUOLO_FORNITORE);
    }
    if (null == this.T_TIPO_MERCATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_MERCATO);
    }
    if (null == this.B_SALVAGUARDIA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_SALVAGUARDIA);
    }
    if (null == this.T_BONUS_SOCIALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_BONUS_SOCIALE);
    }
    if (null == this.D_INIZIO_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_INIZIO_BONUS);
    }
    if (null == this.D_FINE_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_FINE_BONUS);
    }
    if (null == this.T_COMUNIC_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNIC_BONUS);
    }
    if (null == this.N_IMPOSTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_IMPOSTE, __dataOut);
    }
    if (null == this.N_ID_INDIR_ESAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_INDIR_ESAZIONE, __dataOut);
    }
    if (null == this.N_ID_INDIR_COMUNIC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_INDIR_COMUNIC, __dataOut);
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
    if (null == this.T_DIRITTO_TUTELA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DIRITTO_TUTELA);
    }
    if (null == this.T_CODICE_UFFICIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_UFFICIO);
    }
    if (null == this.T_PAGAMENTO_IVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PAGAMENTO_IVA);
    }
    if (null == this.T_ADDIZ_PROVINCIALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ADDIZ_PROVINCIALE);
    }
    if (null == this.T_ADDIZ_COMUNALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ADDIZ_COMUNALE);
    }
    if (null == this.T_TELEFONO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TELEFONO);
    }
    if (null == this.T_IVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_IVA);
    }
    if (null == this.T_IMPOSTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_IMPOSTE);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FORNITURA, __dataOut);
    }
    if (null == this.N_ID_POD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_POD, __dataOut);
    }
    if (null == this.N_ID_CLIENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CLIENTE, __dataOut);
    }
    if (null == this.D_INIZIO_TITOLARITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_INIZIO_TITOLARITA);
    }
    if (null == this.D_FINE_TITOLARITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_FINE_TITOLARITA);
    }
    if (null == this.T_COD_CONTRATTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COD_CONTRATTO);
    }
    if (null == this.D_STIPULA_CONTRATTO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_STIPULA_CONTRATTO);
    }
    if (null == this.D_MESE_RINNOVO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_MESE_RINNOVO);
    }
    if (null == this.N_IVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_IVA, __dataOut);
    }
    if (null == this.B_DISALIMENTABILITA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_DISALIMENTABILITA);
    }
    if (null == this.T_TARIFFA_DISTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TARIFFA_DISTR);
    }
    if (null == this.T_CODICE_ATECO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_ATECO);
    }
    if (null == this.N_ID_FORNITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_FORNITORE, __dataOut);
    }
    if (null == this.T_RUOLO_FORNITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_RUOLO_FORNITORE);
    }
    if (null == this.T_TIPO_MERCATO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TIPO_MERCATO);
    }
    if (null == this.B_SALVAGUARDIA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, B_SALVAGUARDIA);
    }
    if (null == this.T_BONUS_SOCIALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_BONUS_SOCIALE);
    }
    if (null == this.D_INIZIO_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_INIZIO_BONUS);
    }
    if (null == this.D_FINE_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, D_FINE_BONUS);
    }
    if (null == this.T_COMUNIC_BONUS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_COMUNIC_BONUS);
    }
    if (null == this.N_IMPOSTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_IMPOSTE, __dataOut);
    }
    if (null == this.N_ID_INDIR_ESAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_INDIR_ESAZIONE, __dataOut);
    }
    if (null == this.N_ID_INDIR_COMUNIC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_INDIR_COMUNIC, __dataOut);
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
    if (null == this.T_DIRITTO_TUTELA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_DIRITTO_TUTELA);
    }
    if (null == this.T_CODICE_UFFICIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_CODICE_UFFICIO);
    }
    if (null == this.T_PAGAMENTO_IVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_PAGAMENTO_IVA);
    }
    if (null == this.T_ADDIZ_PROVINCIALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ADDIZ_PROVINCIALE);
    }
    if (null == this.T_ADDIZ_COMUNALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_ADDIZ_COMUNALE);
    }
    if (null == this.T_TELEFONO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_TELEFONO);
    }
    if (null == this.T_IVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_IVA);
    }
    if (null == this.T_IMPOSTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, T_IMPOSTE);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_POD==null?"":N_ID_POD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CLIENTE==null?"":N_ID_CLIENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_INIZIO_TITOLARITA==null?"":D_INIZIO_TITOLARITA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_FINE_TITOLARITA==null?"":D_FINE_TITOLARITA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CONTRATTO==null?"":T_COD_CONTRATTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_STIPULA_CONTRATTO==null?"":D_STIPULA_CONTRATTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_MESE_RINNOVO==null?"":D_MESE_RINNOVO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_IVA==null?"":N_IVA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_DISALIMENTABILITA==null?"":B_DISALIMENTABILITA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TARIFFA_DISTR==null?"":T_TARIFFA_DISTR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_ATECO==null?"":T_CODICE_ATECO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_FORNITORE==null?"":N_ID_FORNITORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RUOLO_FORNITORE==null?"":T_RUOLO_FORNITORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_MERCATO==null?"":T_TIPO_MERCATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_SALVAGUARDIA==null?"":B_SALVAGUARDIA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_BONUS_SOCIALE==null?"":T_BONUS_SOCIALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_INIZIO_BONUS==null?"":D_INIZIO_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_FINE_BONUS==null?"":D_FINE_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNIC_BONUS==null?"":T_COMUNIC_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_IMPOSTE==null?"":N_IMPOSTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_INDIR_ESAZIONE==null?"":N_ID_INDIR_ESAZIONE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_INDIR_COMUNIC==null?"":N_ID_INDIR_COMUNIC.toPlainString(), delimiters));
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DIRITTO_TUTELA==null?"":T_DIRITTO_TUTELA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_UFFICIO==null?"":T_CODICE_UFFICIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PAGAMENTO_IVA==null?"":T_PAGAMENTO_IVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ADDIZ_PROVINCIALE==null?"":T_ADDIZ_PROVINCIALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ADDIZ_COMUNALE==null?"":T_ADDIZ_COMUNALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TELEFONO==null?"":T_TELEFONO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_IVA==null?"":T_IVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_IMPOSTE==null?"":T_IMPOSTE, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_FORNITURA==null?"":N_ID_FORNITURA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_POD==null?"":N_ID_POD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CLIENTE==null?"":N_ID_CLIENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_INIZIO_TITOLARITA==null?"":D_INIZIO_TITOLARITA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_FINE_TITOLARITA==null?"":D_FINE_TITOLARITA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COD_CONTRATTO==null?"":T_COD_CONTRATTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_STIPULA_CONTRATTO==null?"":D_STIPULA_CONTRATTO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_MESE_RINNOVO==null?"":D_MESE_RINNOVO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_IVA==null?"":N_IVA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_DISALIMENTABILITA==null?"":B_DISALIMENTABILITA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TARIFFA_DISTR==null?"":T_TARIFFA_DISTR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_ATECO==null?"":T_CODICE_ATECO, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_FORNITORE==null?"":N_ID_FORNITORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_RUOLO_FORNITORE==null?"":T_RUOLO_FORNITORE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TIPO_MERCATO==null?"":T_TIPO_MERCATO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(B_SALVAGUARDIA==null?"":B_SALVAGUARDIA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_BONUS_SOCIALE==null?"":T_BONUS_SOCIALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_INIZIO_BONUS==null?"":D_INIZIO_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(D_FINE_BONUS==null?"":D_FINE_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_COMUNIC_BONUS==null?"":T_COMUNIC_BONUS, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_IMPOSTE==null?"":N_IMPOSTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_INDIR_ESAZIONE==null?"":N_ID_INDIR_ESAZIONE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_INDIR_COMUNIC==null?"":N_ID_INDIR_COMUNIC.toPlainString(), delimiters));
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
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_DIRITTO_TUTELA==null?"":T_DIRITTO_TUTELA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_CODICE_UFFICIO==null?"":T_CODICE_UFFICIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_PAGAMENTO_IVA==null?"":T_PAGAMENTO_IVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ADDIZ_PROVINCIALE==null?"":T_ADDIZ_PROVINCIALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_ADDIZ_COMUNALE==null?"":T_ADDIZ_COMUNALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_TELEFONO==null?"":T_TELEFONO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_IVA==null?"":T_IVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(T_IMPOSTE==null?"":T_IMPOSTE, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_POD = null; } else {
      this.N_ID_POD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CLIENTE = null; } else {
      this.N_ID_CLIENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_INIZIO_TITOLARITA = null; } else {
      this.D_INIZIO_TITOLARITA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_FINE_TITOLARITA = null; } else {
      this.D_FINE_TITOLARITA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_CONTRATTO = null; } else {
      this.T_COD_CONTRATTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_STIPULA_CONTRATTO = null; } else {
      this.D_STIPULA_CONTRATTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_MESE_RINNOVO = null; } else {
      this.D_MESE_RINNOVO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_IVA = null; } else {
      this.N_IVA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_DISALIMENTABILITA = null; } else {
      this.B_DISALIMENTABILITA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TARIFFA_DISTR = null; } else {
      this.T_TARIFFA_DISTR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_ATECO = null; } else {
      this.T_CODICE_ATECO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_FORNITORE = null; } else {
      this.N_ID_FORNITORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RUOLO_FORNITORE = null; } else {
      this.T_RUOLO_FORNITORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_MERCATO = null; } else {
      this.T_TIPO_MERCATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_SALVAGUARDIA = null; } else {
      this.B_SALVAGUARDIA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_BONUS_SOCIALE = null; } else {
      this.T_BONUS_SOCIALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_INIZIO_BONUS = null; } else {
      this.D_INIZIO_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_FINE_BONUS = null; } else {
      this.D_FINE_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNIC_BONUS = null; } else {
      this.T_COMUNIC_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_IMPOSTE = null; } else {
      this.N_IMPOSTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_INDIR_ESAZIONE = null; } else {
      this.N_ID_INDIR_ESAZIONE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_INDIR_COMUNIC = null; } else {
      this.N_ID_INDIR_COMUNIC = new java.math.BigDecimal(__cur_str);
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
    if (__cur_str.equals("null")) { this.T_DIRITTO_TUTELA = null; } else {
      this.T_DIRITTO_TUTELA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_UFFICIO = null; } else {
      this.T_CODICE_UFFICIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PAGAMENTO_IVA = null; } else {
      this.T_PAGAMENTO_IVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ADDIZ_PROVINCIALE = null; } else {
      this.T_ADDIZ_PROVINCIALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ADDIZ_COMUNALE = null; } else {
      this.T_ADDIZ_COMUNALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TELEFONO = null; } else {
      this.T_TELEFONO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_IVA = null; } else {
      this.T_IVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_IMPOSTE = null; } else {
      this.T_IMPOSTE = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_POD = null; } else {
      this.N_ID_POD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CLIENTE = null; } else {
      this.N_ID_CLIENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_INIZIO_TITOLARITA = null; } else {
      this.D_INIZIO_TITOLARITA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_FINE_TITOLARITA = null; } else {
      this.D_FINE_TITOLARITA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COD_CONTRATTO = null; } else {
      this.T_COD_CONTRATTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_STIPULA_CONTRATTO = null; } else {
      this.D_STIPULA_CONTRATTO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_MESE_RINNOVO = null; } else {
      this.D_MESE_RINNOVO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_IVA = null; } else {
      this.N_IVA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_DISALIMENTABILITA = null; } else {
      this.B_DISALIMENTABILITA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TARIFFA_DISTR = null; } else {
      this.T_TARIFFA_DISTR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_ATECO = null; } else {
      this.T_CODICE_ATECO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_FORNITORE = null; } else {
      this.N_ID_FORNITORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_RUOLO_FORNITORE = null; } else {
      this.T_RUOLO_FORNITORE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TIPO_MERCATO = null; } else {
      this.T_TIPO_MERCATO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.B_SALVAGUARDIA = null; } else {
      this.B_SALVAGUARDIA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_BONUS_SOCIALE = null; } else {
      this.T_BONUS_SOCIALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_INIZIO_BONUS = null; } else {
      this.D_INIZIO_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.D_FINE_BONUS = null; } else {
      this.D_FINE_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_COMUNIC_BONUS = null; } else {
      this.T_COMUNIC_BONUS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_IMPOSTE = null; } else {
      this.N_IMPOSTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_INDIR_ESAZIONE = null; } else {
      this.N_ID_INDIR_ESAZIONE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_INDIR_COMUNIC = null; } else {
      this.N_ID_INDIR_COMUNIC = new java.math.BigDecimal(__cur_str);
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
    if (__cur_str.equals("null")) { this.T_DIRITTO_TUTELA = null; } else {
      this.T_DIRITTO_TUTELA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_CODICE_UFFICIO = null; } else {
      this.T_CODICE_UFFICIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_PAGAMENTO_IVA = null; } else {
      this.T_PAGAMENTO_IVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ADDIZ_PROVINCIALE = null; } else {
      this.T_ADDIZ_PROVINCIALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_ADDIZ_COMUNALE = null; } else {
      this.T_ADDIZ_COMUNALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_TELEFONO = null; } else {
      this.T_TELEFONO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_IVA = null; } else {
      this.T_IVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.T_IMPOSTE = null; } else {
      this.T_IMPOSTE = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    rcu_rcu_fornitura o = (rcu_rcu_fornitura) super.clone();
    return o;
  }

  public void clone0(rcu_rcu_fornitura o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_FORNITURA", this.N_ID_FORNITURA);
    __sqoop$field_map.put("N_ID_POD", this.N_ID_POD);
    __sqoop$field_map.put("N_ID_CLIENTE", this.N_ID_CLIENTE);
    __sqoop$field_map.put("D_INIZIO_TITOLARITA", this.D_INIZIO_TITOLARITA);
    __sqoop$field_map.put("D_FINE_TITOLARITA", this.D_FINE_TITOLARITA);
    __sqoop$field_map.put("T_COD_CONTRATTO", this.T_COD_CONTRATTO);
    __sqoop$field_map.put("D_STIPULA_CONTRATTO", this.D_STIPULA_CONTRATTO);
    __sqoop$field_map.put("D_MESE_RINNOVO", this.D_MESE_RINNOVO);
    __sqoop$field_map.put("N_IVA", this.N_IVA);
    __sqoop$field_map.put("B_DISALIMENTABILITA", this.B_DISALIMENTABILITA);
    __sqoop$field_map.put("T_TARIFFA_DISTR", this.T_TARIFFA_DISTR);
    __sqoop$field_map.put("T_CODICE_ATECO", this.T_CODICE_ATECO);
    __sqoop$field_map.put("N_ID_FORNITORE", this.N_ID_FORNITORE);
    __sqoop$field_map.put("T_RUOLO_FORNITORE", this.T_RUOLO_FORNITORE);
    __sqoop$field_map.put("T_TIPO_MERCATO", this.T_TIPO_MERCATO);
    __sqoop$field_map.put("B_SALVAGUARDIA", this.B_SALVAGUARDIA);
    __sqoop$field_map.put("T_BONUS_SOCIALE", this.T_BONUS_SOCIALE);
    __sqoop$field_map.put("D_INIZIO_BONUS", this.D_INIZIO_BONUS);
    __sqoop$field_map.put("D_FINE_BONUS", this.D_FINE_BONUS);
    __sqoop$field_map.put("T_COMUNIC_BONUS", this.T_COMUNIC_BONUS);
    __sqoop$field_map.put("N_IMPOSTE", this.N_IMPOSTE);
    __sqoop$field_map.put("N_ID_INDIR_ESAZIONE", this.N_ID_INDIR_ESAZIONE);
    __sqoop$field_map.put("N_ID_INDIR_COMUNIC", this.N_ID_INDIR_COMUNIC);
    __sqoop$field_map.put("T_NOTA", this.T_NOTA);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("T_DIRITTO_TUTELA", this.T_DIRITTO_TUTELA);
    __sqoop$field_map.put("T_CODICE_UFFICIO", this.T_CODICE_UFFICIO);
    __sqoop$field_map.put("T_PAGAMENTO_IVA", this.T_PAGAMENTO_IVA);
    __sqoop$field_map.put("T_ADDIZ_PROVINCIALE", this.T_ADDIZ_PROVINCIALE);
    __sqoop$field_map.put("T_ADDIZ_COMUNALE", this.T_ADDIZ_COMUNALE);
    __sqoop$field_map.put("T_TELEFONO", this.T_TELEFONO);
    __sqoop$field_map.put("T_IVA", this.T_IVA);
    __sqoop$field_map.put("T_IMPOSTE", this.T_IMPOSTE);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_FORNITURA", this.N_ID_FORNITURA);
    __sqoop$field_map.put("N_ID_POD", this.N_ID_POD);
    __sqoop$field_map.put("N_ID_CLIENTE", this.N_ID_CLIENTE);
    __sqoop$field_map.put("D_INIZIO_TITOLARITA", this.D_INIZIO_TITOLARITA);
    __sqoop$field_map.put("D_FINE_TITOLARITA", this.D_FINE_TITOLARITA);
    __sqoop$field_map.put("T_COD_CONTRATTO", this.T_COD_CONTRATTO);
    __sqoop$field_map.put("D_STIPULA_CONTRATTO", this.D_STIPULA_CONTRATTO);
    __sqoop$field_map.put("D_MESE_RINNOVO", this.D_MESE_RINNOVO);
    __sqoop$field_map.put("N_IVA", this.N_IVA);
    __sqoop$field_map.put("B_DISALIMENTABILITA", this.B_DISALIMENTABILITA);
    __sqoop$field_map.put("T_TARIFFA_DISTR", this.T_TARIFFA_DISTR);
    __sqoop$field_map.put("T_CODICE_ATECO", this.T_CODICE_ATECO);
    __sqoop$field_map.put("N_ID_FORNITORE", this.N_ID_FORNITORE);
    __sqoop$field_map.put("T_RUOLO_FORNITORE", this.T_RUOLO_FORNITORE);
    __sqoop$field_map.put("T_TIPO_MERCATO", this.T_TIPO_MERCATO);
    __sqoop$field_map.put("B_SALVAGUARDIA", this.B_SALVAGUARDIA);
    __sqoop$field_map.put("T_BONUS_SOCIALE", this.T_BONUS_SOCIALE);
    __sqoop$field_map.put("D_INIZIO_BONUS", this.D_INIZIO_BONUS);
    __sqoop$field_map.put("D_FINE_BONUS", this.D_FINE_BONUS);
    __sqoop$field_map.put("T_COMUNIC_BONUS", this.T_COMUNIC_BONUS);
    __sqoop$field_map.put("N_IMPOSTE", this.N_IMPOSTE);
    __sqoop$field_map.put("N_ID_INDIR_ESAZIONE", this.N_ID_INDIR_ESAZIONE);
    __sqoop$field_map.put("N_ID_INDIR_COMUNIC", this.N_ID_INDIR_COMUNIC);
    __sqoop$field_map.put("T_NOTA", this.T_NOTA);
    __sqoop$field_map.put("D_AGGIORNAMENTO", this.D_AGGIORNAMENTO);
    __sqoop$field_map.put("N_ID_TRACCIA", this.N_ID_TRACCIA);
    __sqoop$field_map.put("N_ID_S_PREC", this.N_ID_S_PREC);
    __sqoop$field_map.put("T_DIRITTO_TUTELA", this.T_DIRITTO_TUTELA);
    __sqoop$field_map.put("T_CODICE_UFFICIO", this.T_CODICE_UFFICIO);
    __sqoop$field_map.put("T_PAGAMENTO_IVA", this.T_PAGAMENTO_IVA);
    __sqoop$field_map.put("T_ADDIZ_PROVINCIALE", this.T_ADDIZ_PROVINCIALE);
    __sqoop$field_map.put("T_ADDIZ_COMUNALE", this.T_ADDIZ_COMUNALE);
    __sqoop$field_map.put("T_TELEFONO", this.T_TELEFONO);
    __sqoop$field_map.put("T_IVA", this.T_IVA);
    __sqoop$field_map.put("T_IMPOSTE", this.T_IMPOSTE);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
