// ORM class for table 'prt_rcugas.rcugas_temp_va1'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat Jul 27 09:26:53 CEST 2019
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

public class prt_rcugas_rcugas_temp_va1 extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("N_ID_PRATICA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_PRATICA = (java.math.BigDecimal)value;
      }
    });
    setters.put("PROTOCOLLO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROTOCOLLO = (String)value;
      }
    });
    setters.put("DATA_APERTURA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATA_APERTURA = (String)value;
      }
    });
    setters.put("N_ID_UTENTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_UTENTE = (java.math.BigDecimal)value;
      }
    });
    setters.put("N_ID_VENDITORE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_VENDITORE = (java.math.BigDecimal)value;
      }
    });
    setters.put("COD_PRESTAZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        COD_PRESTAZIONE = (String)value;
      }
    });
    setters.put("DATA_ESECUZIONE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATA_ESECUZIONE = (String)value;
      }
    });
    setters.put("PIVA_UDD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PIVA_UDD = (String)value;
      }
    });
    setters.put("COD_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        COD_PDR = (String)value;
      }
    });
    setters.put("COD_REMI", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        COD_REMI = (String)value;
      }
    });
    setters.put("TIPO_PDR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TIPO_PDR = (String)value;
      }
    });
    setters.put("COD_PROF_PREL_STANDARD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        COD_PROF_PREL_STANDARD = (String)value;
      }
    });
    setters.put("PRELIEVO_ANNUO_PREV", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PRELIEVO_ANNUO_PREV = (String)value;
      }
    });
    setters.put("CF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CF = (String)value;
      }
    });
    setters.put("PIVA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PIVA = (String)value;
      }
    });
    setters.put("CF_STRANIERO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CF_STRANIERO = (String)value;
      }
    });
    setters.put("NOME", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        NOME = (String)value;
      }
    });
    setters.put("COGNOME", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        COGNOME = (String)value;
      }
    });
    setters.put("RAGIONE_SOCIALE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RAGIONE_SOCIALE = (String)value;
      }
    });
    setters.put("DATA_INIZIO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATA_INIZIO = (String)value;
      }
    });
    setters.put("DATA_FINE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATA_FINE = (String)value;
      }
    });
    setters.put("CODICE_COMUNE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CODICE_COMUNE = (String)value;
      }
    });
    setters.put("N_ID_CAUSALE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        N_ID_CAUSALE = (java.math.BigDecimal)value;
      }
    });
    setters.put("COD_ESITO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        COD_ESITO = (String)value;
      }
    });
    setters.put("AMMISSIBILE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        AMMISSIBILE = (String)value;
      }
    });
    setters.put("TIPO_FORNITURA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TIPO_FORNITURA = (String)value;
      }
    });
  }
  public prt_rcugas_rcugas_temp_va1() {
    init0();
  }
  private java.math.BigDecimal N_ID_PRATICA;
  public java.math.BigDecimal get_N_ID_PRATICA() {
    return N_ID_PRATICA;
  }
  public void set_N_ID_PRATICA(java.math.BigDecimal N_ID_PRATICA) {
    this.N_ID_PRATICA = N_ID_PRATICA;
  }
  public prt_rcugas_rcugas_temp_va1 with_N_ID_PRATICA(java.math.BigDecimal N_ID_PRATICA) {
    this.N_ID_PRATICA = N_ID_PRATICA;
    return this;
  }
  private String PROTOCOLLO;
  public String get_PROTOCOLLO() {
    return PROTOCOLLO;
  }
  public void set_PROTOCOLLO(String PROTOCOLLO) {
    this.PROTOCOLLO = PROTOCOLLO;
  }
  public prt_rcugas_rcugas_temp_va1 with_PROTOCOLLO(String PROTOCOLLO) {
    this.PROTOCOLLO = PROTOCOLLO;
    return this;
  }
  private String DATA_APERTURA;
  public String get_DATA_APERTURA() {
    return DATA_APERTURA;
  }
  public void set_DATA_APERTURA(String DATA_APERTURA) {
    this.DATA_APERTURA = DATA_APERTURA;
  }
  public prt_rcugas_rcugas_temp_va1 with_DATA_APERTURA(String DATA_APERTURA) {
    this.DATA_APERTURA = DATA_APERTURA;
    return this;
  }
  private java.math.BigDecimal N_ID_UTENTE;
  public java.math.BigDecimal get_N_ID_UTENTE() {
    return N_ID_UTENTE;
  }
  public void set_N_ID_UTENTE(java.math.BigDecimal N_ID_UTENTE) {
    this.N_ID_UTENTE = N_ID_UTENTE;
  }
  public prt_rcugas_rcugas_temp_va1 with_N_ID_UTENTE(java.math.BigDecimal N_ID_UTENTE) {
    this.N_ID_UTENTE = N_ID_UTENTE;
    return this;
  }
  private java.math.BigDecimal N_ID_VENDITORE;
  public java.math.BigDecimal get_N_ID_VENDITORE() {
    return N_ID_VENDITORE;
  }
  public void set_N_ID_VENDITORE(java.math.BigDecimal N_ID_VENDITORE) {
    this.N_ID_VENDITORE = N_ID_VENDITORE;
  }
  public prt_rcugas_rcugas_temp_va1 with_N_ID_VENDITORE(java.math.BigDecimal N_ID_VENDITORE) {
    this.N_ID_VENDITORE = N_ID_VENDITORE;
    return this;
  }
  private String COD_PRESTAZIONE;
  public String get_COD_PRESTAZIONE() {
    return COD_PRESTAZIONE;
  }
  public void set_COD_PRESTAZIONE(String COD_PRESTAZIONE) {
    this.COD_PRESTAZIONE = COD_PRESTAZIONE;
  }
  public prt_rcugas_rcugas_temp_va1 with_COD_PRESTAZIONE(String COD_PRESTAZIONE) {
    this.COD_PRESTAZIONE = COD_PRESTAZIONE;
    return this;
  }
  private String DATA_ESECUZIONE;
  public String get_DATA_ESECUZIONE() {
    return DATA_ESECUZIONE;
  }
  public void set_DATA_ESECUZIONE(String DATA_ESECUZIONE) {
    this.DATA_ESECUZIONE = DATA_ESECUZIONE;
  }
  public prt_rcugas_rcugas_temp_va1 with_DATA_ESECUZIONE(String DATA_ESECUZIONE) {
    this.DATA_ESECUZIONE = DATA_ESECUZIONE;
    return this;
  }
  private String PIVA_UDD;
  public String get_PIVA_UDD() {
    return PIVA_UDD;
  }
  public void set_PIVA_UDD(String PIVA_UDD) {
    this.PIVA_UDD = PIVA_UDD;
  }
  public prt_rcugas_rcugas_temp_va1 with_PIVA_UDD(String PIVA_UDD) {
    this.PIVA_UDD = PIVA_UDD;
    return this;
  }
  private String COD_PDR;
  public String get_COD_PDR() {
    return COD_PDR;
  }
  public void set_COD_PDR(String COD_PDR) {
    this.COD_PDR = COD_PDR;
  }
  public prt_rcugas_rcugas_temp_va1 with_COD_PDR(String COD_PDR) {
    this.COD_PDR = COD_PDR;
    return this;
  }
  private String COD_REMI;
  public String get_COD_REMI() {
    return COD_REMI;
  }
  public void set_COD_REMI(String COD_REMI) {
    this.COD_REMI = COD_REMI;
  }
  public prt_rcugas_rcugas_temp_va1 with_COD_REMI(String COD_REMI) {
    this.COD_REMI = COD_REMI;
    return this;
  }
  private String TIPO_PDR;
  public String get_TIPO_PDR() {
    return TIPO_PDR;
  }
  public void set_TIPO_PDR(String TIPO_PDR) {
    this.TIPO_PDR = TIPO_PDR;
  }
  public prt_rcugas_rcugas_temp_va1 with_TIPO_PDR(String TIPO_PDR) {
    this.TIPO_PDR = TIPO_PDR;
    return this;
  }
  private String COD_PROF_PREL_STANDARD;
  public String get_COD_PROF_PREL_STANDARD() {
    return COD_PROF_PREL_STANDARD;
  }
  public void set_COD_PROF_PREL_STANDARD(String COD_PROF_PREL_STANDARD) {
    this.COD_PROF_PREL_STANDARD = COD_PROF_PREL_STANDARD;
  }
  public prt_rcugas_rcugas_temp_va1 with_COD_PROF_PREL_STANDARD(String COD_PROF_PREL_STANDARD) {
    this.COD_PROF_PREL_STANDARD = COD_PROF_PREL_STANDARD;
    return this;
  }
  private String PRELIEVO_ANNUO_PREV;
  public String get_PRELIEVO_ANNUO_PREV() {
    return PRELIEVO_ANNUO_PREV;
  }
  public void set_PRELIEVO_ANNUO_PREV(String PRELIEVO_ANNUO_PREV) {
    this.PRELIEVO_ANNUO_PREV = PRELIEVO_ANNUO_PREV;
  }
  public prt_rcugas_rcugas_temp_va1 with_PRELIEVO_ANNUO_PREV(String PRELIEVO_ANNUO_PREV) {
    this.PRELIEVO_ANNUO_PREV = PRELIEVO_ANNUO_PREV;
    return this;
  }
  private String CF;
  public String get_CF() {
    return CF;
  }
  public void set_CF(String CF) {
    this.CF = CF;
  }
  public prt_rcugas_rcugas_temp_va1 with_CF(String CF) {
    this.CF = CF;
    return this;
  }
  private String PIVA;
  public String get_PIVA() {
    return PIVA;
  }
  public void set_PIVA(String PIVA) {
    this.PIVA = PIVA;
  }
  public prt_rcugas_rcugas_temp_va1 with_PIVA(String PIVA) {
    this.PIVA = PIVA;
    return this;
  }
  private String CF_STRANIERO;
  public String get_CF_STRANIERO() {
    return CF_STRANIERO;
  }
  public void set_CF_STRANIERO(String CF_STRANIERO) {
    this.CF_STRANIERO = CF_STRANIERO;
  }
  public prt_rcugas_rcugas_temp_va1 with_CF_STRANIERO(String CF_STRANIERO) {
    this.CF_STRANIERO = CF_STRANIERO;
    return this;
  }
  private String NOME;
  public String get_NOME() {
    return NOME;
  }
  public void set_NOME(String NOME) {
    this.NOME = NOME;
  }
  public prt_rcugas_rcugas_temp_va1 with_NOME(String NOME) {
    this.NOME = NOME;
    return this;
  }
  private String COGNOME;
  public String get_COGNOME() {
    return COGNOME;
  }
  public void set_COGNOME(String COGNOME) {
    this.COGNOME = COGNOME;
  }
  public prt_rcugas_rcugas_temp_va1 with_COGNOME(String COGNOME) {
    this.COGNOME = COGNOME;
    return this;
  }
  private String RAGIONE_SOCIALE;
  public String get_RAGIONE_SOCIALE() {
    return RAGIONE_SOCIALE;
  }
  public void set_RAGIONE_SOCIALE(String RAGIONE_SOCIALE) {
    this.RAGIONE_SOCIALE = RAGIONE_SOCIALE;
  }
  public prt_rcugas_rcugas_temp_va1 with_RAGIONE_SOCIALE(String RAGIONE_SOCIALE) {
    this.RAGIONE_SOCIALE = RAGIONE_SOCIALE;
    return this;
  }
  private String DATA_INIZIO;
  public String get_DATA_INIZIO() {
    return DATA_INIZIO;
  }
  public void set_DATA_INIZIO(String DATA_INIZIO) {
    this.DATA_INIZIO = DATA_INIZIO;
  }
  public prt_rcugas_rcugas_temp_va1 with_DATA_INIZIO(String DATA_INIZIO) {
    this.DATA_INIZIO = DATA_INIZIO;
    return this;
  }
  private String DATA_FINE;
  public String get_DATA_FINE() {
    return DATA_FINE;
  }
  public void set_DATA_FINE(String DATA_FINE) {
    this.DATA_FINE = DATA_FINE;
  }
  public prt_rcugas_rcugas_temp_va1 with_DATA_FINE(String DATA_FINE) {
    this.DATA_FINE = DATA_FINE;
    return this;
  }
  private String CODICE_COMUNE;
  public String get_CODICE_COMUNE() {
    return CODICE_COMUNE;
  }
  public void set_CODICE_COMUNE(String CODICE_COMUNE) {
    this.CODICE_COMUNE = CODICE_COMUNE;
  }
  public prt_rcugas_rcugas_temp_va1 with_CODICE_COMUNE(String CODICE_COMUNE) {
    this.CODICE_COMUNE = CODICE_COMUNE;
    return this;
  }
  private java.math.BigDecimal N_ID_CAUSALE;
  public java.math.BigDecimal get_N_ID_CAUSALE() {
    return N_ID_CAUSALE;
  }
  public void set_N_ID_CAUSALE(java.math.BigDecimal N_ID_CAUSALE) {
    this.N_ID_CAUSALE = N_ID_CAUSALE;
  }
  public prt_rcugas_rcugas_temp_va1 with_N_ID_CAUSALE(java.math.BigDecimal N_ID_CAUSALE) {
    this.N_ID_CAUSALE = N_ID_CAUSALE;
    return this;
  }
  private String COD_ESITO;
  public String get_COD_ESITO() {
    return COD_ESITO;
  }
  public void set_COD_ESITO(String COD_ESITO) {
    this.COD_ESITO = COD_ESITO;
  }
  public prt_rcugas_rcugas_temp_va1 with_COD_ESITO(String COD_ESITO) {
    this.COD_ESITO = COD_ESITO;
    return this;
  }
  private String AMMISSIBILE;
  public String get_AMMISSIBILE() {
    return AMMISSIBILE;
  }
  public void set_AMMISSIBILE(String AMMISSIBILE) {
    this.AMMISSIBILE = AMMISSIBILE;
  }
  public prt_rcugas_rcugas_temp_va1 with_AMMISSIBILE(String AMMISSIBILE) {
    this.AMMISSIBILE = AMMISSIBILE;
    return this;
  }
  private String TIPO_FORNITURA;
  public String get_TIPO_FORNITURA() {
    return TIPO_FORNITURA;
  }
  public void set_TIPO_FORNITURA(String TIPO_FORNITURA) {
    this.TIPO_FORNITURA = TIPO_FORNITURA;
  }
  public prt_rcugas_rcugas_temp_va1 with_TIPO_FORNITURA(String TIPO_FORNITURA) {
    this.TIPO_FORNITURA = TIPO_FORNITURA;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof prt_rcugas_rcugas_temp_va1)) {
      return false;
    }
    prt_rcugas_rcugas_temp_va1 that = (prt_rcugas_rcugas_temp_va1) o;
    boolean equal = true;
    equal = equal && (this.N_ID_PRATICA == null ? that.N_ID_PRATICA == null : this.N_ID_PRATICA.equals(that.N_ID_PRATICA));
    equal = equal && (this.PROTOCOLLO == null ? that.PROTOCOLLO == null : this.PROTOCOLLO.equals(that.PROTOCOLLO));
    equal = equal && (this.DATA_APERTURA == null ? that.DATA_APERTURA == null : this.DATA_APERTURA.equals(that.DATA_APERTURA));
    equal = equal && (this.N_ID_UTENTE == null ? that.N_ID_UTENTE == null : this.N_ID_UTENTE.equals(that.N_ID_UTENTE));
    equal = equal && (this.N_ID_VENDITORE == null ? that.N_ID_VENDITORE == null : this.N_ID_VENDITORE.equals(that.N_ID_VENDITORE));
    equal = equal && (this.COD_PRESTAZIONE == null ? that.COD_PRESTAZIONE == null : this.COD_PRESTAZIONE.equals(that.COD_PRESTAZIONE));
    equal = equal && (this.DATA_ESECUZIONE == null ? that.DATA_ESECUZIONE == null : this.DATA_ESECUZIONE.equals(that.DATA_ESECUZIONE));
    equal = equal && (this.PIVA_UDD == null ? that.PIVA_UDD == null : this.PIVA_UDD.equals(that.PIVA_UDD));
    equal = equal && (this.COD_PDR == null ? that.COD_PDR == null : this.COD_PDR.equals(that.COD_PDR));
    equal = equal && (this.COD_REMI == null ? that.COD_REMI == null : this.COD_REMI.equals(that.COD_REMI));
    equal = equal && (this.TIPO_PDR == null ? that.TIPO_PDR == null : this.TIPO_PDR.equals(that.TIPO_PDR));
    equal = equal && (this.COD_PROF_PREL_STANDARD == null ? that.COD_PROF_PREL_STANDARD == null : this.COD_PROF_PREL_STANDARD.equals(that.COD_PROF_PREL_STANDARD));
    equal = equal && (this.PRELIEVO_ANNUO_PREV == null ? that.PRELIEVO_ANNUO_PREV == null : this.PRELIEVO_ANNUO_PREV.equals(that.PRELIEVO_ANNUO_PREV));
    equal = equal && (this.CF == null ? that.CF == null : this.CF.equals(that.CF));
    equal = equal && (this.PIVA == null ? that.PIVA == null : this.PIVA.equals(that.PIVA));
    equal = equal && (this.CF_STRANIERO == null ? that.CF_STRANIERO == null : this.CF_STRANIERO.equals(that.CF_STRANIERO));
    equal = equal && (this.NOME == null ? that.NOME == null : this.NOME.equals(that.NOME));
    equal = equal && (this.COGNOME == null ? that.COGNOME == null : this.COGNOME.equals(that.COGNOME));
    equal = equal && (this.RAGIONE_SOCIALE == null ? that.RAGIONE_SOCIALE == null : this.RAGIONE_SOCIALE.equals(that.RAGIONE_SOCIALE));
    equal = equal && (this.DATA_INIZIO == null ? that.DATA_INIZIO == null : this.DATA_INIZIO.equals(that.DATA_INIZIO));
    equal = equal && (this.DATA_FINE == null ? that.DATA_FINE == null : this.DATA_FINE.equals(that.DATA_FINE));
    equal = equal && (this.CODICE_COMUNE == null ? that.CODICE_COMUNE == null : this.CODICE_COMUNE.equals(that.CODICE_COMUNE));
    equal = equal && (this.N_ID_CAUSALE == null ? that.N_ID_CAUSALE == null : this.N_ID_CAUSALE.equals(that.N_ID_CAUSALE));
    equal = equal && (this.COD_ESITO == null ? that.COD_ESITO == null : this.COD_ESITO.equals(that.COD_ESITO));
    equal = equal && (this.AMMISSIBILE == null ? that.AMMISSIBILE == null : this.AMMISSIBILE.equals(that.AMMISSIBILE));
    equal = equal && (this.TIPO_FORNITURA == null ? that.TIPO_FORNITURA == null : this.TIPO_FORNITURA.equals(that.TIPO_FORNITURA));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof prt_rcugas_rcugas_temp_va1)) {
      return false;
    }
    prt_rcugas_rcugas_temp_va1 that = (prt_rcugas_rcugas_temp_va1) o;
    boolean equal = true;
    equal = equal && (this.N_ID_PRATICA == null ? that.N_ID_PRATICA == null : this.N_ID_PRATICA.equals(that.N_ID_PRATICA));
    equal = equal && (this.PROTOCOLLO == null ? that.PROTOCOLLO == null : this.PROTOCOLLO.equals(that.PROTOCOLLO));
    equal = equal && (this.DATA_APERTURA == null ? that.DATA_APERTURA == null : this.DATA_APERTURA.equals(that.DATA_APERTURA));
    equal = equal && (this.N_ID_UTENTE == null ? that.N_ID_UTENTE == null : this.N_ID_UTENTE.equals(that.N_ID_UTENTE));
    equal = equal && (this.N_ID_VENDITORE == null ? that.N_ID_VENDITORE == null : this.N_ID_VENDITORE.equals(that.N_ID_VENDITORE));
    equal = equal && (this.COD_PRESTAZIONE == null ? that.COD_PRESTAZIONE == null : this.COD_PRESTAZIONE.equals(that.COD_PRESTAZIONE));
    equal = equal && (this.DATA_ESECUZIONE == null ? that.DATA_ESECUZIONE == null : this.DATA_ESECUZIONE.equals(that.DATA_ESECUZIONE));
    equal = equal && (this.PIVA_UDD == null ? that.PIVA_UDD == null : this.PIVA_UDD.equals(that.PIVA_UDD));
    equal = equal && (this.COD_PDR == null ? that.COD_PDR == null : this.COD_PDR.equals(that.COD_PDR));
    equal = equal && (this.COD_REMI == null ? that.COD_REMI == null : this.COD_REMI.equals(that.COD_REMI));
    equal = equal && (this.TIPO_PDR == null ? that.TIPO_PDR == null : this.TIPO_PDR.equals(that.TIPO_PDR));
    equal = equal && (this.COD_PROF_PREL_STANDARD == null ? that.COD_PROF_PREL_STANDARD == null : this.COD_PROF_PREL_STANDARD.equals(that.COD_PROF_PREL_STANDARD));
    equal = equal && (this.PRELIEVO_ANNUO_PREV == null ? that.PRELIEVO_ANNUO_PREV == null : this.PRELIEVO_ANNUO_PREV.equals(that.PRELIEVO_ANNUO_PREV));
    equal = equal && (this.CF == null ? that.CF == null : this.CF.equals(that.CF));
    equal = equal && (this.PIVA == null ? that.PIVA == null : this.PIVA.equals(that.PIVA));
    equal = equal && (this.CF_STRANIERO == null ? that.CF_STRANIERO == null : this.CF_STRANIERO.equals(that.CF_STRANIERO));
    equal = equal && (this.NOME == null ? that.NOME == null : this.NOME.equals(that.NOME));
    equal = equal && (this.COGNOME == null ? that.COGNOME == null : this.COGNOME.equals(that.COGNOME));
    equal = equal && (this.RAGIONE_SOCIALE == null ? that.RAGIONE_SOCIALE == null : this.RAGIONE_SOCIALE.equals(that.RAGIONE_SOCIALE));
    equal = equal && (this.DATA_INIZIO == null ? that.DATA_INIZIO == null : this.DATA_INIZIO.equals(that.DATA_INIZIO));
    equal = equal && (this.DATA_FINE == null ? that.DATA_FINE == null : this.DATA_FINE.equals(that.DATA_FINE));
    equal = equal && (this.CODICE_COMUNE == null ? that.CODICE_COMUNE == null : this.CODICE_COMUNE.equals(that.CODICE_COMUNE));
    equal = equal && (this.N_ID_CAUSALE == null ? that.N_ID_CAUSALE == null : this.N_ID_CAUSALE.equals(that.N_ID_CAUSALE));
    equal = equal && (this.COD_ESITO == null ? that.COD_ESITO == null : this.COD_ESITO.equals(that.COD_ESITO));
    equal = equal && (this.AMMISSIBILE == null ? that.AMMISSIBILE == null : this.AMMISSIBILE.equals(that.AMMISSIBILE));
    equal = equal && (this.TIPO_FORNITURA == null ? that.TIPO_FORNITURA == null : this.TIPO_FORNITURA.equals(that.TIPO_FORNITURA));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.N_ID_PRATICA = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.PROTOCOLLO = JdbcWritableBridge.readString(2, __dbResults);
    this.DATA_APERTURA = JdbcWritableBridge.readString(3, __dbResults);
    this.N_ID_UTENTE = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.N_ID_VENDITORE = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.COD_PRESTAZIONE = JdbcWritableBridge.readString(6, __dbResults);
    this.DATA_ESECUZIONE = JdbcWritableBridge.readString(7, __dbResults);
    this.PIVA_UDD = JdbcWritableBridge.readString(8, __dbResults);
    this.COD_PDR = JdbcWritableBridge.readString(9, __dbResults);
    this.COD_REMI = JdbcWritableBridge.readString(10, __dbResults);
    this.TIPO_PDR = JdbcWritableBridge.readString(11, __dbResults);
    this.COD_PROF_PREL_STANDARD = JdbcWritableBridge.readString(12, __dbResults);
    this.PRELIEVO_ANNUO_PREV = JdbcWritableBridge.readString(13, __dbResults);
    this.CF = JdbcWritableBridge.readString(14, __dbResults);
    this.PIVA = JdbcWritableBridge.readString(15, __dbResults);
    this.CF_STRANIERO = JdbcWritableBridge.readString(16, __dbResults);
    this.NOME = JdbcWritableBridge.readString(17, __dbResults);
    this.COGNOME = JdbcWritableBridge.readString(18, __dbResults);
    this.RAGIONE_SOCIALE = JdbcWritableBridge.readString(19, __dbResults);
    this.DATA_INIZIO = JdbcWritableBridge.readString(20, __dbResults);
    this.DATA_FINE = JdbcWritableBridge.readString(21, __dbResults);
    this.CODICE_COMUNE = JdbcWritableBridge.readString(22, __dbResults);
    this.N_ID_CAUSALE = JdbcWritableBridge.readBigDecimal(23, __dbResults);
    this.COD_ESITO = JdbcWritableBridge.readString(24, __dbResults);
    this.AMMISSIBILE = JdbcWritableBridge.readString(25, __dbResults);
    this.TIPO_FORNITURA = JdbcWritableBridge.readString(26, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.N_ID_PRATICA = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.PROTOCOLLO = JdbcWritableBridge.readString(2, __dbResults);
    this.DATA_APERTURA = JdbcWritableBridge.readString(3, __dbResults);
    this.N_ID_UTENTE = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.N_ID_VENDITORE = JdbcWritableBridge.readBigDecimal(5, __dbResults);
    this.COD_PRESTAZIONE = JdbcWritableBridge.readString(6, __dbResults);
    this.DATA_ESECUZIONE = JdbcWritableBridge.readString(7, __dbResults);
    this.PIVA_UDD = JdbcWritableBridge.readString(8, __dbResults);
    this.COD_PDR = JdbcWritableBridge.readString(9, __dbResults);
    this.COD_REMI = JdbcWritableBridge.readString(10, __dbResults);
    this.TIPO_PDR = JdbcWritableBridge.readString(11, __dbResults);
    this.COD_PROF_PREL_STANDARD = JdbcWritableBridge.readString(12, __dbResults);
    this.PRELIEVO_ANNUO_PREV = JdbcWritableBridge.readString(13, __dbResults);
    this.CF = JdbcWritableBridge.readString(14, __dbResults);
    this.PIVA = JdbcWritableBridge.readString(15, __dbResults);
    this.CF_STRANIERO = JdbcWritableBridge.readString(16, __dbResults);
    this.NOME = JdbcWritableBridge.readString(17, __dbResults);
    this.COGNOME = JdbcWritableBridge.readString(18, __dbResults);
    this.RAGIONE_SOCIALE = JdbcWritableBridge.readString(19, __dbResults);
    this.DATA_INIZIO = JdbcWritableBridge.readString(20, __dbResults);
    this.DATA_FINE = JdbcWritableBridge.readString(21, __dbResults);
    this.CODICE_COMUNE = JdbcWritableBridge.readString(22, __dbResults);
    this.N_ID_CAUSALE = JdbcWritableBridge.readBigDecimal(23, __dbResults);
    this.COD_ESITO = JdbcWritableBridge.readString(24, __dbResults);
    this.AMMISSIBILE = JdbcWritableBridge.readString(25, __dbResults);
    this.TIPO_FORNITURA = JdbcWritableBridge.readString(26, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(N_ID_PRATICA, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PROTOCOLLO, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATA_APERTURA, 3 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UTENTE, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_VENDITORE, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(COD_PRESTAZIONE, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATA_ESECUZIONE, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PIVA_UDD, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COD_PDR, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COD_REMI, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TIPO_PDR, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COD_PROF_PREL_STANDARD, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PRELIEVO_ANNUO_PREV, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CF, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PIVA, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CF_STRANIERO, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(NOME, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COGNOME, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(RAGIONE_SOCIALE, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATA_INIZIO, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATA_FINE, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CODICE_COMUNE, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CAUSALE, 23 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(COD_ESITO, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(AMMISSIBILE, 25 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TIPO_FORNITURA, 26 + __off, 12, __dbStmt);
    return 26;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(N_ID_PRATICA, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PROTOCOLLO, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATA_APERTURA, 3 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_UTENTE, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_VENDITORE, 5 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(COD_PRESTAZIONE, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATA_ESECUZIONE, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PIVA_UDD, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COD_PDR, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COD_REMI, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TIPO_PDR, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COD_PROF_PREL_STANDARD, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PRELIEVO_ANNUO_PREV, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CF, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PIVA, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CF_STRANIERO, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(NOME, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COGNOME, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(RAGIONE_SOCIALE, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATA_INIZIO, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DATA_FINE, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CODICE_COMUNE, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(N_ID_CAUSALE, 23 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(COD_ESITO, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(AMMISSIBILE, 25 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TIPO_FORNITURA, 26 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.N_ID_PRATICA = null;
    } else {
    this.N_ID_PRATICA = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PROTOCOLLO = null;
    } else {
    this.PROTOCOLLO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATA_APERTURA = null;
    } else {
    this.DATA_APERTURA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_UTENTE = null;
    } else {
    this.N_ID_UTENTE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_VENDITORE = null;
    } else {
    this.N_ID_VENDITORE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.COD_PRESTAZIONE = null;
    } else {
    this.COD_PRESTAZIONE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATA_ESECUZIONE = null;
    } else {
    this.DATA_ESECUZIONE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PIVA_UDD = null;
    } else {
    this.PIVA_UDD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.COD_PDR = null;
    } else {
    this.COD_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.COD_REMI = null;
    } else {
    this.COD_REMI = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TIPO_PDR = null;
    } else {
    this.TIPO_PDR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.COD_PROF_PREL_STANDARD = null;
    } else {
    this.COD_PROF_PREL_STANDARD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PRELIEVO_ANNUO_PREV = null;
    } else {
    this.PRELIEVO_ANNUO_PREV = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CF = null;
    } else {
    this.CF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PIVA = null;
    } else {
    this.PIVA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CF_STRANIERO = null;
    } else {
    this.CF_STRANIERO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.NOME = null;
    } else {
    this.NOME = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.COGNOME = null;
    } else {
    this.COGNOME = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RAGIONE_SOCIALE = null;
    } else {
    this.RAGIONE_SOCIALE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATA_INIZIO = null;
    } else {
    this.DATA_INIZIO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATA_FINE = null;
    } else {
    this.DATA_FINE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CODICE_COMUNE = null;
    } else {
    this.CODICE_COMUNE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.N_ID_CAUSALE = null;
    } else {
    this.N_ID_CAUSALE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.COD_ESITO = null;
    } else {
    this.COD_ESITO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.AMMISSIBILE = null;
    } else {
    this.AMMISSIBILE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TIPO_FORNITURA = null;
    } else {
    this.TIPO_FORNITURA = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_PRATICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PRATICA, __dataOut);
    }
    if (null == this.PROTOCOLLO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROTOCOLLO);
    }
    if (null == this.DATA_APERTURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_APERTURA);
    }
    if (null == this.N_ID_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UTENTE, __dataOut);
    }
    if (null == this.N_ID_VENDITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_VENDITORE, __dataOut);
    }
    if (null == this.COD_PRESTAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_PRESTAZIONE);
    }
    if (null == this.DATA_ESECUZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_ESECUZIONE);
    }
    if (null == this.PIVA_UDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PIVA_UDD);
    }
    if (null == this.COD_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_PDR);
    }
    if (null == this.COD_REMI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_REMI);
    }
    if (null == this.TIPO_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIPO_PDR);
    }
    if (null == this.COD_PROF_PREL_STANDARD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_PROF_PREL_STANDARD);
    }
    if (null == this.PRELIEVO_ANNUO_PREV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PRELIEVO_ANNUO_PREV);
    }
    if (null == this.CF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CF);
    }
    if (null == this.PIVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PIVA);
    }
    if (null == this.CF_STRANIERO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CF_STRANIERO);
    }
    if (null == this.NOME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, NOME);
    }
    if (null == this.COGNOME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COGNOME);
    }
    if (null == this.RAGIONE_SOCIALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RAGIONE_SOCIALE);
    }
    if (null == this.DATA_INIZIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_INIZIO);
    }
    if (null == this.DATA_FINE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_FINE);
    }
    if (null == this.CODICE_COMUNE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CODICE_COMUNE);
    }
    if (null == this.N_ID_CAUSALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CAUSALE, __dataOut);
    }
    if (null == this.COD_ESITO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_ESITO);
    }
    if (null == this.AMMISSIBILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, AMMISSIBILE);
    }
    if (null == this.TIPO_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIPO_FORNITURA);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.N_ID_PRATICA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_PRATICA, __dataOut);
    }
    if (null == this.PROTOCOLLO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PROTOCOLLO);
    }
    if (null == this.DATA_APERTURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_APERTURA);
    }
    if (null == this.N_ID_UTENTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_UTENTE, __dataOut);
    }
    if (null == this.N_ID_VENDITORE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_VENDITORE, __dataOut);
    }
    if (null == this.COD_PRESTAZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_PRESTAZIONE);
    }
    if (null == this.DATA_ESECUZIONE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_ESECUZIONE);
    }
    if (null == this.PIVA_UDD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PIVA_UDD);
    }
    if (null == this.COD_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_PDR);
    }
    if (null == this.COD_REMI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_REMI);
    }
    if (null == this.TIPO_PDR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIPO_PDR);
    }
    if (null == this.COD_PROF_PREL_STANDARD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_PROF_PREL_STANDARD);
    }
    if (null == this.PRELIEVO_ANNUO_PREV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PRELIEVO_ANNUO_PREV);
    }
    if (null == this.CF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CF);
    }
    if (null == this.PIVA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PIVA);
    }
    if (null == this.CF_STRANIERO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CF_STRANIERO);
    }
    if (null == this.NOME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, NOME);
    }
    if (null == this.COGNOME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COGNOME);
    }
    if (null == this.RAGIONE_SOCIALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RAGIONE_SOCIALE);
    }
    if (null == this.DATA_INIZIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_INIZIO);
    }
    if (null == this.DATA_FINE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_FINE);
    }
    if (null == this.CODICE_COMUNE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CODICE_COMUNE);
    }
    if (null == this.N_ID_CAUSALE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.N_ID_CAUSALE, __dataOut);
    }
    if (null == this.COD_ESITO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COD_ESITO);
    }
    if (null == this.AMMISSIBILE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, AMMISSIBILE);
    }
    if (null == this.TIPO_FORNITURA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIPO_FORNITURA);
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
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PRATICA==null?"":N_ID_PRATICA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PROTOCOLLO==null?"":PROTOCOLLO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATA_APERTURA==null?"":DATA_APERTURA, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UTENTE==null?"":N_ID_UTENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_VENDITORE==null?"":N_ID_VENDITORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_PRESTAZIONE==null?"":COD_PRESTAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATA_ESECUZIONE==null?"":DATA_ESECUZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PIVA_UDD==null?"":PIVA_UDD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_PDR==null?"":COD_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_REMI==null?"":COD_REMI, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TIPO_PDR==null?"":TIPO_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_PROF_PREL_STANDARD==null?"":COD_PROF_PREL_STANDARD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PRELIEVO_ANNUO_PREV==null?"":PRELIEVO_ANNUO_PREV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(CF==null?"":CF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PIVA==null?"":PIVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(CF_STRANIERO==null?"":CF_STRANIERO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(NOME==null?"":NOME, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COGNOME==null?"":COGNOME, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(RAGIONE_SOCIALE==null?"":RAGIONE_SOCIALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATA_INIZIO==null?"":DATA_INIZIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATA_FINE==null?"":DATA_FINE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(CODICE_COMUNE==null?"":CODICE_COMUNE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CAUSALE==null?"":N_ID_CAUSALE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_ESITO==null?"":COD_ESITO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(AMMISSIBILE==null?"":AMMISSIBILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TIPO_FORNITURA==null?"":TIPO_FORNITURA, " ", delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_PRATICA==null?"":N_ID_PRATICA.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PROTOCOLLO==null?"":PROTOCOLLO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATA_APERTURA==null?"":DATA_APERTURA, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_UTENTE==null?"":N_ID_UTENTE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_VENDITORE==null?"":N_ID_VENDITORE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_PRESTAZIONE==null?"":COD_PRESTAZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATA_ESECUZIONE==null?"":DATA_ESECUZIONE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PIVA_UDD==null?"":PIVA_UDD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_PDR==null?"":COD_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_REMI==null?"":COD_REMI, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TIPO_PDR==null?"":TIPO_PDR, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_PROF_PREL_STANDARD==null?"":COD_PROF_PREL_STANDARD, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PRELIEVO_ANNUO_PREV==null?"":PRELIEVO_ANNUO_PREV, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(CF==null?"":CF, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(PIVA==null?"":PIVA, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(CF_STRANIERO==null?"":CF_STRANIERO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(NOME==null?"":NOME, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COGNOME==null?"":COGNOME, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(RAGIONE_SOCIALE==null?"":RAGIONE_SOCIALE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATA_INIZIO==null?"":DATA_INIZIO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(DATA_FINE==null?"":DATA_FINE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(CODICE_COMUNE==null?"":CODICE_COMUNE, " ", delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(N_ID_CAUSALE==null?"":N_ID_CAUSALE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(COD_ESITO==null?"":COD_ESITO, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(AMMISSIBILE==null?"":AMMISSIBILE, " ", delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, replacing delimiters \n,\r,\01 with ' ' from strings
    __sb.append(FieldFormatter.hiveStringReplaceDelims(TIPO_FORNITURA==null?"":TIPO_FORNITURA, " ", delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PRATICA = null; } else {
      this.N_ID_PRATICA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROTOCOLLO = null; } else {
      this.PROTOCOLLO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_APERTURA = null; } else {
      this.DATA_APERTURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UTENTE = null; } else {
      this.N_ID_UTENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_VENDITORE = null; } else {
      this.N_ID_VENDITORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COD_PRESTAZIONE = null; } else {
      this.COD_PRESTAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_ESECUZIONE = null; } else {
      this.DATA_ESECUZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PIVA_UDD = null; } else {
      this.PIVA_UDD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COD_PDR = null; } else {
      this.COD_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COD_REMI = null; } else {
      this.COD_REMI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIPO_PDR = null; } else {
      this.TIPO_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COD_PROF_PREL_STANDARD = null; } else {
      this.COD_PROF_PREL_STANDARD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PRELIEVO_ANNUO_PREV = null; } else {
      this.PRELIEVO_ANNUO_PREV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CF = null; } else {
      this.CF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PIVA = null; } else {
      this.PIVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CF_STRANIERO = null; } else {
      this.CF_STRANIERO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.NOME = null; } else {
      this.NOME = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COGNOME = null; } else {
      this.COGNOME = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RAGIONE_SOCIALE = null; } else {
      this.RAGIONE_SOCIALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_INIZIO = null; } else {
      this.DATA_INIZIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_FINE = null; } else {
      this.DATA_FINE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CODICE_COMUNE = null; } else {
      this.CODICE_COMUNE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CAUSALE = null; } else {
      this.N_ID_CAUSALE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COD_ESITO = null; } else {
      this.COD_ESITO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.AMMISSIBILE = null; } else {
      this.AMMISSIBILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIPO_FORNITURA = null; } else {
      this.TIPO_FORNITURA = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_PRATICA = null; } else {
      this.N_ID_PRATICA = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PROTOCOLLO = null; } else {
      this.PROTOCOLLO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_APERTURA = null; } else {
      this.DATA_APERTURA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_UTENTE = null; } else {
      this.N_ID_UTENTE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_VENDITORE = null; } else {
      this.N_ID_VENDITORE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COD_PRESTAZIONE = null; } else {
      this.COD_PRESTAZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_ESECUZIONE = null; } else {
      this.DATA_ESECUZIONE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PIVA_UDD = null; } else {
      this.PIVA_UDD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COD_PDR = null; } else {
      this.COD_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COD_REMI = null; } else {
      this.COD_REMI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIPO_PDR = null; } else {
      this.TIPO_PDR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COD_PROF_PREL_STANDARD = null; } else {
      this.COD_PROF_PREL_STANDARD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PRELIEVO_ANNUO_PREV = null; } else {
      this.PRELIEVO_ANNUO_PREV = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CF = null; } else {
      this.CF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PIVA = null; } else {
      this.PIVA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CF_STRANIERO = null; } else {
      this.CF_STRANIERO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.NOME = null; } else {
      this.NOME = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COGNOME = null; } else {
      this.COGNOME = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RAGIONE_SOCIALE = null; } else {
      this.RAGIONE_SOCIALE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_INIZIO = null; } else {
      this.DATA_INIZIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_FINE = null; } else {
      this.DATA_FINE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CODICE_COMUNE = null; } else {
      this.CODICE_COMUNE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.N_ID_CAUSALE = null; } else {
      this.N_ID_CAUSALE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COD_ESITO = null; } else {
      this.COD_ESITO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.AMMISSIBILE = null; } else {
      this.AMMISSIBILE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIPO_FORNITURA = null; } else {
      this.TIPO_FORNITURA = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    prt_rcugas_rcugas_temp_va1 o = (prt_rcugas_rcugas_temp_va1) super.clone();
    return o;
  }

  public void clone0(prt_rcugas_rcugas_temp_va1 o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("N_ID_PRATICA", this.N_ID_PRATICA);
    __sqoop$field_map.put("PROTOCOLLO", this.PROTOCOLLO);
    __sqoop$field_map.put("DATA_APERTURA", this.DATA_APERTURA);
    __sqoop$field_map.put("N_ID_UTENTE", this.N_ID_UTENTE);
    __sqoop$field_map.put("N_ID_VENDITORE", this.N_ID_VENDITORE);
    __sqoop$field_map.put("COD_PRESTAZIONE", this.COD_PRESTAZIONE);
    __sqoop$field_map.put("DATA_ESECUZIONE", this.DATA_ESECUZIONE);
    __sqoop$field_map.put("PIVA_UDD", this.PIVA_UDD);
    __sqoop$field_map.put("COD_PDR", this.COD_PDR);
    __sqoop$field_map.put("COD_REMI", this.COD_REMI);
    __sqoop$field_map.put("TIPO_PDR", this.TIPO_PDR);
    __sqoop$field_map.put("COD_PROF_PREL_STANDARD", this.COD_PROF_PREL_STANDARD);
    __sqoop$field_map.put("PRELIEVO_ANNUO_PREV", this.PRELIEVO_ANNUO_PREV);
    __sqoop$field_map.put("CF", this.CF);
    __sqoop$field_map.put("PIVA", this.PIVA);
    __sqoop$field_map.put("CF_STRANIERO", this.CF_STRANIERO);
    __sqoop$field_map.put("NOME", this.NOME);
    __sqoop$field_map.put("COGNOME", this.COGNOME);
    __sqoop$field_map.put("RAGIONE_SOCIALE", this.RAGIONE_SOCIALE);
    __sqoop$field_map.put("DATA_INIZIO", this.DATA_INIZIO);
    __sqoop$field_map.put("DATA_FINE", this.DATA_FINE);
    __sqoop$field_map.put("CODICE_COMUNE", this.CODICE_COMUNE);
    __sqoop$field_map.put("N_ID_CAUSALE", this.N_ID_CAUSALE);
    __sqoop$field_map.put("COD_ESITO", this.COD_ESITO);
    __sqoop$field_map.put("AMMISSIBILE", this.AMMISSIBILE);
    __sqoop$field_map.put("TIPO_FORNITURA", this.TIPO_FORNITURA);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("N_ID_PRATICA", this.N_ID_PRATICA);
    __sqoop$field_map.put("PROTOCOLLO", this.PROTOCOLLO);
    __sqoop$field_map.put("DATA_APERTURA", this.DATA_APERTURA);
    __sqoop$field_map.put("N_ID_UTENTE", this.N_ID_UTENTE);
    __sqoop$field_map.put("N_ID_VENDITORE", this.N_ID_VENDITORE);
    __sqoop$field_map.put("COD_PRESTAZIONE", this.COD_PRESTAZIONE);
    __sqoop$field_map.put("DATA_ESECUZIONE", this.DATA_ESECUZIONE);
    __sqoop$field_map.put("PIVA_UDD", this.PIVA_UDD);
    __sqoop$field_map.put("COD_PDR", this.COD_PDR);
    __sqoop$field_map.put("COD_REMI", this.COD_REMI);
    __sqoop$field_map.put("TIPO_PDR", this.TIPO_PDR);
    __sqoop$field_map.put("COD_PROF_PREL_STANDARD", this.COD_PROF_PREL_STANDARD);
    __sqoop$field_map.put("PRELIEVO_ANNUO_PREV", this.PRELIEVO_ANNUO_PREV);
    __sqoop$field_map.put("CF", this.CF);
    __sqoop$field_map.put("PIVA", this.PIVA);
    __sqoop$field_map.put("CF_STRANIERO", this.CF_STRANIERO);
    __sqoop$field_map.put("NOME", this.NOME);
    __sqoop$field_map.put("COGNOME", this.COGNOME);
    __sqoop$field_map.put("RAGIONE_SOCIALE", this.RAGIONE_SOCIALE);
    __sqoop$field_map.put("DATA_INIZIO", this.DATA_INIZIO);
    __sqoop$field_map.put("DATA_FINE", this.DATA_FINE);
    __sqoop$field_map.put("CODICE_COMUNE", this.CODICE_COMUNE);
    __sqoop$field_map.put("N_ID_CAUSALE", this.N_ID_CAUSALE);
    __sqoop$field_map.put("COD_ESITO", this.COD_ESITO);
    __sqoop$field_map.put("AMMISSIBILE", this.AMMISSIBILE);
    __sqoop$field_map.put("TIPO_FORNITURA", this.TIPO_FORNITURA);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
