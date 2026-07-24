package it.eng.au.ammissibilitaRendiconti.controller

import it.eng.au.ammissibilitaRendiconti.model.rules.IndennizziRule
import it.eng.au.ammissibilitaRendiconti.model.{AggregatoTotale, ReportMessage, ZipRzg1Metadata}
import it.eng.au.ammissibilitaRendiconti.utility.constants.Constants._
import it.eng.au.ammissibilitaRendiconti.utility.environment.Properties

/** Implementa una serie di funzioni adibite al controllo delle ammissibilità sui campi relativi agli indennizzi. */
object ValidateIndennizziController extends Serializable {
  /** Esegue sui file CSV i controlli delle regole di ammissibilità attive relative agli indennizzzi. */
  def validate(zipMeta: ZipRzg1Metadata, aggregatoTotale: Option[AggregatoTotale], indennizziRules: List[IndennizziRule]): (ZipRzg1Metadata, Option[AggregatoTotale]) = {
    /** Messaggio di ammissibilità positiva. */
    val okMessage = ReportMessage()
    /** Prima regola, se esiste e in ordine di priorità, che non viene rispettata dal file [[csv]]. */
    val errorRule = indennizziRules.find(rule => rule.isEnabled && !rule.condition(zipMeta, aggregatoTotale))
    /** Messaggio di errore da associare al file [[csv]]. */
    val message = if (errorRule.isDefined) errorRule.get.message else okMessage

    (zipMeta.copy(
      isAmmissibile = message.isAmmissibile,
      statusCode = message.statusCode,
      statusMessage = message.statusMessage
    ), aggregatoTotale)
  }

  /** Controlla che il campo [[field]] sia valorizzato come previsto (in questo caso si tratta del campo di indennizzo, quindi deve essere nel formato decimale con due cifre dopo la virgola). */
  def isOMValued(field: Option[String]): Boolean = field.isEmpty || field.get.matches("([0-9]{1,11})\\.([0-9]{2})")

  /** Controlla che il campo dell'indennizzo relativo alla regola 1 sia valorizzato come previsto. */
  def ruleValidateOM1: IndennizziRule = IndennizziRule(
    ruleName = "ruleValidateOM1",
    isEnabled = Properties.isRuleValidateOM1Enabled,
    condition = (zipMetadata, _) => isOMValued(zipMetadata.csv.get.om1_id),
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_010,
      statusMessage = MOTIVAZIONE_010_OM1
    )
  )

  /** Controlla che il campo dell'indennizzo relativo alla regola 2 sia valorizzato come previsto. */
  def ruleValidateOM2: IndennizziRule = IndennizziRule(
    ruleName = "ruleValidateOM2",
    isEnabled = Properties.isRuleValidateOM2Enabled,
    condition = (zipMetadata, _) => isOMValued(zipMetadata.csv.get.om2_id),
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_010,
      statusMessage = MOTIVAZIONE_010_OM2
    )
  )

  /** Controlla che il campo dell'indennizzo relativo alla regola 3 sia valorizzato come previsto. */
  def ruleValidateOM3: IndennizziRule = IndennizziRule(
    ruleName = "ruleValidateOM3",
    isEnabled = Properties.isRuleValidateOM3Enabled,
    condition = (zipMetadata, _) => isOMValued(zipMetadata.csv.get.om3_id),
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_010,
      statusMessage = MOTIVAZIONE_010_OM3
    )
  )

  /** Controlla che ci sia una corrispondenza tra l'id indennizzo presente nel CSV e la tabella di aggregato totale.
   * In altre parole, il campo id_indennizzo nel CSV deve essere uno tra gli id_indennizzo presenti nella tabella cig_aggregato_totale. */
  def ruleValidateIdIndennizzo: IndennizziRule = IndennizziRule(
    ruleName = "ruleValidateIdIndennizzo",
    isEnabled = Properties.isRuleValidateIdIndennizzoEnabled,
    condition = (zipMetadata, aggregatoTotale) => aggregatoTotale.nonEmpty,
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_011,
      statusMessage = MOTIVAZIONE_011
    )
  )

  /** Controlla che almeno uno tra i campi di indennizzo sia valorizzato. */
  def ruleCheckAtLeastOneOMIsValued: IndennizziRule = IndennizziRule(
    ruleName = "ruleCheckAtLeastOneOMIsValued",
    isEnabled = Properties.isRuleCheckAtLeastOneOMIsValuedEnabled,
    condition = (zipMetadata, _) => zipMetadata.csv.get.om1_id.nonEmpty || zipMetadata.csv.get.om2_id.nonEmpty || zipMetadata.csv.get.om3_id.nonEmpty,
    message = ReportMessage(
      isAmmissibile = false,
      statusCode = COD_010,
      statusMessage = MOTIVAZIONE_010
    )
  )
}