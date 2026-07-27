package it.sferanet.au.model.caFinal

case class TechInfoCa(
                       pres_tds: Boolean,
                       tipologia_uso: Boolean,
                       comp_termica: Boolean,
                       cat_uso_tds: String,
                       classe_prelievo_tds: Option[String],
                       cod_istat_last_rcu: String,
                       zona_climatica_lookup: Option[String],
                       ce_mean: Option[Double]
                     )
