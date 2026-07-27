package it.eng.au.ccgPubblicazione.model.cdp

case class CaFinalCdp(
                       codice_pdr:String = "",
                       piva_distr:String = "",// TODO da aggiungerla nel calcolo
                       piva_udd:String = "",// TODO da aggiungerla nel calcolo
                       piva_udb:String = "",// TODO da aggiungerla nel calcolo
                       codice_remi:String = "",
                       cat_uso:String = "",
                       classe_prelievo:String = "",
                       zona_climatica:String = "",
                       id_reg_clim:String = "",
                       cod_prof_prel_std:String = "",
                       prelievo_annuo_prev:String = "",
                       trattamento:String = "",
                       anno_competenza:String = "2022",//serve per la data
                       tipo_trasmissione:String = "FIN",
                       session:String = "CCG",
                       executionid:String = ""
                     )
