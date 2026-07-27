package it.sferanet.au.model

import java.util.Date

case class Tds(isValid: Boolean,
               pdr: String,
               cat_uso: String,
               classe_prelievo: String,
               data_creazione: Date
              )
