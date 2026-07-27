package it.sferanet.au.model

case class CaParameter(
                        caMethods: CAMethods.Value,
                        codiceProfilo: String,
                        profStdLastRcu: String,
                        id_regClim: Int,
                        t_comune_istat_pdr: String
                      ) {

}

object CaParameter {
  def Empty: CaParameter = new CaParameter(CAMethods.NoSuchTypeConsume, "", "", 0, "")
}
