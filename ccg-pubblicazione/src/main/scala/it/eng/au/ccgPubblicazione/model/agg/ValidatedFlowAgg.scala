package it.eng.au.ccgPubblicazione.model.agg

import java.sql.Timestamp

case class ValidatedFlowAgg(
                          pdr: String = null,
                          date: Timestamp = Timestamp.valueOf("2022-01-01"),
                          service: String = "",
                          iscorrected: Boolean = true,
                          localfile: String = "/mnt/isilon/piva11111111_piva000000000/2020/0101/piva11111111_piva000000000_file.AAOOlls._dxsx.zip"
                        )
