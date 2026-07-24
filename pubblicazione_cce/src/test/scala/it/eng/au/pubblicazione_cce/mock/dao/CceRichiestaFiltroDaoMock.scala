package it.eng.au.pubblicazione_cce.mock.dao

import it.eng.au.pubblicazione_cce.dao.cce.CceRichiestaFiltroDao
import it.eng.au.pubblicazione_cce.model.cce.CceRichiestaFiltroModel
import org.apache.spark.sql.Dataset

class CceRichiestaFiltroDaoMock(var ds: Dataset[CceRichiestaFiltroModel]) extends CceRichiestaFiltroDao {

  override def read(): Dataset[CceRichiestaFiltroModel] = ds
}
