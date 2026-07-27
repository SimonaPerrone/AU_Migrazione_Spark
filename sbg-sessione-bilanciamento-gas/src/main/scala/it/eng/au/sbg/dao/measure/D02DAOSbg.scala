package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.D02DAO
import it.eng.au.aggiustamentoGas.model.measure.Flow

class D02DAOSbg extends D02DAO {
  override val filterFlow: Flow => Boolean = (f: Flow) => Set('E', 'S', 'A').contains(f.readType.getOrElse('-'))
}