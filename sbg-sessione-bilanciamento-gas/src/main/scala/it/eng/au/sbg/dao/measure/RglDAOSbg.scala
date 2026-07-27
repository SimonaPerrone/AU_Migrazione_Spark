package it.eng.au.sbg.dao.measure

import it.eng.au.aggiustamentoGas.dao.measure.RglDAO
import it.eng.au.aggiustamentoGas.model.measure.Flow

class RglDAOSbg extends RglDAO {
  override val filterFlow: Flow => Boolean = (f: Flow) => Set(1, 2, 3, 4, 5, 6, 7).contains(f.motivation.getOrElse(-1))
}
