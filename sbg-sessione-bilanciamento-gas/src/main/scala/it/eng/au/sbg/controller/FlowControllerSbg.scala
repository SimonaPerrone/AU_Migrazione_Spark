package it.eng.au.sbg.controller

import it.eng.au.aggiustamentoGas.controller.FlowController
import it.eng.au.aggiustamentoGas.dao.measure.{MeasureDAO, RmlDAO}
import it.eng.au.aggiustamentoGas.filter.exclusion.ExclusionFilterController
import it.eng.au.aggiustamentoGas.filter.inclusion.InclusionFilterController
import it.eng.au.aggiustamentoGas.model.agg.PdrWithMonthTreatmentYSBG
import it.eng.au.aggiustamentoGas.model.measure.Flow
import it.eng.au.sbg.dao.measure._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

class FlowControllerSbg(private val softExclusionFilterController: ExclusionFilterController,
                        private val strongExclusionFilterController: ExclusionFilterController,
                        private val inclusionFilters: List[InclusionFilterController]) extends FlowController(softExclusionFilterController, strongExclusionFilterController, inclusionFilters) {

  override val rmlDaoList: List[MeasureDAO] = List(
    new RmlDAO
  )

  override val treatmentFlowDAOList: List[MeasureDAO] = List(
    new RglDAOSbg,
    new TglDAOSbg,
    new TmlDAOSbg
  )

  override val mot6DaoList: List[MeasureDAO] = List(
    new TalDAOSbg,
    new TavDAOSbg,
    new TasDAOSbg,
    new RglDAOSbg
  )

  override val igmgList: List[MeasureDAO] = List(
    new IgmgDAOSbg
  )

  override val listDAO: List[MeasureDAO] = List(
    new RmvDAOSbg,
    new TmvDAOSbg,
    new RslDAOSbg,
    new Swg1DAOSbg,
    new FuiDAOSbg,
    //new FddDAOSbg, TODO: eventually read them
    new A01rDAOSbg,
    new A01DAOSbg,
    new A40rDAOSbg,
    new A40DAOSbg,
    new D01rDAOSbg,
    new D01DAOSbg,
    new D02rDAOSbg,
    new D02DAOSbg,
    new Sm1rDAOSbg,
    new Sm1DAOSbg,
    new Sm2rDAOSbg,
    new Sm2DAOSbg,
    new Ad2rDAOSbg,
    new Ad2DAOSbg,
    new Ad3rDAOSbg,
    new Ad3DAOSbg,
    new Ad4rDAOSbg,
    new Ad4DAOSbg,
    new Ad5rDAOSbg,
    new Ad5DAOSbg,
    new A02rDAOSbg,
    new A02DAOSbg,
    new S02rDAOSbg,
    new S02DAOSbg,
    new S40rDAOSbg,
    new S40DAOSbg,
    new R01rDAOSbg,
    new R01DAOSbg,
    new R40rDAOSbg,
    new R40DAOSbg,
    new M01rDAOSbg,
    new M01DAOSbg,
    new V01rDAOSbg,
    new V01DAOSbg,
    new V02rDAOSbg,
    new V02DAOSbg
  )

  override def filterRDDWithTreatmentSBG(rdd: RDD[Flow], rcuTreatment: Dataset[PdrWithMonthTreatmentYSBG]): RDD[Flow] = {
    val rddTreatment = rcuTreatment.rdd.map(t => (t.pdr, ()))

    rdd.map(f => (f.pdr, f))
      .leftOuterJoin(rddTreatment)
      .filter({ case (_, (_, treatment)) => treatment.isEmpty })
      .map({ case (_, (flow, _)) => flow })
  }

}
