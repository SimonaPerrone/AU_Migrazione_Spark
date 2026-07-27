package it.eng.au.freezerPreCalcolo.freezer.flow

import it.eng.au.freezerPreCalcolo.dao.rcugas._
import it.eng.au.freezerPreCalcolo.freezer.prepare.RcuGasMassivoPreparation
import it.eng.au.freezerPreCalcolo.freezer.publication.{RcuGasMassivoPublication, RcuGasTechPublication}
import it.eng.au.freezerPreCalcolo.freezer.transform.{RcuGasMassivoTransformation, RcuGasTechTransformation}
import it.eng.au.freezerPreCalcolo.utility.environment.Environment
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{lit, to_date}
import org.apache.spark.storage.StorageLevel

/** Contains variables and methods needed to perform the freezing process of
 * RcugasMassivo and RcugasTech tables. */
class RcuGasMassivoFlow extends RunnableFreezer {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val rcuGasMassivoDAO = new RcuGasMassivoDAO
  val rcuGasConnessioniDistr2DAO = new RcuGasConnessioniDistr2DAO
  val rcuGasVarMisuratoreDAO = new RcuGasVarMisuratoreDAO
  val rcuGasVarConvertitoreDAO = new RcuGasVarConvertitoreDAO
  val rcuGasMassivoFrozenDao = new RcuGasMassivoFrozenDAO
  val rcuGasTechFrozenDao = new RcuGasTechFrozenDAO

  def runFreezer(): Unit = {

    val executionId = Environment.executionId
    val freezeDateColumn = to_date(lit(Environment.getFreezeDate))
    logger.warn(s"freezeDateColumn: $freezeDateColumn")

    val rcuGasMassivoDF = rcuGasMassivoDAO.readParquet()
    val rcuGasConnessioniDistr2DF = rcuGasConnessioniDistr2DAO.readParquet()
    val rcuGasVarMisuratoreDF = rcuGasVarMisuratoreDAO.readParquet()
    val rcuGasVarConvertitoreDF = rcuGasVarConvertitoreDAO.readParquet()

    val rcuGasMassivo = RcuGasMassivoPreparation.prepareRcuGasMassivo(rcuGasMassivoDF, freezeDateColumn)
    val activePdr = RcuGasMassivoPreparation.prepareActivePdr(rcuGasMassivoDF, freezeDateColumn)
    val idRegClimatica = RcuGasMassivoPreparation.prepareIdRegClimatica(rcuGasConnessioniDistr2DF, freezeDateColumn)
    val rcuGasMassivoFrozen = RcuGasMassivoTransformation.transform(rcuGasMassivo, activePdr, idRegClimatica, freezeDateColumn, executionId.toString)
    val rcuGasMassivoFrozenP = RcuGasMassivoPublication.publication(rcuGasMassivoFrozen)
      .persist(StorageLevel.MEMORY_AND_DISK)

    logger.warn(s"Writing parquet in: ${rcuGasMassivoFrozenDao.hdfsOutput}")
    rcuGasMassivoFrozenDao.writeParquet(rcuGasMassivoFrozenP)

    val rcuGasMassivoFrozenPdr = RcuGasMassivoPreparation.prepareRcuGasMassivoFrozen(rcuGasMassivoFrozenP)
    val rcuGasVarMisuratoreMis = RcuGasMassivoPreparation.prepareMis(rcuGasVarMisuratoreDF)
    val rcuGasVarConvertitoreConv = RcuGasMassivoPreparation.prepareConv(rcuGasVarConvertitoreDF)

    val rcuGasTechFrozen = RcuGasTechTransformation.transform(rcuGasMassivoFrozenPdr, rcuGasVarMisuratoreMis, rcuGasVarConvertitoreConv, freezeDateColumn, executionId.toString)
    val rcuGasTechFrozenP = RcuGasTechPublication.publication(rcuGasTechFrozen)

    logger.warn(s"Writing parquet in: ${rcuGasTechFrozenDao.hdfsOutput}")
    rcuGasTechFrozenDao.writeParquet(rcuGasTechFrozenP)
  }
}
