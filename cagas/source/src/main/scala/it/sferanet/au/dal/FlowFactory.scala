package it.sferanet.au.dal

import it.sferanet.au.dal.MeasureType._
import it.sferanet.au.dal.autolettura._
import it.sferanet.au.dal.periodico._
import it.sferanet.au.dal.prestazionale._
import it.sferanet.au.dal.rettifica._
import it.sferanet.au.model.Flow
import it.sferanet.au.utilities.Environment
import org.apache.spark.rdd.RDD
import sun.reflect.generics.reflectiveObjects.NotImplementedException

object FlowFactory {
  private val log = org.apache.log4j.LogManager.getLogger(this.getClass)

  def build(typeMeasure: MeasureType): RDD[Flow] = {
    log.info(s"building: $typeMeasure")
    typeMeasure match {
      case R40 => new R40Table(Environment.getR40ParquetPath).get().map(_.asInstanceOf[Flow])
      case TGL => new TglTable(Environment.getTglParquetPath).get().map(_.asInstanceOf[Flow])
      case TML => new TmlTable(Environment.getTmlParquetPath).get().map(_.asInstanceOf[Flow])
      /** CR 22/08/2022 Gabrini Federico
       * 7.	autoletture: eliminare completamente le autoletture (TAL,TAS e TAV) dalla fase di ingestione.
       * */
      //case TAL => new TalTable(Environment.getTalParquetPath).get().map(_.asInstanceOf[Flow])
      //case TAV => new TavTable(Environment.getTavParquetPath).get().map(_.asInstanceOf[Flow])
      //case TAS => new TasTable(Environment.getTasParquetPath).get().map(_.asInstanceOf[Flow])
      case TMV => new TmvTable(Environment.getTmvParquetPath).get().map(_.asInstanceOf[Flow])
      case SW1 => new Sw1Table(Environment.getSw1ParquetPath).get().map(_.asInstanceOf[Flow])
      case RSL => new RslTable(Environment.getRslParquetPath).get().map(_.asInstanceOf[Flow])
      case RMV => new RmvTable(Environment.getRmvParquetPath).get().map(_.asInstanceOf[Flow])
      case RML => new RmlTable(Environment.getRmlParquetPath).get().map(_.asInstanceOf[Flow])
      case RGL => new RglTable(Environment.getRglParquetPath).get().map(_.asInstanceOf[Flow])
      case A01 => new A01Table(Environment.getA01ParquetPath).get().map(_.asInstanceOf[Flow])
      case A01R => new A01RTable(Environment.getA01rParquetPath).get().map(_.asInstanceOf[Flow])
      case A40 => new A40Table(Environment.getA40ParquetPath).get().map(_.asInstanceOf[Flow])
      case SM1 => new Sm1Table(Environment.getSm1ParquetPath).get().map(_.asInstanceOf[Flow])
      case IM1POST => new Im1PostTable(Environment.getIm1PostParquetPath).get().map(_.asInstanceOf[Flow])
      case IM1PRE => new Im1PreTable(Environment.getIm1PreParquetPath).get().map(_.asInstanceOf[Flow])
      case IGMGPRE => new IgmgPreTable(Environment.getIgmgPreParquetPath).get().map(_.asInstanceOf[Flow])
      case IGMGPOST => new IgmgPostTable(Environment.getIgmgPostParquetPath).get().map(_.asInstanceOf[Flow])
      case IGMRPRE => new IgmrPreTable(Environment.getIgmrPreParquetPath).get().map(_.asInstanceOf[Flow])
      case IGMRPOST => new IgmrPostTable(Environment.getIgmrPostParquetPath).get().map(_.asInstanceOf[Flow])
      case M01R => new M01rTable(Environment.getM01rParquetPath).get().map(_.asInstanceOf[Flow])
      case R01R => new R01rTable(Environment.getR01rParquetPath).get().map(_.asInstanceOf[Flow])
      case R40R => new R40rTable(Environment.getR40rParquetPath).get().map(_.asInstanceOf[Flow])
      case S02 => new S02Table(Environment.getS02ParquetPath).get().map(_.asInstanceOf[Flow])
      case A02R => new A02RTable(Environment.getA02rParquetPath).get().map(_.asInstanceOf[Flow])
      case A02 => new A02Table(Environment.getA02ParquetPath).get().map(_.asInstanceOf[Flow])
      case AD2 => new AD2Table(Environment.getAd2ParquetPath).get().map(_.asInstanceOf[Flow])
      case AD3 => new AD3Table(Environment.getAd3ParquetPath).get().map(_.asInstanceOf[Flow])
      case AD4 => new AD4Table(Environment.getAd4ParquetPath).get().map(_.asInstanceOf[Flow])
      case AD5 => new AD5Table(Environment.getAd5ParquetPath).get().map(_.asInstanceOf[Flow])
      case FDD => new FDDTable(Environment.getFddParquetPath).get().map(_.asInstanceOf[Flow])
      case FUI => new FUITable(Environment.getFuiParquetPath).get().map(_.asInstanceOf[Flow])
      case M01 => new M01Table(Environment.getM01ParquetPath).get().map(_.asInstanceOf[Flow])
      case R01 => new R01Table(Environment.getR01ParquetPath).get().map(_.asInstanceOf[Flow])
      case S40 => new S40Table(Environment.getS40ParquetPath).get().map(_.asInstanceOf[Flow])
      case SWG1 => new Swg1Table(Environment.getSwg1ParquetPath).get().map(_.asInstanceOf[Flow])
      case V01 => new V01Table(Environment.getV01ParquetPath).get().map(_.asInstanceOf[Flow])
      case V02 => new V02Table(Environment.getV02ParquetPath).get().map(_.asInstanceOf[Flow])
      case A40R => new A40RTable(Environment.getA40rParquetPath).get().map(_.asInstanceOf[Flow])
      case AD2R => new AD2RTable(Environment.getAd2rParquetPath).get().map(_.asInstanceOf[Flow])
      case AD3R => new AD3RTable(Environment.getAd3rParquetPath).get().map(_.asInstanceOf[Flow])
      case AD4R => new AD4RTable(Environment.getAd4rParquetPath).get().map(_.asInstanceOf[Flow])
      case AD5R => new AD5RTable(Environment.getAd5rParquetPath).get().map(_.asInstanceOf[Flow])
      case S02R => new S02RTable(Environment.getS02rParquetPath).get().map(_.asInstanceOf[Flow])
      case S40R => new S40RTable(Environment.getS40rParquetPath).get().map(_.asInstanceOf[Flow])
      case V01R => new V01RTable(Environment.getV01rParquetPath).get().map(_.asInstanceOf[Flow])
      case V02R => new V02RTable(Environment.getV02rParquetPath).get().map(_.asInstanceOf[Flow])
      case SM1R => new SM1RTable(Environment.getSm1rParquetPath).get().map(_.asInstanceOf[Flow])
      case _ =>
        log.error("internal error")
        throw new NotImplementedException
    }
  }


}
