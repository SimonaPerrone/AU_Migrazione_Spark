package it.au.misure.util

import java.util.zip.ZipFile
import java.util.zip.ZipInputStream
import java.io._

import scala.collection.JavaConversions._
import java.util.zip.ZipEntry

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

/**
 * ==ZipArchive==
 * Utilizzato per decomprimere i file di misura in formato Zip.
 */
class ZipArchive extends LoggingSupport{

	val BUFSIZE = 4096
  val buffer = new Array[Byte](BUFSIZE)

  private var _timeStampDecompressione = new java.sql.Timestamp(System.currentTimeMillis())
  private var _annomese_Decompressione = 0
  var listFilesBuff = ListBuffer[Row]()

  def timeStampDecompressione(newVal:java.sql.Timestamp) { _timeStampDecompressione = newVal}
  def annomeseDecompressione(newVal:Int) { _annomese_Decompressione = newVal}

	def isArchive(f:File) : Boolean = {

    if(f.getName.toLowerCase().endsWith(".zip"))
      true
    else
      false

		/*var fileSignature:Int = 0
		try {
			val raf = new RandomAccessFile(f, "r")
			 fileSignature = raf.readInt()
		} catch {
			case z: Exception => {
				return false
			}
		}

		return fileSignature == 0x504B0304 || fileSignature == 0x504B0506 || fileSignature == 0x504B0708
		*/
	}

			/**
			 * Decomprime tutti i file in formato Zip
       * @param source archivio zip
       * @param targetFolder cartella di destinazione file decompresso
			 */
			def unZip(source: String, targetFolder: String,is_Gas : Boolean,print_files :Boolean,fileIntoArchive:String=""):Boolean = {

				   var zipFile:ZipFile = null
           val targetFile =new File(targetFolder)


					 val sp = source.split(File.separator)
					 val annoMeseGiornoDir = (s"${sp(sp.length - 3)}${sp(sp.length - 2)}").toInt
           var is_tried=false

					try {


						if (is_Gas) {
							val fsrc = new File(source)
							if (!isArchive(fsrc) && fsrc.getName!="*") {
                copyFile(source, targetFolder + File.separator + fsrc.getName)
                if(print_files )
                log.info(s"File ${source} copiato nella cartella ${targetFolder}")



                listFilesBuff+=Row("000","OK",source,targetFolder+ File.separator + fsrc.getName,annoMeseGiornoDir,"C",_timeStampDecompressione,_annomese_Decompressione)

								true
              }
							else {

                val result=unZipIt(source,fileIntoArchive, targetFile.getAbsolutePath, print_files, annoMeseGiornoDir)
                if(!result && fsrc.getName!="*") {
                  log.info(s"UNZIP PROCEDURA ALTERNATIVA : ${source}")
                  is_tried = true
									zipFile = new ZipFile(source)
                  try{
                  unzipAllFile(zipFile.entries.toList, getZipEntryInputStream(zipFile) _, targetFile, source,fileIntoArchive, print_files, annoMeseGiornoDir)
                  }
                  catch {
                    case e: Exception => {log.info(s"${e.getMessage} - source: ${source} - targetFolder: ${targetFolder}"); return false;}
                  }
                }
                else result



							}
						} else {

              val result=unZipIt(source,fileIntoArchive, targetFile.getAbsolutePath, print_files, annoMeseGiornoDir)
              if(!result) {
                log.info(s"UNZIP PROCEDURA ALTERNATIVA : ${source}")
                is_tried = true
								zipFile = new ZipFile(source)
                try {
                unzipAllFile(zipFile.entries.toList, getZipEntryInputStream(zipFile) _, targetFile, source,fileIntoArchive, print_files, annoMeseGiornoDir)
                }
                catch {
                  case e: Exception => {log.info(s"${e.getMessage} - source: ${source} - targetFolder: ${targetFolder}"); return false;}
                }
              }
              else result


						}



					}catch {
						case e: Exception => {

              if(!is_tried) {
                try {
                  unZipIt(source, fileIntoArchive,targetFile.getAbsolutePath, print_files, annoMeseGiornoDir)
                }
                catch {
                  case e2: Exception => {
                    log.info(s"${e2.getMessage} - source: ${source} - targetFolder: ${targetFolder}")
                    listFilesBuff += Row("001", s"Errore in decompressione causa : ${e2.getMessage}", source, targetFolder, annoMeseGiornoDir, "E", _timeStampDecompressione,_annomese_Decompressione)
                    false
                  }
                }
              }else {
                log.info(s"${e.getMessage} - source: ${source} - targetFolder: ${targetFolder}")
                listFilesBuff += Row("001", s"Errore in decompressione causa : ${e.getMessage}", source, targetFolder, annoMeseGiornoDir, "E", _timeStampDecompressione,_annomese_Decompressione)
                false
              }


						}
					}
				finally
				{
					if(zipFile!=null)
					zipFile.close()


				}

	}

	def copyFile(src :String,dest:String): Unit ={

		val fdest= new File(dest)
		val name = fdest.getName()
    val ext_index=name.lastIndexOf(".")

    if(ext_index >0) {
      val extension = name.substring(ext_index)
      val extension_lower = extension.toLowerCase();

      val fdestF= new File(dest)
      val fsrcF= new File(src)
      if(fsrcF.canRead && !fdestF.exists()) {
        try {
          FileUtils.copyFile(fsrcF, fdestF)
        }
        catch {
          case e: Exception => {
            if (!fdestF.exists())
              throw e
          }
        }
      }
      else
        throw new Exception(s"Impossibile copiare il file ${src} nella destinazione ${dest}")
    }
    else
      throw new Exception(s"Impossibile copiare il file ${src} nella destinazione ${dest}")
		/*val inputChannel = new FileInputStream(src).getChannel
		val outputChannel = new FileOutputStream(dest).getChannel
		outputChannel.transferFrom(inputChannel, 0, inputChannel.size)
		inputChannel.close
		outputChannel.close()*/
	}
	/**
	 * Retituisce il flusso di ingresso per la lettura del contenuto del file zip specificato.
	 * @param zipFile classe utilizzata per leggere le voci da un file zip
	 */
	    def getZipEntryInputStream(zipFile: ZipFile)(entry: ZipEntry) = zipFile.getInputStream(entry)

			/**
			 * Decomprime l'elenco di file in formato Zip

       * @param targetFolder cartella di destinazione file decompresso
			 */
			def unzipAllFile(entryList: List[ZipEntry], inputGetter: (ZipEntry) => InputStream, targetFolder: File,source:String,fileIntoArchive:String,print_files:Boolean,annomesegiornodir:Int): Boolean = {



					entryList match {
					case entry :: entries =>{

					  if (entry.isDirectory) new File(targetFolder, entry.getName).mkdirs
						else {

              var filename_out = new File(entry.getName()).getName
              val ext_index=filename_out.lastIndexOf(".")
              val isok=if((fileIntoArchive!="" && fileIntoArchive.toLowerCase()== filename_out.toLowerCase()) || fileIntoArchive=="") true else false

              if(ext_index>0 && isok) {
                val extension = filename_out.substring(ext_index)
                val extension_lower = extension.toLowerCase()
                //filename_out = filename_out.replace(extension, extension_lower)

                if (extension_lower.equals(".xml") && !entry.getName.contains("__MACOSX")) {
                  if (filename_out.charAt(0) == '.')
                    filename_out = filename_out.substring(1)

                  val dirtimestamp=System.currentTimeMillis().toString
                  val newFile_tmp = new File(targetFolder.getAbsolutePath + File.separator + filename_out)
                  //val newFile = if(!newFile_tmp.exists())newFile_tmp else new File(targetFolder.getAbsolutePath + File.separator + dirtimestamp + File.separator + filename_out)
									val newFile = newFile_tmp

                  val rtv = saveFile(inputGetter(entry), new FileOutputStream(newFile))
                  if (!rtv) return false
                  if (print_files) log.info(s"File sorgente ${source}  , scompattato : ${newFile.getPath}")
                  listFilesBuff += Row("000", "OK", source, newFile.getPath, annomesegiornodir, "U", _timeStampDecompressione, _annomese_Decompressione)
                } else {
                  if (print_files) log.info(s"File sorgente ${source}  , NON scompattato : ${targetFolder.getAbsolutePath}/${filename_out}")
                  listFilesBuff += Row("003", s"Il file ${targetFolder.getAbsolutePath}/${filename_out} non è stato decompresso poichè non è un file xml", source, targetFolder.getAbsolutePath + File.separator + filename_out, annomesegiornodir, "U", _timeStampDecompressione, _annomese_Decompressione)
                }
              }
            }
							return unzipAllFile(entries, inputGetter, targetFolder,source,fileIntoArchive,print_files,annomesegiornodir)
					}
					case _ =>
						 true
					}

			}

	/**
	 * Scrive i file decompressi su file system.
	 * @param fis flusso di byte in ingresso
	 * @param fos flusso di byte in uscita
	 */
			def saveFile(fis: InputStream, fos: OutputStream):Boolean = {
				try {
					writeToFile(bufferReader(fis) _, fos)
	 				true
				}
				catch {
					case e: StackOverflowError => {
						//println(s"${e.getMessage} - source: ${source} ", e)
						//unZipIt(source,targetF,print_files,listfiles)
						false
					}
				} finally {
					fis.close
					fos.close
				}

			}


			def bufferReader(fis: InputStream)(buffer: Array[Byte]) = (fis.read(buffer), buffer)

					def writeToFile(reader: (Array[Byte]) => Tuple2[Int, Array[Byte]], fos: OutputStream): Boolean = {
							val (length, data) = reader(buffer)
									if (length >= 0) {
										fos.write(data, 0, length)
										writeToFile(reader, fos)
									} else
										true
					}

	def unZipIt(zipFilef: String, fileIntoArchived :String,outputFolder: String,print_files:Boolean,annomesegiornodir_zip:Int): Boolean = {

		val buffer = new Array[Byte](BUFSIZE)


    var zipFile =zipFilef
    var fileIntoArchive=fileIntoArchived
		try {

     val finfo = new File(zipFile)
     val srcpath=finfo.getAbsolutePath.replace("/*","")
     if(finfo.getName=="*")
      {
        if(print_files )
        log.info("**** Ricerca del file " + fileIntoArchive + " nel path sorgente : " + srcpath+File.separator)

        val fileIntoArchive1=fileIntoArchive.replace(".xml",".XML")
        val fileIntoArchive2=fileIntoArchive.replace(".XML",".xml")
        if(new File(srcpath+File.separator+fileIntoArchive1).canRead)
          {
           fileIntoArchive=fileIntoArchive1
           copyFile(srcpath+File.separator+fileIntoArchive, outputFolder + File.separator + fileIntoArchive)
           if(print_files )
           log.info(s"File ${srcpath+File.separator+fileIntoArchive} copiato nella cartella ${outputFolder}")

           listFilesBuff+=Row("000","OK",srcpath+File.separator+fileIntoArchive,outputFolder+ File.separator + fileIntoArchive,annomesegiornodir_zip,"C",_timeStampDecompressione,_annomese_Decompressione)
           return true
          }
        else if(new File(srcpath+File.separator+fileIntoArchive2).canRead)
        {
          fileIntoArchive=fileIntoArchive2
          copyFile(srcpath+File.separator+fileIntoArchive, outputFolder + File.separator + fileIntoArchive)
          if(print_files )
            log.info(s"File ${srcpath+File.separator+fileIntoArchive} copiato nella cartella ${outputFolder}")

          listFilesBuff+=Row("000","OK",srcpath+File.separator+fileIntoArchive,outputFolder+ File.separator + fileIntoArchive,annomesegiornodir_zip,"C",_timeStampDecompressione,_annomese_Decompressione)
          return true
        }
        else
          {
            val files=new File(srcpath).listFiles( new FileFilter {
              override def accept(pathname: File): Boolean = pathname.getName.toLowerCase.endsWith(".zip")
            })
            var notfound=true
            breakable {

              for (f <- files) {
                val zis: ZipInputStream = new ZipInputStream(new FileInputStream(f));
                var ze: ZipEntry = zis.getNextEntry();

                notfound = true
                while (ze != null && notfound) {
                  var fileName = new File(ze.getName()).getName
                  if (fileName.toLowerCase() == fileIntoArchive.toLowerCase()) {
                    fileIntoArchive=fileName
                    zipFile = f.getAbsolutePath
                    notfound = false
                  }
                  else
                    ze = zis.getNextEntry()
                }

                zis.closeEntry()
                zis.close()

                if (!notfound)
                  break

              }
            }
            if(notfound)
              {
                //if (print_files)
                  log.info(s"File sorgente ${fileIntoArchive}  , NON trovato tra tutti gli archivi presenti nel path  : ${srcpath}")
                listFilesBuff += Row("003", s"Il file ${fileIntoArchive} non è stato trovato tra tutti gli archivi presenti nel path  : ${srcpath}", fileIntoArchive, outputFolder + File.separator , annomesegiornodir_zip, "U", _timeStampDecompressione, _annomese_Decompressione)

                return false
              }
          }


      }

			val zis: ZipInputStream = new ZipInputStream(new FileInputStream(zipFile));
			var ze: ZipEntry = zis.getNextEntry();
      var zipis_empty=true

			while (ze != null) {

        zipis_empty=false
				var fileName = new File(ze.getName()).getName
				val index_ext=fileName.lastIndexOf(".")

        val isok=if((fileIntoArchive!="" && fileIntoArchive.toLowerCase()== fileName.toLowerCase()) || fileIntoArchive=="") true else false
				if(index_ext >0 && isok) {

					val extension = fileName.substring(index_ext)
					val extension_lower = extension.toLowerCase()

					val newFile_tmp = new File(outputFolder + File.separator + fileName)
					val sp = newFile_tmp.getCanonicalPath.split(File.separator)
					val annoMeseGiornoDir = (s"${sp(sp.length - 3)}${sp(sp.length - 2)}").toInt

					if (extension_lower.equals(".xml") && !ze.getName.contains("__MACOSX")) {
						if (fileName.charAt(0) == '.')
							fileName = fileName.substring(1)

            val dirtimestamp=System.currentTimeMillis().toString
            //val newFile = if(!newFile_tmp.exists())newFile_tmp else new File(outputFolder + File.separator + dirtimestamp +File.separator+ fileName)
						val newFile = newFile_tmp

						new File(newFile.getParent()).mkdirs()

						val fos = new FileOutputStream(newFile)

						var len: Int = zis.read(buffer)

						while (len > 0) {

							fos.write(buffer, 0, len)
							len = zis.read(buffer)
						}

						fos.close()


						if (print_files)
              log.info(s"File sorgente ${zipFile}  ,  scompattato : ${newFile.getPath}")
						listFilesBuff += Row("000", "OK", zipFile, newFile.getPath, annoMeseGiornoDir, "U", _timeStampDecompressione, _annomese_Decompressione)
					} else {
					  //	if (print_files)
              log.info(s"File sorgente ${zipFile}  , NON scompattato : ${outputFolder}${File.separator}${fileName}")
						listFilesBuff += Row("003", s"Il file ${outputFolder}${File.separator}${fileName} non è stato decompresso poichè non è un file xml", zipFile, outputFolder + File.separator + fileName, annoMeseGiornoDir, "U", _timeStampDecompressione, _annomese_Decompressione)
					}
				}

				ze = zis.getNextEntry()
			}
      if(zipis_empty)
        {
          listFilesBuff += Row("003", s"Attenzione l'archivio sorgente ${zipFile} è vuoto", zipFile, outputFolder, annomesegiornodir_zip, "U", _timeStampDecompressione, _annomese_Decompressione)
        }
			zis.closeEntry()
			zis.close()
			true

		} catch {
			case e: Exception => {
				log.info(s"${e.getMessage} - source: ${zipFile} - targetFolder: ${outputFolder}")
        listFilesBuff+=Row("002",s"Errore in decompressione causa : ${e.getMessage}",zipFile,outputFolder,annomesegiornodir_zip,"E",_timeStampDecompressione,_annomese_Decompressione)
				//log.error(e.getStackTraceString)
				false
			}
		}

	}


}