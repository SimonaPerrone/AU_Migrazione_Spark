package it.au.misure.util

import org.slf4j.LoggerFactory

@SerialVersionUID(114L)
trait LoggingSupport extends Serializable{
	val log = org.slf4j.LoggerFactory.getLogger("FLUSSO-MISURE")
	val blocksize=1024 * 1024 * 100
}