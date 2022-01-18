package com.dispatcher.avnish

case class ColumnLineageItem (fromColName:String,
                              inputTableOrFile: Option[String],
                              columnDest : String ,
                              outputTableOrFile:String )


