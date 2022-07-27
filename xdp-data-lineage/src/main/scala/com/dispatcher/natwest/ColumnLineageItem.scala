package com.dispatcher.natwest

case class ColumnLineageItem (fromColName:String,
                              inputTableOrFile: Option[String],
                              columnDest : String ,
                              outputTableOrFile:String )


