package com.phdata.di.helper

import scala.io.BufferedSource
import net.liftweb.json.DefaultFormats
import net.liftweb.json._

class ConfigProperties() {

  private var _sfURL: String = _
  private var _sfAccount: String = _
  private var _sfUser: String = _
  private var _sfPassword: String = _
  private var _sfDatabase: String = _
  private var _sfSchema: String = _
  private var _sfRole: String = _
  private var _sfTable: String = _
  private var _airlineDataPath: String = _
  private var _airportDataPath: String = _
  private var _flightsDataPath: String = _
  private var _readDBURL: String = _
  private var _masterDBURL: String = _
  private var _dbUser: String = _
  private var _dbPass: String = _
  private var _database: String = _

  def sfURL = _sfURL
  def sfAccount = _sfAccount
  def sfUser = _sfUser
  def sfPassword = _sfPassword
  def sfDatabase = _sfDatabase
  def sfSchema = _sfSchema
  def sfRole = _sfRole
  def sfTable = _sfTable
  def airlineDataPath = _airlineDataPath
  def airportDataPath = _airportDataPath
  def flightsDataPath = _flightsDataPath
  def readDBURL = _readDBURL
  def masterDBURL = _masterDBURL
  def dbUser = _dbUser
  def dbPass = _dbPass
  def database = _database

  def sfURL_=(sfURL: String) {
    _sfURL = sfURL
  }

  def sfAccount_=(sfAccount: String) {
    _sfAccount = sfAccount
  }
  def sfUser_=(sfUser: String) {
    _sfUser = sfUser
  }
  def sfPassword_=(sfPassword: String) {
    _sfPassword = sfPassword
  }
  def sfDatabase_=(sfDatabase: String) {
    _sfDatabase = sfDatabase
  }
  def sfSchema_=(sfSchema: String) {
    _sfSchema = sfSchema
  }
  def sfRole_=(sfRole: String) {
    _sfRole = sfRole
  }
  def sfTable_=(sfTable: String) {
    _sfTable = sfTable
  }
  def airlineDataPath_=(airlineDataPath: String) {
    _airlineDataPath = airlineDataPath
  }
  def airportDataPath_=(airportDataPath: String) {
    _airportDataPath = airportDataPath
  }
  def flightsDataPath_=(flightsDataPath: String) {
    _flightsDataPath = flightsDataPath
  }
  def readDBURL_=(readDBURL: String) {
    _readDBURL = readDBURL
  }
  def masterDBURL_=(masterDBURL: String) {
    _masterDBURL = masterDBURL
  }
  def dbUser_=(dbUser: String) {
    _dbUser = dbUser
  }
  def dbPass_=(dbPass: String) {
    _dbPass = dbPass
  }
  def database_=(database: String) {
    _database = database
  }

  /***
   * Load the json configuration from properties file to class object.
   * @return
   */
  def setConfigPropertey(): ConfigProperties = {
    implicit val formats: Formats = DefaultFormats
    val config = new ConfigProperties()
    val source: BufferedSource = scala.io.Source.fromFile(getClass.getResource("/pipeline-config.json").getFile)
    val lines: String = try source.mkString finally source.close()
    val json: JValue = parse(lines)
    config.sfURL_=((json \ "sfURL").extractOrElse(""))
    config.sfAccount_=((json \ "sfAccount").extractOrElse(""))
    config.sfUser_=((json \ "sfUser").extractOrElse(""))
    config.sfPassword_=((json \ "sfPassword").extractOrElse(""))
    config.sfDatabase_=((json \ "sfDatabase").extractOrElse(""))
    config.sfSchema_=((json \ "sfSchema").extractOrElse(""))
    config.sfRole_=((json \ "sfRole").extractOrElse(""))
    config.airlineDataPath_=((json \ "airlineDataPath").extractOrElse(""))
    config.airportDataPath_=((json \ "airportDataPath").extractOrElse(""))
    config.flightsDataPath_=((json \ "flightsDataPath").extractOrElse(""))
    config.readDBURL_=((json \ "readDBURL").extractOrElse(""))
    config.masterDBURL_=((json \ "masterDBURL").extractOrElse(""))
    config.dbUser_=((json \ "dbUser").extractOrElse(""))
    config.dbPass_=((json \ "dbPass").extractOrElse(""))
    config.database_=((json \ "database").extractOrElse(""))

    config
  }
}
