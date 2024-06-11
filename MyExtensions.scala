import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.{Interval, ParseCancellationException}
import org.antlr.v4.runtime.tree.TerminalNodeImpl

import org.apache.spark.{QueryContext, SparkThrowableHelper}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, SQLConfHelper, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.errors.QueryParsingErrors
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql._
import java.util
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Random, Success, Try}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.encoders.OuterScopes
import org.apache.spark.sql.catalyst.expressions.{Expression, FrameLessOffsetWindowFunction, _}
import org.apache.spark.sql.catalyst.expressions.SubExprUtils._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.optimizer.OptimizeUpdateFields
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.trees.AlwaysProcess
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.util.{toPrettySQL, CharVarcharUtils, StringUtils}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns._
import org.apache.spark.sql.connector.catalog.{View => _, _}
import org.apache.spark.sql.connector.catalog.TableChange.{After, ColumnPosition}
import org.apache.spark.sql.connector.catalog.functions.{AggregateFunction => V2AggregateFunction, ScalarFunction, UnboundFunction}
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.{PartitionOverwriteMode, StoreAssignmentPolicy}
import org.apache.spark.sql.internal.connector.V1Function
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DayTimeIntervalType.DAY
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, SchemaUtils}
import org.apache.spark.util.collection.{Utils => CUtils}
import org.apache.spark.sql.catalyst.analysis._

import org.apache.spark.sql.catalyst.parser.AbstractSqlParser
import scala.io.Source


class HintParser(SparkSession: session, parser: ParserInterface) extends ParserInterface with Logging {
  /**
   * Parse a string to a [[LogicalPlan]].
   */
  override def parsePlan(sqlText: String): LogicalPlan = {
    logInfo(sqlText)
    filePath="/home/hejiahao/test.csv"
    // 读取文件内容
    val fileSource = Source.fromFile(filePath)
    val fileContent = fileSource.getLines().mkString("\n")
    fileSource.close()
    logInfo(fileContent)
    // 这里可以加Broadcast Hint
    var res = session.conf.get("spark.sql.optimizer.excludedRules")
    logInfo(res)
    session.conf.set("spark.sql.optimizer.excludedRules", "ReorderJoin")
    res = session.conf.get("spark.sql.optimizer.excludedRules")
    logInfo("Set ReorderJoin off.")
    logInfo(res)
    parser.parsePlan(sqlText)
  }
  /*
   * Parse a string to an [[Expression]].
   */
  override def parseExpression(sqlText: String): Expression = parser.parseExpression(sqlText)
  /**
   * Parse a string to a [[TableIdentifier]].
   */
  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    parser.parseTableIdentifier(sqlText)

  /**
   * Parse a string to a [[FunctionIdentifier]].
   */
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    parser.parseFunctionIdentifier(sqlText)

  /**
   * Parse a string to a [[StructType]]. The passed SQL string should be a comma separated
   * list of field definitions which will preserve the correct Hive metadata.
   */
  override def parseTableSchema(sqlText: String): StructType =
    parser.parseTableSchema(sqlText)

  /**
   * Parse a string to a [[DataType]].
   */
  override def parseDataType(sqlText: String): DataType = parser.parseDataType(sqlText)
  override def parseMultipartIdentifier(sqlText: String): Seq[String] = parser.parseMultipartIdentifier(sqlText)
  override def parseQuery(sqlText: String): LogicalPlan = parser.parseQuery(sqlText)
}

class MyExtensions extends Function1[SparkSessionExtensions, Unit] {
    override def apply(extensions: SparkSessionExtensions): Unit = {
    type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
    val parserBuilder: ParserBuilder = (session, parser) => new HintParser(session, parser)
    extensions.injectParser(parserBuilder)
    }
}
