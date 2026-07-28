package com.example.sender

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.io.Resource
import org.springframework.http._
import org.springframework.http.MediaType.APPLICATION_JSON
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory
import org.springframework.stereotype.Component
import org.springframework.web.client.{HttpClientErrorException, HttpStatusCodeException, RestTemplate}
import org.springframework.boot.web.client.RestTemplateBuilder
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.LazyLogging
import org.apache.hc.client5.http.impl.classic.HttpClients
import org.apache.hc.client5.http.impl.io.PoolHttpClientConnectionManagerBuilder
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy
import org.apache.hc.core5.ssl.SSLContextBuilder
import org.owasp.encoder.Encode

import scala.jdk.CollectionConverters._ // Use scala.collection.JavaConverters._ for Scala 2.12 and earlier
import scala.util.Try

@Component
class EventSender(restTemplateBuilder: RestTemplateBuilder) extends LazyLogging {

  @Value("${airflow.rest.authentication.user}") var airflowUser: String = _
  @Value("${airflow.rest.authentication.password}") var airflowPassword: String = _
  @Value("${airflow.rest.root-endpoint}") var airflowEndpoint: String = _
  @Value("${airflow.rest.path}") var airflowPath: String = _

  lazy val airflowRestTemplate: RestTemplate = restTemplateBuilder.rootUri(airflowEndpoint).build()

  @Value("${itrust-store-location}") var edfTrustStore: Resource = _
  @Value("${itrust-store-password}") var edfTrustStorePassword: String = _
  @Value("${tedf.rest.root-endpoint}") var edfEndpoint: String = _
  @Value("${tedf.rest.path}") var edfPath: String = _
  @Value("${tedf.rest.child-context-path}") var edfChildContextPath: String = _

  lazy val edfRestTemplate: RestTemplate = restTemplateBuilder
    .rootUri(edfEndpoint)
    .requestFactory(() => createSslRequestFactory(edfTrustStore, edfTrustStorePassword))
    .build()

  @Autowired var mapper: ObjectMapper = _
  @Autowired var repository: EventRepository = _
  @Autowired var clientTokenManager: ClientTokenManager = _

  // Legacy
  def sendEvent(event: EnrichedEvent): ResponseEntity[String] = {
    val headers = new HttpHeaders()
    headers.setContentType(APPLICATION_JSON)
    headers.setBasicAuth(airflowUser, airflowPassword)

    val request = new HttpEntity[AirFlowRequest](AirFlowRequest(event), headers)

    try {
      logger.debug(s"Sending POST request to AirFlow endpoint [${airflowEndpoint}${airflowPath}]")

      airflowRestTemplate.postForEntity(
        airflowPath,
        request,
        classOf[String]
      )
    } catch {
      // HTTP 4xx / 5xx after max retries done
      case ex: HttpStatusCodeException =>
        val message = s"Failed to send event to AirFlow after retries. Status: ${ex.getStatusCode}"
        logger.error(message, ex)
        throw new RuntimeException(message, ex)
    }
  }

  /** Builds the final AirFlow DAG URL from airflowEndpoint and dagId */
  private def buildDagUrl(base: String, dagId: String): String = {
    val marker = "/airflow/api/v1/dags/"
    val idx = base.indexOf(marker)
    if (idx >= 0) {
      val prefix = base.substring(0, idx + marker.length) // keep .../airflow/api/v1/dags/
      s"${prefix}${dagId}${airflowPath}"
    } else {
      // e.g., http://localhost:1234 - leave as-is
      base
    }
  }

  def sendEvent(event: EnrichedEvent, dagId: String): ResponseEntity[String] = {
    val headers = new HttpHeaders()
    headers.setContentType(APPLICATION_JSON)
    headers.setBasicAuth(airflowUser, airflowPassword)

    val request = new HttpEntity[AirFlowRequest](AirFlowRequest(event), headers)
    val dagUrl = buildDagUrl(airflowEndpoint, dagId)

    try {
      logger.debug(s"Sending POST request to Airflow endpoint [$dagUrl] with payload: ${request.getBody}")
      airflowRestTemplate.postForEntity(dagUrl, request, classOf[String])
    } catch {
      case ex: HttpStatusCodeException =>
        val message = s"Failed to send event to Airflow endpoint [$dagUrl]. Status: ${ex.getStatusCode}"
        logger.error(message, ex)
        throw new RuntimeException(message, ex)
    }
  }

  def enrichEvent(event: EventResponse): EnrichedEvent =
    EnrichedEvent(event, fetchContext(event.getContextId).orNull)

  def fetchContext(id: String): Option[ContextResponse] = id match {
    case null =>
      logger.warn("Unable to fetch context for null context id")
      None
    case _ =>
      repository.findContextById(id).orElse {
        getContextFromEdf(id).map { context =>
          repository.save(context)
          context
        }
      }
  }

  def fetchParentContext(id: String, params: Map[String, String]): Option[ContextResponse] = fetchContext(id) match {
    case None => None
    case Some(parentContext) if params.nonEmpty && checkRequestedParams(parentContext, params) => Some(parentContext)
    case Some(parentContext) if parentContext.getParentIds.isEmpty =>
      if (checkRequestedParams(parentContext, params)) Some(parentContext) else None
    case Some(parentContext) =>
      parentContext.getParentIds.asScala.foldLeft[Option[ContextResponse]](None)(
        (context, parentId) => context.orElse(fetchParentContext(parentId, params))
      )
  }

  private def checkRequestedParams(context: ContextResponse, params: Map[String, String]): Boolean = {
    val contextJson = ujson.read(mapper.writeValueAsString(context))
    val paramsToCheckInEvent: Map[String, Seq[String]] = params.filterNot {
      case (k, v) =>
        Try(contextJson("data")(k).strOpt).getOrElse(None).contains(v) ||
          Try(contextJson(k).strOpt).getOrElse(None).contains(v)
    }.view.mapValues(Seq(_)).toMap

    paramsToCheckInEvent.isEmpty
  }

  def getContextFromEdf(id: String): Option[ContextResponse] = {
    val request = createBaseEdfRequest()

    try {
      val response = edfRestTemplate.exchange(edfPath, HttpMethod.GET, request, classOf[ContextResponse], id)
      if (response.getStatusCode != HttpStatus.OK) {
        val sanitizedStatusCode = Encode.forJava(response.getStatusCode.value.toString)
        val sanitizedBody = Encode.forJava(Option(response.getBody).map(_.toString).getOrElse(""))
        logger.info(s"EDF returned context response with unexpected status=[$sanitizedStatusCode], body=[$sanitizedBody]")
      }

      logger.debug(s"GET /context request to EDF with id $id returned ${response.getBody}")
      Option(response.getBody)
    } catch {
      case e: HttpClientErrorException =>
        logger.warn(s"EDF returned context response with status=[${e.getStatusCode}], body=[${e.getResponseBodyAsString}]")
        None
      case ex: Throwable =>
        logger.error(s"Failed to fetch context from EDF for id [$id]", ex)
        None
    }
  }

  def fetchChildContext(id: String, params: Map[String, String], limit: String = "1"): Option[ContextResponse] = {
    fetchContext(id) match {
      case None => None
      case Some(context) if checkRequestedParams(context, params) => Some(context)
      case _ =>
        getChildContextHierarchyFromEdf(id, limit.toInt) match {
          case Some(childrenHierarchy) => getMatchingChildContext(childrenHierarchy, params)
          case None => None
        }
    }
  }

  def getChildContextHierarchyFromEdf(id: String, limit: Int): Option[String] = {
    val request = createBaseEdfRequest()

    try {
      val response = edfRestTemplate.exchange(edfChildContextPath, HttpMethod.GET, request, classOf[String], id, Integer.valueOf(limit))
      if (response.getStatusCode != HttpStatus.OK) {
        val sanitizedStatusCode = Encode.forJava(response.getStatusCode.value.toString)
        val sanitizedBody = Encode.forJava(Option(response.getBody).getOrElse(""))
        logger.info(s"EDF returned context response with unexpected status=[$sanitizedStatusCode], body=[$sanitizedBody]")
      }

      logger.debug(s"GET /parent-child-hierarchy request to EDF with id $id and limit $limit returned ${response.getBody}")
      Option(response.getBody)
    } catch {
      case e: HttpClientErrorException =>
        logger.warn(s"EDF returned context response with status=[${e.getStatusCode}], body=[${e.getResponseBodyAsString}]")
        None
      case ex: Throwable =>
        logger.error(s"Failed to fetch child context hierarchy from EDF for id [$id]", ex)
        None
    }
  }

  private def getMatchingChildContext(childrenHierarchy: String, params: Map[String, String]): Option[ContextResponse] =
    getChildren(childrenHierarchy)
      .flatMap(fetchContext)
      .find(checkRequestedParams(_, params))

  private def getChildren(childrenHierarchy: String): Seq[String] =
    Try(ujson.read(childrenHierarchy).arrOpt)
      .toOption
      .flatten
      .map(_.map(c => c("childId").str).toSeq)
      .getOrElse(Seq.empty)

  private def createBaseEdfRequest(): HttpEntity[Any] = {
    val edfToken = clientTokenManager.fetchEdfToken()
    val headers = new HttpHeaders()
    headers.setBearerAuth(edfToken)
    new HttpEntity(headers)
  }

  private def createSslRequestFactory(trustStore: Resource, trustStorePassword: String) = (trustStore, trustStorePassword) match {
    case _ if isSslEnabled(trustStore, trustStorePassword) =>
      logger.debug(s"Creating EDF Rest Template with SSL enabled to $edfEndpoint")
      val sslContext = new SSLContextBuilder().loadTrustMaterial(trustStore.getURL, trustStorePassword.toCharArray).build()
      val tlsStrategy = new DefaultClientTlsStrategy(sslContext)
      val connectionManager = PoolHttpClientConnectionManagerBuilder.create().setTlsSocketStrategy(tlsStrategy).build()
      val httpClient = HttpClients.custom().setConnectionManager(connectionManager).build()
      new HttpComponentsClientHttpRequestFactory(httpClient)
    case _ => restTemplateBuilder.buildRequestFactory()
  }

  private def isSslEnabled(trustStore: Resource, trustStorePassword: String): Boolean =
    trustStore != null && trustStorePassword != null
}