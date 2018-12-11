package gov.usgs.earthquake.nshmp.www;

import static com.google.common.base.Preconditions.checkArgument;
import static gov.usgs.earthquake.nshmp.ResponseSpectra.spectra;
import static gov.usgs.earthquake.nshmp.gmm.GmmInput.Field.VSINF;
import static gov.usgs.earthquake.nshmp.gmm.Imt.AI;
import static gov.usgs.earthquake.nshmp.gmm.Imt.PGV;
import static gov.usgs.earthquake.nshmp.www.Util.RequestKey.BODY;
import static gov.usgs.earthquake.nshmp.www.Util.RequestKey.HTTP_EVENT;
import static gov.usgs.earthquake.nshmp.www.Util.RequestKey.HTTP_GET;
import static gov.usgs.earthquake.nshmp.www.Util.RequestKey.HTTP_POST;
import static gov.usgs.earthquake.nshmp.www.Util.RequestKey.MULTI_QUERY_STRING_PARAMETERS;
import static gov.usgs.earthquake.nshmp.www.Util.RequestKey.QUERY_STRING_PARAMETERS;
import static gov.usgs.earthquake.nshmp.www.Util.RequestKey.SERVICE;
import static gov.usgs.earthquake.nshmp.www.Util.ResponseKey.STATUS_CODE;
import static gov.usgs.earthquake.nshmp.www.Util.ResponseKey.STATUS_CODE_ERROR;
import static gov.usgs.earthquake.nshmp.www.Util.ResponseKey.STATUS_CODE_OK;
import static gov.usgs.earthquake.nshmp.www.meta.Metadata.errorMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.google.common.base.Enums;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import gov.usgs.earthquake.nshmp.GroundMotions;
import gov.usgs.earthquake.nshmp.GroundMotions.DistanceResult;
import gov.usgs.earthquake.nshmp.ResponseSpectra.MultiResult;
import gov.usgs.earthquake.nshmp.data.Data;
import gov.usgs.earthquake.nshmp.data.XySequence;
import gov.usgs.earthquake.nshmp.gmm.Gmm;
import gov.usgs.earthquake.nshmp.gmm.GmmInput;
import gov.usgs.earthquake.nshmp.gmm.GmmInput.Builder;
import gov.usgs.earthquake.nshmp.gmm.GmmInput.Constraints;
import gov.usgs.earthquake.nshmp.gmm.GmmInput.Field;
import gov.usgs.earthquake.nshmp.gmm.Imt;
import gov.usgs.earthquake.nshmp.internal.Parsing;
import gov.usgs.earthquake.nshmp.internal.Parsing.Delimiter;
import gov.usgs.earthquake.nshmp.www.Util.LambdaHelper;
import gov.usgs.earthquake.nshmp.www.meta.EnumParameter;
import gov.usgs.earthquake.nshmp.www.meta.ParamType;
import gov.usgs.earthquake.nshmp.www.meta.Status;
import gov.usgs.earthquake.nshmp.www.meta.Util;

public class GmmServicesLambda implements RequestStreamHandler {

  private static final Gson GSON;

  private static final String GMM_KEY = "gmm";
  private static final String RMIN_KEY = "rMin";
  private static final String RMAX_KEY = "rMax";
  private static final String IMT_KEY = "imt";
  private static final int ROUND = 5;

  static {
    GSON = new GsonBuilder()
        .setPrettyPrinting()
        .serializeNulls()
        .disableHtmlEscaping()
        .registerTypeAdapter(Double.class, new Util.NaNSerializer())
        .registerTypeAdapter(Parameters.class, new Parameters.Serializer())
        .registerTypeAdapter(Imt.class, new Util.EnumSerializer<Imt>())
        .create();
  }

  /**
   * Handle the AWS Lambda function request for GMM services: response spectra,
   * ground motion vs. distance, and hanging wall effects.
   */
  @Override
  public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
    LambdaHelper lambdaHelper = new LambdaHelper(input, output, context);

    try {
      lambdaHelper.logger.log("\n Request: \n" + GSON.toJson(lambdaHelper.requestJson) + "\n");

      JsonObject parameters;
      JsonObject requestJson = lambdaHelper.requestJson;
      
      if (requestJson.has(HTTP_EVENT) && HTTP_GET.equals(requestJson.get(HTTP_EVENT).getAsString())) {
        parameters = parseHttpGetEvent(lambdaHelper);
        handleGetEvent(parameters, lambdaHelper);
      } else if (requestJson.has(HTTP_EVENT) && HTTP_POST.equals(requestJson.get(HTTP_EVENT).getAsString())) {
       parameters = parseHttpPostEvent(lambdaHelper); 
       handlePostEvent(parameters, lambdaHelper);
      } else  {
        throw new RuntimeException("Event not supported");
      }
      
    } catch (Exception e) {
      JsonObject responseJson = new JsonObject();
      String message = errorMessage("", e, false);
      responseJson.addProperty(STATUS_CODE, STATUS_CODE_ERROR);
      responseJson.addProperty(BODY, message);
      lambdaHelper.logger.log("\n Error: \n" + message + "\n");
      lambdaHelper.logger.log("\n Stack Trace: \n" + Throwables.getStackTraceAsString(e) + "\n");

      lambdaHelper.writer.write(responseJson.toString());
      lambdaHelper.writer.close();
    }

  }

  /* Handle the Lambda trigger event and write results */
  private static void handleGetEvent(JsonObject parameters, LambdaHelper lambdaHelper) throws IOException {
    JsonObject responseJson = new JsonObject();

    Service service = getService(parameters);

    if (parameters.has(GMM_KEY)) {
      ResponseData response = processRequest(parameters, service);
      responseJson.addProperty(BODY, GSON.toJson(response));
    } else {
      Metadata usage = new Metadata(service);
      responseJson.addProperty(BODY, GSON.toJson(usage));
    }

    responseJson.addProperty(STATUS_CODE, STATUS_CODE_OK);

    lambdaHelper.writer.write(responseJson.toString());
    lambdaHelper.writer.close();
    lambdaHelper.logger.log("\n\n Response: \n" + responseJson.get(BODY).getAsString() + "\n\n");
  }
  
  private static void handlePostEvent(JsonObject parameters, LambdaHelper lambdaHelper) throws IOException {
    JsonObject responseJson = new JsonObject();
    Service service = getService(parameters);
    
    if (parameters.has(GMM_KEY) && parameters.has(BODY)) {
      List<ResponseData> gmmResponses = getPostResponses(parameters, service);
      ResponseDataPost svcResponse = new ResponseDataPost(gmmResponses, service);
      responseJson.addProperty(BODY, GSON.toJson(svcResponse));
    } else {
      Metadata usage = new Metadata(service);
      responseJson.addProperty(BODY, GSON.toJson(usage));
    }
    
    responseJson.addProperty(STATUS_CODE, STATUS_CODE_OK);

    lambdaHelper.writer.write(responseJson.toString());
    lambdaHelper.writer.close();
    lambdaHelper.logger.log("\n\n Response: \n" + responseJson.get(BODY).getAsString() + "\n\n");
  }
  
  private static List<ResponseData> getPostResponses(JsonObject parameters, Service service) {
    String body = parameters.get(BODY).getAsString();
    List<String> requestData = Splitter.on("\n").omitEmptyStrings().trimResults().splitToList(body);

    List<String> keys = Parsing.splitToList(requestData.subList(0, 1).get(0), Delimiter.COMMA);
    List<String> values = requestData.subList(1, requestData.size());
    
    List<ResponseData> gmmResponses = values.parallelStream() 
        .filter((line) -> !line.startsWith("#") && !line.trim().isEmpty())
        .map((line) -> {
          List<String> lineValues = Parsing.splitToList(line, Delimiter.COMMA);
         
          JsonObject requestJson = new JsonObject();
          requestJson.add(GMM_KEY, parameters.get(GMM_KEY));
          
          int index = 0;

          for (String key : keys) {
            String value = lineValues.get(index);
            if ("null".equals(value.toLowerCase())) continue;
           
            requestJson.addProperty(key, value);
            index++;
          }
         
          return processRequest(requestJson, service);
        })
        .collect(Collectors.toList());
    
    return gmmResponses;
  }

  /* Process the request */
  private static ResponseData processRequest(JsonObject parameters, Service service) {
    ResponseData svcResponse = null;

    switch (service) {
      case DISTANCE:
      case HW_FW:
        svcResponse = processRequestDistance(parameters, service);
        break;
      case SPECTRA:
        svcResponse = processRequestSpectra(parameters, service);
        break;
      default:
        throw new IllegalStateException("Service not supported [" + service + "]");
    }

    return svcResponse;
  }

  /* Get ground motion vs. distance or hanging wall effects results*/
  private static ResponseData processRequestDistance(JsonObject parameters, Service service) {

    boolean isLogSpace = service.equals(Service.DISTANCE) ? true : false;
    Imt imt = Imt.valueOf(parameters.get(IMT_KEY).getAsString());
    double rMin = parameters.get(RMIN_KEY).getAsDouble();
    double rMax = parameters.get(RMAX_KEY).getAsDouble();

    RequestDataDistance request = new RequestDataDistance(parameters, imt.toString(), rMin, rMax);

    DistanceResult result = GroundMotions.distanceGroundMotions(
        request.gmms,
        request.input,
        imt,
        request.minDistance,
        request.maxDistance,
        isLogSpace);

    ResponseData response = new ResponseData(service, request);
    response.setXY(result.distance, result.means, result.sigmas);

    return response;
  }

  /* Get response spectra results */
  private static ResponseData processRequestSpectra(JsonObject params, Service service) {

    RequestData request = new RequestData(params);
    MultiResult result = spectra(request.gmms, request.input, false);

    ResponseData response = new ResponseData(service, request);
    response.setXY(result.periods, result.means, result.sigmas);

    return response;
  }

  /* Construct a JsonObject of the query parameters from the HTTP request */
  private static JsonObject parseHttpPostEvent(LambdaHelper lambdaHelper) {
    JsonObject queryParameters = parseHttpGetEvent(lambdaHelper);
    
    if (lambdaHelper.requestJson.has(BODY)) {
      queryParameters.add(BODY, lambdaHelper.requestJson.get(BODY));
    }
    
    return queryParameters;
  }
  
  /* Construct a JsonObject of the query parameters from the HTTP request */
  private static JsonObject parseHttpGetEvent(LambdaHelper lambdaHelper) {
    JsonObject queryParameters = getHttpParameters(lambdaHelper.requestJson);
    JsonObject multiValueQueryParameters = getHttpMultiValueParameters(lambdaHelper.requestJson);
    
    if (multiValueQueryParameters.has(GMM_KEY)) {
      queryParameters.remove(GMM_KEY);
      queryParameters.add(GMM_KEY, multiValueQueryParameters.get(GMM_KEY));
    }
    
    return queryParameters;
  }
  
  /* Get query parameters from HTTP request: GMM inputs */
  private static JsonObject getHttpParameters(JsonObject requestJson) {
    JsonObject params;

    if (requestJson.has(QUERY_STRING_PARAMETERS)) {
      params = requestJson.get(QUERY_STRING_PARAMETERS).getAsJsonObject();
    } else {
      throw new RuntimeException("No query parameters");
    }

    return params;
  }

  /* Get multi-value query parameters from HTTP request: GMMs */
  private static JsonObject getHttpMultiValueParameters(JsonObject requestJson) {
    JsonObject params;

    if (requestJson.has(MULTI_QUERY_STRING_PARAMETERS)) {
      params = requestJson.get(MULTI_QUERY_STRING_PARAMETERS).getAsJsonObject();
    } else {
      throw new RuntimeException("No multi value query parameters");
    }

    return params;
  }

  /* Get the type of service to run */
  private static Service getService(JsonObject requestJson) {
    Service service = null;

    if (!requestJson.has(SERVICE)) {
      throw new RuntimeException("Service not defined");
    }

    String serviceCheck = requestJson.get(SERVICE).getAsString();

    switch (serviceCheck) {
      case "distance":
        service = Service.DISTANCE;
        break;
      case "hw-fw":
        service = Service.HW_FW;
        break;
      case "spectra":
        service = Service.SPECTRA;
        break;
      default:
        throw new IllegalStateException("Unsupported service [" + serviceCheck + "]");
    }

    return service;
  }

  /* Create a set of GMMs from the request parameters */
  private static Set<Gmm> buildGmmSet(JsonObject parameters) {
    checkArgument(parameters.has(GMM_KEY),
        "Missing ground motion model key: " + GMM_KEY);

    return Sets.newEnumSet(
        FluentIterable
            .from(GSON.fromJson(parameters.get(GMM_KEY), String[].class))
            .transform(Enums.stringConverter(Gmm.class)),
        Gmm.class);
  }

  /* Build a GmmInput from the request parameters */
  static GmmInput buildInput(JsonObject parameters) {

    Builder builder = GmmInput.builder().withDefaults();
    for (Entry<String, JsonElement> entry : parameters.entrySet()) {
      if (entry.getKey().equals(GMM_KEY) || entry.getKey().equals(IMT_KEY) ||
          entry.getKey().equals(RMAX_KEY) || entry.getKey().equals(RMIN_KEY) ||
          entry.getKey().equals(SERVICE))
        continue;

      Field id = Field.fromString(entry.getKey());
      String value = entry.getValue().getAsString();
      
      if (value.equals("")) {
        continue;
      }
      
      builder.set(id, value);
    }

    return builder.build();
  }

  /* Request data for response spectra */
  private static class RequestData {
    Set<Gmm> gmms;
    GmmInput input;

    RequestData(JsonObject requestJson) {
      gmms = buildGmmSet(requestJson);
      input = buildInput(requestJson);
    }
  }

  /* Request data for ground motion vs. distance and hanging wall effects */
  private static class RequestDataDistance extends RequestData {
    String imt;
    double minDistance;
    double maxDistance;

    RequestDataDistance(
        JsonObject params,
        String imt,
        double rMin,
        double rMax) {

      super(params);

      this.imt = imt;
      minDistance = rMin;
      maxDistance = rMax;
    }
  }

  /* Response data for a HTTP post */
  private static class ResponseDataPost {
    String name;
    String status;
    String date;
    String url;
    Object server;
    List<ResponseData> response;

    ResponseDataPost(List<ResponseData> response, Service service) {
      name = service.resultName;
      status = Status.SUCCESS.toString();
      this.response = response;
      date = ZonedDateTime.now().format(ServletUtil.DATE_FMT);
      server = gov.usgs.earthquake.nshmp.www.meta.Metadata.serverData(1, ServletUtil.timer());
    }
  }

  /* Response data for result */
  @SuppressWarnings("unused")
  private static class ResponseData {
    String name;
    String status;
    String date; 
    String url;
    Object server;
    RequestData request;
    XY_DataGroup means;
    XY_DataGroup sigmas;

    ResponseData(Service service, RequestData request) {
      name = service.resultName;
      status = Status.SUCCESS.toString(); 
      date = ZonedDateTime.now().format(ServletUtil.DATE_FMT); 
      server = gov.usgs.earthquake.nshmp.www.meta.Metadata.serverData(1, ServletUtil.timer());

      this.request = request;

      means = XY_DataGroup.create(
          service.groupNameMean,
          service.xLabel,
          service.yLabelMedian);

      sigmas = XY_DataGroup.create(
          service.groupNameSigma,
          service.xLabel,
          service.yLabelSigma);
    }

    void setXY(
        Map<Gmm, List<Double>> x,
        Map<Gmm, List<Double>> means,
        Map<Gmm, List<Double>> sigmas) {

      for (Gmm gmm : means.keySet()) {
        XySequence xyMeans = XySequence.create(
            x.get(gmm),
            Data.round(ROUND, Data.exp(new ArrayList<>(means.get(gmm)))));
        this.means.add(gmm.name(), gmm.toString(), xyMeans);

        XySequence xySigmas = XySequence.create(
            x.get(gmm),
            Data.round(ROUND, new ArrayList<>(sigmas.get(gmm))));
        this.sigmas.add(gmm.name(), gmm.toString(), xySigmas);
      }
    }

  }

  @SuppressWarnings("unused")
  private static final class Metadata {

    String status = Status.USAGE.toString();
    String description;
    String syntax;
    Parameters parameters;

    Metadata(Service service) {
      this.syntax = "%s://%s/nshmp-haz-ws/gmm" + service.pathInfo + "?";
      this.description = service.description;
      this.parameters = new Parameters(service);
    }
  }

  /*
   * Placeholder class; all parameter serialization is done via the custom
   * Serializer. Service reference needed serialize().
   */
  static final class Parameters {

    private final Service service;

    Parameters(Service service) {
      this.service = service;
    }

    static final class Serializer implements JsonSerializer<Parameters> {

      @Override
      public JsonElement serialize(
          Parameters meta,
          Type type,
          JsonSerializationContext context) {

        JsonObject root = new JsonObject();

        if (!meta.service.equals(Service.SPECTRA)) {
          Set<Imt> imtSet = EnumSet.complementOf(EnumSet.range(PGV, AI));
          final EnumParameter<Imt> imts;
          imts = new EnumParameter<>(
              "Intensity measure type",
              ParamType.STRING,
              imtSet);
          root.add(IMT_KEY, context.serialize(imts));
        }

        /* Serialize input fields. */
        Constraints defaults = Constraints.defaults();
        for (Field field : Field.values()) {
          Param param = createGmmInputParam(field, defaults.get(field));
          JsonElement fieldElem = context.serialize(param);
          root.add(field.id, fieldElem);
        }

        /* Add only add those Gmms that belong to a Group. */
        List<Gmm> gmms = Arrays.stream(Gmm.Group.values())
            .flatMap(group -> group.gmms().stream())
            .sorted(Comparator.comparing(Object::toString))
            .distinct()
            .collect(Collectors.toList());

        GmmParam gmmParam = new GmmParam(
            GMM_NAME,
            GMM_INFO,
            gmms);
        root.add(GMM_KEY, context.serialize(gmmParam));

        /* Add gmm groups. */
        GroupParam groups = new GroupParam(
            GROUP_NAME,
            GROUP_INFO,
            EnumSet.allOf(Gmm.Group.class));
        root.add(GROUP_KEY, context.serialize(groups));

        return root;
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static Param createGmmInputParam(
      Field field,
      Optional<?> constraint) {
    return (field == VSINF) ? new BooleanParam(field)
        : new NumberParam(field, (Range<Double>) constraint.get());
  }

  /*
   * Marker interface for spectra parameters. This was previously implemented as
   * an abstract class for label, info, and units, but Gson serialized subclass
   * fields before parent fields. To maintain a preferred order, one can write
   * custom serializers or repeat these four fields in each implementation.
   */
  private static interface Param {}

  @SuppressWarnings("unused")
  private static final class NumberParam implements Param {

    final String label;
    final String info;
    final String units;
    final Double min;
    final Double max;
    final Double value;

    NumberParam(GmmInput.Field field, Range<Double> constraint) {
      this(field, constraint, field.defaultValue);
    }

    NumberParam(GmmInput.Field field, Range<Double> constraint, Double value) {
      this.label = field.label;
      this.info = field.info;
      this.units = field.units.orElse(null);
      this.min = constraint.lowerEndpoint();
      this.max = constraint.upperEndpoint();
      this.value = Doubles.isFinite(value) ? value : null;
    }
  }

  @SuppressWarnings("unused")
  private static final class BooleanParam implements Param {

    final String label;
    final String info;
    final boolean value;

    BooleanParam(GmmInput.Field field) {
      this(field, field.defaultValue == 1.0);
    }

    BooleanParam(GmmInput.Field field, boolean value) {
      this.label = field.label;
      this.info = field.info;
      this.value = value;
    }
  }

  private static final String GMM_NAME = "Ground Motion Models";
  private static final String GMM_INFO = "Empirical models of ground motion";

  @SuppressWarnings("unused")
  private static class GmmParam implements Param {

    final String label;
    final String info;
    final List<Value> values;

    GmmParam(String label, String info, List<Gmm> gmms) {
      this.label = label;
      this.info = info;
      this.values = gmms.stream()
          .map(gmm -> new Value(gmm))
          .collect(Collectors.toList());
    }

    private static class Value {

      final String id;
      final String label;
      final ArrayList<String> supportedImts;

      Value(Gmm gmm) {
        this.id = gmm.name();
        this.label = gmm.toString();
        this.supportedImts = SupportedImts(gmm.supportedIMTs());
      }
    }

    private static ArrayList<String> SupportedImts(Set<Imt> imts) {
      ArrayList<String> supportedImts = new ArrayList<>();

      for (Imt imt : imts) {
        supportedImts.add(imt.name());
      }

      return supportedImts;
    }

  }

  private static final String GROUP_KEY = "group";
  private static final String GROUP_NAME = "Ground Motion Model Groups";
  private static final String GROUP_INFO = "Groups of related ground motion models ";

  @SuppressWarnings("unused")
  private static final class GroupParam implements Param {

    final String label;
    final String info;
    final List<Value> values;

    GroupParam(String label, String info, Set<Gmm.Group> groups) {
      this.label = label;
      this.info = info;
      this.values = new ArrayList<>();
      for (Gmm.Group group : groups) {
        this.values.add(new Value(group));
      }
    }

    private static class Value {

      final String id;
      final String label;
      final List<Gmm> data;

      Value(Gmm.Group group) {
        this.id = group.name();
        this.label = group.toString();
        this.data = group.gmms();
      }
    }
  }

  private static enum Service {

    DISTANCE(
        "Ground Motion Vs. Distance",
        "Compute ground motion Vs. distance",
        "/distance",
        "Means",
        "Sigmas",
        "Distance (km)",
        "Median ground motion (g)",
        "Standard deviation"),

    HW_FW(
        "Hanging Wall Effect",
        "Compute hanging wall effect on ground motion Vs. distance",
        "/hw-fw",
        "Means",
        "Sigmas",
        "Distance (km)",
        "Median ground motion (g)",
        "Standard deviation"),

    SPECTRA(
        "Deterministic Response Spectra",
        "Compute deterministic response spectra",
        "/spectra",
        "Means",
        "Sigmas",
        "Period (s)",
        "Median ground motion (g)",
        "Standard deviation");

    final String name;
    final String description;
    final String pathInfo;
    final String resultName;
    final String groupNameMean;
    final String groupNameSigma;
    final String xLabel;
    final String yLabelMedian;
    final String yLabelSigma;

    private Service(
        String name, String description,
        String pathInfo, String groupNameMean,
        String groupNameSigma, String xLabel,
        String yLabelMedian, String yLabelSigma) {
      this.name = name;
      this.description = description;
      this.resultName = name + " Results";
      this.pathInfo = pathInfo;
      this.groupNameMean = groupNameMean;
      this.groupNameSigma = groupNameSigma;
      this.xLabel = xLabel;
      this.yLabelMedian = yLabelMedian;
      this.yLabelSigma = yLabelSigma;
    }

  }

}
