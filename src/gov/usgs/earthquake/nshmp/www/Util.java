package gov.usgs.earthquake.nshmp.www;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.ServletRequest;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import gov.usgs.earthquake.nshmp.internal.Parsing;
import gov.usgs.earthquake.nshmp.internal.Parsing.Delimiter;

public class Util {

  /**
   * Returns the value of a servlet request parameter as a double.
   * 
   * @param key of value to get
   * @param request servlet request
   */
  public static <E extends Enum<E>> double readDouble(E key, ServletRequest request) {
    return Double.valueOf(readValue(key, request));
  }

  /**
   * Returns the value of a servlet request parameter as an integer.
   * 
   * @param key of value to get
   * @param request servlet request
   */
  public static <E extends Enum<E>> int readInteger(E key, ServletRequest request) {
    return Integer.valueOf(readValue(key, request));
  }

  /**
   * Returns the value of a servlet request parameter as a string.
   * 
   * @param key of value to get
   * @param request servlet request
   */
  public static <E extends Enum<E>> String readValue(E key, ServletRequest request) {
    return readValues(key, request)[0];
  }

  /**
   * Returns the value of a servlet request parameter as a enum of specified
   * type.
   * 
   * @param key of value to get
   * @param request servlet request
   * @param type of enum to return
   */
  public static <T extends Enum<T>, E extends Enum<E>> T readValue(
      E key,
      ServletRequest request,
      Class<T> type) {
    return Enum.valueOf(type, readValue(key, request));
  }

  /**
   * Returns the value of a servlet request parameter as a string array.
   * 
   * @param key of value to get
   * @param request servlet request
   */
  public static <E extends Enum<E>> String[] readValues(E key, ServletRequest request) {
    return checkNotNull(
        request.getParameterValues(key.toString()),
        "Missing query key [" + key.toString() + "]");
  }

  /**
   * Returns the value of a servlet request parameter as a enum set of specified
   * type.
   * 
   * @param key of value to get
   * @param request servlet request
   * @param type of enum to return
   */
  public static <T extends Enum<T>, E extends Enum<E>> Set<T> readValues(
      E key,
      ServletRequest request,
      Class<T> type) {

    return Arrays.stream(readValues(key, request))
        .map((name) -> Enum.valueOf(type, name))
        .collect(Collectors.toSet());
  }

  /**
   * Parse the Lambda function {@code InputStream} into an {@code JsonObject}.
   */
  static class LambdaHelper {

    JsonObject requestJson;
    OutputStreamWriter writer;
    Context context;
    LambdaLogger logger;

    LambdaHelper(InputStream input, OutputStream output, Context context)
        throws UnsupportedEncodingException {

      BufferedReader reader = new BufferedReader(new InputStreamReader(input));
      JsonParser parser = new JsonParser();

      requestJson = parser.parse(reader).getAsJsonObject();
      writer = new OutputStreamWriter(output, "UTF-8");
      logger = context.getLogger();
      this.context = context;
    }
  }

  enum Key {
    EDITION,
    REGION,
    MODEL,
    VS30,
    LATITUDE,
    LONGITUDE,
    IMT,
    RETURNPERIOD,
    DISTANCE,
    FORMAT,
    TIMESPAN;

    private String label;

    private Key() {
      label = name().toLowerCase();
    }

    @Override
    public String toString() {
      return label;
    }
  }

  /**
   * Common request keys
   */
  static class RequestKey {
    static final String BODY = "body";
    static final String EVENT = "event";
    static final String INVOKE = "invoke";
    static final String INVOKE_BATCH = "invoke-batch";
    static final String QUERY_STRING_PARAMETERS = "queryStringParameters";
    static final String SERVICE = "service";
    static final String MULTI_QUERY_STRING_PARAMETERS = "multiValueQueryStringParameters";
    static final String HTTP_EVENT = "httpMethod";
    static final String HTTP_GET = "GET";
    static final String HTTP_POST = "POST";
  }

  /**
   * Common response keys
   */
  static class ResponseKey {
    static final String STATUS_CODE = "statusCode";
    static final String STATUS_CODE_OK = "200";
    static final String STATUS_CODE_ERROR = "400";
  }

  static <T extends Enum<T>> Set<T> readValues(String values, Class<T> type) {
    return Parsing.splitToList(values, Delimiter.COMMA).stream()
        .map((name) -> Enum.valueOf(type, name))
        .collect(Collectors.toSet());
  }

  static <E extends Enum<E>> String readValue(E key, Map<String, String[]> paramMap) {
    String keyStr = key.toString();
    String[] values = paramMap.get(keyStr);
    checkNotNull(values, "Missing query key: %s", keyStr);
    checkState(values.length > 0, "Empty value array for key: %s", key);
    return values[0];
  }

  static <T extends Enum<T>, E extends Enum<E>> T readValue(
      E key,
      Map<String, String[]> paramMap,
      Class<T> type) {
    return Enum.valueOf(type, readValue(key, paramMap));
  }

}
