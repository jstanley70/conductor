/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * 
 */
package com.netflix.conductor.contribs.http;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang3.StringUtils;
import org.ccctech.apigateway.conductor.tasks.CcctcHttpTask.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.oauth.client.OAuthClientFilter;
import com.sun.jersey.oauth.signature.OAuthParameters;
import com.sun.jersey.oauth.signature.OAuthSecrets;

/**
 * @author Viren
 * Task that enables calling another http endpoint as part of its execution
 * 
 * Basic support is to fail responses < 200 and > 299
 * If you desire additional support for any specific response code  
 * set map input_Parameter:status_support, keys are httpStatuses and value is TaskStatus to be set
 * Use a decision tree task after completion to handle specific httpStatuses
 * input_Parameter:alternate_workflow can be used to indicated default workflow for httpStatuses that are invoked.
 */
/**
 * @author jstanley 
 * 
 * Task updateds to allow http statuses (>200 and > 299)
 * to be handled with custom handlers.
 * 
 * Basic support remains the same to fail responses < 200 and > 299 (exactly the same
 * as orginally designed) Setting http_status_override values overrides default
 * failure for statuses outside the range.
 * 
 * If you desire additional support for any specific response code set
 * map input_parameter:status_support, keys are httpStatuses and value
 * is TaskStatus to be set. Use a decision tree task after completion to
 * handle specific httpStatuses.
 * 
 * outputParameters now include httpStatus and overrideActivated where overrideActivated
 * can be used to 
 * 
 * Example: erp task has type HTTP and sets http_status_support
 *          to 404:COMPLETE, 409:COMPLETE. If 404 or 409 is the returned http_status then
 *          overrideActivated is set to true. 
 *          A DECISION Task can be used to handle each case with a 
 *          unique following task/workflow. 
 *          OR
 *          Use the overrideActivated parameter to have all returned error conditions to
 *          use the same task/workflow.
 * 
 * Example code:
 * [
 * {
 *  "name": "http",
 *  "taskReferenceName": "erp",
 *     "type": "HTTP",
 *     "inputParameters": {
 *       "http_request": {
 *         "method": "GET",
 *         "accept": "application/json",
 *         "contentType": aplication/json",
 *         "headers": {
 *           "Authorization": {
 *           }
 *         },
 *         "uri": {
 *           
 *         }
 *       },
 *       "http_status_override": {
 *         "404": "COMPLETED",
 *         "409": "COMPLETED"
 *       }
 *     }
 *   }
 * ]
*/
@Singleton
public class HttpTask extends WorkflowSystemTask {

	public static final String REQUEST_PARAMETER_NAME = "http_request";
	public static final String HTTP_STATUS_OVERIDE_PARAMETER_NAME = "http_status_override";

	
	static final String MISSING_REQUEST = "Missing HTTP request. Task input MUST have a '" + REQUEST_PARAMETER_NAME + "' key with HttpTask.Input as value. See documentation for HttpTask for required input parameters";

	private static final Logger logger = LoggerFactory.getLogger(HttpTask.class);
	
	public static final String NAME = "HTTP";
	
	private TypeReference<Map<String, Object>> mapOfObj = new TypeReference<Map<String, Object>>(){};
	
	private TypeReference<List<Object>> listOfObj = new TypeReference<List<Object>>(){};
	
	protected ObjectMapper om = objectMapper();

	protected RestClientManager rcm;
	
	protected Configuration config;
	
	private String requestParameter;
	
	@Inject
	public HttpTask(RestClientManager rcm, Configuration config) {
		this(NAME, rcm, config);
	}
	
	public HttpTask(String name, RestClientManager rcm, Configuration config) {
		super(name);
		this.rcm = rcm;
		this.config = config;
		this.requestParameter = REQUEST_PARAMETER_NAME;
		logger.info("HttpTask initialized...");
	}
	
	@Override
	public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		Object request = task.getInputData().get(requestParameter);
		task.setWorkerId(config.getServerId());
		if(request == null) {
			String reason = MISSING_REQUEST;
			task.setReasonForIncompletion(reason);
			task.setStatus(Status.FAILED);
			return;
		}
		
		Input input = om.convertValue(request, Input.class);
		if(input.getUri() == null) {
			String reason = "Missing HTTP URI.  See documentation for HttpTask for required input parameters";
			task.setReasonForIncompletion(reason);
			task.setStatus(Status.FAILED);
			return;
		}
		
		if(input.getMethod() == null) {
			String reason = "No HTTP method specified";
			task.setReasonForIncompletion(reason);
			task.setStatus(Status.FAILED);
			return;
		}
		
		try {
			
			HttpResponse response = httpCall(input);
            if (handleOptionalResponse(task, response)) {
                return;
            } else {
			logger.info("response {}, {}", response.statusCode, response.body);
			if(response.statusCode > 199 && response.statusCode < 300) {
				task.setStatus(Status.COMPLETED);
			} else {
				if(response.body != null) {
					task.setReasonForIncompletion(response.body.toString());
				} else {
					task.setReasonForIncompletion("No response from the remote service");
				}
				task.setStatus(Status.FAILED);
			}
			
		}catch(Exception e) {
			logger.error(String.format("Failed to invoke http task - uri: %s, vipAddress: %s", input.getUri(), input.getVipAddress()), e);
			task.setStatus(Status.FAILED);
			task.setReasonForIncompletion("Failed to invoke http task due to: " + e.toString());
			task.getOutputData().put("response", e.toString());
		}
	}

	/**
	 * 
	 * @param input HTTP Request
	 * @return Response of the http call
	 * @throws Exception If there was an error making http call
	 */
	protected HttpResponse httpCall(Input input) throws Exception {
		Client client = rcm.getClient(input);

		if(input.oauthConsumerKey != null) {
			logger.info("Configuring OAuth filter");
			OAuthParameters params = new OAuthParameters().consumerKey(input.oauthConsumerKey).signatureMethod("HMAC-SHA1").version("1.0");
			OAuthSecrets secrets = new OAuthSecrets().consumerSecret(input.oauthConsumerSecret);
			client.addFilter(new OAuthClientFilter(client.getProviders(), params, secrets));
		}

		Builder builder = client.resource(input.uri).type(input.contentType);

		if(input.body != null) {
			builder.entity(input.body);
		}
		input.headers.entrySet().forEach(e -> {
			builder.header(e.getKey(), e.getValue());
		});
		
		HttpResponse response = new HttpResponse();
		try {

			ClientResponse cr = builder.accept(input.accept).method(input.method, ClientResponse.class);
			if (cr.getStatus() != 204 && cr.hasEntity()) {
				response.body = extractBody(cr);
			}
			response.statusCode = cr.getStatus();
			response.reasonPhrase = cr.getStatusInfo().getReasonPhrase();
			response.headers = cr.getHeaders();
			return response;

		} catch(UniformInterfaceException ex) {
			ClientResponse cr = ex.getResponse();
			logger.error(String.format("Got unexpected http response - uri: %s, vipAddress: %s, status code: %s", input.getUri(), input.getVipAddress(), cr.getStatus()), ex);
			if(cr.getStatus() > 199 && cr.getStatus() < 300) {
				if(cr.getStatus() != 204 && cr.hasEntity()) {
					response.body = extractBody(cr);
				}
				response.headers = cr.getHeaders();
				response.statusCode = cr.getStatus();
				response.reasonPhrase = cr.getStatusInfo().getReasonPhrase();
				return response;
			}else {
				String reason = cr.getEntity(String.class);
				logger.error(reason, ex);
				throw new Exception(reason);
			}
		}
	}

	private Object extractBody(ClientResponse cr) {

		String json = cr.getEntity(String.class);
		logger.info(json);
		
		try {
			
			JsonNode node = om.readTree(json);
			if (node.isArray()) {
				return om.convertValue(node, listOfObj);
			} else if (node.isObject()) {
				return om.convertValue(node, mapOfObj);
			} else if (node.isNumber()) {
				return om.convertValue(node, Double.class);
			} else {
				return node.asText();
			}

		} catch (IOException jpe) {
			logger.error(jpe.getMessage(), jpe);
			return json;
		}
	}
	
    protected boolean handleOptionalResponse(Task task, HttpResponse response) {
        Object statusSupport = task.getInputData().get(HTTP_STATUS_OVERIDE_PARAMETER_NAME);
        Map<String, String> statusSupportValues = om.convertValue(statusSupport, HashMap.class);
        if (response != null) {
            task.getOutputData().put("response", response.asMap());
            task.getOutputData().put("httpStatus", Integer.toString(response.statusCode));
            if(statusSupport != null) {
                if (statusSupportValues.containsKey(Integer.toString(response.statusCode))) {
                    Status status = Status.valueOf(statusSupportValues.get(Integer.toString(response.statusCode)));
                    task.setStatus(status);
                    task.getOutputData().put("overrideActivated", true);
                    return true;
                }
            }
            task.getOutputData().put("overrideActivated", false);
        }
        return false;
    }

	@Override
	public boolean execute(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		return false;
	}
	
	@Override
	public void cancel(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
		task.setStatus(Status.CANCELED);
	}
	
	@Override
	public boolean isAsync() {
		return true;
	}
	
	@Override
	public int getRetryTimeInSecond() {
		return 60;
	}
	
	private static ObjectMapper objectMapper() {
	    final ObjectMapper om = new ObjectMapper();
        om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        om.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        om.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        om.setSerializationInclusion(Include.NON_NULL);
        om.setSerializationInclusion(Include.NON_EMPTY);
	    return om;
	}
	
	public static class HttpResponse {
		
		public Object body;
		
		public MultivaluedMap<String, String> headers;
		
		public int statusCode;

		public String reasonPhrase;

		@Override
		public String toString() {
			return "HttpResponse [body=" + body + ", headers=" + headers + ", statusCode=" + statusCode + ", reasonPhrase=" + reasonPhrase + "]";
		}
		
		public Map<String, Object> asMap() {
			
			Map<String, Object> map = new HashMap<>();
			map.put("body", body);
			map.put("headers", headers);
			map.put("statusCode", statusCode);
			map.put("reasonPhrase", reasonPhrase);
			
			return map;
		}
	}
	
	public static class Input {
		
		private String method;	//PUT, POST, GET, DELETE, OPTIONS, HEAD
		
		private String vipAddress;
		
		private Map<String, Object> headers = new HashMap<>();
		
		private String uri;
		
		private Object body;
		
		private String accept = MediaType.APPLICATION_JSON;

		private String contentType = MediaType.APPLICATION_JSON;
		
		private String oauthConsumerKey;

		private String oauthConsumerSecret;

		/**
		 * @return the method
		 */
		public String getMethod() {
			return method;
		}

		/**
		 * @param method the method to set
		 */
		public void setMethod(String method) {
			this.method = method;
		}

		/**
		 * @return the headers
		 */
		public Map<String, Object> getHeaders() {
			return headers;
		}

		/**
		 * @param headers the headers to set
		 */
		public void setHeaders(Map<String, Object> headers) {
			this.headers = headers;
		}

		/**
		 * @return the body
		 */
		public Object getBody() {
			return body;
		}

		/**
		 * @param body the body to set
		 */
		public void setBody(Object body) {
			this.body = body;
		}

		/**
		 * @return the uri
		 */
		public String getUri() {
			return uri;
		}

		/**
		 * @param uri the uri to set
		 */
		public void setUri(String uri) {
			this.uri = uri;
		}

		/**
		 * @return the vipAddress
		 */
		public String getVipAddress() {
			return vipAddress;
		}

		/**
		 * @param vipAddress the vipAddress to set
		 * 
		 */
		public void setVipAddress(String vipAddress) {
			this.vipAddress = vipAddress;
		}

		/**
		 * @return the accept
		 */
		public String getAccept() {
			return accept;
		}

		/**
		 * @param accept the accept to set
		 * 
		 */
		public void setAccept(String accept) {
			this.accept = accept;
		}

		/**
		 * @return the MIME content type to use for the request
		 */
		public String getContentType() {
			return contentType;
		}

		/**
		 * @param contentType the MIME content type to set
		 */
		public void setContentType(String contentType) {
			this.contentType = contentType;
		}

		/**
		 * @return the OAuth consumer Key
		 */
		public String getOauthConsumerKey() {
			return oauthConsumerKey;
		}

		/**
		 * @param oauthConsumerKey the OAuth consumer key to set
		 */
		public void setOauthConsumerKey(String oauthConsumerKey) {
			this.oauthConsumerKey = oauthConsumerKey;
		}

		/**
		 * @return the OAuth consumer secret
		 */
		public String getOauthConsumerSecret() {
			return oauthConsumerSecret;
		}

		/**
		 * @param oauthConsumerSecret the OAuth consumer secret to set
		 */
		public void setOauthConsumerSecret(String oauthConsumerSecret) {
			this.oauthConsumerSecret = oauthConsumerSecret;
		}
	}
}
