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
 * @author jstanley 
 * 
 * CcctcHttpTask updated to allow http statuses (< 200 and > 299)
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
 *  "taskReferenceName": "google-api",
 *     "type": "CCCTC_HTTP",
 *     "inputParameters": {
 *       "http_request": {
 *         "method": "GET",
 *         "accept": "application/json",
 *         "contentType": aplication/json",
 *         "headers": {
 *           "Accept-Encoding": "gzip"
 *         },
 *         "uri": "https://www.googleapis.com/discovery/v1/apis/not_an_api/2/rest?fields=name%2Cvalu"
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
public class CcctcHttpTask extends HttpTask {

    public static final String HTTP_STATUS_OVERIDE_PARAMETER_NAME = "http_status_override";

    private static final Logger logger = LoggerFactory.getLogger(CcctcHttpTask.class);
    
    public static final String NAME = "CCCTC_HTTP";
    
    private TypeReference<Map<String, Object>> mapOfObj = new TypeReference<Map<String, Object>>(){};
    
    private TypeReference<List<Object>> listOfObj = new TypeReference<List<Object>>(){};
    
    private String requestParameter;
    
    @Inject
    public CcctcHttpTask(RestClientManager rcm, Configuration config) {
        this(NAME, rcm, config);
    }
    
    public CcctcHttpTask(String name, RestClientManager rcm, Configuration config) {
        super(name, rcm, config);
        this.requestParameter = HttpTask.REQUEST_PARAMETER_NAME;
        logger.info("CcctcHttpTask initialized...");
    }
    
    @Override
    public void start(Workflow workflow, Task task, WorkflowExecutor executor) throws Exception {
        Object request = task.getInputData().get(requestParameter);
        task.setWorkerId(config.getServerId());
        if(request == null) {
            String reason = HttpTask.MISSING_REQUEST;
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
            }
            
        }catch(Exception e) {
            logger.error(String.format("Failed to invoke http task - uri: %s, vipAddress: %s", input.getUri(), input.getVipAddress()), e);
            task.setStatus(Status.FAILED);
            task.setReasonForIncompletion("Failed to invoke http task due to: " + e.toString());
            task.getOutputData().put("response", e.toString());
        }
    }
    
    public boolean handleOptionalResponse(Task task, HttpResponse response) {
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
}