/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * you may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.carbon.aws.user.store.mgt.util;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.wso2.carbon.aws.user.store.mgt.AWSConstants;
import org.wso2.carbon.user.api.RealmConfiguration;
import org.wso2.carbon.user.core.UserStoreException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Provides REST API operations to connect with Amazon Cloud Directory.
 */
public class AWSRestApiActions {

    private static final Log log = LogFactory.getLog(AWSRestApiActions.class);
    // Host header value.
    private String hostHeader;
    // Region which is used select a regional endpoint to make requests.
    private String region;
    // AWS access key ID.
    private String accessKeyID;
    // AWS secret access key.
    private String secretAccessKey;
    // The Amazon Resource Name (ARN) of the directory.
    private String directoryArn;
    // Schema arn of the directory.
    private String schemaArn;
    // Base uri to build the canonicalURI.
    private String baseURI;

    public AWSRestApiActions(RealmConfiguration realmConfig) {

        region = realmConfig.getUserStoreProperty(AWSConstants.REGION);
        // Cloud directory API version.
        String apiVersion = realmConfig.getUserStoreProperty(AWSConstants.API_VERSION);
        hostHeader = AWSConstants.SERVICE + "." + region + AWSConstants.AMAZON_AWS_COM;
        accessKeyID = realmConfig.getUserStoreProperty(AWSConstants.ACCESS_KEY_ID);
        secretAccessKey = realmConfig.getUserStoreProperty(AWSConstants.SECRET_ACCESS_KEY);
        directoryArn = realmConfig.getUserStoreProperty(AWSConstants.DIRECTORY_ARN);
        schemaArn = realmConfig.getUserStoreProperty(AWSConstants.SCHEMA_ARN);
        baseURI = AWSConstants.AMAZON_CLOUD_DIRECTORY + apiVersion;
    }

    /**
     * Lists directories that was created within an AWS account.
     *
     * @param nextToken The pagination token.
     * @return List of directories.
     * @throws UserStoreException If error occurred.
     */
    public JSONObject listDirectories(String nextToken) throws UserStoreException {

        if (log.isDebugEnabled()) {
            log.debug(String.format("Listing all directories in AWS cloud with directoryArn: %s and schemaArn: %s.",
                    directoryArn, schemaArn));
        }
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        String canonicalURI = baseURI + AWSConstants.LIST_DIRECTORIES;
        String payload = buildPayloadToListDirectories(nextToken).toJSONString();
        if (log.isDebugEnabled()) {
            log.debug(String.format("Payload to list directories : %s ", payload));
        }
        HttpPost httpPost = preparePostHeaders(canonicalURI, awsHeaders, payload);
        HTTPResponse result = getHttpPostResults(httpPost);
        int statusCode = result.statusCode;
        JSONObject responseObject = result.responseObject;
        if (statusCode == HttpStatus.SC_OK) {
            return responseObject;
        } else {
            handleException(String.format("Error occured while listing directories. " + AWSConstants.RESPONSE,
                    responseObject.toJSONString(), statusCode));
        }
        return null;
    }

    /**
     * Returns a paginated list of all the outgoing TypedLinkSpecifier information for an object.
     *
     * @param typedLinkName   Name of the typed link.
     * @param objectReference The reference that identifies the object in the directory structure.
     * @return Returns outgoing typed link specifiers as output.
     * @throws UserStoreException If error occurred.
     */
    public JSONObject listOutgoingTypedLinks(String typedLinkName, String objectReference) throws UserStoreException {

        if (log.isDebugEnabled()) {
            log.debug(String.format("Getting all the outgoing TypedLinkSpecifier information for an object: %s.",
                    objectReference));
        }
        String canonicalURI = baseURI + AWSConstants.LIST_OUTGOING_TYPEDLINK;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);

        String payload = buildPayloadToGetTypedLink(typedLinkName, objectReference).toJSONString();
        if (log.isDebugEnabled()) {
            log.debug(String.format("Payload to get outgoing TypedLinkSpecifier information : %s ", payload));
        }
        HttpPost httpPost = preparePostHeaders(canonicalURI, awsHeaders, payload);
        httpPost.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);
        HTTPResponse result = getHttpPostResults(httpPost);
        int statusCode = result.statusCode;
        JSONObject responseObject = result.responseObject;
        if (statusCode == HttpStatus.SC_OK) {
            return responseObject;
        } else {
            handleException(String.format("Error occured while getting outgoing TypedLinkSpecifier for object %s. "
                    + AWSConstants.RESPONSE, objectReference, responseObject.toJSONString(), statusCode));
        }
        return null;
    }

    /**
     * Returns a paginated list of all the incoming TypedLinkSpecifier information for an object.
     *
     * @param facetName Name of the facet.
     * @param selector  Path of the object in the directory structure.
     * @return Returns incoming typed link specifiers as output.
     * @throws UserStoreException If error occurred.
     */
    public JSONObject listIncomingTypedLinks(String facetName, String selector) throws UserStoreException {

        if (log.isDebugEnabled()) {
            log.debug(String.format("Getting all the incoming TypedLinkSpecifier information for an object: %s.",
                    selector));
        }
        String canonicalURI = baseURI + AWSConstants.LIST_INCOMING_TYPEDLINK;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);
        String payload = buildPayloadToGetTypedLink(facetName, selector).toJSONString();
        if (log.isDebugEnabled()) {
            log.debug(String.format("Payload to get incoming TypedLinkSpecifier information : %s ", payload));
        }

        HttpPost httpPost = preparePostHeaders(canonicalURI, awsHeaders, payload);
        httpPost.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);
        HTTPResponse result = getHttpPostResults(httpPost);
        int statusCode = result.statusCode;
        JSONObject responseObject = result.responseObject;
        if (statusCode == HttpStatus.SC_OK) {
            return responseObject;
        } else {
            handleException(String.format("Error occured while getting incoming TypedLinkSpecifier for object %s. "
                    + AWSConstants.RESPONSE, selector, responseObject.toJSONString(), statusCode));
        }
        return null;
    }

    /**
     * Get facet information.
     *
     * @param facetName Facet name.
     * @return facet Info.
     * @throws UserStoreException If error occurred.
     */
    public JSONObject getFacetInfo(String facetName) throws UserStoreException {

        if (log.isDebugEnabled()) {
            log.debug(String.format("Get facet information for facetName: %s.", facetName));
        }
        String canonicalURI = baseURI + AWSConstants.FACET;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.PARTITION_HEADER, schemaArn);

        String payload = "{\"Name\": \"" + facetName + "\"}";
        if (log.isDebugEnabled()) {
            log.debug(String.format("Payload to get facet information : %s ", payload));
        }
        HttpPost httpPost = preparePostHeaders(canonicalURI, awsHeaders, payload);
        httpPost.setHeader(AWSConstants.PARTITION_HEADER, schemaArn);
        HTTPResponse result = getHttpPostResults(httpPost);
        int statusCode = result.statusCode;
        JSONObject responseObject = result.responseObject;
        if (statusCode == HttpStatus.SC_OK) {
            return responseObject;
        }
        return null;
    }

    /**
     * Get typed link facet information.
     *
     * @param typedLinkFacetName Typed link facet name.
     * @return facet info.
     * @throws UserStoreException If error occurred.
     */
    public JSONObject getTypedLinkFacetInformation(String typedLinkFacetName) throws UserStoreException {

        if (log.isDebugEnabled()) {
            log.debug(String.format("Get typed link facet information for typedLinkFacetName: %s.",
                    typedLinkFacetName));
        }
        String canonicalURI = baseURI + AWSConstants.GET_TYPED_LINK_FACET;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.PARTITION_HEADER, schemaArn);

        JSONObject payload = new JSONObject();
        payload.put(AWSConstants.NAME, typedLinkFacetName);

        if (log.isDebugEnabled()) {
            log.debug(String.format("Payload to get typed link facet information : %s ", payload));
        }
        HttpPost httpPost = preparePostHeaders(canonicalURI, awsHeaders, payload.toJSONString());
        httpPost.setHeader(AWSConstants.PARTITION_HEADER, schemaArn);
        HTTPResponse result = getHttpPostResults(httpPost);
        int statusCode = result.statusCode;
        JSONObject responseObject = result.responseObject;
        if (statusCode == HttpStatus.SC_OK) {
            return responseObject;
        }
        return null;
    }

    /**
     * Returns a paginated list of child objects that are associated with a given object.
     *
     * @param nextToken The pagination token.
     * @param selector  A path selector selection of an object by the parent/child links.
     * @return List of child objects.
     * @throws UserStoreException If error occurred.
     */
    public JSONObject listObjectChildren(String nextToken, String selector) throws UserStoreException {

        if (log.isDebugEnabled()) {
            log.debug(String.format("Listing the child objects that are associated with a given object. " +
                    "ObjectReference : %s", selector));
        }
        String canonicalURI = baseURI + AWSConstants.LIST_OBJECT_CHILDREN;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);
        awsHeaders.put(AWSConstants.CONSISTENCY_LEVEL_HEADER, AWSConstants.SERIALIZABLE);

        String payload = buildPayloadToListObjectChildren(nextToken, selector).toJSONString();
        if (log.isDebugEnabled()) {
            log.debug(String.format("Payload to list the child objects of a given object : %s ", payload));
        }
        HttpPost httpPost = preparePostHeaders(canonicalURI, awsHeaders, payload);
        httpPost.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);
        httpPost.setHeader(AWSConstants.CONSISTENCY_LEVEL_HEADER, AWSConstants.SERIALIZABLE);

        HTTPResponse result = getHttpPostResults(httpPost);
        int statusCode = result.statusCode;
        JSONObject responseObject = result.responseObject;
        if (statusCode == HttpStatus.SC_OK) {
            return responseObject;
        } else {
            handleException(String.format("Error occured while listing the child objects of a given object. " +
                            "ObjectReference : %s. " + AWSConstants.RESPONSE, selector, responseObject.toJSONString(),
                    statusCode));
        }
        return null;
    }

    /**
     * Deletes an object and its associated attributes.
     *
     * @param selector A path selector selection of an object by the parent/child links.
     * @throws UserStoreException If error occurred.
     */
    public void deleteObject(String selector) throws UserStoreException {

        if (log.isDebugEnabled()) {
            log.debug(String.format("Deleting an object with objectReference %s.", selector));
        }
        String canonicalURI = baseURI + AWSConstants.DELETE_OBJECT;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);

        HashMap<String, String> objectPath = new HashMap<>();
        JSONObject payload = new JSONObject();
        objectPath.put(AWSConstants.SELECTOR, selector);
        payload.put(AWSConstants.REFERENCE, objectPath);

        if (log.isDebugEnabled()) {
            log.debug(String.format("Payload to delete an object : %s ", payload));
        }

        HttpPut httpPut = preparePutHeaders(canonicalURI, awsHeaders, payload.toJSONString());
        httpPut.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);
        HTTPResponse result = getHttpPutResults(httpPut);
        int statusCode = result.statusCode;
        JSONObject responseObject = result.responseObject;
        if (statusCode == HttpStatus.SC_OK) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Successfully deleted object. Response : %s", responseObject));
            }
        } else {
            handleException(String.format("Error occured while delete an object %s. " + AWSConstants.RESPONSE, selector,
                    responseObject.toJSONString(), statusCode));
        }
    }

    /**
     * Updates a given object's attributes.
     *
     * @param action          The action to perform when updating the attribute.
     * @param facetName       Name of the facet.
     * @param objectReference The reference that identifies the object in the directory structure.
     * @param map             List of properties to build the payload.
     * @throws UserStoreException If error occurred.
     */
    public void updateObjectAttributes(String action, String facetName, String objectReference, Map<String, String> map)
            throws UserStoreException {

        if (log.isDebugEnabled()) {
            log.debug(String.format("Updating a given object's attributes of object: %s.", objectReference));
        }
        String canonicalURI = baseURI + AWSConstants.UPDATE_OBJECT;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);
        String payload = buildPayloadToUpdateObjectAttributes(action, facetName, objectReference, map).toJSONString();
        if (log.isDebugEnabled()) {
            log.debug(String.format("Payload to update a given object's attributes : %s ", payload));
        }
        HttpPut httpPut = preparePutHeaders(canonicalURI, awsHeaders, payload);
        httpPut.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);

        HTTPResponse result = getHttpPutResults(httpPut);
        int statusCode = result.statusCode;
        JSONObject responseObject = result.responseObject;
        if (statusCode == HttpStatus.SC_OK) {
            if (log.isDebugEnabled()) {
                log.debug("Successfully updated object's attributes");
            }
        } else {
            handleException(String.format("Error occured while update a given object's attributes. ObjectReference: %s"
                    + AWSConstants.RESPONSE, objectReference, responseObject.toJSONString(), statusCode));
        }
    }

    /**
     * Detaches a typed link from a specified source and target object.
     *
     * @param payload Payload to detach type link.
     * @return Statuscode for this action.
     * @throws UserStoreException If error occurred.
     */
    public int detachTypedLink(String payload) throws UserStoreException {

        if (log.isDebugEnabled()) {
            log.debug(String.format("Detaching a typed link from a specified source and target object in directory %s.",
                    directoryArn));
        }
        String canonicalURI = baseURI + AWSConstants.DETACH_TYPEDLINK;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);
        if (log.isDebugEnabled()) {
            log.debug(String.format("Payload to detach a typed link from a specified source and target object : %s ",
                    payload));
        }
        HttpPut httpPut = preparePutHeaders(canonicalURI, awsHeaders, payload);
        int statusCode = 0;
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            httpPut.setHeader(AWSConstants.HOST_HEADER, hostHeader);
            httpPut.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);
            httpPut.setEntity(new StringEntity(payload, AWSConstants.UTF_8));
            if (log.isDebugEnabled()) {
                log.debug("Invoking HTTP request to detach a typed link from a specified source and target object.");
            }
            statusCode = httpClient.execute(httpPut).getStatusLine().getStatusCode();
            if (log.isDebugEnabled() && statusCode == HttpStatus.SC_OK) {
                log.debug("Successfully detach a typed link from a specified source and target object");
            }
        } catch (IOException e) {
            handleException(AWSConstants.ERROR_WHILE_READING_RESPONSE, e);
        }
        return statusCode;
    }

    /**
     * Performs write operations in a batch.
     *
     * @param payload Payload to detach type link.
     * @throws UserStoreException If error occurred.
     */
    public void batchWrite(String payload) throws UserStoreException {

        if (log.isDebugEnabled()) {
            log.debug("Calling batch write operation");
        }
        String canonicalURI = baseURI + AWSConstants.BATCH_WRITE;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);
        if (log.isDebugEnabled()) {
            log.debug(String.format("Payload for batch write operation : %s ", payload));
        }
        HttpPut httpPut = preparePutHeaders(canonicalURI, awsHeaders, payload);
        httpPut.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);
        HTTPResponse result = getHttpPutResults(httpPut);
        int statusCode = result.statusCode;
        JSONObject responseObject = result.responseObject;
        if (statusCode == HttpStatus.SC_OK) {
            if (log.isDebugEnabled()) {
                log.debug("Successfully executed batch write operation.");
            }
        } else {
            handleException(String.format("Error occurred while performing batch write operation: . "
                    + AWSConstants.RESPONSE, responseObject.toJSONString(), statusCode));
        }
    }

    /**
     * Detaches a given object from the parent object.
     *
     * @param linkName        Name of the link.
     * @param parentReference The parent reference to which this object will be attached.
     * @return The ObjectIdentifier that was detached from the object.
     * @throws UserStoreException If error occurred.
     */
    public JSONObject detachObject(String linkName, String parentReference) throws UserStoreException {

        if (log.isDebugEnabled()) {
            log.debug(String.format("Detaching a given object from the parent object: %s.", parentReference));
        }
        String canonicalURI = baseURI + AWSConstants.DETACH_OBJECT;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);
        String payload = buildPayloadToDetachObject(linkName, parentReference).toJSONString();
        if (log.isDebugEnabled()) {
            log.debug(String.format("Payload to detach a given object from the parent object : %s ", payload));
        }
        HttpPut httpPut = preparePutHeaders(canonicalURI, awsHeaders, payload);
        httpPut.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);

        HTTPResponse result = getHttpPutResults(httpPut);
        int statusCode = result.statusCode;
        JSONObject responseObject = result.responseObject;
        if (statusCode == HttpStatus.SC_OK) {
            return responseObject;
        } else {
            handleException(String.format("Error occured while detach a given object from the parent object : %s. "
                    + AWSConstants.RESPONSE, parentReference, responseObject.toJSONString(), statusCode));
        }
        return null;
    }

    /**
     * Retrieves metadata about an object.
     *
     * @param selector A path selector selection of an object by the parent/child links.
     * @return Object infomation.
     * @throws UserStoreException If error occurred.
     */
    public JSONObject getObjectInformation(String selector) throws UserStoreException {

        if (log.isDebugEnabled()) {
            log.debug(String.format("Retrieving meta data about an object with objectReference %s.", selector));
        }
        String canonicalURI = baseURI + AWSConstants.GET_OBJECT_INFORMATION;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);
        awsHeaders.put(AWSConstants.CONSISTENCY_LEVEL_HEADER, AWSConstants.SERIALIZABLE);

        HashMap<String, String> objectPath = new HashMap<>();
        JSONObject payload = new JSONObject();
        objectPath.put(AWSConstants.SELECTOR, selector);
        payload.put(AWSConstants.REFERENCE, objectPath);

        if (log.isDebugEnabled()) {
            log.debug(String.format("Payload to Retrieve metadata about an object : %s ", payload));
        }
        HttpPost httpPost = preparePostHeaders(canonicalURI, awsHeaders, payload.toJSONString());
        httpPost.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);
        httpPost.setHeader(AWSConstants.CONSISTENCY_LEVEL_HEADER, AWSConstants.SERIALIZABLE);

        HTTPResponse result = getHttpPostResults(httpPost);
        int statusCode = result.statusCode;
        JSONObject responseObject = result.responseObject;
        if (statusCode == HttpStatus.SC_OK) {
            return responseObject;
        }
        return null;
    }

    /**
     * Lists all attributes that are associated with an object.
     *
     * @param facetName       Name of the facet.
     * @param objectReference The reference that identifies the object in the directory structure.
     * @return Object attributes.
     * @throws UserStoreException If error occurred.
     */
    public JSONObject listObjectAttributes(String facetName, String objectReference) throws UserStoreException {

        if (log.isDebugEnabled()) {
            log.debug(String.format("Listing all attributes of an object: %s.", objectReference));
        }
        String canonicalURI = baseURI + AWSConstants.LIST_OBJECT_ATTRIBUTES;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);
        awsHeaders.put(AWSConstants.CONSISTENCY_LEVEL_HEADER, AWSConstants.SERIALIZABLE);
        String payload = buildPayloadToListObjectAttributes(facetName, objectReference).toJSONString();
        if (log.isDebugEnabled()) {
            log.debug(String.format("Payload to list all attributes of an object : %s ", payload));
        }
        HttpPost httpPost = preparePostHeaders(canonicalURI, awsHeaders, payload);
        httpPost.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);
        httpPost.setHeader(AWSConstants.CONSISTENCY_LEVEL_HEADER, AWSConstants.SERIALIZABLE);

        HTTPResponse result = getHttpPostResults(httpPost);
        int statusCode = result.statusCode;
        JSONObject responseObject = result.responseObject;
        if (statusCode == HttpStatus.SC_OK) {
            return responseObject;
        } else {
            handleException(String.format("Error occured while list all attributes of an object: %s. " +
                    AWSConstants.RESPONSE, objectReference, responseObject.toJSONString(), statusCode));
        }
        return null;
    }

    /**
     * Attaches a typed link to a specified source and target object.
     *
     * @param sourceSelector The reference that identifies the source object in the directory structure.
     * @param targetSelector The reference that identifies the target object in the directory structure.
     * @param facetName      Name of the facet.
     * @param map            List of properties to build the payload.
     * @throws UserStoreException If error occurred.
     */
    public void attachTypedLink(String sourceSelector, String targetSelector, String facetName, Map<String, String> map)
            throws UserStoreException {

        if (log.isDebugEnabled()) {
            log.debug("Attaching a typed link to a specified source and target object.");
        }
        String canonicalURI = baseURI + AWSConstants.ATTACH_TYPEDLINK;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);
        String payload =
                buildPayloadToGetAttachTypedLink(sourceSelector, targetSelector, facetName, map).toJSONString();
        if (log.isDebugEnabled()) {
            log.debug(String.format("Payload to attach a typed link to a specified source and target object : %s ",
                    payload));
        }
        HttpPut httpPut = preparePutHeaders(canonicalURI, awsHeaders, payload);
        httpPut.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);

        HTTPResponse result = getHttpPutResults(httpPut);
        int statusCode = result.statusCode;
        JSONObject responseObject = result.responseObject;
        if (statusCode == HttpStatus.SC_OK) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Successfully attached a typed link. Response : %s",
                        responseObject.toJSONString()));
            }
        } else {
            handleException(String.format("Error occured while attach a typed link to a specified source and " +
                    "target object." + AWSConstants.RESPONSE, responseObject.toJSONString(), statusCode));
        }
    }

    /**
     * Creates a TypedLinkFacet.
     *
     * @param facetName  Name of the facet.
     * @param attributes List of attributes to build the payload.
     * @throws UserStoreException If error occurred.
     */
    public void createTypedLinkFacet(String facetName, List attributes) throws UserStoreException {

        if (log.isDebugEnabled()) {
            log.debug(String.format("Creating a TypedLinkFacet %s in schema %s.", facetName, schemaArn));
        }
        String canonicalURI = baseURI + AWSConstants.CREATE_TYPEDLINK;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.PARTITION_HEADER, schemaArn);
        String payload = buildPayloadToGetTypedLinkFacet(facetName, attributes).toJSONString();
        if (log.isDebugEnabled()) {
            log.debug(String.format("Payload to create a TypedLinkFacet : %s ", payload));
        }
        HttpPut httpPut = preparePutHeaders(canonicalURI, awsHeaders, payload);
        httpPut.setHeader(AWSConstants.PARTITION_HEADER, schemaArn);

        HTTPResponse result = getHttpPutResults(httpPut);
        int statusCode = result.statusCode;
        JSONObject responseObject = result.responseObject;
        if (statusCode == HttpStatus.SC_OK) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("TypedLinkFacet is created successfully. Response : %s ",
                        responseObject.toJSONString()));
            }
        } else {
            handleException(String.format("Error occured while create a TypedLinkFacet. " + AWSConstants.RESPONSE,
                    responseObject.toJSONString(), statusCode));
        }
    }

    /**
     * Creates an object in a Directory.
     *
     * @param linkName        Name of the link.
     * @param facetName       Name of the facet.
     * @param parentReference The parent reference to which this object will be attached.
     * @param map             List of properties to build the payload.
     * @throws UserStoreException If error occurred.
     */
    public void createObject(String linkName, String facetName, String parentReference, Map<String, String> map)
            throws UserStoreException {

        if (log.isDebugEnabled()) {
            log.debug(String.format("Creating an object in a directory: %s with link Name %s.",
                    directoryArn, linkName));
        }
        String canonicalURI = baseURI + AWSConstants.CREATE_OBJECT;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);
        String payload = buildPayloadToCreateObject(linkName, facetName, parentReference, map).toJSONString();
        if (log.isDebugEnabled()) {
            log.debug(String.format("Payload to create an object in a directory : %s ", payload));
        }
        HttpPut httpPut = preparePutHeaders(canonicalURI, awsHeaders, payload);
        httpPut.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);

        HTTPResponse result = getHttpPutResults(httpPut);
        int statusCode = result.statusCode;
        JSONObject responseObject = result.responseObject;
        if (statusCode == HttpStatus.SC_OK) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Object is created successfull with ObjectIdentifier %s ",
                        responseObject.get("ObjectIdentifier")));
            }
        } else {
            handleException(String.format("Error occured while create an object in a directory %s. " +
                    AWSConstants.RESPONSE, directoryArn, responseObject.toJSONString(), statusCode));
        }
    }

    /**
     * Creates a new Facet in a schema.
     *
     * @param facetName Name of the facet.
     * @param map       List of properties to build the payload.
     * @throws UserStoreException If error occurred.
     */
    public void createSchemaFacet(String facetName, Map<String, String> map) throws UserStoreException {

        if (log.isDebugEnabled()) {
            log.debug(String.format("Creating a new Facet in a schema %s .", schemaArn));
        }
        String canonicalURI = baseURI + AWSConstants.CREATE_FACET;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.PARTITION_HEADER, schemaArn);
        String facetPayload = buildPayloadToCreateSchemaFacet(facetName, map).toJSONString();
        if (log.isDebugEnabled()) {
            log.debug(String.format("Payload to create a new facet in a schema : %s ", facetPayload));
        }
        HttpPut httpPut = preparePutHeaders(canonicalURI, awsHeaders, facetPayload);
        httpPut.setHeader(AWSConstants.PARTITION_HEADER, schemaArn);

        HTTPResponse result = getHttpPutResults(httpPut);
        int statusCode = result.statusCode;
        JSONObject responseObject = result.responseObject;
        if (statusCode == HttpStatus.SC_OK) {
            if (log.isDebugEnabled()) {
                log.debug("Schema facet is created successfully. Response Object : " + responseObject.toJSONString());
            }
        } else {
            handleException(String.format("Error occured while create a new facet in a schema %s. " +
                    AWSConstants.RESPONSE, schemaArn, responseObject.toJSONString(), statusCode));
        }
    }

    /**
     * Generate payload to list object children .
     *
     * @param nextToken The pagination token.
     * @param selector  Path of the object in the directory structure.
     * @return Payload.
     */
    private JSONObject buildPayloadToListObjectChildren(String nextToken, String selector) {

        HashMap<String, String> path = new HashMap<>();
        path.put(AWSConstants.SELECTOR, selector);
        JSONObject response = new JSONObject();
        response.put(AWSConstants.MAX_RESULTS, AWSConstants.MAX_API_LIMIT);
        response.put(AWSConstants.REFERENCE, path);
        if (StringUtils.isNotEmpty(nextToken)) {
            response.put(AWSConstants.NEXT_TOKEN, nextToken);
        }
        return response;
    }

    /**
     * Generate payload to list directories .
     *
     * @param nextToken The pagination token.
     * @return Payload.
     */
    private JSONObject buildPayloadToListDirectories(String nextToken) {

        JSONObject response = new JSONObject();
        response.put(AWSConstants.MAX_RESULTS, AWSConstants.MAX_API_LIMIT);
        response.put(AWSConstants.STATE, AWSConstants.ENABLED);
        if (StringUtils.isNotEmpty(nextToken)) {
            response.put(AWSConstants.NEXT_TOKEN, nextToken);
        }
        return response;
    }

    /**
     * Generate payload to attach typed link.
     *
     * @param sourceSelector The reference that identifies the source object in the directory structure.
     * @param targetSelector The reference that identifies the target object in the directory structure.
     * @param facetName      Name of the facet.
     * @param map            List of properties to build the payload.
     * @return Payload.
     */
    private JSONObject buildPayloadToGetAttachTypedLink(String sourceSelector, String targetSelector, String facetName,
                                                        Map<String, String> map) {

        JSONArray attributes = new JSONArray();
        JSONObject response = new JSONObject();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            HashMap<String, Object> attribute = new HashMap<>();
            HashMap<String, Object> value = new HashMap<>();
            attribute.put(AWSConstants.ATTRIBUTE_NAME, entry.getKey());
            value.put(AWSConstants.STRING_VALUE, entry.getValue());
            attribute.put(AWSConstants.VALUE, value);
            attributes.add(attribute);
        }
        response.put(AWSConstants.ATTRIBUTES, attributes);
        HashMap sourcePath = new HashMap<>();
        sourcePath.put(AWSConstants.SELECTOR, sourceSelector);
        response.put(AWSConstants.SOURCE_REFERENCE, sourcePath);
        HashMap targetPath = new HashMap<>();
        targetPath.put(AWSConstants.SELECTOR, targetSelector);
        response.put(AWSConstants.TARGET_REFERENCE, targetPath);
        HashMap linkFacet = new HashMap<>();
        linkFacet.put(AWSConstants.SCHEMA_ARN, schemaArn);
        linkFacet.put(AWSConstants.TYPED_LINK_NAME, facetName);
        response.put(AWSConstants.TYPEDLINK_FACET, linkFacet);
        return response;
    }

    /**
     * Generate payload to list object attributes.
     *
     * @param facetName       Name of the facet.
     * @param objectReference The reference that identifies the object in the directory structure.
     * @return Payload.
     */
    private JSONObject buildPayloadToListObjectAttributes(String facetName, String objectReference) {

        HashMap<String, String> facetFilter = new HashMap<>();
        HashMap<String, String> path = new HashMap<>();
        JSONObject response = new JSONObject();
        facetFilter.put(AWSConstants.FACET_NAME, facetName);
        facetFilter.put(AWSConstants.SCHEMA_ARN, schemaArn);
        path.put(AWSConstants.SELECTOR, objectReference);
        response.put(AWSConstants.FACET_FILTER, facetFilter);
        response.put(AWSConstants.MAX_RESULTS, AWSConstants.MAX_API_LIMIT);
        response.put(AWSConstants.REFERENCE, path);
        return response;
    }

    /**
     * Generate payload to get typed link.
     *
     * @param typedLinkName   Name of the Typed Link.
     * @param objectReference The reference that identifies the object in the directory structure.
     * @return Payload.
     */
    private JSONObject buildPayloadToGetTypedLink(String typedLinkName, String objectReference) {

        HashMap<String, String> path = new HashMap<>();
        JSONObject response = new JSONObject();
        path.put(AWSConstants.SELECTOR, objectReference);
        if (typedLinkName != null) {
            HashMap<String, String> filterTypedLink = new HashMap<>();
            filterTypedLink.put(AWSConstants.SCHEMA_ARN, schemaArn);
            filterTypedLink.put(AWSConstants.TYPED_LINK_NAME, typedLinkName);
            response.put(AWSConstants.FILTER_TYPEDLINK, filterTypedLink);
        }
        response.put(AWSConstants.REFERENCE, path);
        return response;
    }

    /**
     * Generate payload to update objectAttributes.
     *
     * @param action          The action to perform when updating the attribute.
     * @param facetName       Name of the facet.
     * @param objectReference The reference that identifies the object in the directory structure.
     * @param map             List of properties to build the payload.
     * @return Payload.
     */
    public JSONObject buildPayloadToUpdateObjectAttributes(String action, String facetName, String objectReference,
                                                           Map<String, String> map) {

        JSONArray attributeUpdates = new JSONArray();
        JSONObject updateObjectAttribute = new JSONObject();
        HashMap<String, String> objectRef = new HashMap<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            HashMap<String, String> objectAttributeKey = new HashMap<>();
            HashMap<String, String> objectAttributeUpdateValue = new HashMap<>();
            HashMap<String, Object> objectAttributeAction = new HashMap<>();
            HashMap<String, Object> attributeUpdate = new HashMap<>();
            objectAttributeAction.put(AWSConstants.ATTRIBUTE_UPDATE_VALUE, objectAttributeUpdateValue);
            objectAttributeAction.put(AWSConstants.ATTRIBUTE_ACTION_TYPE, action);
            objectAttributeKey.put(AWSConstants.FACET_NAME, facetName);
            objectAttributeKey.put(AWSConstants.SCHEMA_ARN, schemaArn);
            attributeUpdate.put(AWSConstants.ATTRIBUTE_KEY, objectAttributeKey);
            attributeUpdate.put(AWSConstants.ATTRIBUTE_ACTION, objectAttributeAction);
            objectAttributeUpdateValue.put(AWSConstants.STRING_VALUE, entry.getValue());
            objectAttributeKey.put(AWSConstants.NAME, entry.getKey());
            attributeUpdates.add(attributeUpdate);
        }
        updateObjectAttribute.put(AWSConstants.ATTRIBUTE_UPDATES, attributeUpdates);
        objectRef.put(AWSConstants.SELECTOR, objectReference);
        updateObjectAttribute.put(AWSConstants.REFERENCE, objectRef);
        return updateObjectAttribute;
    }

    /**
     * Generate payload to create object.
     *
     * @param linkName        Name of the link.
     * @param facetName       Name of the facet.
     * @param parentReference The parent reference to which this object will be attached.
     * @param map             List of properties to build the payload.
     * @return Payload.
     */
    private JSONObject buildPayloadToCreateObject(String linkName, String facetName, String parentReference,
                                                  Map<String, String> map) {

        HashMap<String, String> path = new HashMap<>();
        HashMap<String, String> schemaFacet = new HashMap<>();
        JSONArray schemaFacets = new JSONArray();
        JSONObject response = new JSONObject();
        JSONArray list = new JSONArray();
        path.put(AWSConstants.SELECTOR, parentReference);
        schemaFacet.put(AWSConstants.FACET_NAME, facetName);
        schemaFacet.put(AWSConstants.SCHEMA_ARN, schemaArn);
        schemaFacets.add(schemaFacet);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            HashMap<String, String> key = new HashMap<>();
            HashMap<String, String> value = new HashMap<>();
            HashMap<String, Object> objectAttribute = new HashMap<>();
            key.put(AWSConstants.FACET_NAME, facetName);
            key.put(AWSConstants.NAME, entry.getKey());
            key.put(AWSConstants.SCHEMA_ARN, schemaArn);
            value.put(AWSConstants.STRING_VALUE, entry.getValue());
            objectAttribute.put(AWSConstants.KEY, key);
            objectAttribute.put(AWSConstants.VALUE, value);
            list.add(objectAttribute);
        }
        response.put(AWSConstants.LINK_NAME, linkName);
        response.put(AWSConstants.OBJECT_ATTRIBUTE_LIST, list);
        response.put(AWSConstants.PARENT_REFERENCE, path);
        response.put(AWSConstants.SCHEMA_FACET, schemaFacets);
        return response;
    }

    /**
     * Generate payload to get typedLink facet.
     *
     * @param facetName  Name of the facet.
     * @param attributes List of attributes to build the payload.
     * @return Payload.
     */
    private JSONObject buildPayloadToGetTypedLinkFacet(String facetName, List attributes) {

        JSONArray attributeList = new JSONArray();
        JSONObject response = new JSONObject();
        JSONArray order = new JSONArray();
        HashMap<String, Object> facet = new HashMap<>();
        for (Object key : attributes) {
            HashMap<String, Object> attribute = new HashMap<>();
            attribute.put(AWSConstants.NAME, key);
            attribute.put(AWSConstants.REQUIRED_BEHAVIOR, AWSConstants.REQUIRED_ALWAYS);
            attribute.put(AWSConstants.TYPE, AWSConstants.STRING);
            attributeList.add(attribute);
            order.add(key.toString());
        }
        facet.put(AWSConstants.ATTRIBUTES, attributeList);
        facet.put(AWSConstants.IDENTITY_ATTRIBUTE, order);
        facet.put(AWSConstants.NAME, facetName);
        response.put(AWSConstants.FACET_STR, facet);
        return response;
    }

    /**
     * Generate payload to detach object.
     *
     * @param linkName        Name of the link.
     * @param parentReference The parent reference to which this object will be attached.
     * @return Payload.
     */
    private JSONObject buildPayloadToDetachObject(String linkName, String parentReference) {

        HashMap<String, String> path = new HashMap<>();
        JSONObject response = new JSONObject();
        path.put(AWSConstants.SELECTOR, parentReference);
        response.put(AWSConstants.LINK_NAME, linkName);
        response.put(AWSConstants.PARENT_REFERENCE, path);
        return response;
    }

    /**
     * Generate payload to create schema facet.
     *
     * @param facetName Name of the facet.
     * @param map       List of properties to build the payload.
     * @return Payload.
     */
    private JSONObject buildPayloadToCreateSchemaFacet(String facetName, Map<String, String> map) {

        boolean isImmutable = false;
        if (StringUtils.equals(facetName, AWSConstants.GROUP)) {
            isImmutable = true;
        }
        HashMap<String, Object> attributeDefinition = new HashMap<>();
        HashMap<String, Object> attribute = new HashMap<>();
        JSONObject response = new JSONObject();
        attributeDefinition.put(AWSConstants.IS_IMMUTABLE, isImmutable);
        attributeDefinition.put(AWSConstants.TYPE, AWSConstants.STRING);
        attribute.put(AWSConstants.ATTRIBUTE_DEFINITION, attributeDefinition);

        for (Map.Entry<String, String> entry : map.entrySet()) {
            attribute.put(AWSConstants.NAME, entry.getKey());
            attribute.put(AWSConstants.REQUIRED_BEHAVIOR, entry.getValue());
        }
        JSONArray attributes = new JSONArray();
        attributes.add(attribute);
        response.put(AWSConstants.ATTRIBUTES, attributes);
        response.put(AWSConstants.NAME, facetName);
        response.put(AWSConstants.OBJECT_TYPE, AWSConstants.NODE);
        return response;
    }

    /**
     * Get the status code and response object after executing the post request.
     *
     * @param httpPost HttpPost
     * @return status code and response object
     * @throws UserStoreException If error occurred.
     */
    private HTTPResponse getHttpPostResults(HttpPost httpPost) throws UserStoreException {

        int statusCode = 0;
        JSONObject responseObject = null;

        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build();
             CloseableHttpResponse response = httpClient.execute(httpPost);
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(response.getEntity().getContent(), AWSConstants.UTF_8))
        ) {
            statusCode = response.getStatusLine().getStatusCode();
            responseObject = getParsedObjectByReader(reader);
        } catch (ParseException e) {
            handleException(AWSConstants.ERROR_WHILE_PARSING_RESPONSE, e);
        } catch (IOException e) {
            handleException(AWSConstants.ERROR_WHILE_READING_RESPONSE, e);
        }
        return new HTTPResponse(statusCode, responseObject);
    }

    /**
     * Prepare the http post request.
     *
     * @param canonicalURI Canonical uri of the request.
     * @param awsHeaders   Set of headers.
     * @param payload      Payload.
     * @return HttpPost request.
     * @throws UserStoreException If error occurred.
     */
    private HttpPost preparePostHeaders(String canonicalURI, TreeMap<String, String> awsHeaders, String payload)
            throws UserStoreException {

        HttpPost httpPost = new HttpPost(AWSConstants.HTTPS + hostHeader + canonicalURI);
        awsHeaders.put(AWSConstants.HOST_HEADER, hostHeader);
        AWSSignatureV4Generator aWSV4Auth = new AWSSignatureV4Generator.Builder(accessKeyID, secretAccessKey)
                .regionName(region)
                .serviceName(AWSConstants.SERVICE)
                .httpMethodName(AWSConstants.HTTP_POST)
                .canonicalURI(canonicalURI)
                .queryParametes(null)
                .awsHeaders(awsHeaders)
                .payload(payload)
                .build();

        /* Get header calculated for request */
        Map<String, String> header = aWSV4Auth.getHeaders();
        for (Map.Entry<String, String> entrySet : header.entrySet()) {
            httpPost.setHeader(entrySet.getKey(), entrySet.getValue());
        }
        try {
            httpPost.setHeader(AWSConstants.HOST_HEADER, hostHeader);
            httpPost.setEntity(new StringEntity(payload, AWSConstants.UTF_8));
        } catch (UnsupportedEncodingException e) {
            handleException(AWSConstants.ERROR_WHILE_CHARACTOR_ENCODING, e);
        }
        return httpPost;
    }

    /**
     * Get the status code and response object after executing the put request.
     *
     * @param httpPut HttpPut
     * @return status code and response object
     * @throws UserStoreException If error occurred.
     */
    private HTTPResponse getHttpPutResults(HttpPut httpPut) throws UserStoreException {

        int statusCode = 0;
        JSONObject responseObject = null;

        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build();
             CloseableHttpResponse response = httpClient.execute(httpPut);
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(response.getEntity().getContent(), AWSConstants.UTF_8))
        ) {
            statusCode = response.getStatusLine().getStatusCode();
            responseObject = getParsedObjectByReader(reader);
        } catch (ParseException e) {
            handleException(AWSConstants.ERROR_WHILE_PARSING_RESPONSE, e);
        } catch (IOException e) {
            handleException(AWSConstants.ERROR_WHILE_READING_RESPONSE, e);
        }
        return new HTTPResponse(statusCode, responseObject);
    }

    /**
     * Prepare the http put request.
     *
     * @param canonicalURI Canonical uri of the request.
     * @param awsHeaders   Set of headers.
     * @param payload      Payload.
     * @return HttpPut request.
     * @throws UserStoreException If error occurred.
     */
    private HttpPut preparePutHeaders(String canonicalURI, TreeMap<String, String> awsHeaders, String payload)
            throws UserStoreException {

        HttpPut httpPut = new HttpPut(AWSConstants.HTTPS + hostHeader + canonicalURI);
        awsHeaders.put(AWSConstants.HOST_HEADER, hostHeader);
        AWSSignatureV4Generator aWSV4Auth = new AWSSignatureV4Generator.Builder(accessKeyID, secretAccessKey)
                .regionName(region)
                .serviceName(AWSConstants.SERVICE)
                .httpMethodName(AWSConstants.HTTP_PUT)
                .canonicalURI(canonicalURI)
                .queryParametes(null)
                .awsHeaders(awsHeaders)
                .payload(payload)
                .build();

        /* Get header calculated for request */
        Map<String, String> header = aWSV4Auth.getHeaders();
        for (Map.Entry<String, String> entrySet : header.entrySet()) {
            httpPut.setHeader(entrySet.getKey(), entrySet.getValue());
        }
        try {
            httpPut.setHeader(AWSConstants.HOST_HEADER, hostHeader);
            httpPut.setEntity(new StringEntity(payload, AWSConstants.UTF_8));
        } catch (UnsupportedEncodingException e) {
            handleException(AWSConstants.ERROR_WHILE_CHARACTOR_ENCODING, e);
        }
        return httpPut;
    }

    /**
     * Can be used to parse {@code BufferedReader} object that are taken from response stream, to a {@code JSONObject}.
     *
     * @param reader {@code BufferedReader} object from response.
     * @return JSON payload as a name value map.
     * @throws ParseException Error while parsing response json.
     * @throws IOException    Error while reading response body
     */
    private JSONObject getParsedObjectByReader(BufferedReader reader) throws ParseException, IOException {

        JSONObject parsedObject = null;
        JSONParser parser = new JSONParser();
        if (reader != null) {
            parsedObject = (JSONObject) parser.parse(reader);
        }
        return parsedObject;
    }

    /**
     * Common method to throw exceptions.
     *
     * @param msg this parameter contain error message that we need to throw.
     * @param e   Exception object.
     * @throws UserStoreException If error occurred.
     */
    private void handleException(String msg, Exception e) throws UserStoreException {

        throw new UserStoreException(msg, e);
    }

    /**
     * Common method to throw exceptions. This will only expect one parameter.
     *
     * @param msg error message as a string.
     * @throws UserStoreException If error occurred.
     */
    private void handleException(String msg) throws UserStoreException {

        throw new UserStoreException(msg);
    }

    /**
     * This class provide the facility to get the HTTP response details such as statuscode and response object as Json.
     */
    public static class HTTPResponse {

        int statusCode;
        JSONObject responseObject;

        /**
         * It will return HTTPResponse Object.
         *
         * @param statusCode     Status code of the http response.
         * @param responseObject Response object as Json
         */
        HTTPResponse(int statusCode, JSONObject responseObject) {

            this.statusCode = statusCode;
            this.responseObject = responseObject;
        }
    }
}
