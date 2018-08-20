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
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.wso2.carbon.aws.user.store.mgt.AWSConstants;
import org.wso2.carbon.user.api.RealmConfiguration;
import org.wso2.carbon.user.core.UserStoreException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
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
    private boolean debug = log.isDebugEnabled();

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

        if (debug) {
            log.debug(String.format("Listing all directories in AWS cloud with directoryArn: %s and schemaArn: %s.",
                    directoryArn, schemaArn));
        }
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.HOST_HEADER, hostHeader);

        String canonicalURI = baseURI + AWSConstants.LIST_DIRECTORIES;
        String payload = buildPayloadTolistDirectories(nextToken);
        if (debug) {
            log.debug(String.format("Payload to list directories : %s ", payload));
        }
        AWSSignatureV4Generator aWSV4Auth = new AWSSignatureV4Generator.Builder(accessKeyID, secretAccessKey)
                .regionName(region)
                .serviceName(AWSConstants.SERVICE)
                .httpMethodName(AWSConstants.HTTP_POST)
                .canonicalURI(canonicalURI)
                .queryParametes(null)
                .awsHeaders(awsHeaders)
                .payload(payload)
                .build();

        HttpPost httpPost = new HttpPost(AWSConstants.HTTPS + hostHeader + canonicalURI);

        /* Get header calculated for request */
        Map<String, String> header = aWSV4Auth.getHeaders();
        for (Map.Entry<String, String> entrySet : header.entrySet()) {
            httpPost.setHeader(entrySet.getKey(), entrySet.getValue());
        }
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            httpPost.setHeader(AWSConstants.HOST_HEADER, hostHeader);
            httpPost.setEntity(new StringEntity(payload, AWSConstants.UTF_8));
            if (debug) {
                log.debug("Invoking HTTP request to list directories.");
            }
            HttpResponse response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                handleException(String.format(AWSConstants.STRING_FORMAT, AWSConstants.ERROR_COULD_NOT_READ_HTTP_ENTITY,
                        response));
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), AWSConstants.UTF_8));
            JSONObject responseObject = getParsedObjectByReader(reader);
            if (statusCode == HttpStatus.SC_OK) {
                return responseObject;
            } else {
                handleException(String.format("Error occured while list directories. " +
                        AWSConstants.RESPONSE, responseObject.toJSONString()));
            }
        } catch (ParseException e) {
            handleException(AWSConstants.ERROR_WHILE_PARSING_RESPONSE, e);
        } catch (IOException e) {
            handleException(AWSConstants.ERROR_WHILE_READING_RESPONSE, e);
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

        if (debug) {
            log.debug(String.format("Getting all the outgoing TypedLinkSpecifier information for an object: %s.",
                    objectReference));
        }
        String canonicalURI = baseURI + AWSConstants.LIST_OUTGOING_TYPEDLINK;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.HOST_HEADER, hostHeader);
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);
        String payload = buildPayloadToGetTypedLink(typedLinkName, objectReference);
        if (debug) {
            log.debug(String.format("Payload to get outgoing TypedLinkSpecifier information : %s ", payload));
        }
        AWSSignatureV4Generator aWSV4Auth = new AWSSignatureV4Generator.Builder(accessKeyID, secretAccessKey)
                .regionName(region)
                .serviceName(AWSConstants.SERVICE)
                .httpMethodName(AWSConstants.HTTP_POST)
                .canonicalURI(canonicalURI)
                .queryParametes(null)
                .awsHeaders(awsHeaders)
                .payload(payload)
                .build();

        HttpPost httpPost = new HttpPost(AWSConstants.HTTPS + hostHeader + canonicalURI);

        /* Get header calculated for request */
        Map<String, String> header = aWSV4Auth.getHeaders();
        for (Map.Entry<String, String> entrySet : header.entrySet()) {
            httpPost.setHeader(entrySet.getKey(), entrySet.getValue());
        }
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            httpPost.setHeader(AWSConstants.HOST_HEADER, hostHeader);
            httpPost.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);
            httpPost.setEntity(new StringEntity(payload, AWSConstants.UTF_8));
            if (debug) {
                log.debug("Invoking HTTP request to get outgoing TypedLinkSpecifier information.");
            }
            HttpResponse response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                handleException(String.format(AWSConstants.STRING_FORMAT,
                        AWSConstants.ERROR_COULD_NOT_READ_HTTP_ENTITY, response));
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), AWSConstants.UTF_8));
            JSONObject responseObject = getParsedObjectByReader(reader);
            if (statusCode == HttpStatus.SC_OK) {
                return responseObject;
            } else {
                handleException(String.format("Error occured while getting outgoing TypedLinkSpecifier for object %s. "
                        + AWSConstants.RESPONSE, objectReference, responseObject.toJSONString()));
            }
        } catch (ParseException e) {
            handleException(AWSConstants.ERROR_WHILE_PARSING_RESPONSE, e);
        } catch (IOException e) {
            handleException(AWSConstants.ERROR_WHILE_READING_RESPONSE, e);
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

        if (debug) {
            log.debug(String.format("Getting all the incoming TypedLinkSpecifier information for an object: %s.",
                    selector));
        }
        String canonicalURI = baseURI + AWSConstants.LIST_INCOMING_TYPEDLINK;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.HOST_HEADER, hostHeader);
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);
        String payload = buildPayloadToListIncomingTypedLinks(facetName, selector);
        if (debug) {
            log.debug(String.format("Payload to get incoming TypedLinkSpecifier information : %s ", payload));
        }
        AWSSignatureV4Generator aWSV4Auth = new AWSSignatureV4Generator.Builder(accessKeyID, secretAccessKey)
                .regionName(region)
                .serviceName(AWSConstants.SERVICE)
                .httpMethodName(AWSConstants.HTTP_POST)
                .canonicalURI(canonicalURI)
                .queryParametes(null)
                .awsHeaders(awsHeaders)
                .payload(payload)
                .build();

        HttpPost httpPost = new HttpPost(AWSConstants.HTTPS + hostHeader + canonicalURI);

        /* Get header calculated for request */
        Map<String, String> header = aWSV4Auth.getHeaders();
        for (Map.Entry<String, String> entrySet : header.entrySet()) {
            httpPost.setHeader(entrySet.getKey(), entrySet.getValue());
        }
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            httpPost.setHeader(AWSConstants.HOST_HEADER, hostHeader);
            httpPost.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);
            httpPost.setEntity(new StringEntity(payload, AWSConstants.UTF_8));
            if (debug) {
                log.debug("Invoking HTTP request to get incoming TypedLinkSpecifier information.");
            }
            HttpResponse response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                handleException(String.format(AWSConstants.STRING_FORMAT, AWSConstants.ERROR_COULD_NOT_READ_HTTP_ENTITY,
                        response));
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), AWSConstants.UTF_8));
            JSONObject responseObject = getParsedObjectByReader(reader);
            if (statusCode == HttpStatus.SC_OK) {
                return responseObject;
            } else {
                handleException(String.format("Error occured while getting incoming TypedLinkSpecifier for object %s. "
                        + AWSConstants.RESPONSE, selector, responseObject.toJSONString()));
            }
        } catch (ParseException e) {
            handleException(AWSConstants.ERROR_WHILE_PARSING_RESPONSE, e);
        } catch (IOException e) {
            handleException(AWSConstants.ERROR_WHILE_READING_RESPONSE, e);
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

        if (debug) {
            log.debug(String.format("Get facet information for facetName: %s.", facetName));
        }
        String canonicalURI = baseURI + AWSConstants.FACET;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.HOST_HEADER, hostHeader);
        awsHeaders.put(AWSConstants.PARTITION_HEADER, schemaArn);

        String payload = "{\"Name\": \"" + facetName + "\"}";
        if (debug) {
            log.debug(String.format("Payload to get facet information : %s ", payload));
        }
        AWSSignatureV4Generator aWSV4Auth = new AWSSignatureV4Generator.Builder(accessKeyID, secretAccessKey)
                .regionName(region)
                .serviceName(AWSConstants.SERVICE)
                .httpMethodName(AWSConstants.HTTP_POST)
                .canonicalURI(canonicalURI)
                .queryParametes(null)
                .awsHeaders(awsHeaders)
                .payload(payload)
                .build();

        HttpPost httpPost = new HttpPost(AWSConstants.HTTPS + hostHeader + canonicalURI);

        /* Get header calculated for request */
        Map<String, String> header = aWSV4Auth.getHeaders();
        for (Map.Entry<String, String> entrySet : header.entrySet()) {
            httpPost.setHeader(entrySet.getKey(), entrySet.getValue());
        }
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            httpPost.setHeader(AWSConstants.HOST_HEADER, hostHeader);
            httpPost.setHeader(AWSConstants.PARTITION_HEADER, schemaArn);
            httpPost.setEntity(new StringEntity(payload, AWSConstants.UTF_8));
            if (debug) {
                log.debug("Invoking HTTP request to get facet information.");
            }
            HttpResponse response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                handleException(String.format(AWSConstants.STRING_FORMAT, AWSConstants.ERROR_COULD_NOT_READ_HTTP_ENTITY,
                        response));
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), AWSConstants.UTF_8));
            JSONObject responseObject = getParsedObjectByReader(reader);
            if (statusCode == HttpStatus.SC_OK) {
                return responseObject;
            }
        } catch (ParseException e) {
            handleException(AWSConstants.ERROR_WHILE_PARSING_RESPONSE, e);
        } catch (IOException e) {
            handleException(AWSConstants.ERROR_WHILE_READING_RESPONSE, e);
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

        if (debug) {
            log.debug(String.format("Get typed link facet information for typedLinkFacetName: %s.",
                    typedLinkFacetName));
        }
        String canonicalURI = baseURI + AWSConstants.GET_TYPED_LINK_FACET;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.HOST_HEADER, hostHeader);
        awsHeaders.put(AWSConstants.PARTITION_HEADER, schemaArn);

        String payload = "{\"Name\": \"" + typedLinkFacetName + "\"}";
        if (debug) {
            log.debug(String.format("Payload to get typed link facet information : %s ", payload));
        }
        AWSSignatureV4Generator aWSV4Auth = new AWSSignatureV4Generator.Builder(accessKeyID, secretAccessKey)
                .regionName(region)
                .serviceName(AWSConstants.SERVICE)
                .httpMethodName(AWSConstants.HTTP_POST)
                .canonicalURI(canonicalURI)
                .queryParametes(null)
                .awsHeaders(awsHeaders)
                .payload(payload)
                .build();

        HttpPost httpPost = new HttpPost(AWSConstants.HTTPS + hostHeader + canonicalURI);

        /* Get header calculated for request */
        Map<String, String> header = aWSV4Auth.getHeaders();
        for (Map.Entry<String, String> entrySet : header.entrySet()) {
            httpPost.setHeader(entrySet.getKey(), entrySet.getValue());
        }
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            httpPost.setHeader(AWSConstants.HOST_HEADER, hostHeader);
            httpPost.setHeader(AWSConstants.PARTITION_HEADER, schemaArn);
            httpPost.setEntity(new StringEntity(payload, AWSConstants.UTF_8));
            if (debug) {
                log.debug("Invoking HTTP request to get typed link facet information.");
            }
            HttpResponse response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                handleException(String.format(AWSConstants.STRING_FORMAT, AWSConstants.ERROR_COULD_NOT_READ_HTTP_ENTITY,
                        response));
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), AWSConstants.UTF_8));
            JSONObject responseObject = getParsedObjectByReader(reader);
            if (statusCode == HttpStatus.SC_OK) {
                return responseObject;
            }
        } catch (ParseException e) {
            handleException(AWSConstants.ERROR_WHILE_PARSING_RESPONSE, e);
        } catch (IOException e) {
            handleException(AWSConstants.ERROR_WHILE_READING_RESPONSE, e);
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

        if (debug) {
            log.debug(String.format("Listing the child objects that are associated with a given object. " +
                    "ObjectReference : %s", selector));
        }
        String canonicalURI = baseURI + AWSConstants.LIST_OBJECT_CHILDREN;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.HOST_HEADER, hostHeader);
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);
        awsHeaders.put(AWSConstants.CONSISTENCY_LEVEL_HEADER, AWSConstants.SERIALIZABLE);

        String payload = buildPayloadTolistObjectChildren(nextToken, selector);
        if (debug) {
            log.debug(String.format("Payload to list the child objects of a given object : %s ", payload));
        }
        AWSSignatureV4Generator aWSV4Auth = new AWSSignatureV4Generator.Builder(accessKeyID, secretAccessKey)
                .regionName(region)
                .serviceName(AWSConstants.SERVICE)
                .httpMethodName(AWSConstants.HTTP_POST)
                .canonicalURI(canonicalURI)
                .queryParametes(null)
                .awsHeaders(awsHeaders)
                .payload(payload)
                .build();

        HttpPost httpPost = new HttpPost(AWSConstants.HTTPS + hostHeader + canonicalURI);

        /* Get header calculated for request */
        Map<String, String> header = aWSV4Auth.getHeaders();
        for (Map.Entry<String, String> entrySet : header.entrySet()) {
            httpPost.setHeader(entrySet.getKey(), entrySet.getValue());
        }
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            httpPost.setHeader(AWSConstants.HOST_HEADER, hostHeader);
            httpPost.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);
            httpPost.setHeader(AWSConstants.CONSISTENCY_LEVEL_HEADER, AWSConstants.SERIALIZABLE);
            httpPost.setEntity(new StringEntity(payload, AWSConstants.UTF_8));
            if (debug) {
                log.debug("Invoking HTTP request to list the child objects of a given object.");
            }
            HttpResponse response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                handleException(String.format(AWSConstants.STRING_FORMAT, AWSConstants.ERROR_COULD_NOT_READ_HTTP_ENTITY,
                        response));
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), AWSConstants.UTF_8));
            JSONObject responseObject = getParsedObjectByReader(reader);
            if (statusCode == HttpStatus.SC_OK) {
                return responseObject;
            } else {
                handleException(String.format("Error occured while listing the child objects of a given object. " +
                        "ObjectReference : %s, Response : %s", selector, responseObject.toJSONString()));
            }
        } catch (ParseException e) {
            handleException(AWSConstants.ERROR_WHILE_PARSING_RESPONSE, e);
        } catch (IOException e) {
            handleException(AWSConstants.ERROR_WHILE_READING_RESPONSE, e);
        }
        return null;
    }

    /**
     * Deletes an object and its associated attributes.
     *
     * @param selector A path selector selection of an object by the parent/child links.
     * @return Status code.
     * @throws UserStoreException If error occurred.
     */
    public int deleteObject(String selector) throws UserStoreException {

        if (debug) {
            log.debug(String.format("Deleting an object with objectReference %s.", selector));
        }
        String canonicalURI = baseURI + AWSConstants.DELETE_OBJECT;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.HOST_HEADER, hostHeader);
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);
        String payload = "{\"ObjectReference\": {\"Selector\": \"" + selector + "\"}}";
        if (debug) {
            log.debug(String.format("Payload to delete an object : %s ", payload));
        }
        AWSSignatureV4Generator aWSV4Auth = new AWSSignatureV4Generator.Builder(accessKeyID, secretAccessKey)
                .regionName(region)
                .serviceName(AWSConstants.SERVICE)
                .httpMethodName(AWSConstants.HTTP_PUT)
                .canonicalURI(canonicalURI)
                .queryParametes(null)
                .awsHeaders(awsHeaders)
                .payload(payload)
                .build();

        HttpPut httpPut = new HttpPut(AWSConstants.HTTPS + hostHeader + canonicalURI);

        /* Get header calculated for request */
        Map<String, String> header = aWSV4Auth.getHeaders();
        for (Map.Entry<String, String> entrySet : header.entrySet()) {
            httpPut.setHeader(entrySet.getKey(), entrySet.getValue());
        }
        int statusCode = 0;
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            httpPut.setHeader(AWSConstants.HOST_HEADER, hostHeader);
            httpPut.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);
            httpPut.setEntity(new StringEntity(payload, AWSConstants.UTF_8));
            if (debug) {
                log.debug("Invoking HTTP request to delete an object.");
            }
            HttpResponse response = httpClient.execute(httpPut);
            statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                handleException(String.format(AWSConstants.STRING_FORMAT, AWSConstants.ERROR_COULD_NOT_READ_HTTP_ENTITY,
                        response));
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), AWSConstants.UTF_8));
            JSONObject responseObject = getParsedObjectByReader(reader);
            if (statusCode == HttpStatus.SC_OK) {
                if (debug) {
                    log.debug(String.format("Successfully deleted object. Response : %s", responseObject));
                }
            } else {
                handleException(String.format("Error occured while delete an object %s. " +
                        AWSConstants.RESPONSE, selector, responseObject.toJSONString()));
            }
        } catch (ParseException e) {
            handleException(AWSConstants.ERROR_WHILE_PARSING_RESPONSE, e);
        } catch (IOException e) {
            handleException(AWSConstants.ERROR_WHILE_READING_RESPONSE, e);
        }
        return statusCode;
    }

    /**
     * Updates a given object's attributes.
     *
     * @param action          The action to perform when updating the attribute.
     * @param facetName       Name of the facet.
     * @param objectReference The reference that identifies the object in the directory structure.
     * @param map             List of properties to build the payload.
     * @return Status code.
     * @throws UserStoreException If error occurred.
     */
    public int updateObjectAttributes(String action, String facetName, String objectReference, Map<String, String> map)
            throws UserStoreException {

        if (debug) {
            log.debug(String.format("Updating a given object's attributes of object: %s.", objectReference));
        }
        String canonicalURI = baseURI + AWSConstants.UPDATE_OBJECT;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.HOST_HEADER, hostHeader);
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);
        String payload = buildPayloadToUpdateObjectAttributes(action, facetName, objectReference, map);
        if (debug) {
            log.debug(String.format("Payload to update a given object's attributes : %s ", payload));
        }
        AWSSignatureV4Generator aWSV4Auth = new AWSSignatureV4Generator.Builder(accessKeyID, secretAccessKey)
                .regionName(region)
                .serviceName(AWSConstants.SERVICE)
                .httpMethodName(AWSConstants.HTTP_PUT)
                .canonicalURI(canonicalURI)
                .queryParametes(null)
                .awsHeaders(awsHeaders)
                .payload(payload)
                .build();

        HttpPut httpPut = new HttpPut(AWSConstants.HTTPS + hostHeader + canonicalURI);

        /* Get header calculated for request */
        Map<String, String> header = aWSV4Auth.getHeaders();
        for (Map.Entry<String, String> entrySet : header.entrySet()) {
            httpPut.setHeader(entrySet.getKey(), entrySet.getValue());
        }
        int statusCode = 0;
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            httpPut.setHeader(AWSConstants.HOST_HEADER, hostHeader);
            httpPut.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);
            httpPut.setEntity(new StringEntity(payload, AWSConstants.UTF_8));
            if (debug) {
                log.debug("Invoking HTTP request to update a given object's attributes.");
            }
            HttpResponse response = httpClient.execute(httpPut);
            statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                handleException(String.format(AWSConstants.STRING_FORMAT, AWSConstants.ERROR_COULD_NOT_READ_HTTP_ENTITY,
                        response));
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), AWSConstants.UTF_8));
            JSONObject responseObject = getParsedObjectByReader(reader);
            if (statusCode == HttpStatus.SC_OK) {
                if (debug) {
                    log.debug("Successfully updated object's attributes");
                }
            } else {
                handleException(String.format("Error occured while update a given object's attributes. Object " +
                        "reference: %s" + AWSConstants.RESPONSE, objectReference, responseObject.toJSONString()));
            }
        } catch (ParseException e) {
            handleException(AWSConstants.ERROR_WHILE_PARSING_RESPONSE, e);
        } catch (IOException e) {
            handleException(AWSConstants.ERROR_WHILE_READING_RESPONSE, e);
        }
        return statusCode;
    }

    /**
     * Detaches a typed link from a specified source and target object.
     *
     * @param payload Payload to detach type link.
     * @return Statuscode for this action.
     * @throws UserStoreException If error occurred.
     */
    public int detachTypedLink(String payload) throws UserStoreException {

        if (debug) {
            log.debug(String.format("Detaching a typed link from a specified source and target object in directory %s.",
                    directoryArn));
        }
        String canonicalURI = baseURI + AWSConstants.DETACH_TYPEDLINK;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.HOST_HEADER, hostHeader);
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);
        if (debug) {
            log.debug(String.format("Payload to detach a typed link from a specified source and target object : %s ",
                    payload));
        }
        AWSSignatureV4Generator aWSV4Auth = new AWSSignatureV4Generator.Builder(accessKeyID, secretAccessKey)
                .regionName(region)
                .serviceName(AWSConstants.SERVICE)
                .httpMethodName(AWSConstants.HTTP_PUT)
                .canonicalURI(canonicalURI)
                .queryParametes(null)
                .awsHeaders(awsHeaders)
                .payload(payload)
                .build();

        HttpPut httpPut = new HttpPut(AWSConstants.HTTPS + hostHeader + canonicalURI);
        int statusCode = 0;
        /* Get header calculated for request */
        Map<String, String> header = aWSV4Auth.getHeaders();
        for (Map.Entry<String, String> entrySet : header.entrySet()) {
            httpPut.setHeader(entrySet.getKey(), entrySet.getValue());
        }
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            httpPut.setHeader(AWSConstants.HOST_HEADER, hostHeader);
            httpPut.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);
            httpPut.setEntity(new StringEntity(payload, AWSConstants.UTF_8));
            if (debug) {
                log.debug("Invoking HTTP request to detach a typed link from a specified source and target object.");
            }
            HttpResponse response = httpClient.execute(httpPut);
            statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                handleException(String.format(AWSConstants.STRING_FORMAT, AWSConstants.ERROR_COULD_NOT_READ_HTTP_ENTITY,
                        response));
            }
            if (debug && statusCode == HttpStatus.SC_OK) {
                log.debug("Successfully detach a typed link from a specified source and target object");
            }
        } catch (IOException e) {
            handleException(AWSConstants.ERROR_WHILE_READING_RESPONSE, e);
        }
        return statusCode;
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

        if (debug) {
            log.debug(String.format("Detaching a given object from the parent object: %s.", parentReference));
        }
        String canonicalURI = baseURI + AWSConstants.DETACH_OBJECT;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.HOST_HEADER, hostHeader);
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);
        String payload = buildPayloadToDetachObject(linkName, parentReference);
        if (debug) {
            log.debug(String.format("Payload to detach a given object from the parent object : %s ", payload));
        }
        AWSSignatureV4Generator aWSV4Auth = new AWSSignatureV4Generator.Builder(accessKeyID, secretAccessKey)
                .regionName(region)
                .serviceName(AWSConstants.SERVICE)
                .httpMethodName(AWSConstants.HTTP_PUT)
                .canonicalURI(canonicalURI)
                .queryParametes(null)
                .awsHeaders(awsHeaders)
                .payload(payload)
                .build();

        HttpPut httpPut = new HttpPut(AWSConstants.HTTPS + hostHeader + canonicalURI);

        /* Get header calculated for request */
        Map<String, String> header = aWSV4Auth.getHeaders();
        for (Map.Entry<String, String> entrySet : header.entrySet()) {
            httpPut.setHeader(entrySet.getKey(), entrySet.getValue());
        }
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            httpPut.setHeader(AWSConstants.HOST_HEADER, hostHeader);
            httpPut.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);
            httpPut.setEntity(new StringEntity(payload, AWSConstants.UTF_8));
            if (debug) {
                log.debug("Invoking HTTP request to detach a given object from the parent object.");
            }
            HttpResponse response = httpClient.execute(httpPut);
            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                handleException(String.format(AWSConstants.STRING_FORMAT, AWSConstants.ERROR_COULD_NOT_READ_HTTP_ENTITY,
                        response));
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), AWSConstants.UTF_8));
            JSONObject responseObject = getParsedObjectByReader(reader);
            if (statusCode == HttpStatus.SC_OK) {
                return responseObject;
            } else {
                handleException(String.format("Error occured while detach a given object from the parent object : %s. "
                        + AWSConstants.RESPONSE, parentReference, responseObject.toJSONString()));
            }
        } catch (ParseException e) {
            handleException(AWSConstants.ERROR_WHILE_PARSING_RESPONSE, e);
        } catch (IOException e) {
            handleException(AWSConstants.ERROR_WHILE_READING_RESPONSE, e);
        }
        return null;
    }

    /**
     * Update the facet.
     *
     * @param facetName     Name of the facet.
     * @param attributeList Attribute list.
     * @throws UserStoreException If error occurred.
     */
    public void updateFacet(String facetName, List<String> attributeList) throws UserStoreException {

        if (debug) {
            log.debug(String.format("Updating facet: %s.", facetName));
        }
        String canonicalURI = baseURI + AWSConstants.FACET;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.HOST_HEADER, hostHeader);
        awsHeaders.put(AWSConstants.PARTITION_HEADER, schemaArn);
        String payload = buildPayloadToupdateFacet(facetName, attributeList);
        if (debug) {
            log.debug(String.format("Payload to update a facet : %s ", payload));
        }
        AWSSignatureV4Generator aWSV4Auth = new AWSSignatureV4Generator.Builder(accessKeyID, secretAccessKey)
                .regionName(region)
                .serviceName(AWSConstants.SERVICE)
                .httpMethodName(AWSConstants.HTTP_PUT)
                .canonicalURI(canonicalURI)
                .queryParametes(null)
                .awsHeaders(awsHeaders)
                .payload(payload)
                .build();

        HttpPut httpPut = new HttpPut(AWSConstants.HTTPS + hostHeader + canonicalURI);

        /* Get header calculated for request */
        Map<String, String> header = aWSV4Auth.getHeaders();
        for (Map.Entry<String, String> entrySet : header.entrySet()) {
            httpPut.setHeader(entrySet.getKey(), entrySet.getValue());
        }
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            httpPut.setHeader(AWSConstants.HOST_HEADER, hostHeader);
            httpPut.setHeader(AWSConstants.PARTITION_HEADER, schemaArn);
            httpPut.setEntity(new StringEntity(payload, AWSConstants.UTF_8));
            if (debug) {
                log.debug("Invoking HTTP request to update a facet.");
            }
            HttpResponse response = httpClient.execute(httpPut);
            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                handleException(String.format(AWSConstants.STRING_FORMAT, AWSConstants.ERROR_COULD_NOT_READ_HTTP_ENTITY,
                        response));
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), AWSConstants.UTF_8));
            JSONObject responseObject = getParsedObjectByReader(reader);
            if (statusCode == HttpStatus.SC_OK) {
                if (debug) {
                    log.debug("Successfully updated the facet.");
                }
            } else {
                handleException(String.format("Error occured while updating a facet : %s. " +
                        AWSConstants.RESPONSE, facetName, responseObject.toJSONString()));
            }
        } catch (ParseException e) {
            handleException(AWSConstants.ERROR_WHILE_PARSING_RESPONSE, e);
        } catch (IOException e) {
            handleException(AWSConstants.ERROR_WHILE_READING_RESPONSE, e);
        }
    }

    /**
     * Retrieves metadata about an object.
     *
     * @param selector A path selector selection of an object by the parent/child links.
     * @return Object infomation.
     * @throws UserStoreException If error occurred.
     */
    public JSONObject getObjectInformation(String selector) throws UserStoreException {

        if (debug) {
            log.debug(String.format("Retrieving meta data about an object with objectReference %s.", selector));
        }
        String canonicalURI = baseURI + AWSConstants.GET_OBJECT_INFORMATION;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.HOST_HEADER, hostHeader);
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);
        awsHeaders.put(AWSConstants.CONSISTENCY_LEVEL_HEADER, AWSConstants.SERIALIZABLE);

        String payload = "{\"ObjectReference\": {\"Selector\": \"" + selector + "\"}}";
        if (debug) {
            log.debug(String.format("Payload to Retrieve metadata about an object : %s ", payload));
        }
        AWSSignatureV4Generator aWSV4Auth = new AWSSignatureV4Generator.Builder(accessKeyID, secretAccessKey)
                .regionName(region)
                .serviceName(AWSConstants.SERVICE)
                .httpMethodName(AWSConstants.HTTP_POST)
                .canonicalURI(canonicalURI)
                .queryParametes(null)
                .awsHeaders(awsHeaders)
                .payload(payload)
                .build();

        HttpPost httpPost = new HttpPost(AWSConstants.HTTPS + hostHeader + canonicalURI);

        /* Get header calculated for request */
        Map<String, String> header = aWSV4Auth.getHeaders();
        for (Map.Entry<String, String> entrySet : header.entrySet()) {
            httpPost.setHeader(entrySet.getKey(), entrySet.getValue());
        }
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            httpPost.setHeader(AWSConstants.HOST_HEADER, hostHeader);
            httpPost.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);
            httpPost.setHeader(AWSConstants.CONSISTENCY_LEVEL_HEADER, AWSConstants.SERIALIZABLE);
            httpPost.setEntity(new StringEntity(payload, AWSConstants.UTF_8));
            if (debug) {
                log.debug("Invoking HTTP request to retrieve metadata about an object.");
            }
            HttpResponse response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                handleException(String.format(AWSConstants.STRING_FORMAT, AWSConstants.ERROR_COULD_NOT_READ_HTTP_ENTITY,
                        response));
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), AWSConstants.UTF_8));
            JSONObject responseObject = getParsedObjectByReader(reader);
            if (statusCode == HttpStatus.SC_OK) {
                return responseObject;
            }
        } catch (ParseException e) {
            handleException(AWSConstants.ERROR_WHILE_PARSING_RESPONSE, e);
        } catch (IOException e) {
            handleException(AWSConstants.ERROR_WHILE_READING_RESPONSE, e);
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

        if (debug) {
            log.debug(String.format("Listing all attributes of an object: %s.", objectReference));
        }
        String canonicalURI = baseURI + AWSConstants.LIST_OBJECT_ATTRIBUTES;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.HOST_HEADER, hostHeader);
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);
        awsHeaders.put(AWSConstants.CONSISTENCY_LEVEL_HEADER, AWSConstants.SERIALIZABLE);
        String payload = buildPayloadToListObjectAttributes(facetName, objectReference);
        if (debug) {
            log.debug(String.format("Payload to list all attributes of an object : %s ", payload));
        }
        AWSSignatureV4Generator aWSV4Auth = new AWSSignatureV4Generator.Builder(accessKeyID, secretAccessKey)
                .regionName(region)
                .serviceName(AWSConstants.SERVICE)
                .httpMethodName(AWSConstants.HTTP_POST)
                .canonicalURI(canonicalURI)
                .queryParametes(null)
                .awsHeaders(awsHeaders)
                .payload(payload)
                .build();

        HttpPost httpPost = new HttpPost(AWSConstants.HTTPS + hostHeader + canonicalURI);

        /* Get header calculated for request */
        Map<String, String> header = aWSV4Auth.getHeaders();
        for (Map.Entry<String, String> entrySet : header.entrySet()) {
            httpPost.setHeader(entrySet.getKey(), entrySet.getValue());
        }
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            httpPost.setHeader(AWSConstants.HOST_HEADER, hostHeader);
            httpPost.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);
            httpPost.setHeader(AWSConstants.CONSISTENCY_LEVEL_HEADER, AWSConstants.SERIALIZABLE);
            httpPost.setEntity(new StringEntity(payload, AWSConstants.UTF_8));
            if (debug) {
                log.debug("Invoking HTTP request to list all attributes of an object.");
            }
            HttpResponse response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                handleException(String.format(AWSConstants.STRING_FORMAT, AWSConstants.ERROR_COULD_NOT_READ_HTTP_ENTITY,
                        response));
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), AWSConstants.UTF_8));
            JSONObject responseObject = getParsedObjectByReader(reader);
            if (statusCode == HttpStatus.SC_OK) {
                return responseObject;
            } else {
                handleException(String.format("Error occured while list all attributes of an object: %s. " +
                        AWSConstants.RESPONSE, objectReference, responseObject.toJSONString()));
            }
        } catch (ParseException e) {
            handleException(AWSConstants.ERROR_WHILE_PARSING_RESPONSE, e);
        } catch (IOException e) {
            handleException(AWSConstants.ERROR_WHILE_READING_RESPONSE, e);
        }
        return null;
    }

    /**
     * Listing all attributes of an facet
     *
     * @param facetName Facet name.
     * @return Facet attributes.
     * @throws UserStoreException If error occurred.
     */
    public JSONObject listFacetAttributes(String facetName) throws UserStoreException {

        if (debug) {
            log.debug(String.format("Listing all attributes of an facet: %s.", facetName));
        }
        String canonicalURI = baseURI + AWSConstants.LIST_FACET_ATTRIBUTES;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.HOST_HEADER, hostHeader);
        awsHeaders.put(AWSConstants.PARTITION_HEADER, schemaArn);
        String payload = buildPayloadTolistFacetAttributes(facetName);
        if (debug) {
            log.debug(String.format("Payload to list all attributes of an facet : %s ", payload));
        }
        AWSSignatureV4Generator aWSV4Auth = new AWSSignatureV4Generator.Builder(accessKeyID, secretAccessKey)
                .regionName(region)
                .serviceName(AWSConstants.SERVICE)
                .httpMethodName(AWSConstants.HTTP_POST)
                .canonicalURI(canonicalURI)
                .queryParametes(null)
                .awsHeaders(awsHeaders)
                .payload(payload)
                .build();

        HttpPost httpPost = new HttpPost(AWSConstants.HTTPS + hostHeader + canonicalURI);

        /* Get header calculated for request */
        Map<String, String> header = aWSV4Auth.getHeaders();
        for (Map.Entry<String, String> entrySet : header.entrySet()) {
            httpPost.setHeader(entrySet.getKey(), entrySet.getValue());
        }
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            httpPost.setHeader(AWSConstants.HOST_HEADER, hostHeader);
            httpPost.setHeader(AWSConstants.PARTITION_HEADER, schemaArn);
            httpPost.setEntity(new StringEntity(payload, AWSConstants.UTF_8));
            if (debug) {
                log.debug("Invoking HTTP request to list all attributes of an facet.");
            }
            HttpResponse response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                handleException(String.format(AWSConstants.STRING_FORMAT, AWSConstants.ERROR_COULD_NOT_READ_HTTP_ENTITY,
                        response));
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), AWSConstants.UTF_8));
            JSONObject responseObject = getParsedObjectByReader(reader);
            if (statusCode == HttpStatus.SC_OK) {
                return responseObject;
            } else {
                handleException(String.format("Error occured while list all attributes of an facet: %s. " +
                        AWSConstants.RESPONSE, facetName, responseObject.toJSONString()));
            }
        } catch (ParseException e) {
            handleException(AWSConstants.ERROR_WHILE_PARSING_RESPONSE, e);
        } catch (IOException e) {
            handleException(AWSConstants.ERROR_WHILE_READING_RESPONSE, e);
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

        if (debug) {
            log.debug("Attaching a typed link to a specified source and target object.");
        }
        String canonicalURI = baseURI + AWSConstants.ATTACH_TYPEDLINK;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.HOST_HEADER, hostHeader);
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);
        String payload = buildPayloadToGetAttachTypedLink(sourceSelector, targetSelector, facetName, map);
        if (debug) {
            log.debug(String.format("Payload to attach a typed link to a specified source and target object : %s ",
                    payload));
        }
        AWSSignatureV4Generator aWSV4Auth = new AWSSignatureV4Generator.Builder(accessKeyID, secretAccessKey)
                .regionName(region)
                .serviceName(AWSConstants.SERVICE)
                .httpMethodName(AWSConstants.HTTP_PUT)
                .canonicalURI(canonicalURI)
                .queryParametes(null)
                .awsHeaders(awsHeaders)
                .payload(payload)
                .build();

        HttpPut httpPut = new HttpPut(AWSConstants.HTTPS + hostHeader + canonicalURI);

        /* Get header calculated for request */
        Map<String, String> header = aWSV4Auth.getHeaders();
        for (Map.Entry<String, String> entrySet : header.entrySet()) {
            httpPut.setHeader(entrySet.getKey(), entrySet.getValue());
        }
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            httpPut.setHeader(AWSConstants.HOST_HEADER, hostHeader);
            httpPut.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);
            httpPut.setEntity(new StringEntity(payload, AWSConstants.UTF_8));
            if (debug) {
                log.debug("Invoking HTTP request to attach a typed link to a specified source and target object.");
            }
            HttpResponse response = httpClient.execute(httpPut);
            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                handleException(String.format(AWSConstants.STRING_FORMAT, AWSConstants.ERROR_COULD_NOT_READ_HTTP_ENTITY,
                        response));
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), AWSConstants.UTF_8));
            JSONObject responseObject = getParsedObjectByReader(reader);
            if (statusCode == HttpStatus.SC_OK) {
                if (debug) {
                    log.debug(String.format("Successfully attached a typed link. Response : %s",
                            responseObject.toJSONString()));
                }
            } else {
                handleException(String.format("Error occured while attach a typed link to a specified source and " +
                        "target object. Response : %s", responseObject.toJSONString()));
            }
        } catch (ParseException e) {
            handleException(AWSConstants.ERROR_WHILE_PARSING_RESPONSE, e);
        } catch (IOException e) {
            handleException(AWSConstants.ERROR_WHILE_READING_RESPONSE, e);
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

        if (debug) {
            log.debug(String.format("Creating a TypedLinkFacet %s in schema %s.", facetName, schemaArn));
        }
        String canonicalURI = baseURI + AWSConstants.CREATE_TYPEDLINK;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.HOST_HEADER, hostHeader);
        awsHeaders.put(AWSConstants.PARTITION_HEADER, schemaArn);
        String payload = buildPayloadTogetTypedLinkFacet(facetName, attributes);
        if (debug) {
            log.debug(String.format("Payload to create a TypedLinkFacet : %s ", payload));
        }
        AWSSignatureV4Generator aWSV4Auth = new AWSSignatureV4Generator.Builder(accessKeyID, secretAccessKey)
                .regionName(region)
                .serviceName(AWSConstants.SERVICE)
                .httpMethodName(AWSConstants.HTTP_PUT)
                .canonicalURI(canonicalURI)
                .queryParametes(null)
                .awsHeaders(awsHeaders)
                .payload(payload)
                .build();

        HttpPut httpPut = new HttpPut(AWSConstants.HTTPS + hostHeader + canonicalURI);

        /* Get header calculated for request */
        Map<String, String> header = aWSV4Auth.getHeaders();
        for (Map.Entry<String, String> entrySet : header.entrySet()) {
            httpPut.setHeader(entrySet.getKey(), entrySet.getValue());
        }
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            httpPut.setHeader(AWSConstants.HOST_HEADER, hostHeader);
            httpPut.setHeader(AWSConstants.PARTITION_HEADER, schemaArn);
            httpPut.setEntity(new StringEntity(payload, AWSConstants.UTF_8));
            if (debug) {
                log.debug("Invoking HTTP request to create a TypedLinkFacet.");
            }
            HttpResponse response = httpClient.execute(httpPut);
            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                handleException(String.format(AWSConstants.STRING_FORMAT, AWSConstants.ERROR_COULD_NOT_READ_HTTP_ENTITY,
                        response));
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), AWSConstants.UTF_8));
            JSONObject responseObject = getParsedObjectByReader(reader);
            if (statusCode == HttpStatus.SC_OK) {
                if (debug) {
                    log.debug(String.format("TypedLinkFacet is created successfully. Response : %s ",
                            responseObject.toJSONString()));
                }
            } else {
                handleException(String.format("Error occured while create a TypedLinkFacet. " +
                        AWSConstants.RESPONSE, responseObject.toJSONString()));
            }
        } catch (ParseException e) {
            handleException(AWSConstants.ERROR_WHILE_PARSING_RESPONSE, e);
        } catch (IOException e) {
            handleException(AWSConstants.ERROR_WHILE_READING_RESPONSE, e);
        }
    }

    /**
     * Creates an object in a Directory.
     *
     * @param linkName        Name of the link.
     * @param facetName       Name of the facet.
     * @param parentReference The parent reference to which this object will be attached.
     * @param map             List of properties to build the payload.
     * @return Status code.
     * @throws UserStoreException If error occurred.
     */
    public int createObject(String linkName, String facetName, String parentReference, Map<String, String> map)
            throws UserStoreException {

        if (debug) {
            log.debug(String.format("Creating an object in a directory: %s with link Name %s.",
                    directoryArn, linkName));
        }
        String canonicalURI = baseURI + AWSConstants.CREATE_OBJECT;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.HOST_HEADER, hostHeader);
        awsHeaders.put(AWSConstants.PARTITION_HEADER, directoryArn);
        String payload = buildPayloadToCreateObject(linkName, facetName, parentReference, map);
        if (debug) {
            log.debug(String.format("Payload to create an object in a directory : %s ", payload));
        }
        AWSSignatureV4Generator aWSV4Auth = new AWSSignatureV4Generator.Builder(accessKeyID, secretAccessKey)
                .regionName(region)
                .serviceName(AWSConstants.SERVICE)
                .httpMethodName(AWSConstants.HTTP_PUT)
                .canonicalURI(canonicalURI)
                .queryParametes(null)
                .awsHeaders(awsHeaders)
                .payload(payload)
                .build();

        HttpPut httpPut = new HttpPut(AWSConstants.HTTPS + hostHeader + canonicalURI);

        /* Get header calculated for request */
        Map<String, String> header = aWSV4Auth.getHeaders();
        for (Map.Entry<String, String> entrySet : header.entrySet()) {
            httpPut.setHeader(entrySet.getKey(), entrySet.getValue());
        }
        int statusCode = 0;
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            httpPut.setHeader(AWSConstants.HOST_HEADER, hostHeader);
            httpPut.setHeader(AWSConstants.PARTITION_HEADER, directoryArn);
            httpPut.setEntity(new StringEntity(payload, AWSConstants.UTF_8));
            if (debug) {
                log.debug("Invoking HTTP request to create an object in a directory.");
            }
            HttpResponse response = httpClient.execute(httpPut);
            statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                handleException(String.format(AWSConstants.STRING_FORMAT, AWSConstants.ERROR_COULD_NOT_READ_HTTP_ENTITY,
                        response));
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), AWSConstants.UTF_8));
            JSONObject responseObject = getParsedObjectByReader(reader);
            if (statusCode == HttpStatus.SC_OK) {
                if (debug) {
                    log.debug(String.format("Object is created successfull with ObjectIdentifier %s ",
                            responseObject.get("ObjectIdentifier")));
                }
            } else {
                handleException(String.format("Error occured while create an object in a directory %s. " +
                        AWSConstants.RESPONSE, directoryArn, responseObject.toJSONString()));
            }
        } catch (ParseException e) {
            handleException(AWSConstants.ERROR_WHILE_PARSING_RESPONSE, e);
        } catch (IOException e) {
            handleException(AWSConstants.ERROR_WHILE_READING_RESPONSE, e);
        }
        return statusCode;
    }

    /**
     * Creates a new Facet in a schema.
     *
     * @param facetName Name of the facet.
     * @param map       List of properties to build the payload.
     * @throws UserStoreException If error occurred.
     */
    public void createSchemaFacet(String facetName, Map<String, String> map) throws UserStoreException {

        if (debug) {
            log.debug(String.format("Creating a new Facet in a schema %s .", schemaArn));
        }
        String canonicalURI = baseURI + AWSConstants.CREATE_FACET;
        TreeMap<String, String> awsHeaders = new TreeMap<>();
        awsHeaders.put(AWSConstants.HOST_HEADER, hostHeader);
        awsHeaders.put(AWSConstants.PARTITION_HEADER, schemaArn);
        String facetPayload = buildPayloadToCreateSchemaFacet(facetName, map);
        if (debug) {
            log.debug(String.format("Payload to create a new facet in a schema : %s ", facetPayload));
        }
        AWSSignatureV4Generator aWSV4Auth = new AWSSignatureV4Generator.Builder(accessKeyID, secretAccessKey)
                .regionName(region)
                .serviceName(AWSConstants.SERVICE)
                .httpMethodName(AWSConstants.HTTP_PUT)
                .canonicalURI(canonicalURI)
                .queryParametes(null)
                .awsHeaders(awsHeaders)
                .payload(facetPayload)
                .build();

        HttpPut httpPut = new HttpPut(AWSConstants.HTTPS + hostHeader + canonicalURI);

        /* Get header calculated for request */
        Map<String, String> header = aWSV4Auth.getHeaders();
        for (Map.Entry<String, String> entrySet : header.entrySet()) {
            httpPut.setHeader(entrySet.getKey(), entrySet.getValue());
        }
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            httpPut.setHeader(AWSConstants.HOST_HEADER, hostHeader);
            httpPut.setHeader(AWSConstants.PARTITION_HEADER, schemaArn);
            httpPut.setEntity(new StringEntity(facetPayload, AWSConstants.UTF_8));
            if (debug) {
                log.debug("Invoking HTTP request to create a new facet in a schema.");
            }
            HttpResponse response = httpClient.execute(httpPut);
            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                handleException(String.format(AWSConstants.STRING_FORMAT, AWSConstants.ERROR_COULD_NOT_READ_HTTP_ENTITY,
                        response));
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), AWSConstants.UTF_8));
            JSONObject responseObject = getParsedObjectByReader(reader);
            if (statusCode == HttpStatus.SC_OK) {
                if (debug) {
                    log.debug("Schema facet is created successfully. Response Object : "
                            + responseObject.toJSONString());
                }
            } else {
                handleException(String.format("Error occured while create a new facet in a schema %s. " +
                        AWSConstants.RESPONSE, schemaArn, responseObject.toJSONString()));
            }
        } catch (ParseException e) {
            handleException(AWSConstants.ERROR_WHILE_PARSING_RESPONSE, e);
        } catch (IOException e) {
            handleException(AWSConstants.ERROR_WHILE_READING_RESPONSE, e);
        }
    }

    /**
     * Generate payload to list object children .
     *
     * @param nextToken The pagination token.
     * @param selector  Path of the object in the directory structure.
     * @return Payload.
     */
    private String buildPayloadTolistObjectChildren(String nextToken, String selector) {

        StringBuilder builder = new StringBuilder();
        builder.append("{\"MaxResults\": 30,");
        if (StringUtils.isNotEmpty(nextToken)) {
            builder.append("\"NextToken\": \"").append(nextToken).append("\",");
        }
        builder.append(AWSConstants.OBJECT_REFERENCE).append(selector).append("\"}}");
        return builder.toString();
    }

    /**
     * Generate payload to list directories .
     *
     * @param nextToken The pagination token.
     * @return Payload.
     */
    private String buildPayloadTolistDirectories(String nextToken) {

        StringBuilder builder = new StringBuilder();
        builder.append("{\"MaxResults\": 30,");
        if (StringUtils.isNotEmpty(nextToken)) {
            builder.append("\"NextToken\": \"").append(nextToken).append("\",");
        }
        builder.append("\"state\": \"ENABLED\"}");
        return builder.toString();
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
    private String buildPayloadToGetAttachTypedLink(String sourceSelector, String targetSelector, String facetName,
                                                    Map<String, String> map) {

        StringBuilder builder = new StringBuilder();
        List<String> attributes = new LinkedList<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            attributes.add("{\"AttributeName\": \"" + entry.getKey() + "\", \"Value\": {\"StringValue\": \""
                    + entry.getValue() + "\"}}");
        }
        builder.append("{\"Attributes\": [").append(String.join(",", attributes))
                .append("], \"SourceObjectReference\": {\"Selector\": \"").append(sourceSelector)
                .append("\"}, \"TargetObjectReference\": {\"Selector\": \"").append(targetSelector)
                .append("\"}, \"TypedLinkFacet\": {\"SchemaArn\": \"").append(schemaArn)
                .append(AWSConstants.TYPED_LINK_NAME).append(facetName).append("\"}}");
        return builder.toString();
    }

    /**
     * Generate payload to list object attributes.
     *
     * @param facetName       Name of the facet.
     * @param objectReference The reference that identifies the object in the directory structure.
     * @return Payload.
     */
    private String buildPayloadToListObjectAttributes(String facetName, String objectReference) {

        return "{\"FacetFilter\": { \"FacetName\": \"" + facetName + AWSConstants.SCHEMAARN + schemaArn
                + "\"}, \"MaxResults\": 30, \"ObjectReference\": {\"Selector\": \"" + objectReference + "\"}}";
    }

    private String buildPayloadTolistFacetAttributes(String facetName) {

        return "{\"MaxResults\": 100, \"Name\": \"" + facetName + "\"}";
    }

    /**
     * Generate payload to get typed link.
     *
     * @param typedLinkName   Name of the Typed Link.
     * @param objectReference The reference that identifies the object in the directory structure.
     * @return Payload.
     */
    private String buildPayloadToGetTypedLink(String typedLinkName, String objectReference) {

        StringBuilder builder = new StringBuilder();
        builder.append("{");
        if (typedLinkName != null) {
            builder.append("\"FilterTypedLink\": {\"SchemaArn\": \"").append(schemaArn)
                    .append(AWSConstants.TYPED_LINK_NAME).append(typedLinkName).append("\"}, ");
        }
        builder.append(AWSConstants.OBJECT_REFERENCE).append(objectReference).append("\"}}");
        return builder.toString();
    }

    /**
     * Generate payload to list incoming typedLinks.
     *
     * @param facetName Name of the Typed Link.
     * @param selector  Path of the object in the directory structure.
     * @return Payload.
     */
    private String buildPayloadToListIncomingTypedLinks(String facetName, String selector) {

        StringBuilder builder = new StringBuilder();
        builder.append("{");
        if (StringUtils.isNotEmpty(facetName)) {
            builder.append("\"FilterTypedLink\": {\"SchemaArn\": \"")
                    .append(schemaArn).append(AWSConstants.TYPED_LINK_NAME).append(facetName).append("\"}, ");
        }
        builder.append(AWSConstants.OBJECT_REFERENCE).append(selector).append("\"}}");
        return builder.toString();
    }

    /**
     * Generate payload to update facet.
     *
     * @param facetName     Name of the facet.
     * @param attributeList Attribute list.
     * @return Payload.
     */
    private String buildPayloadToupdateFacet(String facetName, List<String> attributeList) {

        StringBuilder builder = new StringBuilder();
        List<String> attributes = new LinkedList<>();
        for (String attributeName : attributeList) {
            attributes.add("{\"Action\": \"CREATE_OR_UPDATE\", \"Attribute\": { \"AttributeDefinition\": {\"Type\": " +
                    "\"STRING\"}, \"Name\": \"" + attributeName + "\", \"RequiredBehavior\": \"NOT_REQUIRED\"}}");
        }
        return builder.append("{\"AttributeUpdates\": [").append(String.join(",", attributes))
                .append(AWSConstants.NAME_STR).append(facetName).append("\"}").toString();
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
    private String buildPayloadToUpdateObjectAttributes(String action, String facetName, String objectReference,
                                                        Map<String, String> map) {

        StringBuilder builder = new StringBuilder();
        List<String> attributes = new LinkedList<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            attributes.add("{\"ObjectAttributeAction\": {\"ObjectAttributeActionType\": \"" + action + "\", " +
                    "\"ObjectAttributeUpdateValue\": {\"StringValue\": \"" + entry.getValue() + "\"}}, " +
                    "\"ObjectAttributeKey\": {\"FacetName\": \"" + facetName + "\", \"Name\": \"" + entry.getKey()
                    + AWSConstants.SCHEMAARN + schemaArn + "\"}}");
        }
        return builder.append("{\"AttributeUpdates\": [").append(String.join(",", attributes))
                .append("], \"ObjectReference\": {\"Selector\": \"").append(objectReference).append("\"}}").toString();
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
    private String buildPayloadToCreateObject(String linkName, String facetName, String parentReference,
                                              Map<String, String> map) {

        StringBuilder builder = new StringBuilder();
        List<String> attributes = new LinkedList<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            attributes.add("{\"Key\": {\"FacetName\": \"" + facetName + "\", \"Name\": \"" + entry.getKey() + "\", " +
                    "\"SchemaArn\": \"" + schemaArn + "\"}, \"Value\": {\"StringValue\": \"" + entry.getValue()
                    + "\"}}");
        }
        return builder.append("{\"LinkName\": \"").append(linkName).append("\", \"ObjectAttributeList\": [")
                .append(String.join(",", attributes)).append("], \"ParentReference\": {\"Selector\": \"")
                .append(parentReference).append("\"}, \"SchemaFacets\": [{\"FacetName\" : \"").append(facetName)
                .append(AWSConstants.SCHEMAARN).append(schemaArn).append("\"}]}").toString();
    }

    /**
     * Generate payload to get typedLink facet.
     *
     * @param facetName  Name of the facet.
     * @param attributes List of attributes to build the payload.
     * @return Payload.
     */
    private String buildPayloadTogetTypedLinkFacet(String facetName, List attributes) {

        StringBuilder builder = new StringBuilder();
        List<String> attribute = new LinkedList<>();
        List<String> attributeOrder = new LinkedList<>();
        for (Object key : attributes) {
            attribute.add("{\"Name\":\"" + key + "\",\"RequiredBehavior\": \"REQUIRED_ALWAYS\", \"Type\": \"STRING\"}");
            attributeOrder.add("\"" + key + "\"");
        }
        return builder.append("{\"Facet\": {\"Attributes\": [").append(String.join(",", attribute))
                .append("], \"IdentityAttributeOrder\": [").append(String.join(",", attributeOrder))
                .append(AWSConstants.NAME_STR).append(facetName).append("\"}}").toString();
    }

    /**
     * Generate payload to detach object.
     *
     * @param linkName        Name of the link.
     * @param parentReference The parent reference to which this object will be attached.
     * @return Payload.
     */
    private String buildPayloadToDetachObject(String linkName, String parentReference) {

        return "{\"LinkName\": \"" + linkName + "\", \"ParentReference\": {\"Selector\": \"" + parentReference + "\"}}";
    }

    /**
     * Generate payload to create schema facet.
     *
     * @param facetName Name of the facet.
     * @param map       List of properties to build the payload.
     * @return Payload.
     */
    private String buildPayloadToCreateSchemaFacet(String facetName, Map<String, String> map) {

        StringBuilder builder = new StringBuilder();
        boolean isImmutable = false;
        if (StringUtils.equals(facetName, AWSConstants.GROUP)) {
            isImmutable = true;
        }
        List<String> attributes = new LinkedList<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            attributes.add("{\"AttributeDefinition\": {\"IsImmutable\": " + isImmutable + ", \"Type\": \"STRING\"}, " +
                    "\"Name\": \"" + entry.getKey() + "\", \"RequiredBehavior\": \"" + entry.getValue() + "\"}");
        }
        return builder.append("{\"Attributes\": [").append(String.join(",", attributes)).append(AWSConstants.NAME_STR)
                .append(facetName).append("\", \"ObjectType\": \"NODE\"}").toString();
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

        log.error(msg, e);
        throw new UserStoreException(msg, e);
    }

    /**
     * Common method to throw exceptions. This will only expect one parameter.
     *
     * @param msg error message as a string.
     * @throws UserStoreException If error occurred.
     */
    private void handleException(String msg) throws UserStoreException {

        log.error(msg);
        throw new UserStoreException(msg);
    }
}
