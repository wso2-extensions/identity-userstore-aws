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
package org.wso2.carbon.aws.user.store.mgt;

/**
 * This class will hold constants related to AWS Userstore manager.
 */
public class AWSConstants {

    public static final String ACCESS_KEY_ID = "AccessKeyID";
    public static final String SECRET_ACCESS_KEY = "SecretAccessKey";
    public static final String REGION = "Region";
    public static final String API_VERSION = "APIVersion";
    public static final String PATH_TO_USERS = "PathToUsers";
    public static final String PATH_TO_ROLES = "PathToRoles";
    public static final String MEMBERSHIP_TYPE_OF_ROLES = "MembershipTypeOfRoles";
    public static final String FACET_NAME_OF_USER = "FacetNameOfUser";
    public static final String FACET_NAME_OF_ROLE = "FacetNameOfRole";
    public static final String USER_NAME_ATTRIBUTE = "UserNameAttribute";
    public static final String PASS_ATTRIBUTE = "PasswordAttribute";
    public static final String ROLE_NAME_ATTRIBUTE = "RoleNameAttribute";
    public static final String HOST_HEADER = "host";
    public static final String DATE_HEADER = "x-amz-date";
    public static final String AUTHORIZATION_HEADER = "Authorization";
    public static final String PARTITION_HEADER = "x-amz-data-partition";
    public static final String CONSISTENCY_LEVEL_HEADER = "x-amz-consistency-level";
    public static final String SERIALIZABLE = "SERIALIZABLE";
    public static final String HTTP_POST = "POST";
    public static final String HTTP_PUT = "PUT";
    public static final String HTTPS = "https://";
    public static final String GROUP = "GROUP";
    public static final String SERVICE = "clouddirectory";
    public static final String AMAZON_AWS_COM = ".amazonaws.com";
    public static final String AMAZON_CLOUD_DIRECTORY = "/amazonclouddirectory/";
    public static final String CREATE_FACET = "/facet/create";
    public static final String CREATE_OBJECT = "/object";
    public static final String CREATE_TYPEDLINK = "/typedlink/facet/create";
    public static final String ATTACH_TYPEDLINK = "/typedlink/attach";
    public static final String LIST_OBJECT_ATTRIBUTES = "/object/attributes";
    public static final String GET_OBJECT_INFORMATION = "/object/information";
    public static final String DETACH_OBJECT = "/object/detach";
    public static final String FACET = "/facet";
    public static final String DETACH_TYPEDLINK = "/typedlink/detach";
    public static final String BATCH_WRITE = "/batchwrite";
    public static final String UPDATE_OBJECT = "/object/update";
    public static final String DELETE_OBJECT = "/object/delete";
    public static final String LIST_OBJECT_CHILDREN = "/object/children";
    public static final String LIST_INCOMING_TYPEDLINK = "/typedlink/incoming";
    public static final String GET_TYPED_LINK_FACET = "/typedlink/facet/get";
    public static final String LIST_OUTGOING_TYPEDLINK = "/typedlink/outgoing";
    public static final String LIST_DIRECTORIES = "/directory/list";
    public static final String NAME = "Name";
    public static final String CHILDREN = "Children";
    public static final String NEXT_TOKEN = "NextToken";
    public static final String TYPEDLINK_SPECIFIERS = "TypedLinkSpecifiers";
    public static final String REQUIRED_ALWAYS = "REQUIRED_ALWAYS";
    public static final String LINK_SPECIFIERS = "LinkSpecifiers";
    public static final String ATTRIBUTES = "Attributes";
    public static final String KEY = "Key";
    public static final String VALUE = "Value";
    public static final String CREATE_OR_UPDATE = "CREATE_OR_UPDATE";
    public static final String DELETE = "DELETE";
    public static final String ATTRIBUTE = "attribute";
    public static final String ATTRIBUTE_NAME = "AttributeName";
    public static final String IDENTITY_ATTRIBUTE_VALUES = "IdentityAttributeValues";
    public static final String LINK = "link";
    public static final String USER_ROLE_ASSOCIATION = "USER_ROLE_ASSOCIATION";
    public static final String STRING_VALUE = "StringValue";
    public static final String DETACHED_OBJECT_IDENTIFIER = "DetachedObjectIdentifier";
    public static final String DIRECTORIES = "Directories";
    public static final String DIRECTORY_ARN = "DirectoryArn";
    public static final String SCHEMA_ARN = "SchemaArn";
    public static final String OBJECT_REFERENCE = "\"ObjectReference\": {\"Selector\": \"";
    public static final String OPERATIONS = "{\"Operations\": [";
    public static final String DETACH_TYPED_LINK = "{\"DetachTypedLink\":";
    public static final String UPDATE_ATTRIBUTES = "{\"UpdateObjectAttributes\":";
    public static final String TYPED_LINK_NAME = "\", \"TypedLinkName\": \"";
    public static final String SCHEMAARN = "\", \"SchemaArn\": \"";
    public static final String NAME_STR = "], \"Name\": \"";
    public static final String MAXRESULTS = "{\"MaxResults\": 30,";
    public static final String NEXTTOKEN = "\"NextToken\": \"";
    public static final String ENABLED = "\"state\": \"ENABLED\"}";
    public static final String SOURCE_REFERENCE = "], \"SourceObjectReference\": {\"Selector\": \"";
    public static final String TARGET_REFERENCE = "\"}, \"TargetObjectReference\": {\"Selector\": \"";
    public static final String TYPEDLINK_FACET = "\"}, \"TypedLinkFacet\": {\"SchemaArn\": \"";
    public static final String ATTRIBUTES_STR = "{\"Attributes\": [";
    public static final String NODE = "\", \"ObjectType\": \"NODE\"}";
    public static final String ATTRIBUTE_NAME_STR = "{\"AttributeName\": \"";
    public static final String STRING_STR = "\", \"Value\": {\"StringValue\": \"";
    public static final String FACET_FILTER = "{\"FacetFilter\": { \"FacetName\": \"";
    public static final String OBJECT_REF = "\"}, \"MaxResults\": 30, " + OBJECT_REFERENCE;
    public static final String STRING_VALUE_STR = "\"}, \"Value\": {\"StringValue\": \"";
    public static final String LINK_NAME = "{\"LinkName\": \"";
    public static final String PARENT_SELECTOR = "\", \"ParentReference\": {\"Selector\": \"";
    public static final String OBJECT_ATTRIBUTE_LIST = "\", \"ObjectAttributeList\": [";
    public static final String PARENT_REFERENCE = "], \"ParentReference\": {\"Selector\": \"";
    public static final String ATTRIBUTE_STR = "{\"Name\":\"";
    public static final String ATTRIBUTE_DEFINITION = "{\"AttributeDefinition\": {\"IsImmutable\": ";
    public static final String REQUIRED_BEHAVIOR = "\",\"RequiredBehavior\": \"REQUIRED_ALWAYS\", " +
            "\"Type\": \"STRING\"}";
    public static final String STRING_TYPE = ", \"Type\": \"STRING\"}, \"Name\": \"";
    public static final String FACET_ATTRIBUTE = "{\"Facet\": {\"Attributes\": [";
    public static final String IDENTITY_ATTRIBUTE = "], \"IdentityAttributeOrder\": [";
    public static final String SCHEMA_FACET = "\"}, \"SchemaFacets\": [{\"FacetName\" : \"";
    public static final String REQUIRED = "\", \"RequiredBehavior\": \"";
    public static final String FILTER_TYPEDLINK = "\"FilterTypedLink\": {\"SchemaArn\": \"";
    public static final String RESPONSE = "Response : %s, StatusCode : %s";
    public static final String TYPED_LINK_SPECIFIER = "{\"TypedLinkSpecifier\": ";
    public static final String ATTRIBUTE_UPDATE = "{\"AttributeUpdates\": [";
    public static final String OBJECT_ATTRIBUTE = "{\"ObjectAttributeAction\": {\"ObjectAttributeActionType\": \"";
    public static final String OBJECT_ATTRIBUTE_UPDATE = "\", \"ObjectAttributeUpdateValue\": {\"StringValue\": \"";
    public static final String OBJECT_ATTRIBUTE_KEY = "\"}}, \"ObjectAttributeKey\": {\"FacetName\": \"";
    public static final String OBJECT_NAME = "\", \"Name\": \"";
    public static final String OBJECT_SELECTOR = "], " + OBJECT_REFERENCE;
    public static final String FACET_NAME = "{\"Key\": {\"FacetName\": \"";
    public static final String PROPERTY_PASS_ERROR_MSG = "PasswordJavaRegExViolationErrorMsg";
    public static final String PROPERTY_USER_NAME_ERROR_MSG = "UsernameJavaRegExViolationErrorMsg";
    public static final String UTF_8 = "UTF-8";
    public static final String UTC = "UTC";
    public static final String HEX_ARRAY_STRING = "0123456789ABCDEF";
    public static final String HMAC_ALGORITHM = "AWS4-HMAC-SHA256";
    public static final String AWS4_REQUEST = "aws4_request";
    public static final String AWS4 = "AWS4";
    public static final double MAX_API_LIMIT = 30.0;
    public static final String HMAC_SHA = "HmacSHA256";
    public static final String SHA_ALGORITHM = "SHA-256";
    public static final String DATE_TIME_FORMAT = "yyyyMMdd'T'HHmmss'Z'";
    public static final String DATE_FORMAT = "yyyyMMdd";
    public static final String ERROR_WHILE_PARSING_RESPONSE = "Error while parsing response json";
    public static final String ERROR_WHILE_CHARACTOR_ENCODING = "Error while character encoding is not supported";
    public static final String ERROR_WHILE_READING_RESPONSE = "Error while reading response body";
    public static final String ERROR_WHILE_DETACH_TYPED_LINK = "Could not detach typed link from object. Link: ";
    public static final String ERROR_WHILE_GETTING_CLAIM_ATTRIBUTE = "Error occurred while getting claim attribute" +
            " for user: ";
    protected static final String PASS_HASH_METHOD = "PasswordHashMethod";

    private AWSConstants() {

    }
}
