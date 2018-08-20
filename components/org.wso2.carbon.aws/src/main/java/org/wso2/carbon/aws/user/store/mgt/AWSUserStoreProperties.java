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

import org.wso2.carbon.user.api.Property;
import org.wso2.carbon.user.core.UserCoreConstants;
import org.wso2.carbon.user.core.UserStoreConfigConstants;

import java.util.ArrayList;

/**
 * Default AWS user store properties.
 */
public class AWSUserStoreProperties {

    private AWSUserStoreProperties() {

    }

    //Properties for AWS User Store Manager
    protected static final ArrayList<Property> AWS_MANDATORY_PROPERTIES = new ArrayList<>();
    protected static final ArrayList<Property> AWS_OPTIONAL_PROPERTIES = new ArrayList<>();
    protected static final ArrayList<Property> AWS_ADVANCED_PROPERTIES = new ArrayList<>();
    private static final String USERNAME_JAVA_REG_EX_VIOLATION_ERROR_MSG_DESCRIPTION = "Error message when the " +
            "Username is not matched with UsernameJavaRegEx";
    private static final String PASSWORD_JAVA_REG_EX_VIOLATION_ERROR_MSG_DESCRIPTION = "Error message when the " +
            "Password is not matched with passwordJavaRegEx";

    static {
        // Set mandatory properties
        setMandatoryProperty(AWSConstants.ACCESS_KEY_ID, "AccessKey ID", "", "AWS access key ID", false);
        setMandatoryProperty(AWSConstants.SECRET_ACCESS_KEY, "SecretAccessKey", "", "AWS secret access key", false);
        setMandatoryProperty(AWSConstants.REGION, "Region", "", "Region which is used select a regional endpoint to " +
                "make requests", false);
        setMandatoryProperty(AWSConstants.API_VERSION, "API Version", "", "Cloud directory API version", false);
        setMandatoryProperty(AWSConstants.PATH_TO_USERS, "Path To Users", "", "This is a path to identify the Users " +
                "object in the tree structure.", false);
        setMandatoryProperty(AWSConstants.PATH_TO_ROLES, "Path To Roles", "", "This is a path to identify the Roles " +
                "object in the tree structure.", false);
        setMandatoryProperty(AWSConstants.MEMBERSHIP_TYPE_OF_ROLES, "Membership Type Of Roles", "",
                "Indicates how you are going to maintain user and role objects relationship. " +
                        "Possible values: link, attribute.", false);
        setMandatoryProperty(AWSConstants.DIRECTORY_ARN, "DirectoryArn", "", "Directory in which the objects will be " +
                "created.", false);
        setMandatoryProperty(AWSConstants.SCHEMA_ARN, "SchemaArn", "", "Schema arn of the directory.", false);
        setMandatoryProperty(AWSConstants.FACET_NAME_OF_USER, "FacetName Of User", "", "Facet name of the user object.",
                false);
        setMandatoryProperty(AWSConstants.FACET_NAME_OF_ROLE, "FacetName Of Role", "", "Facet name of the role object.",
                false);
        setMandatoryProperty(AWSConstants.USER_NAME_ATTRIBUTE, "UserName Attribute", "", "The name of the attribute" +
                " is used to identify the user name of user entry.", false);
        setMandatoryProperty(AWSConstants.PASS_ATTRIBUTE, "Password Attribute", "", "The name of the attribute is" +
                " used to identify the password of user entry.", false);
        setMandatoryProperty(AWSConstants.ROLE_NAME_ATTRIBUTE, "RoleName Attribute", "", "The name of the attribute " +
                "is used to identify the role name of role entry.", false);

        // Set optional properties
        setProperty(UserStoreConfigConstants.readGroups, "Read Groups", "true",
                UserStoreConfigConstants.readLDAPGroupsDescription);
        setProperty(UserStoreConfigConstants.writeGroups, "Write Groups", "true",
                UserStoreConfigConstants.writeGroupsDescription);
        setProperty(UserStoreConfigConstants.disabled, "Disabled", "false",
                UserStoreConfigConstants.disabledDescription);
        setProperty(UserStoreConfigConstants.membershipAttribute, "Membership Attribute", "", "Define the attribute " +
                "that contains the distinguished names of user objects that are in a role.");
        setProperty(UserStoreConfigConstants.memberOfAttribute, "Member Of Attribute", "", "Define the attribute that" +
                " contains the distinguished names of role objects that user is assigned to.");
        setProperty("PasswordJavaRegEx", "Password RegEx (Java)", "^[\\S]{5,30}$", "A regular expression to validate " +
                "passwords");
        setProperty(UserStoreConfigConstants.passwordJavaScriptRegEx, "Password RegEx (Javascript)", "^[\\S]{5,30}$",
                UserStoreConfigConstants.passwordJavaScriptRegExDescription);
        setProperty(AWSConstants.PROPERTY_PASS_ERROR_MSG, "Password RegEx Violation Error Message", "Password pattern" +
                        " policy violated.",
                PASSWORD_JAVA_REG_EX_VIOLATION_ERROR_MSG_DESCRIPTION);
        setProperty(UserStoreConfigConstants.usernameJavaRegEx, "Username RegEx (Java)", "[a-zA-Z0-9._-|//]{3,30}$",
                UserStoreConfigConstants.usernameJavaRegExDescription);
        setProperty(UserStoreConfigConstants.usernameJavaScriptRegEx, "Username RegEx (Javascript)", "^[\\S]{3,30}$",
                UserStoreConfigConstants.usernameJavaScriptRegExDescription);
        setProperty(AWSConstants.PROPERTY_USER_NAME_ERROR_MSG, "Username RegEx Violation Error Message", "Username " +
                        "pattern policy violated.",
                USERNAME_JAVA_REG_EX_VIOLATION_ERROR_MSG_DESCRIPTION);
        setProperty(UserStoreConfigConstants.roleNameJavaRegEx, "Role Name RegEx (Java)", "[a-zA-Z0-9._-|//]{3,30}$",
                UserStoreConfigConstants.roleNameJavaRegExDescription);
        setProperty(UserStoreConfigConstants.roleNameJavaScriptRegEx, "Role Name RegEx (Javascript)", "^[\\S]{3,30}$",
                UserStoreConfigConstants.roleNameJavaScriptRegExDescription);

        // Set advanced properties
        setAdvancedProperty(AWSConstants.PASS_HASH_METHOD, "PLAIN_TEXT");
        setAdvancedProperty(UserCoreConstants.RealmConfig.PROPERTY_MAX_USER_LIST, "100");
        setAdvancedProperty(UserCoreConstants.RealmConfig.PROPERTY_MAX_ROLE_LIST, "100");
    }

    private static void setMandatoryProperty(String name, String displayName, String value, String description,
                                             boolean encrypt) {

        String propertyDescription = displayName + "#" + description;
        if (encrypt) {
            propertyDescription += "#encrypt";
        }
        Property property = new Property(name, value, propertyDescription, null);
        AWS_MANDATORY_PROPERTIES.add(property);

    }

    private static void setProperty(String name, String displayName, String value, String description) {

        Property property = new Property(name, value, displayName + "#" + description, null);
        AWS_OPTIONAL_PROPERTIES.add(property);

    }

    private static void setAdvancedProperty(String name, String value) {

        Property property = new Property(name, value, "", null);
        AWS_ADVANCED_PROPERTIES.add(property);

    }
}
