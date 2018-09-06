/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.aws.user.store.mgt;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.wso2.carbon.CarbonConstants;
import org.wso2.carbon.aws.user.store.mgt.util.AWSRestApiActions;
import org.wso2.carbon.user.api.Properties;
import org.wso2.carbon.user.api.Property;
import org.wso2.carbon.user.api.RealmConfiguration;
import org.wso2.carbon.user.core.UserCoreConstants;
import org.wso2.carbon.user.core.UserRealm;
import org.wso2.carbon.user.core.UserStoreConfigConstants;
import org.wso2.carbon.user.core.UserStoreException;
import org.wso2.carbon.user.core.claim.ClaimManager;
import org.wso2.carbon.user.core.common.AbstractUserStoreManager;
import org.wso2.carbon.user.core.common.RoleContext;
import org.wso2.carbon.user.core.jdbc.JDBCRoleContext;
import org.wso2.carbon.user.core.profile.ProfileConfigurationManager;
import org.wso2.carbon.user.core.tenant.Tenant;
import org.wso2.carbon.user.core.util.DatabaseUtil;
import org.wso2.carbon.user.core.util.UserCoreUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import javax.sql.DataSource;

public class AWSUserStoreManager extends AbstractUserStoreManager {

    private static final Log log = LogFactory.getLog(AWSUserStoreManager.class);
    // Unique name to identify the user store.
    private String domain = null;
    private AWSRestApiActions awsActions;
    // This is a path to identify the “Users” object in the tree structure.
    private String pathToUsers;
    // This is a path to identify the “Roles” object in the tree structure.
    private String pathToRoles;
    // Facet name of the user object.
    private String facetNameOfUser;
    // Facet name of the role object.
    private String facetNameOfRole;
    // Indicates how you are going to maintain user and role objects relationship.
    private String membershipType;
    // The name of the attribute is used to identify the user name of user entry.
    private String userNameAttribute;
    // The name of the attribute is used to identify the password of user entry.
    private String passwordAttribute;
    // Define the attribute that contains the distinguished names of user objects that are in a role.
    private String membershipAttribute;
    // The name of the attribute is used to identify the role name of role entry.
    private String roleNameAttribute;
    // Define the attribute that contains the distinguished names of role objects that user is assigned to.
    private String memberOfAttribute;
    // Facet name of the typedLink for user and role association.
    private String typedLinkFacetName;
    // Specifies the Password Hashing Algorithm used the hash the password before storing in the userstore.
    private String passwordHashMethod;

    public AWSUserStoreManager() {

    }

    public AWSUserStoreManager(RealmConfiguration realmConfig, int tenantId) throws UserStoreException {

        this.realmConfig = realmConfig;
        this.tenantId = tenantId;
        awsActions = new AWSRestApiActions(realmConfig);
        // Set groups read/write configuration
        if (realmConfig.getUserStoreProperty(UserCoreConstants.RealmConfig.READ_GROUPS_ENABLED) != null) {
            readGroupsEnabled = Boolean.parseBoolean(realmConfig
                    .getUserStoreProperty(UserCoreConstants.RealmConfig.READ_GROUPS_ENABLED));
        }

        if (realmConfig.getUserStoreProperty(UserCoreConstants.RealmConfig.WRITE_GROUPS_ENABLED) != null) {
            writeGroupsEnabled = Boolean.parseBoolean(realmConfig
                    .getUserStoreProperty(UserCoreConstants.RealmConfig.WRITE_GROUPS_ENABLED));
        } else {
            if (!isReadOnly()) {
                writeGroupsEnabled = true;
            }
        }
        if (writeGroupsEnabled) {
            readGroupsEnabled = true;
        }
        // Initialize user roles cache as implemented in AbstractUserStoreManager
        initUserRolesCache();

        setUpAWSDirectory();
    }

    public AWSUserStoreManager(RealmConfiguration realmConfig, Map<String, Object> properties,
                               ClaimManager claimManager, ProfileConfigurationManager profileManager, UserRealm realm,
                               Integer tenantId) throws UserStoreException {

        this(realmConfig, tenantId);

        if (log.isDebugEnabled()) {
            log.debug("AWS Userstore manager initialization Started " + System.currentTimeMillis());
        }
        this.claimManager = claimManager;
        this.userRealm = realm;

        dataSource = (DataSource) properties.get(UserCoreConstants.DATA_SOURCE);
        if (dataSource == null) {
            dataSource = DatabaseUtil.getRealmDataSource(realmConfig);
        }
        if (dataSource == null) {
            throw new UserStoreException("User Management Data Source is null");
        }

        doInitialSetup();
        this.persistDomain();
        if (realmConfig.isPrimary()) {
            addInitialAdminData(Boolean.parseBoolean(realmConfig.getAddAdmin()), !isInitSetupDone());
        }

        properties.put(UserCoreConstants.DATA_SOURCE, dataSource);

        if (log.isDebugEnabled()) {
            log.debug("The AWSDataSource being used by AWSUserStoreManager :: " + dataSource.hashCode());
        }
        domain = realmConfig.getUserStoreProperty(UserCoreConstants.RealmConfig.PROPERTY_DOMAIN_NAME);
        /*
         * Initialize user roles cache as implemented in AbstractUserStoreManager
         */
        initUserRolesCache();
        if (log.isDebugEnabled()) {
            log.debug("AWS UserStore manager initialization Ended " + System.currentTimeMillis());
        }
    }

    /**
     * Checks if the role is existing the role store.
     *
     * @param roleName Rolename.
     * @return Whether the role is existing in role store or not.
     * @throws UserStoreException If error occurred.
     */
    @Override
    protected boolean doCheckExistingRole(String roleName) throws UserStoreException {

        RoleContext roleContext = createRoleContext(roleName);
        roleName = roleContext.getRoleName();
        if (log.isDebugEnabled()) {
            log.debug("Searching for role " + roleName);
        }
        boolean isExistingRole = checkExistenceOfUserOrRole(pathToRoles, roleName);
        if (log.isDebugEnabled()) {
            log.debug("Role: " + roleName + " is exists in user store");
        }
        return isExistingRole;
    }

    /**
     * Adds the user to the user store.
     *
     * @param userName              of new user.
     * @param credential            of new user.
     * @param roleList              of new user.
     * @param claims                user claim values.
     * @param profileName           user profile name.
     * @param requirePasswordChange Status to change password.
     * @throws UserStoreException If error occurred.
     */
    @Override
    public void doAddUser(String userName, Object credential, String[] roleList, Map<String, String> claims,
                          String profileName, boolean requirePasswordChange) throws UserStoreException {

        if (!checkUserNameValid(userName)) {
            String errorMsg = realmConfig.getUserStoreProperty(AWSConstants.PROPERTY_USER_NAME_ERROR_MSG);
            if (errorMsg != null) {
                throw new UserStoreException(errorMsg);
            }
            throw new UserStoreException(String.format("User name not valid. It must be a non null string " +
                    "with following format: %s.", realmConfig.getUserStoreProperty(
                    UserCoreConstants.RealmConfig.PROPERTY_USER_NAME_JAVA_REG_EX)));
        }
        if (!checkUserPasswordValid(credential)) {
            String errorMsg = realmConfig.getUserStoreProperty(AWSConstants.PROPERTY_PASS_ERROR_MSG);
            if (errorMsg != null) {
                throw new UserStoreException(errorMsg);
            }
            throw new UserStoreException(String.format("Credential not valid. It must be a non null string " +
                    "with following format: %s", realmConfig.getUserStoreProperty(
                    UserCoreConstants.RealmConfig.PROPERTY_JAVA_REG_EX)));
        }
        boolean isExisting = doCheckExistingUser(userName);
        if (isExisting) {
            throw new UserStoreException(String.format("User name : %s exists in the system. Please pick another " +
                    "user name", userName));
        }

        Map<String, String> map = new HashMap<>();
        byte[] passwordToStore = UserCoreUtil.getPasswordToStore(credential, passwordHashMethod, false);

        map.put(userNameAttribute, userName);
        map.put(passwordAttribute, new String(passwordToStore));
        boolean hasRoles = roleList != null && roleList.length > 0;
        if (membershipType.equals(AWSConstants.ATTRIBUTE) && hasRoles) {
            map.put(membershipAttribute, String.join(",", roleList));
        }
        if (MapUtils.isNotEmpty(claims)) {
            Map<String, String> claimList = getClaimAttributes(userName, claims);
            map.putAll(claimList);
        }
        awsActions.createObject(userName, facetNameOfUser, pathToUsers, map);
        if (hasRoles) {
            // Add roles to user.
            addRolesToUser(userName, roleList);
        }
    }

    /**
     * Deletes a user by userName from userstore.
     *
     * @param userName of user to delete
     * @throws UserStoreException exception if any occur.
     */
    @Override
    public void doDeleteUser(String userName) throws UserStoreException {

        String selector = pathToUsers + "/" + userName;
        if (membershipType.equals(AWSConstants.LINK)) {
            // List and detach all outgoing typed links from a user object.
            JSONObject outgoingTypedLinks = awsActions.listOutgoingTypedLinks(null, selector);
            detachOutgoingTypedLinks(outgoingTypedLinks);
        } else if (membershipType.equals(AWSConstants.ATTRIBUTE)) {
            // Remove the user from all role objects.
            removeUserFromRoles(userName);
        }
        // Detach object from parent object.
        JSONObject detachObject = awsActions.detachObject(userName, pathToUsers);
        if (detachObject.get(AWSConstants.DETACHED_OBJECT_IDENTIFIER) != null) {
            String identifier = "$" + detachObject.get(AWSConstants.DETACHED_OBJECT_IDENTIFIER).toString();
            // Delete object from directory.
            awsActions.deleteObject(identifier);
        }
    }

    /**
     * Remove a particular user from all role objects.
     *
     * @param userName User name.
     * @throws UserStoreException If any error occur.
     */
    protected void removeUserFromRoles(String userName) throws UserStoreException {

        Map<String, String> map = new HashMap<>();
        List<String> attributes = new LinkedList<>();
        String nextToken = null;
        do {
            JSONObject listChildren = awsActions.listObjectChildren(nextToken, pathToRoles);
            Object token = listChildren.get(AWSConstants.NEXT_TOKEN);
            nextToken = (token != null) ? token.toString() : null;
            Object childrensObj = listChildren.get(AWSConstants.CHILDREN);
            JSONObject childrens = (childrensObj != null) ? (JSONObject) childrensObj : null;
            if (childrens != null) {
                for (Object key : childrens.keySet()) {
                    String keyValue = pathToRoles + "/" + key.toString();
                    String existingUsers = getAttributeValue(facetNameOfRole, keyValue, memberOfAttribute);
                    if (StringUtils.isNotEmpty(existingUsers) && existingUsers.contains(userName)) {
                        List<String> updatedUserList = new LinkedList<>(Arrays.asList(existingUsers.split(",")));
                        updatedUserList.remove(userName);

                        map.put(memberOfAttribute, String.join(",", updatedUserList));
                        attributes.add(AWSConstants.UPDATE_ATTRIBUTES + awsActions.buildPayloadToUpdateObjectAttributes(
                                AWSConstants.CREATE_OR_UPDATE, facetNameOfRole, keyValue, map) + "}");
                    }
                }
                awsActions.batchWrite(AWSConstants.OPERATIONS + String.join(",", attributes) + "]}");
            }
        } while (StringUtils.isNotEmpty(nextToken));
    }

    /**
     * Detach outgoing typed links.
     *
     * @param outgoingTypedLinks Outgoing links.
     * @throws UserStoreException If any error occur.
     */
    protected void detachOutgoingTypedLinks(JSONObject outgoingTypedLinks) throws UserStoreException {

        Object specifiers = outgoingTypedLinks.get(AWSConstants.TYPEDLINK_SPECIFIERS);
        List<String> attributes = new LinkedList<>();
        JSONArray typedLinkSpecifiers = (specifiers != null) ? (JSONArray) specifiers : null;
        if (typedLinkSpecifiers != null) {
            for (Object typedLinkSpecifier : typedLinkSpecifiers) {
                String keyValue = ((JSONObject) typedLinkSpecifier).toJSONString();
                attributes.add(AWSConstants.DETACH_TYPED_LINK + AWSConstants.TYPED_LINK_SPECIFIER + keyValue + "}}");
            }
        }
        awsActions.batchWrite(AWSConstants.OPERATIONS + String.join(",", attributes) + "]}");
    }

    /**
     * Changes the password of the user.
     *
     * @param userName      of user to update credentials.
     * @param oldCredential of user.
     * @param newCredential of user to update.
     * @throws UserStoreException exception if any error occur.
     */
    @Override
    public void doUpdateCredential(String userName, Object newCredential, Object oldCredential)
            throws UserStoreException {

        this.doUpdateCredentialByAdmin(userName, newCredential);
    }

    /**
     * Update admin user credentials in user store.
     *
     * @param userName      of admin to update credentials.
     * @param newCredential of user to update.
     * @throws UserStoreException exception if any occur.
     */
    @Override
    public void doUpdateCredentialByAdmin(String userName, Object newCredential) throws UserStoreException {

        if (!checkUserPasswordValid(newCredential)) {
            String errorMsg = realmConfig.getUserStoreProperty(AWSConstants.PROPERTY_PASS_ERROR_MSG);
            if (errorMsg != null) {
                throw new UserStoreException(errorMsg);
            }
            throw new UserStoreException(String.format("Credential not valid. It must be a non null string " +
                    "with following format: %s", realmConfig.getUserStoreProperty(
                    UserCoreConstants.RealmConfig.PROPERTY_JAVA_REG_EX)));
        }

        Map<String, String> map = new HashMap<>();
        byte[] passwordToStore = UserCoreUtil.getPasswordToStore(newCredential, passwordHashMethod, false);
        map.put(passwordAttribute, new String(passwordToStore));
        awsActions.updateObjectAttributes(AWSConstants.CREATE_OR_UPDATE, facetNameOfUser, pathToUsers + "/"
                + userName, map);
    }

    /**
     * Check whether the user is exists in user store.
     *
     * @param userName given to check.
     * @return boolean true or false respectively for user exists or not.
     * @throws UserStoreException exception if any occur.
     */
    @Override
    protected boolean doCheckExistingUser(String userName) throws UserStoreException {

        if (log.isDebugEnabled()) {
            log.debug("Searching for user " + userName);
        }
        boolean isExistingUser = checkExistenceOfUserOrRole(pathToUsers, userName);
        if (log.isDebugEnabled()) {
            log.debug("User: " + userName + " is exists in user store");
        }
        return isExistingUser;
    }

    /**
     * Check whether the user/role object exist in user store or not.
     *
     * @param selector Path of object in tree structure.
     * @param name     Name of the object.
     * @return Boolean.
     * @throws UserStoreException if any exception occurred.
     */
    private boolean checkExistenceOfUserOrRole(String selector, String name) throws UserStoreException {

        String nextToken = null;
        do {
            JSONObject objectChildrens = awsActions.listObjectChildren(nextToken, selector);
            Object token = objectChildrens.get(AWSConstants.NEXT_TOKEN);
            nextToken = (token != null) ? token.toString() : null;
            if (objectChildrens.get(AWSConstants.CHILDREN) != null) {
                JSONObject childrens = (JSONObject) objectChildrens.get(AWSConstants.CHILDREN);
                for (Object key : childrens.keySet()) {
                    if (key.equals(name)) {
                        return true;
                    }
                }
            }
        } while (StringUtils.isNotEmpty(nextToken));

        return false;
    }

    /**
     * Adds a role to the role store.
     *
     * @param roleName of new role.
     * @param userList of new role to add.
     * @param shared   status of whether the role is shared or not.
     * @throws UserStoreException if any exception occurred.
     */
    @Override
    public void doAddRole(String roleName, String[] userList, boolean shared) throws UserStoreException {

        boolean isExisting = doCheckExistingRole(roleName);
        if (isExisting) {
            throw new UserStoreException(String.format("RoleName : %s exists in the system. Please pick another " +
                    "role name", roleName));
        }
        Map<String, String> map = new HashMap<>();
        map.put(roleNameAttribute, roleName);
        boolean hasUsers = userList != null && userList.length > 0;
        if (membershipType.equals(AWSConstants.ATTRIBUTE) && hasUsers) {
            map.put(memberOfAttribute, String.join(",", userList));
        }
        awsActions.createObject(roleName, facetNameOfRole, pathToRoles, map);
        if (hasUsers) {
            // Add users to role.
            addUsersToRole(userList, roleName);
        }
    }

    /**
     * Deletes a role by role name from the role store.
     *
     * @param roleName to delete from user store
     * @throws UserStoreException if any exception occurred
     */
    @Override
    public void doDeleteRole(String roleName) throws UserStoreException {

        String selector = pathToRoles + "/" + roleName;
        if (membershipType.equals(AWSConstants.LINK)) {
            // List and detach all incoming typed links to role object.
            JSONObject incomingTypedLinks = awsActions.listIncomingTypedLinks(null, selector);
            detachIncomingTypedLinks(incomingTypedLinks);
        } else if (membershipType.equals(AWSConstants.ATTRIBUTE)) {
            // Remove a particular role from all user objects.
            removeRoleFromUsers(roleName);
        }
        // Detach object from parent object.
        JSONObject detachObject = awsActions.detachObject(roleName, pathToRoles);
        if (detachObject.get(AWSConstants.DETACHED_OBJECT_IDENTIFIER) != null) {
            String identifier = "$" + detachObject.get(AWSConstants.DETACHED_OBJECT_IDENTIFIER).toString();
            // Delete object from directory.
            awsActions.deleteObject(identifier);
        }
    }

    /**
     * Remove a particular role from all user objects.
     *
     * @param roleName Role name.
     * @throws UserStoreException if any exception occurred.
     */
    protected void removeRoleFromUsers(String roleName) throws UserStoreException {

        Map<String, String> map = new HashMap<>();
        List<String> attributes = new LinkedList<>();
        String nextToken = null;
        do {
            JSONObject objectChildrens = awsActions.listObjectChildren(nextToken, pathToUsers);
            Object token = objectChildrens.get(AWSConstants.NEXT_TOKEN);
            nextToken = (token != null) ? token.toString() : null;
            if (objectChildrens.get(AWSConstants.CHILDREN) != null) {
                JSONObject childrens = (JSONObject) objectChildrens.get(AWSConstants.CHILDREN);
                for (Object key : childrens.keySet()) {
                    String value = pathToUsers + "/" + key.toString();
                    String existingRoles = getAttributeValue(facetNameOfUser, value, membershipAttribute);
                    if (StringUtils.isNotEmpty(existingRoles) && existingRoles.contains(roleName)) {
                        List<String> updatedRoleList = new LinkedList<>(Arrays.asList(existingRoles.split(",")));
                        updatedRoleList.remove(roleName);

                        map.put(membershipAttribute, String.join(",", updatedRoleList));
                        attributes.add(AWSConstants.UPDATE_ATTRIBUTES + awsActions.buildPayloadToUpdateObjectAttributes(
                                AWSConstants.CREATE_OR_UPDATE, facetNameOfUser, value, map) + "}");
                    }
                }
                awsActions.batchWrite(AWSConstants.OPERATIONS + String.join(",", attributes) + "]}");
            }
        } while (StringUtils.isNotEmpty(nextToken));
    }

    /**
     * Detach incoming TypedLinks.
     *
     * @param incomingTypedLinks Incoming TypedLinks.
     * @throws UserStoreException if any exception occurred.
     */
    protected void detachIncomingTypedLinks(JSONObject incomingTypedLinks) throws UserStoreException {

        Object specifiers = incomingTypedLinks.get(AWSConstants.LINK_SPECIFIERS);
        List<String> attributes = new LinkedList<>();
        JSONArray linkSpecifiers = (specifiers != null) ? (JSONArray) specifiers : null;
        if (linkSpecifiers != null) {
            for (Object linkSpecifier : linkSpecifiers) {
                String keyValue = ((JSONObject) linkSpecifier).toJSONString();
                attributes.add(AWSConstants.DETACH_TYPED_LINK + AWSConstants.TYPED_LINK_SPECIFIER + keyValue + "}}");
            }
            awsActions.batchWrite(AWSConstants.OPERATIONS + String.join(",", attributes) + "]}");
        }
    }

    /**
     * Updates the role name in the role store.
     *
     * @param roleName    to update.
     * @param newRoleName to be updated.
     * @throws UserStoreException if any exception occurred.
     */
    @Override
    public void doUpdateRoleName(String roleName, String newRoleName) throws UserStoreException {

        JDBCRoleContext context = (JDBCRoleContext) createRoleContext(roleName);
        roleName = context.getRoleName();

        List<String> tempList = new ArrayList<>();
        // Get user list of role.
        String[] userListWithDomain = doGetUserListOfRole(roleName, "*");
        for (String userName : userListWithDomain) {
            String[] removedUserNames = userName.split(CarbonConstants.DOMAIN_SEPARATOR);
            if (removedUserNames.length > 1) {
                userName = removedUserNames[1];
            }
            tempList.add(userName);
        }
        String[] users = tempList.toArray(new String[tempList.size()]);
        // Delete old role
        doDeleteRole(roleName);

        /* Add new role and Add users to the new role
           Since AWS user store does not support for shared roles, setting to false*/
        doAddRole(newRoleName, users, false);
        if (log.isDebugEnabled()) {
            log.debug(String.format("Successfully updated the role: %s", roleName));
        }
    }

    /**
     * Authenticates a user given the user name and password against the user store.
     *
     * @param userName   of authenticating user.
     * @param credential include user password of authenticating user.
     * @return boolean if authenticate fail or not.
     * @throws UserStoreException If any exception occurred.
     */
    @Override
    public boolean doAuthenticate(String userName, Object credential) throws UserStoreException {

        userName = userName.trim();
        if (StringUtils.isEmpty(userName) || credential == null) {
            return false;
        }
        if (log.isDebugEnabled()) {
            log.debug("Authenticating user " + userName);
        }
        String selector = pathToUsers + "/" + userName;
        String storedPassword = getAttributeValue(facetNameOfUser, selector, passwordAttribute);
        byte[] password = UserCoreUtil.getPasswordToStore(credential, passwordHashMethod, false);
        boolean isAuthed = (storedPassword != null) && (storedPassword.equals(new String(password)));
        if (isAuthed) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Successfully authenticated user: %s, status: %s", userName, true));
            }
        } else {
            handleException(String.format("Error while authenticating user: %s", userName));
        }
        return isAuthed;
    }

    /**
     * Lists the users in the user store.
     *
     * @param filter       to filter the search.
     * @param maxItemLimit to display per page.
     * @return String[] of users.
     * @throws UserStoreException if any exception occurred.
     */
    @Override
    protected String[] doListUsers(String filter, int maxItemLimit) throws UserStoreException {

        int givenMax;
        String[] users = new String[0];
        if (maxItemLimit == 0) {
            return users;
        }
        try {
            givenMax = Integer.parseInt(realmConfig
                    .getUserStoreProperty(UserCoreConstants.RealmConfig.PROPERTY_MAX_USER_LIST));
        } catch (Exception e) {
            givenMax = UserCoreConstants.MAX_USER_ROLE_LIST;

            if (log.isDebugEnabled()) {
                log.debug("Realm configuration maximum not set : Using User Core Constant value instead!", e);
            }
        }
        if (maxItemLimit < 0 || maxItemLimit > givenMax) {
            maxItemLimit = givenMax;
        }

        List<String> tempList = getAllChildrens(pathToUsers, filter, maxItemLimit);
        int usersCount = tempList.size();
        if (usersCount > 0) {
            users = tempList.toArray(new String[tempList.size()]);
        }
        Arrays.sort(users);
        if (usersCount < maxItemLimit) {
            maxItemLimit = usersCount;
        }
        users = Arrays.copyOfRange(users, 0, maxItemLimit);

        return users;
    }

    /**
     * Match strings against a pattern.
     *
     * @param text   String that need to be matched.
     * @param filter Pattern.
     * @return Whether the provided text is matched against the pattern or not.
     */
    protected boolean matchFilter(String text, String filter) {

        if (text == null || filter == null) return true;

        StringBuilder builder = new StringBuilder(".*");
        for (StringTokenizer st = new StringTokenizer(filter, "%*", true); st.hasMoreTokens(); ) {
            String token = st.nextToken();
            if ("*".equals(token)) {
                builder.append(".*");
            } else {
                builder.append(Pattern.quote(token));
            }
        }
        builder.append(".*");
        return text.matches(builder.toString());
    }

    /**
     * Get the role names in the roles store.
     *
     * @param filter       to filter the search.
     * @param maxItemLimit to display per page.
     * @return String[] of roles.
     * @throws UserStoreException if any exception occurred.
     */
    @Override
    public String[] doGetRoleNames(String filter, int maxItemLimit) throws UserStoreException {

        int givenMax;
        String[] roles = new String[0];
        if (maxItemLimit == 0) {
            return roles;
        }

        try {
            givenMax = Integer.parseInt(realmConfig
                    .getUserStoreProperty(UserCoreConstants.RealmConfig.PROPERTY_MAX_ROLE_LIST));
        } catch (Exception e) {
            givenMax = UserCoreConstants.MAX_USER_ROLE_LIST;

            if (log.isDebugEnabled()) {
                log.debug("Realm configuration maximum not set : Using User Core Constant value instead!", e);
            }
        }

        if (maxItemLimit < 0 || maxItemLimit > givenMax) {
            maxItemLimit = givenMax;
        }

        List<String> tempList = getAllChildrens(pathToRoles, filter, maxItemLimit);
        int rolesCount = tempList.size();
        if (rolesCount > 0) {
            roles = tempList.toArray(new String[tempList.size()]);
        }
        Arrays.sort(roles);
        if (rolesCount < maxItemLimit) {
            maxItemLimit = rolesCount;
        }
        roles = Arrays.copyOfRange(roles, 0, maxItemLimit);

        return roles;
    }

    /**
     * Get all child elements of an object.
     *
     * @param selector Path of an object in the tree structure.
     * @param filter   To filter the search.
     * @return List of children
     * @throws UserStoreException If error occurred.
     */
    protected List<String> getAllChildrens(String selector, String filter, double maxLimit) throws UserStoreException {

        String nextToken = null;
        String name;
        List<String> tempList = new LinkedList<>();
        double apiCallLimit = Math.ceil(maxLimit / AWSConstants.MAX_API_LIMIT);
        int counter = 1;
        do {
            JSONObject objectChildrens = awsActions.listObjectChildren(nextToken, selector);
            Object token = objectChildrens.get(AWSConstants.NEXT_TOKEN);
            nextToken = (token != null) ? token.toString() : null;
            if (objectChildrens.get(AWSConstants.CHILDREN) != null) {
                JSONObject childrens = (JSONObject) objectChildrens.get(AWSConstants.CHILDREN);
                for (Object key : childrens.keySet()) {
                    String keyValue = key.toString();
                    if (matchFilter(keyValue, filter)) {
                        name = UserCoreUtil.addDomainToName(keyValue, domain);
                        tempList.add(name);
                    }
                }
            }
            counter++;
        } while (StringUtils.isNotEmpty(nextToken) && counter <= apiCallLimit);

        return tempList;
    }

    /**
     * Get the list of users mapped to a role.
     *
     * @param filter   to filter the search.
     * @param roleName to search for users.
     * @return String[] of users.
     * @throws UserStoreException if any exception occurred.
     */
    @Override
    public String[] doGetUserListOfRole(String roleName, String filter) throws UserStoreException {

        String[] users = new String[0];
        List<String> tempList = new LinkedList<>();
        RoleContext roleContext = createRoleContext(roleName);
        roleName = roleContext.getRoleName();
        String selector = pathToRoles + "/" + roleName;
        if (AWSConstants.LINK.equals(membershipType)) {
            tempList = getUserListOfRoleByLink(selector, filter);
        } else if (AWSConstants.ATTRIBUTE.equals(membershipType)) {
            String[] userList = new String[0];
            String existingUsers = getAttributeValue(facetNameOfRole, selector, memberOfAttribute);
            if (StringUtils.isNotEmpty(existingUsers)) {
                userList = existingUsers.split(",");
            }
            for (String user : userList) {
                if (matchFilter(user, filter)) {
                    tempList.add(UserCoreUtil.addDomainToName(user, domain));
                }
            }
        }
        if (!tempList.isEmpty()) {
            users = tempList.toArray(new String[tempList.size()]);
        }

        return users;
    }

    /**
     * Get UserList Of Role, when we use links.
     *
     * @param selector Path of an object in the tree structure.
     * @return List of users.
     */
    protected List<String> getUserListOfRoleByLink(String selector, String filter) throws UserStoreException {

        JSONObject links = awsActions.listIncomingTypedLinks(typedLinkFacetName, selector);
        List<String> tempList = new LinkedList<>();
        JSONArray linkSpecifiers = (JSONArray) links.get(AWSConstants.LINK_SPECIFIERS);
        if (!linkSpecifiers.isEmpty()) {
            for (Object linkSpecifier : linkSpecifiers) {
                JSONObject keyValue = (JSONObject) linkSpecifier;
                JSONArray attributes = (JSONArray) keyValue.get(AWSConstants.IDENTITY_ATTRIBUTE_VALUES);
                for (Object attribute : attributes) {
                    JSONObject key = (JSONObject) attribute;
                    Object attributeName = key.get(AWSConstants.ATTRIBUTE_NAME);
                    JSONObject attributeValue = (JSONObject) key.get(AWSConstants.VALUE);
                    String userName = attributeValue.get(AWSConstants.STRING_VALUE).toString();
                    if (attributeName.equals(userNameAttribute) && matchFilter(userName, filter)) {
                        tempList.add(UserCoreUtil.addDomainToName(userName, domain));
                    }
                }
            }
        }
        return tempList;
    }

    /**
     * Gets the external role list of a user.
     *
     * @param userName of user to get role list.
     * @param filter   if any filtering apply.
     * @return String[] of rolelist of user.
     * @throws UserStoreException if any error occurred.
     */
    @Override
    public String[] doGetExternalRoleListOfUser(String userName, String filter) throws UserStoreException {

        String[] roles = new String[0];
        List<String> tempList = new LinkedList<>();
        String selector = pathToUsers + "/" + userName;
        if (AWSConstants.LINK.equals(membershipType)) {
            JSONObject outgoingTypedLinks = awsActions.listOutgoingTypedLinks(typedLinkFacetName, selector);
            if (outgoingTypedLinks != null) {
                tempList = getRoleListOfUserByLink(outgoingTypedLinks, filter);
            }
        } else if (AWSConstants.ATTRIBUTE.equals(membershipType)) {
            String[] roleList = new String[0];
            String existingRoles = getAttributeValue(facetNameOfUser, selector, membershipAttribute);
            if (StringUtils.isNotEmpty(existingRoles)) {
                roleList = existingRoles.split(",");
            }
            for (String role : roleList) {
                if (matchFilter(role, filter)) {
                    tempList.add(UserCoreUtil.addDomainToName(role, domain));
                }
            }
        }
        if (!tempList.isEmpty()) {
            roles = tempList.toArray(new String[tempList.size()]);
        }

        return roles;
    }

    /**
     * Get role list Of user, when we use MembershipTypeOfRoles as links.
     *
     * @param outgoingTypedLinks Outgoing typed links.
     * @param filter             To filter the search.
     * @return List of roles.
     */
    protected List<String> getRoleListOfUserByLink(JSONObject outgoingTypedLinks, String filter) {

        List<String> tempList = new LinkedList<>();
        JSONArray linkSpecifiers = (JSONArray) outgoingTypedLinks.get(AWSConstants.TYPEDLINK_SPECIFIERS);
        if (!linkSpecifiers.isEmpty()) {
            for (Object linkSpecifier : linkSpecifiers) {
                JSONObject keyValue = (JSONObject) linkSpecifier;
                JSONArray attributes = (JSONArray) keyValue.get(AWSConstants.IDENTITY_ATTRIBUTE_VALUES);
                for (Object attribute : attributes) {
                    JSONObject key = (JSONObject) attribute;
                    Object attributeName = key.get(AWSConstants.ATTRIBUTE_NAME);
                    JSONObject attributeValue = (JSONObject) key.get(AWSConstants.VALUE);
                    String roleName = attributeValue.get(AWSConstants.STRING_VALUE).toString();
                    if (attributeName.equals(roleNameAttribute) && matchFilter(roleName, filter)) {
                        tempList.add(roleName);
                    }
                }
            }
        }
        return tempList;
    }

    /**
     * Update the role list of a user.
     *
     * @param userName     of user to update.
     * @param deletedRoles send this param fill with if want to remove role from user.
     * @param newRoles     send this paramfill with if want to add role to user.
     * @throws UserStoreException if any error occurred.
     */
    @Override
    public void doUpdateRoleListOfUser(String userName, String[] deletedRoles, String[] newRoles)
            throws UserStoreException {

        String[] userNames = userName.split(CarbonConstants.DOMAIN_SEPARATOR);
        if (userNames.length > 1) {
            userName = userNames[1];
        }
        if (deletedRoles != null && deletedRoles.length > 0) {
            for (String roleName : deletedRoles) {
                String selector = pathToRoles + "/" + roleName.trim();
                if (membershipType.equals(AWSConstants.LINK)) {
                    removeUserFromRoleByLink(selector, userName);
                } else if (membershipType.equals(AWSConstants.ATTRIBUTE)) {
                    removeUserFromRoleByAttribute(selector, userName);
                }
            }
            if (membershipType.equals(AWSConstants.ATTRIBUTE)) {
                removeRolesFromUser(userName, deletedRoles);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Roles are un assigned from user: %s successfully.", userName));
        }
        if (newRoles != null && newRoles.length > 0) {
            // Update role list of user.
            updateRolesListOfUser(userName, newRoles);
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("New roles are assign to user: %s successfully.", userName));
        }
    }

    /**
     * Remove a list of roles from user, when we use MembershipTypeOfRoles as attribute.
     *
     * @param userName     of user to update.
     * @param deletedRoles List of roles to remove from user.
     * @throws UserStoreException If error occurred.
     */
    protected void removeRolesFromUser(String userName, String[] deletedRoles) throws UserStoreException {

        Map<String, String> map = new HashMap<>();
        String selector = pathToUsers + "/" + userName;
        String existingRoles = getAttributeValue(facetNameOfUser, selector, membershipAttribute);
        if (StringUtils.isNotEmpty(existingRoles)) {
            List<String> list = new LinkedList<>(Arrays.asList(existingRoles.split(",")));
            list.removeAll(Arrays.asList(deletedRoles));

            map.put(membershipAttribute, String.join(",", list));
            awsActions.updateObjectAttributes(AWSConstants.CREATE_OR_UPDATE, facetNameOfUser, selector, map);
        }
    }

    /**
     * Remove user from role, when we use MembershipTypeOfRoles as attribute.
     *
     * @param selector Path of an object in the tree structure.
     * @param userName User name.
     * @throws UserStoreException If error occurred.
     */
    protected void removeUserFromRoleByAttribute(String selector, String userName) throws UserStoreException {

        Map<String, String> map = new HashMap<>();
        String existingUsers = getAttributeValue(facetNameOfRole, selector, memberOfAttribute);
        if (StringUtils.isNotEmpty(existingUsers) && existingUsers.contains(userName)) {
            List<String> list = new LinkedList<>(Arrays.asList(existingUsers.split(",")));
            list.remove(userName);

            map.put(memberOfAttribute, String.join(",", list));
            awsActions.updateObjectAttributes(AWSConstants.CREATE_OR_UPDATE, facetNameOfRole, selector, map);
        }
    }

    /**
     * Remove user from role, when we use MembershipTypeOfRoles as link.
     *
     * @param selector Path of an object in the tree structure.
     * @param userName User name.
     * @throws UserStoreException If error occurred.
     */
    protected void removeUserFromRoleByLink(String selector, String userName) throws UserStoreException {

        JSONObject incomingTypedLinks = awsActions.listIncomingTypedLinks(typedLinkFacetName, selector);
        Object object = incomingTypedLinks.get(AWSConstants.LINK_SPECIFIERS);
        if (object != null) {
            JSONArray linkSpecifiers = (JSONArray) object;
            for (Object linkSpecifier : linkSpecifiers) {
                JSONObject keyValue = (JSONObject) linkSpecifier;
                JSONArray identityAttributeValues = (JSONArray) keyValue.get(AWSConstants.IDENTITY_ATTRIBUTE_VALUES);
                if (isUserNameExistInLink(identityAttributeValues, userName)) {
                    String detachPayload = AWSConstants.TYPED_LINK_SPECIFIER + keyValue.toJSONString() + "}";
                    int statusCode = awsActions.detachTypedLink(detachPayload);
                    if (statusCode != 200) {
                        log.error(AWSConstants.ERROR_WHILE_DETACH_TYPED_LINK + keyValue.toJSONString());
                    }
                    break;
                }
            }
        }
    }

    /**
     * Check whether provided username is exist in the typed link or not.
     *
     * @param identityAttributeValues Attributes list.
     * @param userName                User name.
     * @return Boolean.
     */
    protected boolean isUserNameExistInLink(JSONArray identityAttributeValues, String userName) {

        for (Object attribute : identityAttributeValues) {
            JSONObject key = (JSONObject) attribute;
            Object attributeName = key.get(AWSConstants.ATTRIBUTE_NAME);
            JSONObject attributeValue = (JSONObject) key.get(AWSConstants.VALUE);
            if (attributeName.equals(userNameAttribute) &&
                    attributeValue.get(AWSConstants.STRING_VALUE).equals(userName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Update the user list mapped to a role.
     *
     * @param roleName     of user to update.
     * @param deletedUsers send this param fill with if want to remove user from role.
     * @param newUsers     send this paramfill with if want to add user to role.
     * @throws UserStoreException if any error occurred.
     */
    @Override
    public void doUpdateUserListOfRole(String roleName, String[] deletedUsers, String[] newUsers)
            throws UserStoreException {

        Map<String, String> map = new HashMap<>();
        if (deletedUsers != null && deletedUsers.length > 0) {
            for (String userName : deletedUsers) {
                String selector = pathToUsers + "/" + userName.trim();
                if (membershipType.equals(AWSConstants.LINK)) {
                    removeRoleFromUserByLink(selector, roleName);
                } else if (membershipType.equals(AWSConstants.ATTRIBUTE)) {
                    removeRoleFromUserByAttribute(selector, roleName);
                }
            }
            if (membershipType.equals(AWSConstants.ATTRIBUTE)) {
                String selector = pathToRoles + "/" + roleName;
                String existingUsers = getAttributeValue(facetNameOfRole, selector, memberOfAttribute);
                if (StringUtils.isNotEmpty(existingUsers)) {
                    List<String> list = new LinkedList<>(Arrays.asList(existingUsers.split(",")));
                    list.removeAll(Arrays.asList(deletedUsers));

                    map.put(memberOfAttribute, String.join(",", list));
                    awsActions.updateObjectAttributes(AWSConstants.CREATE_OR_UPDATE, facetNameOfRole, selector, map);
                }
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Users are un assigned from role: %s successfully.", roleName));
        }
        if (newUsers != null && newUsers.length > 0) {
            // Update user list of role.
            updateUserListOfRole(newUsers, roleName);
        }
    }

    /**
     * Remove role from user, when we use MembershipTypeOfRoles as attribute.
     *
     * @param selector Path of an object in the tree structure.
     * @param roleName Role name.
     * @throws UserStoreException If error occurred.
     */
    protected void removeRoleFromUserByAttribute(String selector, String roleName) throws UserStoreException {

        Map<String, String> map = new HashMap<>();
        String existingRoles = getAttributeValue(facetNameOfUser, selector, membershipAttribute);
        if (StringUtils.isNotEmpty(existingRoles) && existingRoles.contains(roleName)) {
            List<String> list = new LinkedList<>(Arrays.asList(existingRoles.split(",")));
            list.remove(roleName);

            map.put(membershipAttribute, String.join(",", list));
            awsActions.updateObjectAttributes(AWSConstants.CREATE_OR_UPDATE, facetNameOfUser, selector, map);
        }
    }

    /**
     * Remove role from user, when we use MembershipTypeOfRoles as link.
     *
     * @param selector Path of an object in the tree structure.
     * @param roleName Role name.
     * @throws UserStoreException If error occurred.
     */
    protected void removeRoleFromUserByLink(String selector, String roleName) throws UserStoreException {

        JSONObject outgoingTypedLinks = awsActions.listOutgoingTypedLinks(typedLinkFacetName, selector);
        if (outgoingTypedLinks.get(AWSConstants.TYPEDLINK_SPECIFIERS) != null) {
            JSONArray typedLinkSpecifiers = (JSONArray) outgoingTypedLinks.get(
                    AWSConstants.TYPEDLINK_SPECIFIERS);
            for (Object typedLinkSpecifier : typedLinkSpecifiers) {
                JSONObject keyValue = (JSONObject) typedLinkSpecifier;
                JSONArray identityAttributeValues = (JSONArray) keyValue.get(AWSConstants.IDENTITY_ATTRIBUTE_VALUES);
                if (isRoleNameExistInLink(identityAttributeValues, roleName)) {
                    String detachPayload = AWSConstants.TYPED_LINK_SPECIFIER + keyValue.toJSONString() + "}";
                    int statusCode = awsActions.detachTypedLink(detachPayload);
                    if (statusCode != 200) {
                        log.error(AWSConstants.ERROR_WHILE_DETACH_TYPED_LINK + keyValue.toJSONString());
                    }
                    break;
                }
            }
        }
    }

    /**
     * Check whether provided rolename is exist in the typed link or not.
     *
     * @param roleName                Role name.
     * @param identityAttributeValues Attributes list.
     * @return Boolean.
     */
    protected boolean isRoleNameExistInLink(JSONArray identityAttributeValues, String roleName) {

        for (Object attribute : identityAttributeValues) {
            JSONObject key = (JSONObject) attribute;
            Object attributeName = key.get(AWSConstants.ATTRIBUTE_NAME);
            JSONObject attributeValue = (JSONObject) key.get(AWSConstants.VALUE);
            if (attributeName.equals(roleNameAttribute) &&
                    attributeValue.get(AWSConstants.STRING_VALUE).equals(roleName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Create context of given role.
     *
     * @param roleName Role name to create context.
     * @return RoleContext created for given role.
     */
    @Override
    protected RoleContext createRoleContext(String roleName) {

        JDBCRoleContext searchCtx = new JDBCRoleContext();
        String[] roleNameParts = roleName.split(UserCoreConstants.TENANT_DOMAIN_COMBINER);
        int tenantId;
        if (roleNameParts.length > 1) {
            tenantId = Integer.parseInt(roleNameParts[1]);
            searchCtx.setTenantId(tenantId);
        } else {
            tenantId = this.tenantId;
            searchCtx.setTenantId(tenantId);
        }

        searchCtx.setRoleName(roleNameParts[0]);
        return searchCtx;
    }

    /**
     * Get realm configuration.
     *
     * @return RealmConfiguration of logged in users.
     */
    @Override
    public RealmConfiguration getRealmConfiguration() {

        return this.realmConfig;
    }

    /**
     * Get user list from provided properties.
     *
     * @param property    Property name.
     * @param value       of property name.
     * @param profileName where property belongs to.
     * @return String[] of users.
     */
    @Override
    public String[] getUserListFromProperties(String property, String value, String profileName)
            throws UserStoreException {

        if (StringUtils.isEmpty(property) || StringUtils.isEmpty(value)) {
            return new String[0];
        }
        Set<String> userList = new LinkedHashSet<>();
        String nextToken = null;
        do {
            JSONObject objectChildrens = awsActions.listObjectChildren(nextToken, pathToUsers);
            Object token = objectChildrens.get(AWSConstants.NEXT_TOKEN);
            nextToken = (token != null) ? token.toString() : null;
            Object object = objectChildrens.get(AWSConstants.CHILDREN);
            JSONObject childrens = (object != null) ? (JSONObject) object : null;
            getUserList(userList, childrens, property, value);
        } while (StringUtils.isNotEmpty(nextToken));

        return userList.toArray(new String[userList.size()]);
    }

    /**
     * Get filtered user list by properties.
     *
     * @param userList  Filtered users by properties.
     * @param childrens Contain all users.
     * @param property  Property name.
     * @param value     of property name.
     * @throws UserStoreException If error occurred.
     */
    protected void getUserList(Set<String> userList, JSONObject childrens, String property, String value)
            throws UserStoreException {

        if (childrens != null) {
            for (Object key : childrens.keySet()) {
                String keyValue = pathToUsers + "/" + key.toString();
                JSONObject objectAttributes = awsActions.listObjectAttributes(facetNameOfUser, keyValue);
                JSONArray attributes = (JSONArray) objectAttributes.get(AWSConstants.ATTRIBUTES);
                for (Object attribute : attributes) {
                    JSONObject keyVal = (JSONObject) attribute;
                    Object propertyName = ((JSONObject) keyVal.get(AWSConstants.KEY)).get(AWSConstants.NAME);
                    Object propertyValue = ((JSONObject) keyVal.get(AWSConstants.VALUE)).get(AWSConstants.STRING_VALUE);
                    if (propertyName.equals(property) && propertyValue.equals(value)) {
                        userList.add(key.toString());
                    }
                }
            }
        }
    }

    /**
     * Get shared role names of user store.
     *
     * @param tenantDomain of currently logged in.
     * @param filter       to filter the search.
     * @param maxItemLimit to display per page.
     * @return String[] of shared roles.
     * @throws UserStoreException if any exception occurred.
     */
    @Override
    protected String[] doGetSharedRoleNames(String tenantDomain, String filter, int maxItemLimit)
            throws UserStoreException {

        throw new UserStoreException("doGetSharedRoleNames not implemented for AWSUserStoreManager");
    }

    /**
     * Check whether the user in given role.
     *
     * @param userName to filter the search.
     * @param roleName to display per page.
     * @return boolean status.
     * @throws UserStoreException if any exception occurred.
     */
    @Override
    public boolean doCheckIsUserInRole(String userName, String roleName) throws UserStoreException {

        String[] roles = doGetExternalRoleListOfUser(userName, "*");
        if (roles != null) {
            for (String role : roles) {
                if (role.equalsIgnoreCase(roleName)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Get all shared role list of user.
     *
     * @param userName     of user to get shared role list.
     * @param filter       if any filter.
     * @param tenantDomain of currently logged in.
     * @throws UserStoreException if any exception occurred.
     */
    @Override
    protected String[] doGetSharedRoleListOfUser(String userName, String tenantDomain, String filter)
            throws UserStoreException {

        throw new UserStoreException("doGetSharedRoleListOfUser not implemented for AWSUserStoreManager");
    }

    /**
     * We do not have multiple profile support with AWS.
     *
     * @return String[] of profile names.
     */
    @Override
    public String[] getAllProfileNames() {

        return new String[]{UserCoreConstants.DEFAULT_PROFILE};
    }

    /**
     * Check the status if read only.
     *
     * @return boolean status.
     */
    @Override
    public boolean isReadOnly() {

        return "true".equalsIgnoreCase(realmConfig.getUserStoreProperty(
                UserCoreConstants.RealmConfig.PROPERTY_READ_ONLY));
    }

    /**
     * get user id of given user
     *
     * @param username to find userId
     * @return int userId
     * @throws UserStoreException if any exception occurred
     */
    @Override
    public int getUserId(String username) throws UserStoreException {

        throw new UserStoreException("Invalid operation");
    }

    /**
     * Get tenantId of given user.
     *
     * @param username to find tenantId.
     * @return int tenantId.
     */
    @Override
    public int getTenantId(String username) throws UserStoreException {

        throw new UserStoreException("Invalid operation");
    }

    /**
     * Get currently logged in tenantId.
     *
     * @return int tenantId.
     */
    @Override
    public int getTenantId() {

        return this.tenantId;
    }

    /**
     * Get properties of given tenant.
     *
     * @param tenant to  search for properties.
     * @return Map of properties.
     */
    @Override
    public Map<String, String> getProperties(org.wso2.carbon.user.api.Tenant tenant) {

        return getProperties((Tenant) tenant);
    }

    /**
     * This method is to check whether multiple profiles are allowed with a particular user-store.
     * Currently, AWS user store allows multiple profiles. Hence return true.
     *
     * @return Boolean status of multiple profile.
     */
    @Override
    public boolean isMultipleProfilesAllowed() {

        return false;
    }

    @Override
    public void addRememberMe(String userName, String token) throws org.wso2.carbon.user.api.UserStoreException {

        throw new UserStoreException("addRememberMe not implemented for AWSUserStoreManager");
    }

    @Override
    public boolean isValidRememberMeToken(String userName, String token)
            throws org.wso2.carbon.user.api.UserStoreException {

        throw new UserStoreException("isValidRememberMeToken not implemented for AWSUserStoreManager");
    }

    /**
     * Check whether the bulk import support or not.
     *
     * @return boolean status.
     */
    @Override
    public boolean isBulkImportSupported() {

        return Boolean.valueOf(realmConfig.getUserStoreProperty("IsBulkImportSupported"));
    }

    /**
     * Set user claim value of registered user in user store.
     *
     * @param userName    of registered user.
     * @param claimValue  of user to set.
     * @param claimURI    of user claim.
     * @param profileName of user claims belongs to.
     * @throws UserStoreException if any error occurred.
     */
    @Override
    public void doSetUserClaimValue(String userName, String claimURI, String claimValue, String profileName)
            throws UserStoreException {

        // If user name contains domain name, remove domain name
        String[] userNames = userName.split(CarbonConstants.DOMAIN_SEPARATOR);
        if (userNames.length > 1) {
            userName = userNames[1];
        }
        Map<String, String> map = new HashMap<>();
        String attributeName;
        try {
            attributeName = getClaimAtrribute(claimURI, userName, null);
        } catch (org.wso2.carbon.user.api.UserStoreException e) {
            throw new UserStoreException(AWSConstants.ERROR_WHILE_GETTING_CLAIM_ATTRIBUTE + userName, e);
        }

        if (log.isDebugEnabled()) {
            log.debug(String.format("Processing user claim with Claim URI: %s. Mapped attribute: %s. " +
                    "Attribute value: %s", claimURI, attributeName, claimValue));
        }
        map.put(attributeName, claimValue);
        awsActions.updateObjectAttributes(AWSConstants.CREATE_OR_UPDATE, facetNameOfUser, pathToUsers + "/" + userName,
                map);
    }

    /**
     * Set user claim values of registered user in user store.
     *
     * @param userName    of registered user.
     * @param claims      of user to set.
     * @param profileName of user claims belongs to.
     * @throws UserStoreException if any error occurred.
     */
    @Override
    public void doSetUserClaimValues(String userName, Map<String, String> claims, String profileName)
            throws UserStoreException {

        Map<String, String> map = getClaimAttributes(userName, claims);
        awsActions.updateObjectAttributes(AWSConstants.CREATE_OR_UPDATE, facetNameOfUser, pathToUsers + "/" + userName,
                map);
    }

    /**
     * Get user claim values of registered user.
     *
     * @param userName Username.
     * @param claims   of user to set.
     * @return claim values.
     * @throws UserStoreException if any error occurred.
     */
    private Map<String, String> getClaimAttributes(String userName, Map<String, String> claims)
            throws UserStoreException {

        if (log.isDebugEnabled()) {
            log.debug("Processing user claims");
        }
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<String, String> entry : claims.entrySet()) {
            // Needs to get attribute name from claim mapping
            String claimURI = entry.getKey();
            String attributeName;
            try {
                attributeName = getClaimAtrribute(claimURI, userName, null);
            } catch (org.wso2.carbon.user.api.UserStoreException e) {
                throw new UserStoreException(AWSConstants.ERROR_WHILE_GETTING_CLAIM_ATTRIBUTE + userName, e);
            }
            if (log.isDebugEnabled()) {
                log.debug(String.format("Claim URI: %s. Mapped attribute: %s. Attribute value: %s", claimURI,
                        attributeName, claims.get(entry.getKey())));
            }
            map.put(attributeName, claims.get(entry.getKey()));
        }
        return map;
    }

    /**
     * delete user claim value of given user claim
     *
     * @param userName    of user
     * @param claimURI    to delete from user
     * @param profileName where claim belongs to
     * @throws UserStoreException if error occurred
     */
    @Override
    public void doDeleteUserClaimValue(String userName, String claimURI, String profileName) throws UserStoreException {

        Map<String, String> map = new HashMap<>();
        String attributeName;
        try {
            attributeName = getClaimAtrribute(claimURI, userName, null);
        } catch (org.wso2.carbon.user.api.UserStoreException e) {
            throw new UserStoreException(AWSConstants.ERROR_WHILE_GETTING_CLAIM_ATTRIBUTE + userName, e);
        }
        map.put(attributeName, null);
        awsActions.updateObjectAttributes(AWSConstants.DELETE, facetNameOfUser, pathToUsers + "/"
                + userName, map);
    }

    /**
     * delete user claim values of given user claims
     *
     * @param userName    of user
     * @param claims      to delete from user
     * @param profileName where claim belongs to
     * @throws UserStoreException if error occurred
     */
    @Override
    public void doDeleteUserClaimValues(String userName, String[] claims, String profileName)
            throws UserStoreException {

        for (String claimURI : claims) {
            doDeleteUserClaimValue(userName, claimURI, profileName);
        }
    }

    /**
     * get internal role names of given user
     *
     * @param userNames to filter the search
     * @return String[] of internal roles
     * @throws UserStoreException if any exception occurred
     */
    @Override
    protected String[] doGetDisplayNamesForInternalRole(String[] userNames) throws UserStoreException {

        throw new UserStoreException("doGetDisplayNamesForInternalRole not implemented for AWSUserStoreManager");
    }

    /**
     * Get profile names of user.
     *
     * @param userName to search.
     * @return String[] of profile names.
     */
    @Override
    public String[] getProfileNames(String userName) {

        return new String[]{UserCoreConstants.DEFAULT_PROFILE};
    }

    /**
     * Get properties of tenant.
     *
     * @param tenant to  search.
     * @return Map of properties.
     */
    @Override
    public Map<String, String> getProperties(Tenant tenant) {

        return this.realmConfig.getUserStoreProperties();
    }

    /**
     * Load default user store configration properties.
     *
     * @return Properties of default user store.
     */
    @Override
    public Properties getDefaultUserStoreProperties() {

        Properties properties = new Properties();
        properties.setMandatoryProperties(AWSUserStoreProperties.AWS_MANDATORY_PROPERTIES.toArray
                (new Property[AWSUserStoreProperties.AWS_MANDATORY_PROPERTIES.size()]));
        properties.setOptionalProperties(AWSUserStoreProperties.AWS_OPTIONAL_PROPERTIES.toArray
                (new Property[AWSUserStoreProperties.AWS_OPTIONAL_PROPERTIES.size()]));
        properties.setAdvancedProperties(AWSUserStoreProperties.AWS_ADVANCED_PROPERTIES.toArray
                (new Property[AWSUserStoreProperties.AWS_ADVANCED_PROPERTIES.size()]));

        return properties;
    }

    /**
     * Get all user properties belong to provided user profile.
     *
     * @param userName      username of user.
     * @param propertyNames names of properties to get.
     * @param profileName   profile name of user.
     * @return map object of properties.
     */
    @Override
    public Map<String, String> getUserPropertyValues(String userName, String[] propertyNames, String profileName)
            throws UserStoreException {

        Map<String, String> map = new HashMap<>();
        if (propertyNames == null) {
            return map;
        }
        if (log.isDebugEnabled()) {
            log.debug("Requesting attributes :" + Arrays.toString(propertyNames));
        }
        String[] propertyNamesSorted = propertyNames.clone();
        Arrays.sort(propertyNamesSorted);
        JSONObject objectAttributes = awsActions.listObjectAttributes(facetNameOfUser, pathToUsers + "/" + userName);
        JSONArray attributes = (JSONArray) objectAttributes.get(AWSConstants.ATTRIBUTES);
        for (Object attribute : attributes) {
            JSONObject keyVal = (JSONObject) attribute;
            String attributeName = ((JSONObject) keyVal.get(AWSConstants.KEY)).get(AWSConstants.NAME).toString();
            if (Arrays.binarySearch(propertyNamesSorted, attributeName) < 0) {
                continue;
            }
            map.put(attributeName, ((JSONObject) keyVal.get(AWSConstants.VALUE))
                    .get(AWSConstants.STRING_VALUE).toString());
        }
        return map;
    }

    /**
     * Create directory schema, create schema facets, publish schema, create directory, create (user, role,
     * user attribute) objects, create user to role association facet and create user to attribute association facet
     * if already not exists.
     *
     * @throws UserStoreException If error occurred.
     */
    protected void setUpAWSDirectory() throws UserStoreException {

        pathToUsers = realmConfig.getUserStoreProperty(AWSConstants.PATH_TO_USERS);
        pathToRoles = realmConfig.getUserStoreProperty(AWSConstants.PATH_TO_ROLES);
        membershipType = realmConfig.getUserStoreProperty(AWSConstants.MEMBERSHIP_TYPE_OF_ROLES);
        // The Amazon Resource Name (ARN) of the directory.
        String directoryArn = realmConfig.getUserStoreProperty(AWSConstants.DIRECTORY_ARN);
        facetNameOfUser = realmConfig.getUserStoreProperty(AWSConstants.FACET_NAME_OF_USER);
        facetNameOfRole = realmConfig.getUserStoreProperty(AWSConstants.FACET_NAME_OF_ROLE);
        userNameAttribute = realmConfig.getUserStoreProperty(AWSConstants.USER_NAME_ATTRIBUTE);
        passwordAttribute = realmConfig.getUserStoreProperty(AWSConstants.PASS_ATTRIBUTE);
        membershipAttribute = realmConfig.getUserStoreProperty(UserStoreConfigConstants.membershipAttribute);
        roleNameAttribute = realmConfig.getUserStoreProperty(AWSConstants.ROLE_NAME_ATTRIBUTE);
        memberOfAttribute = realmConfig.getUserStoreProperty(UserStoreConfigConstants.memberOfAttribute);
        passwordHashMethod = realmConfig.getUserStoreProperty(AWSConstants.PASS_HASH_METHOD);

        if (!checkDirectoryExist(directoryArn)) {
            throw new UserStoreException(String.format("Couldn't found any directory with DirectoryArn: %s in AWS.",
                    directoryArn));
        }

        //Create group facet.
        String groupFacetName = AWSConstants.GROUP;
        Map<String, String> map = new HashMap<>();
        if (awsActions.getFacetInfo(groupFacetName) == null) {
            map.put(AWSConstants.NAME, AWSConstants.REQUIRED_ALWAYS);
            awsActions.createSchemaFacet(groupFacetName, map);
            map.clear();
        }

        Set<String> objectsPath = new LinkedHashSet<>();
        setObjectPaths(objectsPath, pathToUsers);
        setObjectPaths(objectsPath, pathToRoles);

        for (String path : objectsPath) {
            int lastIndex = path.lastIndexOf('/');
            String parentPath;
            if (lastIndex == 0) {
                parentPath = "/";
            } else {
                parentPath = path.substring(0, lastIndex);
            }
            String objectName = path.substring(lastIndex + 1);
            JSONObject objectInfo = awsActions.getObjectInformation(path);
            if (objectInfo == null) {
                map.put(AWSConstants.NAME, objectName);
                awsActions.createObject(objectName, groupFacetName, parentPath, map);
            }
        }
        typedLinkFacetName = AWSConstants.USER_ROLE_ASSOCIATION;
        if (membershipType.equals(AWSConstants.LINK) &&
                awsActions.getTypedLinkFacetInformation(typedLinkFacetName) == null) {
            List attributes = Arrays.asList(userNameAttribute, roleNameAttribute);
            awsActions.createTypedLinkFacet(typedLinkFacetName, attributes);
        }
    }

    /**
     * Get the list of objects path in the tree structure.
     *
     * @param objectsPath Collection of objects path.
     * @param path        Object path.
     */
    private void setObjectPaths(Set<String> objectsPath, String path) {

        int index = path.indexOf('/', 1);
        while (index >= 0) {
            objectsPath.add(path.substring(0, index));
            index = path.indexOf('/', index + 1);
        }
        objectsPath.add(path);
    }

    protected void addUsersToRole(String[] userList, String roleName) throws UserStoreException {

        Map<String, String> map = new HashMap<>();
        String targetSelector = pathToRoles + "/" + roleName;
        for (String userName : userList) {
            String sourceSelector = pathToUsers + "/" + userName;
            if (AWSConstants.LINK.equals(membershipType)) {
                map.put(userNameAttribute, userName);
                map.put(roleNameAttribute, roleName);
                awsActions.attachTypedLink(sourceSelector, targetSelector, typedLinkFacetName, map);
            } else if (membershipType.equals(AWSConstants.ATTRIBUTE)) {
                String[] roleList = {roleName};
                updateUserWithRoles(roleList, userName);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Users: %s are added to role: %s successfully", Arrays.toString(userList),
                    roleName));
        }
    }

    /**
     * Assign role list to user.
     *
     * @param userName User name.
     * @param roleList Role list.
     * @throws UserStoreException If error occurred.
     */
    protected void addRolesToUser(String userName, String[] roleList) throws UserStoreException {

        String sourceSelector = pathToUsers + "/" + userName;
        for (String role : roleList) {
            String targetSelector = pathToRoles + "/" + role;
            assignUserToRole(userName, role, sourceSelector, targetSelector);
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Roles: %s are added to user: %s successfully", Arrays.toString(roleList),
                    userName));
        }
    }

    /**
     * Map the role to user list.
     *
     * @param userList The username of the user the roles need to be added to.
     * @param roleName The list of roles that needs to be mapped against the user.
     * @throws UserStoreException If error occurred.
     */
    protected void updateUserListOfRole(String[] userList, String roleName) throws UserStoreException {

        addUsersToRole(userList, roleName);
        if (membershipType.equals(AWSConstants.ATTRIBUTE)) {
            updateRoleWithUsers(userList, roleName);
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Users: %s are added to role: %s successfully", Arrays.toString(userList),
                    roleName));
        }
    }

    private void updateRoleWithUsers(String[] userList, String roleName) throws UserStoreException {

        Map<String, String> map = new HashMap<>();
        String targetSelector = pathToRoles + "/" + roleName;
        StringBuilder users = new StringBuilder(String.join(",", userList));
        String existingUsers = getAttributeValue(facetNameOfRole, targetSelector, memberOfAttribute);
        if (StringUtils.isNotEmpty(existingUsers)) {
            users.append(",").append(existingUsers);
        }
        map.put(memberOfAttribute, users.toString());
        awsActions.updateObjectAttributes(AWSConstants.CREATE_OR_UPDATE, facetNameOfRole, targetSelector, map);
    }

    /**
     * Map the user to role list.
     *
     * @param userName The username of the user the roles need to be added to.
     * @param roleList The list of roles that needs to be mapped against the user.
     * @throws UserStoreException If error occurred.
     */
    protected void updateRolesListOfUser(String userName, String[] roleList) throws UserStoreException {

        addRolesToUser(userName, roleList);
        if (membershipType.equals(AWSConstants.ATTRIBUTE)) {
            updateUserWithRoles(roleList, userName);
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Roles: %s are added to user: %s successfully", Arrays.toString(roleList),
                    userName));
        }
    }

    private void updateUserWithRoles(String[] roleList, String userName) throws UserStoreException {

        Map<String, String> map = new HashMap<>();
        String sourceSelector = pathToUsers + "/" + userName;
        StringBuilder roles = new StringBuilder(String.join(",", roleList));
        // Get already existing role list the append this list then update the attribute
        String existingRoles = getAttributeValue(facetNameOfUser, sourceSelector, membershipAttribute);
        if (StringUtils.isNotEmpty(existingRoles)) {
            roles.append(",").append(existingRoles);
        }
        map.put(membershipAttribute, roles.toString());
        awsActions.updateObjectAttributes(AWSConstants.CREATE_OR_UPDATE, facetNameOfUser, sourceSelector, map);
    }

    /**
     * Assign user to role.
     *
     * @param userName       User name.
     * @param role           Role name.
     * @param sourceSelector Path of an source object in the tree structure.
     * @param targetSelector Path of an target object in the tree structure.
     * @throws UserStoreException If error occurred.
     */
    protected void assignUserToRole(String userName, String role, String sourceSelector, String targetSelector)
            throws UserStoreException {

        Map<String, String> map = new HashMap<>();
        if (AWSConstants.LINK.equals(membershipType)) {
            map.put(userNameAttribute, userName);
            map.put(roleNameAttribute, role);
            awsActions.attachTypedLink(sourceSelector, targetSelector, typedLinkFacetName, map);
        } else if (AWSConstants.ATTRIBUTE.equals(membershipType)) {
            String[] userList = {userName};
            updateRoleWithUsers(userList, role);
        }
    }

    /**
     * Get attribute value for a particular attribute.
     *
     * @param facetName       Name of the facet.
     * @param objectReference The reference that identifies the object in the directory structure.
     * @param attributeKey    Attribute name for which the attribute value is needed.
     * @return Value of the attribute name.
     * @throws UserStoreException If error occurred.
     */
    protected String getAttributeValue(String facetName, String objectReference, String attributeKey)
            throws UserStoreException {

        JSONObject objectAttributes = awsActions.listObjectAttributes(facetName, objectReference);
        JSONArray attributes = (JSONArray) objectAttributes.get(AWSConstants.ATTRIBUTES);
        String attributeValue = null;
        for (Object attribute : attributes) {
            JSONObject keyVal = (JSONObject) attribute;
            JSONObject key = (JSONObject) keyVal.get(AWSConstants.KEY);
            JSONObject value = (JSONObject) keyVal.get(AWSConstants.VALUE);
            if (attributeKey.equals(key.get(AWSConstants.NAME))) {
                attributeValue = value.get(AWSConstants.STRING_VALUE).toString();
                break;
            }
        }
        return attributeValue;
    }

    /**
     * Check whether the directory is exist in AWS or not.
     *
     * @param directoryArn The Amazon Resource Name (ARN).
     * @return Boolean
     * @throws UserStoreException If error occurred.
     */
    protected boolean checkDirectoryExist(String directoryArn) throws UserStoreException {

        String nextToken = null;
        do {
            JSONObject directoryObjects = awsActions.listDirectories(nextToken);
            if (directoryObjects != null) {
                Object token = directoryObjects.get(AWSConstants.NEXT_TOKEN);
                nextToken = (token != null) ? token.toString() : null;
                JSONArray directories = (JSONArray) directoryObjects.get(AWSConstants.DIRECTORIES);
                for (Object directory : directories) {
                    Object arn = ((JSONObject) directory).get(AWSConstants.DIRECTORY_ARN);
                    if (arn.equals(directoryArn)) {
                        return true;
                    }
                }
            }
        } while (StringUtils.isNotEmpty(nextToken));
        return false;
    }

    /**
     * Common method to throw exceptions. This will only expect one parameter.
     *
     * @param msg error message as a string.
     * @throws UserStoreException If error occurred.
     */
    protected void handleException(String msg) throws UserStoreException {

        throw new UserStoreException(msg);
    }
}
