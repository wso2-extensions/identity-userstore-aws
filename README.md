# AWS User Store Extension for WSO2 IS

This extension will allow users to use AWS cloud directory [API Doc](https://docs.aws.amazon.com/clouddirectory/latest/developerguide/what_is_cloud_directory.html) as
the user store for WSO2 IS using [REST API](https://docs.aws.amazon.com/directoryservice/latest/APIReference/welcome.html).
Cloud Directory is a specialized graph-based directory store that provides a foundational building block for developers. With Cloud Directory, we can organize directory objects into multiple hierarchies to support many organizational pivots and relationships across directory information.
This AWS user store extension can be used as both primary and secondary user store for WSO2 IS. This extension is compatible with IS version 5.5.0.


## Steps to Configure

1. Download the AWS user store extension jar from [WSO2 store](https://store.wso2.com/store/assets/isconnector/list)
2. Copy the downloaded jar and put it into the `IS_HOME/repository/components/dropins` folder.
3. Finally, open a terminal, navigate to the `IS_HOME/bin` folder and start the IS server by executing the following command
   ```bash
      ./wso2server.sh
   ```

Now you have successfully added the AWS user store extension to the WSO2 IS. You should see AWS user store listed along with other user stores IS management console UI. Using that you can create a AWS secondary user store and perform your user management operations.

### Configuring AWS as the Primary User Store

The above configurations are good enough for you to use the AWS as a secondary user store manager. However, in order to use the AWS as the primary user store of WSO2 IS you need some additional configurations as follow.

4. After following steps 1-2, prior to start the IS server, add the following in the `user-mgt.xml` file of WSO2 IS. You can find this file inside `IS_HOME/repository/conf` folder.
   Make sure to replace the following properties.

##### user-mgt.xml

```xml
<UserStoreManager class="org.wso2.carbon.aws.user.store.mgt.AWSUserStoreManager">
    <Property name="TenantManager">org.wso2.carbon.user.core.tenant.JDBCTenantManager</Property>
    <Property name="AccessKeyID">xxxxxxxxxxxxxxxxxxxxxxxxx</Property>
    <Property name="SecretAccessKey">xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx</Property>
    <Property name="Region">us-west-2</Property>
    <Property name="APIVersion">2017-01-11</Property>
    <Property name="PathToUsers">/com/users</Property>
    <Property name="PathToRoles">/com/roles</Property>
    <Property name="MembershipTypeOfRoles">link</Property>
    <Property name="DirectoryArn">arn:aws:clouddirectory:us-west-2:610968236798:directory/ASc_ZQAllU0Ot0_vpmXmwF4</Property>
    <Property name="SchemaArn">arn:aws:clouddirectory:us-west-2:610968236798:directory/ASc_ZQAllU0Ot0_vpmXmwF4/schema/userstoreSchema/1.0</Property>
    <Property name="FacetNameOfUser">USERS</Property>
    <Property name="FacetNameOfRole">ROLES</Property>
    <Property name="ReadOnly">false</Property>
    <Property name="ReadGroups">true</Property>
    <Property name="WriteGroups">true</Property>
    <Property name="Disabled">false</Property>
    <Property name="UserNameAttribute">UserName</Property>
    <Property name="PasswordAttribute">Password</Property>
    <Property name="MembershipAttribute">Member</Property>
    <Property name="RoleNameAttribute">RoleName</Property>
    <Property name="MemberOfAttribute">MemberOf</Property>
    <Property name="UserNameJavaRegEx">[a-zA-Z0-9._\-|//]{3,30}$</Property>
    <Property name="UserNameJavaScriptRegEx">^[\S]{3,30}$</Property>
    <Property name="UsernameJavaRegExViolationErrorMsg">Username pattern policy violated</Property>
    <Property name="PasswordJavaRegEx">^[\S]{5,30}$</Property>
    <Property name="PasswordJavaScriptRegEx">^[\S]{5,30}$</Property>
    <Property name="PasswordJavaRegExViolationErrorMsg">Password pattern policy violated.</Property>
    <Property name="RoleNameJavaRegEx">[a-zA-Z0-9._-|//]{3,30}$</Property>
    <Property name="RoleNameJavaScriptRegEx">^[\S]{3,30}$</Property>
    <Property name="PasswordHashMethod">PLAIN_TEXT</Property>
    <Property name="MaxUserNameListLength">100</Property>
    <Property name="MaxRoleNameListLength">100</Property>
</UserStoreManager>
```