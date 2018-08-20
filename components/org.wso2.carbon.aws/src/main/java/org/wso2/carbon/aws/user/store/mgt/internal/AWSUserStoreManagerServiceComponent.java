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

package org.wso2.carbon.aws.user.store.mgt.internal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.osgi.service.component.ComponentContext;
import org.wso2.carbon.aws.user.store.mgt.AWSUserStoreManager;
import org.wso2.carbon.user.api.UserStoreManager;
import org.wso2.carbon.user.core.service.RealmService;
import org.wso2.carbon.user.core.tracker.UserStoreManagerRegistry;

/**
 * @scr.component name="aws.user.store.mgt.dscomponent" immediate=true
 * @scr.reference name="user.realmservice.default"
 * interface="org.wso2.carbon.user.core.service.RealmService" cardinality="1..1"
 * policy="dynamic" bind="setRealmService"
 * unbind="unsetRealmService"
 */
@SuppressWarnings({"unused", "JavaDoc"})
public class AWSUserStoreManagerServiceComponent {

    private static final Log log = LogFactory.getLog(AWSUserStoreManagerServiceComponent.class);

    /**
     * To activate the AWS OSGI component.
     *
     * @param context ComponentContext
     */
    protected void activate(ComponentContext context) {

        try {
            UserStoreManager awsUserStoreManager = new AWSUserStoreManager();
            context.getBundleContext().registerService(UserStoreManager.class.getName(), awsUserStoreManager, null);

            UserStoreManagerRegistry.init(context.getBundleContext());
            log.info("AWSUserStoreMgtDSComponent activated successfully.");
        } catch (Exception e) {
            log.error("Failed to activate Carbon UserStoreMgtDSComponent ", e);
        }
    }

    /**
     * To de activate the AWS OSGI component.
     *
     * @param context ComponentContext
     */
    protected void deactivate(ComponentContext context) {

        if (log.isDebugEnabled()) {
            log.debug("AWS User Store Manager is deactivated ");
        }
    }

    /**
     * Bind method.
     *
     * @param realmService RealmService
     */
    protected void setRealmService(RealmService realmService) {

        if (log.isDebugEnabled()) {
            log.debug("Set the realmService");
        }
        AWSUserStoreManagerServiceDataHolder.getInstance().setRealmService(realmService);
    }

    /**
     * Unbind method.
     *
     * @param realmService RealmService
     */
    protected void unsetRealmService(RealmService realmService) {

        if (log.isDebugEnabled()) {
            log.debug("Unset the realmService");
        }
        AWSUserStoreManagerServiceDataHolder.getInstance().setRealmService(null);
    }
}