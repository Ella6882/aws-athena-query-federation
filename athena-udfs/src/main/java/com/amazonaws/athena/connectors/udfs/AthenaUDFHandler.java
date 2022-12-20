/*-
 * #%L
 * athena-udfs
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.udfs;

import com.amazonaws.athena.connector.lambda.handlers.UserDefinedFunctionHandler;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClient;
import com.google.common.annotations.VisibleForTesting;
import nl.basjes.parse.useragent.UserAgent;
import nl.basjes.parse.useragent.UserAgentAnalyzer;

import java.util.HashMap;
import java.util.Map;

public class AthenaUDFHandler
        extends UserDefinedFunctionHandler 
{
    private static final String SOURCE_TYPE = "athena_common_udfs";

    private final CachableSecretsManager cachableSecretsManager;
    private UserAgentAnalyzer uaa;

    public AthenaUDFHandler() 
    {
        this(new CachableSecretsManager(AWSSecretsManagerClient.builder().build()));

        System.out.println("before");
        this.uaa = UserAgentAnalyzer.
                newBuilder().
                hideMatcherLoadStats().
                // withField will omit sections the do not pertain to the desired fields.
                // It will sometimes pull other fields outside of the list if they are required
                // for intermediate results (as some fields are dependent on each other).
                withField(UserAgent.DEVICE_CLASS).
                withField(UserAgent.AGENT_NAME).
                withField(UserAgent.AGENT_VERSION_MAJOR).
                withField(UserAgent.AGENT_VERSION).
                withCache(10000).
                build();
        System.out.println("after");
    }

    @VisibleForTesting
    AthenaUDFHandler(CachableSecretsManager cachableSecretsManager) 
    {
        super(SOURCE_TYPE);
        this.cachableSecretsManager = cachableSecretsManager;
        this.uaa = null;
    }

    public Map<String, String> parseuseragent(String useragent) 
    {
        UserAgent agent = uaa.parse(useragent);
        Map<String, String> userAgentDetails = new HashMap<String, String>();

        userAgentDetails.put("DEVICE_CLASS", agent.getValue(UserAgent.DEVICE_CLASS));
        userAgentDetails.put("AGENT_NAME", agent.getValue(UserAgent.AGENT_NAME));
        userAgentDetails.put("AGENT_VERSION_MAJOR", agent.getValue(UserAgent.AGENT_VERSION_MAJOR));
        userAgentDetails.put("AGENT_VERSION", agent.getValue(UserAgent.AGENT_VERSION));

        return userAgentDetails;
    }
}
