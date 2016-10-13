/**
 * Copyright (c) 2016 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trustedanalytics.scheduler;


import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.test.context.ContextConfiguration;
import org.trustedanalytics.scheduler.filesystem.HdfsConfigProviderFromEnv;
import org.trustedanalytics.scheduler.oozie.TestConfiguration;
import org.trustedanalytics.scheduler.utils.FileLoader;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
@ContextConfiguration(classes=TestConfiguration.class)
public class HdfsConfigProviderFromEnvTest {

    public static final String RESOURCE_MANAGER_HOST1 = "ip-10-0-3-35.ec2.internal:8032";

    @Test
    public void should_return_first_rm() throws IOException {
        MockEnvironment mockEnvironment = getMockEnvironment();
        HdfsConfigProviderFromEnv provider = new HdfsConfigProviderFromEnv(mockEnvironment);
        String rm = provider.getResourceManager();
        assertEquals(RESOURCE_MANAGER_HOST1,rm);
    }

    @Test
    public void should_return_rm_no_ha() throws IOException {
        MockEnvironment mockEnvironment = getMockEnvironment();
        mockEnvironment.setProperty("yarn.conf.dir",FileLoader.getResourceFilePath("no_ha_yarn_conf"));
        HdfsConfigProviderFromEnv provider = new HdfsConfigProviderFromEnv(mockEnvironment);
        String rm = provider.getResourceManager();
        assertEquals(RESOURCE_MANAGER_HOST1, rm);
    }


    @Test
    public void should_return_empty() throws IOException {
        MockEnvironment mockEnvironment = getMockEnvironment();
        mockEnvironment.setProperty("yarn.conf.dir",FileLoader.getResourceFilePath("empty_conf"));
        HdfsConfigProviderFromEnv provider = new HdfsConfigProviderFromEnv(mockEnvironment);
        String rm = provider.getResourceManager();
        assertTrue(StringUtils.isEmpty(rm));
    }

    private MockEnvironment getMockEnvironment() {
        MockEnvironment mockEnvironment = new MockEnvironment();
        mockEnvironment.setProperty("krb.realm","REALM");
        mockEnvironment.setProperty("krb.kdc","KDC");
        mockEnvironment.setProperty("yarn.conf.dir", FileLoader.getResourceFilePath("yarn"));
        mockEnvironment.setProperty("hadoop.conf.dir", FileLoader.getResourceFilePath("hadoop"));
        return mockEnvironment;
    }

}
