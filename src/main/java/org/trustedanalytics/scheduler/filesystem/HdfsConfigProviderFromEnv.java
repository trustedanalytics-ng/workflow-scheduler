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
package org.trustedanalytics.scheduler.filesystem;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

@Component
@Profile("cloud")
public class HdfsConfigProviderFromEnv implements HdfsConfigProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsConfigProviderFromEnv.class);
    private static final String AUTHENTICATION_METHOD = "kerberos";
    private static final String AUTHENTICATION_METHOD_PROPERTY = "hadoop.security.authentication";

    @Getter
    private String kdc;
    @Getter
    private String realm;
    @Getter
    private  Configuration hadoopConf;
    @Getter
    private  Configuration yarnConf;

    private String hadoopConfDir;
    private String yarnConfDir;

    @Autowired
    public HdfsConfigProviderFromEnv(Environment env) throws IOException {
        this.kdc = env.getProperty("krb.kdc");
        this.realm = env.getProperty("krb.realm");
        this.hadoopConfDir = env.getProperty("hadoop.conf.dir");
        this.yarnConfDir = env.getProperty("yarn.conf.dir");

        LOGGER.info("KERBEROS KDC: {} ", kdc);
        LOGGER.info("KERBEROS REALM: {} ", realm);
        LOGGER.info("HADOOP conf dir: {}", hadoopConfDir);
        LOGGER.info("YARN conf dir: {}", yarnConfDir);
        hadoopConf = getHadoopConfiguration();
        yarnConf = getYarnConfiguration();
        LOGGER.info("HADOOP config : {}", hadoopConf);
        LOGGER.info("YARN config : {}", yarnConf);
    }

    @Override
    public boolean isKerberosEnabled() {
        return AUTHENTICATION_METHOD.equals(hadoopConf.get(AUTHENTICATION_METHOD_PROPERTY));
    }

    @Override
    public String getHdfsUri() {
        String defaultFs = hadoopConf.get("fs.defaultFS");
        LOGGER.info("Default hadoop fs {} ", defaultFs);
        if (StringUtils.isEmpty(defaultFs)) {
            LOGGER.error("Empty hadoop fs");
        }
        return  defaultFs;
    }

    @Override
    public String getResourceManager() {
        String resourceManager = "";
        String ids = yarnConf.get("yarn.resourcemanager.ha.rm-ids");
        LOGGER.info("Resource manager ids: {}", ids);
        if (StringUtils.isNotEmpty(ids)) {
                String id1 = ids.split(",")[0];
                resourceManager = yarnConf.get("yarn.resourcemanager.address." + id1);
                LOGGER.info("Resource manager from yarn config {}", resourceManager);
            }
        if (StringUtils.isEmpty(resourceManager)) {
            LOGGER.error("Could not determine resource manager host");
        }
        return resourceManager;
    }

    @Override
    public String getHdfsOrgUri(UUID org) {
        return PathTemplate.resolveOrg(getHdfsUri(), org);
    }

    private static class PathTemplate {
        private static final String ORG_PLACEHOLDER = "organization";
        private static final String PLACEHOLDER_PREFIX = "%{";
        private static final String PLACEHOLDER_SUFIX = "}";

        private PathTemplate() {
        }

        private static String resolveOrg(String url, UUID org) {
            ImmutableMap<String, UUID> values = ImmutableMap.of(ORG_PLACEHOLDER, org);
            return new StrSubstitutor(values, PLACEHOLDER_PREFIX, PLACEHOLDER_SUFIX).replace(url);
        }
    }

    private Configuration getHadoopConfiguration() throws IOException {
        LOGGER.info("Reading hadoop conf dir: {}", hadoopConfDir);
        return getHadoopConfiguration(hadoopConfDir);
    }

    private Configuration getYarnConfiguration() throws IOException {
        LOGGER.info("Reading yarn conf from dir: {}", yarnConfDir);
        return getYarnConfiguration(yarnConfDir);
    }

    private Configuration getHadoopConfiguration(String confDir) throws IOException {
        return Arrays.asList("core-site.xml", "hdfs-site.xml").stream()
                .collect(Configuration::new, (c, f) -> c.addResource(new Path(confDir,f)), (c, d) -> {});
    }

    private Configuration getYarnConfiguration(String confDir) throws IOException {
        return Arrays.asList("yarn-site.xml").stream()
                .collect(Configuration::new, (c, f) -> c.addResource(new Path(confDir,f)), (c, d) -> {});
    }
}
