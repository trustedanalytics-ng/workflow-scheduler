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
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.trustedanalytics.hadoop.config.client.*;

import java.io.IOException;
import java.util.UUID;

@Component
@Profile("cloud")
public class HdfsConfigProviderFromEnv implements HdfsConfigProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsConfigProviderFromEnv.class);

    private static final String AUTHENTICATION_METHOD = "kerberos";
    private static final String AUTHENTICATION_METHOD_PROPERTY = "hadoop.security.authentication";


    @Getter
    @Value("${krb.kdc}")
    private String kdc;

    @Getter
    @Value("${krb.realm}")
    private String realm;

    @Value("${hadoop.conf.dir}")
    private String hadoopConfDir;

    @Getter
    private  Configuration hadoopConf;

    public HdfsConfigProviderFromEnv() throws IOException {
        hadoopConf = getHadoopConfiguration();
        LOGGER.info("Hadoop config : {}", hadoopConf);
    }

    @Override
    public boolean isKerberosEnabled() {
        return AUTHENTICATION_METHOD.equals(hadoopConf.get(AUTHENTICATION_METHOD_PROPERTY));
    }

    @Override
    public String getHdfsUri() {
        return hadoopConf.get("fs.defaultFS");
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

    private org.apache.hadoop.conf.Configuration getHadoopConfiguration() throws IOException {
        org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
        config.addResource(new Path("/etc/hadoop/" + "core-site.xml"));
        config.addResource(new Path("/etc/hadoop/" + "hdfs-site.xml"));
        return config;
    }
}
