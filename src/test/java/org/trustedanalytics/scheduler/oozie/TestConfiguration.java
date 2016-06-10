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
package org.trustedanalytics.scheduler.oozie;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.trustedanalytics.scheduler.*;
import org.trustedanalytics.scheduler.client.OozieClient;
import org.trustedanalytics.scheduler.config.Database;
import org.trustedanalytics.scheduler.oozie.jobs.sqoop.SqoopJobMapper;
import org.trustedanalytics.scheduler.oozie.serialization.JobContext;
import org.trustedanalytics.scheduler.utils.*;
import rx.Observable;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Properties;

@Configuration
public class TestConfiguration {
    public static final String TEST_JOB_TRACKER = "test_job_tracker";
    public static final String TEST_NAMENODE = "test_namenode";
    public static final String TEST_METASTORE_URL = "test_metastore_url:32158";
    public static final String OOZIE_API_URL = "oozie_api_url";
    public static final String TEST_HADOOP_HOME = "test_hadoop_home";

    /*
     *  create sqoop.metastore property for unit test
     *
     */

    private JobContext jobContext;

    public TestConfiguration() {
        jobContext = JobContext.builder().jobTracker(TEST_JOB_TRACKER)
                                        .nameNode(TEST_NAMENODE)
                                        .sqoopMetastore(TEST_METASTORE_URL)
                                        .oozieApiUrl(OOZIE_API_URL).build();
    }

    @Bean
    public static PropertyPlaceholderConfigurer propertyPlaceholderConfigurer() {
        PropertyPlaceholderConfigurer ppc = new PropertyPlaceholderConfigurer();
        ppc.setIgnoreResourceNotFound(true);
        final Properties properties = new Properties();
        properties.setProperty("sqoop.metastore", TEST_METASTORE_URL);
        properties.setProperty("hadoop.home", TEST_HADOOP_HOME);
        properties.setProperty("job.tracker", TEST_JOB_TRACKER);
        properties.setProperty("oozie.api.url", OOZIE_API_URL);
        ppc.setProperties(properties);
        return ppc;
    }

    @Mock
    private OozieClient oozieClient;

    @Bean
    public OozieService getOozieService() {

        oozieClient = Mockito.mock(OozieClient.class);
        return new OozieService(new InMemoryOrgSpecificSpaceFactory(),
                oozieClient,
                new ConstantJobIdSupplier(),
                new SqoopJobMapper(databaseEngines()),
                jobContext
                );
    }

    @Bean
    public OozieClient getOozieClient() throws IOException {

        return new OozieClient(new MockRestOperationsFactory(), new MockTokenProvider(), jobContext);
    }

    public Observable<Database> databaseEngines() {
        try (final InputStream input = new ClassPathResource("databases.json").getInputStream()) {
            TypeReference<Collection<Database>> type = new TypeReference<Collection<Database>>() {};
            Collection<Database> databases = objectMapper().readValue(input, type);
            return Observable.from(databases);
        } catch (IOException e) {

            throw new IllegalStateException("Unable to load supported databases");
        }
    }

    @Bean
    public ObjectMapper objectMapper() {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        final SimpleModule simpleModule = new SimpleModule();
        simpleModule.addDeserializer(ZoneId.class, new ZoneIdDeserializer());
        simpleModule.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer());

        objectMapper.registerModules(new Jdk8Module(), simpleModule);

        return objectMapper;
    }
}

