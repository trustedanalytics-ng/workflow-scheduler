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

import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.trustedanalytics.scheduler.oozie.jobs.sqoop.SqoopImport;
import org.trustedanalytics.scheduler.oozie.jobs.sqoop.SqoopImportJob;
import org.trustedanalytics.scheduler.oozie.jobs.sqoop.SqoopScheduledImportJob;
import org.trustedanalytics.scheduler.utils.FileLoader;
import org.trustedanalytics.scheduler.utils.InMemoryOrgSpecificSpace;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=TestConfiguration.class)
public class OozieServiceTest  {

    private static final Logger LOGGER = LoggerFactory.getLogger(OozieServiceTest.class);

    @Autowired
    private Environment env;

    @Autowired
    OozieService oozieService;

    private UUID orgId = UUID.fromString("1981838e-bcc9-4402-95eb-60c7f3ca6fbc");;

    @Before
    public void prepare() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void createValidXMLConfigTestAppendModeUTCCoordinatedJob() throws IOException {
        SqoopScheduledImportJob sqoopScheduledImportJob = getDefaultScheduledSqoopJob();
        sqoopScheduledImportJob.getSqoopImport().setSchema("");
        oozieService.sqoopScheduledImportJob(sqoopScheduledImportJob, orgId);
        validateCoordinatorAndWorkflow("workflow.xml", "coordinator.xml");
    }



    @Test
    public void createValidXMLConfigTestAppendModeNoUsernameDriverDefined() throws IOException {

        SqoopScheduledImportJob sqoopScheduledImportJob = getDefaultScheduledSqoopJob();
        sqoopScheduledImportJob.getSqoopImport().setDriver("FAKE_DRIVER");
        sqoopScheduledImportJob.getSqoopImport().setUsername("");
        sqoopScheduledImportJob.getSqoopImport().setPassword("");
        oozieService.sqoopScheduledImportJob(sqoopScheduledImportJob, orgId);
        validateCoordinatorAndWorkflow("workflow_no_username.xml","coordinator.xml");
    }

    @Test
    public void createValidXMLConfigTestAppendModeLosAngelesTimeCoordinatedJob() throws IOException {
        SqoopScheduledImportJob sqoopScheduledImportJob = getDefaultScheduledSqoopJob();
        String timeZone = "America/Los_Angeles";

        OozieSchedule oozieSchedule = new OozieSchedule(LocalDateTime.of(2077, 7, 4, 1, 15),
                LocalDateTime.of(2077, 7, 6, 1, 15), ZoneId.of(timeZone));
        OozieFrequency frequency = new OozieFrequency("minutes", 10L);
        oozieSchedule.setFrequency(frequency);

        sqoopScheduledImportJob.setSchedule(oozieSchedule);
        sqoopScheduledImportJob.getSqoopImport().setSchema("");

        oozieService.sqoopScheduledImportJob(sqoopScheduledImportJob, orgId);

        validateCoordinatorAndWorkflow("workflow.xml", "coordinator_la_timezone.xml");
    }

    @Test
    public void importModeIncrementalFlagsAreSetCorrectlyCoordinatedJob() throws IOException {
        SqoopScheduledImportJob sqoopScheduledImportJob = getDefaultScheduledSqoopJob();
        sqoopScheduledImportJob.getSqoopImport().setImportMode("incremental");
        sqoopScheduledImportJob.getSqoopImport().setCheckColumn("id");
        oozieService.sqoopScheduledImportJob(sqoopScheduledImportJob, orgId);
        assertTrue(sqoopScheduledImportJob.getSqoopImport().getAppend());
        assertTrue(sqoopScheduledImportJob.getSqoopImport().getIncremental());
        assertFalse(sqoopScheduledImportJob.getSqoopImport().getOverwrite());
    }

    @Test
    public void createValidXMLConfigWithSchemaCoordinatedJob() throws IOException {
        SqoopScheduledImportJob sqoopScheduledImportJob = getDefaultScheduledSqoopJob();
        oozieService.sqoopScheduledImportJob(sqoopScheduledImportJob, orgId);
        validateCoordinatorAndWorkflow("workflow_with_schema.xml", "coordinator.xml");
    }

    @Test
    public void createValidXMLConfigOverwriteModeCoordinatedJob() throws IOException {
        SqoopScheduledImportJob sqoopScheduledImportJob = getDefaultScheduledSqoopJob();
        sqoopScheduledImportJob.getSqoopImport().setOverwrite(true);
        sqoopScheduledImportJob.getSqoopImport().setSchema("");
        oozieService.sqoopScheduledImportJob(sqoopScheduledImportJob, orgId);
        validateCoordinatorAndWorkflow("workflow_overwrite.xml", "coordinator.xml");
    }



    @Test
    public void createValidXMLConfigPostgresDriverCoordinatedJob () throws IOException {
        SqoopScheduledImportJob sqoopScheduledImportJob = getDefaultScheduledSqoopJob();
        sqoopScheduledImportJob.getSqoopImport().setSchema("");
        sqoopScheduledImportJob.getSqoopImport().setJdbcUri("jdbc:postgresql");
        oozieService.sqoopScheduledImportJob(sqoopScheduledImportJob, orgId);
        validateCoordinatorAndWorkflow("workflow_postgresql.xml", "coordinator.xml");
    }

    @Test
    public void createValidXMLConfigWithSchemaJob () throws IOException {
        SqoopImportJob sqoopImportJob = new SqoopImportJob();
        sqoopImportJob.setName("test");

        SqoopImport sqoopImport = new SqoopImport();
        sqoopImport.setJdbcUri("FAKE_JDBC_URI");
        sqoopImport.setTable("table_in_database");
        sqoopImport.setImportMode("append");
        sqoopImport.setUsername("john");
        sqoopImport.setPassword("doe");
        sqoopImport.setSchema("test_schema");
        sqoopImportJob.setSqoopImport(sqoopImport);


        oozieService.sqoopImportJob(sqoopImportJob, orgId);

        String generatedWorkflow = InMemoryOrgSpecificSpace.getWorkflowXml().replaceAll("[ \t\r]","").trim();
        String validWorkflow = FileLoader.readFileResourceNormalized("/job/workflow_with_schema.xml");
        String workflowDiff = StringUtils.difference(generatedWorkflow.trim(), validWorkflow.trim());

        assertTrue(workflowDiff.length() == 0);
    }

    @Test
    public void createValidXMLConfigPostgresDriverJob () throws IOException {
        SqoopImportJob sqoopImportJob = new SqoopImportJob();
        sqoopImportJob.setName("test");

        SqoopImport sqoopImport = new SqoopImport();
        sqoopImport.setJdbcUri("jdbc:postgresql");
        sqoopImport.setTable("table_in_database");
        sqoopImport.setImportMode("append");
        sqoopImport.setUsername("john");
        sqoopImport.setPassword("doe");
        sqoopImportJob.setSqoopImport(sqoopImport);

        oozieService.sqoopImportJob(sqoopImportJob, orgId);

        String generatedWorkflow = InMemoryOrgSpecificSpace.getWorkflowXml().replaceAll("[ \t\r]","").trim();
        String validWorkflow = FileLoader.readFileResourceNormalized("/job/workflow_postgresql.xml");
        String workflowDiff = StringUtils.difference(generatedWorkflow.trim(), validWorkflow.trim());

        assertTrue(workflowDiff.length() == 0);
    }

    private SqoopScheduledImportJob getDefaultScheduledSqoopJob() {
        SqoopScheduledImportJob sqoopScheduledImportJob = new SqoopScheduledImportJob();
        sqoopScheduledImportJob.setName("test");
        OozieSchedule oozieSchedule = new OozieSchedule(LocalDateTime.of(2077, 7, 4, 8, 15),
                LocalDateTime.of(2077,7,6,8,15), ZoneId.of("UTC"));
        OozieFrequency frequency = new OozieFrequency("minutes", 10L);
        oozieSchedule.setFrequency(frequency);
        sqoopScheduledImportJob.setSchedule(oozieSchedule);

        SqoopImport sqoopImport = new SqoopImport();
        sqoopImport.setJdbcUri("FAKE_JDBC_URI");
        sqoopImport.setTable("table_in_database");
        sqoopImport.setImportMode("append");
        sqoopImport.setSchema("my_schema");
        sqoopImport.setUsername("john");
        sqoopImport.setPassword("doe");
        sqoopScheduledImportJob.setSqoopImport(sqoopImport);
        return sqoopScheduledImportJob;
    }

    private void validateCoordinatorAndWorkflow(String workflowXmlFile, String coordXmlFile) {
        String generatedWorkflow = InMemoryOrgSpecificSpace.getWorkflowXml().replaceAll("[ \t\r]","").trim();
        String generatedCoordinator = InMemoryOrgSpecificSpace.getCoordinatorXml().replaceAll("[ \t\r]", "").trim();

        String validWorkflow = FileLoader.readFileResourceNormalized("/scheduledJob/" + workflowXmlFile);
        String validCoordinator = FileLoader.readFileResourceNormalized("/scheduledJob/" + coordXmlFile);

        String workflowDiff = StringUtils.difference(generatedWorkflow.trim(), validWorkflow.trim());
        String coordinatorDiff = StringUtils.difference(generatedCoordinator.trim(),validCoordinator.trim());


        assertTrue(workflowDiff.length() == 0);
        assertTrue(coordinatorDiff.length() == 0);
    }
}