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
package org.trustedanalytics.scheduler.oozie.jobs.sqoop;

import org.hsqldb.lib.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trustedanalytics.scheduler.oozie.jobs.AbstractCommandLine;

import org.apache.commons.lang.StringUtils;

import java.util.Objects;

public class SqoopCommand extends AbstractCommandLine {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqoopCommand.class);
    private final String sqoopMetastore;
    private final String name;

    public SqoopCommand(String name, String sqoopMetastore) {
        this.name = Objects.requireNonNull(name, "Command name is required");
        this.sqoopMetastore = Objects.requireNonNull(sqoopMetastore, "Sqoop metastore is required");
    }

    @Override
    public String name() {
        return name;
    }

    public SqoopCommand create(String jobId, SqoopImport sqoopImport) {
        requiredArgument("--create", jobId);
        requiredArgument("--meta-connect", sqoopMetastore);
        requiredArgument("--");
        requiredArgument("import");
        requiredArgument("--connect", sqoopImport.getJdbcUri());
        requiredArgument("--table", sqoopImport.getTable());
        optionalArgument("--num-mappers", "1");
        optionalArgument("--target-dir", sqoopImport.getTargetDir());
        optionalStringArgument("--check-column", sqoopImport.getCheckColumn(), sqoopImport.getIncremental());
        optionalStringArgument("--last-value", sqoopImport.getLastValue(), sqoopImport.getIncremental());
        optionalArgument("--incremental append", sqoopImport.getIncremental());
        optionalArgument("--append", sqoopImport.getAppend());
        requiredArgument("--connection-param-file", "driver.properties");
        optionalStringArgument("--driver", sqoopImport.getDriver(), !StringUtils.isEmpty(sqoopImport.getDriver()));

        return this;
    }

    public SqoopCommand exec(String jobId, SqoopImport sqoopImport) {
        requiredArgument("--exec", jobId);
        requiredArgument("--meta-connect", sqoopMetastore);

        final String username = sqoopImport.getUsername();

        if(StringUtils.isNotBlank(username)) {
            requiredArgument("--");
            requiredArgument("--username", sqoopImport.getUsername());
            optionalStringArgument("--password", sqoopImport.getPassword(), !StringUtils.isEmpty(sqoopImport.getPassword()));
            requiredArgument(" --");
        }

        LOGGER.info("Schema: {}", sqoopImport.getSchema());

        optionalStringArgument("--schema", sqoopImport.getSchema() + " -- --", !StringUtil.isEmpty(sqoopImport.getSchema()));

        return this;
    }

    public SqoopCommand sqoopImport(SqoopImport sqoopImport) {
        requiredArgument("--connect", sqoopImport.getJdbcUri());
        requiredArgument("--table", sqoopImport.getTable());
        requiredArgument("--username", sqoopImport.getUsername());
        requiredArgument("--password", sqoopImport.getPassword());
        optionalArgument("--num-mappers", "1");
        optionalArgument("--target-dir", sqoopImport.getTargetDir());
        optionalArgument("--append");
        requiredArgument("--connection-param-file", "driver.properties");
        optionalStringArgument("--driver",sqoopImport.getDriver(),!StringUtils.isEmpty(sqoopImport.getDriver()));
        optionalStringArgument("--schema", sqoopImport.getSchema(), !StringUtil.isEmpty(sqoopImport.getSchema()));

        return this;
    }

}
