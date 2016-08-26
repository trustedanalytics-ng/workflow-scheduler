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

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.trustedanalytics.scheduler.DatabaseProvider;
import org.trustedanalytics.scheduler.config.Database;
import rx.Observable;

@Component
public class SqoopJobMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqoopJobMapper.class);

    private final Observable<Database> databases;

    @Autowired
    public SqoopJobMapper(DatabaseProvider databaseProvider) {
        this.databases = databaseProvider.getEnabledEngines();
    }

    public void adjust(SqoopImportJob job) {
        job.setName(job.getName().replace(" ", "_"));
        adjustSqoopImport(job.getSqoopImport());
    }

    public void adjust(SqoopScheduledImportJob job) {
        job.setName(job.getName().replace(" ", "_"));
        adjustSqoopImport(job.getSqoopImport());

        if (job.getSchedule() != null
                && job.getSchedule().getFrequency() != null
                && job.getSchedule().getFrequency().getUnit() != null) {
            job.getSchedule().getFrequency().setUnit(job.getSchedule().getFrequency().getUnit().toLowerCase());
        }
        if (StringUtils.isEmpty(job.getSqoopImport().getUsername())) {
            LOGGER.info("Empty username detected, using 'tap'");
            job.getSqoopImport().setUsername("tap");
        }
    }

    private void adjustSqoopImport(SqoopImport sqoopImport) {
        if ("overwrite".equalsIgnoreCase(sqoopImport.getImportMode())) {
            sqoopImport.setIncremental(false);
            sqoopImport.setOverwrite(true);
        }

        if ("incremental".equalsIgnoreCase(sqoopImport.getImportMode())) {
            sqoopImport.setIncremental(true);
            sqoopImport.setOverwrite(false);
        }

        Observable.just(sqoopImport).map(s -> {
            if (OracleJobMapper.isOracle(s)) {
                OracleJobMapper.transform(s);
            } else {
                adjustJdbcStringForAnyDatabaseExceptOracle(s);
            }
            return s;
        }).subscribe();

        setDriverClassNameIfEmpty(sqoopImport);
    }

    private void adjustJdbcStringForAnyDatabaseExceptOracle(SqoopImport importJob) {
        importJob.setJdbcUri(importJob.getJdbcUri().replace(":@", "://"));
    }

    private void setDriverClassNameIfEmpty(SqoopImport sqoopImport) {
        if (databases != null && StringUtils.isEmpty(sqoopImport.getDriver())) {
            databases
                    .flatMapIterable(db -> db.getDrivers())
                    .filter(driver -> sqoopImport.getJdbcUri().contains(driver.getName()))
                    .limit(1)
                    .subscribe(driver -> sqoopImport.setDriver(driver.getClassName()));
        }
    }

    private Observable.Transformer<SqoopImport, SqoopImport> transform() {
        return sqoopImport -> sqoopImport;
    }
}
