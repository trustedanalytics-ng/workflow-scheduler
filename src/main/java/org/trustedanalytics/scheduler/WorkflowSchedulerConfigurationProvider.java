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

import org.trustedanalytics.scheduler.config.Database;
import org.trustedanalytics.scheduler.filesystem.HdfsConfigProvider;
import org.trustedanalytics.scheduler.security.TokenProvider;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

@Service
public class WorkflowSchedulerConfigurationProvider {

    private static final String MAIN_TIMEZONES = ".*(GMT|UTC|US|Europe/Warsaw).*";

    private final List<Database> databases;

    private final List<String> zones;

    private final TokenProvider tokenProvider;

    @Value("${oozie.schedule.frequency.minimum}")
    private long scheduleMinimumFrequency;

    private final HdfsConfigProvider hdfsConfigProvider;

    @Autowired
    public WorkflowSchedulerConfigurationProvider(DatabaseProvider databaseProvider, TokenProvider tokenProvider, HdfsConfigProvider hdfsConfigProvider) {
        this.databases = databaseProvider.getEnabledEngines().toList().toBlocking().single();
        this.zones = Arrays.stream(TimeZone.getAvailableIDs())
                .filter(timezone -> timezone.matches(MAIN_TIMEZONES))
                .collect(Collectors.toList());
        this.tokenProvider = tokenProvider;
        this.hdfsConfigProvider = hdfsConfigProvider;
    }

    public WorkflowSchedulerConfigurationEntity getConfiguration(String orgId) {
        return WorkflowSchedulerConfigurationEntity.builder()
            .databases(databases)
            .timezones(zones)
            .organizationDirectory(
                String.format("%s/org/%s/user/%s/", hdfsConfigProvider.getHdfsUri(), orgId, tokenProvider.getUserId()))
            .minimumFrequencyInSeconds(scheduleMinimumFrequency)
            .build();
    }
}
