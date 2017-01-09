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
package org.trustedanalytics.scheduler.utils;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.trustedanalytics.scheduler.filesystem.OrgSpecificSpace;

import java.io.IOException;

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;


@Component
public class UniqueIdSupplier implements JobIdSupplier {
    private static final Logger LOGGER = LoggerFactory.getLogger(UniqueIdSupplier.class);
    private final static int RANDOM_ELEMENTS_COUNT = 2;


    @Override
    public synchronized  String get(String jobName, OrgSpecificSpace space) throws IOException {
        UniqueId id = UniqueId.generate(jobName);
        Path path = idToPath(id.getId(), space);
        LOGGER.info("Trying path " + path);
        String postfix = "";
        while(space.fileExists(path)) {
            LOGGER.info("Path already exists " + path);
            postfix += "-" + randomAlphanumeric(RANDOM_ELEMENTS_COUNT);
            path = new Path(path.toString() + postfix);
        }
        LOGGER.info("Accepted path " + path);
        return id.getId() + postfix;
    }

    private Path idToPath(String id, OrgSpecificSpace space) {
        return space.resolveOozieDir(id,"");
    }
}
