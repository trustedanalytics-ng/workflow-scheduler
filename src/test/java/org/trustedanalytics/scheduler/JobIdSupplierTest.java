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

import org.apache.commons.collections.list.SynchronizedList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.trustedanalytics.scheduler.utils.UniqueIdSupplier;
import org.trustedanalytics.scheduler.utils.JobIdSupplier;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;


@RunWith(MockitoJUnitRunner.class)
public class JobIdSupplierTest {

    @Test
    public void testIdUniquenes() {
        JobIdSupplier supplier = new UniqueIdSupplier();
        String id1 = supplier.get("kung");
        String id2 = supplier.get("kung");
        assertNotEquals(id1,id2);
    }

    @Test
    public void testIdUniqenesMultithread() throws InterruptedException {
        int noThreads = 5;
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < noThreads;i++) {
            new Thread() {
                public void run() {
                    JobIdSupplier supplier = new UniqueIdSupplier();
                    ids.add(supplier.get("a"));
                }

            }.start();
        }
        Thread.sleep(6000);
        assertEquals(ids.size(), noThreads);
    }
}
