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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.Errors;
import org.trustedanalytics.scheduler.oozie.jobs.sqoop.SqoopImport;
import org.trustedanalytics.scheduler.oozie.jobs.sqoop.SqoopImportValidator;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class SqoopImportValidatorTest {

    SqoopImportValidator sqoopImportValidator;

    @Before
    public void setUp() {
        sqoopImportValidator = new SqoopImportValidator();
    }

    @Test
    public void should_addError_when_importModeNull() {
        SqoopImport sqoopImport = new SqoopImport();

        Errors errors = new BeanPropertyBindingResult(sqoopImport, "sqoopImport");
        sqoopImportValidator.validate(sqoopImport, errors);

        assertTrue(errors.hasErrors());
        assertNotNull(errors.getFieldError("importMode"));
    }

    @Test
    public void should_addError_when_importModeIncrementalMissingCheckColumn() {
        SqoopImport sqoopImport = new SqoopImport();
        sqoopImport.setImportMode("incremental");

        Errors errors = new BeanPropertyBindingResult(sqoopImport, "sqoopImport");
        sqoopImportValidator.validate(sqoopImport, errors);

        assertTrue(errors.hasErrors());
        assertNotNull(errors.getFieldError("checkColumn"));
    }
}
