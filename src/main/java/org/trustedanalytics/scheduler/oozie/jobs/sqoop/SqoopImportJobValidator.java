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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.ValidationUtils;
import org.springframework.validation.Validator;

@Component
public class SqoopImportJobValidator implements Validator {

    private final SqoopImportValidator sqoopImportValidator;

    @Autowired
    public SqoopImportJobValidator(SqoopImportValidator sqoopImportValidator) {
        this.sqoopImportValidator = sqoopImportValidator;
    }

    @Override
    public boolean supports(Class<?> aClass) {
        return SqoopScheduledImportJob.class.equals(aClass) | SqoopImportJob.class.equals((aClass));
    }

    @Override
    public void validate(Object o, Errors errors) {
        SqoopImport sqoopImport;
        if(SqoopScheduledImportJob.class.equals(o.getClass())) {
            sqoopImport = ((SqoopScheduledImportJob) o).getSqoopImport();
        } else {
            sqoopImport = ((SqoopImportJob) o).getSqoopImport();
        }
        errors.pushNestedPath("sqoopImport");
        ValidationUtils.invokeValidator(sqoopImportValidator, sqoopImport, errors);
        errors.popNestedPath();
    }
}
