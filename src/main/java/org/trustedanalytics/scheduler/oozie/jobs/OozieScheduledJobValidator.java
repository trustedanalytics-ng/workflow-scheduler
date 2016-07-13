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
package org.trustedanalytics.scheduler.oozie.jobs;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.ValidationUtils;
import org.springframework.validation.Validator;
import org.trustedanalytics.scheduler.oozie.OozieScheduleValidator;

@Component
public class OozieScheduledJobValidator implements Validator{

    private final OozieScheduleValidator oozieScheduleValidator;

    @Autowired
    public OozieScheduledJobValidator(OozieScheduleValidator oozieScheduleValidator) {
        this.oozieScheduleValidator = oozieScheduleValidator;
    }

    @Override
    public boolean supports(Class<?> aClass) {
        return OozieScheduledJob.class.isAssignableFrom(aClass);
    }

    @Override
    public void validate(Object o, Errors errors) {
        OozieScheduledJob oozieScheduledJob = (OozieScheduledJob) o;
        errors.pushNestedPath("schedule");
        ValidationUtils.invokeValidator(this.oozieScheduleValidator, oozieScheduledJob.getSchedule(), errors);
        errors.popNestedPath();
    }
}
