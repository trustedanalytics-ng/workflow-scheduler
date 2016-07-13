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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Component
public class OozieScheduleValidator implements Validator {
    private static final Logger LOGGER = LoggerFactory.getLogger(OozieScheduleValidator.class);

    private final Set<String> timeUnits = new HashSet<>(Arrays.asList("minutes", "hours", "days", "months"));

    @Value("${oozie.schedule.frequency.minimum}")
    private long scheduleMinimumFrequency;

    @Override
    public boolean supports(Class<?> aClass) {
        return OozieSchedule.class.equals(aClass);
    }

    @Override
    public void validate(Object o, Errors errors) {
        OozieSchedule oozieSchedule = (OozieSchedule) o;
        validateStartAndEndTime(oozieSchedule, errors);
        validateFrequency(oozieSchedule, errors);
    }

    private void validateStartAndEndTime(OozieSchedule oozieSchedule, Errors errors) {
        if(oozieSchedule.getStartTimeUtc().isAfter(oozieSchedule.getEndTimeUtc())) {
            errors.rejectValue("startTimeUtc", "startTimeUtc.invalid", String.format("Start time (%s) must be before end time (%s)",
                    oozieSchedule.getStartTimeUtc(), oozieSchedule.getEndTimeUtc()));
        }
    }

    private void validateFrequency(OozieSchedule oozieSchedule, Errors errors) {
        LOGGER.info("Schedule frequency unit: {}", oozieSchedule.getFrequency().getUnit());
        if (! timeUnits.contains(oozieSchedule.getFrequency().getUnit().toLowerCase())) {
            errors.rejectValue("frequency.unit", "frequency.unit.unknown", "Unknown job freqency unit: "
                    + oozieSchedule.getFrequency().getUnit());
        }
        Optional.ofNullable(oozieSchedule.getFrequency())
                .filter(f -> !f.getUnit().equalsIgnoreCase("minutes") || toUnit(f.getAmount(), TimeUnit.SECONDS) >= scheduleMinimumFrequency)
                .orElseThrow(() -> new IllegalArgumentException(String.format("Job schedule period can not be smaller than (%d) seconds",
                        scheduleMinimumFrequency)));
    }

    private long toUnit(long amount, TimeUnit unit) {
        return unit.convert(amount, TimeUnit.MINUTES);
    }
}
