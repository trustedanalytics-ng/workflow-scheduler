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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.trustedanalytics.scheduler.oozie.OozieFrequency;
import org.trustedanalytics.scheduler.oozie.OozieSchedule;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Component
public class OozieJobScheduleValidator {

    @Value("${oozie.schedule.frequency.minimum}")
    private long scheduleMinimumFrequency;

    public void validate(OozieSchedule oozieSchedule) {
        validateStartAndEndTime(oozieSchedule);
        validateFrequency(oozieSchedule);
    }

    private void validateStartAndEndTime(OozieSchedule oozieSchedule) {
        if(oozieSchedule.getStartTimeUtc().isAfter(oozieSchedule.getEndTimeUtc())) {
            throw new IllegalArgumentException(String.format("Start time (%s) must be before end time (%s)",
                    oozieSchedule.getStartTimeUtc(), oozieSchedule.getEndTimeUtc()));
        }
    }

    private void validateFrequency(OozieSchedule oozieSchedule) {
        Optional.ofNullable(oozieSchedule.getFrequency())
                .filter(f -> !f.getUnit().equalsIgnoreCase("minutes") || toUnit(f.getAmount(), TimeUnit.SECONDS) >= scheduleMinimumFrequency)
                .orElseThrow(() -> new IllegalArgumentException(String.format("Job schedule period can not be smaller than (%d) seconds",
                        scheduleMinimumFrequency)));
    }

    private long toUnit(long amount, TimeUnit unit) {
        return unit.convert(amount, TimeUnit.MINUTES);
    }
}
