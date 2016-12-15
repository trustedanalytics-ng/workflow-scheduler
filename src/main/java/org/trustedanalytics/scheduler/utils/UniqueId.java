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

import lombok.Getter;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;


public class UniqueId {

    @Getter
    private String id;

    public static synchronized UniqueId generate(String name) {
        UniqueId uid = new UniqueId(name);
        try {
            Thread.sleep(1001);
        } catch (InterruptedException e) {
            return null;
        }
        return uid;
    }

    private UniqueId() { }

    private UniqueId(String name) {
        String normalized_name = name.replaceAll(" ","_");
        id = normalized_name + "-" + getTimestamp();
    }

    private String getTimestamp() {
        DateFormat dateFormat = new SimpleDateFormat("yyMMdd-HHmmss");
        return dateFormat.format(new Date());
    }

}
