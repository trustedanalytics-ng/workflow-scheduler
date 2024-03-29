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


public class OracleJobMapper {

    private OracleJobMapper() {
    }

    public static void transform(SqoopImport job) {
        adjustOracleJdbcString(job);

        if (isSchemaPresentAndTableNameIsWithoutSchema(job)) {
            incorporateSchemaInTableName(job);
        }
    }

    private static boolean isSchemaPresentAndTableNameIsWithoutSchema(SqoopImport job) {
        return !job.getSchema().isEmpty() && (!job.getTable().contains("."));
    }

    private static void incorporateSchemaInTableName(SqoopImport job) {
        String tableWithSchema = job.getSchema() + "." + job.getTable();
        job.setTable(tableWithSchema);
        job.setSchema("");
    }

    private static void adjustOracleJdbcString(SqoopImport job) {
        job.setJdbcUri(job.getJdbcUri().replace("://", ":@"));
    }

    public static boolean isOracle(SqoopImport job) {
        return job.getJdbcUri().toLowerCase().contains("jdbc:oracle:thin");
    }

}
