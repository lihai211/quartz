/*
 * Copyright (c) 2017 Evolveum
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.quartz.impl.execgroups;

import org.quartz.simpl.RAMJobStore;
import org.quartz.spi.JobStore;

/**
 * @author mederly (Evolveum)
 */
public class RamJobStoreExecutionLimitationTest extends AbstractExecutionLimitationTest {

    @Override
    protected void prepare(String testName) {
    }

    @Override
    protected RAMJobStore createJobStore(String name) {
        return new RAMJobStore();
    }

    @Override
    protected JobStore createJobStore(String testName, long misfireThreshold) {
        RAMJobStore jobStore = createJobStore(testName);
        jobStore.setMisfireThreshold(misfireThreshold);
        return jobStore;
    }

    @Override
    protected void cleanup(String testName) {
    }
}
