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

import org.quartz.InterruptableJob;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mederly (Evolveum)
 */
public class ExecutionLimitationTestJob implements InterruptableJob {
    private Thread executionThread;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

        Logger log = LoggerFactory.getLogger(ExecutionLimitationTestJob.class);
        executionThread = Thread.currentThread();
        System.out.println("Job starting in " + executionThread);
        log.info("Job starting; execution group = {}", context.getTrigger().getExecutionGroup());
        try {
            for (;;) {
                Thread.sleep(60000);
            }
        } catch (InterruptedException e) {
            log.info("Job interrupted, stopping");
        }
    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        executionThread.interrupt();
    }
}
