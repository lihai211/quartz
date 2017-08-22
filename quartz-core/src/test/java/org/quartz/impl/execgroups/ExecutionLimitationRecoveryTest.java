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

import org.junit.Test;
import org.quartz.*;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.impl.jdbcjobstore.JdbcQuartzTestUtilities;
import org.quartz.impl.jdbcjobstore.JobStoreTX;
import org.quartz.simpl.SimpleThreadPool;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for execution limitations feature (issue#175) when recovering jobs.
 *
 * @author mederly (Evolveum)
 */
public class ExecutionLimitationRecoveryTest {

    static final String EXECUTION_GROUP_0 = "group0";
    static final String EXECUTION_GROUP_1 = "group1";
    static final String EXECUTION_GROUP_2 = "group2";

    /**
     * Here we check for correct preservation of execution group during job recovery.
     */
    @Test
    public void testRecovery() throws SchedulerException, SQLException, InterruptedException {
        String TEST_NAME = "testRecovery";
        prepare(TEST_NAME);
        try {
            JobStoreTX jobStore = new JobStoreTX();
            jobStore.setDataSource(TEST_NAME);
            jobStore.setInstanceId(TEST_NAME);
            jobStore.setInstanceName(TEST_NAME);
            jobStore.setTriggerAcquisitionMaxFetchCount(1);
            DirectSchedulerFactory factory = DirectSchedulerFactory.getInstance();

            SimpleThreadPool threadPool = new SimpleThreadPool(10, Thread.NORM_PRIORITY);
            factory.createScheduler(threadPool, jobStore);
            Scheduler scheduler = factory.getScheduler();
            Map<String, Integer> limits = new HashMap<>();
            limits.put(EXECUTION_GROUP_0, 0);
            limits.put(EXECUTION_GROUP_1, 1);
            limits.put(EXECUTION_GROUP_2, 2);
            scheduler.setExecutionLimits(limits);

            JobDetail job = JobBuilder.newJob(ExecutionLimitationTestJob.class)
                    .withIdentity("test")
                    .storeDurably()
                    .requestRecovery()
                    .build();
            Trigger trigger0 = TriggerBuilder.newTrigger()
                    .withIdentity("t0")
                    .forJob(job)
                    .executionGroup(EXECUTION_GROUP_0)
                    .startNow()
                    .build();
            Trigger trigger1 = TriggerBuilder.newTrigger()
                    .withIdentity("t1")
                    .forJob(job)
                    .executionGroup(EXECUTION_GROUP_1)
                    .startNow()
                    .build();
            Trigger trigger2a = TriggerBuilder.newTrigger()
                    .withIdentity("t2a")
                    .forJob(job)
                    .executionGroup(EXECUTION_GROUP_2)
                    .startNow()
                    .build();
            Trigger trigger2b = TriggerBuilder.newTrigger()
                    .withIdentity("t2b")
                    .forJob(job)
                    .executionGroup(EXECUTION_GROUP_2)
                    .startNow()
                    .build();
            Trigger triggerNull = TriggerBuilder.newTrigger()
                    .withIdentity("tNull")
                    .forJob(job)
                    .startNow()
                    .build();
            scheduler.addJob(job, false);
            scheduler.scheduleJob(trigger0);
            scheduler.scheduleJob(trigger1);
            scheduler.scheduleJob(trigger2a);
            scheduler.scheduleJob(trigger2b);
            scheduler.scheduleJob(triggerNull);
            scheduler.start();

            // wait to give triggers chance to fire
            Thread.sleep(2000);

            Map<String, Integer> runningMap = threadPool.getRunningJobsPerExecutionGroup();
            System.out.println("Jobs per execution group: " + runningMap);
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_0, null, runningMap.get(EXECUTION_GROUP_0));
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_1, (Integer) 1, runningMap.get(EXECUTION_GROUP_1));
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_2, (Integer) 2, runningMap.get(EXECUTION_GROUP_2));
            assertEquals("Wrong # of jobs with no execution group", (Integer) 1, runningMap.get(null));

            scheduler.shutdown();

            // let's NOT stop the thread, so DB would not contain record on their completion

            // -----------------------------------------------------------------------------------------------------------------
            // now let's start the scheduler (presumably on another node) to recover the jobs
            // we simulate other node that has different execution limits

            JobStoreTX jobStoreNew = new JobStoreTX();
            jobStoreNew.setInstanceName(TEST_NAME);
            jobStoreNew.setInstanceId(TEST_NAME);
            jobStoreNew.setDataSource(TEST_NAME);
            jobStoreNew.setTriggerAcquisitionMaxFetchCount(1);
            SimpleThreadPool threadPoolNew = new SimpleThreadPool(10, Thread.NORM_PRIORITY);
            factory.createScheduler(threadPoolNew, jobStoreNew);
            Scheduler schedulerNew = factory.getScheduler();
            Map<String, Integer> limitsNew = new HashMap<>();
            limitsNew.put(EXECUTION_GROUP_0, 1);
            limitsNew.put(EXECUTION_GROUP_1, 0);
            limitsNew.put(EXECUTION_GROUP_2, 1);
            schedulerNew.setExecutionLimits(limitsNew);

            schedulerNew.start();

            // wait to give triggers chance to fire again
            Thread.sleep(2000);

            System.out.println("Running jobs as per scheduler after restart: " + schedulerNew.getCurrentlyExecutingJobs());
            runningMap = threadPoolNew.getRunningJobsPerExecutionGroup();

            schedulerNew.shutdown();

            System.out.println("Jobs per execution group (after restart): " + runningMap);
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_0, (Integer) 1, runningMap.get(EXECUTION_GROUP_0));
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_1, null, runningMap.get(EXECUTION_GROUP_1));
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_2, (Integer) 1, runningMap.get(EXECUTION_GROUP_2));
            assertEquals("Wrong # of jobs with no execution group", (Integer) 1, runningMap.get(null));

            System.out.println("Jobs running after second shutdown: " + threadPoolNew.getRunningJobsPerExecutionGroup());
            System.out.println("Running jobs as per scheduler after second shutdown: " + schedulerNew.getCurrentlyExecutingJobs());

            //interruptJobs(schedulerNew, job);

        } finally {
            cleanup(TEST_NAME);
        }
    }

    /**
     * Here we check for correct preservation of execution group during job recovery in clustered environment
     */
    @Test
    public void testRecoveryClustered() throws SchedulerException, SQLException, InterruptedException {
        String TEST_NAME = "testRecoveryClustered";
        prepare(TEST_NAME);
        try {

            // node 1
            JobStoreTX jobStore1 = new JobStoreTX();
            jobStore1.setDataSource(TEST_NAME);
            jobStore1.setInstanceId("1");
            jobStore1.setInstanceName(TEST_NAME);
            jobStore1.setTriggerAcquisitionMaxFetchCount(1);
            jobStore1.setIsClustered(true);
            DirectSchedulerFactory factory1 = DirectSchedulerFactory.getInstance();

            SimpleThreadPool threadPool1 = new SimpleThreadPool(10, Thread.NORM_PRIORITY);
            factory1.createScheduler(DirectSchedulerFactory.DEFAULT_SCHEDULER_NAME, "1", threadPool1, jobStore1);
            Scheduler scheduler1 = factory1.getScheduler();
            Map<String, Integer> limits1 = new HashMap<>();
            limits1.put(EXECUTION_GROUP_0, 0);
            limits1.put(EXECUTION_GROUP_1, 1);
            limits1.put(EXECUTION_GROUP_2, 2);
            scheduler1.setExecutionLimits(limits1);

            // creating jobs
            JobDetail job = JobBuilder.newJob(ExecutionLimitationTestJob.class)
                    .withIdentity("test")
                    .storeDurably()
                    .requestRecovery()
                    .build();
            Trigger trigger0 = TriggerBuilder.newTrigger()
                    .withIdentity("t0")
                    .forJob(job)
                    .executionGroup(EXECUTION_GROUP_0)
                    .startNow()
                    .build();
            Trigger trigger1 = TriggerBuilder.newTrigger()
                    .withIdentity("t1")
                    .forJob(job)
                    .executionGroup(EXECUTION_GROUP_1)
                    .startNow()
                    .build();
            Trigger trigger2a = TriggerBuilder.newTrigger()
                    .withIdentity("t2a")
                    .forJob(job)
                    .executionGroup(EXECUTION_GROUP_2)
                    .startNow()
                    .build();
            Trigger trigger2b = TriggerBuilder.newTrigger()
                    .withIdentity("t2b")
                    .forJob(job)
                    .executionGroup(EXECUTION_GROUP_2)
                    .startNow()
                    .build();
            Trigger triggerNull = TriggerBuilder.newTrigger()
                    .withIdentity("tNull")
                    .forJob(job)
                    .startNow()
                    .build();
            scheduler1.addJob(job, false);
            scheduler1.scheduleJob(trigger0);
            scheduler1.scheduleJob(trigger1);
            scheduler1.scheduleJob(trigger2a);
            scheduler1.scheduleJob(trigger2b);
            scheduler1.scheduleJob(triggerNull);

            // first, scheduler1 is running
            scheduler1.start();

            // wait to give triggers chance to fire
            Thread.sleep(2000);

            Map<String, Integer> runningMap = threadPool1.getRunningJobsPerExecutionGroup();
            System.out.println("Jobs per execution group: " + runningMap);
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_0, null, runningMap.get(EXECUTION_GROUP_0));
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_1, (Integer) 1, runningMap.get(EXECUTION_GROUP_1));
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_2, (Integer) 2, runningMap.get(EXECUTION_GROUP_2));
            assertEquals("Wrong # of jobs with no execution group", (Integer) 1, runningMap.get(null));

            // -----------------------------------------------------------------------------------------------------------------
            // then it "crashes" and scheduler2 takes over
            scheduler1.shutdown();

            // node 2
            JobStoreTX jobStore2 = new JobStoreTX();
            jobStore2.setDataSource(TEST_NAME);
            jobStore2.setInstanceId("2");
            jobStore2.setInstanceName(TEST_NAME);
            jobStore2.setTriggerAcquisitionMaxFetchCount(1);
            jobStore2.setIsClustered(true);
            DirectSchedulerFactory factory2 = DirectSchedulerFactory.getInstance();

            SimpleThreadPool threadPool2 = new SimpleThreadPool(10, Thread.NORM_PRIORITY);
            factory2.createScheduler(DirectSchedulerFactory.DEFAULT_SCHEDULER_NAME, "2", threadPool2, jobStore2);
            Scheduler scheduler2 = factory2.getScheduler();
            Map<String, Integer> limits2 = new HashMap<>();
            limits2.put(EXECUTION_GROUP_0, 1);
            limits2.put(EXECUTION_GROUP_1, 0);
            limits2.put(EXECUTION_GROUP_2, 1);
            scheduler2.setExecutionLimits(limits2);

            scheduler2.start();

            // wait to give triggers chance to fire again + to detect other node failure (min 2x7500 ms)
            Thread.sleep(20000);

            System.out.println("Running jobs as per scheduler after restart: " + scheduler2.getCurrentlyExecutingJobs());
            runningMap = threadPool2.getRunningJobsPerExecutionGroup();

            scheduler2.shutdown();

            System.out.println("Jobs per execution group (after restart): " + runningMap);
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_0, (Integer) 1, runningMap.get(EXECUTION_GROUP_0));
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_1, null, runningMap.get(EXECUTION_GROUP_1));
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_2, (Integer) 1, runningMap.get(EXECUTION_GROUP_2));
            assertEquals("Wrong # of jobs with no execution group", (Integer) 1, runningMap.get(null));

            System.out.println("Jobs running after second shutdown: " + threadPool2.getRunningJobsPerExecutionGroup());
            System.out.println("Running jobs as per scheduler after second shutdown: " + scheduler2.getCurrentlyExecutingJobs());

            //interruptJobs(schedulerNew, job);

        } finally {
            cleanup(TEST_NAME);
        }
    }

    private void prepare(String testName) throws SQLException {
        JdbcQuartzTestUtilities.createDatabase(testName);
    }

    private void cleanup(String testName) throws SQLException {
        JdbcQuartzTestUtilities.destroyDatabase(testName);
    }

}
