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
import org.quartz.simpl.RAMJobStore;
import org.quartz.simpl.SimpleThreadPool;
import org.quartz.spi.JobStore;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for execution limitations feature (issue#175); for both JDBC and RAM job stores.
 *
 * @author mederly (Evolveum)
 */
public abstract class AbstractExecutionLimitationTest {

    private static final String EXECUTION_GROUP_0 = "group0";
    private static final String EXECUTION_GROUP_1 = "group1";
    private static final String EXECUTION_GROUP_2 = "group2";
    private static final String EXECUTION_GROUP_OTHER_1 = "groupOther1";
    private static final String EXECUTION_GROUP_OTHER_2 = "groupOther2";

    @Test
    public void testNothingAllowed() throws SchedulerException, SQLException, InterruptedException {
        String TEST_NAME = "testNothingAllowed";
        prepare(TEST_NAME);
        try {
            JobStore jobStore = createJobStore(TEST_NAME);
            DirectSchedulerFactory factory = DirectSchedulerFactory.getInstance();

            SimpleThreadPool threadPool = new SimpleThreadPool(20, Thread.NORM_PRIORITY);
            factory.createScheduler(threadPool, jobStore);
            Scheduler scheduler = factory.getScheduler();
            Map<String, Integer> limits = new HashMap<>();
            limits.put(Scheduler.LIMIT_FOR_OTHER_GROUPS, 0);
            scheduler.setExecutionLimits(limits);

            JobDetail job = createStandardTriggersAndStartScheduler(scheduler);

            // wait to give triggers chance to fire
            Thread.sleep(2000);

            Map<String, Integer> expected = new HashMap<>();
            checkRunningJobs(threadPool, expected);

            scheduler.standby();

            interruptJobs(scheduler, job);

            scheduler.shutdown();

        } finally {
            cleanup(TEST_NAME);
        }
    }

    @Test
    public void testOtherUnspecifiedNullUnspecified() throws SchedulerException, SQLException, InterruptedException {
        String TEST_NAME = "testOtherUnspecifiedNullUnspecified";
        prepare(TEST_NAME);
        try {
            JobStore jobStore = createJobStore(TEST_NAME);
            DirectSchedulerFactory factory = DirectSchedulerFactory.getInstance();

            SimpleThreadPool threadPool = new SimpleThreadPool(20, Thread.NORM_PRIORITY);
            factory.createScheduler(threadPool, jobStore);
            Scheduler scheduler = factory.getScheduler();
            Map<String, Integer> limits = new HashMap<>();
            limits.put(EXECUTION_GROUP_0, 0);
            limits.put(EXECUTION_GROUP_1, 1);
            limits.put(EXECUTION_GROUP_2, 2);
            scheduler.setExecutionLimits(limits);

            JobDetail job = createStandardTriggersAndStartScheduler(scheduler);

            // wait to give triggers chance to fire
            Thread.sleep(2000);

            Map<String, Integer> expected = new HashMap<>();
            expected.put(EXECUTION_GROUP_0, null);
            expected.put(EXECUTION_GROUP_1, 1);
            expected.put(EXECUTION_GROUP_2, 2);
            expected.put(EXECUTION_GROUP_OTHER_1, 2);
            expected.put(EXECUTION_GROUP_OTHER_2, 2);
            expected.put(null, 2);
            checkRunningJobs(threadPool, expected);

            scheduler.standby();

            interruptJobs(scheduler, job);

            scheduler.shutdown();

        } finally {
            cleanup(TEST_NAME);
        }
    }

    @Test
    public void testOther1NullUnlimited() throws SchedulerException, SQLException, InterruptedException {
        String TEST_NAME = "testOther1NullUnlimited";
        prepare(TEST_NAME);
        try {
            JobStore jobStore = createJobStore(TEST_NAME);
            DirectSchedulerFactory factory = DirectSchedulerFactory.getInstance();

            SimpleThreadPool threadPool = new SimpleThreadPool(20, Thread.NORM_PRIORITY);
            factory.createScheduler(threadPool, jobStore);
            Scheduler scheduler = factory.getScheduler();
            Map<String, Integer> limits = new HashMap<>();
            limits.put(EXECUTION_GROUP_0, 0);
            limits.put(EXECUTION_GROUP_1, 1);
            limits.put(EXECUTION_GROUP_2, 2);
            limits.put(Scheduler.LIMIT_FOR_OTHER_GROUPS, 1);
            limits.put(null, null);
            scheduler.setExecutionLimits(limits);

            JobDetail job = createStandardTriggersAndStartScheduler(scheduler);

            // wait to give triggers chance to fire
            Thread.sleep(2000);

            Map<String, Integer> expected = new HashMap<>();
            expected.put(EXECUTION_GROUP_0, null);
            expected.put(EXECUTION_GROUP_1, 1);
            expected.put(EXECUTION_GROUP_2, 2);
            expected.put(EXECUTION_GROUP_OTHER_1, 1);
            expected.put(EXECUTION_GROUP_OTHER_2, 1);
            expected.put(null, 2);
            checkRunningJobs(threadPool, expected);

            scheduler.standby();

            interruptJobs(scheduler, job);

            scheduler.shutdown();

        } finally {
            cleanup(TEST_NAME);
        }
    }

    @Test
    public void testOther1NullUnspecified() throws SchedulerException, SQLException, InterruptedException {
        String TEST_NAME = "testOther1NullUnspecified";
        prepare(TEST_NAME);
        try {
            JobStore jobStore = createJobStore(TEST_NAME);
            DirectSchedulerFactory factory = DirectSchedulerFactory.getInstance();

            SimpleThreadPool threadPool = new SimpleThreadPool(20, Thread.NORM_PRIORITY);
            factory.createScheduler(threadPool, jobStore);
            Scheduler scheduler = factory.getScheduler();
            Map<String, Integer> limits = new HashMap<>();
            limits.put(EXECUTION_GROUP_0, 0);
            limits.put(EXECUTION_GROUP_1, 1);
            limits.put(EXECUTION_GROUP_2, 2);
            limits.put(Scheduler.LIMIT_FOR_OTHER_GROUPS, 1);
            scheduler.setExecutionLimits(limits);

            JobDetail job = createStandardTriggersAndStartScheduler(scheduler);

            // wait to give triggers chance to fire
            Thread.sleep(2000);

            Map<String, Integer> expected = new HashMap<>();
            expected.put(EXECUTION_GROUP_0, null);
            expected.put(EXECUTION_GROUP_1, 1);
            expected.put(EXECUTION_GROUP_2, 2);
            expected.put(EXECUTION_GROUP_OTHER_1, 1);
            expected.put(EXECUTION_GROUP_OTHER_2, 1);
            expected.put(null, 1);          // because null is driven by other in this case
            checkRunningJobs(threadPool, expected);

            scheduler.standby();

            interruptJobs(scheduler, job);

            scheduler.shutdown();

        } finally {
            cleanup(TEST_NAME);
        }
    }

    @Test
    public void testOther0NullUnlimited() throws SchedulerException, SQLException, InterruptedException {
        String TEST_NAME = "testOther0NullUnlimited";
        prepare(TEST_NAME);
        try {
            JobStore jobStore = createJobStore(TEST_NAME);
            DirectSchedulerFactory factory = DirectSchedulerFactory.getInstance();

            SimpleThreadPool threadPool = new SimpleThreadPool(20, Thread.NORM_PRIORITY);
            factory.createScheduler(threadPool, jobStore);
            Scheduler scheduler = factory.getScheduler();
            Map<String, Integer> limits = new HashMap<>();
            limits.put(EXECUTION_GROUP_0, 0);
            limits.put(EXECUTION_GROUP_1, 1);
            limits.put(EXECUTION_GROUP_2, 2);
            limits.put(Scheduler.LIMIT_FOR_OTHER_GROUPS, 0);
            limits.put(null, null);
            scheduler.setExecutionLimits(limits);

            JobDetail job = createStandardTriggersAndStartScheduler(scheduler);

            // wait to give triggers chance to fire
            Thread.sleep(2000);

            Map<String, Integer> expected = new HashMap<>();
            expected.put(EXECUTION_GROUP_0, null);
            expected.put(EXECUTION_GROUP_1, 1);
            expected.put(EXECUTION_GROUP_2, 2);
            expected.put(EXECUTION_GROUP_OTHER_1, null);
            expected.put(EXECUTION_GROUP_OTHER_2, null);
            expected.put(null, 2);
            checkRunningJobs(threadPool, expected);

            scheduler.standby();

            interruptJobs(scheduler, job);

            scheduler.shutdown();

        } finally {
            cleanup(TEST_NAME);
        }
    }

    @Test
    public void testOther0NullUnspecified() throws SchedulerException, SQLException, InterruptedException {
        String TEST_NAME = "testOther0NullUnspecified";
        prepare(TEST_NAME);
        try {
            JobStore jobStore = createJobStore(TEST_NAME);
            DirectSchedulerFactory factory = DirectSchedulerFactory.getInstance();

            SimpleThreadPool threadPool = new SimpleThreadPool(20, Thread.NORM_PRIORITY);
            factory.createScheduler(threadPool, jobStore);
            Scheduler scheduler = factory.getScheduler();
            Map<String, Integer> limits = new HashMap<>();
            limits.put(EXECUTION_GROUP_0, 0);
            limits.put(EXECUTION_GROUP_1, 1);
            limits.put(EXECUTION_GROUP_2, 2);
            limits.put(Scheduler.LIMIT_FOR_OTHER_GROUPS, 0);
            scheduler.setExecutionLimits(limits);

            JobDetail job = createStandardTriggersAndStartScheduler(scheduler);

            // wait to give triggers chance to fire
            Thread.sleep(2000);

            Map<String, Integer> expected = new HashMap<>();
            expected.put(EXECUTION_GROUP_0, null);
            expected.put(EXECUTION_GROUP_1, 1);
            expected.put(EXECUTION_GROUP_2, 2);
            expected.put(EXECUTION_GROUP_OTHER_1, null);
            expected.put(EXECUTION_GROUP_OTHER_2, null);
            expected.put(null, null);
            checkRunningJobs(threadPool, expected);

            scheduler.standby();

            interruptJobs(scheduler, job);

            scheduler.shutdown();

        } finally {
            cleanup(TEST_NAME);
        }
    }

    @Test
    public void testOther1Null1() throws SchedulerException, SQLException, InterruptedException {
        String TEST_NAME = "testOther1Null1";
        prepare(TEST_NAME);
        try {
            JobStore jobStore = createJobStore(TEST_NAME);
            DirectSchedulerFactory factory = DirectSchedulerFactory.getInstance();

            SimpleThreadPool threadPool = new SimpleThreadPool(20, Thread.NORM_PRIORITY);
            factory.createScheduler(threadPool, jobStore);
            Scheduler scheduler = factory.getScheduler();
            Map<String, Integer> limits = new HashMap<>();
            limits.put(EXECUTION_GROUP_0, 0);
            limits.put(EXECUTION_GROUP_1, 1);
            limits.put(EXECUTION_GROUP_2, 2);
            limits.put(Scheduler.LIMIT_FOR_OTHER_GROUPS, 1);
            limits.put(null, 1);
            scheduler.setExecutionLimits(limits);

            JobDetail job = createStandardTriggersAndStartScheduler(scheduler);

            // wait to give triggers chance to fire
            Thread.sleep(2000);

            Map<String, Integer> expected = new HashMap<>();
            expected.put(EXECUTION_GROUP_0, null);
            expected.put(EXECUTION_GROUP_1, 1);
            expected.put(EXECUTION_GROUP_2, 2);
            expected.put(EXECUTION_GROUP_OTHER_1, 1);
            expected.put(EXECUTION_GROUP_OTHER_2, 1);
            expected.put(null, 1);
            checkRunningJobs(threadPool, expected);

            scheduler.standby();

            interruptJobs(scheduler, job);

            scheduler.shutdown();

        } finally {
            cleanup(TEST_NAME);
        }
    }

    @Test
    public void testOther1Null0() throws SchedulerException, SQLException, InterruptedException {
        String TEST_NAME = "testOther1Null0";
        prepare(TEST_NAME);
        try {
            JobStore jobStore = createJobStore(TEST_NAME);
            DirectSchedulerFactory factory = DirectSchedulerFactory.getInstance();

            SimpleThreadPool threadPool = new SimpleThreadPool(20, Thread.NORM_PRIORITY);
            factory.createScheduler(threadPool, jobStore);
            Scheduler scheduler = factory.getScheduler();
            Map<String, Integer> limits = new HashMap<>();
            limits.put(EXECUTION_GROUP_0, 0);
            limits.put(EXECUTION_GROUP_1, 1);
            limits.put(EXECUTION_GROUP_2, 2);
            limits.put(Scheduler.LIMIT_FOR_OTHER_GROUPS, 1);
            limits.put(null, 0);
            scheduler.setExecutionLimits(limits);

            JobDetail job = createStandardTriggersAndStartScheduler(scheduler);

            // wait to give triggers chance to fire
            Thread.sleep(2000);

            Map<String, Integer> expected = new HashMap<>();
            expected.put(EXECUTION_GROUP_0, null);
            expected.put(EXECUTION_GROUP_1, 1);
            expected.put(EXECUTION_GROUP_2, 2);
            expected.put(EXECUTION_GROUP_OTHER_1, 1);
            expected.put(EXECUTION_GROUP_OTHER_2, 1);
            expected.put(null, null);
            checkRunningJobs(threadPool, expected);

            scheduler.standby();

            interruptJobs(scheduler, job);

            scheduler.shutdown();

        } finally {
            cleanup(TEST_NAME);
        }
    }

    @Test
    public void testOtherUnspecifiedNull0() throws SchedulerException, SQLException, InterruptedException {
        String TEST_NAME = "testOtherUnspecifiedNull0";
        prepare(TEST_NAME);
        try {
            JobStore jobStore = createJobStore(TEST_NAME);
            DirectSchedulerFactory factory = DirectSchedulerFactory.getInstance();

            SimpleThreadPool threadPool = new SimpleThreadPool(20, Thread.NORM_PRIORITY);
            factory.createScheduler(threadPool, jobStore);
            Scheduler scheduler = factory.getScheduler();
            Map<String, Integer> limits = new HashMap<>();
            limits.put(EXECUTION_GROUP_0, 0);
            limits.put(EXECUTION_GROUP_1, 1);
            limits.put(EXECUTION_GROUP_2, 2);
            limits.put(null, 0);
            scheduler.setExecutionLimits(limits);

            JobDetail job = createStandardTriggersAndStartScheduler(scheduler);

            // wait to give triggers chance to fire
            Thread.sleep(2000);

            Map<String, Integer> expected = new HashMap<>();
            expected.put(EXECUTION_GROUP_0, null);
            expected.put(EXECUTION_GROUP_1, 1);
            expected.put(EXECUTION_GROUP_2, 2);
            expected.put(EXECUTION_GROUP_OTHER_1, 2);
            expected.put(EXECUTION_GROUP_OTHER_2, 2);
            expected.put(null, null);
            checkRunningJobs(threadPool, expected);

            scheduler.standby();

            interruptJobs(scheduler, job);

            scheduler.shutdown();

        } finally {
            cleanup(TEST_NAME);
        }
    }

    @Test
    public void testOtherLimitedNull0() throws SchedulerException, SQLException, InterruptedException {
        String TEST_NAME = "testOtherLimitedNull0";
        prepare(TEST_NAME);
        try {
            JobStore jobStore = createJobStore(TEST_NAME);
            DirectSchedulerFactory factory = DirectSchedulerFactory.getInstance();

            SimpleThreadPool threadPool = new SimpleThreadPool(20, Thread.NORM_PRIORITY);
            factory.createScheduler(threadPool, jobStore);
            Scheduler scheduler = factory.getScheduler();
            Map<String, Integer> limits = new HashMap<>();
            limits.put(EXECUTION_GROUP_0, 0);
            limits.put(EXECUTION_GROUP_1, 1);
            limits.put(EXECUTION_GROUP_2, 2);
            limits.put(Scheduler.LIMIT_FOR_OTHER_GROUPS, null);
            limits.put(null, 0);
            scheduler.setExecutionLimits(limits);

            JobDetail job = createStandardTriggersAndStartScheduler(scheduler);

            // wait to give triggers chance to fire
            Thread.sleep(2000);

            Map<String, Integer> expected = new HashMap<>();
            expected.put(EXECUTION_GROUP_0, null);
            expected.put(EXECUTION_GROUP_1, 1);
            expected.put(EXECUTION_GROUP_2, 2);
            expected.put(EXECUTION_GROUP_OTHER_1, 2);
            expected.put(EXECUTION_GROUP_OTHER_2, 2);
            expected.put(null, null);
            checkRunningJobs(threadPool, expected);

            scheduler.standby();

            interruptJobs(scheduler, job);

            scheduler.shutdown();

        } finally {
            cleanup(TEST_NAME);
        }
    }

    private void interruptJobs(Scheduler scheduler, JobDetail job) throws SchedulerException, InterruptedException {
        scheduler.interrupt(job.getKey());
        int attempts = 100;
        while (!scheduler.getCurrentlyExecutingJobs().isEmpty() && attempts-- > 0) {
            Thread.sleep(100);
        }
    }

    protected abstract void prepare(String testName);

    protected abstract JobStore createJobStore(String testName);

    protected abstract JobStore createJobStore(String testName, long misfireThreshold);

    protected abstract void cleanup(String testName);

    private JobDetail createStandardTriggersAndStartScheduler(Scheduler scheduler) throws InterruptedException, SchedulerException {
        JobDetail job = JobBuilder.newJob(ExecutionLimitationTestJob.class)
                .withIdentity("test")
                .storeDurably()
                .build();
        Trigger trigger0 = TriggerBuilder.newTrigger()
                .withIdentity("t0")
                .forJob(job)
                .executionGroup(EXECUTION_GROUP_0)
                .startNow()
                .build();
        Thread.sleep(200);          // t0 should be fetched first
        Trigger trigger1a = TriggerBuilder.newTrigger()
                .withIdentity("t1a")
                .forJob(job)
                .executionGroup(EXECUTION_GROUP_1)
                .startNow()
                .build();
        Trigger trigger1b = TriggerBuilder.newTrigger()
                .withIdentity("t1b")
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
        Trigger trigger2c = TriggerBuilder.newTrigger()
                .withIdentity("t2c")
                .forJob(job)
                .executionGroup(EXECUTION_GROUP_2)
                .startNow()
                .build();
        Trigger triggerOther1a = TriggerBuilder.newTrigger()
                .withIdentity("tOther1a")
                .forJob(job)
                .executionGroup(EXECUTION_GROUP_OTHER_1)
                .startNow()
                .build();
        Trigger triggerOther1b = TriggerBuilder.newTrigger()
                .withIdentity("tOther1b")
                .forJob(job)
                .executionGroup(EXECUTION_GROUP_OTHER_1)
                .startNow()
                .build();
        Trigger triggerOther2a = TriggerBuilder.newTrigger()
                .withIdentity("tOther2a")
                .forJob(job)
                .executionGroup(EXECUTION_GROUP_OTHER_2)
                .startNow()
                .build();
        Trigger triggerOther2b = TriggerBuilder.newTrigger()
                .withIdentity("tOther2b")
                .forJob(job)
                .executionGroup(EXECUTION_GROUP_OTHER_2)
                .startNow()
                .build();
        Trigger triggerNull1 = TriggerBuilder.newTrigger()
                .withIdentity("tNull1")
                .forJob(job)
                .startNow()
                .build();
        Trigger triggerNull2 = TriggerBuilder.newTrigger()
                .withIdentity("tNull2")
                .forJob(job)
                .startNow()
                .build();
        scheduler.addJob(job, false);
        scheduler.scheduleJob(trigger0);
        scheduler.scheduleJob(trigger1a);
        scheduler.scheduleJob(trigger1b);
        scheduler.scheduleJob(trigger2a);
        scheduler.scheduleJob(trigger2b);
        scheduler.scheduleJob(trigger2c);
        scheduler.scheduleJob(triggerOther1a);
        scheduler.scheduleJob(triggerOther1b);
        scheduler.scheduleJob(triggerOther2a);
        scheduler.scheduleJob(triggerOther2b);
        scheduler.scheduleJob(triggerNull1);
        scheduler.scheduleJob(triggerNull2);
        scheduler.start();
        return job;
    }

    private void checkRunningJobs(SimpleThreadPool threadPool, Map<String, Integer> expected) {
        Map<String, Integer> running = threadPool.getRunningJobsPerExecutionGroup();
        System.out.println("Jobs per execution group: " + running);
        for (Map.Entry<String, Integer> e : expected.entrySet()) {
            assertEquals("Wrong # of jobs in execution group " + e.getKey(), e.getValue(), running.get(e.getKey()));
        }
        // no other keys are present
        for (String runningGroup : running.keySet()) {
            assertTrue("Group " + runningGroup + " is running but not expected", expected.keySet().contains(runningGroup));
        }
    }

    @Test
    public void testMisfiredWhileSchedulerStopped() throws SchedulerException, SQLException, InterruptedException {
        String TEST_NAME = "testMisfiredWhileSchedulerStopped";
        prepare(TEST_NAME);
        try {
            JobStore jobStore = createJobStore(TEST_NAME, 1000);

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
                    .build();
            Trigger trigger0 = TriggerBuilder.newTrigger()
                    .withIdentity("t0")
                    .forJob(job)
                    .executionGroup(EXECUTION_GROUP_0)
                    .startNow()
                    .build();
            Trigger trigger1a = TriggerBuilder.newTrigger()
                    .withIdentity("t1a")
                    .forJob(job)
                    .executionGroup(EXECUTION_GROUP_1)
                    .startNow()
                    .build();
            Trigger trigger1b = TriggerBuilder.newTrigger()
                    .withIdentity("t1b")
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
            Trigger trigger2c = TriggerBuilder.newTrigger()
                    .withIdentity("t2c")
                    .forJob(job)
                    .executionGroup(EXECUTION_GROUP_2)
                    .startNow()
                    .build();
            Trigger triggerOther1 = TriggerBuilder.newTrigger()
                    .withIdentity("tOther1")
                    .forJob(job)
                    .executionGroup(EXECUTION_GROUP_OTHER_1)
                    .startNow()
                    .build();
            Trigger triggerNull1 = TriggerBuilder.newTrigger()
                    .withIdentity("tNull1")
                    .forJob(job)
                    .startNow()
                    .build();
            Trigger triggerNull2 = TriggerBuilder.newTrigger()
                    .withIdentity("tNull2")
                    .forJob(job)
                    .startNow()
                    .build();
            scheduler.addJob(job, false);
            scheduler.scheduleJob(trigger0);
            scheduler.scheduleJob(trigger1a);
            scheduler.scheduleJob(trigger1b);
            scheduler.scheduleJob(trigger2a);
            scheduler.scheduleJob(trigger2b);
            scheduler.scheduleJob(trigger2c);
            scheduler.scheduleJob(triggerOther1);
            scheduler.scheduleJob(triggerNull1);
            scheduler.scheduleJob(triggerNull2);

            Thread.sleep(4000);         // for misfire-on-startup to be activated

            scheduler.start();

            // wait to give triggers chance to fire
            Thread.sleep(2000);

            Map<String, Integer> running = threadPool.getRunningJobsPerExecutionGroup();
            System.out.println("Jobs per execution group: " + running);
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_0, null, running.get(EXECUTION_GROUP_0));
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_1, (Integer) 1, running.get(EXECUTION_GROUP_1));
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_1, (Integer) 2, running.get(EXECUTION_GROUP_2));
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_OTHER_1, (Integer) 1, running.get(EXECUTION_GROUP_OTHER_1));
            assertEquals("Wrong # of jobs with no execution group", (Integer) 2, running.get(null));

            scheduler.standby();

            interruptJobs(scheduler, job);

            scheduler.shutdown();

        } finally {
            cleanup(TEST_NAME);
        }
    }

    @Test
    public void testMisfiredWhileSchedulerPaused() throws SchedulerException, SQLException, InterruptedException {
        String TEST_NAME = "testMisfiredWhileSchedulerPaused";
        prepare(TEST_NAME);
        try {
            JobStore jobStore = createJobStore(TEST_NAME, 1000);

            DirectSchedulerFactory factory = DirectSchedulerFactory.getInstance();

            SimpleThreadPool threadPool = new SimpleThreadPool(10, Thread.NORM_PRIORITY);
            factory.createScheduler(threadPool, jobStore);
            Scheduler scheduler = factory.getScheduler();

            Map<String, Integer> limits = new HashMap<>();
            limits.put(EXECUTION_GROUP_0, 0);
            limits.put(EXECUTION_GROUP_1, 1);
            limits.put(EXECUTION_GROUP_2, 2);
            scheduler.setExecutionLimits(limits);

            scheduler.start();
            scheduler.standby();

            JobDetail job = JobBuilder.newJob(ExecutionLimitationTestJob.class)
                    .withIdentity("test")
                    .storeDurably()
                    .build();
            Trigger trigger0 = TriggerBuilder.newTrigger()
                    .withIdentity("t0")
                    .forJob(job)
                    .executionGroup(EXECUTION_GROUP_0)
                    .startNow()
                    .build();
            Trigger trigger1a = TriggerBuilder.newTrigger()
                    .withIdentity("t1a")
                    .forJob(job)
                    .executionGroup(EXECUTION_GROUP_1)
                    .startNow()
                    .build();
            Trigger trigger1b = TriggerBuilder.newTrigger()
                    .withIdentity("t1b")
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
            Trigger trigger2c = TriggerBuilder.newTrigger()
                    .withIdentity("t2c")
                    .forJob(job)
                    .executionGroup(EXECUTION_GROUP_2)
                    .startNow()
                    .build();
            Trigger triggerOther1 = TriggerBuilder.newTrigger()
                    .withIdentity("tOther1")
                    .forJob(job)
                    .executionGroup(EXECUTION_GROUP_OTHER_1)
                    .startNow()
                    .build();
            Trigger triggerNull1 = TriggerBuilder.newTrigger()
                    .withIdentity("tNull1")
                    .forJob(job)
                    .startNow()
                    .build();
            Trigger triggerNull2 = TriggerBuilder.newTrigger()
                    .withIdentity("tNull2")
                    .forJob(job)
                    .startNow()
                    .build();
            scheduler.addJob(job, false);
            scheduler.scheduleJob(trigger0);
            scheduler.scheduleJob(trigger1a);
            scheduler.scheduleJob(trigger1b);
            scheduler.scheduleJob(trigger2a);
            scheduler.scheduleJob(trigger2b);
            scheduler.scheduleJob(trigger2c);
            scheduler.scheduleJob(triggerOther1);
            scheduler.scheduleJob(triggerNull1);
            scheduler.scheduleJob(triggerNull2);

            Thread.sleep(4000);         // for misfire (using thread) to be activated

            scheduler.start();                  // resume the scheduler

            // wait to give triggers chance to fire
            Thread.sleep(2000);

            Map<String, Integer> running = threadPool.getRunningJobsPerExecutionGroup();
            System.out.println("Jobs per execution group: " + running);
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_0, null, running.get(EXECUTION_GROUP_0));
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_1, (Integer) 1, running.get(EXECUTION_GROUP_1));
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_1, (Integer) 2, running.get(EXECUTION_GROUP_2));
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_OTHER_1, (Integer) 1, running.get(EXECUTION_GROUP_OTHER_1));
            assertEquals("Wrong # of jobs with no execution group", (Integer) 2, running.get(null));

            scheduler.standby();

            interruptJobs(scheduler, job);

            scheduler.shutdown();

        } finally {
            cleanup(TEST_NAME);
        }
    }

    @Test
    public void testMisfiredWhileExecutionNotAllowed() throws SchedulerException, SQLException, InterruptedException {
        String TEST_NAME = "testMisfiredWhileExecutionNotAllowed";
        prepare(TEST_NAME);
        try {
            JobStore jobStore = createJobStore(TEST_NAME, 1000);

            DirectSchedulerFactory factory = DirectSchedulerFactory.getInstance();

            SimpleThreadPool threadPool = new SimpleThreadPool(10, Thread.NORM_PRIORITY);
            factory.createScheduler(threadPool, jobStore);
            Scheduler scheduler = factory.getScheduler();

            Map<String, Integer> limits = new HashMap<>();
            limits.put(EXECUTION_GROUP_0, 0);
            limits.put(EXECUTION_GROUP_1, 1);
            limits.put(EXECUTION_GROUP_2, 2);
            scheduler.setExecutionLimits(limits);

            scheduler.start();

            JobDetail job = JobBuilder.newJob(ExecutionLimitationTestJob.class)
                    .withIdentity("test")
                    .storeDurably()
                    .build();
            Trigger trigger0 = TriggerBuilder.newTrigger()
                    .withIdentity("t0")
                    .forJob(job)
                    .executionGroup(EXECUTION_GROUP_0)
                    .startNow()
                    .build();
            Trigger trigger1a = TriggerBuilder.newTrigger()
                    .withIdentity("t1a")
                    .forJob(job)
                    .executionGroup(EXECUTION_GROUP_1)
                    .startNow()
                    .build();
            Trigger trigger1b = TriggerBuilder.newTrigger()
                    .withIdentity("t1b")
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
            Trigger trigger2c = TriggerBuilder.newTrigger()
                    .withIdentity("t2c")
                    .forJob(job)
                    .executionGroup(EXECUTION_GROUP_2)
                    .startNow()
                    .build();
            Trigger triggerOther1 = TriggerBuilder.newTrigger()
                    .withIdentity("tOther1")
                    .forJob(job)
                    .executionGroup(EXECUTION_GROUP_OTHER_1)
                    .startNow()
                    .build();
            Trigger triggerNull1 = TriggerBuilder.newTrigger()
                    .withIdentity("tNull1")
                    .forJob(job)
                    .startNow()
                    .build();
            Trigger triggerNull2 = TriggerBuilder.newTrigger()
                    .withIdentity("tNull2")
                    .forJob(job)
                    .startNow()
                    .build();
            scheduler.addJob(job, false);
            scheduler.scheduleJob(trigger0);
            scheduler.scheduleJob(trigger1a);
            scheduler.scheduleJob(trigger1b);
            scheduler.scheduleJob(trigger2a);
            scheduler.scheduleJob(trigger2b);
            scheduler.scheduleJob(trigger2c);
            scheduler.scheduleJob(triggerOther1);
            scheduler.scheduleJob(triggerNull1);
            scheduler.scheduleJob(triggerNull2);

            // wait to give triggers chance to fire
            Thread.sleep(2000);

            Map<String, Integer> running = threadPool.getRunningJobsPerExecutionGroup();
            System.out.println("Jobs per execution group: " + running);
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_0, null, running.get(EXECUTION_GROUP_0));
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_1, (Integer) 1, running.get(EXECUTION_GROUP_1));
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_1, (Integer) 2, running.get(EXECUTION_GROUP_2));
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_OTHER_1, (Integer) 1, running.get(EXECUTION_GROUP_OTHER_1));
            assertEquals("Wrong # of jobs with no execution group", (Integer) 2, running.get(null));

            System.out.println("Setting new limits");

            Map<String, Integer> limits2 = new HashMap<>();
            limits2.put(EXECUTION_GROUP_0, 1);      // 1 new
            limits2.put(EXECUTION_GROUP_1, 2);      // 1 new
            limits2.put(EXECUTION_GROUP_2, 2);
            scheduler.setExecutionLimits(limits2);

            System.out.println("Waiting for misfired triggers to trigger");

            // wait to give triggers chance to fire
            if (jobStore instanceof RAMJobStore) {
                System.out.println("Waiting 60 seconds for RAMJobStore to process triggers");
                Thread.sleep(60000);
            } else {
                Thread.sleep(4000);
            }

            Map<String, Integer> running2 = threadPool.getRunningJobsPerExecutionGroup();
            System.out.println("Jobs per execution group: " + running2);
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_0, (Integer) 1, running2.get(EXECUTION_GROUP_0));
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_1, (Integer) 2, running2.get(EXECUTION_GROUP_1));
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_2, (Integer) 2, running2.get(EXECUTION_GROUP_2));
            assertEquals("Wrong # of jobs in " + EXECUTION_GROUP_OTHER_1, (Integer) 1, running2.get(EXECUTION_GROUP_OTHER_1));
            assertEquals("Wrong # of jobs with no execution group", (Integer) 2, running2.get(null));

            scheduler.standby();

            interruptJobs(scheduler, job);

            scheduler.shutdown();

        } finally {
            cleanup(TEST_NAME);
        }
    }

}
