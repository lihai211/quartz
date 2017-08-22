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
