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
