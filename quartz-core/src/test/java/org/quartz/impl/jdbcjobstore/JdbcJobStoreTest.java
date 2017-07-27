/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.quartz.impl.jdbcjobstore;

import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.quartz.AbstractJobStoreTest;
import org.quartz.Trigger;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;

public class JdbcJobStoreTest extends AbstractJobStoreTest {

    private HashMap<String, JobStoreSupport> stores = new HashMap<String, JobStoreSupport>();

    private static final String STORE_EXECUTION_CAPABILITY_1 = "cap1";
    private static final String STORE_EXECUTION_CAPABILITY_2 = "cap2";
    private static final String OTHER_EXECUTION_CAPABILITY = "cap-other";

    public void testNothing() {
        // nothing
    }

    @Override
    protected JobStore createJobStore(String name) {
        try {
            JdbcQuartzTestUtilities.createDatabase(name);
            JobStoreTX jdbcJobStore = new JobStoreTX();
            jdbcJobStore.setDataSource(name);
            jdbcJobStore.setTablePrefix("QRTZ_");
            jdbcJobStore.setInstanceId("SINGLE_NODE_TEST");
            jdbcJobStore.setInstanceName(name);
            jdbcJobStore.setUseDBLocks(true);
            jdbcJobStore.setExecutionCapabilities(STORE_EXECUTION_CAPABILITY_1 + "," + STORE_EXECUTION_CAPABILITY_2);

            stores.put(name, jdbcJobStore);
            
            return jdbcJobStore;
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    /**
     *  Similar to {@link AbstractJobStoreTest#testAcquireNextTriggerBatch()}; but this one takes execution capabilities into account:
     *  early and trigger1 require capabilities that are present in current node, trigger2 requires non-existing execution capability.
     *  Trigger2 should not be returned by {@link JobStore#acquireNextTriggers} method.
     */
    public void testAcquireNextTriggerBatchExecutionCapabilities() throws Exception {

        long baseFireTime = System.currentTimeMillis() - 1000;

        OperableTrigger early =
                new SimpleTriggerImpl("early", "triggerGroup1", this.fJobDetail.getName(),
                        this.fJobDetail.getGroup(), new Date(baseFireTime),
                        new Date(baseFireTime + 5), 2, 2000);
        early.setRequiredCapability(STORE_EXECUTION_CAPABILITY_1);
        OperableTrigger trigger1 =
                new SimpleTriggerImpl("trigger1", "triggerGroup1", this.fJobDetail.getName(),
                        this.fJobDetail.getGroup(), new Date(baseFireTime + 200000),
                        new Date(baseFireTime + 200005), 2, 2000);
        trigger1.setRequiredCapability(STORE_EXECUTION_CAPABILITY_2);
        OperableTrigger trigger2 =
                new SimpleTriggerImpl("trigger2", "triggerGroup1", this.fJobDetail.getName(),
                        this.fJobDetail.getGroup(), new Date(baseFireTime + 210000),
                        new Date(baseFireTime + 210005), 2, 2000);
        trigger2.setRequiredCapability(OTHER_EXECUTION_CAPABILITY);
        OperableTrigger trigger3 =
                new SimpleTriggerImpl("trigger3", "triggerGroup1", this.fJobDetail.getName(),
                        this.fJobDetail.getGroup(), new Date(baseFireTime + 220000),
                        new Date(baseFireTime + 220005), 2, 2000);
        OperableTrigger trigger4 =
                new SimpleTriggerImpl("trigger4", "triggerGroup1", this.fJobDetail.getName(),
                        this.fJobDetail.getGroup(), new Date(baseFireTime + 230000),
                        new Date(baseFireTime + 230005), 2, 2000);
        OperableTrigger trigger10 =
                new SimpleTriggerImpl("trigger10", "triggerGroup2", this.fJobDetail.getName(),
                        this.fJobDetail.getGroup(), new Date(baseFireTime + 500000),
                        new Date(baseFireTime + 700000), 2, 2000);

        early.computeFirstFireTime(null);
        early.setMisfireInstruction(Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY);
        trigger1.computeFirstFireTime(null);
        trigger2.computeFirstFireTime(null);
        trigger3.computeFirstFireTime(null);
        trigger4.computeFirstFireTime(null);
        trigger10.computeFirstFireTime(null);
        this.fJobStore.storeTrigger(early, false);
        this.fJobStore.storeTrigger(trigger1, false);
        this.fJobStore.storeTrigger(trigger2, false);
        this.fJobStore.storeTrigger(trigger3, false);
        this.fJobStore.storeTrigger(trigger4, false);
        this.fJobStore.storeTrigger(trigger10, false);

        long firstFireTime = new Date(trigger1.getNextFireTime().getTime()).getTime();

        List<OperableTrigger> acquiredTriggers = this.fJobStore.acquireNextTriggers(firstFireTime + 10000, 4, 1000L);
        assertEquals(1, acquiredTriggers.size());
        assertEquals(early.getKey(), acquiredTriggers.get(0).getKey());
        this.fJobStore.releaseAcquiredTrigger(early);

        acquiredTriggers = this.fJobStore.acquireNextTriggers(firstFireTime + 10000, 4, 205000);
        assertEquals(2, acquiredTriggers.size());
        assertEquals(early.getKey(), acquiredTriggers.get(0).getKey());
        assertEquals(trigger1.getKey(), acquiredTriggers.get(1).getKey());
        this.fJobStore.releaseAcquiredTrigger(early);
        this.fJobStore.releaseAcquiredTrigger(trigger1);

        this.fJobStore.removeTrigger(early.getKey());

        acquiredTriggers = this.fJobStore.acquireNextTriggers(firstFireTime + 10000, 5, 100000L);
        assertEquals(3, acquiredTriggers.size());
        assertEquals(trigger1.getKey(), acquiredTriggers.get(0).getKey());
        // trigger2 should not be present because of execution capability
        assertEquals(trigger3.getKey(), acquiredTriggers.get(1).getKey());
        assertEquals(trigger4.getKey(), acquiredTriggers.get(2).getKey());
        this.fJobStore.releaseAcquiredTrigger(trigger1);
        this.fJobStore.releaseAcquiredTrigger(trigger3);
        this.fJobStore.releaseAcquiredTrigger(trigger4);

        acquiredTriggers = this.fJobStore.acquireNextTriggers(firstFireTime + 10000, 6, 100000L);
        assertEquals(3, acquiredTriggers.size());
        assertEquals(trigger1.getKey(), acquiredTriggers.get(0).getKey());
        // trigger2 should not be present because of execution capability
        assertEquals(trigger3.getKey(), acquiredTriggers.get(1).getKey());
        assertEquals(trigger4.getKey(), acquiredTriggers.get(2).getKey());
        this.fJobStore.releaseAcquiredTrigger(trigger1);
        this.fJobStore.releaseAcquiredTrigger(trigger3);
        this.fJobStore.releaseAcquiredTrigger(trigger4);

        acquiredTriggers = this.fJobStore.acquireNextTriggers(firstFireTime + 1, 5, 0L);
        assertEquals(1, acquiredTriggers.size());
        assertEquals(trigger1.getKey(), acquiredTriggers.get(0).getKey());
        this.fJobStore.releaseAcquiredTrigger(trigger1);

        acquiredTriggers = this.fJobStore.acquireNextTriggers(firstFireTime + 250, 5, 19999L);
        assertEquals(1, acquiredTriggers.size());
        assertEquals(trigger1.getKey(), acquiredTriggers.get(0).getKey());
        // trigger2 should not be present because of execution capability
        this.fJobStore.releaseAcquiredTrigger(trigger1);
        this.fJobStore.releaseAcquiredTrigger(trigger3);

        acquiredTriggers = this.fJobStore.acquireNextTriggers(firstFireTime + 150, 5, 5000L);
        assertEquals(1, acquiredTriggers.size());
        assertEquals(trigger1.getKey(), acquiredTriggers.get(0).getKey());
        this.fJobStore.releaseAcquiredTrigger(trigger1);
    }

    @Override
    protected void destroyJobStore(String name) {
        try {
        	JobStoreSupport jdbcJobStore = stores.remove(name);
        	jdbcJobStore.shutdown();
        	
            JdbcQuartzTestUtilities.destroyDatabase(name);
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }
}
