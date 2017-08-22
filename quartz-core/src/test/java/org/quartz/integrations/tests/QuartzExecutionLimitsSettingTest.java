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
package org.quartz.integrations.tests;

import org.junit.Test;
import org.quartz.Scheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Tests setting execution limits via properties
 * @author mederly
 */
public class QuartzExecutionLimitsSettingTest extends QuartzMemoryTestSupport {
    @Test
    public void testParseExecutionLimits() throws Exception {
        Map<String, Integer> limits = new HashMap<>(scheduler.getExecutionLimits());
        System.out.println("Execution limits as parsed: " + limits);

        Map<String, Integer> expected = new HashMap<>();
        expected.put("group1", 12);
        expected.put("group2", 0);
        expected.put("group3", null);
        expected.put("group4", null);
        expected.put("group5", null);
        expected.put("group6", null);
        expected.put(null, 11);
        expected.put(Scheduler.LIMIT_FOR_OTHER_GROUPS, 42);
        assertEquals("Wrong execution limits", expected, limits);
    }

    @Override
    protected Properties createSchedulerProperties() {
        Properties props = super.createSchedulerProperties();
        props.put("org.quartz.executionLimit.group1", "12");
        props.put("org.quartz.executionLimit.group2", "0");             // no threads for group2

        // ways to set 'no limit' (i.e. unlimited number)
        props.put("org.quartz.executionLimit.group3", "null");
        props.put("org.quartz.executionLimit.group4", "none");
        props.put("org.quartz.executionLimit.group5", "unlimited");
        props.put("org.quartz.executionLimit.group6", "");

        // ways to set limit for triggers without an execution group
        props.put("org.quartz.executionLimit._", "11");
        props.put("org.quartz.executionLimit.null", "11");

        // setting default limit for unlisted groups
        props.put("org.quartz.executionLimit.*", "42");

        return props;
    }
}
