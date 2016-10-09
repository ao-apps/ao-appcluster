/*
 * ao-appcluster - Application-level clustering tools.
 * Copyright (C) 2011, 2016  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of ao-appcluster.
 *
 * ao-appcluster is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ao-appcluster is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with ao-appcluster.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.appcluster;

import com.aoindustries.cron.Schedule;

/**
 * The configuration for one resource that is scheduled as a CronJob.
 *
 * @author  AO Industries, Inc.
 */
public abstract class CronResourcePropertiesConfiguration<R extends CronResource<R,RN>,RN extends CronResourceNode<R,RN>> extends ResourcePropertiesConfiguration<R,RN> implements CronResourceConfiguration<R,RN> {

	private final int synchronizeTimeout;
	private final int testTimeout;

	protected CronResourcePropertiesConfiguration(AppClusterPropertiesConfiguration properties, String id) throws AppClusterConfigurationException {
		super(properties, id);
		this.synchronizeTimeout = properties.getInt("appcluster.resource."+id+".timeout.sync");
		this.testTimeout = properties.getInt("appcluster.resource."+id+".timeout.test");
	}

	@Override
	public int getSynchronizeTimeout() {
		return synchronizeTimeout;
	}

	@Override
	public Schedule getSynchronizeSchedule(RN localResourceNode, RN remoteResourceNode) throws AppClusterConfigurationException {
		assert localResourceNode.getResource()==remoteResourceNode.getResource();
		return properties.getSchedule("appcluster.resource."+id+".schedule.sync."+localResourceNode.getNode().getId()+"."+remoteResourceNode.getNode().getId());
	}

	@Override
	public int getTestTimeout() {
		return testTimeout;
	}

	@Override
	public Schedule getTestSchedule(RN localResourceNode, RN remoteResourceNode) throws AppClusterConfigurationException {
		assert localResourceNode.getResource()==remoteResourceNode.getResource();
		return properties.getSchedule("appcluster.resource."+id+".schedule.test."+localResourceNode.getNode().getId()+"."+remoteResourceNode.getNode().getId());
	}
}
