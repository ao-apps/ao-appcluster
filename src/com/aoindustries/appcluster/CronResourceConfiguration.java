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

import com.aoindustries.cron.CronJob;
import com.aoindustries.cron.Schedule;

/**
 * The configuration for one resource that is scheduled as a CronJob.
 *
 * @see  CronJob
 *
 * @author  AO Industries, Inc.
 */
public interface CronResourceConfiguration<R extends Resource<R,RN>,RN extends ResourceNode<R,RN>> extends ResourceConfiguration<R,RN> {

	/**
	 * Gets the number of seconds before a synchronization pass times-out.
	 */
	int getSynchronizeTimeout();

	/**
	 * Gets the synchronization schedule between the local node and the remote node.
	 */
	Schedule getSynchronizeSchedule(RN localResourceNode, RN remoteResourceNode) throws AppClusterConfigurationException;

	/**
	 * Gets the number of seconds before a test pass times-out.
	 */
	int getTestTimeout();

	/**
	 * Gets the test schedule between the local node and the remote node.
	 */
	Schedule getTestSchedule(RN localResourceNode, RN remoteResourceNode) throws AppClusterConfigurationException;
}
