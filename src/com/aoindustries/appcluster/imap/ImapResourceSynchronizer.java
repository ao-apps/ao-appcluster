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
package com.aoindustries.appcluster.imap;

import com.aoindustries.appcluster.CronResourceSynchronizer;
import com.aoindustries.appcluster.NodeDnsStatus;
import com.aoindustries.appcluster.ResourceNodeDnsResult;
import com.aoindustries.appcluster.ResourceStatus;
import com.aoindustries.appcluster.ResourceSynchronizationMode;
import com.aoindustries.appcluster.ResourceSynchronizationResult;
import com.aoindustries.appcluster.ResourceSynchronizationResultStep;
import com.aoindustries.cron.Schedule;
import java.util.Collection;
import java.util.Collections;

/**
 * Performs synchronization using IMAP.
 *
 * @author  AO Industries, Inc.
 */
public class ImapResourceSynchronizer extends CronResourceSynchronizer<ImapResource,ImapResourceNode> {

	protected ImapResourceSynchronizer(ImapResourceNode localResourceNode, ImapResourceNode remoteResourceNode, Schedule synchronizeSchedule, Schedule testSchedule) {
		super(localResourceNode, remoteResourceNode, synchronizeSchedule, testSchedule);
	}

	/*
	 * May synchronize from a master to a slave.
	 * May test from a master to a slave or a slave to a master.
	 */
	@Override
	protected boolean canSynchronize(ResourceSynchronizationMode mode, ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult) {
		NodeDnsStatus localDnsStatus = localDnsResult.getNodeStatus();
		NodeDnsStatus remoteDnsStatus = remoteDnsResult.getNodeStatus();
		switch(mode) {
			case SYNCHRONIZE :
				return
					localDnsStatus==NodeDnsStatus.MASTER
					&& remoteDnsStatus==NodeDnsStatus.SLAVE
				;
			case TEST_ONLY :
				return
					(
						localDnsStatus==NodeDnsStatus.MASTER
						&& remoteDnsStatus==NodeDnsStatus.SLAVE
					) || (
						localDnsStatus==NodeDnsStatus.SLAVE
						&& remoteDnsStatus==NodeDnsStatus.MASTER
					)
				;
			default : throw new AssertionError("Unexpected mode: "+mode);
		}
	}

	@Override
	protected ResourceSynchronizationResult synchronize(ResourceSynchronizationMode mode, ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult) {
		long startTime = System.currentTimeMillis();
		return new ResourceSynchronizationResult(
			localResourceNode,
			remoteResourceNode,
			mode,
			Collections.singletonList(
				new ResourceSynchronizationResultStep(
					startTime,
					System.currentTimeMillis(),
					ResourceStatus.HEALTHY,
					"TODO",
					(Collection<String>)null,
					null,
					null
				)
			)
		);
	}
}
