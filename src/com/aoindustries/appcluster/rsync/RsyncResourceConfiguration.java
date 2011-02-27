/*
 * ao-appcluster - Coordinates system components installed in master/slave replication.
 * Copyright (C) 2011  AO Industries, Inc.
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
package com.aoindustries.appcluster.rsync;

import com.aoindustries.appcluster.AppCluster;
import com.aoindustries.appcluster.AppClusterConfigurationException;
import com.aoindustries.appcluster.ResourceConfiguration;
import com.aoindustries.appcluster.ResourceNode;
import java.util.Collection;
import java.util.Set;

/**
 * The configuration for a rsync resource.
 *
 * @author  AO Industries, Inc.
 */
public interface RsyncResourceConfiguration extends ResourceConfiguration<RsyncResource,RsyncResourceNode> {

    /**
     * @see RsyncResource#getAllowMultiMaster()
     */
    boolean getAllowMultiMaster();

    /**
     * Gets if files will be deleted during the rsync.
     */
    boolean isDelete();

    @Override
    Set<? extends RsyncResourceNodeConfiguration> getResourceNodeConfigurations() throws AppClusterConfigurationException;

    @Override
    RsyncResource newResource(AppCluster cluster, Collection<? extends ResourceNode<?,?>> resourceNodes) throws AppClusterConfigurationException;
}
