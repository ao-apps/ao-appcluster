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
package com.aoindustries.appcluster;

/**
 * The node settings for rsync synchronization.
 *
 * @author  AO Industries, Inc.
 */
public class RsyncResourceNode extends ResourceNode<RsyncResource,RsyncResourceNode> {

    //private static final Logger logger = Logger.getLogger(ResourceNode.class.getName());

    private final String username;
    private final String path;
    private final String backupDir;
    private final int backupDays;

    RsyncResourceNode(RsyncResource resource, Node node, AppClusterConfiguration.RsyncResourceNodeConfiguration resourceNodeConfiguration) {
        super(resource, node, resourceNodeConfiguration);
        this.username = resourceNodeConfiguration.getUsername();
        this.path = resourceNodeConfiguration.getPath();
        this.backupDir = resourceNodeConfiguration.getBackupDir();
        this.backupDays = resourceNodeConfiguration.getBackupDays();
    }

    /**
     * Gets the username used to connect to this node.
     */
    public String getUsername() {
        return username;
    }

    /**
     * Gets the path (without trailing slash) where the resource resides on this node.
     */
    public String getPath() {
        return path;
    }

    /**
     * Gets the directory where backups will be kept during synchronization.
     */
    public String getBackupDir() {
        return backupDir;
    }

    /**
     * Gets the number of days to keep backups.
     */
    public int getBackupDays() {
        return backupDays;
    }
}
