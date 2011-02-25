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

import com.aoindustries.util.Collections;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Contains the results of one DNS monitoring pass.
 *
 * @author  AO Industries, Inc.
 */
public class DnsResult {

    // private static final Logger logger = Logger.getLogger(DnsResult.class.getName());

    private final long startTime;
    private final long endTime;
    private final DnsStatus status;
    private final String statusMessage;
    private final SortedSet<String> warnings;
    private final SortedSet<String> errors;

    DnsResult(long startTime, long endTime, DnsStatus status, String statusMessage, Collection<String> warnings, Collection<String> errors) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.status = status;
        this.statusMessage = statusMessage;
        if(warnings==null) this.warnings = Collections.emptySortedSet();
        else this.warnings = java.util.Collections.unmodifiableSortedSet(new TreeSet<String>(warnings));
        if(errors==null) this.errors = Collections.emptySortedSet();
        else this.errors = java.util.Collections.unmodifiableSortedSet(new TreeSet<String>(errors));
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    /**
     * Gets the current status of this node.
     */
    public DnsStatus getStatus() {
        return status;
    }

    /**
     * Gets the current DNS status message or <code>null</code> if none.
     */
    public String getStatusMessage() {
        return statusMessage;
    }

    /**
     * Gets the most recent warnings for this resource.
     */
    public SortedSet<String> getWarnings() {
        return warnings;
    }

    /**
     * Gets the most recent errors for this resource.
     */
    public SortedSet<String> getErrors() {
        return errors;
    }
}
