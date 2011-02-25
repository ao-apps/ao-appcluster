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
 * The set of statuses that apply to individual DNS records.
 *
 * @see DnsRecordResult
 *
 * @author  AO Industries, Inc.
 */
public enum DnsLookupStatus {
    SUCCESSFUL,
    UNRECOVERABLE,
    TRY_AGAIN,
    HOST_NOT_FOUND,
    TYPE_NOT_FOUND;

    @Override
    public String toString() {
        return ApplicationResources.accessor.getMessage("DnsLookupStatus." + name());
    }
}
