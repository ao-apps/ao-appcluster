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

import java.util.Collection;
import java.util.SortedSet;
import org.xbill.DNS.Name;

/**
 * Contains the results of one DNS record lookup.
 *
 * @author  AO Industries, Inc.
 */
public class DnsLookupResult {

    // private static final Logger logger = Logger.getLogger(DnsLookupResult.class.getName());

    private final Name name;
    private final DnsLookupStatus status;
    private final SortedSet<String> addresses;
    private final SortedSet<String> warnings;
    private final SortedSet<String> errors;

    /**
     * Sorts the addresses as they are added.
     */
    DnsLookupResult(
        Name name,
        DnsLookupStatus status,
        String[] addresses,
        Collection<String> warnings,
        Collection<String> errors
    ) {
        this.name = name;
        this.status = status;
        this.addresses = ResourceDnsResult.getUnmodifiableSortedSet(addresses, null); // Sorts lexically for speed since not human readable
        assert status==DnsLookupStatus.SUCCESSFUL ? !this.addresses.isEmpty() : this.addresses.isEmpty();
        this.warnings = ResourceDnsResult.getUnmodifiableSortedSet(warnings, ResourceDnsResult.defaultLocaleCollator);
        this.errors = ResourceDnsResult.getUnmodifiableSortedSet(errors, ResourceDnsResult.defaultLocaleCollator);
    }

    public Name getName() {
        return name;
    }

    public DnsLookupStatus getStatus() {
        return status;
    }

    /**
     * Only relevant for SUCCESSFUL lookups.
     */
    public SortedSet<String> getAddresses() {
        return addresses;
    }

    /**
     * Gets the warnings for this lookup.
     */
    public SortedSet<String> getWarnings() {
        return warnings;
    }

    /**
     * Gets the errors for this lookup.
     */
    public SortedSet<String> getErrors() {
        return errors;
    }
}
