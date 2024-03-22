package com.ascendix.jdbc.salesforce.statement.processor;

import com.ascendix.jdbc.salesforce.delegates.PartnerService;

import java.util.Locale;

public class SoslQueryAnalyzer extends SoqlQueryAnalyzer {

    public SoslQueryAnalyzer(String soql, PartnerService partnerService) {
        super(soql, partnerService);
    }

    public boolean analyse(String soql) {
        if (soql == null || soql.trim().isEmpty()) {
            return false;
        }
        return soql.toLowerCase(Locale.ROOT).startsWith("find ");
    }

}
