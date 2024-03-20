package com.ascendix.salesforce.oauth;

import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ForceUserInfo {

    private String userId;
    private String organizationId;
    private String preferredUsername;
    private String nickName;
    private String name;
    private String email;
    private String timeZone;
    private String locale;
    private String instance;
    private String partnerUrl;
    private Map<String, String> urls;
}
