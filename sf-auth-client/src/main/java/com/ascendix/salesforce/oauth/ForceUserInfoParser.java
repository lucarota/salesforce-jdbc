package com.ascendix.salesforce.oauth;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

@SuppressWarnings("unchecked")
public class ForceUserInfoParser {

    private static final String API_VERSION = "45";

    public static ForceUserInfo parse(String json) {
        ForceUserInfo userInfo = new ForceUserInfo();
        JSONParser parser = new JSONParser(json);
        Object o = parser.parse();
        if (o instanceof Map<?,?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                String key = (String) entry.getKey();

                if ("urls".equals(key) && entry.getValue() instanceof Map<?, ?> urls) {
                    userInfo.setUrls((Map<String, String>) urls);
                }
                if (entry.getValue() instanceof String value) {
                    switch (key) {
                        case "user_id":
                            userInfo.setUserId(value);
                            break;
                        case "organization_id":
                            userInfo.setOrganizationId(value);
                            break;
                        case "preferred_username":
                            userInfo.setPreferredUsername(value);
                            break;
                        case "nickname":
                            userInfo.setNickName(value);
                            break;
                        case "name":
                            userInfo.setName(value);
                            break;
                        case "email":
                            userInfo.setEmail(value);
                            break;
                        case "zoneinfo":
                            userInfo.setTimeZone(value);
                            break;
                        case "locale":
                            userInfo.setLocale(value);
                            break;
                        case "instance":
                            userInfo.setInstance(value);
                            break;
                        case "partner_url":
                            userInfo.setPartnerUrl(value);
                            break;
                    }
                }
            }
        }

        extractPartnerUrl(userInfo);
        extractInstance(userInfo);

        return userInfo;
    }

    private static void extractPartnerUrl(ForceUserInfo userInfo) {
        if (userInfo.getUrls() == null || !userInfo.getUrls().containsKey("partner")) {
            throw new IllegalStateException("User info doesn't contain partner URL: " + userInfo.getUrls());
        }
        userInfo.setPartnerUrl(userInfo.getUrls().get("partner").replace("{version}", API_VERSION));
    }

    private static void extractInstance(ForceUserInfo userInfo) {
        String profileUrl = userInfo.getPartnerUrl();
        if (StringUtils.isBlank(profileUrl)) {
            return;
        }
        profileUrl = profileUrl.replace("https://", "");
        String instance = StringUtils.split(profileUrl, '.')[0];
        userInfo.setInstance(instance);
    }
}
