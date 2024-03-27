package com.ascendix.jdbc.salesforce.oauth;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ascendix.jdbc.salesforce.oauth.ForceUserInfo;
import com.ascendix.jdbc.salesforce.oauth.ForceUserInfoParser;
import org.junit.jupiter.api.Test;

public class ForceUserInfoParserTest {

    @Test
    public void testParse() {
        String json = """
{
   "user_id": "12345",
   "organization_id": "67890",
   "preferred_username": "johndoe",
   "nickname": "john",
   "name": "John Doe",
   "email": "john.doe@example.com",
   "zoneinfo": "America/Los_Angeles",
   "locale": "en-US",
   "instance": "https://my-instance.salesforce.com",
   "urls": {
      "login_url": "https://login.salesforce.com",
      "partner": "https://my-instance.salesforce.com/services/data/v{version}/sobjects/"
   }
}
""";

        ForceUserInfo userInfo = ForceUserInfoParser.parse(json);

        assertEquals("12345", userInfo.getUserId());
        assertEquals("67890", userInfo.getOrganizationId());
        assertEquals("johndoe", userInfo.getPreferredUsername());
        assertEquals("john", userInfo.getNickName());
        assertEquals("John Doe", userInfo.getName());
        assertEquals("john.doe@example.com", userInfo.getEmail());
        assertEquals("America/Los_Angeles", userInfo.getTimeZone());
        assertEquals("en-US", userInfo.getLocale());
        assertEquals("my-instance", userInfo.getInstance());
        assertEquals("https://my-instance.salesforce.com/services/data/v45/sobjects/", userInfo.getPartnerUrl());
    }
}