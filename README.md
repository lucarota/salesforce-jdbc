# Salesforce JDBC Driver

The Salesforce JDBC driver enables Java applications to connect to Salesforce data services using standard, database-independent Java code. It is an open-source JDBC driver written in pure Java that communicates over the SOAP/HTTP(S) protocol.

The primary purpose of this driver is to retrieve data from Salesforce services for data analysis. It is particularly optimized for use with the Eclipse BIRT engine.

This project is a fork of the [original repository](https://github.com/ascendix/salesforce-jdbc), which had compatibility issues with IntelliJ IDEA. This version addresses those limitations by implementing:
* Table and column name filtering
* Case-insensitive handling for table and column names
* Metadata support for queries, allowing IntelliJ to correctly process results

[Watch the demo video](docs/SOQL-JDBC-IntelliJ-demo-264.mp4)

[![Watch the demo video](docs/intelliJ.png)](docs/SOQL-JDBC-IntelliJ-demo-264.mp4)

## Supported Versions
* **Salesforce Partner API:** Version 64.0
* **Java:** Java 17

## Getting the Driver
You can download the latest driver JAR file from the [Releases page](https://github.com/lucarota/salesforce-jdbc/releases).

## Supported Features

1. **Native SOQL Queries**
   ```sql
   SELECT Id, Account.Name, Owner.Id, Owner.Name FROM Account;
   
   -- The * wildcard expands to the first 100 fields of the root entity
   SELECT * FROM Account;
   ```

2. **Nested Queries**

3. **INSERT and UPDATE Statements**
   Supported functions for value calculation:
   * `NOW()`
   * `GETDATE()`
    
   Example:
   ```sql
   INSERT INTO Account(Name, Phone) VALUES 
    ('Account01', '555-123-1111'),
    ('Account02', '555-123-2222');
    
   INSERT INTO Contact(FirstName, LastName, AccountId) 
      SELECT Name, Phone, Id 
      FROM Account
      WHERE Name LIKE 'Account0%';

   UPDATE Contact SET LastName = 'Updated_Now_' + NOW()
      WHERE AccountId IN (
          SELECT Id FROM Account WHERE Phone = '555-123-1111' AND CreatedDate > '{ts 2020-01-01 00:10:12Z}'
      );
   ```

4. **DELETE Statements**
   ```sql
   DELETE FROM Opportunity WHERE Name LIKE 'Pushed Out Insightor Opp%';
   ```   
   ![Example of response](docs/delete_where.png)

5. **Request Caching**
   Supports local caching in two modes:
   * **Global:** Cached results are accessible to all system users within the JVM session.
   * **Session:** Caching is isolated to each Salesforce connection session.
   
   Cache duration is limited to the JVM lifespan or a maximum of 1 hour. Enable caching using a query prefix:

   * Global cache mode:
     ```sql
     CACHE GLOBAL SELECT Id, Name FROM Account
     ```
   * Session cache mode:
     ```sql
     CACHE SESSION SELECT Id, Name FROM Account
     ```

6. **Nullity Comparison Rewriting**
   Standard SQL uses `IS NULL` and `IS NOT NULL` to check for nullity, which Salesforce SOQL does not natively support (it requires `= NULL` and `<> NULL` / `!= NULL`). The driver automatically rewrites standard `IS NULL` and `IS NOT NULL` comparisons to their SOQL-compatible equivalents.

   Example:
   ```sql
   SELECT Id FROM Account WHERE Phone IS NULL;
   -- Rewritten internally to:
   -- SELECT Id FROM Account WHERE Phone = NULL
   ```
 
7. **Custom Functions**
   The driver implements client-side functions that are not natively supported by Salesforce SOQL:
   * **`COALESCE(expression1, expression2, ...)`**
     Evaluates the arguments in order and returns the first non-NULL value.
     
     * **In SELECT projection:**
       Since SOQL does not natively support `COALESCE` in projection, the driver rewrites the query to request all underlying columns, then evaluates the function in-memory on the client side. This may impact query performance and increase network traffic.
       
       Example:
       ```sql
       SELECT COALESCE(Phone, Fax, 'N/A') AS contact_info FROM Account;
       ```
       
     * **In WHERE clause:**
       When used in a `WHERE` clause, the driver automatically rewrites the condition into equivalent standard SOQL boolean logic (`AND` / `OR` expressions). This allows Salesforce to perform the filtering database-side.
      
      Example:
      ```sql
      SELECT Id, Name FROM Account WHERE COALESCE(Phone, Fax) = '555-123-1111';
      -- Rewritten internally to:
      -- SELECT Id, Name FROM Account WHERE ((Phone = '555-123-1111') OR (Phone = NULL AND Fax = '555-123-1111'))
      ```
      
       Supported operators in the `WHERE` clause include standard comparisons (`=`, `!=`, `<`, `>`, `LIKE`, etc.) as well as nullity checks (`IS NULL` / `IS NOT NULL`). The rewriter also performs client-side constant folding to simplify logic and prevent illegal SOQL comparisons.

8. **Client-Side `CASE WHEN` Expression in SELECT**
   Since Salesforce SOQL does not natively support `CASE WHEN` expressions, the driver evaluates them in-memory on the client side. The underlying columns referenced in the `CASE` expression are automatically fetched from Salesforce, and the result is computed row-by-row before being returned in the `ResultSet`.

   Both **simple** and **searched** forms are supported:

   * **Searched `CASE` (with `WHEN <condition>`):**
     ```sql
     SELECT
       CASE
         WHEN Amount > 1000 THEN 'BIG'
         ELSE 'SMALL'
       END AS size_label
     FROM Opportunity;
     ```

   * **Simple `CASE` (with `WHEN <value>`):**
     ```sql
     SELECT
       CASE Status
         WHEN 'Open' THEN 'Active'
         WHEN 'Closed' THEN 'Done'
         ELSE 'Unknown'
       END AS status_label
     FROM Opportunity;
     ```

   > **Note:** Since evaluation is client-side, all columns referenced in the `CASE` expression are fetched from Salesforce before filtering. This may impact performance and increase network traffic for large result sets.

## Maven Dependency

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>it.rotaliano.salesforce</groupId>
    <artifactId>salesforce-jdbc</artifactId>
    <version>2.0.2</version>
</dependency>
```

## Connection Configuration

### Driver Class Name
`it.rotaliano.jdbc.salesforce.ForceDriver`

### JDBC URL Format
```
jdbc:rotaliano:salesforce://[;propertyName1=propertyValue1[;propertyName2=propertyValue2]...]
```

You can connect using **User/Password**, **Session ID**, or **OAuth 2.0 Client Credentials**.

**1. User and Password**
```
jdbc:rotaliano:salesforce://;user=myname@companyorg.com;password=passwordandsecretkey
```
*Note: The password must be a concatenation of your Salesforce password and security token.*

**2. Session ID**
```
jdbc:rotaliano:salesforce://;sessionId=uniqueIdAssociatedWithTheSession
```
*Note: User and password parameters are ignored if `sessionId` is provided.*

**3. OAuth 2.0 Client Credentials**
```
jdbc:rotaliano:salesforce://mycompany.my.salesforce.com;clientId=yourClientId;clientSecret=yourClientSecret
```
*Note: A custom `loginDomain` is required for this authentication flow (e.g. `mycompany.my.salesforce.com`). Generic domains like `login.salesforce.com` and `test.salesforce.com` are not supported by Salesforce for the Client Credentials flow. User/password parameters and session ID are ignored when `clientId` and `clientSecret` are provided.*

### Configuration Properties

| Property        | Description                                          | Default Value |
| --------------- | ---------------------------------------------------- | ------------- |
| `user`          | Login username.                                      |               |
| `password`      | Login password concatenated with the security token. |               |
| `sessionId`     | Unique ID associated with an active session.         |               |
| `clientId` / `client_id` | OAuth 2.0 Client ID (Consumer Key) for Client Credentials flow. | |
| `clientSecret` / `client_secret` | OAuth 2.0 Client Secret (Consumer Secret) for Client Credentials flow. | |
| `loginDomain`   | Top-level domain for login requests. Set to `test.salesforce.com` for legacy sandbox login, or custom domain (e.g. `mycompany.my.salesforce.com`) for OAuth. | `login.salesforce.com` |
| `https`         | Use HTTP instead of HTTPS if set to `false`.         | `true`        |
| `api`           | API version to use.                                  | `64`          |
| `client`        | Legacy client identifier parameter.                  | Empty         |
| `insecurehttps` | Allow invalid SSL certificates.                      | `false`       |

## IDE Configuration

### Eclipse BIRT
1. Follow the guide on [How to add a JDBC driver](https://help.eclipse.org/mars/index.jsp?topic=%2Forg.eclipse.birt.doc%2Fbirt%2Fcon-HowToAddAJDBCDriver.html).
2. Set configuration properties using property binding in the data source editor.
   
   ![BIRT Data Source](docs/birt/Data%20source%20-%20property%20binding.png)
   
   Refer to the [Salesforce JDBC report sample](docs/birt/Salesforce JDBC sample.rptdesign) for a complete example.

### IntelliJ IDEA
1. Follow the guide on [How to add a JDBC driver](https://www.jetbrains.com/help/idea/data-sources-and-drivers-dialog.html).
2. Configure the JDBC URL with the necessary properties.
   
   Example URL:
   ```
   jdbc:rotaliano:salesforce://dev@Local.org:123456@localorg.localhost.internal.salesforce.com:6109?https=false&api=61.0
   ```
   
   Ensure you verify your access type (HTTP/HTTPS) and API version.
   
   IntelliJ supports autocomplete for SOQL queries:
   ![Autocomplete SOQL](docs/Autocomplete-SOQL.png)

## Troubleshooting

### WSDL Issues
To update `partners.wsdl`:

1. Clone and build [force-wsc](https://github.com/forcedotcom/wsc).
2. Run the following command:
   ```bash
   java -jar target/force-wsc-50.0.0-uber.jar blt/app/main/core/shared/submodules/wsdl/src/main/wsdl/partner.wsdl sforce-partner.jar
   ```
3. Copy `com.sforce.soap` to the driver source.

## Version History
See [CHANGELOG.md](https://github.com/lucarota/salesforce-jdbc/blob/master/CHANGELOG.md).

## Contributing
We welcome contributions! Please review the following guides:

- [Contributing Guidelines](https://github.com/lucarota/salesforce-jdbc/blob/master/CONTRIBUTING.md)
- [Code of Conduct](https://github.com/lucarota/salesforce-jdbc/blob/master/.github/CODE-OF-CONDUCT.md)

Also, consider sponsoring this project! ✌️

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
