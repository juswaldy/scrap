/*------------------------------------------------------------------------------
-- Security Audit & Remediation Toolkit
--
-- Database: _meta   Schema: sec
--
-- Purpose:
--   Declarative security management for SQL Server. The desired state of
--   logins, database principals, role memberships, and permissions is
--   stored in sec.* tables. The objects in this file compare that desired
--   state against the live server and generate (or execute) T-SQL to
--   reconcile the two.
--
-- Object inventory:
--   Functions (inline table-valued)
--     sec.ifGetActualServerLogins          – snapshot of server logins
--     sec.ifGetActualServerRoleMemberships  – server role memberships
--     sec.ifGetActualServerPermissions      – server-level permissions
--
--   Comparison procedures (report-only)
--     sec.spCompareServerLogins            – logins: missing / extra / drift
--     sec.spCompareServerRoleMemberships   – server role membership drift
--     sec.spCompareServerPermissions       – server permission drift
--     sec.spCompareDbPrincipals            – database users & roles drift
--     sec.spCompareDbRoleMemberships       – database role membership drift
--     sec.spCompareDbPermissions           – database permission drift
--
--   Orchestrator / action procedures
--     sec.spAuditPermissions               – runs all six comparisons
--     sec.spApplySecurityChanges            – collects & optionally executes
--                                            remediation commands
--
-- Dependencies:
--   Tables / views referenced (must exist before running):
--     sec.ServerLogin, sec.ServerSecurityAssignment,
--     sec.vwServerRoleMemberships, sec.vwServerPermissions,
--     sec.DbPrincipal, sec.DbSecurityAssignment.
--
-- Quick-start examples:
--
--   -- Full audit of the current server
--   EXEC sec.spAuditPermissions;
--
--   -- Audit a single database
--   EXEC sec.spAuditPermissions @DbName = 'MyDatabase';
--
--   -- Preview remediation commands (dry run)
--   EXEC sec.spApplySecurityChanges @WhatIf = 1;
--
--   -- Apply missing items only (safe)
--   EXEC sec.spApplySecurityChanges @WhatIf = 0, @ApplyMissing = 1,
--                                   @ApplyExtras = 0;
--
--   -- Apply everything including drops (dangerous)
--   EXEC sec.spApplySecurityChanges @WhatIf = 0, @ApplyMissing = 1,
--                                   @ApplyExtras = 1;
--
-- Change history:
-- 2026-01-26 JJJ Initial version.
------------------------------------------------------------------------------*/
USE _meta;
GO

---------------------------------------------------------------------------
-- sec.ifGetActualServerLogins
--
-- Inline table-valued function that returns a snapshot of all "real"
-- server logins from sys.server_principals, normalising the type codes
-- into human-readable LoginType values (SQL / WINDOWS / EXTERNAL).
--
-- Excluded principals:
--   - Internal certificate-based logins (##...##).
--   - Built-in accounts: sa, NT AUTHORITY\SYSTEM, the SQL Server and
--     SQL Agent service accounts.
--
-- The @HostName parameter is not used for filtering — it is passed
-- through as a column so that results can be joined with the sec.*
-- definition tables, which are partitioned by HostName.
--
-- Columns returned:
--   HostName, LoginName, LoginType, CheckPolicy, CheckExpiration,
--   DefaultDatabase, DefaultLanguage, Disabled, SID.
--
-- Used by: sec.spCompareServerLogins, sec.spApplySecurityChanges.
--
-- Change history:
-- 2026-01-26 JJJ Initial version.
---------------------------------------------------------------------------
CREATE OR ALTER FUNCTION sec.ifGetActualServerLogins(@HostName sysname)
RETURNS TABLE
AS
RETURN
(
    SELECT
        @HostName AS HostName,
        sp.name AS LoginName,
        CASE sp.type
            WHEN 'S' THEN 'SQL'
            WHEN 'U' THEN 'WINDOWS'
            WHEN 'G' THEN 'WINDOWS'
            WHEN 'E' THEN 'EXTERNAL'
            WHEN 'X' THEN 'EXTERNAL'
            ELSE 'UNKNOWN'
        END AS LoginType,
        CASE WHEN sl.is_policy_checked = 1 THEN 1 ELSE 0 END AS CheckPolicy,
        CASE WHEN sl.is_expiration_checked = 1 THEN 1 ELSE 0 END AS CheckExpiration,
        sp.default_database_name AS DefaultDatabase,
        sp.default_language_name AS DefaultLanguage,
        CASE WHEN sp.is_disabled = 1 THEN 1 ELSE 0 END AS Disabled,
        sp.sid AS SID
    FROM sys.server_principals sp
    LEFT JOIN sys.sql_logins sl ON sp.principal_id = sl.principal_id
    WHERE sp.type IN ('S', 'U', 'G', 'E', 'X')
      AND sp.name NOT LIKE '##%##'
      AND sp.name NOT IN ('sa', 'NT AUTHORITY\SYSTEM', 'NT SERVICE\MSSQLSERVER', 'NT SERVICE\SQLSERVERAGENT')
);
GO

---------------------------------------------------------------------------
-- sec.ifGetActualServerRoleMemberships
--
-- Inline table-valued function that returns every server-role-to-login
-- membership on the current instance by joining sys.server_role_members
-- with sys.server_principals.
--
-- Excluded principals:
--   - Internal certificate-based logins (##...##).
--
-- The @HostName parameter is passed through as a column (not used for
-- filtering) so results can be compared against sec.ServerSecurityAssignment.
--
-- Columns returned:
--   HostName, LoginName, ServerRoleName.
--
-- Used by: sec.spCompareServerRoleMemberships, sec.spApplySecurityChanges.
--
-- Change history:
-- 2026-01-26 JJJ Initial version.
---------------------------------------------------------------------------
CREATE OR ALTER FUNCTION sec.ifGetActualServerRoleMemberships(@HostName sysname)
RETURNS TABLE
AS
RETURN
(
    SELECT
        @HostName AS HostName,
        member.name AS LoginName,
        role.name AS ServerRoleName
    FROM sys.server_role_members rm
    JOIN sys.server_principals role ON rm.role_principal_id = role.principal_id
    JOIN sys.server_principals member ON rm.member_principal_id = member.principal_id
    WHERE role.type = 'R'
      AND member.type IN ('S', 'U', 'G', 'E', 'X')
      AND member.name NOT LIKE '##%##'
);
GO

---------------------------------------------------------------------------
-- sec.ifGetActualServerPermissions
--
-- Inline table-valued function that returns all explicit server-level
-- permissions from sys.server_permissions, including permissions scoped
-- to endpoints and availability groups.
--
-- Permission states are normalised to GRANT (covers both G and W) or
-- DENY. The implicit CONNECT SQL permission that every login receives
-- is excluded to reduce noise in comparison reports.
--
-- Scope classification:
--   class 100 → SERVER
--   class 105 → ENDPOINT  (EndpointName populated)
--   class 108 → AVAILABILITY_GROUP (AGName populated)
--
-- The @HostName parameter is passed through as a column for joining
-- with the sec.* definition tables.
--
-- Columns returned:
--   HostName, LoginName, State, Scope, PermissionName,
--   EndpointName, AGName, WithGrantOption.
--
-- Used by: sec.spCompareServerPermissions, sec.spApplySecurityChanges.
--
-- Change history:
-- 2026-01-26 JJJ Initial version.
---------------------------------------------------------------------------
CREATE OR ALTER FUNCTION sec.ifGetActualServerPermissions(@HostName sysname)
RETURNS TABLE
AS
RETURN
(
    SELECT
        @HostName AS HostName,
        pr.name AS LoginName,
        CASE pe.state WHEN 'G' THEN 'GRANT' WHEN 'W' THEN 'GRANT' WHEN 'D' THEN 'DENY' END AS State,
        CASE 
            WHEN pe.class = 100 THEN 'SERVER'
            WHEN pe.class = 105 THEN 'ENDPOINT'
            WHEN pe.class = 108 THEN 'AVAILABILITY_GROUP'
            ELSE 'SERVER'
        END AS Scope,
        pe.permission_name AS PermissionName,
        ep.name AS EndpointName,
        ag.name AS AGName,
        CASE WHEN pe.state = 'W' THEN 1 ELSE 0 END AS WithGrantOption
    FROM sys.server_permissions pe
    JOIN sys.server_principals pr ON pe.grantee_principal_id = pr.principal_id
    LEFT JOIN sys.endpoints ep ON pe.class = 105 AND pe.major_id = ep.endpoint_id
    LEFT JOIN sys.availability_replicas ar ON pe.class = 108 AND pe.major_id = ar.replica_metadata_id
    LEFT JOIN sys.availability_groups ag ON ar.group_id = ag.group_id
    WHERE pr.type IN ('S', 'U', 'G', 'E', 'X', 'R')
      AND pr.name NOT LIKE '##%##'
      AND pe.permission_name != 'CONNECT SQL' -- Exclude implicit CONNECT SQL
);
GO

---------------------------------------------------------------------------
-- sec.spCompareServerLogins
--
-- Compares the desired server login definitions in sec.ServerLogin with
-- the actual logins on the current SQL Server instance (via
-- sec.ifGetActualServerLogins) and produces a detailed mismatch report
-- with ready-to-run T-SQL remediation commands.
--
-- Three categories of mismatches are detected:
--   MISSING_ON_SERVER   – Login defined in sec.ServerLogin but does not
--                        exist on the server. Proposed SQL: CREATE LOGIN
--                        with the correct type (SQL/WINDOWS/EXTERNAL),
--                        password handling (hashed, plain, or placeholder),
--                        and options (CHECK_POLICY, CHECK_EXPIRATION,
--                        DEFAULT_DATABASE).
--   EXTRA_ON_SERVER     – Login exists on the server but has no matching
--                        row in sec.ServerLogin. Proposed SQL is a
--                        commented-out DROP LOGIN for manual review.
--   PROPERTY_MISMATCH   – Login exists in both places but properties
--                        differ (Disabled status, DefaultDatabase).
--                        Proposed SQL: ALTER LOGIN to reconcile.
--
-- Output:
--   1. Three result sets (one per mismatch type) with columns:
--      HostName, MismatchType, LoginName, Detail, ProposedSQL.
--   2. A consolidated remediation script printed to the messages tab.
--
-- Parameters:
--   @HostName   – Server name to compare against (default @@SERVERNAME).
--   @ReportOnly – Reserved for future use; currently always report-only.
--
-- Called by: sec.spAuditPermissions (Section 1).
--
-- Change history:
-- 2026-01-26 JJJ Initial version.
---------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE sec.spCompareServerLogins
    @HostName sysname = NULL,
    @ReportOnly bit = 1
AS
BEGIN
    SET NOCOUNT ON;

    -- Default hostname to current server
    SET @HostName = ISNULL(@HostName, @@SERVERNAME);

    PRINT N'-- ============================================================';
    PRINT N'-- Server Login Comparison Report';
    PRINT N'-- Host: ' + @HostName;
    PRINT N'-- Generated: ' + CONVERT(nvarchar(30), GETDATE(), 126);
    PRINT N'-- ============================================================';
    PRINT N'';

    -- Temp table for results
    CREATE TABLE #LoginMismatches (
        MismatchType varchar(32),
        LoginName sysname,
        Detail nvarchar(max),
        ProposedSQL nvarchar(max)
    );

    -- Find logins in sec table but not on server (need to CREATE)
    INSERT INTO #LoginMismatches (MismatchType, LoginName, Detail, ProposedSQL)
    SELECT 
        'MISSING_ON_SERVER',
        s.LoginName,
        N'Login exists in sec.ServerLogin but not on server',
        CASE s.LoginType
            WHEN 'SQL' THEN 
                N'CREATE LOGIN ' + QUOTENAME(s.LoginName) + N' WITH PASSWORD = ' + 
                CASE 
                    WHEN s.PasswordMode = 'HASHED' AND s.PasswordHash IS NOT NULL 
                    THEN s.PasswordHash + N' HASHED'
                    WHEN s.PasswordPlain IS NOT NULL 
                    THEN N'N''' + REPLACE(s.PasswordPlain, '''', '''''') + N''''
                    ELSE N'''CHANGEME!123'''
                END +
                CASE WHEN s.CheckPolicy IS NOT NULL THEN N', CHECK_POLICY = ' + CASE s.CheckPolicy WHEN 1 THEN 'ON' ELSE 'OFF' END ELSE '' END +
                CASE WHEN s.CheckExpiration IS NOT NULL THEN N', CHECK_EXPIRATION = ' + CASE s.CheckExpiration WHEN 1 THEN 'ON' ELSE 'OFF' END ELSE '' END +
                CASE WHEN s.DefaultDatabase IS NOT NULL THEN N', DEFAULT_DATABASE = ' + QUOTENAME(s.DefaultDatabase) ELSE '' END +
                N';'
            WHEN 'WINDOWS' THEN 
                N'CREATE LOGIN ' + QUOTENAME(s.LoginName) + N' FROM WINDOWS' +
                CASE WHEN s.DefaultDatabase IS NOT NULL THEN N' WITH DEFAULT_DATABASE = ' + QUOTENAME(s.DefaultDatabase) ELSE '' END +
                N';'
            WHEN 'EXTERNAL' THEN 
                N'CREATE LOGIN ' + QUOTENAME(s.LoginName) + N' FROM EXTERNAL PROVIDER;'
            ELSE N'-- Unknown login type: ' + s.LoginType
        END
    FROM sec.ServerLogin s
    WHERE s.HostName = @HostName
      AND NOT EXISTS (
          SELECT 1 FROM sec.ifGetActualServerLogins(@HostName) a WHERE a.LoginName = s.LoginName
      );

    -- Find logins on server but not in sec table (may need to DROP or add to sec)
    INSERT INTO #LoginMismatches (MismatchType, LoginName, Detail, ProposedSQL)
    SELECT 
        'EXTRA_ON_SERVER',
        a.LoginName,
        N'Login exists on server but not in sec.ServerLogin (consider adding to sec table or dropping)',
        N'-- DROP LOGIN ' + QUOTENAME(a.LoginName) + N'; -- CAUTION: Review before dropping!'
    FROM sec.ifGetActualServerLogins(@HostName) a
    WHERE NOT EXISTS (
        SELECT 1 FROM sec.ServerLogin s WHERE s.HostName = @HostName AND s.LoginName = a.LoginName
    );

    -- Find logins with property mismatches
    INSERT INTO #LoginMismatches (MismatchType, LoginName, Detail, ProposedSQL)
    SELECT 
        'PROPERTY_MISMATCH',
        s.LoginName,
        N'Properties differ - Sec: Disabled=' + CAST(s.Disabled AS varchar) + 
        N', DefaultDB=' + ISNULL(s.DefaultDatabase, 'NULL') +
        N' | Actual: Disabled=' + CAST(a.Disabled AS varchar) +
        N', DefaultDB=' + ISNULL(a.DefaultDatabase, 'NULL'),
        CASE 
            WHEN s.Disabled != a.Disabled THEN
                CASE WHEN s.Disabled = 1 THEN N'ALTER LOGIN ' + QUOTENAME(s.LoginName) + N' DISABLE;'
                ELSE N'ALTER LOGIN ' + QUOTENAME(s.LoginName) + N' ENABLE;'
                END
            ELSE N''
        END +
        CASE 
            WHEN ISNULL(s.DefaultDatabase, '') != ISNULL(a.DefaultDatabase, '') AND s.DefaultDatabase IS NOT NULL THEN
                CHAR(13) + N'ALTER LOGIN ' + QUOTENAME(s.LoginName) + N' WITH DEFAULT_DATABASE = ' + QUOTENAME(s.DefaultDatabase) + N';'
            ELSE N''
        END
    FROM sec.ServerLogin s
    JOIN sec.ifGetActualServerLogins(@HostName) a ON s.LoginName = a.LoginName
    WHERE s.HostName = @HostName
      AND (s.Disabled != a.Disabled 
           OR ISNULL(s.DefaultDatabase, '') != ISNULL(a.DefaultDatabase, ''));

    -- Report results
    PRINT N'-- MISSING ON SERVER (need CREATE):';
    SELECT @HostName AS HostName, MismatchType, LoginName, Detail, ProposedSQL 
    FROM #LoginMismatches WHERE MismatchType = 'MISSING_ON_SERVER';

    PRINT N'';
    PRINT N'-- EXTRA ON SERVER (consider DROP or add to sec):';
    SELECT @HostName AS HostName, MismatchType, LoginName, Detail, ProposedSQL 
    FROM #LoginMismatches WHERE MismatchType = 'EXTRA_ON_SERVER';

    PRINT N'';
    PRINT N'-- PROPERTY MISMATCHES (need ALTER):';
    SELECT @HostName AS HostName, MismatchType, LoginName, Detail, ProposedSQL 
    FROM #LoginMismatches WHERE MismatchType = 'PROPERTY_MISMATCH';

    -- Generate consolidated script
    PRINT N'';
    PRINT N'-- ============================================================';
    PRINT N'-- CONSOLIDATED REMEDIATION SCRIPT';
    PRINT N'-- ============================================================';
    
    DECLARE @sql nvarchar(max);
    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT ProposedSQL FROM #LoginMismatches WHERE ProposedSQL IS NOT NULL AND ProposedSQL != '' ORDER BY MismatchType;
    
    OPEN cur;
    FETCH NEXT FROM cur INTO @sql;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        PRINT @sql;
        FETCH NEXT FROM cur INTO @sql;
    END
    CLOSE cur;
    DEALLOCATE cur;

    DROP TABLE #LoginMismatches;
END;
GO

---------------------------------------------------------------------------
-- sec.spCompareServerRoleMemberships
--
-- Compares the desired server role memberships from
-- sec.vwServerRoleMemberships (derived from sec.ServerSecurityAssignment
-- where AssignmentType = 'SERVER_ROLE') with the actual memberships on
-- the instance (via sec.ifGetActualServerRoleMemberships).
--
-- Two categories of mismatches are detected:
--   MISSING_MEMBERSHIP – Membership in sec but not on server.
--                       Proposed SQL: ALTER SERVER ROLE ... ADD MEMBER.
--   EXTRA_MEMBERSHIP   – Membership on server but not in sec.
--                       Proposed SQL: commented-out DROP MEMBER for review.
--
-- Output:
--   1. Two result sets (missing / extra) with columns:
--      HostName, MismatchType, LoginName, ServerRoleName, Detail,
--      ProposedSQL.
--   2. A consolidated remediation script printed to the messages tab.
--
-- Parameters:
--   @HostName   – Server name to compare against (default @@SERVERNAME).
--   @ReportOnly – Reserved for future use; currently always report-only.
--
-- Called by: sec.spAuditPermissions (Section 2).
--
-- Change history:
-- 2026-01-26 JJJ Initial version.
---------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE sec.spCompareServerRoleMemberships
    @HostName sysname = NULL,
    @ReportOnly bit = 1
AS
BEGIN
    SET NOCOUNT ON;

    SET @HostName = ISNULL(@HostName, @@SERVERNAME);

    PRINT N'-- ============================================================';
    PRINT N'-- Server Role Membership Comparison Report';
    PRINT N'-- Host: ' + @HostName;
    PRINT N'-- Generated: ' + CONVERT(nvarchar(30), GETDATE(), 126);
    PRINT N'-- ============================================================';
    PRINT N'';

    CREATE TABLE #RoleMismatches (
        MismatchType varchar(32),
        LoginName sysname,
        ServerRoleName sysname,
        Detail nvarchar(max),
        ProposedSQL nvarchar(max)
    );

    -- Missing role memberships (in sec but not on server)
    INSERT INTO #RoleMismatches (MismatchType, LoginName, ServerRoleName, Detail, ProposedSQL)
    SELECT 
        'MISSING_MEMBERSHIP',
        s.LoginName,
        s.ServerRoleName,
        N'Role membership in sec table but not on server',
        N'ALTER SERVER ROLE ' + QUOTENAME(s.ServerRoleName) + N' ADD MEMBER ' + QUOTENAME(s.LoginName) + N';'
    FROM sec.vwServerRoleMemberships s
    WHERE s.HostName = @HostName
      AND NOT EXISTS (
          SELECT 1 FROM sec.ifGetActualServerRoleMemberships(@HostName) a 
          WHERE a.LoginName = s.LoginName AND a.ServerRoleName = s.ServerRoleName
      );

    -- Extra role memberships (on server but not in sec)
    INSERT INTO #RoleMismatches (MismatchType, LoginName, ServerRoleName, Detail, ProposedSQL)
    SELECT 
        'EXTRA_MEMBERSHIP',
        a.LoginName,
        a.ServerRoleName,
        N'Role membership on server but not in sec table',
        N'-- ALTER SERVER ROLE ' + QUOTENAME(a.ServerRoleName) + N' DROP MEMBER ' + QUOTENAME(a.LoginName) + N'; -- REVIEW!'
    FROM sec.ifGetActualServerRoleMemberships(@HostName) a
    WHERE NOT EXISTS (
        SELECT 1 FROM sec.vwServerRoleMemberships s 
        WHERE s.HostName = @HostName AND s.LoginName = a.LoginName AND s.ServerRoleName = a.ServerRoleName
    );

    -- Report
    PRINT N'-- MISSING MEMBERSHIPS (need ADD):';
    SELECT @HostName AS HostName, * FROM #RoleMismatches WHERE MismatchType = 'MISSING_MEMBERSHIP';

    PRINT N'';
    PRINT N'-- EXTRA MEMBERSHIPS (consider DROP):';
    SELECT @HostName AS HostName, * FROM #RoleMismatches WHERE MismatchType = 'EXTRA_MEMBERSHIP';

    PRINT N'';
    PRINT N'-- CONSOLIDATED REMEDIATION SCRIPT';
    DECLARE @sql nvarchar(max);
    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT ProposedSQL FROM #RoleMismatches WHERE ProposedSQL IS NOT NULL ORDER BY MismatchType;
    OPEN cur;
    FETCH NEXT FROM cur INTO @sql;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        PRINT @sql;
        FETCH NEXT FROM cur INTO @sql;
    END
    CLOSE cur;
    DEALLOCATE cur;

    DROP TABLE #RoleMismatches;
END;
GO

---------------------------------------------------------------------------
-- sec.spCompareServerPermissions
--
-- Compares the desired server-level permissions from
-- sec.vwServerPermissions (derived from sec.ServerSecurityAssignment
-- where AssignmentType = 'PERMISSION') with the actual permissions on
-- the instance (via sec.ifGetActualServerPermissions).
--
-- Two categories of mismatches are detected:
--   MISSING_PERMISSION – Permission in sec but not granted on server.
--                       Proposed SQL: GRANT/DENY ... TO login.
--                       Handles SERVER, ENDPOINT, and AVAILABILITY_GROUP
--                       scopes, including WITH GRANT OPTION.
--   EXTRA_PERMISSION   – Permission on server but not in sec.
--                       Proposed SQL: commented-out REVOKE for review.
--
-- Collation-safe comparison (COLLATE DATABASE_DEFAULT) is used when
-- matching login and permission names across databases.
--
-- Output:
--   1. Two result sets (missing / extra) with columns:
--      HostName, MismatchType, LoginName, PermissionName, Scope,
--      Detail, ProposedSQL.
--   2. A consolidated remediation script printed to the messages tab.
--
-- Parameters:
--   @HostName   – Server name to compare against (default @@SERVERNAME).
--   @ReportOnly – Reserved for future use; currently always report-only.
--
-- Called by: sec.spAuditPermissions (Section 3).
--
-- Change history:
-- 2026-01-26 JJJ Initial version.
---------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE sec.spCompareServerPermissions
    @HostName sysname = NULL,
    @ReportOnly bit = 1
AS
BEGIN
    SET NOCOUNT ON;

    SET @HostName = ISNULL(@HostName, @@SERVERNAME);

    PRINT N'-- ============================================================';
    PRINT N'-- Server Permissions Comparison Report';
    PRINT N'-- Host: ' + @HostName;
    PRINT N'-- Generated: ' + CONVERT(nvarchar(30), GETDATE(), 126);
    PRINT N'-- ============================================================';
    PRINT N'';

    CREATE TABLE #PermMismatches (
        MismatchType varchar(32),
        LoginName sysname,
        PermissionName varchar(128),
        Scope varchar(32),
        Detail nvarchar(max),
        ProposedSQL nvarchar(max)
    );

    -- Missing permissions (in sec but not on server)
    INSERT INTO #PermMismatches (MismatchType, LoginName, PermissionName, Scope, Detail, ProposedSQL)
    SELECT 
        'MISSING_PERMISSION',
        s.LoginName,
        s.PermissionName,
        s.Scope,
        N'Permission in sec table but not on server',
        s.State + N' ' + s.PermissionName + 
        CASE s.Scope
            WHEN 'ENDPOINT' THEN N' ON ENDPOINT::' + QUOTENAME(s.EndpointName)
            WHEN 'AVAILABILITY_GROUP' THEN N' ON AVAILABILITY GROUP::' + QUOTENAME(s.AGName)
            ELSE N''
        END +
        N' TO ' + QUOTENAME(s.LoginName) +
        CASE WHEN s.WithGrantOption = 1 THEN N' WITH GRANT OPTION' ELSE N'' END + N';'
    FROM sec.vwServerPermissions s
    WHERE s.HostName = @HostName
      AND NOT EXISTS (
          SELECT 1 FROM sec.ifGetActualServerPermissions(@HostName) a 
          WHERE a.LoginName = s.LoginName COLLATE DATABASE_DEFAULT
            AND a.PermissionName = s.PermissionName COLLATE DATABASE_DEFAULT
            AND ISNULL(a.Scope, 'SERVER') = ISNULL(s.Scope, 'SERVER') COLLATE DATABASE_DEFAULT
      );

    -- Extra permissions (on server but not in sec)
    INSERT INTO #PermMismatches (MismatchType, LoginName, PermissionName, Scope, Detail, ProposedSQL)
    SELECT 
        'EXTRA_PERMISSION',
        a.LoginName,
        a.PermissionName,
        a.Scope,
        N'Permission on server but not in sec table',
        N'-- REVOKE ' + a.PermissionName + 
        CASE a.Scope
            WHEN 'ENDPOINT' THEN N' ON ENDPOINT::' + QUOTENAME(a.EndpointName)
            WHEN 'AVAILABILITY_GROUP' THEN N' ON AVAILABILITY GROUP::' + QUOTENAME(a.AGName)
            ELSE N''
        END +
        N' FROM ' + QUOTENAME(a.LoginName) + N'; -- REVIEW!'
    FROM sec.ifGetActualServerPermissions(@HostName) a
    WHERE NOT EXISTS (
        SELECT 1 FROM sec.vwServerPermissions s 
        WHERE s.HostName = @HostName 
          AND s.LoginName COLLATE DATABASE_DEFAULT = a.LoginName
          AND s.PermissionName COLLATE DATABASE_DEFAULT = a.PermissionName
    );

    -- Report
    PRINT N'-- MISSING PERMISSIONS (need GRANT/DENY):';
    SELECT @HostName AS HostName, * FROM #PermMismatches WHERE MismatchType = 'MISSING_PERMISSION';

    PRINT N'';
    PRINT N'-- EXTRA PERMISSIONS (consider REVOKE):';
    SELECT @HostName AS HostName, * FROM #PermMismatches WHERE MismatchType = 'EXTRA_PERMISSION';

    PRINT N'';
    PRINT N'-- CONSOLIDATED REMEDIATION SCRIPT';
    DECLARE @sql nvarchar(max);
    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT ProposedSQL FROM #PermMismatches WHERE ProposedSQL IS NOT NULL ORDER BY MismatchType;
    OPEN cur;
    FETCH NEXT FROM cur INTO @sql;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        PRINT @sql;
        FETCH NEXT FROM cur INTO @sql;
    END
    CLOSE cur;
    DEALLOCATE cur;

    DROP TABLE #PermMismatches;
END;
GO

---------------------------------------------------------------------------
-- sec.spCompareDbPrincipals
--
-- Compares the desired database-level users and roles defined in
-- sec.DbPrincipal with the actual principals in each target database.
--
-- Scope:
--   If @DbName is supplied, only that database is checked. Otherwise
--   every database listed in sec.DbPrincipal for the given host is
--   inspected.
--
-- How it works:
--   For each database, dynamic SQL is executed via
--   <db>.sys.sp_executesql so that sys.database_principals resolves in
--   the correct database context. Four checks are performed:
--
--   MISSING_USER  – User in sec.DbPrincipal (PrincipalType = 'USER') but
--                  not in the database. Proposed SQL: CREATE USER with
--                  the appropriate auth type (LOGIN / WITHOUT_LOGIN /
--                  EXTERNAL).
--   MISSING_ROLE  – Role in sec.DbPrincipal (PrincipalType = 'ROLE') but
--                  not in the database. Proposed SQL: CREATE ROLE.
--   EXTRA_USER    – User in the database but not in sec.DbPrincipal.
--                  Built-in principals (dbo, guest, INFORMATION_SCHEMA,
--                  sys, ##...##) are excluded. Proposed SQL: commented-
--                  out DROP USER for review.
--   EXTRA_ROLE    – Custom (non-fixed) role in the database but not in
--                  sec.DbPrincipal. The public role is excluded.
--                  Proposed SQL: commented-out DROP ROLE for review.
--
--   If a database listed in sec.DbPrincipal does not exist on the
--   server, a DATABASE_NOT_FOUND row is reported.
--
-- Output:
--   Five result sets (one per mismatch type + databases not found),
--   followed by a consolidated remediation script on the messages tab.
--
-- Parameters:
--   @HostName   – Server name (default @@SERVERNAME).
--   @DbName     – Optional single database to check; NULL = all.
--   @ReportOnly – Reserved for future use.
--
-- Called by: sec.spAuditPermissions (Section 4).
--
-- Change history:
-- 2026-01-26 JJJ Initial version.
---------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE sec.spCompareDbPrincipals
    @HostName sysname = NULL,
    @DbName sysname = NULL,
    @ReportOnly bit = 1
AS
BEGIN
    SET NOCOUNT ON;

    SET @HostName = ISNULL(@HostName, @@SERVERNAME);

    PRINT N'-- ============================================================';
    PRINT N'-- Database Principals Comparison Report';
    PRINT N'-- Host: ' + @HostName;
    PRINT N'-- Database: ' + ISNULL(@DbName, 'ALL');
    PRINT N'-- Generated: ' + CONVERT(nvarchar(30), GETDATE(), 126);
    PRINT N'-- ============================================================';
    PRINT N'';

    CREATE TABLE #DbPrincipalMismatches (
        MismatchType varchar(32),
        DbName sysname,
        PrincipalName sysname,
        PrincipalType varchar(8),
        Detail nvarchar(max),
        ProposedSQL nvarchar(max)
    );

    -- Build dynamic SQL to check each database
    DECLARE @sql nvarchar(max);
    DECLARE @db sysname;
    DECLARE @execsql nvarchar(256);

    DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT DISTINCT DbName 
        FROM sec.DbPrincipal 
        WHERE HostName = @HostName
          AND (@DbName IS NULL OR DbName = @DbName);

    OPEN db_cursor;
    FETCH NEXT FROM db_cursor INTO @db;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Check if database exists
        IF DB_ID(@db) IS NOT NULL
        BEGIN
            SET @execsql = QUOTENAME(@db) + N'.sys.sp_executesql';

            -- Check for missing users
            SET @sql = N'
            SELECT 
                ''MISSING_USER'',
                @DbNameParam,
                s.PrincipalName,
                s.PrincipalType,
                N''User in sec.DbPrincipal but not in database'',
                CASE s.AuthType
                    WHEN ''LOGIN'' THEN N''USE '' + QUOTENAME(@DbNameParam) + N''; CREATE USER '' + QUOTENAME(s.PrincipalName) + N'' FOR LOGIN '' + QUOTENAME(s.LoginName) + N'' WITH DEFAULT_SCHEMA = '' + QUOTENAME(ISNULL(s.DefaultSchema, ''dbo'')) + N'';''
                    WHEN ''WITHOUT_LOGIN'' THEN N''USE '' + QUOTENAME(@DbNameParam) + N''; CREATE USER '' + QUOTENAME(s.PrincipalName) + N'' WITHOUT LOGIN WITH DEFAULT_SCHEMA = '' + QUOTENAME(ISNULL(s.DefaultSchema, ''dbo'')) + N'';''
                    WHEN ''EXTERNAL'' THEN N''USE '' + QUOTENAME(@DbNameParam) + N''; CREATE USER '' + QUOTENAME(s.PrincipalName) + N'' FROM EXTERNAL PROVIDER;''
                    ELSE N''-- Unknown auth type for user '' + QUOTENAME(s.PrincipalName)
                END
            FROM _meta.sec.DbPrincipal s
            WHERE s.HostName = @HostNameParam
              AND s.DbName = @DbNameParam
              AND s.PrincipalType = ''USER''
              AND NOT EXISTS (
                  SELECT 1 FROM sys.database_principals dp 
                  WHERE dp.name = s.PrincipalName AND dp.type IN (''S'',''U'',''G'',''E'',''X'')
              )';

            INSERT INTO #DbPrincipalMismatches
            EXEC @execsql @sql, 
                N'@HostNameParam sysname, @DbNameParam sysname', 
                @HostNameParam = @HostName, @DbNameParam = @db;

            -- Check for missing roles
            SET @sql = N'
            SELECT 
                ''MISSING_ROLE'',
                @DbNameParam,
                s.PrincipalName,
                s.PrincipalType,
                N''Role in sec.DbPrincipal but not in database'',
                N''USE '' + QUOTENAME(@DbNameParam) + N''; CREATE ROLE '' + QUOTENAME(s.PrincipalName) + N'';''
            FROM _meta.sec.DbPrincipal s
            WHERE s.HostName = @HostNameParam
              AND s.DbName = @DbNameParam
              AND s.PrincipalType = ''ROLE''
              AND NOT EXISTS (
                  SELECT 1 FROM sys.database_principals dp 
                  WHERE dp.name = s.PrincipalName AND dp.type = ''R''
              )';

            INSERT INTO #DbPrincipalMismatches
            EXEC @execsql @sql, 
                N'@HostNameParam sysname, @DbNameParam sysname', 
                @HostNameParam = @HostName, @DbNameParam = @db;

            -- Check for extra users (in DB but not in sec)
            SET @sql = N'
            SELECT 
                ''EXTRA_USER'',
                @DbNameParam,
                dp.name,
                ''USER'',
                N''User in database but not in sec.DbPrincipal'',
                N''-- USE '' + QUOTENAME(@DbNameParam) + N''; DROP USER '' + QUOTENAME(dp.name) + N''; -- REVIEW!''
            FROM sys.database_principals dp
            WHERE dp.type IN (''S'',''U'',''G'',''E'',''X'')
              AND dp.name NOT IN (''dbo'', ''guest'', ''INFORMATION_SCHEMA'', ''sys'')
              AND dp.name NOT LIKE ''##%##''
              AND NOT EXISTS (
                  SELECT 1 FROM _meta.sec.DbPrincipal s 
                  WHERE s.HostName = @HostNameParam 
                    AND s.DbName = @DbNameParam 
                    AND s.PrincipalName = dp.name
                    AND s.PrincipalType = ''USER''
              )';

            INSERT INTO #DbPrincipalMismatches
            EXEC @execsql @sql, 
                N'@HostNameParam sysname, @DbNameParam sysname', 
                @HostNameParam = @HostName, @DbNameParam = @db;

            -- Check for extra roles (in DB but not in sec)
            SET @sql = N'
            SELECT 
                ''EXTRA_ROLE'',
                @DbNameParam,
                dp.name,
                ''ROLE'',
                N''Role in database but not in sec.DbPrincipal'',
                N''-- USE '' + QUOTENAME(@DbNameParam) + N''; DROP ROLE '' + QUOTENAME(dp.name) + N''; -- REVIEW!''
            FROM sys.database_principals dp
            WHERE dp.type = ''R''
              AND dp.is_fixed_role = 0
              AND dp.name NOT IN (''public'')
              AND NOT EXISTS (
                  SELECT 1 FROM _meta.sec.DbPrincipal s 
                  WHERE s.HostName = @HostNameParam 
                    AND s.DbName = @DbNameParam 
                    AND s.PrincipalName = dp.name
                    AND s.PrincipalType = ''ROLE''
              )';

            INSERT INTO #DbPrincipalMismatches
            EXEC @execsql @sql, 
                N'@HostNameParam sysname, @DbNameParam sysname', 
                @HostNameParam = @HostName, @DbNameParam = @db;
        END
        ELSE
        BEGIN
            INSERT INTO #DbPrincipalMismatches (MismatchType, DbName, PrincipalName, PrincipalType, Detail, ProposedSQL)
            VALUES ('DATABASE_NOT_FOUND', @db, N'N/A', N'N/A', N'Database does not exist on this server', N'-- Database ' + QUOTENAME(@db) + N' not found');
        END

        FETCH NEXT FROM db_cursor INTO @db;
    END

    CLOSE db_cursor;
    DEALLOCATE db_cursor;

    -- Report
    PRINT N'-- MISSING USERS:';
    SELECT @HostName AS HostName, * FROM #DbPrincipalMismatches WHERE MismatchType = 'MISSING_USER' ORDER BY DbName;

    PRINT N'';
    PRINT N'-- MISSING ROLES:';
    SELECT @HostName AS HostName, * FROM #DbPrincipalMismatches WHERE MismatchType = 'MISSING_ROLE' ORDER BY DbName;

    PRINT N'';
    PRINT N'-- EXTRA USERS (consider DROP or add to sec):';
    SELECT @HostName AS HostName, * FROM #DbPrincipalMismatches WHERE MismatchType = 'EXTRA_USER' ORDER BY DbName;

    PRINT N'';
    PRINT N'-- EXTRA ROLES (consider DROP or add to sec):';
    SELECT @HostName AS HostName, * FROM #DbPrincipalMismatches WHERE MismatchType = 'EXTRA_ROLE' ORDER BY DbName;

    PRINT N'';
    PRINT N'-- DATABASES NOT FOUND:';
    SELECT @HostName AS HostName, * FROM #DbPrincipalMismatches WHERE MismatchType = 'DATABASE_NOT_FOUND';

    PRINT N'';
    PRINT N'-- CONSOLIDATED REMEDIATION SCRIPT';
    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT ProposedSQL FROM #DbPrincipalMismatches WHERE ProposedSQL IS NOT NULL ORDER BY MismatchType, DbName;
    OPEN cur;
    FETCH NEXT FROM cur INTO @sql;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        PRINT @sql;
        FETCH NEXT FROM cur INTO @sql;
    END
    CLOSE cur;
    DEALLOCATE cur;

    DROP TABLE #DbPrincipalMismatches;
END;
GO

---------------------------------------------------------------------------
-- sec.spCompareDbRoleMemberships
--
-- Compares desired database role memberships from
-- sec.DbSecurityAssignment (AssignmentType = 'ROLE_MEMBERSHIP') with
-- the actual memberships in each target database.
--
-- Scope:
--   If @DbName is supplied, only that database is checked. Otherwise
--   every database that has ROLE_MEMBERSHIP rows in
--   sec.DbSecurityAssignment for the given host is inspected.
--
-- How it works:
--   For each database, dynamic SQL is executed via
--   <db>.sys.sp_executesql, joining sys.database_role_members with
--   sys.database_principals to resolve role and member names.
--
--   MISSING_MEMBERSHIP – Membership in sec but not in the database.
--                       Proposed SQL: ALTER ROLE ... ADD MEMBER.
--   EXTRA_MEMBERSHIP   – Membership in the database but not in sec.
--                       The dbo user is excluded. Proposed SQL:
--                       commented-out ALTER ROLE ... DROP MEMBER.
--
-- Output:
--   Two result sets (missing / extra) followed by a consolidated
--   remediation script on the messages tab.
--
-- Parameters:
--   @HostName   – Server name (default @@SERVERNAME).
--   @DbName     – Optional single database; NULL = all.
--   @ReportOnly – Reserved for future use.
--
-- Called by: sec.spAuditPermissions (Section 5).
--
-- Change history:
-- 2026-01-26 JJJ Initial version.
---------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE sec.spCompareDbRoleMemberships
    @HostName sysname = NULL,
    @DbName sysname = NULL,
    @ReportOnly bit = 1
AS
BEGIN
    SET NOCOUNT ON;

    SET @HostName = ISNULL(@HostName, @@SERVERNAME);

    PRINT N'-- ============================================================';
    PRINT N'-- Database Role Membership Comparison Report';
    PRINT N'-- Host: ' + @HostName;
    PRINT N'-- Database: ' + ISNULL(@DbName, 'ALL');
    PRINT N'-- Generated: ' + CONVERT(nvarchar(30), GETDATE(), 126);
    PRINT N'-- ============================================================';
    PRINT N'';

    CREATE TABLE #MembershipMismatches (
        MismatchType varchar(32),
        DbName sysname,
        RoleName sysname,
        UserName sysname,
        Detail nvarchar(max),
        ProposedSQL nvarchar(max)
    );

    DECLARE @sql nvarchar(max);
    DECLARE @db sysname;
    DECLARE @execsql nvarchar(256);

    DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT DISTINCT DbName 
        FROM sec.DbSecurityAssignment 
        WHERE HostName = @HostName
          AND AssignmentType = 'ROLE_MEMBERSHIP'
          AND (@DbName IS NULL OR DbName = @DbName);

    OPEN db_cursor;
    FETCH NEXT FROM db_cursor INTO @db;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF DB_ID(@db) IS NOT NULL
        BEGIN
            SET @execsql = QUOTENAME(@db) + N'.sys.sp_executesql';

            -- Missing memberships
            SET @sql = N'
            SELECT 
                ''MISSING_MEMBERSHIP'',
                @DbNameParam,
                s.RoleName,
                s.UserName,
                N''Membership in sec table but not in database'',
                N''USE '' + QUOTENAME(@DbNameParam) + N''; ALTER ROLE '' + QUOTENAME(s.RoleName) + N'' ADD MEMBER '' + QUOTENAME(s.UserName) + N'';''
            FROM _meta.sec.DbSecurityAssignment s
            WHERE s.HostName = @HostNameParam
              AND s.DbName = @DbNameParam
              AND s.AssignmentType = ''ROLE_MEMBERSHIP''
              AND NOT EXISTS (
                  SELECT 1 
                  FROM sys.database_role_members rm
                  JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
                  JOIN sys.database_principals m ON rm.member_principal_id = m.principal_id
                  WHERE r.name = s.RoleName AND m.name = s.UserName
              )';

            INSERT INTO #MembershipMismatches
            EXEC @execsql @sql, 
                N'@HostNameParam sysname, @DbNameParam sysname', 
                @HostNameParam = @HostName, @DbNameParam = @db;

            -- Extra memberships
            SET @sql = N'
            SELECT 
                ''EXTRA_MEMBERSHIP'',
                @DbNameParam,
                r.name,
                m.name,
                N''Membership in database but not in sec table'',
                N''-- USE '' + QUOTENAME(@DbNameParam) + N''; ALTER ROLE '' + QUOTENAME(r.name) + N'' DROP MEMBER '' + QUOTENAME(m.name) + N''; -- REVIEW!''
            FROM sys.database_role_members rm
            JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
            JOIN sys.database_principals m ON rm.member_principal_id = m.principal_id
            WHERE r.type = ''R''
              AND m.name NOT IN (''dbo'')
              AND NOT EXISTS (
                  SELECT 1 FROM _meta.sec.DbSecurityAssignment s 
                  WHERE s.HostName = @HostNameParam 
                    AND s.DbName = @DbNameParam 
                    AND s.AssignmentType = ''ROLE_MEMBERSHIP''
                    AND s.RoleName = r.name
                    AND s.UserName = m.name
              )';

            INSERT INTO #MembershipMismatches
            EXEC @execsql @sql, 
                N'@HostNameParam sysname, @DbNameParam sysname', 
                @HostNameParam = @HostName, @DbNameParam = @db;
        END

        FETCH NEXT FROM db_cursor INTO @db;
    END

    CLOSE db_cursor;
    DEALLOCATE db_cursor;

    -- Report
    PRINT N'-- MISSING MEMBERSHIPS:';
    SELECT @HostName AS HostName, * FROM #MembershipMismatches WHERE MismatchType = 'MISSING_MEMBERSHIP' ORDER BY DbName;

    PRINT N'';
    PRINT N'-- EXTRA MEMBERSHIPS (consider DROP):';
    SELECT @HostName AS HostName, * FROM #MembershipMismatches WHERE MismatchType = 'EXTRA_MEMBERSHIP' ORDER BY DbName;

    PRINT N'';
    PRINT N'-- CONSOLIDATED REMEDIATION SCRIPT';
    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT ProposedSQL FROM #MembershipMismatches WHERE ProposedSQL IS NOT NULL ORDER BY MismatchType, DbName;
    OPEN cur;
    FETCH NEXT FROM cur INTO @sql;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        PRINT @sql;
        FETCH NEXT FROM cur INTO @sql;
    END
    CLOSE cur;
    DEALLOCATE cur;

    DROP TABLE #MembershipMismatches;
END;
GO

---------------------------------------------------------------------------
-- sec.spCompareDbPermissions
--
-- Compares desired database-level permissions from
-- sec.DbSecurityAssignment (AssignmentType = 'ROLE_PERMISSION') with
-- the actual permissions in each target database.
--
-- Scope:
--   If @DbName is supplied, only that database is checked. Otherwise
--   every database that has ROLE_PERMISSION rows in
--   sec.DbSecurityAssignment for the given host is inspected.
--
-- How it works:
--   For each database, dynamic SQL is executed via
--   <db>.sys.sp_executesql, joining sys.database_permissions with
--   sys.database_principals, sys.schemas, and sys.objects to match
--   permissions at three scope levels:
--
--     DATABASE (class 0) – Database-wide permissions (e.g. CONNECT,
--                         CREATE TABLE).
--     SCHEMA   (class 3) – Schema-scoped permissions (e.g. SELECT ON
--                         SCHEMA::dbo).
--     OBJECT   (class 1) – Object-scoped permissions (e.g. EXECUTE ON
--                         dbo.spFoo).
--
--   MISSING_PERMISSION – Permission in sec but not in the database.
--                       Proposed SQL: GRANT/DENY with correct scope,
--                       including WITH GRANT OPTION if applicable.
--   EXTRA_PERMISSION   – Permission in the database but not in sec.
--                       Only non-fixed, non-public custom roles are
--                       inspected. Proposed SQL: commented-out REVOKE.
--
-- Output:
--   Two result sets (missing / extra) followed by a consolidated
--   remediation script on the messages tab.
--
-- Parameters:
--   @HostName   – Server name (default @@SERVERNAME).
--   @DbName     – Optional single database; NULL = all.
--   @ReportOnly – Reserved for future use.
--
-- Called by: sec.spAuditPermissions (Section 6).
--
-- Change history:
-- 2026-01-26 JJJ Initial version.
---------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE sec.spCompareDbPermissions
    @HostName sysname = NULL,
    @DbName sysname = NULL,
    @ReportOnly bit = 1
AS
BEGIN
    SET NOCOUNT ON;

    SET @HostName = ISNULL(@HostName, @@SERVERNAME);

    PRINT N'-- ============================================================';
    PRINT N'-- Database Permissions Comparison Report';
    PRINT N'-- Host: ' + @HostName;
    PRINT N'-- Database: ' + ISNULL(@DbName, 'ALL');
    PRINT N'-- Generated: ' + CONVERT(nvarchar(30), GETDATE(), 126);
    PRINT N'-- ============================================================';
    PRINT N'';

    CREATE TABLE #PermMismatches (
        MismatchType varchar(32),
        DbName sysname,
        RoleName sysname,
        PermissionName varchar(128),
        Scope varchar(16),
        SchemaName sysname NULL,
        ObjectName sysname NULL,
        Detail nvarchar(max),
        ProposedSQL nvarchar(max)
    );

    DECLARE @sql nvarchar(max);
    DECLARE @db sysname;
    DECLARE @execsql nvarchar(256);

    DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT DISTINCT DbName 
        FROM sec.DbSecurityAssignment 
        WHERE HostName = @HostName
          AND AssignmentType = 'ROLE_PERMISSION'
          AND (@DbName IS NULL OR DbName = @DbName);

    OPEN db_cursor;
    FETCH NEXT FROM db_cursor INTO @db;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF DB_ID(@db) IS NOT NULL
        BEGIN
            SET @execsql = QUOTENAME(@db) + N'.sys.sp_executesql';

            -- Missing permissions
            SET @sql = N'
            SELECT 
                ''MISSING_PERMISSION'',
                @DbNameParam,
                s.RoleName,
                s.PermissionName,
                s.Scope,
                s.SchemaName,
                s.ObjectName,
                N''Permission in sec table but not in database'',
                N''USE '' + QUOTENAME(@DbNameParam) + N''; '' + s.State + N'' '' + s.PermissionName +
                CASE s.Scope
                    WHEN ''SCHEMA'' THEN N'' ON SCHEMA::'' + QUOTENAME(s.SchemaName)
                    WHEN ''OBJECT'' THEN N'' ON '' + QUOTENAME(s.SchemaName) + N''.'' + QUOTENAME(s.ObjectName)
                    ELSE N''''
                END +
                N'' TO '' + QUOTENAME(s.RoleName) +
                CASE WHEN s.WithGrantOption = 1 THEN N'' WITH GRANT OPTION'' ELSE N'''' END + N'';''
            FROM _meta.sec.DbSecurityAssignment s
            WHERE s.HostName = @HostNameParam
              AND s.DbName = @DbNameParam
              AND s.AssignmentType = ''ROLE_PERMISSION''
              AND NOT EXISTS (
                  SELECT 1 
                  FROM sys.database_permissions pe
                  JOIN sys.database_principals pr ON pe.grantee_principal_id = pr.principal_id
                  LEFT JOIN sys.schemas sch ON pe.class = 3 AND pe.major_id = sch.schema_id
                  LEFT JOIN sys.objects obj ON pe.class = 1 AND pe.major_id = obj.object_id
                  LEFT JOIN sys.schemas obj_sch ON obj.schema_id = obj_sch.schema_id
                  WHERE pr.name = s.RoleName
                    AND pe.permission_name = s.PermissionName
                    AND (
                        (s.Scope = ''DATABASE'' AND pe.class = 0)
                        OR (s.Scope = ''SCHEMA'' AND pe.class = 3 AND sch.name = s.SchemaName)
                        OR (s.Scope = ''OBJECT'' AND pe.class = 1 AND obj.name = s.ObjectName AND obj_sch.name = s.SchemaName)
                    )
              )';

            INSERT INTO #PermMismatches
            EXEC @execsql @sql, 
                N'@HostNameParam sysname, @DbNameParam sysname', 
                @HostNameParam = @HostName, @DbNameParam = @db;

            -- Extra permissions (simplified - only database-level)
            SET @sql = N'
            SELECT 
                ''EXTRA_PERMISSION'',
                @DbNameParam,
                pr.name,
                pe.permission_name,
                CASE pe.class 
                    WHEN 0 THEN ''DATABASE''
                    WHEN 3 THEN ''SCHEMA''
                    WHEN 1 THEN ''OBJECT''
                    ELSE ''OTHER''
                END,
                CASE WHEN pe.class = 3 THEN sch.name WHEN pe.class = 1 THEN obj_sch.name ELSE NULL END,
                CASE WHEN pe.class = 1 THEN obj.name ELSE NULL END,
                N''Permission in database but not in sec table'',
                N''-- USE '' + QUOTENAME(@DbNameParam) + N''; REVOKE '' + pe.permission_name +
                CASE pe.class 
                    WHEN 3 THEN N'' ON SCHEMA::'' + QUOTENAME(sch.name)
                    WHEN 1 THEN N'' ON '' + QUOTENAME(obj_sch.name) + N''.'' + QUOTENAME(obj.name)
                    ELSE N''''
                END +
                N'' FROM '' + QUOTENAME(pr.name) + N''; -- REVIEW!''
            FROM sys.database_permissions pe
            JOIN sys.database_principals pr ON pe.grantee_principal_id = pr.principal_id
            LEFT JOIN sys.schemas sch ON pe.class = 3 AND pe.major_id = sch.schema_id
            LEFT JOIN sys.objects obj ON pe.class = 1 AND pe.major_id = obj.object_id
            LEFT JOIN sys.schemas obj_sch ON obj.schema_id = obj_sch.schema_id
            WHERE pr.type = ''R''
              AND pr.is_fixed_role = 0
              AND pr.name NOT IN (''public'')
              AND pe.class IN (0, 1, 3)
              AND NOT EXISTS (
                  SELECT 1 FROM _meta.sec.DbSecurityAssignment s 
                  WHERE s.HostName = @HostNameParam 
                    AND s.DbName = @DbNameParam 
                    AND s.AssignmentType = ''ROLE_PERMISSION''
                    AND s.RoleName = pr.name
                    AND s.PermissionName = pe.permission_name
              )';

            INSERT INTO #PermMismatches
            EXEC @execsql @sql, 
                N'@HostNameParam sysname, @DbNameParam sysname', 
                @HostNameParam = @HostName, @DbNameParam = @db;
        END

        FETCH NEXT FROM db_cursor INTO @db;
    END

    CLOSE db_cursor;
    DEALLOCATE db_cursor;

    -- Report
    PRINT N'-- MISSING PERMISSIONS:';
    SELECT @HostName AS HostName, * FROM #PermMismatches WHERE MismatchType = 'MISSING_PERMISSION' ORDER BY DbName;

    PRINT N'';
    PRINT N'-- EXTRA PERMISSIONS (consider REVOKE):';
    SELECT @HostName AS HostName, * FROM #PermMismatches WHERE MismatchType = 'EXTRA_PERMISSION' ORDER BY DbName;

    PRINT N'';
    PRINT N'-- CONSOLIDATED REMEDIATION SCRIPT';
    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT ProposedSQL FROM #PermMismatches WHERE ProposedSQL IS NOT NULL ORDER BY MismatchType, DbName;
    OPEN cur;
    FETCH NEXT FROM cur INTO @sql;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        PRINT @sql;
        FETCH NEXT FROM cur INTO @sql;
    END
    CLOSE cur;
    DEALLOCATE cur;

    DROP TABLE #PermMismatches;
END;
GO

---------------------------------------------------------------------------
-- sec.spAuditPermissions
--
-- Master audit procedure that runs all six comparison procedures in
-- sequence and produces a single, comprehensive security comparison
-- report covering every layer of the permission model:
--
--   Section 1 – Server Logins          (sec.spCompareServerLogins)
--   Section 2 – Server Role Memberships (sec.spCompareServerRoleMemberships)
--   Section 3 – Server Permissions      (sec.spCompareServerPermissions)
--   Section 4 – Database Principals     (sec.spCompareDbPrincipals)
--   Section 5 – Database Role Memberships (sec.spCompareDbRoleMemberships)
--   Section 6 – Database Permissions    (sec.spCompareDbPermissions)
--
-- Each section outputs its own result sets and remediation script to the
-- messages tab. The overall report is bracketed by header/footer banners
-- for easy navigation.
--
-- Parameters:
--   @HostName   – Server name to audit (default @@SERVERNAME).
--   @DbName     – Optional: restrict database-level sections to a single
--                 database. NULL = audit all databases listed in sec.*.
--   @ReportOnly – Passed through to sub-procedures (reserved for future
--                 use; currently all comparisons are report-only).
--
-- Usage:
--   EXEC sec.spAuditPermissions;                         -- Full audit
--   EXEC sec.spAuditPermissions @DbName = 'OTR';         -- Single DB
--
-- Change history:
-- 2026-01-26 JJJ Initial version.
---------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE sec.spAuditPermissions
    @HostName sysname = NULL,
    @DbName sysname = NULL,
    @ReportOnly bit = 1
AS
BEGIN
    SET NOCOUNT ON;

    SET @HostName = ISNULL(@HostName, @@SERVERNAME);

    PRINT N'-- ############################################################';
    PRINT N'-- COMPREHENSIVE SECURITY COMPARISON REPORT';
    PRINT N'-- Host: ' + @HostName;
    PRINT N'-- Database: ' + ISNULL(@DbName, 'ALL');
    PRINT N'-- Generated: ' + CONVERT(nvarchar(30), GETDATE(), 126);
    PRINT N'-- ############################################################';
    PRINT N'';

    -- Server-level comparisons
    PRINT N'';
    PRINT N'-- ************************************************************';
    PRINT N'-- SECTION 1: SERVER LOGINS';
    PRINT N'-- ************************************************************';
    EXEC sec.spCompareServerLogins @HostName = @HostName, @ReportOnly = @ReportOnly;

    PRINT N'';
    PRINT N'-- ************************************************************';
    PRINT N'-- SECTION 2: SERVER ROLE MEMBERSHIPS';
    PRINT N'-- ************************************************************';
    EXEC sec.spCompareServerRoleMemberships @HostName = @HostName, @ReportOnly = @ReportOnly;

    PRINT N'';
    PRINT N'-- ************************************************************';
    PRINT N'-- SECTION 3: SERVER PERMISSIONS';
    PRINT N'-- ************************************************************';
    EXEC sec.spCompareServerPermissions @HostName = @HostName, @ReportOnly = @ReportOnly;

    -- Database-level comparisons
    PRINT N'';
    PRINT N'-- ************************************************************';
    PRINT N'-- SECTION 4: DATABASE PRINCIPALS (Users & Roles)';
    PRINT N'-- ************************************************************';
    EXEC sec.spCompareDbPrincipals @HostName = @HostName, @DbName = @DbName, @ReportOnly = @ReportOnly;

    PRINT N'';
    PRINT N'-- ************************************************************';
    PRINT N'-- SECTION 5: DATABASE ROLE MEMBERSHIPS';
    PRINT N'-- ************************************************************';
    EXEC sec.spCompareDbRoleMemberships @HostName = @HostName, @DbName = @DbName, @ReportOnly = @ReportOnly;

    PRINT N'';
    PRINT N'-- ************************************************************';
    PRINT N'-- SECTION 6: DATABASE PERMISSIONS';
    PRINT N'-- ************************************************************';
    EXEC sec.spCompareDbPermissions @HostName = @HostName, @DbName = @DbName, @ReportOnly = @ReportOnly;

    PRINT N'';
    PRINT N'-- ############################################################';
    PRINT N'-- END OF COMPREHENSIVE SECURITY COMPARISON REPORT';
    PRINT N'-- ############################################################';
END;
GO

---------------------------------------------------------------------------
-- sec.spApplySecurityChanges
--
-- Collects all remediation commands needed to synchronise the live server
-- with the desired state in the sec.* tables, then either prints them
-- (@WhatIf = 1) or executes them (@WhatIf = 0).
--
-- This procedure consolidates the logic from every Compare* procedure
-- into a single execution pipeline, processing changes in dependency
-- order:
--   1. Server logins       (CREATE LOGIN)
--   2. Server role memberships (ALTER SERVER ROLE ... ADD MEMBER)
--   3. Server permissions  (GRANT / DENY)
--   4. Database users      (CREATE USER)
--   5. Database roles      (CREATE ROLE)
--   6. Database role memberships (ALTER ROLE ... ADD MEMBER)
--   7. Database permissions (GRANT / DENY on DATABASE / SCHEMA / OBJECT)
--
-- All commands are collected into a #Commands temp table first, then
-- iterated in insertion order. Each command is executed inside its own
-- TRY/CATCH so a failure on one item does not block the rest.
--
-- Safety controls:
--   @WhatIf       = 1 (default) – Print commands only, change nothing.
--   @ApplyMissing = 1 (default) – Create / grant items that are in sec
--                                 but missing on the server.
--   @ApplyExtras  = 0 (default) – Do NOT drop/revoke extras. Set to 1
--                                 only after careful review; this will
--                                 DROP logins, users, roles, and REVOKE
--                                 permissions that exist on the server
--                                 but are absent from the sec tables.
--
-- Collation-safe comparisons (COLLATE DATABASE_DEFAULT) are used
-- throughout the dynamic SQL to avoid collation conflicts when the
-- target database uses a different collation than _meta.
--
-- Parameters:
--   @HostName     – Server name (default @@SERVERNAME).
--   @DbName       – Optional: restrict to a single database; NULL = all.
--   @ApplyMissing – Whether to create/grant missing items (default 1).
--   @ApplyExtras  – Whether to drop/revoke extra items (default 0).
--   @WhatIf       – 1 = dry run (print only), 0 = execute (default 1).
--
-- Usage:
--   -- Preview all changes
--   EXEC sec.spApplySecurityChanges @WhatIf = 1;
--
--   -- Apply missing items only (safe)
--   EXEC sec.spApplySecurityChanges @WhatIf = 0, @ApplyMissing = 1, @ApplyExtras = 0;
--
--   -- Apply everything including drops (dangerous)
--   EXEC sec.spApplySecurityChanges @WhatIf = 0, @ApplyMissing = 1, @ApplyExtras = 1;
--
-- Change history:
-- 2026-01-26 JJJ Initial version.
---------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE sec.spApplySecurityChanges
    @HostName sysname = NULL,
    @DbName sysname = NULL,
    @ApplyMissing bit = 1,      -- Apply missing logins/users/roles/memberships/permissions
    @ApplyExtras bit = 0,       -- Drop extras (DANGEROUS - default OFF)
    @WhatIf bit = 1             -- If 1, only print commands; if 0, execute them
AS
BEGIN
    SET NOCOUNT ON;

    SET @HostName = ISNULL(@HostName, @@SERVERNAME);

    IF @WhatIf = 0 AND @ApplyExtras = 1
    BEGIN
        PRINT N'WARNING: ApplyExtras=1 will DROP logins/users/roles. Proceed with caution!';
        PRINT N'To confirm, run with @WhatIf=0 after reviewing the WhatIf output.';
    END

    PRINT N'-- ############################################################';
    PRINT N'-- APPLY SECURITY CHANGES';
    PRINT N'-- Host: ' + @HostName;
    PRINT N'-- Database: ' + ISNULL(@DbName, 'ALL');
    PRINT N'-- WhatIf: ' + CASE WHEN @WhatIf = 1 THEN 'YES (no changes)' ELSE 'NO (EXECUTING!)' END;
    PRINT N'-- ApplyMissing: ' + CASE WHEN @ApplyMissing = 1 THEN 'YES' ELSE 'NO' END;
    PRINT N'-- ApplyExtras: ' + CASE WHEN @ApplyExtras = 1 THEN 'YES (DANGEROUS!)' ELSE 'NO' END;
    PRINT N'-- Generated: ' + CONVERT(nvarchar(30), GETDATE(), 126);
    PRINT N'-- ############################################################';
    PRINT N'';

    -- Collect all commands into a temp table
    CREATE TABLE #Commands (
        Seq int IDENTITY(1,1),
        Category varchar(32),
        CommandType varchar(16),
        Command nvarchar(max)
    );

    -- ================================================================
    -- SERVER LOGINS - Missing
    -- ================================================================
    IF @ApplyMissing = 1
    BEGIN
        INSERT INTO #Commands (Category, CommandType, Command)
        SELECT 
            'SERVER_LOGIN', 'CREATE',
            CASE s.LoginType
                WHEN 'SQL' THEN 
                    N'CREATE LOGIN ' + QUOTENAME(s.LoginName) + N' WITH PASSWORD = ' + 
                    CASE 
                        WHEN s.PasswordMode = 'HASHED' AND s.PasswordHash IS NOT NULL 
                        THEN s.PasswordHash + N' HASHED'
                        WHEN s.PasswordPlain IS NOT NULL 
                        THEN N'N''' + REPLACE(s.PasswordPlain, '''', '''''') + N''''
                        ELSE N'''CHANGEME!123'''
                    END +
                    CASE WHEN s.CheckPolicy IS NOT NULL THEN N', CHECK_POLICY = ' + CASE s.CheckPolicy WHEN 1 THEN 'ON' ELSE 'OFF' END ELSE '' END +
                    CASE WHEN s.CheckExpiration IS NOT NULL THEN N', CHECK_EXPIRATION = ' + CASE s.CheckExpiration WHEN 1 THEN 'ON' ELSE 'OFF' END ELSE '' END +
                    CASE WHEN s.DefaultDatabase IS NOT NULL THEN N', DEFAULT_DATABASE = ' + QUOTENAME(s.DefaultDatabase) ELSE '' END + N';'
                WHEN 'WINDOWS' THEN 
                    N'CREATE LOGIN ' + QUOTENAME(s.LoginName) + N' FROM WINDOWS' +
                    CASE WHEN s.DefaultDatabase IS NOT NULL THEN N' WITH DEFAULT_DATABASE = ' + QUOTENAME(s.DefaultDatabase) ELSE '' END + N';'
                WHEN 'EXTERNAL' THEN 
                    N'CREATE LOGIN ' + QUOTENAME(s.LoginName) + N' FROM EXTERNAL PROVIDER;'
                ELSE N'-- Unknown login type'
            END
        FROM sec.ServerLogin s
        WHERE s.HostName = @HostName
          AND NOT EXISTS (SELECT 1 FROM sec.ifGetActualServerLogins(@HostName) a WHERE a.LoginName = s.LoginName COLLATE DATABASE_DEFAULT);

        -- Server role memberships - Missing
        INSERT INTO #Commands (Category, CommandType, Command)
        SELECT 'SERVER_ROLE_MEMBERSHIP', 'ADD',
            N'ALTER SERVER ROLE ' + QUOTENAME(s.ServerRoleName) + N' ADD MEMBER ' + QUOTENAME(s.LoginName) + N';'
        FROM sec.vwServerRoleMemberships s
        WHERE s.HostName = @HostName
          AND NOT EXISTS (
              SELECT 1 FROM sec.ifGetActualServerRoleMemberships(@HostName) a 
              WHERE a.LoginName = s.LoginName COLLATE DATABASE_DEFAULT AND a.ServerRoleName = s.ServerRoleName COLLATE DATABASE_DEFAULT
          );

        -- Server permissions - Missing
        INSERT INTO #Commands (Category, CommandType, Command)
        SELECT 'SERVER_PERMISSION', 'GRANT',
            s.State + N' ' + s.PermissionName + 
            CASE s.Scope
                WHEN 'ENDPOINT' THEN N' ON ENDPOINT::' + QUOTENAME(s.EndpointName)
                WHEN 'AVAILABILITY_GROUP' THEN N' ON AVAILABILITY GROUP::' + QUOTENAME(s.AGName)
                ELSE N''
            END +
            N' TO ' + QUOTENAME(s.LoginName) +
            CASE WHEN s.WithGrantOption = 1 THEN N' WITH GRANT OPTION' ELSE N'' END + N';'
        FROM sec.vwServerPermissions s
        WHERE s.HostName = @HostName
          AND NOT EXISTS (
              SELECT 1 FROM sec.ifGetActualServerPermissions(@HostName) a 
              WHERE a.LoginName = s.LoginName COLLATE DATABASE_DEFAULT AND a.PermissionName = s.PermissionName COLLATE DATABASE_DEFAULT
          );
    END

    -- ================================================================
    -- DATABASE USERS - Missing
    -- ================================================================
    IF @ApplyMissing = 1
    BEGIN
        DECLARE @db sysname;
        DECLARE @execsql nvarchar(256);
        DECLARE @sql nvarchar(max);

        DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT DISTINCT DbName 
            FROM sec.DbPrincipal 
            WHERE HostName = @HostName
              AND (@DbName IS NULL OR DbName = @DbName);

        OPEN db_cursor;
        FETCH NEXT FROM db_cursor INTO @db;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            IF DB_ID(@db) IS NOT NULL
            BEGIN
                SET @execsql = QUOTENAME(@db) + N'.sys.sp_executesql';

                -- Missing users
                SET @sql = N'
                SELECT 
                    ''DB_USER'',
                    ''CREATE'',
                    CASE s.AuthType
                        WHEN ''LOGIN'' THEN N''USE '' + QUOTENAME(@DbNameParam) + N''; CREATE USER '' + QUOTENAME(s.PrincipalName) + N'' FOR LOGIN '' + QUOTENAME(s.LoginName) + N'' WITH DEFAULT_SCHEMA = '' + QUOTENAME(ISNULL(s.DefaultSchema, ''dbo'')) + N'';''
                        WHEN ''WITHOUT_LOGIN'' THEN N''USE '' + QUOTENAME(@DbNameParam) + N''; CREATE USER '' + QUOTENAME(s.PrincipalName) + N'' WITHOUT LOGIN WITH DEFAULT_SCHEMA = '' + QUOTENAME(ISNULL(s.DefaultSchema, ''dbo'')) + N'';''
                        WHEN ''EXTERNAL'' THEN N''USE '' + QUOTENAME(@DbNameParam) + N''; CREATE USER '' + QUOTENAME(s.PrincipalName) + N'' FROM EXTERNAL PROVIDER;''
                        ELSE N''-- Unknown auth type for user '' + QUOTENAME(s.PrincipalName)
                    END
                FROM _meta.sec.DbPrincipal s
                WHERE s.HostName = @HostNameParam
                  AND s.DbName = @DbNameParam
                  AND s.PrincipalType = ''USER''
                  AND NOT EXISTS (
                      SELECT 1 FROM sys.database_principals dp 
                      WHERE dp.name COLLATE DATABASE_DEFAULT = s.PrincipalName COLLATE DATABASE_DEFAULT 
                        AND dp.type IN (''S'',''U'',''G'',''E'',''X'')
                  )';

                INSERT INTO #Commands (Category, CommandType, Command)
                EXEC @execsql @sql, 
                    N'@HostNameParam sysname, @DbNameParam sysname', 
                    @HostNameParam = @HostName, @DbNameParam = @db;

                -- Missing roles
                SET @sql = N'
                SELECT 
                    ''DB_ROLE'',
                    ''CREATE'',
                    N''USE '' + QUOTENAME(@DbNameParam) + N''; CREATE ROLE '' + QUOTENAME(s.PrincipalName) + N'';''
                FROM _meta.sec.DbPrincipal s
                WHERE s.HostName = @HostNameParam
                  AND s.DbName = @DbNameParam
                  AND s.PrincipalType = ''ROLE''
                  AND NOT EXISTS (
                      SELECT 1 FROM sys.database_principals dp 
                      WHERE dp.name COLLATE DATABASE_DEFAULT = s.PrincipalName COLLATE DATABASE_DEFAULT 
                        AND dp.type = ''R''
                  )';

                INSERT INTO #Commands (Category, CommandType, Command)
                EXEC @execsql @sql, 
                    N'@HostNameParam sysname, @DbNameParam sysname', 
                    @HostNameParam = @HostName, @DbNameParam = @db;

                -- Missing role memberships
                SET @sql = N'
                SELECT 
                    ''DB_ROLE_MEMBERSHIP'',
                    ''ADD'',
                    N''USE '' + QUOTENAME(@DbNameParam) + N''; ALTER ROLE '' + QUOTENAME(s.RoleName) + N'' ADD MEMBER '' + QUOTENAME(s.UserName) + N'';''
                FROM _meta.sec.DbSecurityAssignment s
                WHERE s.HostName = @HostNameParam
                  AND s.DbName = @DbNameParam
                  AND s.AssignmentType = ''ROLE_MEMBERSHIP''
                  AND NOT EXISTS (
                      SELECT 1 
                      FROM sys.database_role_members rm
                      JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
                      JOIN sys.database_principals m ON rm.member_principal_id = m.principal_id
                      WHERE r.name COLLATE DATABASE_DEFAULT = s.RoleName COLLATE DATABASE_DEFAULT 
                        AND m.name COLLATE DATABASE_DEFAULT = s.UserName COLLATE DATABASE_DEFAULT
                  )';

                INSERT INTO #Commands (Category, CommandType, Command)
                EXEC @execsql @sql, 
                    N'@HostNameParam sysname, @DbNameParam sysname', 
                    @HostNameParam = @HostName, @DbNameParam = @db;

                -- Missing permissions
                SET @sql = N'
                SELECT 
                    ''DB_PERMISSION'',
                    ''GRANT'',
                    N''USE '' + QUOTENAME(@DbNameParam) + N''; '' + s.State + N'' '' + s.PermissionName +
                    CASE s.Scope
                        WHEN ''SCHEMA'' THEN N'' ON SCHEMA::'' + QUOTENAME(s.SchemaName)
                        WHEN ''OBJECT'' THEN N'' ON '' + QUOTENAME(s.SchemaName) + N''.'' + QUOTENAME(s.ObjectName)
                        ELSE N''''
                    END +
                    N'' TO '' + QUOTENAME(s.RoleName) +
                    CASE WHEN s.WithGrantOption = 1 THEN N'' WITH GRANT OPTION'' ELSE N'''' END + N'';''
                FROM _meta.sec.DbSecurityAssignment s
                WHERE s.HostName = @HostNameParam
                  AND s.DbName = @DbNameParam
                  AND s.AssignmentType = ''ROLE_PERMISSION''
                  AND NOT EXISTS (
                      SELECT 1 
                      FROM sys.database_permissions pe
                      JOIN sys.database_principals pr ON pe.grantee_principal_id = pr.principal_id
                      LEFT JOIN sys.schemas sch ON pe.class = 3 AND pe.major_id = sch.schema_id
                      LEFT JOIN sys.objects obj ON pe.class = 1 AND pe.major_id = obj.object_id
                      LEFT JOIN sys.schemas obj_sch ON obj.schema_id = obj_sch.schema_id
                      WHERE pr.name COLLATE DATABASE_DEFAULT = s.RoleName COLLATE DATABASE_DEFAULT
                        AND pe.permission_name COLLATE DATABASE_DEFAULT = s.PermissionName COLLATE DATABASE_DEFAULT
                        AND (
                            (s.Scope = ''DATABASE'' AND pe.class = 0)
                            OR (s.Scope = ''SCHEMA'' AND pe.class = 3 AND sch.name COLLATE DATABASE_DEFAULT = s.SchemaName COLLATE DATABASE_DEFAULT)
                            OR (s.Scope = ''OBJECT'' AND pe.class = 1 AND obj.name COLLATE DATABASE_DEFAULT = s.ObjectName COLLATE DATABASE_DEFAULT AND obj_sch.name COLLATE DATABASE_DEFAULT = s.SchemaName COLLATE DATABASE_DEFAULT)
                        )
                  )';

                INSERT INTO #Commands (Category, CommandType, Command)
                EXEC @execsql @sql, 
                    N'@HostNameParam sysname, @DbNameParam sysname', 
                    @HostNameParam = @HostName, @DbNameParam = @db;
            END

            FETCH NEXT FROM db_cursor INTO @db;
        END

        CLOSE db_cursor;
        DEALLOCATE db_cursor;
    END

    -- Execute or print commands
    DECLARE @cmd nvarchar(max);
    DECLARE @cat varchar(32);
    DECLARE cmd_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT Category, Command FROM #Commands ORDER BY Seq;

    OPEN cmd_cursor;
    FETCH NEXT FROM cmd_cursor INTO @cat, @cmd;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @WhatIf = 1
        BEGIN
            PRINT N'-- [' + @cat + N'] ' + @cmd;
        END
        ELSE
        BEGIN
            BEGIN TRY
                PRINT N'EXECUTING: ' + @cmd;
                EXEC sp_executesql @cmd;
                PRINT N'  SUCCESS';
            END TRY
            BEGIN CATCH
                PRINT N'  ERROR: ' + ERROR_MESSAGE();
            END CATCH
        END

        FETCH NEXT FROM cmd_cursor INTO @cat, @cmd;
    END

    CLOSE cmd_cursor;
    DEALLOCATE cmd_cursor;

    DROP TABLE #Commands;

    PRINT N'';
    PRINT N'-- ############################################################';
    PRINT N'-- END APPLY SECURITY CHANGES';
    PRINT N'-- ############################################################';
END;
GO