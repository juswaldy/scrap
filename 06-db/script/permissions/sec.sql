USE _meta;
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'sec')
BEGIN
    EXEC('CREATE SCHEMA sec');
END
GO

--------------------------------------------------------------------------------
-- Tables
--------------------------------------------------------------------------------
IF OBJECT_ID('sec.ServerLogin', 'U') IS NULL
BEGIN
    CREATE TABLE sec.ServerLogin (
        HostName         sysname      NOT NULL,          -- SQL Server instance / host
        LoginName        sysname      NOT NULL,
        LoginType        varchar(16)  NOT NULL,          -- 'SQL','WINDOWS','EXTERNAL'
        PasswordMode     varchar(16)  NULL,              -- 'PLAIN','HASHED' (for SQL)
        PasswordPlain    nvarchar(512) NULL,             -- if you keep PLAIN
        PasswordHash     nvarchar(512) NULL,             -- if you keep HASHED
        CheckPolicy      bit          NULL,
        CheckExpiration  bit          NULL,
        MustChange       bit          NULL,
        DefaultDatabase  sysname      NULL,
        DefaultLanguage  sysname      NULL,
        Disabled         bit          NOT NULL DEFAULT 0,
        SID              varbinary(85) NULL,

        CONSTRAINT PK_ServerLogin PRIMARY KEY (HostName, LoginName)
    );
END
GO

IF OBJECT_ID('sec.ServerSecurityAssignment', 'U') IS NULL
BEGIN
    CREATE TABLE sec.ServerSecurityAssignment (
        AssignmentId     int IDENTITY(1,1) PRIMARY KEY,
        HostName         sysname      NOT NULL,
        LoginName        sysname      NOT NULL,   -- grantee

        AssignmentType   varchar(16)  NOT NULL,   -- 'PERMISSION' or 'SERVER_ROLE'

        -- For PERMISSION (from ServerPermissions sheet):
        State           varchar(5)   NULL,        -- 'GRANT' or 'DENY'
        Scope           varchar(32)  NULL,        -- 'SERVER','ENDPOINT','AVAILABILITY_GROUP'
        PermissionName  varchar(128) NULL,
        EndpointName    sysname      NULL,        -- when Scope='ENDPOINT'
        AGName          sysname      NULL,        -- when Scope='AVAILABILITY_GROUP'
        WithGrantOption bit          NULL,

        -- For SERVER_ROLE (from ServerRoleMemberships sheet):
        ServerRoleName  sysname      NULL        -- 'sysadmin', 'securityadmin', etc.
    );

    CREATE INDEX IX_ServerSecurityAssignment_Login
        ON sec.ServerSecurityAssignment (HostName, LoginName, AssignmentType);
END
GO

IF OBJECT_ID('sec.DbPrincipal', 'U') IS NULL
BEGIN
    CREATE TABLE sec.DbPrincipal (
        HostName        sysname      NOT NULL,
        DbName          sysname      NOT NULL,
        PrincipalName   sysname      NOT NULL,         -- role or user name
        PrincipalType   varchar(8)   NOT NULL,         -- 'ROLE' or 'USER'

        -- User-specific columns (valid when PrincipalType='USER'):
        AuthType        varchar(16)  NULL,             -- 'LOGIN','WITHOUT_LOGIN','EXTERNAL'
        LoginName       sysname      NULL,             -- when AuthType='LOGIN'
        DefaultSchema   sysname      NULL,             -- default 'dbo' if NULL in script

        CONSTRAINT PK_DbPrincipal PRIMARY KEY (HostName, DbName, PrincipalName)
    );

    CREATE INDEX IX_DbPrincipal_Type
        ON sec.DbPrincipal (HostName, DbName, PrincipalType);
END
GO

IF OBJECT_ID('sec.DbSecurityAssignment', 'U') IS NULL
BEGIN
    CREATE TABLE sec.DbSecurityAssignment (
        AssignmentId      int IDENTITY(1,1) PRIMARY KEY,
        HostName          sysname      NOT NULL,
        DbName            sysname      NOT NULL,

        AssignmentType    varchar(16)  NOT NULL,  -- 'ROLE_PERMISSION' or 'ROLE_MEMBERSHIP'

        -- For ROLE_PERMISSION (Permissions sheet; grants/denies to roles):
        RoleName         sysname      NULL,       -- grantee role
        State            varchar(5)   NULL,       -- 'GRANT' or 'DENY'
        Scope            varchar(16)  NULL,       -- 'DATABASE','SCHEMA','OBJECT'
        SchemaName       sysname      NULL,
        ObjectName       sysname      NULL,
        PermissionName   varchar(128) NULL,
        WithGrantOption  bit          NULL,

        -- For ROLE_MEMBERSHIP (Memberships sheet):
        UserName         sysname      NULL       -- DB user added to RoleName
    );

    CREATE INDEX IX_DbSecurityAssignment_Db
        ON sec.DbSecurityAssignment (HostName, DbName, AssignmentType);
END
GO

--------------------------------------------------------------------------------
-- Basic views
--------------------------------------------------------------------------------
CREATE OR ALTER VIEW sec.vwRoles
AS
SELECT
    HostName,
    DbName,
    RoleName = PrincipalName
FROM sec.DbPrincipal
WHERE PrincipalType = 'ROLE';
GO

CREATE OR ALTER VIEW sec.vwUsers
AS
SELECT
    HostName,
    DbName,
    UserName = PrincipalName,
    AuthType,
    LoginName,
    DefaultSchema
FROM sec.DbPrincipal
WHERE PrincipalType = 'USER';
GO

CREATE OR ALTER VIEW sec.vwPermissions
AS
SELECT
    HostName,
    DbName,
    RoleName,
    State,
    Scope,
    SchemaName,
    ObjectName,
    PermissionName,
    WithGrantOption
FROM sec.DbSecurityAssignment
WHERE AssignmentType = 'ROLE_PERMISSION';
GO

CREATE OR ALTER VIEW sec.vwMemberships
AS
SELECT
    HostName,
    DbName,
    UserName,
    RoleName
FROM sec.DbSecurityAssignment
WHERE AssignmentType = 'ROLE_MEMBERSHIP';
GO

CREATE OR ALTER VIEW sec.vwServerPermissions
AS
SELECT
    HostName,
    LoginName,
    State,
    Scope,
    PermissionName,
    EndpointName,
    AGName,
    WithGrantOption
FROM sec.ServerSecurityAssignment
WHERE AssignmentType = 'PERMISSION';
GO

CREATE OR ALTER VIEW sec.vwServerRoleMemberships
AS
SELECT
    HostName,
    LoginName,
    ServerRoleName
FROM sec.ServerSecurityAssignment
WHERE AssignmentType = 'SERVER_ROLE';
GO

CREATE OR ALTER VIEW sec.vwLogins
AS
SELECT
    HostName,
    LoginName,
    LoginType,
    PasswordMode,
    PasswordPlain,
    PasswordHash,
    CheckPolicy,
    CheckExpiration,
    MustChange,
    DefaultDatabase,
    DefaultLanguage,
    Disabled,
    SID
FROM sec.ServerLogin;
GO

CREATE OR ALTER VIEW sec.vwEndpoints
AS
SELECT
    NULL AS HostName,
    NULL AS EndpointName,
    NULL AS EndpointType,
    NULL AS Port,
    NULL AS State,
    NULL AS Role,
    NULL AS Encryption,
    NULL AS Algorithm,
    NULL AS Authentication,
    NULL AS OwnerLogin,
    NULL AS ForceRecreate
FROM sys.endpoints
WHERE 1 = 0;  -- empty view placeholder
GO

--------------------------------------------------------------------------------
-- Helper views
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- sec.vwMissingBronzeUsers
--
-- Identifies database users that should exist in every bronze database
-- but are not yet recorded in sec.DbPrincipal.
--
-- The "desired state" is the Cartesian product of all bronze databases
-- (from dbo.vwBronzeDatabases) and all server logins (from
-- sec.ServerLogin). Each login is expected to have a same-named database
-- user with AuthType = 'LOGIN' and DefaultSchema = 'dbo'.
--
-- The view uses EXCEPT to subtract the users already present in
-- sec.DbPrincipal, so only the gaps are returned.
--
-- Consumed by:  sec.spSyncBronzeDbPrincipals (to create missing users
--               and record them in sec.DbPrincipal).
--
-- Change history:
-- 2026-01-26 JJJ  Initial version.
--------------------------------------------------------------------------------
CREATE OR ALTER VIEW sec.vwMissingBronzeUsers
AS
WITH 
current_users AS (
    SELECT x.*
    FROM _meta.dbo.vwBronzeDatabases b
    JOIN _meta.sec.DbPrincipal x ON b._catalog = x.DbName
    WHERE x.PrincipalType = 'USER'
),
future_users AS (
    SELECT
        x.HostName,
        b._catalog AS DbName,
        x.LoginName AS PrincipalName,
        'USER' AS PrincipalType,
        'LOGIN' AS AuthType,
        x.LoginName,
        'dbo' AS DefaultSchema
    FROM _meta.dbo.vwBronzeDatabases b
    CROSS APPLY _meta.sec.ServerLogin x
),
missing_users AS (
    SELECT * FROM future_users
    EXCEPT
    SELECT * FROM current_users
)
SELECT * FROM missing_users;
GO

--------------------------------------------------------------------------------
-- sec.vwMissingBronzeRoles
--
-- Identifies database roles that should exist in every bronze database
-- but are not yet recorded in sec.DbPrincipal.
--
-- The expected role set is hard-coded:
--   db_datareader    – built-in; grants SELECT on all user tables.
--   db_datawriter    – built-in; grants INSERT/UPDATE/DELETE.
--   db_ddladmin      – built-in; grants DDL rights.
--   IntegrationRole  – custom role for the ETL service account (cloveretl).
--   SourceReaderRole – custom role for read-only source access.
--
-- Each role is cross-joined with every bronze database. The EXCEPT
-- clause removes roles that are already tracked in sec.DbPrincipal,
-- leaving only the missing ones.
--
-- Note: Built-in roles (db_datareader, db_datawriter, db_ddladmin)
-- always exist in SQL Server databases and do not need CREATE ROLE.
-- The consumer (spSyncBronzeDbPrincipals) filters them out before
-- issuing CREATE ROLE statements, but they are included here so the
-- metadata in sec.DbPrincipal is complete.
--
-- Consumed by:  sec.spSyncBronzeDbPrincipals.
--
-- Change history:
-- 2026-01-14 JJJ Initial version.
--------------------------------------------------------------------------------
CREATE OR ALTER VIEW sec.vwMissingBronzeRoles
AS
WITH 
current_roles AS (
    SELECT x.*
    FROM _meta.dbo.vwBronzeDatabases b
    JOIN _meta.sec.DbPrincipal x ON b._catalog = x.DbName
    WHERE x.PrincipalType = 'ROLE'
),
bronze_roles AS (
    SELECT 'db_datareader' AS RoleName
    UNION
    SELECT 'db_datawriter' AS RoleName
    UNION
    SELECT 'db_ddladmin' AS RoleName
    UNION
    SELECT 'IntegrationRole' AS RoleName
    UNION
    SELECT 'SourceReaderRole' AS RoleName
),
future_roles AS (
    SELECT
        'Datahub' AS HostName,
        b._catalog AS DbName,
        x.RoleName AS PrincipalName,
        'ROLE' AS PrincipalType,
        NULL AS AuthType,
        NULL AS LoginName,
        NULL AS DefaultSchema
    FROM _meta.dbo.vwBronzeDatabases b
    CROSS APPLY bronze_roles x
),
missing_roles AS (
    SELECT * FROM future_roles
    EXCEPT
    SELECT * FROM current_roles
)
SELECT * FROM missing_roles;
GO

--------------------------------------------------------------------------------
-- sec.vwMissingBronzeMemberships
--
-- Identifies role-membership assignments that should exist in every
-- bronze database but are not yet recorded in sec.DbSecurityAssignment.
--
-- The desired membership model has two parts:
--
--   1. Per-login memberships
--      Every server login (except 'cloveretl') is expected to be a
--      member of SourceReaderRole in every bronze database. This gives
--      all non-ETL logins read access through the role hierarchy.
--
--   2. Fixed structural memberships (same in every bronze DB)
--        SourceReaderRole  → db_datareader   (read access)
--        IntegrationRole   → db_datareader   (read access for ETL)
--        IntegrationRole   → db_datawriter   (write access for ETL)
--        IntegrationRole   → db_ddladmin     (DDL for ETL)
--        cloveretl         → IntegrationRole (ETL service account)
--
-- The EXCEPT clause subtracts memberships already tracked in
-- sec.DbSecurityAssignment (filtered to the five relevant roles),
-- leaving only the gaps.
--
-- Consumed by:  sec.spSyncBronzeDbPrincipals.
--
-- Change history:
-- 2026-01-14 JJJ Initial version.
--------------------------------------------------------------------------------
CREATE OR ALTER VIEW sec.vwMissingBronzeMemberships
AS
WITH
current_memberships AS (
    SELECT x.HostName, x.DbName, x.AssignmentType, x.RoleName, x.UserName
    FROM _meta.dbo.vwBronzeDatabases b
    JOIN _meta.sec.DbSecurityAssignment x ON b._catalog = x.DbName
    WHERE x.AssignmentType = 'ROLE_MEMBERSHIP'
    AND x.RoleName IN ('db_datareader', 'db_datawriter', 'db_ddladmin', 'IntegrationRole', 'SourceReaderRole')
),
future_memberships AS (
    SELECT
        x.HostName,
        b._catalog AS DbName,
        'ROLE_MEMBERSHIP' AS AssignmentType,
        'SourceReaderRole' AS RoleName,
        x.LoginName AS UserName
    FROM _meta.dbo.vwBronzeDatabases b
    CROSS APPLY _meta.sec.ServerLogin x
    WHERE x.LoginName != 'cloveretl'
    UNION
    SELECT
        'Datahub' AS HostName,
        b._catalog AS DbName,
        'ROLE_MEMBERSHIP' AS AssignmentType,
        x.RoleName AS RoleName,
        x.UserName AS UserName
    FROM _meta.dbo.vwBronzeDatabases b
    CROSS APPLY (
        SELECT 'db_datareader' AS RoleName, 'SourceReaderRole' AS UserName
        UNION
        SELECT 'db_datareader' AS RoleName, 'IntegrationRole' AS UserName
        UNION
        SELECT 'db_datawriter' AS RoleName, 'IntegrationRole' AS UserName
        UNION
        SELECT 'db_ddladmin' AS RoleName, 'IntegrationRole' AS UserName
        UNION
        SELECT 'IntegrationRole' AS RoleName, 'cloveretl' AS UserName
    ) x
),
missing_memberships AS (
    SELECT * FROM future_memberships
    EXCEPT
    SELECT * FROM current_memberships
)
SELECT * FROM missing_memberships;
GO

--------------------------------------------------------------------------------
-- Stored Procedures
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- sec.spSyncBronzeDbPrincipals
--
-- Purpose:
--   For each "bronze" database (from _meta.dbo.vwBronzeDatabases), ensure:
--     1) Expected DB users exist for server logins
--     2) Expected custom roles exist
--     3) Expected role memberships are present
--   Then, record the created/required principals and assignments into the
--   _meta.sec metadata tables.
--
-- Notes:
--   - This procedure is intended to be idempotent.
--   - Logging is done via PRINT statements.
--   - @Debug = 1 (dry-run): prints statements but does not execute them.
--
-- Change history:
-- 2026-01-26 JJJ Initial version.
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE sec.spSyncBronzeDbPrincipals
    @Debug bit = 1
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @started_at datetime2(0) = SYSDATETIME();
    PRINT N'spSyncBronzeDbPrincipals: START ' + CONVERT(nvarchar(30), @started_at, 126);
    PRINT N'spSyncBronzeDbPrincipals: mode=' + CASE WHEN @Debug = 1 THEN N'DRY_RUN (print only)' ELSE N'EXECUTE' END;

    -- Generate GRANT statements to assign users to roles in bronze DBs and execute them.
    DECLARE @execsql NVARCHAR(256);
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @DbName sysname;
    DECLARE @PrincipalName sysname;
    DECLARE @PrincipalType VARCHAR(8);
    DECLARE @LoginName sysname;
    DECLARE @RoleName sysname;
    DECLARE @UserName sysname;

    DECLARE @missing_users int = 0;
    DECLARE @missing_roles int = 0;
    DECLARE @missing_memberships int = 0;

    SELECT @missing_users = COUNT(*) FROM _meta.sec.vwMissingBronzeUsers;
    SELECT @missing_roles = COUNT(*) FROM _meta.sec.vwMissingBronzeRoles;
    SELECT @missing_memberships = COUNT(*) FROM _meta.sec.vwMissingBronzeMemberships;

    PRINT N'spSyncBronzeDbPrincipals: missing users=' + CONVERT(nvarchar(20), @missing_users)
        + N', roles=' + CONVERT(nvarchar(20), @missing_roles)
        + N', memberships=' + CONVERT(nvarchar(20), @missing_memberships);

    -----------------------------------------------------------------------
    -- Create missing users
    -----------------------------------------------------------------------
    PRINT N'spSyncBronzeDbPrincipals: creating missing users (if any)';
    DECLARE user_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT DbName, PrincipalName, LoginName
        FROM _meta.sec.vwMissingBronzeUsers;

    OPEN user_cursor;
    FETCH NEXT FROM user_cursor INTO @DbName, @PrincipalName, @LoginName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @execsql = QUOTENAME(@DbName) + N'.sys.sp_executesql';

        BEGIN TRY
            PRINT N'  [' + @DbName + N'] ensure USER ' + QUOTENAME(@PrincipalName) + N' for LOGIN ' + QUOTENAME(@LoginName);

            IF @Debug = 1
            BEGIN
                PRINT N'    DRY RUN: USE ' + QUOTENAME(@DbName) + N'; CREATE USER '
                    + QUOTENAME(@PrincipalName) + N' FOR LOGIN ' + QUOTENAME(@LoginName)
                    + N' WITH DEFAULT_SCHEMA = [dbo];';
            END
            ELSE
            BEGIN
                EXEC @execsql N'
                    IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = @name)
                    BEGIN
                        DECLARE @stmt nvarchar(max) =
                            N''CREATE USER '' + QUOTENAME(@name)
                            + N'' FOR LOGIN '' + QUOTENAME(@login)
                            + N'' WITH DEFAULT_SCHEMA = [dbo];'';

                        EXEC (@stmt);

                        PRINT N''    created USER '' + QUOTENAME(@name) + N'' for LOGIN '' + QUOTENAME(@login);
                    END
                    ELSE
                    BEGIN
                        PRINT N''    USER already exists: '' + QUOTENAME(@name);
                    END',
                    N'@name sysname, @login sysname',
                    @name = @PrincipalName, @login = @LoginName;
            END
        END TRY
        BEGIN CATCH
            PRINT N'  [' + @DbName + N'] ERROR creating USER ' + QUOTENAME(@PrincipalName)
                + N': ' + ERROR_MESSAGE();
            THROW;
        END CATCH

        FETCH NEXT FROM user_cursor INTO @DbName, @PrincipalName, @LoginName;
    END;

    CLOSE user_cursor;
    DEALLOCATE user_cursor;

    -----------------------------------------------------------------------
    -- Create missing roles
    -----------------------------------------------------------------------
    PRINT N'spSyncBronzeDbPrincipals: creating missing roles (if any)';
    DECLARE role_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT DbName, PrincipalName
        FROM _meta.sec.vwMissingBronzeRoles
        WHERE PrincipalName NOT IN ('db_datareader', 'db_datawriter', 'db_ddladmin'); -- skip built-in roles

    OPEN role_cursor;
    FETCH NEXT FROM role_cursor INTO @DbName, @PrincipalName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @execsql = QUOTENAME(@DbName) + N'.sys.sp_executesql';

        BEGIN TRY
            PRINT N'  [' + @DbName + N'] ensure ROLE ' + QUOTENAME(@PrincipalName);

            IF @Debug = 1
            BEGIN
                PRINT N'    DRY RUN: USE ' + QUOTENAME(@DbName) + N'; CREATE ROLE ' + QUOTENAME(@PrincipalName) + N';';
            END
            ELSE
            BEGIN
                EXEC @execsql N'
                    IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = @RoleName AND type = ''R'')
                    BEGIN
                        DECLARE @stmt nvarchar(max) = N''CREATE ROLE '' + QUOTENAME(@RoleName) + N'';'';
                        EXEC (@stmt);
                        PRINT N''    created ROLE '' + QUOTENAME(@RoleName);
                    END
                    ELSE
                    BEGIN
                        PRINT N''    ROLE already exists: '' + QUOTENAME(@RoleName);
                    END',
                    N'@RoleName sysname',
                    @RoleName = @PrincipalName;
            END
        END TRY
        BEGIN CATCH
            PRINT N'  [' + @DbName + N'] ERROR creating ROLE ' + QUOTENAME(@PrincipalName)
                + N': ' + ERROR_MESSAGE();
            THROW;
        END CATCH

        FETCH NEXT FROM role_cursor INTO @DbName, @PrincipalName;
    END;

    CLOSE role_cursor;
    DEALLOCATE role_cursor;

    -----------------------------------------------------------------------
    -- Add missing role memberships
    -----------------------------------------------------------------------
    PRINT N'spSyncBronzeDbPrincipals: adding missing role memberships (if any)';
    DECLARE membership_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT DbName, RoleName, UserName
        FROM _meta.sec.vwMissingBronzeMemberships;

    OPEN membership_cursor;
    FETCH NEXT FROM membership_cursor INTO @DbName, @RoleName, @UserName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @execsql = QUOTENAME(@DbName) + N'.sys.sp_executesql';

        BEGIN TRY
            PRINT N'  [' + @DbName + N'] spSyncBronzeDbPrincipals: ensure membership: ' + QUOTENAME(@UserName) + N' -> ' + QUOTENAME(@RoleName);

            IF @Debug = 1
            BEGIN
                PRINT N'    DRY RUN: USE ' + QUOTENAME(@DbName) + N'; ALTER ROLE '
                    + QUOTENAME(@RoleName) + N' ADD MEMBER ' + QUOTENAME(@UserName) + N';';
            END
            ELSE
            BEGIN
                EXEC @execsql N'
                    IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = @UserName)
                    BEGIN
                        PRINT N''    SKIP: user missing: '' + QUOTENAME(@UserName);
                        RETURN;
                    END

                    IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = @RoleName)
                    BEGIN
                        PRINT N''    SKIP: role missing: '' + QUOTENAME(@RoleName);
                        RETURN;
                    END

                    IF EXISTS (
                        SELECT 1
                        FROM sys.database_role_members rm
                        JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
                        JOIN sys.database_principals m ON rm.member_principal_id = m.principal_id
                        WHERE r.name = @RoleName AND m.name = @UserName
                    )
                    BEGIN
                        PRINT N''    membership already exists'';
                        RETURN;
                    END

                    DECLARE @stmt nvarchar(max) =
                        N''ALTER ROLE '' + QUOTENAME(@RoleName) + N'' ADD MEMBER '' + QUOTENAME(@UserName) + N'';'';
                    EXEC (@stmt);
                    PRINT N''    added member '' + QUOTENAME(@UserName) + N'' to role '' + QUOTENAME(@RoleName);',
                    N'@RoleName sysname, @UserName sysname',
                    @RoleName = @RoleName,
                    @UserName = @UserName;
            END
        END TRY
        BEGIN CATCH
            PRINT N'  [' + @DbName + N'] ERROR adding membership ' + QUOTENAME(@UserName) + N' -> ' + QUOTENAME(@RoleName)
                + N': ' + ERROR_MESSAGE();
            THROW;
        END CATCH

        FETCH NEXT FROM membership_cursor INTO @DbName, @RoleName, @UserName;
    END;

    CLOSE membership_cursor;
    DEALLOCATE membership_cursor;

    -----------------------------------------------------------------------
    -- Record changes into _meta.sec tables (skipped in dry-run)
    -----------------------------------------------------------------------
    PRINT N'spSyncBronzeDbPrincipals: recording changes into _meta.sec tables';

    IF @Debug = 1
    BEGIN
        PRINT N'  DRY RUN: would INSERT DbPrincipal users from _meta.sec.vwMissingBronzeUsers (count=' + CONVERT(nvarchar(20), @missing_users) + N')';
        PRINT N'  DRY RUN: would INSERT DbPrincipal roles from _meta.sec.vwMissingBronzeRoles (count=' + CONVERT(nvarchar(20), @missing_roles) + N')';
        PRINT N'  DRY RUN: would INSERT DbSecurityAssignment memberships from _meta.sec.vwMissingBronzeMemberships (count=' + CONVERT(nvarchar(20), @missing_memberships) + N')';
    END
    ELSE
    BEGIN
        INSERT INTO _meta.sec.DbPrincipal (HostName, DbName, PrincipalName, PrincipalType, AuthType, LoginName, DefaultSchema)
        SELECT * FROM _meta.sec.vwMissingBronzeUsers;

        PRINT N'  inserted DbPrincipal users: ' + CONVERT(nvarchar(20), @@ROWCOUNT);

        INSERT INTO _meta.sec.DbPrincipal (HostName, DbName, PrincipalName, PrincipalType, AuthType, LoginName, DefaultSchema)
        SELECT * FROM _meta.sec.vwMissingBronzeRoles;

        PRINT N'  inserted DbPrincipal roles: ' + CONVERT(nvarchar(20), @@ROWCOUNT);

        INSERT INTO _meta.sec.DbSecurityAssignment (HostName, DbName, AssignmentType, RoleName, UserName)
        SELECT * FROM _meta.sec.vwMissingBronzeMemberships;

        PRINT N'  inserted DbSecurityAssignment memberships: ' + CONVERT(nvarchar(20), @@ROWCOUNT);
    END

    PRINT N'spSyncBronzeDbPrincipals: END ' + CONVERT(nvarchar(30), SYSDATETIME(), 126)
        + N' (duration_seconds=' + CONVERT(nvarchar(20), DATEDIFF(SECOND, @started_at, SYSDATETIME())) + N')';
END;
GO
