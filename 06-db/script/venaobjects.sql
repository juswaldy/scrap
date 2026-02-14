USE Vena;
--GO

-- app-perms
IF OBJECT_ID(N'[dbo].[app_perms]', N'U') IS NOT NULL
    DROP TABLE [dbo].[app_perms];
--GO

CREATE TABLE [dbo].[app_perms] (
    [_group]          NVARCHAR(255) NULL,
    [_access_type]    NVARCHAR(255) NULL,
    [_entity]         NVARCHAR(255) NULL,
    [_entity_name]    NVARCHAR(255) NULL,
    [_entity_1]       NVARCHAR(255) NULL,
    [_entity_name_1]  NVARCHAR(255) NULL,
    [_cmd]            NVARCHAR(255) NULL
);
--GO


-- datapermissions
IF OBJECT_ID(N'[dbo].[datapermissions]', N'U') IS NOT NULL
    DROP TABLE [dbo].[datapermissions];
--GO

CREATE TABLE [dbo].[datapermissions] (
    [_Group]           NVARCHAR(255) NULL,
    [_Access_Type]     NVARCHAR(255) NULL,
    [_dim_1_GL01]      NVARCHAR(255) NULL,
    [_dim_2_GL02]      NVARCHAR(1024) NULL,
    [_dim_3_GL03]      NVARCHAR(1024) NULL,
    [_dim_4_GL04]      NVARCHAR(1024) NULL,
    [_dim_5_GL05]      NVARCHAR(1024) NULL,
    [_dim_6_GL06]      NVARCHAR(1024) NULL,
    [_dim_7_YEAR]      NVARCHAR(255) NULL,
    [_dim_8_PERD]      NVARCHAR(255) NULL,
    [_dim_9_SCNR]      NVARCHAR(255) NULL,
    [_dim_10_FLEX01]   NVARCHAR(255) NULL,
    [_dim_11_FLEX02]   NVARCHAR(255) NULL,
    [_dim_12_FLEX03]   NVARCHAR(255) NULL,
    [_dim_13_FLEX04]   NVARCHAR(255) NULL,
    [_dim_14_MEASURE]  NVARCHAR(255) NULL,
    [_cmd]             NVARCHAR(255) NULL
);
--GO


-- groups
IF OBJECT_ID(N'[dbo].[groups]', N'U') IS NOT NULL
    DROP TABLE [dbo].[groups];
--GO

CREATE TABLE [dbo].[groups] (
    [_Group_Name]  NVARCHAR(255) NULL,
    [_Email]       NVARCHAR(255) NULL,
    [_cmd]         NVARCHAR(255) NULL
);
--GO


-- users
IF OBJECT_ID(N'[dbo].[users]', N'U') IS NOT NULL
    DROP TABLE [dbo].[users];
--GO

CREATE TABLE [dbo].[users] (
    [_first_name]          NVARCHAR(255) NULL,
    [_last_name]           NVARCHAR(255) NULL,
    [_email]               NVARCHAR(255) NULL,
    [_manager]             BIT NULL,
    [_contributor]         BIT NULL,
    [_modeler]             BIT NULL,
    [_dashboards]          NVARCHAR(MAX) NULL,
    [_reports]             NVARCHAR(MAX) NULL,
    [_copilot]             BIT NULL,
    [_viewer]              BIT NULL,
    [_admin]               BIT NULL,
    [_phone_number]        NVARCHAR(255) NULL,
    [_active]              BIT NULL,
    [_notification_email]  NVARCHAR(255) NULL,
    [_last_login]          NVARCHAR(255) NULL
);
--GO

-- For Vena db, insert missing users and assign them to SourceReaderRole
WITH bronze AS (
    SELECT 'Vena' AS _host, 'Vena' AS _catalog
),
current_users AS (
    SELECT x.*
    FROM bronze b
    JOIN _meta.sec.DbPrincipal x ON b._catalog = x.DbName
    WHERE x.PrincipalType = 'USER'
    AND x.PrincipalName != 'cloveretl'
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
    FROM bronze b
    CROSS APPLY _meta.sec.ServerLogin x
    WHERE x.LoginName != 'cloveretl'
),
new_users AS (
    SELECT * FROM future_users
    EXCEPT
    SELECT * FROM current_users
)
-- INSERT INTO _meta.sec.DbPrincipal (HostName, DbName, PrincipalName, PrincipalType, AuthType, LoginName, DefaultSchema)
SELECT * FROM new_users;

WITH bronze AS (
    SELECT DISTINCT _host, _catalog
    FROM _meta.dbo._object
    WHERE _host != 'Datahub' AND _status = 'active'
),
current_memberships AS (
    SELECT x.HostName, x.DbName, x.AssignmentType, x.RoleName, x.UserName
    FROM bronze b
    JOIN _meta.sec.DbSecurityAssignment x ON b._catalog = x.DbName
    WHERE x.AssignmentType = 'ROLE_MEMBERSHIP'
    AND x.RoleName = 'SourceReaderRole'
),
future_memberships AS (
    SELECT
        x.HostName,
        b._catalog AS DbName,
        'ROLE_MEMBERSHIP' AS AssignmentType,
        'SourceReaderRole' AS RoleName,
        x.LoginName AS UserName
    FROM bronze b
    CROSS APPLY _meta.sec.ServerLogin x
    WHERE x.LoginName != 'cloveretl'
),
new_memberships AS (
    SELECT * FROM future_memberships
    EXCEPT
    SELECT * FROM current_memberships
)
INSERT INTO _meta.sec.DbSecurityAssignment (HostName, DbName, AssignmentType, RoleName, UserName)
SELECT * FROM new_memberships;