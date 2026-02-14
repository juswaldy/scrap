USE _meta;
GO

--------------------------------------------------------------------------------
-- Tables for Data Objects and Ingestion Tracking
--------------------------------------------------------------------------------

-- 1. Object Table
IF OBJECT_ID('dbo._object', 'U') IS NULL
BEGIN
    CREATE TABLE dbo._object (
        -- _id INT IDENTITY(1,1) PRIMARY KEY,
        _id INT PRIMARY KEY,
        _type VARCHAR(32) NOT NULL,
        _name NVARCHAR(256) NOT NULL,
        _host NVARCHAR(256) NOT NULL,
        _catalog NVARCHAR(256) NOT NULL,
        _schema NVARCHAR(256) NOT NULL,
        _object NVARCHAR(256) NOT NULL,
        _keyfields NVARCHAR(1024),
        _filter NVARCHAR(MAX),
        _frequency INT,
        _status VARCHAR(8) DEFAULT 'active',
        _description NVARCHAR(MAX),
        _created DATETIME2 DEFAULT SYSDATETIME(),
        _modified DATETIME2 DEFAULT SYSDATETIME(),
        _verified DATETIME2,
        _refreshed DATETIME2,
        _numrows BIGINT,
        _checksum BIGINT,
        _checksum_binary BIGINT,
        _datasizemb NUMERIC(10,2),
        
        CONSTRAINT CHK_meta_object_type CHECK (_type IN ('api', 'db', 'db-large', 'entra', 'ldap', 'file', 'salesforce')),
        CONSTRAINT CHK_meta_object_status CHECK (_status IN ('active', 'inactive')),
        CONSTRAINT UQ_meta_object UNIQUE(_type, _name, _host, _catalog, _schema, _object)
    );

    -- Add index for common lookups
    CREATE INDEX IX_object_status_type ON dbo._object(_status, _type);
END
GO

-- 2. Ingestion Table
IF OBJECT_ID('dbo._ingestion', 'U') IS NULL
BEGIN
    CREATE TABLE dbo._ingestion (
        _id BIGINT IDENTITY(1,1) PRIMARY KEY,
        _object_id INT NOT NULL,
        _name NVARCHAR(256) NOT NULL,
        _filter NVARCHAR(MAX),
        _numrows BIGINT,
        _checksum BIGINT,
        _checksum_binary BIGINT,
        _status VARCHAR(10) NOT NULL,
        _statustime DATETIME2 DEFAULT SYSDATETIME(),
        _duration_seconds INT,
        
        CONSTRAINT CHK_meta_ingestion_status CHECK (_status IN ('pending', 'running', 'validating', 'failed', 'error', 'completed')),
        CONSTRAINT FK_ingestion_object FOREIGN KEY (_object_id) REFERENCES dbo._object(_id) ON DELETE CASCADE
    );

    -- Index for finding recent ingestions by object
    CREATE INDEX IX_ingestion_object_time ON dbo._ingestion(_object_id, _statustime DESC);
END
GO

--------------------------------------------------------------------------------
-- Tables for Validation Tracking
--------------------------------------------------------------------------------

-- 3. Validation Definition Table
IF OBJECT_ID('dbo._validation', 'U') IS NULL
BEGIN
    CREATE TABLE dbo._validation (
        -- _id INT IDENTITY(1,1) PRIMARY KEY,
        _id INT PRIMARY KEY,
        _name NVARCHAR(1024) NOT NULL,
        _type VARCHAR(32),
        _query_expected NVARCHAR(MAX),
        _query_actual NVARCHAR(MAX),
        _created DATETIME2 DEFAULT SYSDATETIME(),
        _modified DATETIME2 DEFAULT SYSDATETIME()
    );
END
GO

-- 4. Validation Run Table
IF OBJECT_ID('dbo._validation_run', 'U') IS NULL
BEGIN
    CREATE TABLE dbo._validation_run (
        _id BIGINT IDENTITY(1,1) PRIMARY KEY,
        _validation_id INT NOT NULL,
        _status VARCHAR(8) NOT NULL,
        _status_message NVARCHAR(MAX),
        _query_expected NVARCHAR(MAX),
        _query_actual NVARCHAR(MAX),
        _results_expected NVARCHAR(MAX),
        _results_actual NVARCHAR(MAX),
        _run_at DATETIME2 DEFAULT SYSDATETIME(),
        
        CONSTRAINT FK_validation_run_validation FOREIGN KEY (_validation_id) REFERENCES dbo._validation(_id) ON DELETE CASCADE
    );

    CREATE INDEX IX_validation_run_validation_time ON dbo._validation_run(_validation_id, _run_at DESC);
END
GO

--------------------------------------------------------------------------------
-- dbo.vwObjectFreshness
--
-- Core view that calculates the freshness and scheduling state of every
-- data object. Most other ingestion views and the candidate-selection
-- logic depend on this view.
--
-- For each row in dbo._object, the view exposes:
--
--   Last ingestion attempt (any status)
--     last_ingestion_status  – status of the most recent _ingestion row.
--     last_ingestion_time    – timestamp of that attempt.
--
--   Last successful ingestion
--     last_successful_ingestion_time – most recent 'completed' ingestion.
--
--   Schedule timestamps (computed from _frequency in hours)
--     next_due              – _refreshed + _frequency hours.
--     next_verify           – _verified  + _frequency hours.
--     next_verify_anchored  – next_verify but anchored to the time-of-day
--                             from _modified (so schedules stay pinned to
--                             a consistent daily slot).
--     now_plus_50           – CURRENT_TIMESTAMP + 50 minutes. Used as the
--                             look-ahead window by vwIngestionCandidates.
--
--   Reliability
--     consecutive_failures  – count of consecutive 'failed'/'error'
--                             ingestions since the last 'completed' one.
--                             Zero if the last attempt succeeded.
--
--   Freshness
--     freshness_hours / freshness_minutes – elapsed time since the most
--       recent trust point (the later of last successful ingestion or
--       manual verification via _verified). NULL if the object has never
--       been successfully ingested.
--
-- Implementation notes:
--   - Uses OUTER APPLY with TOP (1) ... ORDER BY _statustime DESC to
--     efficiently fetch the latest ingestion rows without a subquery
--     per column.
--   - The consecutive-failures count uses a correlated subquery to find
--     the MAX(_statustime) of 'completed' ingestions as the cutoff.
--   - CROSS APPLY is used for the schedule calculations so the
--     expressions can reference each other without repeating logic.
--
-- Change history:
-- 2025-12-11 JJJ Initial version.
--------------------------------------------------------------------------------
CREATE OR ALTER VIEW dbo.vwObjectFreshness AS
SELECT
    o._id,
    o._type,
    o._name,
    o._host,
    o._catalog,
    o._schema,
    o._object,
    o._frequency,
    -- Object state
    o._status,
    o._verified,
    o._refreshed,
    o._numrows,
    o._datasizemb,
    -- Latest ingestion attempt (any status)
    li._status    AS last_ingestion_status,
    li._statustime AS last_ingestion_time,
    -- Latest successful ingestion
    ls._statustime AS last_successful_ingestion_time,
    -- Next schedule times
    t.now_plus_50,
    t.next_due,
    t.next_verify,
    t.next_verify_anchored,
    -- Count of consecutive failures since last success
    COALESCE(cf.consecutive_failures, 0) AS consecutive_failures,
    -- Freshness in hours
    CASE
        WHEN ls._statustime IS NULL THEN NULL
        WHEN o._verified IS NOT NULL AND o._verified > ls._statustime
            THEN DATEDIFF(HOUR, o._verified, CURRENT_TIMESTAMP)
        ELSE DATEDIFF(HOUR, ls._statustime, CURRENT_TIMESTAMP)
    END AS freshness_hours,
    -- Freshness in minutes
    CASE
        WHEN ls._statustime IS NULL THEN NULL
        WHEN o._verified IS NOT NULL AND o._verified > ls._statustime
            THEN DATEDIFF(MINUTE, o._verified, CURRENT_TIMESTAMP)
        ELSE DATEDIFF(MINUTE, ls._statustime, CURRENT_TIMESTAMP)
    END AS freshness_minutes
FROM _meta.dbo._object o
-- Latest ingestion attempt (any status)
OUTER APPLY (
    SELECT TOP (1)
        i._status,
        i._statustime
    FROM _meta.dbo._ingestion i
    WHERE i._object_id = o._id
    ORDER BY i._statustime DESC
) li
-- Latest successful ingestion
OUTER APPLY (
    SELECT TOP (1)
        i._statustime
    FROM _meta.dbo._ingestion i
    WHERE i._object_id = o._id
      AND i._status = 'completed'
    ORDER BY i._statustime DESC
) ls
-- Count consecutive failures since last success
OUTER APPLY (
    SELECT COUNT(*) AS consecutive_failures
    FROM _meta.dbo._ingestion i
    WHERE i._object_id = o._id
      AND i._status IN ('failed', 'error')
      AND i._statustime > COALESCE(
          (
              SELECT MAX(i2._statustime)
              FROM _meta.dbo._ingestion i2
              WHERE i2._object_id = o._id
                AND i2._status = 'completed'
          ),
          '1900-01-01'  -- If no success ever, count all failures
      )
) cf
-- Next due / verify timestamps
CROSS APPLY (
    SELECT
        DATEADD(HOUR, o._frequency, COALESCE(o._refreshed, '1900-01-01')) AS next_due,
        DATEADD(HOUR, o._frequency, COALESCE(o._verified,  '1900-01-01')) AS next_verify,
        DATEADD(HOUR, o._frequency, COALESCE(
            CAST(CAST(o._verified AS date) AS datetime)
            + CAST(CAST(o._modified AS time(7)) AS datetime),
            '1900-01-01')) AS next_verify_anchored,
        DATEADD(MINUTE, 50, CURRENT_TIMESTAMP) AS now_plus_50
) t;
GO

--------------------------------------------------------------------------------
-- dbo.vwIngestionSchedule
--
-- Flattened schedule view that combines object metadata with the timing
-- and status fields from vwObjectFreshness. Intended for dashboards and
-- monitoring queries that need a quick overview of every object's
-- ingestion cadence and current state.
--
-- Key columns:
--   status – A composite label in the form "<_status>-<sub_status>" where
--            _status is 'active' or 'inactive', and sub_status is:
--              'new'       – never ingested and has no row count.
--              'empty'     – never ingested but _numrows = 0.
--              'verified'  – manually verified more recently than refreshed.
--              <ingestion> – the last_ingestion_status value otherwise
--                            (completed, failed, error, etc.).
--
--   Schedule timestamps (now_plus_50, next_due, next_verify,
--   next_verify_anchored) and freshness metrics are passed through
--   directly from vwObjectFreshness.
--
-- Change history:
-- 2025-12-17 JJJ Initial version.
--------------------------------------------------------------------------------
CREATE OR ALTER VIEW dbo.vwIngestionSchedule AS
SELECT
    f._id,
    f._type,
    f._name,
    f._frequency,
    f._numrows,
    f._datasizemb,
    CONCAT(f._status, '-',
        CASE
        WHEN f.last_ingestion_status IS NULL THEN
            CASE
            WHEN COALESCE(f._numrows, -1) = 0 THEN 'empty'
            ELSE 'new'
            END
        WHEN f._verified IS NOT NULL AND (f._refreshed IS NULL OR f._verified > f._refreshed) THEN 'verified'
        ELSE f.last_ingestion_status
        END
    ) AS status,
    f.last_ingestion_status,
    f.last_ingestion_time,
    f.last_successful_ingestion_time,
    f.now_plus_50,
    f.next_due,
    f.next_verify,
    f.next_verify_anchored,
    f.consecutive_failures,
    f.freshness_hours,
    f.freshness_minutes
FROM _meta.dbo.vwObjectFreshness f
GO

--------------------------------------------------------------------------------
-- dbo.vwIngestionCandidates
--
-- Returns the set of active data objects that are currently due for
-- ingestion. This is the view queried by the ingestion orchestrator to
-- decide which objects to process in the current cycle.
--
-- An object qualifies if ANY of the following conditions is true:
--
--   1. Never ingested
--      No prior ingestion attempts AND _refreshed is NULL AND the
--      anchored verification schedule falls within the 50-minute
--      look-ahead window (next_verify_anchored <= now_plus_50).
--
--   2. Last ingestion failed
--      The most recent attempt has status 'failed' or 'error' AND the
--      object has been verified at or before its last refresh (i.e. it
--      is not waiting on manual re-verification).
--
--   3. Scheduled verification due
--      The last ingestion succeeded ('completed') AND the anchored
--      verification schedule falls between now and the 50-minute
--      look-ahead window.
--
-- The status column uses the same composite label logic as
-- vwIngestionSchedule ("active-new", "active-completed", etc.).
--
-- All columns from dbo._object are included so the orchestrator has
-- everything it needs (connection details, key fields, filter, etc.)
-- without a second lookup.
--
-- Change history:
-- 2025-12-11 JJJ  Initial version.
--------------------------------------------------------------------------------
CREATE OR ALTER VIEW dbo.vwIngestionCandidates AS
SELECT
    o._id,
    o._type,
    o._name,
    o._host,
    o._catalog,
    o._schema,
    o._object,
    o._keyfields,
    o._filter,
    o._frequency,
    CONCAT(o._status, '-',
        CASE
        WHEN f.last_ingestion_status IS NULL THEN
            CASE
            WHEN COALESCE(o._numrows, -1) = 0 THEN 'empty'
            ELSE 'new'
            END
        WHEN o._verified IS NOT NULL AND (o._refreshed IS NULL OR o._verified > o._refreshed) THEN 'verified'
        ELSE f.last_ingestion_status
        END
    ) AS status,
    o._description,
    o._created,
    o._modified,
    o._verified,
    o._refreshed,
    o._numrows,
    o._checksum,
    o._checksum_binary,
    o._datasizemb
FROM _meta.dbo._object o
JOIN _meta.dbo.vwObjectFreshness f ON o._id = f._id
WHERE o._status = 'active'
AND
(
    (
        -- Never ingested
        f.last_ingestion_status IS NULL
        AND o._refreshed IS NULL
        AND f.next_verify_anchored <= f.now_plus_50
    )
    OR
    (
        -- Last ingestion failed
        f.last_ingestion_status IN ('failed', 'error')
        AND o._verified <= o._refreshed
    )
    OR
    (
        -- Scheduled verification due
        f.last_ingestion_status = 'completed'
        AND f.next_verify_anchored BETWEEN CURRENT_TIMESTAMP AND f.now_plus_50
    )
);
GO

--------------------------------------------------------------------------------
-- dbo.vwBronzeDatabases
--
-- Returns the distinct set of (host, catalog) pairs that represent
-- bronze-tier databases in the data platform. Used by provisioning and
-- monitoring scripts to enumerate which databases should exist on the
-- Datahub server.
--
-- Filters:
--   - Only active objects (_status = 'active') are considered.
--   - The 'Datahub' host is excluded because it is the destination,
--     not a bronze source.
--
-- Change history:
-- 2026-01-14 JJJ Initial version.
--------------------------------------------------------------------------------
CREATE OR ALTER VIEW dbo.vwBronzeDatabases AS
SELECT DISTINCT
    _host,
    _catalog
FROM _meta.dbo._object
WHERE _host != 'Datahub' AND _status = 'active';
GO

--------------------------------------------------------------------------------
-- dbo.spCreateBronzeDatabase
--
-- Creates a new bronze-tier database with standardised file paths and sizing.
--
-- Data file:  D:\Data\<DbName>.mdf
-- Log file:   L:\Log\<DbName>_log.ldf
--
-- Parameters:
--   @DbName      - Name of the database to create (required).
--   @DbMaxSizeMB - Maximum size of the data file in MB (default UNLIMITED via -1).
--   @LogMaxSizeMB - Maximum size of the log file in MB (default 102400 = 100 GB).
--
-- Notes:
--   - Uses dynamic SQL because CREATE DATABASE does not accept variables
--     for database name, file names, or file paths.
--   - The procedure will fail if the database already exists.
--   - Initial sizes and filegrowth are standardised and not parameterised
--     to enforce consistency across bronze databases.
--
-- Change history:
-- 2026-01-14 JJJ Initial version.
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.spCreateBronzeDatabase
    @DbName       SYSNAME,
    @DbMaxSizeMB  INT = -1,        -- -1 = UNLIMITED
    @LogMaxSizeMB INT = 102400      -- 100 GB
AS
BEGIN
    SET NOCOUNT ON;

    -- Fail fast if the database already exists
    IF DB_ID(@DbName) IS NOT NULL
    BEGIN
        RAISERROR('Database [%s] already exists.', 16, 1, @DbName);
        RETURN;
    END

    DECLARE @QDbName      SYSNAME = QUOTENAME(@DbName);
    DECLARE @DataFileName  NVARCHAR(256) = @DbName;
    DECLARE @LogFileName   NVARCHAR(256) = @DbName + N'_log';
    DECLARE @DataFilePath  NVARCHAR(512) = N'D:\Data\' + @DbName + N'.mdf';
    DECLARE @LogFilePath   NVARCHAR(512) = N'L:\Log\' + @DbName + N'_log.ldf';
    DECLARE @DbMaxSize     NVARCHAR(20);
    DECLARE @LogMaxSize    NVARCHAR(20);
    DECLARE @SQL           NVARCHAR(MAX);

    -- Resolve MAXSIZE values: -1 means UNLIMITED, otherwise use the MB value
    SET @DbMaxSize  = CASE WHEN @DbMaxSizeMB  = -1 THEN N'UNLIMITED' ELSE CAST(@DbMaxSizeMB  AS NVARCHAR(20)) + N'MB' END;
    SET @LogMaxSize = CASE WHEN @LogMaxSizeMB = -1 THEN N'UNLIMITED' ELSE CAST(@LogMaxSizeMB AS NVARCHAR(20)) + N'MB' END;

    SET @SQL = N'
CREATE DATABASE ' + @QDbName + N'
ON PRIMARY
(
    NAME = ' + QUOTENAME(@DataFileName) + N',
    FILENAME = ''' + @DataFilePath + N''',
    SIZE = 512MB,
    MAXSIZE = ' + @DbMaxSize + N',
    FILEGROWTH = 1024MB
)
LOG ON
(
    NAME = ' + QUOTENAME(@LogFileName) + N',
    FILENAME = ''' + @LogFilePath + N''',
    SIZE = 256MB,
    MAXSIZE = ' + @LogMaxSize + N',
    FILEGROWTH = 1024MB
);';

    PRINT @SQL;
    EXEC (@SQL);

    PRINT 'Database ' + @QDbName + ' created successfully.';
END
GO