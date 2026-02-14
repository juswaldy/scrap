USE _meta;
GO

--------------------------------------------------------------------------------
-- dbo.spShrinkDbLog
--
-- Shrinks the transaction log file of a single database to a target size.
--
-- How it works:
--   1. Validates that the database exists and has a log file.
--   2. Records the database's current recovery model.
--   3. Temporarily switches the recovery model to SIMPLE (if it isn't already)
--      so that the log can be truncated and freed for shrinking.
--   4. Runs DBCC SHRINKFILE to reduce the log file to @TargetSizeMB.
--   5. Restores the original recovery model, even if an error occurs.
--
-- Parameters:
--   @DbName       - Name of the target database (required).
--   @TargetSizeMB - Desired log file size in MB after shrinking (default 16).
--
-- Notes:
--   - Switching away from FULL recovery breaks the log chain. A new full or
--     differential backup should be taken afterward to re-establish it.
--   - The procedure uses dynamic SQL because DBCC SHRINKFILE and ALTER DATABASE
--     must execute in the context of the target database.
--   - The recovery model is restored in the CATCH block to minimise the window
--     during which the database is in SIMPLE recovery.
--   - If multiple log files exist, only the first one returned by
--     sys.master_files is shrunk.
--
-- Change history:
-- 2025-12-01 JJJ Initial version.
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.spShrinkDbLog
    @DbName       SYSNAME,
    @TargetSizeMB INT = 16
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL        NVARCHAR(MAX);
    DECLARE @LogFile    SYSNAME;
    DECLARE @DbId       INT;
    DECLARE @QDbName    SYSNAME;          -- Bracket-quoted database name for safe SQL injection
    DECLARE @OriginalRecovery NVARCHAR(60);  -- Original recovery model to restore later

    -- Resolve the database ID; fail fast if it doesn't exist
    SET @DbId = DB_ID(@DbName);
    IF @DbId IS NULL
    BEGIN
        RAISERROR('Database %s does not exist.', 16, 1, @DbName);
        RETURN;
    END

    SET @QDbName = QUOTENAME(@DbName);

    -- Look up the logical name of the log file from sys.master_files.
    -- This name is needed by DBCC SHRINKFILE.
    SELECT @LogFile = mf.name
    FROM sys.master_files mf
    WHERE mf.database_id = @DbId AND mf.type_desc = 'LOG';

    IF @LogFile IS NULL
    BEGIN
        RAISERROR('Could not find a log file for database %s.', 16, 1, @DbName);
        RETURN;
    END

    -- Capture the current recovery model so we can restore it after shrinking.
    -- Possible values: FULL, BULK_LOGGED, SIMPLE.
    SELECT @OriginalRecovery = recovery_model_desc 
    FROM sys.databases 
    WHERE database_id = @DbId;

    PRINT '============================================================';
    PRINT 'Starting log shrink for ' + @QDbName;
    PRINT 'Original Recovery: ' + @OriginalRecovery;

    -- Build dynamic SQL that:
    --   a) Switches context to the target database (required by DBCC SHRINKFILE).
    --   b) Temporarily sets recovery to SIMPLE to allow the log to be truncated.
    --   c) Shrinks the log file to the requested size.
    --   d) Restores the original recovery model.
    -- The entire block is wrapped in TRY/CATCH so the recovery model is
    -- restored even if the shrink fails.
    SET @SQL = N'
    USE ' + @QDbName + ';
    BEGIN TRY
        -- Only switch recovery model if not already SIMPLE
        IF ''' + @OriginalRecovery + ''' <> ''SIMPLE''
        BEGIN
            PRINT ''Temporarily setting recovery to SIMPLE...'';
            ALTER DATABASE ' + @QDbName + ' SET RECOVERY SIMPLE;
        END

        PRINT ''Shrinking log file [' + @LogFile + ']...'';
        DBCC SHRINKFILE (N''' + @LogFile + ''', ' + CONVERT(varchar(20), @TargetSizeMB) + N');

        -- Restore original recovery model
        IF ''' + @OriginalRecovery + ''' <> ''SIMPLE''
        BEGIN
            PRINT ''Restoring recovery model to ' + @OriginalRecovery + '...'';
            ALTER DATABASE ' + @QDbName + ' SET RECOVERY ' + @OriginalRecovery + ';
        END

        PRINT ''COMPLETED successfully.'';
    END TRY
    BEGIN CATCH
        PRINT ''ERROR: '' + ERROR_MESSAGE();
        -- Attempt to restore recovery model even on error so the database
        -- is not left in SIMPLE recovery unintentionally.
        IF ''' + @OriginalRecovery + ''' <> ''SIMPLE''
        BEGIN
            ALTER DATABASE ' + @QDbName + ' SET RECOVERY ' + @OriginalRecovery + ';
        END
    END CATCH;';

    EXEC (@SQL);
END
GO

--------------------------------------------------------------------------------
-- dbo.spShrinkAllDbLogs
--
-- Iterates over every eligible user database on the server and calls
-- spShrinkDbLog for each one, shrinking all transaction logs to a uniform
-- target size.
--
-- Eligibility criteria (databases that are skipped):
--   - System databases (database_id <= 4: master, tempdb, model, msdb).
--   - Databases that are not ONLINE (e.g. restoring, offline, suspect).
--   - Read-only databases (DBCC SHRINKFILE requires write access).
--
-- Parameters:
--   @TargetSizeMB - Desired log file size in MB for every database (default 16).
--                   Passed through to spShrinkDbLog for each database.
--
-- Notes:
--   - Uses a LOCAL FAST_FORWARD cursor for efficient, read-only iteration.
--   - Errors in one database do not stop processing of the remaining databases
--     because spShrinkDbLog handles errors internally with TRY/CATCH.
--   - Progress and errors are printed to the messages tab via spShrinkDbLog.
--
-- Change history:
-- 2025-12-01 JJJ Initial version.
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.spShrinkAllDbLogs
    @TargetSizeMB INT = 16
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @DbName SYSNAME;

    -- Cursor selects only user databases (id > 4) that are writable and online.
    DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT name
        FROM sys.databases
        WHERE database_id > 4      -- Exclude system databases
          AND state_desc = 'ONLINE' -- Must be accessible
          AND is_read_only = 0;     -- Must be writable for DBCC SHRINKFILE

    OPEN db_cursor;
    FETCH NEXT FROM db_cursor INTO @DbName;

    -- Process each database sequentially; spShrinkDbLog handles its own errors.
    WHILE @@FETCH_STATUS = 0
    BEGIN
        EXEC _meta.dbo.spShrinkDbLog @DbName = @DbName, @TargetSizeMB = @TargetSizeMB;
        FETCH NEXT FROM db_cursor INTO @DbName;
    END

    CLOSE db_cursor;
    DEALLOCATE db_cursor;
END
GO

--------------------------------------------------------------------------------
-- Code Signing for the above procedures
--
-- The procedures live in _meta and use dynamic SQL to ALTER DATABASE and
-- DBCC SHRINKFILE across all user databases. This requires server-level
-- permissions. The approach:
--
--   1. Create certificate in _meta (same DB as the procs) and sign them.
--   2. Copy the certificate (public key only) to master.
--   3. Create a LOGIN from the certificate in master.
--   4. Grant ALTER ANY DATABASE to the login (covers SET RECOVERY and
--      DBCC SHRINKFILE in any database).
--   5. Grant EXECUTE on both procs to IntegrationRole.
--
-- ALTER ANY DATABASE is the least-privilege server-level permission that
-- covers both ALTER DATABASE ... SET RECOVERY and DBCC SHRINKFILE.
--------------------------------------------------------------------------------

-- Step 1: Create the certificate in _meta and sign both procedures
USE _meta;
GO

IF NOT EXISTS (SELECT 1 FROM sys.certificates WHERE name = N'cert_spShrinkDbLog')
BEGIN
    CREATE CERTIFICATE cert_spShrinkDbLog
        ENCRYPTION BY PASSWORD = '<strong_password_here>'
        WITH SUBJECT = 'Certificate for spShrinkDbLog proc signing';
END
GO

ADD SIGNATURE TO _meta.dbo.spShrinkDbLog
    BY CERTIFICATE cert_spShrinkDbLog
    WITH PASSWORD = '<strong_password_here>';
GO

ADD SIGNATURE TO _meta.dbo.spShrinkAllDbLogs
    BY CERTIFICATE cert_spShrinkDbLog
    WITH PASSWORD = '<strong_password_here>';
GO

-- Step 2: Back up the certificate (public key only) to disk, then restore
-- it into master. This lets us create a server-level login from it.
BACKUP CERTIFICATE cert_spShrinkDbLog
    TO FILE = 'T:\cert_spShrinkDbLog.cer';
GO

USE master;
GO

IF NOT EXISTS (SELECT 1 FROM sys.certificates WHERE name = N'cert_spShrinkDbLog')
BEGIN
    CREATE CERTIFICATE cert_spShrinkDbLog
        FROM FILE = 'T:\cert_spShrinkDbLog.cer';
END
GO

-- Step 3: Create a login from the certificate in master.
-- This login is the server-level security principal that inherits
-- permissions when the signed procedures execute.
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'certlogin_spShrinkDbLog')
BEGIN
    CREATE LOGIN certlogin_spShrinkDbLog FROM CERTIFICATE cert_spShrinkDbLog;
END
GO

-- Step 4: Grant the login ALTER ANY DATABASE.
-- This single permission covers:
--   - ALTER DATABASE ... SET RECOVERY (SIMPLE / FULL / BULK_LOGGED)
--   - DBCC SHRINKFILE in any database
GRANT ALTER ANY DATABASE TO certlogin_spShrinkDbLog;
GO

-- Step 5: Grant EXECUTE on the procedures to IntegrationRole
USE _meta;
GO

GRANT EXECUTE ON dbo.spShrinkDbLog TO IntegrationRole;
GRANT EXECUTE ON dbo.spShrinkAllDbLogs TO IntegrationRole;
GO

-- Clean up the temporary certificate file
-- (run in PowerShell or cmd after deployment):
-- Remove-Item T:\cert_spShrinkDbLog.cer

--------------------------------------------------------------------------------
-- Undo Code Signing
--
-- Run these steps to completely reverse the code-signing setup above.
-- Execute in order from most-dependent to least-dependent.
--------------------------------------------------------------------------------

-- 1. Revoke EXECUTE from IntegrationRole
USE _meta;
GO

REVOKE EXECUTE ON dbo.spShrinkDbLog FROM IntegrationRole;
REVOKE EXECUTE ON dbo.spShrinkAllDbLogs FROM IntegrationRole;
GO

-- 2. Remove signatures from both procedures (in _meta)
DROP SIGNATURE FROM _meta.dbo.spShrinkDbLog
    BY CERTIFICATE cert_spShrinkDbLog;
GO

DROP SIGNATURE FROM _meta.dbo.spShrinkAllDbLogs
    BY CERTIFICATE cert_spShrinkDbLog;
GO

-- 3. Drop the certificate from _meta
DROP CERTIFICATE IF EXISTS cert_spShrinkDbLog;
GO

-- 4. Revoke server-level permission and drop the login (in master)
USE master;
GO

REVOKE ALTER ANY DATABASE FROM certlogin_spShrinkDbLog;
DROP LOGIN certlogin_spShrinkDbLog;
GO

-- 5. Drop the certificate from master
DROP CERTIFICATE IF EXISTS cert_spShrinkDbLog;
GO