USE _meta;
GO

SET NOCOUNT ON;
BEGIN TRANSACTION;

BEGIN TRY
    ----------------------------------------------------------------------------
    -- 1. Rename existing tables to backup
    ----------------------------------------------------------------------------
    IF OBJECT_ID('dbo._ingestion', 'U') IS NOT NULL AND OBJECT_ID('dbo._ingestion_old', 'U') IS NULL
    BEGIN
        EXEC sp_rename 'dbo._ingestion', '_ingestion_old';
        PRINT 'Renamed _ingestion to _ingestion_old';
    END

    IF OBJECT_ID('dbo._object', 'U') IS NOT NULL AND OBJECT_ID('dbo._object_old', 'U') IS NULL
    BEGIN
        EXEC sp_rename 'dbo._object', '_object_old';
        PRINT 'Renamed _object to _object_old';
    END

    ----------------------------------------------------------------------------
    -- 2. Drop conflicting constraints from backup tables
    --    (Required because sp_rename does not rename constraints)
    ----------------------------------------------------------------------------
    
    -- Clean up _ingestion_old constraints
    IF OBJECT_ID('dbo._ingestion_old', 'U') IS NOT NULL
    BEGIN
        -- Drop Foreign Key
        IF OBJECT_ID('dbo.FK_ingestion_object', 'F') IS NOT NULL 
            ALTER TABLE dbo._ingestion_old DROP CONSTRAINT FK_ingestion_object;
            
        -- Drop Check Constraint
        IF OBJECT_ID('dbo.CHK_meta_ingestion_status', 'C') IS NOT NULL 
            ALTER TABLE dbo._ingestion_old DROP CONSTRAINT CHK_meta_ingestion_status;
            
        PRINT 'Dropped constraints from _ingestion_old';
    END

    -- Clean up _object_old constraints
    IF OBJECT_ID('dbo._object_old', 'U') IS NOT NULL
    BEGIN
        -- Drop Unique Constraint
        IF OBJECT_ID('dbo.UQ_meta_object', 'UQ') IS NOT NULL 
            ALTER TABLE dbo._object_old DROP CONSTRAINT UQ_meta_object;

        -- Drop Check Constraints
        IF OBJECT_ID('dbo.CHK_meta_object_type', 'C') IS NOT NULL 
            ALTER TABLE dbo._object_old DROP CONSTRAINT CHK_meta_object_type;
            
        IF OBJECT_ID('dbo.CHK_meta_object_status', 'C') IS NOT NULL 
            ALTER TABLE dbo._object_old DROP CONSTRAINT CHK_meta_object_status;

        PRINT 'Dropped constraints from _object_old';
    END

    ----------------------------------------------------------------------------
    -- 3. Create new tables
    ----------------------------------------------------------------------------
    
    -- Object Table
    IF OBJECT_ID('dbo._object', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo._object (
            _id INT IDENTITY(1,1) PRIMARY KEY,
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

        CREATE INDEX IX_object_status_type ON dbo._object(_status, _type);
        PRINT 'Created new _object table';
    END

    -- Ingestion Table
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

        CREATE INDEX IX_ingestion_object_time ON dbo._ingestion(_object_id, _statustime DESC);
        PRINT 'Created new _ingestion table';
    END

    ----------------------------------------------------------------------------
    -- 3. Migrate Data
    ----------------------------------------------------------------------------
    
    -- Migrate _object
    IF OBJECT_ID('dbo._object_old', 'U') IS NOT NULL
    BEGIN
        -- Enable IDENTITY_INSERT to preserve existing ingestion IDs
        SET IDENTITY_INSERT dbo._object ON;
        
        INSERT INTO dbo._object (
            _id, _type, _name, _host, _catalog, _schema, _object, 
            _keyfields, _filter, _frequency, _status, _description, 
            _created, _modified, _verified, _refreshed, 
            _numrows, _checksum, _checksum_binary, _datasizemb
        )
        SELECT 
            _id, 
            _type, 
            _name, 
            _host, 
            _catalog, 
            _schema, 
            _object, 
            _keyfields, 
            _filter, 
            _frequency, 
            -- Validate status against new constraint
            CASE WHEN _status IN ('active', 'inactive') THEN _status ELSE 'inactive' END, 
            _description, 
            _created, 
            _modified, 
            _verified, 
            _refreshed, 
            _numrows, 
            _checksum, 
            _checksum_binary, 
            _datasizemb
        FROM dbo._object_old;
        
        PRINT 'Migrated ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows to _object';
        
        SET IDENTITY_INSERT dbo._object OFF;
    END

    -- Migrate _ingestion
    IF OBJECT_ID('dbo._ingestion_old', 'U') IS NOT NULL
    BEGIN
        -- Enable IDENTITY_INSERT to preserve existing ingestion IDs
        SET IDENTITY_INSERT dbo._ingestion ON;
        
        INSERT INTO dbo._ingestion (
            _id, _object_id, _name, _filter, _numrows, 
            _checksum, _checksum_binary, _status, _statustime, _duration_seconds
        )
        SELECT 
            _id, 
            _object_id, 
            _name, 
            _filter, 
            _numrows, 
            _checksum, 
            _checksum_binary, 
            -- Validate status against new constraint
            CASE WHEN _status IN ('pending', 'running', 'validating', 'failed', 'error', 'completed') THEN _status ELSE 'error' END, 
            _statustime, 
            _duration_seconds
        FROM dbo._ingestion_old;
        
        PRINT 'Migrated ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows to _ingestion';
        
        SET IDENTITY_INSERT dbo._ingestion OFF;
END

COMMIT TRANSACTION;
PRINT 'Migration completed successfully.';

END TRY
BEGIN CATCH
IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
PRINT 'Error during migration: ' + ERROR_MESSAGE();
END CATCH;
GO