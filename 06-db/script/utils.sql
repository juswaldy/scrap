USE _meta;
GO

--------------------------------------------------------------------------------
-- dbo.spSplitTable
--
-- Generates and executes a SELECT statement that returns one "chunk" of a
-- table's data, enabling parallel or incremental extraction by dividing the
-- table either by columns or by rows.
--
-- Modes:
--   COLUMN split (@SplitBy = 'column')
--     Divides the table's non-key columns into @NumChunks roughly equal
--     groups. Each chunk's SELECT always includes the primary-key / unique-
--     index columns so results can be reassembled by joining on the key.
--     Useful when a table is very wide and you want to extract subsets of
--     columns independently.
--
--   ROW split (@SplitBy = 'row')
--     Assigns every row a bucket number (1..@NumChunks) using NTILE() over
--     a deterministic ORDER BY expression, then returns only the rows in
--     bucket @ChunkToRun. Useful for parallel full-table extraction across
--     multiple workers.
--
-- Parameters:
--   @TableName  - Fully or partially qualified table name: 'db.schema.table',
--                 'schema.table', or 'table' (defaults to current DB, dbo).
--   @NumChunks  - Total number of chunks to divide into (>= 1, required).
--   @ChunkToRun - Which chunk to execute (1..@NumChunks, required).
--   @SplitBy    - Split strategy: 'column' or 'row' (default 'column').
--   @SplitKey   - (Row mode only) Column name or ORDER BY expression for
--                 NTILE bucketing. If NULL or empty, the procedure auto-
--                 derives a key from the table's primary key or first
--                 unique non-filtered index.
--
-- Behaviour:
--   1. Parses the table name with PARSENAME and defaults missing parts.
--   2. Queries the target database's catalog views (sys.indexes,
--      sys.index_columns, sys.columns) to derive a deterministic key
--      from the PK or best unique index.
--   3. Builds the SQL for every chunk and stores it in a table variable.
--   4. Prints all chunk SQL to the messages tab (using RAISERROR/NOWAIT
--      to avoid PRINT's 8000-char truncation).
--   5. Executes only the chunk identified by @ChunkToRun.
--
-- Column-mode details:
--   - Key columns are determined from the derived key and included in
--     every chunk's SELECT so the results can be joined back together.
--   - Non-key columns are distributed across chunks as evenly as possible
--     (first @rem chunks get one extra column).
--   - Fails if the table has no PK/unique index to derive key columns, if
--     all columns are key columns, or if @NumChunks exceeds the non-key
--     column count.
--
-- Row-mode details:
--   - If @SplitKey is a simple identifier (letters, digits, underscores),
--     it is validated against the table's column list and bracket-quoted.
--   - If @SplitKey contains special characters (e.g. expressions like
--     'Col1, Col2'), it is used as-is in the ORDER BY clause.
--   - If @SplitKey is NULL, the procedure falls back to the auto-derived
--     PK/unique key (same logic as column mode).
--
-- Notes:
--   - All catalog queries use dynamic SQL with sp_executesql to run in the
--     context of the target database, so the procedure works cross-database.
--   - The procedure does not modify any data; it is read-only.
--   - NTILE distribution is approximate — chunks may differ by one row.
--
-- Examples:
--   -- Column split: 4 chunks, run chunk 2
--   EXEC dbo.spSplitTable 'OTR.dbo.Students', 4, 2, 'column';
--
--   -- Row split: 3 chunks, run chunk 1, auto-derive key from PK
--   EXEC dbo.spSplitTable 'OTR.dbo.Students', 3, 1, 'row';
--
--   -- Row split with explicit split key
--   EXEC dbo.spSplitTable 'OTR.dbo.Students', 3, 1, 'row', 'StudentID';
--
-- Change history:
-- 2026-01-09 JJJ Initial version.
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[spSplitTable]
(
    @TableName   NVARCHAR(776) = NULL,       -- 'db.schema.table' OR 'schema.table'
    @NumChunks   INT = NULL,
    @ChunkToRun  INT = NULL,
    @SplitBy     NVARCHAR(10) = N'column',   -- 'column' or 'row'
    @SplitKey    NVARCHAR(512) = NULL        -- optional: row split ORDER BY key/expression; auto-derived if NULL/empty
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @TableName IS NULL OR LTRIM(RTRIM(@TableName)) = N''
        THROW 50000, 'Usage: EXEC dbo.spSplitTable ''db.schema.table'', @numchunks, @selectedchunk  [, @splitby] [, @splitkey]', 1;

    IF @NumChunks IS NULL OR @NumChunks < 1
        THROW 50000, '@NumChunks must be >= 1.', 1;

    IF @ChunkToRun IS NULL OR @ChunkToRun < 1 OR @ChunkToRun > @NumChunks
        THROW 50000, '@ChunkToRun must be between 1 and @NumChunks.', 1;

    SET @SplitBy = LOWER(LTRIM(RTRIM(@SplitBy)));
    IF @SplitBy NOT IN (N'column', N'row')
        THROW 50000, '@SplitBy must be ''column'' or ''row''.', 1;

    -- Parse db.schema.table (db optional, schema optional -> dbo)
    DECLARE
        @db     SYSNAME = PARSENAME(@TableName, 3),
        @schema SYSNAME = PARSENAME(@TableName, 2),
        @table  SYSNAME = PARSENAME(@TableName, 1);

    IF @table IS NULL
        THROW 50000, '@TableName must be at least ''schema.table'' (db optional).', 1;

    IF @schema IS NULL SET @schema = N'dbo';
    IF @db IS NULL     SET @db = DB_NAME();

    DECLARE
        @QDb       NVARCHAR(260) = QUOTENAME(@db),
        @QSchema   NVARCHAR(260) = QUOTENAME(@schema),
        @QTable    NVARCHAR(260) = QUOTENAME(@table),
        @ThreePart NVARCHAR(900) = QUOTENAME(@db) + N'.' + QUOTENAME(@schema) + N'.' + QUOTENAME(@table);

    DECLARE @AllSql TABLE
    (
        ChunkNo INT NOT NULL PRIMARY KEY,
        SqlText NVARCHAR(MAX) NOT NULL
    );

    -------------------------------------------------------------------------
    -- Helper: derive a deterministic key ORDER BY expr from PK/unique index
    -------------------------------------------------------------------------
    DECLARE @DerivedKeyOrderBy NVARCHAR(MAX) = NULL;

    DECLARE @deriveKeySql NVARCHAR(MAX) =
N';WITH tgt AS
(
    SELECT o.object_id
    FROM ' + @QDb + N'.sys.objects o
    INNER JOIN ' + @QDb + N'.sys.schemas s ON o.schema_id = s.schema_id
    WHERE o.type = ''U'' AND s.name = @schema AND o.name = @table
),
cands AS
(
    SELECT
        i.object_id,
        i.index_id,
        pref =
            CASE
                WHEN i.is_primary_key = 1 THEN 1
                WHEN i.is_unique_constraint = 1 THEN 2
                WHEN i.is_unique = 1 THEN 3
                ELSE 99
            END
    FROM ' + @QDb + N'.sys.indexes i
    INNER JOIN tgt ON tgt.object_id = i.object_id
    WHERE i.index_id > 0
      AND i.is_hypothetical = 0
      AND i.has_filter = 0
      AND (i.is_primary_key = 1 OR i.is_unique_constraint = 1 OR i.is_unique = 1)
),
pick AS
(
    SELECT TOP (1) *
    FROM cands
    ORDER BY pref, index_id
)
SELECT
    @expr =
        STRING_AGG(QUOTENAME(col.name), N'', '') WITHIN GROUP (ORDER BY ic.key_ordinal),
    @found = CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
FROM pick p
INNER JOIN ' + @QDb + N'.sys.index_columns ic
    ON ic.object_id = p.object_id AND ic.index_id = p.index_id
INNER JOIN ' + @QDb + N'.sys.columns col
    ON col.object_id = ic.object_id AND col.column_id = ic.column_id
WHERE ic.is_included_column = 0
  AND ic.key_ordinal > 0;';

    DECLARE @kFound BIT = 0, @kExpr NVARCHAR(MAX) = NULL;

    EXEC sp_executesql
        @deriveKeySql,
        N'@schema SYSNAME, @table SYSNAME, @found BIT OUTPUT, @expr NVARCHAR(MAX) OUTPUT',
        @schema = @schema, @table = @table, @found = @kFound OUTPUT, @expr = @kExpr OUTPUT;

    IF @kFound = 1 AND @kExpr IS NOT NULL
        SET @DerivedKeyOrderBy = @kExpr;

    -------------------------------------------------------------------------
    -- COLUMN SPLIT (include key cols in every chunk)
    -------------------------------------------------------------------------
    IF @SplitBy = N'column'
    BEGIN
        IF @DerivedKeyOrderBy IS NULL
            THROW 50000, 'Column mode now includes key columns in every chunk, but the table has no PRIMARY KEY or UNIQUE (non-filtered) index/constraint to derive them.', 1;

        CREATE TABLE #Cols (column_id INT NOT NULL, name SYSNAME NOT NULL);

        DECLARE @colsSql NVARCHAR(MAX) =
N'INSERT INTO #Cols(column_id, name)
  SELECT c.column_id, c.name
  FROM ' + @QDb + N'.sys.columns c
  INNER JOIN ' + @QDb + N'.sys.objects o ON c.object_id = o.object_id
  INNER JOIN ' + @QDb + N'.sys.schemas s ON o.schema_id = s.schema_id
  WHERE o.type = ''U''
    AND s.name = @schema
    AND o.name = @table
  ORDER BY c.column_id;';

        EXEC sp_executesql
            @colsSql,
            N'@schema SYSNAME, @table SYSNAME',
            @schema = @schema, @table = @table;

        DECLARE @TotalCols INT = (SELECT COUNT(*) FROM #Cols);
        IF @TotalCols = 0
            THROW 50000, 'Table not found or has 0 columns (check db/schema/table).', 1;

        -- Key columns (list like: [Id], [OtherId])
        DECLARE @KeySelect NVARCHAR(MAX) = @DerivedKeyOrderBy;

        -- Exclude key columns from the chunked set so they don't duplicate
        CREATE TABLE #NonKeyCols (column_id INT NOT NULL, name SYSNAME NOT NULL);

        DECLARE @nonKeySql NVARCHAR(MAX) =
N'INSERT INTO #NonKeyCols(column_id, name)
  SELECT c.column_id, c.name
  FROM #Cols c
  WHERE c.name NOT IN (
      SELECT REPLACE(REPLACE(value, ''['', ''''), '']'', '''')
      FROM STRING_SPLIT(@KeyColsCsv, '','')
  );';

        -- Convert "[A], [B]" -> "A,B" for STRING_SPLIT
        DECLARE @KeyColsCsv NVARCHAR(MAX) =
            REPLACE(REPLACE(@KeySelect, N'[', N''), N']', N'');

        EXEC sp_executesql
            @nonKeySql,
            N'@KeyColsCsv NVARCHAR(MAX)',
            @KeyColsCsv = @KeyColsCsv;

        DECLARE @TotalNonKey INT = (SELECT COUNT(*) FROM #NonKeyCols);

        IF @TotalNonKey = 0
            THROW 50000, 'All columns are key columns; nothing left to chunk.', 1;

        IF @NumChunks > @TotalNonKey
            THROW 50000, 'In column mode, @NumChunks cannot exceed the number of NON-key columns (key columns are included in every chunk).', 1;

        DECLARE @base INT = @TotalNonKey / @NumChunks,
                @rem  INT = @TotalNonKey % @NumChunks;

        ;WITH ColsRN AS
        (
            SELECT c.column_id, c.name, rn = ROW_NUMBER() OVER (ORDER BY c.column_id)
            FROM #NonKeyCols c
        ),
        Chunks AS
        (
            SELECT
                ChunkNo = v.n,
                ChunkSize = @base + CASE WHEN v.n <= @rem THEN 1 ELSE 0 END
            FROM (SELECT TOP (@NumChunks) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
                  FROM sys.all_objects) v
        ),
        Bounds AS
        (
            SELECT
                ChunkNo,
                StartPos = 1 + COALESCE(
                              SUM(ChunkSize) OVER (ORDER BY ChunkNo ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                              0
                          ),
                EndPos = SUM(ChunkSize) OVER (ORDER BY ChunkNo ROWS UNBOUNDED PRECEDING)
            FROM Chunks
        )
        INSERT INTO @AllSql(ChunkNo, SqlText)
        SELECT
            b.ChunkNo,
            N'SELECT ' + @KeySelect + N', ' +
            (
                SELECT STRING_AGG(QUOTENAME(cr.name), N', ') WITHIN GROUP (ORDER BY cr.column_id)
                FROM ColsRN cr
                WHERE cr.rn BETWEEN b.StartPos AND b.EndPos
            ) +
            N' FROM ' + @ThreePart + N';'
        FROM Bounds b;

        DROP TABLE #NonKeyCols;
        DROP TABLE #Cols;
    END
    -------------------------------------------------------------------------
    -- ROW SPLIT (auto-derive SplitKey from PK/unique index if not provided)
    -------------------------------------------------------------------------
    ELSE
    BEGIN
        SET @SplitKey = NULLIF(LTRIM(RTRIM(@SplitKey)), N'');

        DECLARE @OrderByExpr NVARCHAR(MAX);

        IF @SplitKey IS NULL
        BEGIN
            IF @DerivedKeyOrderBy IS NULL
                THROW 50000,
                    'Row mode requires @SplitKey, or the table must have a PRIMARY KEY or UNIQUE (non-filtered) index/constraint to derive one.',
                    1;

            -- Use derived key for ORDER BY (stable)
            SET @OrderByExpr = @DerivedKeyOrderBy;
            RAISERROR(N'Using derived split key: ORDER BY %s', 0, 1, @OrderByExpr) WITH NOWAIT;
        END
        ELSE
        BEGIN
            -- simple identifier? validate + quote. otherwise treat as expression.
            DECLARE @IsSimple BIT =
                CASE WHEN @SplitKey LIKE N'%[^A-Za-z0-9_]%' THEN 0 ELSE 1 END;

            IF @IsSimple = 1
            BEGIN
                DECLARE @colExistsSql NVARCHAR(MAX) =
N'SELECT @exists = CASE WHEN EXISTS
(
    SELECT 1
    FROM ' + @QDb + N'.sys.columns c
    INNER JOIN ' + @QDb + N'.sys.objects o ON c.object_id = o.object_id
    INNER JOIN ' + @QDb + N'.sys.schemas s ON o.schema_id = s.schema_id
    WHERE o.type = ''U''
      AND s.name = @schema
      AND o.name = @table
      AND c.name = @col
) THEN 1 ELSE 0 END;';

                DECLARE @exists BIT = 0;

                EXEC sp_executesql
                    @colExistsSql,
                    N'@schema SYSNAME, @table SYSNAME, @col SYSNAME, @exists BIT OUTPUT',
                    @schema=@schema, @table=@table, @col=@SplitKey, @exists=@exists OUTPUT;

                IF @exists = 0
                    THROW 50000, '@SplitKey does not match a column name in the target table.', 1;

                SET @OrderByExpr = QUOTENAME(@SplitKey);
            END
            ELSE
            BEGIN
                SET @OrderByExpr = @SplitKey;
            END
        END

        ;WITH Chunks AS
        (
            SELECT v.n AS ChunkNo
            FROM (SELECT TOP (@NumChunks) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
                  FROM sys.all_objects) v
        )
        INSERT INTO @AllSql(ChunkNo, SqlText)
        SELECT
            ChunkNo,
N';WITH __src AS
(
    SELECT t.*,
           NTILE(' + CAST(@NumChunks AS NVARCHAR(20)) + N') OVER (ORDER BY ' + @OrderByExpr + N') AS __chunk
    FROM ' + @ThreePart + N' AS t
)
SELECT *
FROM __src
WHERE __chunk = ' + CAST(ChunkNo AS NVARCHAR(20)) + N';'
        FROM Chunks;
    END

    -------------------------------------------------------------------------
    -- Print all chunk SQL (RAISERROR avoids PRINT truncation issues)
    -------------------------------------------------------------------------
    DECLARE @i INT = 1, @sql NVARCHAR(MAX), @pos INT, @slice NVARCHAR(4000);

    WHILE @i <= @NumChunks
    BEGIN
        SELECT @sql = SqlText FROM @AllSql WHERE ChunkNo = @i;

        RAISERROR(N'----- Chunk %d / %d (%s) -----', 0, 1, @i, @NumChunks, @SplitBy) WITH NOWAIT;

        SET @pos = 1;
        WHILE @pos <= LEN(@sql)
        BEGIN
            SET @slice = SUBSTRING(@sql, @pos, 4000);
            RAISERROR(N'%s', 0, 1, @slice) WITH NOWAIT;
            SET @pos += 4000;
        END

        SET @i += 1;
    END

    -------------------------------------------------------------------------
    -- Execute requested chunk
    -------------------------------------------------------------------------
    SELECT @sql = SqlText FROM @AllSql WHERE ChunkNo = @ChunkToRun;

    RAISERROR(N'===== Executing chunk %d (%s) =====', 0, 1, @ChunkToRun, @SplitBy) WITH NOWAIT;
    EXEC sp_executesql @sql;
END
GO

--------------------------------------------------------------------------------
-- dbo.spCompareTables
--
-- Performs a full-outer-join diff of two tables (or views / CTEs) on a
-- caller-supplied key, producing two complementary report tables stored
-- as GLOBAL temp tables (##DiffList_… / ##DiffWide_…) so they survive
-- the procedure boundary and can be queried interactively.
--
-- Report formats:
--
--   WIDE  (##DiffWide_<suffix>)
--     One row per changed, added, or deleted key. Contains the key
--     columns, a _ChangeType column ('Added' / 'Deleted' / 'Changed'),
--     and one NVARCHAR(MAX) column per non-key column. Changed cells
--     show "prev <--> curr"; unchanged cells are blank ('').
--     Convenient for spreadsheet-style review of row-level changes.
--
--   LIST  (##DiffList_<suffix>)
--     One row per changed *cell*. Contains the key columns,
--     _ChangeType, ChangedColumn (SYSNAME), PreviousValue and
--     CurrentValue (both NVARCHAR(MAX)). Produced by unpivoting the
--     wide table, so only non-blank diffs appear. Convenient for
--     programmatic processing, aggregation, or filtering by column.
--
-- Parameters:
--   @keyfields          - Comma-separated list of key column names that
--                         uniquely identify a row across both tables.
--                         Example: N'YR_CDE, TRM_CDE, CRS_CDE'
--   @prevtable          - Fully or partially qualified name of the
--                         "before" table (may include brackets for names
--                         with dots/hyphens, e.g. backup snapshots).
--   @currtable          - Fully or partially qualified name of the
--                         "after" table.
--   @filtertable        - (Optional) Table containing only the key
--                         columns. When supplied, both @prevtable and
--                         @currtable are inner-joined to it before
--                         comparison, limiting the diff to a subset of
--                         keys. Useful for large tables where only
--                         specific rows are of interest.
--   @DiffListTableName  - OUTPUT. Returns the generated ##DiffList name.
--   @DiffWideTableName  - OUTPUT. Returns the generated ##DiffWide name.
--   @TotalRows          - OUTPUT. Union-distinct key count across both
--                         tables (after optional filter).
--   @AddedRows          - OUTPUT. Keys present in @currtable only.
--   @DeletedRows        - OUTPUT. Keys present in @prevtable only.
--   @ChangedRows        - OUTPUT. Keys present in both but with at least
--                         one non-key column value difference.
--   @UnchangedRows      - OUTPUT. TotalRows - Added - Deleted - Changed.
--   @TotalColumnDiffs   - OUTPUT. Total cell-level differences (= row
--                         count of the LIST table).
--
-- Behaviour:
--   1. Generates a 12-char random suffix (from NEWID) for unique global
--      temp table names to allow concurrent sessions.
--   2. Parses @keyfields via STRING_SPLIT into a table variable.
--   3. Discovers all columns from @currtable by doing SELECT TOP(0) *
--      INTO a temp schema table, then querying tempdb.sys.columns.
--   4. Partitions columns into key vs non-key sets.
--   5. Builds dynamic SQL fragments:
--        - FULL OUTER JOIN on key columns
--        - Per-column CASE expressions for the wide "prev <--> curr" format
--        - Per-column change-detection OR predicates
--        - Per-column VALUES rows for CROSS APPLY unpivot
--   6. Creates the WIDE table via SELECT … INTO from the full outer join,
--      filtering to rows with at least one difference.
--   7. Creates the LIST table by unpivoting the WIDE table's non-key
--      columns via CROSS APPLY (VALUES …), keeping only non-blank diffs.
--   8. Computes summary statistics (added, deleted, changed, unchanged,
--      total column diffs) and returns them as a single-row result set.
--
-- Notes:
--   - All comparison uses CAST(… AS NVARCHAR(MAX)) so heterogeneous
--     column types (int, datetime, etc.) are compared as strings.
--   - NULL handling: NULL-to-value changes show "NULL <--> value" and
--     vice versa. Both-NULL is treated as unchanged.
--   - The caller is responsible for dropping the ##DiffList and
--     ##DiffWide tables when done.
--   - The procedure is read-only with respect to user data; the only
--     writes are to the global temp tables.
--   - SET XACT_ABORT ON ensures clean rollback on errors.
--
-- Examples:
--   -- Simple two-table comparison
--   DECLARE @list SYSNAME, @wide SYSNAME,
--           @total INT, @added INT, @deleted INT, @changed INT,
--           @unchanged INT, @coldiffs INT;
--
--   EXEC dbo.spCompareTables
--       @keyfields = N'Id, BusinessDate',
--       @prevtable = N'dbo.FactSales_2026_01_31',
--       @currtable = N'dbo.FactSales',
--       @DiffListTableName = @list OUTPUT,
--       @DiffWideTableName = @wide OUTPUT,
--       @TotalRows = @total OUTPUT,
--       @AddedRows = @added OUTPUT,
--       @DeletedRows = @deleted OUTPUT,
--       @ChangedRows = @changed OUTPUT,
--       @UnchangedRows = @unchanged OUTPUT,
--       @TotalColumnDiffs = @coldiffs OUTPUT;
--
--   -- Review results (table names returned by OUTPUT params)
--   SELECT TOP (200) * FROM ##DiffList_A1B2C3D4E5F6;
--   SELECT TOP (200) * FROM ##DiffWide_A1B2C3D4E5F6;
--
--   -- Filtered comparison (only check specific keys)
--   EXEC dbo.spCompareTables
--       @keyfields   = N'APPID',
--       @prevtable   = N'IT.Backup.[TmsEPrd.dbo.STUDENT_CRS_HIST-20260209]',
--       @currtable   = N'TmsEPrd.dbo.STUDENT_CRS_HIST',
--       @filtertable = N'IT.dbo.sch_diff',
--       @DiffListTableName = @list OUTPUT,
--       @DiffWideTableName = @wide OUTPUT,
--       @TotalRows = @total OUTPUT,
--       @AddedRows = @added OUTPUT,
--       @DeletedRows = @deleted OUTPUT,
--       @ChangedRows = @changed OUTPUT,
--       @UnchangedRows = @unchanged OUTPUT,
--       @TotalColumnDiffs = @coldiffs OUTPUT;
--
--   -- Clean up when done
--   DROP TABLE ##DiffList_A1B2C3D4E5F6;
--   DROP TABLE ##DiffWide_A1B2C3D4E5F6;
--
-- Change history:
-- 2026-02-12 JJJ Initial version.
--------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.spCompareTables
(
    @keyfields    NVARCHAR(max),        -- e.g. N'APPID' or N'YR_CDE, TRM_CDE, CRS_CDE'
    @prevtable    NVARCHAR(512),        -- e.g. N'IT.Backup.[TmsEPrd.dbo.STUDENT_CRS_HIST-20260209-170818]' or N'dbo.MyTable_Old'
    @currtable    NVARCHAR(512),        -- e.g. N'TmsEPrd.dbo.STUDENT_CRS_HIST' or N'dbo.Current'
    @filtertable  NVARCHAR(512) = NULL, -- e.g. N'#KeysToCheck' or N'Mydb.Staging.KeysToCheck' (must have same key column names)
    @DiffListTableName SYSNAME OUTPUT,
    @DiffWideTableName SYSNAME OUTPUT,

    @TotalRows        INT OUTPUT,
    @AddedRows        INT OUTPUT,
    @DeletedRows      INT OUTPUT,
    @ChangedRows      INT OUTPUT,
    @UnchangedRows    INT OUTPUT,
    @TotalColumnDiffs INT OUTPUT
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    --------------------------------------------------------------------------
    -- 1. Generate unique suffix for global temp table names
    --------------------------------------------------------------------------
    DECLARE @suffix NVARCHAR(32) = REPLACE(CONVERT(NVARCHAR(36), NEWID()), N'-', N'');
    SET @suffix = LEFT(@suffix, 12);
    SET @DiffListTableName = N'##DiffList_' + @suffix;
    SET @DiffWideTableName = N'##DiffWide_' + @suffix;

    --------------------------------------------------------------------------
    -- 2. Parse @keyfields into a table variable
    --------------------------------------------------------------------------
    DECLARE @keys TABLE (pos INT IDENTITY(1,1), col SYSNAME);
    INSERT INTO @keys (col)
    SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@keyfields, N',');

    DECLARE @firstKeyCol SYSNAME;
    SELECT TOP 1 @firstKeyCol = col FROM @keys ORDER BY pos;

    --------------------------------------------------------------------------
    -- 3. Discover all columns from @currtable via a temp schema table
    --------------------------------------------------------------------------
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @schemaTable NVARCHAR(256) = N'##_schema_' + @suffix;

    SET @sql = N'IF OBJECT_ID(N''' + @schemaTable + N''') IS NOT NULL DROP TABLE ' + @schemaTable + N'; '
             + N'SELECT TOP (0) * INTO ' + @schemaTable + N' FROM ' + @currtable;
    EXEC sp_executesql @sql;

    --------------------------------------------------------------------------
    -- 4. Identify non-key columns
    --------------------------------------------------------------------------
    DECLARE @allcols TABLE (col SYSNAME);
    SET @sql = N'SELECT c.name FROM tempdb.sys.columns c '
             + N'WHERE c.object_id = OBJECT_ID(N''tempdb..' + @schemaTable + N''') '
             + N'ORDER BY c.column_id';
    INSERT INTO @allcols (col) EXEC sp_executesql @sql;

    -- Drop the schema temp table
    SET @sql = N'DROP TABLE ' + @schemaTable;
    EXEC sp_executesql @sql;

    DECLARE @nonkeycols TABLE (pos INT IDENTITY(1,1), col SYSNAME);
    INSERT INTO @nonkeycols (col)
    SELECT a.col FROM @allcols a
    WHERE NOT EXISTS (SELECT 1 FROM @keys k WHERE k.col = a.col);

    --------------------------------------------------------------------------
    -- 5. Build dynamic SQL fragments
    --------------------------------------------------------------------------

    -- 5a. Key column fragments
    DECLARE @joinCond      NVARCHAR(MAX) = N'',
            @keyCoalesce   NVARCHAR(MAX) = N'',
            @keyList       NVARCHAR(MAX) = N'',
            @keySelectWide NVARCHAR(MAX) = N'';

    SELECT @joinCond      = @joinCond      + IIF(@joinCond      = N'', N'', N' AND ') + N'p.' + QUOTENAME(col) + N' = c.' + QUOTENAME(col),
           @keyCoalesce   = @keyCoalesce   + IIF(@keyCoalesce   = N'', N'', N', ')    + N'COALESCE(c.' + QUOTENAME(col) + N', p.' + QUOTENAME(col) + N') AS ' + QUOTENAME(col),
           @keyList       = @keyList       + IIF(@keyList       = N'', N'', N', ')    + QUOTENAME(col),
           @keySelectWide = @keySelectWide + IIF(@keySelectWide = N'', N'', N', ')    + N'w.' + QUOTENAME(col)
    FROM @keys ORDER BY pos;

    -- 5b. Source CTEs (optionally filtered by @filtertable)
    DECLARE @prevSrc NVARCHAR(MAX), @currSrc NVARCHAR(MAX);

    IF @filtertable IS NOT NULL
    BEGIN
        DECLARE @filterJoin NVARCHAR(MAX) = N'';
        SELECT @filterJoin = @filterJoin + IIF(@filterJoin = N'', N'', N' AND ')
             + N'f.' + QUOTENAME(col) + N' = t.' + QUOTENAME(col)
        FROM @keys ORDER BY pos;

        SET @prevSrc = N'SELECT t.* FROM ' + @prevtable + N' t INNER JOIN ' + @filtertable + N' f ON ' + @filterJoin;
        SET @currSrc = N'SELECT t.* FROM ' + @currtable + N' t INNER JOIN ' + @filtertable + N' f ON ' + @filterJoin;
    END
    ELSE
    BEGIN
        SET @prevSrc = N'SELECT * FROM ' + @prevtable;
        SET @currSrc = N'SELECT * FROM ' + @currtable;
    END;

    -- 5c. _ChangeType expression
    DECLARE @changeTypeExpr NVARCHAR(MAX) =
        N'CASE WHEN p.' + QUOTENAME(@firstKeyCol) + N' IS NULL THEN N''Added'' '
      + N'WHEN c.' + QUOTENAME(@firstKeyCol) + N' IS NULL THEN N''Deleted'' '
      + N'ELSE N''Changed'' END AS _ChangeType';

    -- 5d. Per-non-key-column: wide SELECT expressions, change-detection OR list, unpivot VALUES
    DECLARE @wideSelectCols NVARCHAR(MAX) = N'',
            @hasChangeCond  NVARCHAR(MAX) = N'',
            @unpivotValues  NVARCHAR(MAX) = N'';

    SELECT
        @wideSelectCols = @wideSelectCols
            + N', CASE'
            + N' WHEN p.' + QUOTENAME(col) + N' IS NULL AND c.' + QUOTENAME(col) + N' IS NULL THEN N'''''
            + N' WHEN p.' + QUOTENAME(col) + N' IS NULL THEN N''NULL <--> '' + CAST(c.' + QUOTENAME(col) + N' AS NVARCHAR(MAX))'
            + N' WHEN c.' + QUOTENAME(col) + N' IS NULL THEN CAST(p.' + QUOTENAME(col) + N' AS NVARCHAR(MAX)) + N'' <--> NULL'''
            + N' WHEN CAST(p.' + QUOTENAME(col) + N' AS NVARCHAR(MAX)) <> CAST(c.' + QUOTENAME(col) + N' AS NVARCHAR(MAX))'
            + N'  THEN CAST(p.' + QUOTENAME(col) + N' AS NVARCHAR(MAX)) + N'' <--> '' + CAST(c.' + QUOTENAME(col) + N' AS NVARCHAR(MAX))'
            + N' ELSE N'''' END AS ' + QUOTENAME(col),

        @hasChangeCond = @hasChangeCond
            + IIF(@hasChangeCond = N'', N'', N' OR ')
            + N'(p.' + QUOTENAME(col) + N' IS NULL AND c.' + QUOTENAME(col) + N' IS NOT NULL)'
            + N' OR (p.' + QUOTENAME(col) + N' IS NOT NULL AND c.' + QUOTENAME(col) + N' IS NULL)'
            + N' OR (p.' + QUOTENAME(col) + N' <> c.' + QUOTENAME(col) + N')',

        @unpivotValues = @unpivotValues
            + IIF(@unpivotValues = N'', N'', N', ')
            + N'(N''' + REPLACE(col, N'''', N'''''') + N''', ' + QUOTENAME(col) + N')'
    FROM @nonkeycols ORDER BY pos;

    -- Guard: if there are zero non-key columns
    IF @hasChangeCond = N'' SET @hasChangeCond = N'1=0';

    --------------------------------------------------------------------------
    -- 6. Create the WIDE diff table  (keys + _ChangeType + one col per non-key)
    --------------------------------------------------------------------------
    SET @sql = N'
    SELECT ' + @keyCoalesce + N',
           ' + @changeTypeExpr
             + @wideSelectCols + N'
    INTO ' + @DiffWideTableName + N'
    FROM (' + @prevSrc + N') p
    FULL OUTER JOIN (' + @currSrc + N') c
      ON ' + @joinCond + N'
    WHERE p.' + QUOTENAME(@firstKeyCol) + N' IS NULL          -- Added
       OR c.' + QUOTENAME(@firstKeyCol) + N' IS NULL          -- Deleted
       OR (' + @hasChangeCond + N');';                        -- Changed

    EXEC sp_executesql @sql;

    --------------------------------------------------------------------------
    -- 7. Create the LIST diff table  (keys + _ChangeType + ChangedColumn + PreviousValue + CurrentValue)
    --------------------------------------------------------------------------
    IF @unpivotValues = N''
    BEGIN
        -- No non-key columns: create an empty list table with the correct schema
        SET @sql = N'
        SELECT ' + @keySelectWide + N',
               w._ChangeType,
               CAST(NULL AS SYSNAME)       AS ChangedColumn,
               CAST(NULL AS NVARCHAR(MAX)) AS PreviousValue,
               CAST(NULL AS NVARCHAR(MAX)) AS CurrentValue
        INTO ' + @DiffListTableName + N'
        FROM ' + @DiffWideTableName + N' w
        WHERE 1 = 0;';
    END
    ELSE
    BEGIN
        SET @sql = N'
        SELECT ' + @keySelectWide + N',
               w._ChangeType,
               v.ChangedColumn,
               CASE WHEN CHARINDEX(N'' <--> '', v.DiffVal) > 0
                    THEN LEFT(v.DiffVal, CHARINDEX(N'' <--> '', v.DiffVal) - 1)
                    ELSE NULL END AS PreviousValue,
               CASE WHEN CHARINDEX(N'' <--> '', v.DiffVal) > 0
                    THEN SUBSTRING(v.DiffVal, CHARINDEX(N'' <--> '', v.DiffVal) + 6, LEN(v.DiffVal))
                    ELSE NULL END AS CurrentValue
        INTO ' + @DiffListTableName + N'
        FROM ' + @DiffWideTableName + N' w
        CROSS APPLY (VALUES
            ' + @unpivotValues + N'
        ) v(ChangedColumn, DiffVal)
        WHERE v.DiffVal <> N'''';';
    END;

    EXEC sp_executesql @sql;

    --------------------------------------------------------------------------
    -- 8. Compute statistics
    --------------------------------------------------------------------------
    SET @AddedRows        = 0;
    SET @DeletedRows      = 0;
    SET @ChangedRows      = 0;
    SET @TotalColumnDiffs  = 0;

    -- Counts from wide table
    SET @sql = N'SELECT '
             + N'@Added   = SUM(CASE WHEN _ChangeType = N''Added''   THEN 1 ELSE 0 END), '
             + N'@Deleted = SUM(CASE WHEN _ChangeType = N''Deleted'' THEN 1 ELSE 0 END), '
             + N'@Changed = SUM(CASE WHEN _ChangeType = N''Changed'' THEN 1 ELSE 0 END) '
             + N'FROM ' + @DiffWideTableName;
    EXEC sp_executesql @sql,
        N'@Added INT OUTPUT, @Deleted INT OUTPUT, @Changed INT OUTPUT',
        @Added   = @AddedRows   OUTPUT,
        @Deleted = @DeletedRows OUTPUT,
        @Changed = @ChangedRows OUTPUT;

    -- Column-level diffs from list table
    SET @sql = N'SELECT @ColDiffs = COUNT(*) FROM ' + @DiffListTableName;
    EXEC sp_executesql @sql, N'@ColDiffs INT OUTPUT', @ColDiffs = @TotalColumnDiffs OUTPUT;

    -- Total distinct keys across both tables (union)
    SET @sql = N'SELECT @Total = COUNT(*) FROM ('
             + N'SELECT ' + @keyList + N' FROM (' + @prevSrc + N') x '
             + N'UNION '
             + N'SELECT ' + @keyList + N' FROM (' + @currSrc + N') x'
             + N') u';
    EXEC sp_executesql @sql, N'@Total INT OUTPUT', @Total = @TotalRows OUTPUT;

    SET @UnchangedRows = @TotalRows - ISNULL(@AddedRows, 0) - ISNULL(@DeletedRows, 0) - ISNULL(@ChangedRows, 0);

    --------------------------------------------------------------------------
    -- 9. Return summary result set
    --------------------------------------------------------------------------
    SELECT
        @TotalRows         AS TotalRows,
        @AddedRows         AS AddedRows,
        @DeletedRows       AS DeletedRows,
        @ChangedRows       AS ChangedRows,
        @UnchangedRows     AS UnchangedRows,
        @TotalColumnDiffs  AS TotalColumnDiffs,
        @DiffListTableName AS DiffListTableName,
        @DiffWideTableName AS DiffWideTableName;

END
GO
