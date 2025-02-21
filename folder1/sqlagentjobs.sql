DECLARE @isenabledonly BIT = 0;
DECLARE @iszeroexcluded BIT = 1;
WITH base AS (
    SELECT
        j.name AS jobname,
        CONVERT(VARCHAR(8), h.run_date) AS run_date,
        RIGHT(CONCAT('000000', h.run_time), 6) AS run_time,
        CONVERT(INT, h.run_duration) AS run_duration
    FROM msdb.dbo.sysjobs j 
    JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id 
    WHERE ((@isenabledonly = 0 AND 1=1) OR j.enabled = @isenabledonly)
    AND ((@iszeroexcluded = 0 AND 1=1) OR CONVERT(INT, h.run_duration) != 0)
    AND j.name != 'cdc.TmsEPrd_capture'
),
withduration AS (
    SELECT
        jobname,
        CONCAT(
            SUBSTRING(run_date, 1, 4), '-', SUBSTRING(run_date, 5, 2), '-', SUBSTRING(run_date, 7, 2), ' ',
            SUBSTRING(run_time, 1, 2), ':', SUBSTRING(run_time, 3, 2), ':', SUBSTRING(run_time, 5, 2)) AS run_datetime,
        run_duration,
        (run_duration/10000*3600.0 + (run_duration/100)%100*60.0 + run_duration%100*1.0) AS duration_seconds
    FROM base
),
summary AS (
    SELECT
        jobname,
        MIN(run_datetime) min_rundatetime,
        MAX(run_datetime) max_rundatetime,
        MIN(duration_seconds) min_duration,
        MAX(duration_seconds) max_duration,
        COUNT(jobname) run_count,
        CONVERT(NUMERIC(30, 2), AVG(duration_seconds)) avg_duration,
        CONVERT(NUMERIC(30, 2), STDEV(duration_seconds)) stdev_duration
    FROM withduration
    GROUP BY jobname
)
SELECT * FROM summary ORDER BY avg_duration DESC;