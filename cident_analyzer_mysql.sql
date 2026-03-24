 cat /opt/scripts/incident_analyzer_mysql.sql
-- incident_analyzer_mariadb_fixed.sql
DELIMITER //

DROP PROCEDURE IF EXISTS analyze_incident_fast//
CREATE PROCEDURE analyze_incident_fast(
    IN start_time DATETIME,
    IN end_time DATETIME,
    IN min_duration_ms INT
)
BEGIN
    DECLARE slow_queries_count INT DEFAULT 0;
    DECLARE top_table VARCHAR(64);
    DECLARE top_table_time DOUBLE;
    DECLARE top_pattern VARCHAR(32);
    DECLARE top_pattern_time DOUBLE;
    DECLARE inefficient_count INT DEFAULT 0;

    -- Создаем временные таблицы для анализа
    DROP TEMPORARY TABLE IF EXISTS temp_slow_queries;
    DROP TEMPORARY TABLE IF EXISTS temp_query_patterns;
    DROP TEMPORARY TABLE IF EXISTS temp_table_stats;

    CREATE TEMPORARY TABLE temp_slow_queries (
        id INT AUTO_INCREMENT PRIMARY KEY,
        query_time DOUBLE,
        lock_time DOUBLE,
        rows_sent INT,
        rows_examined INT,
        db_name VARCHAR(64),
        query_text LONGTEXT,
        query_hash VARCHAR(32),
        pattern_type VARCHAR(50)
    );

    CREATE TEMPORARY TABLE temp_query_patterns (
        pattern_hash VARCHAR(32),
        pattern_type VARCHAR(50),
        query_count INT,
        avg_duration DOUBLE,
        max_duration DOUBLE,
        total_rows_examined BIGINT,
        total_rows_sent BIGINT
    );

    CREATE TEMPORARY TABLE temp_table_stats (
        table_name VARCHAR(64),
        query_count INT,
        avg_duration DOUBLE,
        max_duration DOUBLE,
        total_rows_examined BIGINT,
        total_rows_sent BIGINT
    );

    -- Получаем медленные запросы из performance_schema
    INSERT INTO temp_slow_queries (query_time, lock_time, rows_sent, rows_examined, db_name, query_text, query_hash, patte
    SELECT
        ROUND(avg_timer_wait/1000000000, 2) as query_time_ms,
        ROUND(avg_lock_time/1000000000, 2) as lock_time_ms,
        rows_sent,
        rows_examined,
        SCHEMA_NAME as db_name,
        DIGEST_TEXT as query_text,
        MD5(COALESCE(DIGEST_TEXT, '')) as query_hash,
        CASE
            WHEN DIGEST_TEXT LIKE 'SELECT%' THEN 'SELECT'
            WHEN DIGEST_TEXT LIKE 'UPDATE%' THEN 'UPDATE'
            WHEN DIGEST_TEXT LIKE 'INSERT%' THEN 'INSERT'
            WHEN DIGEST_TEXT LIKE 'DELETE%' THEN 'DELETE'
            ELSE 'OTHER'
        END as pattern_type
    FROM performance_schema.events_statements_summary_by_digest
    WHERE ROUND(avg_timer_wait/1000000000, 2) > min_duration_ms
        AND LAST_SEEN BETWEEN start_time AND end_time
    ORDER BY avg_timer_wait DESC
    LIMIT 500;

    SET slow_queries_count = ROW_COUNT();

    IF slow_queries_count = 0 THEN
        SELECT 'No slow queries found' as result;
    ELSE
        -- Анализ паттернов запросов
        INSERT INTO temp_query_patterns
        SELECT
            query_hash,
            pattern_type,
            COUNT(*) as query_count,
            AVG(query_time) as avg_duration,
            MAX(query_time) as max_duration,
            SUM(rows_examined) as total_rows_examined,
            SUM(rows_sent) as total_rows_sent
        FROM temp_slow_queries
        GROUP BY query_hash, pattern_type
        ORDER BY max_duration DESC;

        -- Статистика по таблицам/базам
        INSERT INTO temp_table_stats
        SELECT
            db_name as table_name,
            COUNT(*) as query_count,
            AVG(query_time) as avg_duration,
            MAX(query_time) as max_duration,
            SUM(rows_examined) as total_rows_examined,
            SUM(rows_sent) as total_rows_sent
        FROM temp_slow_queries
        WHERE db_name IS NOT NULL
        GROUP BY db_name
        ORDER BY max_duration DESC;

        -- Топ таблица по максимальному времени
        SELECT table_name, max_duration
        INTO top_table, top_table_time
        FROM temp_table_stats
        ORDER BY max_duration DESC
        LIMIT 1;

        -- Топ паттерн по максимальному времени
        SELECT pattern_type, max_duration
        INTO top_pattern, top_pattern_time
        FROM temp_query_patterns
        ORDER BY max_duration DESC
        LIMIT 1;

        -- Подсчет неэффективных запросов
        SELECT COUNT(*) INTO inefficient_count
        FROM temp_slow_queries
        WHERE rows_examined > 1000 AND rows_sent < 100;

        -- Вывод результатов в одной таблице
        SELECT
            'ANALYSIS SUMMARY' as section,
            CONCAT('Period: ', start_time, ' - ', end_time) as period_info,
            CONCAT('Min duration: ', min_duration_ms, 'ms') as threshold_info,
            CONCAT('Slow queries: ', slow_queries_count) as queries_info,
            CONCAT('Top table: ', COALESCE(top_table, 'N/A')) as top_table_info,
            CONCAT('Top operation: ', COALESCE(top_pattern, 'N/A')) as top_operation_info,
            CONCAT('Inefficient queries: ', inefficient_count) as inefficient_info
        FROM DUAL
        UNION ALL
        SELECT
            'RECOMMENDATIONS' as section,
            CASE
                WHEN inefficient_count > 0 THEN 'Check indexes for WHERE conditions'
                ELSE 'No critical issues found'
            END as period_info,
            CASE
                WHEN COALESCE(top_table_time, 0) > 10000 THEN 'Table requires urgent optimization'
                ELSE 'Performance within acceptable limits'
            END as threshold_info,
            'Review slow queries below' as queries_info,
            'Consider query optimization' as top_table_info,
            'Monitor performance regularly' as top_operation_info,
            'Use EXPLAIN for slow queries' as inefficient_info
        FROM DUAL;

        -- Показываем топ медленных запросов
        SELECT 'TOP SLOW QUERIES:' as description;
        SELECT query_time, db_name, pattern_type, rows_examined, rows_sent, LEFT(query_text, 100) as query_preview
        FROM temp_slow_queries
        ORDER BY query_time DESC
        LIMIT 10;

        -- Показываем топ паттернов
        SELECT 'TOP PATTERNS:' as description;
        SELECT pattern_type, query_count, max_duration, avg_duration
        FROM temp_query_patterns
        ORDER BY max_duration DESC
        LIMIT 5;

        -- Показываем топ таблиц
        SELECT 'TOP TABLES:' as description;
        SELECT table_name, query_count, max_duration, avg_duration
        FROM temp_table_stats
        ORDER BY max_duration DESC
        LIMIT 5;

    END IF;
END//

DELIMITER ;

-- Создаем простую процедуру для анализа через slow log
DELIMITER //

DROP PROCEDURE IF EXISTS analyze_from_slow_log//
CREATE PROCEDURE analyze_from_slow_log(
    IN start_time DATETIME,
    IN end_time DATETIME
)
BEGIN
    DECLARE table_exists INT DEFAULT 0;

    -- Проверяем существование таблицы slow_log
    SELECT COUNT(*) INTO table_exists
    FROM information_schema.tables
    WHERE table_schema = 'mysql' AND table_name = 'slow_log';

    IF table_exists = 1 THEN
        SELECT 'Slow log analysis results:' as description;
        SELECT
            start_time,
            query_time,
            lock_time,
            rows_sent,
            rows_examined,
            db,
            LEFT(sql_text, 200) as query_preview
        FROM mysql.slow_log
        WHERE start_time BETWEEN start_time AND end_time
        ORDER BY query_time DESC
        LIMIT 20;
    ELSE
        SELECT 'Slow log table not found' as error;
        SELECT 'Enable with: SET GLOBAL slow_query_log = 1' as suggestion;
    END IF;
END//

DELIMITER ;

-- Инструкции по использованию
SELECT '=== MYSQL INCIDENT ANALYZER ===' as title;
SELECT 'Usage instructions:' as section;
SELECT '1. CALL analyze_incident_fast(start_time, end_time, min_duration_ms)' as command;
SELECT '2. CALL analyze_from_slow_log(start_time, end_time)' as command;
SELECT ' ' as spacer;
SELECT 'Example:' as example;
SELECT 'CALL analyze_incident_fast(''2024-01-20 10:00:00'', ''2024-01-20 11:00:00'', 5000)' as example_command;

-- Проверка доступности performance_schema
SELECT '=== SYSTEM CHECKS ===' as title;
SELECT 'Performance_schema consumers:' as check_name;
SELECT NAME, ENABLED
FROM performance_schema.setup_consumers
WHERE NAME LIKE 'events_statements%';

SELECT 'Slow query log status:' as check_name;
SHOW VARIABLES LIKE 'slow_query_log%';

-- Автоматический запуск демо-анализа
SELECT ' ' as spacer;
SELECT '=== DEMO ANALYSIS ===' as title;

SET @start_time = NOW() - INTERVAL 1 HOUR;
SET @end_time = NOW();
SET @min_duration = 1000;

SELECT CONCAT('Analyzing period: ', @start_time, ' to ', @end_time) as demo_info;
SELECT CONCAT('Minimum duration: ', @min_duration, 'ms') as demo_info;

CALL analyze_incident_fast(@start_time, @end_time, @min_duration);
