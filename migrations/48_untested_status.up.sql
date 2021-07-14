ALTER TYPE status_enum RENAME TO status_enum_old;

CREATE TYPE status_enum AS ENUM ('CANCELLED', 'FAILED', 'INTERRUPTED', 'IN_PROGRESS', 'PASSED', 'RESETED', 'SKIPPED', 'STOPPED', 'INFO', 'WARN', 'UNTESTED');

ALTER TABLE launch
    ALTER COLUMN status TYPE status_enum USING status::text::status_enum;
ALTER TABLE test_item_results
    ALTER COLUMN status TYPE status_enum USING status::text::status_enum;

DROP TYPE status_enum_old;

CREATE OR REPLACE FUNCTION update_executions_statistics()
    RETURNS TRIGGER AS
$$
DECLARE
    DECLARE
    executions_field          VARCHAR;
    DECLARE
    executions_field_id       BIGINT;
    DECLARE
    executions_field_old      VARCHAR;
    DECLARE
    executions_field_old_id   BIGINT;
    DECLARE
    executions_field_total    VARCHAR;
    DECLARE
    executions_field_total_id BIGINT;
    DECLARE
    cur_launch_id             BIGINT;
    DECLARE
    counter_decrease          INTEGER;

BEGIN
    IF exists(SELECT 1
              FROM test_item
              WHERE (test_item.parent_id = new.result_id
                  AND test_item.has_stats)
                 OR (test_item.item_id = new.result_id AND (NOT test_item.has_stats OR
                                                            (test_item.type != 'TEST' :: TEST_ITEM_TYPE_ENUM AND
                                                             test_item.type != 'SCENARIO' :: TEST_ITEM_TYPE_ENUM AND
                                                             test_item.type != 'STEP' :: TEST_ITEM_TYPE_ENUM)))
              LIMIT 1)
    THEN
        RETURN new;
    END IF;

    IF exists(SELECT 1
              FROM test_item
              WHERE item_id = new.result_id
                AND retry_of IS NOT NULL
              LIMIT 1)
    THEN
        RETURN new;
    END IF;

    cur_launch_id := (SELECT launch_id FROM test_item WHERE test_item.item_id = new.result_id);

    IF cur_launch_id IS NULL
    THEN
        RETURN new;
    END IF;

    IF new.status = 'INTERRUPTED' :: STATUS_ENUM
    THEN
        executions_field := 'statistics$executions$failed';
    ELSE
        executions_field := concat('statistics$executions$', lower(new.status :: VARCHAR));
    END IF;

    executions_field_total := 'statistics$executions$total';

    INSERT INTO statistics_field (name) VALUES (executions_field) ON CONFLICT DO NOTHING;

    INSERT INTO statistics_field (name) VALUES (executions_field_total) ON CONFLICT DO NOTHING;

    executions_field_id = (SELECT DISTINCT ON (statistics_field.name) sf_id
                           FROM statistics_field
                           WHERE statistics_field.name = executions_field);
    executions_field_total_id = (SELECT DISTINCT ON (statistics_field.name) sf_id
                                 FROM statistics_field
                                 WHERE statistics_field.name = executions_field_total);

    IF old.status = 'IN_PROGRESS' :: STATUS_ENUM
    THEN
        INSERT INTO statistics (s_counter, statistics_field_id, item_id)
            (SELECT 1, executions_field_id, item_id
             FROM (SELECT item_id FROM test_item WHERE path @> (SELECT path FROM test_item WHERE item_id = new.result_id)) AS temp_bulk)
        ON CONFLICT (statistics_field_id,
            item_id)
            DO UPDATE SET s_counter = statistics.s_counter + 1;

        INSERT INTO statistics (s_counter, statistics_field_id, item_id)
            (SELECT 1, executions_field_total_id, item_id
             FROM (SELECT item_id FROM test_item WHERE path @> (SELECT path FROM test_item WHERE item_id = new.result_id)) AS temp_bulk)
        ON CONFLICT (statistics_field_id,
            item_id)
            DO UPDATE SET s_counter = statistics.s_counter + 1;

        /* increment launch executions statistics for concrete field */
        INSERT INTO statistics (s_counter, statistics_field_id, launch_id)
        VALUES (1, executions_field_id, cur_launch_id)
        ON CONFLICT (statistics_field_id,
            launch_id)
            DO UPDATE SET s_counter = statistics.s_counter + 1;
        /* increment launch executions statistics for total field */
        INSERT INTO statistics (s_counter, statistics_field_id, launch_id)
        VALUES (1, executions_field_total_id, cur_launch_id)
        ON CONFLICT (statistics_field_id,
            launch_id)
            DO UPDATE SET s_counter = statistics.s_counter + 1;
        RETURN new;
    END IF;

    IF old.status != 'IN_PROGRESS' :: STATUS_ENUM AND old.status != new.status
    THEN
        IF old.status = 'INTERRUPTED' :: STATUS_ENUM
        THEN
            executions_field_old := 'statistics$executions$failed';
        ELSE
            executions_field_old := concat('statistics$executions$', lower(old.status :: VARCHAR));
        END IF;

        executions_field_old_id = (SELECT DISTINCT ON (statistics_field.name) sf_id
                                   FROM statistics_field
                                   WHERE statistics_field.name = executions_field_old);

        SELECT s_counter
        INTO counter_decrease
        FROM statistics
        WHERE item_id = new.result_id
          AND statistics_field_id = executions_field_old_id;

        /* decrease item executions statistics for old field */
        UPDATE statistics
        SET s_counter = s_counter - counter_decrease
        WHERE statistics_field_id = executions_field_old_id
          AND item_id IN (SELECT item_id FROM test_item WHERE path @> (SELECT path FROM test_item WHERE item_id = new.result_id));

        /* increment item executions statistics for concrete field */
        INSERT INTO statistics (s_counter, statistics_field_id, item_id)
            (SELECT 1, executions_field_id, item_id
             FROM (SELECT item_id FROM test_item WHERE path @> (SELECT path FROM test_item WHERE item_id = new.result_id)) AS temp_bulk)
        ON CONFLICT (statistics_field_id,
            item_id)
            DO UPDATE SET s_counter = statistics.s_counter + 1;


        /* decrease item executions statistics for old field */
        UPDATE statistics
        SET s_counter = s_counter - counter_decrease
        WHERE statistics_field_id = executions_field_old_id
          AND launch_id = cur_launch_id;
        /* increment launch executions statistics for concrete field */
        INSERT INTO statistics (s_counter, statistics_field_id, launch_id)
        VALUES (1, executions_field_id, cur_launch_id)
        ON CONFLICT (statistics_field_id,
            launch_id)
            DO UPDATE SET s_counter = statistics.s_counter + 1;
        RETURN new;
    END IF;
    RETURN new;
END;
$$
    LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION merge_launch(launchid BIGINT)
    RETURNS INTEGER
AS
$$
DECLARE
    target_item_cursor CURSOR  (id BIGINT, lvl INT) FOR
        SELECT DISTINCT ON (unique_id) unique_id, item_id, path AS path_value, has_children
        FROM test_item parent
        WHERE parent.launch_id = id
          AND nlevel(parent.path) = lvl
          AND has_stats
          AND (parent.type = 'SUITE' OR (SELECT EXISTS(SELECT 1 FROM test_item t WHERE t.parent_id = parent.item_id LIMIT 1)));
    DECLARE
    merging_item_cursor CURSOR (uniqueid VARCHAR, lvl INT, launchid BIGINT) FOR
        SELECT item_id, path AS path_value, has_retries
        FROM test_item
        WHERE test_item.unique_id = uniqueid
          AND has_stats
          AND nlevel(test_item.path) = lvl
          AND test_item.launch_id = launchid;
    DECLARE
    target_item_field    RECORD;
    DECLARE
    merging_item_field   RECORD;
    DECLARE
    max_level            BIGINT;
    DECLARE
    first_item_unique_id VARCHAR;
    DECLARE
    item_has_children    BOOLEAN;
    DECLARE
    parent_item_id       BIGINT;
    DECLARE
    parent_item_path     LTREE;
    DECLARE
    nested_step_path     LTREE;
    DECLARE
    previous_parent_path LTREE;
    DECLARE
    concatenated_descr   TEXT;
    DECLARE
    nested_step_nlevel   INTEGER;
BEGIN
    max_level := (SELECT MAX(nlevel(path))
                  FROM test_item
                  WHERE launch_id = launchid
                    AND has_stats);
    IF (max_level ISNULL)
    THEN
        RETURN 0;
    END IF;

    FOR i IN 1..max_level
        LOOP

            OPEN target_item_cursor(launchid, i);

            LOOP
                FETCH target_item_cursor INTO target_item_field;

                EXIT WHEN NOT found;

                first_item_unique_id := target_item_field.unique_id;
                parent_item_id := target_item_field.item_id;
                parent_item_path := target_item_field.path_value;
                item_has_children := target_item_field.has_children;

                EXIT WHEN first_item_unique_id ISNULL;

                IF item_has_children
                THEN
                    SELECT string_agg(description, chr(10))
                    INTO concatenated_descr
                    FROM test_item
                    WHERE test_item.unique_id = first_item_unique_id
                      AND has_stats
                      AND has_children
                      AND nlevel(test_item.path) = i
                      AND test_item.launch_id = launchid;

                    UPDATE test_item SET description = concatenated_descr WHERE test_item.item_id = parent_item_id;

                    UPDATE test_item
                    SET start_time = (SELECT min(start_time)
                                      FROM test_item
                                      WHERE test_item.unique_id = first_item_unique_id
                                        AND has_stats
                                        AND has_children
                                        AND nlevel(test_item.path) = i
                                        AND test_item.launch_id = launchid)
                    WHERE test_item.item_id = parent_item_id;

                    UPDATE test_item_results
                    SET end_time = (SELECT max(end_time)
                                    FROM test_item
                                             JOIN test_item_results result ON test_item.item_id = result.result_id
                                    WHERE test_item.unique_id = first_item_unique_id
                                      AND has_stats
                                      AND has_children
                                      AND nlevel(test_item.path) = i
                                      AND test_item.launch_id = launchid)
                    WHERE test_item_results.result_id = parent_item_id;

                    INSERT INTO statistics (statistics_field_id, item_id, launch_id, s_counter)
                    SELECT statistics_field_id, parent_item_id, NULL, sum(s_counter)
                    FROM statistics
                             JOIN test_item ti ON statistics.item_id = ti.item_id
                    WHERE ti.unique_id = first_item_unique_id
                      AND ti.launch_id = launchid
                      AND nlevel(ti.path) = i
                      AND ti.has_stats
                      AND ti.has_children
                    GROUP BY statistics_field_id
                    ON CONFLICT ON CONSTRAINT unique_stats_item DO UPDATE
                        SET s_counter = excluded.s_counter;

                    IF exists(SELECT 1
                              FROM test_item_results
                                       JOIN test_item t ON test_item_results.result_id = t.item_id
                              WHERE (test_item_results.status != 'PASSED' AND test_item_results.status != 'SKIPPED' AND test_item_results.status != 'UNTESTED')
                                AND t.unique_id = first_item_unique_id
                                AND nlevel(t.path) = i
                                AND t.has_stats
                                AND t.has_children
                                AND t.launch_id = launchid)
                    THEN
                        UPDATE test_item_results SET status = 'FAILED' WHERE test_item_results.result_id = parent_item_id;
                    ELSEIF exists(SELECT 1
                                  FROM test_item_results
                                           JOIN test_item t ON test_item_results.result_id = t.item_id
                                  WHERE (test_item_results.status != 'PASSED' AND test_item_results.status != 'UNTESTED')
                                    AND t.unique_id = first_item_unique_id
                                    AND nlevel(t.path) = i
                                    AND t.has_stats
                                    AND t.has_children
                                    AND t.launch_id = launchid)
                    THEN
                        UPDATE test_item_results SET status = 'SKIPPED' WHERE test_item_results.result_id = parent_item_id;
                    ELSEIF exists(SELECT 1
                                  FROM test_item_results
                                           JOIN test_item t ON test_item_results.result_id = t.item_id
                                  WHERE test_item_results.status != 'PASSED'
                                    AND t.unique_id = first_item_unique_id
                                    AND nlevel(t.path) = i
                                    AND t.has_stats
                                    AND t.launch_id = launchid)
                    THEN
                        UPDATE test_item_results SET status = 'UNTESTED' WHERE test_item_results.result_id = parent_item_id;
                    ELSE
                        UPDATE test_item_results SET status = 'PASSED' WHERE test_item_results.result_id = parent_item_id;
                    END IF;
                END IF;


                OPEN merging_item_cursor(target_item_field.unique_id, i, launchid);

                LOOP

                    FETCH merging_item_cursor INTO merging_item_field;

                    EXIT WHEN NOT found;

                    IF (SELECT EXISTS(SELECT 1
                                      FROM test_item t
                                      WHERE (t.parent_id = merging_item_field.item_id
                                          OR (t.item_id = merging_item_field.item_id AND t.type = 'SUITE'))
                                        AND t.has_stats))
                    THEN
                        UPDATE test_item
                        SET parent_id = parent_item_id,
                            path      = text2ltree(concat(parent_item_path :: TEXT, '.', test_item.item_id :: TEXT))
                        WHERE test_item.parent_id = merging_item_field.item_id
                          AND nlevel(test_item.path) = i + 1
                          AND has_stats
                          AND test_item.retry_of IS NULL;

                        UPDATE test_item
                        SET parent_id = parent_item_id,
                            path      = text2ltree(
                                    concat(parent_item_path :: TEXT, '.', test_item.retry_of ::TEXT, '.', test_item.item_id :: TEXT))
                        WHERE test_item.parent_id = merging_item_field.item_id
                          AND test_item.retry_of IS NOT NULL;

                        IF parent_item_id != merging_item_field.item_id
                        THEN
                            UPDATE attachment
                            SET item_id = parent_item_id
                            WHERE attachment.item_id = merging_item_field.item_id;

                            UPDATE log
                            SET item_id = parent_item_id
                            WHERE log.item_id = merging_item_field.item_id;

                            DELETE
                            FROM test_item
                            WHERE test_item.item_id = merging_item_field.item_id
                              AND test_item.has_stats;
                        END IF;

                    ELSE
                        IF
                            merging_item_field.has_retries
                        THEN
                            UPDATE test_item
                            SET path = text2ltree(concat(merging_item_field.path_value :: TEXT, '.', test_item.item_id :: TEXT))
                            WHERE test_item.retry_of = merging_item_field.item_id;
                        END IF;

                        nested_step_path :=
                                (SELECT path FROM test_item WHERE parent_id = merging_item_field.item_id AND NOT has_stats LIMIT 1);
                        IF
                            nested_step_path IS NOT NULL
                        THEN

                            nested_step_nlevel = nlevel(nested_step_path) - 2;
                            IF nested_step_nlevel < 0
                            THEN
                                nested_step_nlevel = 0;
                            END IF;

                            previous_parent_path := text2ltree(
                                    concat(subpath(nested_step_path, 0, nested_step_nlevel) :: TEXT, '.', merging_item_field.item_id :: TEXT));

                            UPDATE test_item
                            SET path = text2ltree(concat(merging_item_field.path_value :: TEXT, '.',
                                                         subpath(path, nlevel(merging_item_field.path_value)) :: TEXT))
                            WHERE path <@ previous_parent_path
                              AND item_id != merging_item_field.item_id
                              AND NOT has_stats;
                        END IF;


                    END IF;


                END LOOP;

                CLOSE merging_item_cursor;

            END LOOP;

            CLOSE target_item_cursor;

        END LOOP;


    INSERT INTO statistics (statistics_field_id, launch_id, s_counter)
    SELECT statistics_field_id, launchid, sum(s_counter)
    FROM statistics
             JOIN test_item ti ON statistics.item_id = ti.item_id
    WHERE ti.launch_id = launchid
      AND ti.has_stats
      AND ti.parent_id IS NULL
    GROUP BY statistics_field_id
    ON CONFLICT ON CONSTRAINT unique_stats_launch DO UPDATE
        SET s_counter = excluded.s_counter;

    RETURN 0;
END;
$$
    LANGUAGE plpgsql;