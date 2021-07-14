UPDATE test_item_results
SET status = 'PASSED'
WHERE status = 'UNTESTED';

DELETE
FROM pg_enum
WHERE enumlabel = 'UNTESTED'
  AND enumtypid = (
    SELECT oid
    FROM pg_type
    WHERE typname = 'status_enum'
);