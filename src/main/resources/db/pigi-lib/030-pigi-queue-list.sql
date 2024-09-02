-- liquibase formatted sql
-- changeset GG:1 runOnChange:true splitStatements:false
-- comment: Extra API to get Queue List

CREATE OR REPLACE FUNCTION pigi_queue_list()
RETURNS SETOF VARCHAR AS $$
DECLARE
    sql TEXT;
BEGIN
    sql=FORMAT(
        $QUERY$
        select queue_name from t_pigi_meta
         order by queue_name;
        $QUERY$);
    RETURN QUERY EXECUTE sql;
END;
$$ LANGUAGE plpgsql;
