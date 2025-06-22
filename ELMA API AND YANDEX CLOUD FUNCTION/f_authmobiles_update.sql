CREATE OR REPLACE FUNCTION public.f_authmobiles_update()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
    UPDATE authmobiles AS t
    SET 
        mobile = s.phone,
        createdt = CURRENT_DATE,
        is_active = 1
    FROM stg_authmobiles AS s
    WHERE t.originalid = s.id;


    INSERT INTO authmobiles (originalid, mobile, createdt, is_active)
    SELECT s.id, s.phone, CURRENT_DATE, 1
    FROM stg_authmobiles AS s
    WHERE NOT EXISTS (
        SELECT 1 
        FROM authmobiles AS t
        WHERE t.originalid = s.id
    );


    UPDATE authmobiles
    SET is_active = 0
    WHERE originalid NOT IN (SELECT id FROM stg_authmobiles);
END;
$function$
;