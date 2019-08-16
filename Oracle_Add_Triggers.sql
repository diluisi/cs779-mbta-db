CREATE OR REPLACE TRIGGER VEHICLES_HISTORY
BEFORE DELETE ON VEHICLES_DATA_HISTORY
BEGIN
    raise_application_error(-20001,'Vehicle History records can not be deleted');
END;