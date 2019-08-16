CREATE OR REPLACE Procedure DAILY_VEHICLES_DATA_BACKUP
IS
    
BEGIN
    -- Select vehicle_data_ids that were updated within the last day (midnight onwards)
    INSERT INTO VEHICLES_DATA_HISTORY (vehicle_data_id, vehicle_id, bearing, current_stop_sequence, 
    latitude, longitude, speed, updated_at, direction_id,
    route_id, label, current_status, stop_id) 
        SELECT vehicle_data_id, vehicle_id, bearing, current_stop_sequence, latitude, longitude, speed, updated_at, direction_id,
        route_id, label, current_status, stop_id FROM VEHICLES_DATA WHERE vehicle_data_id IN(
            SELECT vehicle_data_id from VEHICLES_DATA WHERE updated_at >= trunc(sysdate));
END;

