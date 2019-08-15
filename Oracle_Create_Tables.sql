REM
REM DROP TABLES
REM

DROP TABLE VEHICLES_DATA;
DROP TABLE VEHICLES;
-- DROP TABLE VEHICLES_HISTORY;
DROP TABLE STATUSES;
DROP TABLE DESTINATIONS_ROUTES_BRIDGE;
DROP TABLE DIRECTION_NAMES_ROUTES_BRIDGE;
DROP TABLE ROUTES;
DROP TABLE DIRECTION_NAMES;
DROP TABLE DIRECTIONS;
DROP TABLE DESTINATIONS;
DROP TABLE LINES;
DROP TABLE COLORS;
DROP TABLE STOPS;
DROP TABLE STREETS;
DROP TABLE MUNICIPALITIES;
REM
REM CREATE TABLES
REM

CREATE TABLE VEHICLES(
	vehicle_id VARCHAR2(32) NOT NULL,
	label VARCHAR2(32) UNIQUE,
	CONSTRAINT vehicles_pk PRIMARY KEY (vehicle_id)
);

CREATE TABLE STATUSES(
	status_id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1) NOT NULL,
	status VARCHAR2(32) UNIQUE NOT NULL,
	CONSTRAINT statuses_pk PRIMARY KEY (status_id)
);

CREATE TABLE STREETS(
	street_id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1) NOT NULL,
	STREET VARCHAR2(32) UNIQUE NOT NULL,
	CONSTRAINT streets_pk PRIMARY KEY (street_id)
);

CREATE TABLE DIRECTIONS(
	direction_id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1) NOT NULL,
	direction VARCHAR2(32) UNIQUE NOT NULL,
	CONSTRAINT directions_pk PRIMARY KEY (direction_id)
);

CREATE TABLE DIRECTION_NAMES(
	direction_name_id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1) NOT NULL,
	direction_name VARCHAR2(32) UNIQUE NOT NULL,
	CONSTRAINT direction_names_pk PRIMARY KEY (direction_name_id)
);

CREATE TABLE MUNICIPALITIES(
	municipality_id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1) NOT NULL,
	municipality VARCHAR2(32) UNIQUE NOT NULL,
	CONSTRAINT municiplity_pk PRIMARY KEY (municipality_id)
);

CREATE TABLE DESTINATIONS(
	destination_id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1) NOT NULL,
	destination VARCHAR2(128) UNIQUE NOT NULL,
	CONSTRAINT destinations_pk PRIMARY KEY (destination_id)
);

CREATE TABLE COLORS(
	color_id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1) NOT NULL,
	color VARCHAR2(32) UNIQUE NOT NULL,
	CONSTRAINT colors_pk PRIMARY KEY (color_id)
);

CREATE TABLE LINES(
	line_id VARCHAR(128) NOT NULL,
	long_name VARCHAR2(128),
	short_name VARCHAR2(128),
	color NUMBER,
	text_color NUMBER,
	CONSTRAINT lines_pk PRIMARY KEY (line_id),
	CONSTRAINT lines_color_fk FOREIGN KEY (color) REFERENCES colors(color_id),
	CONSTRAINT lines_text_color_fk FOREIGN KEY (text_color) REFERENCES colors(color_id)
);

CREATE TABLE STOPS(
	stop_id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1) NOT NULL,
	name VARCHAR2(32),
	address VARCHAR2(32),
	description VARCHAR2(32),
	latitude FLOAT(63)	NOT NULL,
	longitude FLOAT(63)	NOT NULL,
	municipality_id NUMBER,
	at_street NUMBER,
	on_street NUMBER,
	CONSTRAINT stops_pk PRIMARY KEY (stop_id),
	CONSTRAINT stops_municipality_fk FOREIGN KEY (municipality_id) REFERENCES municipalities(municipality_id),
	CONSTRAINT stops_at_street_fk FOREIGN KEY (at_street) REFERENCES streets(street_id),
	CONSTRAINT stops_on_street_fk FOREIGN KEY (on_street) REFERENCES streets(street_id)
);

CREATE TABLE ROUTES(
	route_id VARCHAR2(32) NOT NULL,
	description VARCHAR2(32),
	fare_class VARCHAR2(32),
	long_name VARCHAR2(128),
	short_name VARCHAR2(32),
	direction_id NUMBER,
	destination_id NUMBER,
	line_id VARCHAR(128),
	color NUMBER,
	text_color NUMBER,
	CONSTRAINT routes_pk PRIMARY KEY (route_id),
	CONSTRAINT routes_direction_id_fk FOREIGN KEY (direction_id) REFERENCES directions(direction_id),
	CONSTRAINT routes_destination_id_fk FOREIGN KEY (destination_id) REFERENCES destinations(destination_id),
	CONSTRAINT routes_line_id_fk FOREIGN KEY (line_id) REFERENCES lines(line_id),
	CONSTRAINT routes_color_fk FOREIGN KEY (color) REFERENCES colors(color_id),
	CONSTRAINT routes_text_color_fk FOREIGN KEY (text_color) REFERENCES colors(color_id)
);

CREATE TABLE VEHICLES_DATA(
	vehicle_data_id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1) NOT NULL,
    vehicle_id NUMBER NOT NULL,
	bearing	NUMBER(5),
	current_stop_sequence NUMBER(5),
	latitude FLOAT(63)	NOT NULL,
	longitude FLOAT(63)	NOT NULL,
	speed NUMBER(5),
	updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
	direction_id NUMBER,
	route_id VARCHAR(32),
	current_status NUMBER,
	stop_id NUMBER,
	CONSTRAINT vehicles_data_pk PRIMARY KEY (vehicle_data_id),
	CONSTRAINT vehicles_data_direction_id_fk FOREIGN KEY (direction_id) REFERENCES directions(direction_id),
	CONSTRAINT vehicles_data_route_id_fk FOREIGN KEY (route_id) REFERENCES routes(route_id),
	CONSTRAINT vehicles_data_current_status_fk FOREIGN KEY (current_status) REFERENCES statuses(status_id)
);

CREATE TABLE DESTINATIONS_ROUTES_BRIDGE(
	route_id VARCHAR2(32) NOT NULL,
	destination_id VARCHAR2(32) NOT NULL,
	CONSTRAINT destinations_routes_bridge_pk PRIMARY KEY (route_id, destination_id)
);

CREATE TABLE DIRECTION_NAMES_ROUTES_BRIDGE(
	route_id VARCHAR2(32) NOT NULL,
	direction_name_id VARCHAR2(32) NOT NULL,
	CONSTRAINT destinations_names_bridge_pk PRIMARY KEY (route_id, direction_name_id)
);