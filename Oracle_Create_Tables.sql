REM
REM DROP TABLES
REM
DROP TABLE STATUSES;
DROP TABLE STREETS;
DROP TABLE DIRECTIONS;
DROP TABLE DESTINATIONS;
DROP TABLE MUNICIPALITIES;
DROP TABLE COLORS;
DROP TABLE LINES;
DROP TABLE ROUTES;
DROP TABLE STOPS;
DROP TABLE VEHICLES;
REM
REM CREATE TABLES
REM

CREATE TABLE STATUSES(
	status_id VARCHAR2(32) NOT NULL,
	status VARCHAR2(32) NOT NULL,
	CONSTRAINT statuses_pk PRIMARY KEY (status_id)
);

CREATE TABLE STREETS(
	street_id NUMBER(5) NOT NULL,
	STREET VARCHAR2(32) NOT NULL,
	CONSTRAINT streets_pk PRIMARY KEY (street_id)
);

CREATE TABLE DIRECTIONS(
	direction_id NUMBER(5) NOT NULL,
	direction VARCHAR2(32) NOT NULL,
	CONSTRAINT directions_pk PRIMARY KEY (direction_id)
);

CREATE TABLE MUNICIPALITIES(
	municipality_id NUMBER(5) NOT NULL,
	municipality VARCHAR2(32) NOT NULL,
	CONSTRAINT municiplity_pk PRIMARY KEY (municipality_id)
);

CREATE TABLE DESTINATIONS(
	destination_id NUMBER(5) NOT NULL,
	destination VARCHAR2(32) NOT NULL,
	CONSTRAINT destinations_pk PRIMARY KEY (destination_id)
);

CREATE TABLE COLORS(
	color_id NUMBER(5) NOT NULL,
	color VARCHAR2(32) NOT NULL,
	CONSTRAINT colors_pk PRIMARY KEY (color_id)
);

CREATE TABLE LINES(
	line_id NUMBER(5) NOT NULL,
	long_name VARCHAR2(32),
	short_name VARCHAR2(32),
	color NUMBER(5),
	text_color NUMBER(5),
	CONSTRAINT lines_pk PRIMARY KEY (line_id),
	CONSTRAINT lines(color) FOREIGN KEY (color) REFERNECES colors(color_id),
	CONSTRAINT lines(text_color) FOREIGN KEY (text_color) REFERNECES colors(color_id)
);

CREATE TABLE STOPS(
	stop_id NUMBER(5) NOT NULL,
	name VARCHAR2(32),
	address VARCHAR2(32),
	description VARCHAR2(32),
	latitude FLOAT(63)	NOT NULL,
	longitude FLOAT(63)	NOT NULL,
	municipality_id NUMBER(5),
	at_street NUMBER(5),
	on_street NUMBER(5),
	CONSTRAINT stops_pk PRIMARY KEY (stop_id),
	CONSTRAINT stops_municipality FOREIGN KEY (municipality_id) REFERNECES municipalities(municipality_id),
	CONSTRAINT stops_at_street FOREIGN KEY (at_street) REFERNECES streets(street_id),
	CONSTRAINT stops_on_street FOREIGN KEY (stops_on_street) REFERNECES municipalities(street_id)
);

CREATE TABLE ROUTES(
	route_id NUMBER(5) NOT NULL,
	description VARCHAR2(32),
	fare_class VARCHAR2(32),
	long_name VARCHAR2(32),
	short_name VARCHAR2(32),
	direction_id NUMBER(5),
	destination_id NUMBER(5),
	line_id NUMBER(5),
	color NUMBER(5),
	text_color NUMBER(5),
	CONSTRAINT routes_pk PRIMARY KEY (route_id),
	CONSTRAINT routes_direction_id_fk FOREIGN KEY (direction_id) REFERNECES directions(direction_id),
	CONSTRAINT routes_destination_id_fk FOREIGN KEY (destination_id) REFERNECES destinations(destination_id),
	CONSTRAINT routes_line_id_fk FOREIGN KEY (line_id) REFERNECES lines(line_id),
	CONSTRAINT routes_color_fk FOREIGN KEY (color) REFERNECES colors(color_id),
	CONSTRAINT routes_text_color_fk FOREIGN KEY (text_color) REFERNECES colors(color_id)
);

CREATE TABLE VEHICLES(
	vehicle_id VARCHAR2(32)	NOT NULL,
	label VARCHAR2(32),
	bearing	NUMBER(5),
	current_stop_sequence NUMBER(5),
	latitude FLOAT(63)	NOT NULL,
	longitude FLOAT(63)	NOT NULL,
	speed NUMBER(5),
	updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
	direction_id NUMBER(2),
	route_id VARCHAR2(32),
	current_status VARCHAR2(32),
	stop_id NUMBER(5),
	CONSTRAINT vehicles_pk PRIMARY KEY (vehicle_id),
	CONSTRAINT vehicles_direction_id_fk FOREIGN KEY (direction_id) REFERENCES directions(direction_id),
	CONSTRAINT vehicles_route_id_fk FOREIGN KEY (route_id) REFERENCES routes(route_id),
	CONSTRAINT vehicles_current_status_fk FOREIGN KEY (current_status) REFERENCES statuses(status_id)
);
