SET search_path TO public;
CREATE TABLE dummy (
    id integer,
    name character varying(255)
);
INSERT INTO dummy (id,name) VALUES (1, 'dummy1');
INSERT INTO dummy (id,name) VALUES (2, 'dummy2');
INSERT INTO dummy (id,name) VALUES (3, 'dummy3');
