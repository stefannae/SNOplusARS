/*
SNO+ Run Selection Automated Checks Database Model
*/

/* Create the tables storing the DQLL and DQHL data outputed by the corresponding nearline clients.*/
CREATE TABLE dqll_data (
  run_number int primary key,
  data jsonb not null
  );
CREATE INDEX on dqll_data USING gin (data); -- or gist? test...

CREATE TABLE dqhl_data (
  run_number int primary key,
  data jsonb not null
  );
CREATE INDEX on dqhl_data USING gin (data); -- or gist? test...

/* Create the table for storing the thresholds used in the RS checks.*/
CREATE TABLE rs_criteria (
  /* Fields used for tracking criteria */
  id SERIAL primary key,
  validity int4range not null,
  /* for DQLL */
  duration int not null,
  hv_dac_a int[19] not null,
  hv_status_a boolean[19] not null,
  hv_dac_b int not null,
  hv_status_b boolean not null,
  alarms int not null,
  /* for DQHL */
  /* Trigger Processor */
  n100l_trig_rate boolean not null,
  esumh_trig_rate boolean not null,
  /* Time Processor */
  ev_rate boolean not null,
  ev_sep boolean not null,
  retrig boolean not null,
  run_hdr boolean not null,
  clock_comp boolean not null,
  clock_fwd boolean not null,
  /* Run Processor */
  run_type boolean not null,
  mc_flag boolean not null,
  run_trig boolean not null,
  /* PMT Processor */
  gen_cvrg boolean not null,
  crt_cvrg boolean not null,
  pnl_cvrg boolean not null
  );

/* \dt */

/* Insert test criteria */
INSERT INTO rs_criteria (validity,duration,hv_dac_a,hv_dac_b,hv_status_a,hv_status_b,alarms,n100l_trig_rate,esumh_trig_rate,ev_rate,ev_sep,retrig,run_hdr,clock_comp,clock_fwd,run_type,mc_flag,run_trig,gen_cvrg,crt_cvrg,pnl_cvrg)
       VALUES ('[100000,2147483646]',800, '{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}', 2000, '{true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true}', true, 1, false, false, false, true, true, true, true, true, true, true, true, false, true, true);
INSERT INTO rs_criteria (validity,duration,hv_dac_a,hv_dac_b,hv_status_a,hv_status_b,alarms,n100l_trig_rate,esumh_trig_rate,ev_rate,ev_sep,retrig,run_hdr,clock_comp,clock_fwd,run_type,mc_flag,run_trig,gen_cvrg,crt_cvrg,pnl_cvrg)
       VALUES ('[100000,2100400600]',1600, '{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}', 1, '{true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true}', true, 0, true, true, true, true, true, true, true, true, true, true, true, true, true, true);

SELECT * FROM rs_criteria;

/* Create tables for storing the data from the primary tables dqll_data and dqhl_data that will be used in doing the RS checks.*/

/* for DQLL */
CREATE TABLE dqll (
  run_number int primary key references dqll_data
    on update cascade
    on delete cascade,
  duration int not null,
  hv_status_a boolean ARRAY[19] not null,
  hv_status_b boolean not null,
  hv_dac_a int ARRAY[19] not null,
  hv_dac_b int not null,
  alarm_hv_current_zero_a int ARRAY[19] not null,
  alarm_hv_current_zero_b int not null,
  alarm_hv_over_current_a int ARRAY[19] not null,
  alarm_hv_over_current_b int not null,
  alarm_hv_setpoint_a int ARRAY[19] not null,
  alarm_hv_setpoint_b int not null
  );

CREATE INDEX ON dqll (duration DESC); -- sorting only works on btree index (the default)

/* Function for extracting the DQLL data used in the checks from dqll_data table into dqll table.*/
CREATE OR REPLACE FUNCTION dqll_insert() RETURNS TRIGGER
  SECURITY DEFINER
  LANGUAGE plpgsql
AS $$
  BEGIN
    INSERT INTO dqll
      SELECT new.run_number::int,

             (new.data->>'duration')::int as duration,

             ARRAY(SELECT (jsonb_array_elements(new.data->'crate_hv_status_a'))::text::boolean) as hv_status_a,

             (new.data->>'crate_16_hv_status_b')::boolean as hv_status_b,

             ARRAY(SELECT (jsonb_array_elements(new.data->'crate_hv_dac_a'))::text::int) as hv_dac_a,

             (new.data->>'crate_16_hv_dac_b')::int as hv_dac_b,

             ARRAY(SELECT (jsonb_array_elements(new.data#>'{detector_db_alarms,HV_current_near_zero_A}'))::text::int) as alarm_hv_current_zero_a,

             (new.data#>>'{detector_db_alarms,HV_current_near_zero_B,0}')::int as alarm_hv_current_zero_b,

             ARRAY(SELECT (jsonb_array_elements(new.data#>'{detector_db_alarms,HV_over_current_A}'))::text::int) as alarm_hv_over_current_a,

             (new.data#>>'{detector_db_alarms,HV_over_current_B,0}')::int as alarm_hv_over_current_b,

             ARRAY(SELECT (jsonb_array_elements(new.data#>'{detector_db_alarms,HV_setpoint_discrepancy_A}'))::text::int) as alarm_hv_setpoint_a,

             (new.data#>>'{detector_db_alarms,HV_setpoint_discrepancy_B,0}')::int as alarm_hv_setpoint_b;
    RETURN new;
  END;
$$;

/* Trigger for extracting the DQLL data used in the checks from dqll_data table into dqll table using the dqll_insert function.*/
CREATE TRIGGER dqll_insert AFTER INSERT ON dqll_data
  FOR EACH ROW EXECUTE PROCEDURE dqll_insert();

/* SELECT * FROM dqll_data; */
/* SELECT * FROM dqll; */

/* for DQHL */
CREATE TABLE dqhl (
  run_number int primary key references dqhl_data
    on update cascade
    on delete cascade,

  n100l_trig_rate boolean not null,
  esumh_trig_rate boolean not null,

  ev_rate boolean not null,
  ev_sep boolean not null,
  retrig boolean not null,
  run_hdr boolean not null,
  clock_comp boolean not null,
  clock_fwd boolean not null,

  run_type boolean not null,
  mc_flag boolean not null,
  run_trig boolean not null,

  gen_cvrg boolean not null,
  crt_cvrg boolean not null,
  pnl_cvrg boolean not null
  );

/* Function for extracting the DQHL data used in the checks from dqhl_data table into dqhl table.*/
CREATE OR REPLACE FUNCTION dqhl_insert() RETURNS TRIGGER
  SECURITY DEFINER
  LANGUAGE plpgsql
AS $$
  BEGIN
    INSERT INTO dqhl
        SELECT new.run_number::int,
               /*TRIGGER PROC*/
               (new.data#>>'{checks,dqtriggerproc,n100l_trigger_rate}')::boolean as n100l_trig_rate,
               (new.data#>>'{checks,dqtriggerproc,esumh_trigger_rate}')::boolean as esumh_trig_rate,

               /*TIME PROC*/
               (new.data#>>'{checks,dqtimeproc,event_rate}')::boolean as ev_rate,
               (new.data#>>'{checks,dqtimeproc,event_separation}')::boolean as ev_sep,
               (new.data#>>'{checks,dqtimeproc,retriggers}')::boolean as retrig,
               (new.data#>>'{checks,dqtimeproc,run_header}')::boolean as run_hdr,
               (new.data#>>'{checks,dqtimeproc,10Mhz_UT_comparrison}')::boolean as clock_comp,
               (new.data#>>'{checks,dqtimeproc,clock_forward}')::boolean as clock_fwd,

               /*RUN PROC*/
               (new.data#>>'{checks,dqrunproc,run_type}')::boolean as run_type,
               (new.data#>>'{checks,dqrunproc,mc_flag}')::boolean as mc_flag,
               (new.data#>>'{checks,dqrunproc,trigger}')::boolean as run_trig,

               /*PMT PROC*/
               (new.data#>>'{checks,dqpmtproc,general_coverage}')::boolean as gen_cvrg,
               (new.data#>>'{checks,dqpmtproc,crate_coverage}')::boolean as crt_cvrg,
               (new.data#>>'{checks,dqpmtproc,panel_coverage}')::boolean as pnl_cvrg;
    RETURN new;
  END;
$$;

/* Trigger for extracting the DQHL data used in the checks from dqhl_data table into dqhl table using the dqhl_insert function.*/
CREATE TRIGGER dqhl_insert AFTER INSERT ON dqhl_data
  FOR EACH ROW EXECUTE PROCEDURE dqhl_insert();

/* SELECT * FROM dqhl_data; */
/* SELECT * FROM dqhl; */

--
-- Functions for DQLL related checks
--
CREATE OR REPLACE FUNCTION duration(
    duration_run integer,
    duration_check integer,
    OUT result boolean)
AS $$
BEGIN
  IF duration_run >= duration_check THEN
    result := TRUE;
  ELSE
    result := FALSE;
  END IF;
  -- RETURN result;
END; $$
LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION hv_dac_a(
    input_val integer[19],
    check_val integer[19],
    OUT result boolean)
AS $$
BEGIN
  result := TRUE;
  FOR counter IN 0..18 LOOP
    IF input_val[counter] < check_val[counter] THEN
      result := FALSE;
      EXIT;
    END IF;
  END LOOP;
  -- RETURN result;
END; $$
LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION hv_dac_b(
    input_val integer,
    check_val integer,
    OUT result boolean)
AS $$
BEGIN
  IF input_val >= check_val THEN
    result := TRUE;
  ELSE
    result := FALSE;
  END IF;
  -- RETURN result;
END; $$
LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION hv_status_a(
    input_val boolean[19],
    check_val boolean[19],
    OUT result boolean)
AS $$
BEGIN
  result := TRUE;
  FOR counter IN 0..18 LOOP
    IF input_val[counter] != check_val[counter] THEN
      result := FALSE;
      EXIT;
    END IF;
  END LOOP;
  -- RETURN result;
END; $$
LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION hv_status_b(
    input_val boolean,
    check_val boolean,
    OUT result boolean)
AS $$
BEGIN
  IF input_val = check_val THEN
    result := TRUE;
  ELSE
    result := FALSE;
  END IF;
  -- RETURN result;
END; $$
LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION hv_a_alarms(
    input_val integer[19],
    check_val integer,
    OUT result boolean)
AS $$
BEGIN
  result := TRUE;
  FOR counter IN 0..18 LOOP
    IF input_val[counter] > check_val THEN
      result := FALSE;
      EXIT;
    END IF;
  END LOOP;
  -- RETURN result;
END; $$
LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION hv_b_alarms(
    input_val integer,
    check_val integer,
    OUT result boolean)
AS $$
BEGIN
  IF input_val > check_val THEN
    result := FALSE;
  ELSE
    result := TRUE;
  END IF;
  -- RETURN result;
END; $$
LANGUAGE plpgsql STRICT;

--
-- function for DQHL related checks
--

/* This function is the same as hv_status_b for DQLL checks, it will be replaced when proper threshold will be provided for DQHL.*/
CREATE OR REPLACE FUNCTION hl_check(
    input_val boolean,
    check_val boolean,
    OUT result boolean)
AS $$
BEGIN
  IF input_val = check_val THEN
    result := TRUE;
  ELSE
    result := FALSE;
  END IF;
  -- RETURN result;
END; $$
LANGUAGE plpgsql STRICT;

/* Create table for keeping a list of possible fail modes.*/
CREATE TABLE fail_modes (
  id SERIAL primary key,
  name text not null,
  description text
  );

/* List the column names associated with RS checks. All the columns from the rs_criteria table.*/
/* SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'rs_criteria' AND TABLE_SCHEMA='public' AND COLUMN_NAME != 'id'; */

/* Initialize the fail modes with the rs_criteria column names.*/
INSERT INTO fail_modes (name)
  SELECT COLUMN_NAME
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_NAME = 'rs_criteria' AND TABLE_SCHEMA='public' AND COLUMN_NAME != 'id';

/* SELECT * FROM fail_modes; */

/* Create a table for storing the names of lists a run belongs to.*/
CREATE TABLE run_list (
  id SERIAL primary key,
  name text not null
  );

/* Initialize the run list names.*/
INSERT INTO run_list (name) VALUES
  ('GOLD'),
  ('MINUS_ONE_CRATE'),
  ('ANTINU'),
  ('SN');

/* SELECT * FROM run_list; */

/* Create a table for storing the members of the RS group.*/
CREATE TABLE rs_group (
  id SERIAL primary key,
  name text not null,
  email text not null
  );

/* Initialize the RS group table with it's members.*/
INSERT INTO rs_group (name, email) VALUES
  ('Ian Coulter','icoulter@hep.upenn.edu'),
  ('Eric Marzec','marzece@hep.upenn.edu'),
  ('Gersende Prior','gersende@lip.pt'),
  ('Stefan Nae', 'stefan@lip.pt'),
  ('Janet Rumleskie','jrrumles@snolab.ca'),
  ('Lisa Falk','e.falk@sussex.ac.uk'),
  ('Mark Stringer','ms711@sussex.ac.uk'),
  ('Miro Mlejnek','mm679@sussex.ac.uk'),
  ('Francesca Di Lodivico','f.di.lodovico@qmul.ac.uk'),
  ('Jeanne Wilson','j.r.wilson@qmul.ac.uk'),
  ('Ed Leming','e.leming09@googlemail.com'),
  ('Kalpana Singh','kalpana.singh@ualberta.ca'),
  ('Erica Caden','ecaden@snolab.ca');

/* SELECT * FROM rs_group; */

/* This table keeps track of the RS checks. It flags the sections that need to be reviewed for new tables and when criteria change.*/
CREATE TABLE rs_status (
  run_number int primary key,
  /* If needs_review is false it means that it was reviewd and, if any, threshold changes do not affect this run. If need_review is true the run was not reviewed or threshold changes put the run in a pending review state. What should be the status if the table is not present, false?*/
  needs_review boolean not null,
  needs_review_ll boolean not null,
  needs_review_hl boolean not null,
  tstamp timestamp default (now() at time zone 'utc')
  );

/* This table stores the result of the RS shift.*/
CREATE TABLE rs_report (
  run_number int primary key references rs_status,
  dqll_ok boolean not null,
  dqll_note text,
  dqhl_ok boolean not null,
  dqhl_note text,
  sr_ok boolean not null,
  sr_note text,
  det_ok boolean not null,
  det_note text,
  decision boolean not null,
  decision_note text,
  run_list_id int references run_list,
  fail_modes int array,
  signature int not null references rs_group,
  completeness boolean not null,
  --completeness boolean default false,
  --t timestamp with time zone
  t timestamp default (now() at time zone 'utc')
  );

CREATE INDEX ON rs_report USING brin (t); -- or maybe the default btree should be used with DESC? I should create an index which will facilitate statistics from this table.
CREATE INDEX ON rs_report (run_list_id);
CREATE INDEX ON rs_report (fail_modes);

/* Insert test report. It will not work if there is no data for the particular run number. */
/* INSERT INTO rs_report (run_number, dqll_ok, dqll_note, dqhl_ok, dqhl_note, sr_ok, sr_note, det_ok, det_note, decision, decision_note, run_list_id, fail_modes, signature, completeness)
VALUES (105340, true, '', true, '', false, '', false, '', false, 'detector turned off', NULL, NULL, 7, true); */

/* Tables for storing automated checks results on the DQLL and DQHL tables. These tables will exist for any implementation of the checks, be it in the database as is the case here or as destination for the run-selection checks scrips and/or the snopl.us views for run selection.*/
CREATE TABLE rs_checks_table_ll (
  run_number int primary key references dqll
      on update cascade
      on delete cascade,
  duration boolean,
  hv_dac_a boolean,
  hv_dac_b boolean,
  hv_status_a boolean,
  hv_status_b boolean,
  alarm_hv_I_zero_a boolean,
  alarm_hv_I_zero_b boolean,
  alarm_hv_over_I_a boolean,
  alarm_hv_over_I_b boolean,
  alarm_hv_setpt_a boolean,
  alarm_hv_setpt_b boolean
  );

CREATE TABLE rs_checks_table_hl (
  run_number int primary key references dqhl
      on update cascade
      on delete cascade,
  /*  */
  n100l_trig_rate boolean,
  esumh_trig_rate boolean,
  /*  */
  ev_rate boolean,
  ev_sep boolean,
  retrig boolean,
  run_hdr boolean,
  clock_comp boolean,
  clock_fwd boolean,
  /*  */
  run_type boolean,
  mc_flag boolean,
  run_trig boolean,
  /*  */
  gen_cvrg boolean,
  crt_cvrg boolean,
  pnl_cvrg boolean
  );

/* Function used in monitoring the status of run selection, stored in the rs_status, with respect to changes in the database.*/
CREATE OR REPLACE FUNCTION rs_status_monitor() RETURNS TRIGGER
  SECURITY DEFINER
  LANGUAGE plpgsql
AS $$
  BEGIN
    IF (TG_TABLE_NAME = 'rs_checks_table_ll') THEN
      IF (TG_OP = 'DELETE') THEN
        UPDATE rs_status SET needs_review = true, needs_review_ll = true, tstamp = (now() at time zone 'utc')
        WHERE run_number = OLD.run_number;
        RETURN OLD;
      ELSIF (TG_OP = 'UPDATE') THEN
        IF NEW IS DISTINCT FROM OLD THEN
          UPDATE rs_status SET needs_review = true, needs_review_ll = true, tstamp = (now() at time zone 'utc')
          WHERE run_number = NEW.run_number;
        END IF;
        RETURN NEW;
      ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO rs_status (run_number, needs_review, needs_review_ll, needs_review_hl, tstamp)
          VALUES (NEW.run_number, true, true, false, (now() at time zone 'utc'))
        ON CONFLICT (run_number) DO UPDATE
          SET needs_review = true, needs_review_ll = true, tstamp = (now() at time zone 'utc');
        RETURN NEW;
      END IF;


    ELSIF (TG_TABLE_NAME = 'rs_checks_table_hl') THEN
      IF (TG_OP = 'DELETE') THEN
        UPDATE rs_status SET needs_review = true, needs_review_hl = true, tstamp = (now() at time zone 'utc')
        WHERE run_number = OLD.run_number;
        RETURN OLD;
      ELSIF (TG_OP = 'UPDATE') THEN
        IF NEW IS DISTINCT FROM OLD THEN
          UPDATE rs_status SET needs_review = true, needs_review_hl = true, tstamp = (now() at time zone 'utc')
          WHERE run_number = NEW.run_number;
        END IF;
        RETURN NEW;
      ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO rs_status (run_number, needs_review, needs_review_ll, needs_review_hl, tstamp)
          VALUES (NEW.run_number, true, false, true, (now() at time zone 'utc'))
        ON CONFLICT (run_number) DO UPDATE
          SET needs_review = true, needs_review_hl = true, tstamp = (now() at time zone 'utc');
        RETURN NEW;
      END IF;
    END IF;

  END;
$$;

CREATE TRIGGER rs_status_ll AFTER INSERT OR UPDATE OR DELETE ON rs_checks_table_ll
  FOR EACH ROW EXECUTE PROCEDURE rs_status_monitor();

CREATE TRIGGER rs_status_hl AFTER INSERT OR UPDATE OR DELETE ON rs_checks_table_hl
  FOR EACH ROW EXECUTE PROCEDURE rs_status_monitor();

/* Function used for doing the DQLL checks.*/
CREATE OR REPLACE FUNCTION rs_checks_add_ll() RETURNS TRIGGER
  SECURITY DEFINER
  LANGUAGE plpgsql
AS $$
  BEGIN
    INSERT INTO rs_checks_table_ll
      WITH x AS (
        SELECT * FROM rs_criteria WHERE validity @> new.run_number ORDER BY id DESC LIMIT 1
        )
      SELECT new.run_number AS run_number,
             duration(new.duration, x.duration) AS duration,
             hv_dac_a(new.hv_dac_a, x.hv_dac_a) AS hv_dac_a,
             hv_dac_b(new.hv_dac_b, x.hv_dac_b) AS hv_dac_b,
             hv_status_a(new.hv_status_a, x.hv_status_a) AS hv_status_a,
             hv_status_b(new.hv_status_b, x.hv_status_b) AS hv_status_b,
             hv_a_alarms(new.alarm_hv_current_zero_a, x.alarms) AS alarm_hv_I_zero_a,
             hv_b_alarms(new.alarm_hv_current_zero_b, x.alarms) AS alarm_hv_I_zero_b,
             hv_a_alarms(new.alarm_hv_over_current_a, x.alarms) AS alarm_hv_over_I_a,
             hv_b_alarms(new.alarm_hv_over_current_b, x.alarms) AS alarm_hv_over_I_b,
             hv_a_alarms(new.alarm_hv_setpoint_a, x.alarms) AS alarm_hv_setpt_a,
             hv_b_alarms(new.alarm_hv_setpoint_b, x.alarms) AS alarm_hv_setpt_b
      FROM x;
    RETURN new;
  END;
$$;

CREATE TRIGGER rs_checks_add_ll AFTER INSERT ON dqll
  FOR EACH ROW EXECUTE PROCEDURE rs_checks_add_ll();

/* Inser test DQLL data */
INSERT INTO dqll_data (run_number, data) VALUES (105340, '{"type": "DQLL", "duration": 3601, "crate_hv_dac_a": [2805, 2995, 2789, 2895, 2747, 2890, 2910, 2891, 2760, 3233, 2992, 3209, 2988, 3032, 2631, 2678, 2745, 2624, 2701], "crate_16_hv_dac_b": 3335, "crate_hv_status_a": [true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true], "crate_16_hv_status_b": true, "detector_db_alarms": {"HV_current_near_zero_A": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], "HV_current_near_zero_B": [0], "HV_over_current_A": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], "HV_setpoint_discrepancy_B": [0], "HV_setpoint_discrepancy_A": [0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0], "HV_over_current_B": [0]}}');

/* SELECT * FROM dqll_data; */
/* SELECT * FROM dqll; */

INSERT INTO dqll_data (run_number, data) VALUES (106001, '{"type": "DQLL", "duration": 2421, "crate_hv_dac_a": [2807, 2995, 2789, 2895, 2747, 2890, 2910, 2891, 2760, 3233, 2992, 3209, 2988, 3032, 2631, 2678, 2745, 2624, 2701], "crate_16_hv_dac_b": 3337, "crate_hv_status_a": [true, true, true, true, false, true, true, true, true, true, true, true, true, true, true, true, true, true, true], "crate_16_hv_status_b": false, "detector_db_alarms": {"HV_current_near_zero_A": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], "HV_current_near_zero_B": [0], "HV_over_current_A": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], "HV_setpoint_discrepancy_B": [0], "HV_setpoint_discrepancy_A": [0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], "HV_over_current_B": [0]}}');

/* SELECT * FROM dqll_data; */
/* SELECT * FROM dqll; */

INSERT INTO dqll_data (run_number, data) VALUES (101025, '{"type": "DQLL", "duration": 315, "crate_hv_dac_a": [2800, 2995, 2789, 2895, 2747, 2890, 2910, 2891, 2760, 3233, 2992, 3209, 2988, 3032, 2631, 2678, 2745, 2624, 2701], "crate_16_hv_dac_b": 0, "crate_hv_status_a": [true, true, true, true, true, true, true, true, true, true, false, true, true, true, true, true, false, true, true], "crate_16_hv_status_b": true, "detector_db_alarms": {"HV_current_near_zero_A": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], "HV_current_near_zero_B": [0], "HV_over_current_A": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], "HV_setpoint_discrepancy_B": [1], "HV_setpoint_discrepancy_A": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], "HV_over_current_B": [0]}}');

/* SELECT * FROM dqll_data; */
/* SELECT * FROM dqll; */

INSERT INTO dqll_data (run_number, data) VALUES (104075, '{"type": "DQLL", "duration": 1106, "crate_hv_dac_a": [2801, 2995, 2789, 2895, 2747, 2890, 2910, 2891, 2760, 3233, 2992, 3209, 2988, 3032, 2631, 2678, 2745, 2624, 2701], "crate_16_hv_dac_b": 3330, "crate_hv_status_a": [true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true], "crate_16_hv_status_b": true, "detector_db_alarms": {"HV_current_near_zero_A": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], "HV_current_near_zero_B": [0], "HV_over_current_A": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], "HV_setpoint_discrepancy_B": [0], "HV_setpoint_discrepancy_A": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], "HV_over_current_B": [0]}}');

/* SELECT * FROM dqll_data; */
/* SELECT * FROM dqll; */
/* SELECT * FROM rs_status; */
SELECT * FROM rs_checks_table_ll;

/* Function used for doing the DQHL checks.*/
CREATE OR REPLACE FUNCTION rs_checks_add_hl() RETURNS TRIGGER
  SECURITY DEFINER
  LANGUAGE plpgsql
AS $$
  BEGIN
    INSERT INTO rs_checks_table_hl
      WITH x AS (
        SELECT * FROM rs_criteria WHERE validity @> new.run_number ORDER BY id DESC LIMIT 1
        )
      SELECT new.run_number AS run_number,
        hl_check(new.n100l_trig_rate, x.n100l_trig_rate) AS n100l_trig_rate,
        hl_check(new.esumh_trig_rate, x.esumh_trig_rate) AS esumh_trig_rate,
        --
        hl_check(new.ev_rate, x.ev_rate) AS ev_rate,
        hl_check(new.ev_sep, x.ev_sep) AS ev_sep,
        hl_check(new.retrig, x.retrig) AS retrig,
        hl_check(new.run_hdr, x.run_hdr) AS run_hdr,
        hl_check(new.clock_comp, x.clock_comp) AS clock_comp,
        hl_check(new.clock_fwd, x.clock_fwd) AS clock_fwd,
        --
        hl_check(new.run_type, x.run_type) AS run_type,
        hl_check(new.mc_flag, x.mc_flag) AS mc_flag,
        hl_check(new.run_trig, x.run_trig) AS run_trig,
        --
        hl_check(new.gen_cvrg, x.gen_cvrg) AS gen_cvrg,
        hl_check(new.crt_cvrg, x.crt_cvrg) AS crt_cvrg,
        hl_check(new.pnl_cvrg, x.pnl_cvrg) AS pnl_cvrg
      FROM x;
    RETURN new;
  END;
$$;

CREATE TRIGGER rs_checks_add_hl AFTER INSERT ON dqhl
  FOR EACH ROW EXECUTE PROCEDURE rs_checks_add_hl();

/* Inser test DQHL data */
INSERT INTO dqhl_data (run_number, data) VALUES (103931, '{"_id":"6c392e6f01ae07e135db48f7660f0365","_rev":"1-5b7354216b35b6e72e01ec05e736dc1a","comment":"Created by SNO+ DQ. Processed run 103931 with the following checks: dqrunproc, dqtimeproc, dqtriggerproc, dqpmtproc","index":"","version":2,"timestamp":"2017-08-10T18:34:10Z","run_range":[103931,103931],"production":false,"pass":-1,"type":"DATAQUALITY_RECORDS","checks":{"dqtriggerproc":{"triggerProcMissingGTID":1,"triggerProcBitFlipGTID":1,"check_params":{"orphans_count":1,"n100l_trigger_rate":59.576670454631901919,"bitflip_gtids":[],"esumh_trigger_rate":17.260107410723598775,"missing_gtids":[]},"esumh_trigger_rate":1,"criteria":{"min_esum_hi_rate":5,"max_num_bitflip_gtids":0,"min_nhit100l_rate":5,"max_num_missing_gtids":0},"n100l_trigger_rate":1},"status_mask":"0xc020000c01dee","dqrunproc":{"run_type":1,"trigger":1,"check_params":{"universal_time_run_length":2870,"count_50_run_length":2870.2750115200001346,"count_10_run_length":2870.3181748000001789,"mean_nhit":7.2202763557434099795,"run_length":2870.3181748000001789,"run_length_source":"count_10"},"mc_flag":1,"criteria":{"trigger_check_thresh":90,"mc_flag_criteria":0,"trigger_check_criteria":33582814}},"dqtimeproc":{"event_rate":0,"event_separation":1,"check_params":{"event_rate_agreement":-0.10438385776418999951,"num_UT_10MhzClock_comp_fails":0,"mean_event_rate":1626.0584073837799224,"delta_t_event_rate":1627.7575234818400531,"max_event_rate":1200},"retriggers":1,"criteria":{"retriggers_thresh":10,"run_header_thresh":1000000000,"min_event_rate":5,"clock_forward_thresh":99,"max_event_rate":1200,"event_separation_thresh":1},"10Mhz_UT_comparrison":1,"clock_forward":1,"run_header":1},"dqpmtproc":{"general_coverage":1,"panel_coverage":1,"crate_coverage":1,"check_params":{"number_of_panels_failing_coverage":7,"percentage_of_panels_passing_coverage":99.067909454061293673,"overall_detector_coverage":95.921192758253496891,"crates_failing_coverage":0,"crates_coverage_percentage":100},"criteria":{"panel_cov_thresh":80,"general_cov_thresh":70,"crate_cov_thresh":100,"in_crate_cov_thresh":50}},"applied_mask":"0xc020000c03dee"}}');

/* SELECT * FROM dqhl_data; */
/* SELECT * FROM dqhl; */

INSERT INTO dqhl_data (run_number, data) VALUES (102540, '{"_id":"3522cecb97f6239b921c80b1ae10fd4d","_rev":"1-b6881dc0c929f31a3a0ce700c98b6ab0","comment":"Created by SNO+ DQ. Processed run 102540 with the following checks: dqrunproc, dqtimeproc, dqtriggerproc, dqpmtproc","index":"","version":2,"timestamp":"2017-07-15T20:58:03Z","run_range":[102540,102540],"production":false,"pass":-1,"type":"DATAQUALITY_RECORDS","checks":{"dqtriggerproc":{"triggerProcMissingGTID":1,"triggerProcBitFlipGTID":1,"check_params":{"orphans_count":7,"n100l_trigger_rate":72.718037054381596818,"bitflip_gtids":[],"esumh_trigger_rate":15.749961443101799574,"missing_gtids":[]},"esumh_trigger_rate":1,"criteria":{"min_esum_hi_rate":5,"max_num_bitflip_gtids":0,"min_nhit100l_rate":5,"max_num_missing_gtids":0},"n100l_trigger_rate":1},"status_mask":"0xc020000c01dee","dqrunproc":{"run_type":1,"trigger":1,"check_params":{"universal_time_run_length":3602,"count_50_run_length":3601.5474742000001243,"count_10_run_length":3601.5961185000001024,"mean_nhit":6.3208827972412100493,"run_length":3601.5961185000001024,"run_length_source":"count_10"},"mc_flag":1,"criteria":{"trigger_check_thresh":90,"mc_flag_criteria":0,"trigger_check_criteria":33582814}},"dqtimeproc":{"event_rate":0,"event_separation":1,"check_params":{"event_rate_agreement":-0.0024177027043509498327,"num_UT_10MhzClock_comp_fails":0,"mean_event_rate":1474.7517004244600685,"delta_t_event_rate":1474.7873563982600444,"max_event_rate":1200},"retriggers":1,"criteria":{"retriggers_thresh":10,"run_header_thresh":1000000000,"min_event_rate":5,"clock_forward_thresh":99,"max_event_rate":1200,"event_separation_thresh":1},"10Mhz_UT_comparrison":1,"clock_forward":1,"run_header":1},"dqpmtproc":{"general_coverage":1,"panel_coverage":1,"crate_coverage":1,"check_params":{"number_of_panels_failing_coverage":24,"percentage_of_panels_passing_coverage":96.804260985352897251,"overall_detector_coverage":93.418530351437695458,"crates_failing_coverage":0,"crates_coverage_percentage":100},"criteria":{"panel_cov_thresh":80,"general_cov_thresh":70,"crate_cov_thresh":100,"in_crate_cov_thresh":50}},"applied_mask":"0xc020000c03dee"}}');

/* SELECT * FROM dqhl_data; */
/* SELECT * FROM dqhl; */

INSERT INTO dqhl_data (run_number, data) VALUES (105340, '{"_id":"8911fb0218fe2c4c8f53330f9ba9626e","_rev":"1-c74d68e9916b0bb9e3b0e4509bbc7246","comment":"Created by SNO+ DQ. Processed run 105340 with the following checks: dqrunproc, dqtimeproc, dqtriggerproc, dqpmtproc","index":"","version":2,"timestamp":"2017-09-23T06:57:14Z","run_range":[105340,105340],"production":false,"pass":-1,"type":"DATAQUALITY_RECORDS","checks":{"dqtriggerproc":{"triggerProcMissingGTID":1,"triggerProcBitFlipGTID":1,"check_params":{"orphans_count":15,"n100l_trigger_rate":721.62393903074905666,"bitflip_gtids":[],"esumh_trigger_rate":14.588208875167699929,"missing_gtids":[]},"esumh_trigger_rate":1,"criteria":{"min_esum_hi_rate":5,"max_num_bitflip_gtids":0,"min_nhit100l_rate":5,"max_num_missing_gtids":0},"n100l_trigger_rate":1},"status_mask":"0xc020000c03dee","dqrunproc":{"run_type":1,"trigger":1,"check_params":{"universal_time_run_length":3602,"count_50_run_length":3601.6925542200001473,"count_10_run_length":3601.7444258999998965,"mean_nhit":11.367016792297400585,"run_length":3601.7444258999998965,"run_length_source":"count_10"},"mc_flag":1,"criteria":{"trigger_check_thresh":90,"mc_flag_criteria":0,"trigger_check_criteria":33580767}},"dqtimeproc":{"event_rate":1,"event_separation":1,"check_params":{"event_rate_agreement":-0.0023526953797368401963,"num_UT_10MhzClock_comp_fails":0,"mean_event_rate":792.38798274445900915,"delta_t_event_rate":792.40662565852903754},"retriggers":1,"criteria":{"retriggers_thresh":10,"run_header_thresh":1000000000,"min_event_rate":5,"clock_forward_thresh":99,"max_event_rate":7000,"event_separation_thresh":1},"10Mhz_UT_comparrison":1,"clock_forward":1,"run_header":1},"dqpmtproc":{"general_coverage":1,"panel_coverage":1,"crate_coverage":1,"check_params":{"number_of_panels_failing_coverage":31,"percentage_of_panels_passing_coverage":95.872170439414105658,"overall_detector_coverage":93.077742279020199589,"crates_failing_coverage":0,"crates_coverage_percentage":100},"criteria":{"panel_cov_thresh":80,"general_cov_thresh":70,"crate_cov_thresh":100,"in_crate_cov_thresh":50}},"applied_mask":"0xc020000c03dee"}}');

/* SELECT * FROM dqhl_data; */
/* SELECT * FROM dqhl; */

INSERT INTO dqhl_data (run_number, data) VALUES (106001, '{"_id":"18aa68549e0568660557fe43c97bd960","_rev":"1-ee6e5e042e24d347e47b95389df4f823","comment":"Created by SNO+ DQ. Processed run 106001 with the following checks: dqrunproc, dqtimeproc, dqtriggerproc, dqpmtproc","index":"","version":2,"timestamp":"2017-10-17T07:59:15Z","run_range":[106001,106001],"production":false,"pass":-1,"type":"DATAQUALITY_RECORDS","checks":{"dqtriggerproc":{"triggerProcMissingGTID":1,"triggerProcBitFlipGTID":1,"check_params":{"orphans_count":24,"n100l_trigger_rate":662.70744992352399549,"bitflip_gtids":[],"esumh_trigger_rate":11.545479123941300159,"missing_gtids":[]},"esumh_trigger_rate":1,"criteria":{"min_esum_hi_rate":5,"max_num_bitflip_gtids":0,"min_nhit100l_rate":5,"max_num_missing_gtids":0},"n100l_trigger_rate":1},"status_mask":"0xc020000c03dee","dqrunproc":{"run_type":1,"trigger":1,"check_params":{"universal_time_run_length":3605,"count_50_run_length":3604.3885958000000755,"count_10_run_length":3604.4411455999997997,"mean_nhit":10.478493690490699564,"run_length":3604.4411455999997997,"run_length_source":"count_10"},"mc_flag":1,"criteria":{"trigger_check_thresh":90,"mc_flag_criteria":0,"trigger_check_criteria":33580767}},"dqtimeproc":{"event_rate":1,"event_separation":1,"check_params":{"event_rate_agreement":-0.0027831864562255200293,"num_UT_10MhzClock_comp_fails":0,"mean_event_rate":683.92238919180499579,"delta_t_event_rate":683.94142455690200677},"retriggers":1,"criteria":{"retriggers_thresh":10,"run_header_thresh":1000000000,"min_event_rate":5,"clock_forward_thresh":99,"max_event_rate":7000,"event_separation_thresh":1},"10Mhz_UT_comparrison":1,"clock_forward":1,"run_header":1},"dqpmtproc":{"general_coverage":1,"panel_coverage":1,"crate_coverage":1,"check_params":{"number_of_panels_failing_coverage":39,"percentage_of_panels_passing_coverage":94.806924101198404742,"overall_detector_coverage":91.991480298189600262,"crates_failing_coverage":0,"crates_coverage_percentage":100},"criteria":{"panel_cov_thresh":80,"general_cov_thresh":70,"crate_cov_thresh":100,"in_crate_cov_thresh":50}},"applied_mask":"0xc020000c03dee"}}');

/* SELECT * FROM dqhl_data; */
/* SELECT * FROM dqhl; */
/* SELECT * FROM rs_status; */
SELECT * FROM rs_checks_table_hl;

/* Functions used in monitoring changes in the results of the checks when criteria change.*/
/* for DQLL */
CREATE OR REPLACE FUNCTION rs_checks_monitor_ll() RETURNS TRIGGER
  SECURITY DEFINER
  LANGUAGE plpgsql
AS $$
  DECLARE
    run_ck_l rs_checks_table_ll%rowtype;
    run_dt_l dqll%rowtype;
    run_no RECORD;
    run_data RECORD;
    temp_checks RECORD;
    old_checks RECORD;
    run int;
  BEGIN

    FOR run_data IN
      WITH run_list AS (
        SELECT NEW.validity
        )
      SELECT * FROM dqll, rs_checks_table_ll, run_list
      WHERE dqll.run_number <@ run_list.validity
    LOOP
      SELECT
        run_data.run_number AS run_number,
        duration(run_data.duration, NEW.duration) AS duration,
        hv_dac_a(run_data.hv_dac_a, NEW.hv_dac_a) AS hv_dac_a,
        hv_dac_b(run_data.hv_dac_b, NEW.hv_dac_b) AS hv_dac_b,
        hv_status_a(run_data.hv_status_a, NEW.hv_status_a) AS hv_status_a,
        hv_status_b(run_data.hv_status_b, NEW.hv_status_b) AS hv_status_b,
        hv_a_alarms(run_data.alarm_hv_current_zero_a, NEW.alarms) AS alarm_hv_I_zero_a,
        hv_b_alarms(run_data.alarm_hv_current_zero_b, NEW.alarms) AS alarm_hv_I_zero_b,
        hv_a_alarms(run_data.alarm_hv_over_current_a, NEW.alarms) AS alarm_hv_over_I_a,
        hv_b_alarms(run_data.alarm_hv_over_current_b, NEW.alarms) AS alarm_hv_over_I_b,
        hv_a_alarms(run_data.alarm_hv_setpoint_a, NEW.alarms) AS alarm_hv_setpt_a,
        hv_b_alarms(run_data.alarm_hv_setpoint_b, NEW.alarms) AS alarm_hv_setpt_b
      INTO temp_checks;

      SELECT *
      INTO old_checks
      FROM rs_checks_table_ll
      WHERE run_number = run_data.run_number;

      IF ROW(temp_checks) IS DISTINCT FROM ROW(old_checks) THEN
        UPDATE rs_checks_table_ll
        SET (duration, hv_dac_a, hv_dac_b, hv_status_a, hv_status_b, alarm_hv_I_zero_a, alarm_hv_I_zero_b, alarm_hv_over_I_a, alarm_hv_over_I_b, alarm_hv_setpt_a, alarm_hv_setpt_b)
        = (temp_checks.duration, temp_checks.hv_dac_a, temp_checks.hv_dac_b, temp_checks.hv_status_a, temp_checks.hv_status_b, temp_checks.alarm_hv_I_zero_a, temp_checks.alarm_hv_I_zero_b, temp_checks.alarm_hv_over_I_a, temp_checks.alarm_hv_over_I_b, temp_checks.alarm_hv_setpt_a, temp_checks.alarm_hv_setpt_b)
        WHERE run_number = run_data.run_number;
      END IF;
    END LOOP;

    RETURN NEW;
  END;
$$;

CREATE TRIGGER rs_checks_monitor_ll AFTER INSERT OR UPDATE OR DELETE ON rs_criteria
  FOR EACH ROW EXECUTE PROCEDURE rs_checks_monitor_ll();

/* for DQHL */
CREATE OR REPLACE FUNCTION rs_checks_monitor_hl() RETURNS TRIGGER
  SECURITY DEFINER
  LANGUAGE plpgsql
AS $$
  DECLARE
    run_ck_h rs_checks_table_hl%rowtype;
    run_dt_h dqhl%rowtype;
    run_no RECORD;
    run_data RECORD;
    temp_checks RECORD;
    old_checks RECORD;
    run int;
  BEGIN

    FOR run_data IN
      WITH run_list AS (
        SELECT NEW.validity
        )
      SELECT * FROM dqhl, rs_checks_table_hl, run_list
      WHERE dqhl.run_number <@ run_list.validity
    LOOP
      SELECT
        run_data.run_number AS run_number,
        hl_check(run_data.n100l_trig_rate, NEW.n100l_trig_rate) AS n100l_trig_rate,
        hl_check(run_data.esumh_trig_rate, NEW.esumh_trig_rate) AS esumh_trig_rate,
        --
        hl_check(run_data.ev_rate, NEW.ev_rate) AS ev_rate,
        hl_check(run_data.ev_sep, NEW.ev_sep) AS ev_sep,
        hl_check(run_data.retrig, NEW.retrig) AS retrig,
        hl_check(run_data.run_hdr, NEW.run_hdr) AS run_hdr,
        hl_check(run_data.clock_comp, NEW.clock_comp) AS clock_comp,
        hl_check(run_data.clock_fwd, NEW.clock_fwd) AS clock_fwd,
        --
        hl_check(run_data.run_type, NEW.run_type) AS run_type,
        hl_check(run_data.mc_flag, NEW.mc_flag) AS mc_flag,
        hl_check(run_data.run_trig, NEW.run_trig) AS run_trig,
        --
        hl_check(run_data.gen_cvrg, NEW.gen_cvrg) AS gen_cvrg,
        hl_check(run_data.crt_cvrg, NEW.crt_cvrg) AS crt_cvrg,
        hl_check(run_data.pnl_cvrg, NEW.pnl_cvrg) AS pnl_cvrg
      INTO temp_checks;

      SELECT *
      INTO old_checks
      FROM rs_checks_table_hl
      WHERE run_number = run_data.run_number;

      IF ROW(temp_checks) IS DISTINCT FROM ROW(old_checks) THEN
        UPDATE rs_checks_table_hl
        SET (n100l_trig_rate, esumh_trig_rate, ev_rate, ev_sep, retrig, run_hdr, clock_comp, clock_fwd, run_type, mc_flag, run_trig, gen_cvrg, crt_cvrg, pnl_cvrg)
        = (temp_checks.n100l_trig_rate, temp_checks.esumh_trig_rate, temp_checks.ev_rate, temp_checks.ev_sep, temp_checks.retrig, temp_checks.run_hdr, temp_checks.clock_comp, temp_checks.clock_fwd, temp_checks.run_type, temp_checks.mc_flag, temp_checks.run_trig, temp_checks.gen_cvrg, temp_checks.crt_cvrg, temp_checks.pnl_cvrg)
        WHERE run_number = run_data.run_number;
      END IF;
    END LOOP;

    RETURN NEW;
  END;
$$;

CREATE TRIGGER rs_checks_monitor_hl AFTER INSERT OR UPDATE OR DELETE ON rs_criteria
  FOR EACH ROW EXECUTE PROCEDURE rs_checks_monitor_hl();

/* Insert test criteria to verify the checks results monitors. */
INSERT INTO rs_criteria (validity,duration,hv_dac_a,hv_dac_b,hv_status_a,hv_status_b,alarms,n100l_trig_rate,esumh_trig_rate,ev_rate,ev_sep,retrig,run_hdr,clock_comp,clock_fwd,run_type,mc_flag,run_trig,gen_cvrg,crt_cvrg,pnl_cvrg)
       VALUES ('[100000,2147483646]',200, '{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}', 2000, '{true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true}', true, 1, false, false, false, true, true, true, true, true, true, true, true, false, true, false);

/* SELECT * FROM rs_criteria; */
SELECT * FROM rs_checks_table_ll;
SELECT * FROM rs_checks_table_hl;

SELECT * FROM rs_checks_table_ll ll FULL JOIN rs_checks_table_hl hl
  ON ll.run_number=hl.run_number
  ORDER BY ll.run_number, hl.run_number DESC;

/* Insert test rs reports */
INSERT INTO rs_report (run_number, dqll_ok, dqll_note, dqhl_ok, dqhl_note, sr_ok, sr_note, det_ok, det_note, decision, decision_note, run_list_id, fail_modes, signature, completeness)
VALUES (105340, true, '', true, '', false, '', false, '', false, 'detector turned off', NULL, NULL, 3, true);

/* Insert test report. It will not work if there is no data for the particular run number. */
/* INSERT INTO rs_report (run_number, dqll_ok, dqll_note, dqhl_ok, dqhl_note, sr_ok, sr_note, det_ok, det_note, decision, decision_note, run_list_id, fail_modes, signature, completeness)
VALUES (105350, true, '', true, '', false, '', false, '', false, 'detector turned off', NULL, NULL, 3, true); */

SELECT * FROM rs_report;

/* Show the data stored for the checks. */
/* SELECT * FROM dqll ll FULL JOIN dqhl hl ON ll.run_number=hl.run_number
ORDER BY ll.run_number, hl.run_number DESC; */

/* Example of MATERIALIZED VIEW for check results. This will not be used anymore but the idea might serve other purposes. It is equivalent with the combined results from the rs_checks_table_ll and rs_checks_table_hl tables. The issue with using the MATERIALIZED VIEW comes from the fact that it stores a snapshot of the checks which makes it's use inefficient when monitoring the status of the checks. ...*/
CREATE MATERIALIZED VIEW rs_checks
AS
  WITH x AS (
    SELECT * FROM rs_criteria ORDER BY id DESC LIMIT 1
    )
--  SELECT ll.run_number AS run, ll.duration AS dur, x.duration AS durcheck,
--  SELECT ll.run_number AS run, duration(ll.duration, x.duration) AS duration
  SELECT ll.run_number AS run_ll,
         hl.run_number AS run_hl,
         duration(ll.duration, x.duration) AS duration,
         hv_dac_a(ll.hv_dac_a, x.hv_dac_a) AS hv_dac_a,
         hv_dac_b(ll.hv_dac_b, x.hv_dac_b) AS hv_dac_b,
         hv_status_a(ll.hv_status_a, x.hv_status_a) AS hv_status_a,
         hv_status_b(ll.hv_status_b, x.hv_status_b) AS hv_status_b,
         hv_a_alarms(ll.alarm_hv_current_zero_a, x.alarms) AS alarm_hv_I_zero_a,
         hv_b_alarms(ll.alarm_hv_current_zero_b, x.alarms) AS alarm_hv_I_zero_b,
         hv_a_alarms(ll.alarm_hv_over_current_a, x.alarms) AS alarm_hv_over_I_a,
         hv_b_alarms(ll.alarm_hv_over_current_b, x.alarms) AS alarm_hv_over_I_b,
         hv_a_alarms(ll.alarm_hv_setpoint_a, x.alarms) AS alarm_hv_setpt_a,
         hv_b_alarms(ll.alarm_hv_setpoint_b, x.alarms) AS alarm_hv_setpt_b,
         --
         --
         --
         hl_check(hl.n100l_trig_rate, x.n100l_trig_rate) AS n100l_trig_rate,
         hl_check(hl.esumh_trig_rate, x.esumh_trig_rate) AS esumh_trig_rate,
         --
         hl_check(hl.ev_rate, x.ev_rate) AS ev_rate,
         hl_check(hl.ev_sep, x.ev_sep) AS ev_sep,
         hl_check(hl.retrig, x.retrig) AS retrig,
         hl_check(hl.run_hdr, x.run_hdr) AS run_hdr,
         hl_check(hl.clock_comp, x.clock_comp) AS clock_comp,
         hl_check(hl.clock_fwd, x.clock_fwd) AS clock_fwd,
         --
         hl_check(hl.run_type, x.run_type) AS run_type,
         hl_check(hl.mc_flag, x.mc_flag) AS mc_flag,
         hl_check(hl.run_trig, x.run_trig) AS run_trig,
         --
         hl_check(hl.gen_cvrg, x.gen_cvrg) AS gen_cvrg,
         hl_check(hl.crt_cvrg, x.crt_cvrg) AS crt_cvrg,
         hl_check(hl.pnl_cvrg, x.pnl_cvrg) AS pnl_cvrg
--  FROM dqll AS ll, rs_criteria AS x
--  FROM dqll AS ll, dqhl AS hl, x
FROM dqll ll FULL JOIN dqhl hl ON ll.run_number=hl.run_number, x
ORDER BY ll.run_number, hl.run_number DESC
WITH NO DATA;

SELECT * from rs_checks;
REFRESH MATERIALIZED VIEW rs_checks;
SELECT * from rs_checks;

/*  Check privileges. None were defined here. */
/* \dp dqll_data */
/* \dp dqhl_data */

/* \dp rs_report */
/* \dp rs_checks */

/* Empty the database. Roughly, it should be in reverse order.*/
DROP MATERIALIZED VIEW rs_checks;

DROP TRIGGER IF EXISTS rs_checks_monitor_ll ON rs_criteria;
DROP TRIGGER IF EXISTS rs_checks_monitor_hl ON rs_criteria;
DROP TRIGGER IF EXISTS rs_status_ll ON rs_checks_table_ll;
DROP TRIGGER IF EXISTS rs_status_hl ON rs_checks_table_hl;
DROP TRIGGER IF EXISTS rs_checks_add_ll ON dqll;
DROP TRIGGER IF EXISTS rs_checks_add_hl ON dqhl;
DROP TRIGGER IF EXISTS dqll_insert ON dqll_data;
DROP TRIGGER IF EXISTS dqhl_insert ON dqhl_data;

DROP TABLE rs_checks_table_ll;
DROP TABLE rs_checks_table_hl;
DROP TABLE rs_report;
DROP TABLE rs_status;
DROP TABLE rs_group;

DROP TABLE run_list;
DROP TABLE fail_modes;

DROP TABLE rs_criteria;

DROP TABLE dqll;
DROP TABLE dqhl;
DROP TABLE dqll_data;
DROP TABLE dqhl_data;

\dt
