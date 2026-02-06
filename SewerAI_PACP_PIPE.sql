/* PACP Facitity Type = PIPE 8, Unknown Pipe 10.4, Unknown Catch Basin Lead 10.5*/
-- Optional: uncomment this line if your worksheet still prompts for substitution variables
-- SET DEFINE OFF

/* PACP Facility Type = PIPE 8, Unknown Pipe 10.4, Unknown Catch Basin Lead 10.5 */

WITH
/* -------------------- CLOB-safe HTML cleaner (no ampersand literals) -------------------- */
clean_longdesc AS (
  SELECT
    a.workordertaskoi,

    /* Step 1: Un-double-escape common entities (e.g., &amp;lt; -> &lt;), keep as CLOB */
    REPLACE(
      REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(
              REPLACE(TO_CLOB(a.longdescript),
                CHR(38)||'amp;lt;',   CHR(38)||'lt;'
              ),
              CHR(38)||'amp;gt;',     CHR(38)||'gt;'
            ),
            CHR(38)||'amp;nbsp;',     CHR(38)||'nbsp;'
          ),
          CHR(38)||'amp;quot;',       CHR(38)||'quot;'
        ),
        CHR(38)||'amp;#39;',          CHR(38)||'#39;'
      ),
      CHR(38)||'amp;amp;',            CHR(38)||'amp;'
    ) AS step1_unDoubleEscaped

  FROM mnt.workordertask a
),

clean_longdesc_final AS (
  SELECT
    workordertaskoi,

    /* Step 2: Decode single-escaped entities, remove blocks, strip tags, collapse spaces, trim */
    REPLACE(
      REGEXP_REPLACE(                                          -- 6) trim
        REGEXP_REPLACE(                                        -- 5) collapse spaces
          REGEXP_REPLACE(                                      -- 4c) strip any remaining tags
            REGEXP_REPLACE(                                    -- 4b) remove <script> blocks
              REGEXP_REPLACE(                                  -- 4a) remove <style> blocks
                REGEXP_REPLACE(                                -- 3b) remove <head>...</head>
                  REGEXP_REPLACE(                              -- 3a) remove <!DOCTYPE ...>
                    REPLACE(
                      REPLACE(
                        REPLACE(
                          REPLACE(
                            REPLACE(step1_unDoubleEscaped,
                              CHR(38)||'nbsp;', ' '            -- &nbsp; -> space
                            ),
                            CHR(38)||'lt;', '<'                -- &lt;   -> <
                          ),
                          CHR(38)||'gt;', '>'                  -- &gt;   -> >
                        ),
                        CHR(38)||'quot;', '"'                  -- &quot; -> "
                      ),
                      CHR(38)||'#39;', ''''                    -- &#39;  -> '
                    ),
                    '<!DOCTYPE[^>]*>', '', 1, 0, 'in'
                  ),
                  '<head[^>]*>.*?</head>', '', 1, 0, 'in'
                ),
                '<style[^>]*>.*?</style>', '', 1, 0, 'in'
              ),
              '<script[^>]*>.*?</script>', '', 1, 0, 'in'
            ),
            '<[^>]+>', '', 1, 0, 'n'                           -- strip any tags
          ),
          '\s+', ' '                                           -- collapse whitespace
        ),
        '^\s+|\s+$', ''                                        -- trim
      ),
      CHR(38)||'amp;', CHR(38)                                 -- decode &amp; -> &
    ) AS additional_information_clob

  FROM clean_longdesc
),

/* ------------------------------ PACP base dataset ------------------------------ */
base AS (
  SELECT
    /* Work order + task info */
    wo.wonumber || '.' || a.tasknumber AS work_orders_number,
    a.wotasktitle                       AS work_order_task_title,
    s.assetnumber                       AS asset_number,

    /* Cleaned long description (CLOB) */
    cli.additional_information_clob     AS additional_information,

    /* Facility information */
    e1.epdrdrainagefacilityoi           AS facilityoi,
    e1.facilityid                       AS facility_id,
    e1.facilitytype                     AS facility_type,   -- 8 Pipe OR 10 Unknown
    e.epdrfacility_oi,
    e.epdrfacilityworkhistoryoi,
    e.createdate_dttm,
    e.lastupdate_dttm,

    /* PACP (Pipe) source */
    ep1.material          AS raw_material,
    ep1.shape             AS raw_shape,
    ep1.wwtype            AS raw_wwtype,
    ep1.pipeid            AS raw_pipeid,

    /* Network & measurements */
    ep1.usfacilityid      AS upstream_id,
    ep1.dsfacilityid      AS downstream_id,
    ep1.diameter_fl       AS raw_diameter_fl,
    ep1.usgroundelev_fl   AS raw_usgroundelev_fl,
    ep1.usinvertelev_fl   AS raw_usinvertelev_fl,
    ep1.dsgroundelev_fl   AS raw_dsgroundelev_fl,
    ep1.dsinvertelev_fl   AS raw_dsinvertelev_fl,
    ep1.location          AS raw_location,

    /* Additional PACP attributes */
    ep1.length_fl         AS raw_length_fl,        -- -> TOTAL_LENGTH
    ep1.yearconst         AS raw_yearconst,        -- -> YEAR_CONSTRUCTED
    ep1.usneighbour       AS raw_usneighbour,      -- -> DRAINAGE_AREA

    /* Inspection type: PACP for pipes and unknown pipe-like facilities */
    CASE
      WHEN e1.facilitytype = 8 THEN 'PACP'
      WHEN e1.facilitytype = 10 AND ep2.unknfacType IN (4,5,6,7,8) THEN 'PACP'
      ELSE NULL
    END AS inspection_type,

    'Pipe' AS pip_type,

    /* normalized EPDRPIPE.PIPEID (strip PIP/CBL prefix if present) */
    CASE
      WHEN ep1.pipeid IS NULL THEN NULL
      WHEN UPPER(ep1.pipeid) LIKE 'PIP%' THEN SUBSTR(ep1.pipeid, 4)
      WHEN UPPER(ep1.pipeid) LIKE 'CBL%' THEN SUBSTR(ep1.pipeid, 4)
      ELSE ep1.pipeid
    END AS base_pipe_ref,

    /* Unknown facility sub-type (4=Unknown Pipe, 5=Unknown CBL Lead, etc.) */
    ep2.unknfacType AS unknown_type,

    /* UUIDs */
    a.uuid AS work_order_task_uuid,
    e.uuid AS dr_uuid

  FROM mnt.workordertask a
  LEFT JOIN mnt.workorders wo ON a.workorder_oi = wo.workordersoi
  LEFT JOIN mnt.asset s       ON a.asset_oi     = s.assetoi

  JOIN customerdata.epdrfacworkhistory e
       ON e.wotask_oi = a.workordertaskoi

  LEFT JOIN customerdata.epdrdrainfacility e1
         ON e.epdrfacility_oi = e1.epdrdrainagefacilityoi

  /* PACP (Pipe) source */
  LEFT JOIN customerdata.epdrpipe ep1
         ON e1.epdrpipe_oi = ep1.epdrpipeoi

  /* Unknown facility mapping (to catch PACP-like unknowns) */
  LEFT JOIN customerdata.epdrunknfac ep2
         ON e1.epdrunknownf_oi = ep2.epdrunknownfacilityoi

  /* Join the cleaned long description (final) */
  LEFT JOIN clean_longdesc_final cli
         ON cli.workordertaskoi = a.workordertaskoi

  /* PACP rows: Pipe (8) or Unknown mapped to pipe-like (4..8) */
  WHERE e1.facilitytype = 8
     OR (e1.facilitytype = 10 AND ep2.unknfacType IN (4,5,6,7,8))
)

/* ------------------------------ Final projection ------------------------------ */
SELECT
  b.work_orders_number,
  b.work_order_task_title,
  b.asset_number,

  /* Cleaned full CLOB */
  b.additional_information,

  /* Facility context */
  b.facilityoi,
  b.facility_id,
  b.facility_type,
  b.epdrfacility_oi,
  b.epdrfacilityworkhistoryoi,
  b.createdate_dttm,
  b.lastupdate_dttm,

  /* Material mapping (fallback to raw if no map) */
  COALESCE(mc.pioneers_code, b.raw_material) AS material,

  /* SHAPE mapping with NULL when no match (no default 'Z') */
  sc.pioneers_code AS shape,

  /* PIPE_USE mapping from ep1.wwtype */
  CASE
    WHEN UPPER(TRIM(b.raw_wwtype)) = 'FOUNDATION' THEN 'PN'
    WHEN UPPER(TRIM(b.raw_wwtype)) = 'SANITARY'   THEN 'SS'
    WHEN UPPER(TRIM(b.raw_wwtype)) = 'STORM'      THEN 'SW'
    WHEN UPPER(TRIM(b.raw_wwtype)) = 'WATER'      THEN 'XX'
    WHEN UPPER(TRIM(b.raw_wwtype)) = 'COMBINED'   THEN 'CB'
    WHEN UPPER(TRIM(b.raw_wwtype)) IN ('NOT APPLICABLE','N/A','NA') THEN 'XX'
    ELSE NULL
  END AS pipe_use,

  /* Type */
  b.pip_type,

  /* Pipe segment reference:
     Use facility_id for Unknown Pipe/CBL Lead (10.4 / 10.5), else normalized pipeid */
  CASE
    WHEN b.facility_type = 10 AND b.unknown_type IN (4,5) THEN b.facility_id
    ELSE b.base_pipe_ref
  END AS pipe_segment_reference,

  /* IDs / UUIDs */
  b.upstream_id,
  b.downstream_id,
  b.work_order_task_uuid,
  b.dr_uuid,
  b.inspection_type,
  b.unknown_type,

  /* PACP numeric outputs from ep1 */
  b.raw_diameter_fl     AS height,
  b.raw_usgroundelev_fl AS up_elevation,
  b.raw_usinvertelev_fl AS up_grade_to_invert,
  b.raw_dsgroundelev_fl AS down_elevation,
  b.raw_dsinvertelev_fl AS down_grade_to_invert,

  /* Misc */
  b.raw_location        AS street,
  b.raw_length_fl       AS total_length,
  b.raw_yearconst       AS year_constructed,
  b.raw_usneighbour     AS drainage_area

FROM base b

/* Material mapping for ep1.material */
LEFT JOIN CUSTOMERDATA.EPSEWERAI_MATERIAL_CODE mc
  ON UPPER(TRIM(mc.ivara_material)) = UPPER(TRIM(b.raw_material))

/* Shape lookup for pipes */
LEFT JOIN CUSTOMERDATA.EPSEWERAI_SHAPE_CODE sc
  ON UPPER(TRIM(sc.ivara_shape))    = UPPER(TRIM(b.raw_shape))

/* Narrow to a specific work order task if desired */
--WHERE b.work_orders_number = '279598.1'

ORDER BY
  b.facility_type,
  b.work_orders_number DESC;