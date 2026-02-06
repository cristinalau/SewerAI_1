-- Optional: uncomment if your worksheet prompts for substitution variables
-- SET DEFINE OFF

/* LACP Facility Type = Service Connection 4, Unknown Service Connection 10.6, Unknown CBL Lead (LACP) 10.7 */

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

/* ------------------------------ LACP base dataset ------------------------------ */
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
    e1.facilitytype                     AS facility_type,   -- 4 Service Connection OR 10 Unknown
    e.epdrfacility_oi,
    e.epdrfacilityworkhistoryoi,
    e.createdate_dttm,
    e.lastupdate_dttm,

    /* LACP source (Service Connection) */
    esc.pipelength        AS lacp_pipelength,
    esc.pipesize          AS lacp_pipesize,
    esc.recordtype        AS lacp_recordtype,
    esc.location          AS lacp_location,
    esc.neighbourhd       AS lacp_neighbourhd,
    esc.pipetype          AS lacp_pipetype,
    esc.wass_appid        AS lacp_wass_appid,

    /* Derived fields for LACP only */
    'LACP'                AS inspection_type,
    'Service Connection'  AS pip_type,

    /* Lateral segment reference (from WASS_APPID left part before last '-') */
    CASE
      WHEN INSTR(esc.wass_appid, '-', -1) > 1
        THEN SUBSTR(esc.wass_appid, 1, INSTR(esc.wass_appid, '-', -1) - 1)
      ELSE esc.wass_appid
    END AS base_pipe_ref,

    /* Unknown facility sub-type (6/7 for LACP unknowns) */
    ep2.unknfacType       AS unknown_type,

    /* UUIDs */
    a.uuid                AS work_order_task_uuid,
    e.uuid                AS dr_uuid

  FROM mnt.workordertask a
  LEFT JOIN mnt.workorders wo
         ON a.workorder_oi = wo.workordersoi
  LEFT JOIN mnt.asset s
         ON a.asset_oi = s.assetoi

  JOIN customerdata.epdrfacworkhistory e
       ON e.wotask_oi = a.workordertaskoi

  LEFT JOIN customerdata.epdrdrainfacility e1
         ON e.epdrfacility_oi = e1.epdrdrainagefacilityoi

  /* LACP service connection metadata (the LACP source) */
  LEFT JOIN customerdata."EPDRSERVICECONNECT" esc
         ON esc.wass_appid = e1.facilityid

  /* Unknown facility mapping (to allow Unknown 10.6/10.7) */
  LEFT JOIN customerdata.epdrunknfac ep2
         ON e1.epdrunknownf_oi = ep2.epdrunknownfacilityoi

  /* Join the cleaned long description (final) */
  LEFT JOIN clean_longdesc_final cli
         ON cli.workordertaskoi = a.workordertaskoi

  /* LACP rows: Service Connection OR Unknown mapped to LACP (6,7) */
  WHERE e1.facilitytype = 4
     OR (e1.facilitytype = 10 AND ep2.unknfacType IN (6, 7))
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
  COALESCE(mc_sc.pioneers_code, b.lacp_pipetype) AS material,

  /* LACP pipeline type */
  b.pip_type,

  /* PIPE_USE mapping from recordtype */
  CASE
    WHEN UPPER(TRIM(b.lacp_recordtype)) = 'FOUNDATION' THEN 'PN'
    WHEN UPPER(TRIM(b.lacp_recordtype)) = 'SANITARY'   THEN 'SS'
    WHEN UPPER(TRIM(b.lacp_recordtype)) = 'STORM'      THEN 'SW'
    WHEN UPPER(TRIM(b.lacp_recordtype)) = 'WATER'      THEN 'XX'
    WHEN UPPER(TRIM(b.lacp_recordtype)) IN ('NOT APPLICABLE', 'N/A', 'NA') THEN 'XX'
    ELSE NULL
  END AS pipe_use,

  /* Lateral segment reference:
     Use facility_id for Unknown LACP (10.6 / 10.7), else normalized WASS_APPID */
  CASE
    WHEN b.facility_type = 10 AND b.unknown_type IN (6, 7) THEN b.facility_id
    ELSE b.base_pipe_ref
  END AS lateral_segment_reference,

  /* IDs / UUIDs */
  b.work_order_task_uuid,
  b.dr_uuid,

  /* LACP sizes/lengths */
  b.lacp_pipelength  AS total_length,
  b.lacp_pipesize    AS "SIZE",

  /* LACP street/area */
  b.lacp_location    AS street,
  b.lacp_neighbourhd AS drainage_area,

  /* Explicit for clarity (already filtered) */
  b.inspection_type

FROM base b

/* Material mapping from service connection PIPETYPE */
LEFT JOIN CUSTOMERDATA.EPSEWERAI_MATERIAL_CODE mc_sc
  ON UPPER(TRIM(mc_sc.ivara_material)) = UPPER(TRIM(b.lacp_pipetype))

WHERE b.WORK_ORDERS_NUMBER = '279600.1'
ORDER BY
  b.facility_type,
  b.work_orders_number DESC;