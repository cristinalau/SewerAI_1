/* MACP Facitity Type = CBL, Manhole 1, Catch Basin 2, Unknown Manhole 10.2, Unknown Catch Basin 10.3*/


WITH
/* ---------------- MH_USE mapping ---------------- */
mh_use_map AS (
  SELECT 'COMBINED'         AS ivara_mh_use, 'C'  AS pioneers_code FROM DUAL UNION ALL
  SELECT 'FOUNDATION DRAIN' AS ivara_mh_use, 'ST' AS pioneers_code FROM DUAL UNION ALL
  SELECT 'NOT APPLICABLE'   AS ivara_mh_use, 'O'  AS pioneers_code FROM DUAL UNION ALL
  SELECT 'SANITARY'         AS ivara_mh_use, 'S'  AS pioneers_code FROM DUAL UNION ALL
  SELECT 'STORM'            AS ivara_mh_use, 'ST' AS pioneers_code FROM DUAL
),

/* -------- Clean long description as CLOB end-to-end (NO implicit VARCHAR2) -------- */
clean_longdesc AS (
  SELECT
    a.workordertaskoi,

    /* CLOB-safe pipeline to normalize entities, remove tags/script/style, collapse whitespace, and trim */
    REGEXP_REPLACE(                                         -- 6) trim leading/trailing spaces
      REGEXP_REPLACE(                                       -- 5) collapse internal whitespace to single space
        REGEXP_REPLACE(                                     -- 4b) remove escaped tags
          REGEXP_REPLACE(                                   -- 4a) remove literal tags
            REGEXP_REPLACE(                                 -- 3b) remove escaped <script> blocks
              REGEXP_REPLACE(                               -- 3a) remove literal <script> blocks
                REGEXP_REPLACE(                             -- 2b) remove escaped <style> blocks
                  REGEXP_REPLACE(                           -- 2a) remove literal <style> blocks
                    /* 1) Normalize entities (double-escaped -> single; then decode common ones) */
                    REPLACE(
                      REPLACE(
                        REPLACE(
                          REPLACE(
                            REPLACE(
                              REPLACE(
                                TO_CLOB(a.longdescript),    -- anchor as CLOB at the start
                                '&amp;nbsp;', '&nbsp;'      -- un-double-escape common entities
                              ),
                              '&amp;lt;',   '&lt;'
                            ),
                            '&amp;gt;',   '&gt;'
                          ),
                          '&amp;quot;', '&quot;'
                        ),
                        '&amp;#39;',  '&#39;'
                      ),
                      '&amp;amp;',  '&amp;'
                    ),
                    '<style[^>]*>.*?</style>', '', 1, 0, 'in'
                  ),
                  '&lt;style[^&gt;]*&gt;.*?&lt;/style&gt;', '', 1, 0, 'in'
                ),
                '<script[^>]*>.*?</script>', '', 1, 0, 'in'
              ),
              '&lt;script[^&gt;]*&gt;.*?&lt;/script&gt;', '', 1, 0, 'in'
            ),
            '<[^>]+>', '', 1, 0
          ),
          '&lt;[^&gt;]+&gt;', '', 1, 0, 'in'
        ),
        '\s+', ' '
      ),
      '^\s+|\s+$', ''
    ) AS additional_information_clob
  FROM mnt.workordertask a
),

base AS (
    SELECT
        /* Work order + task info */
        wo.wonumber || '.' || a.tasknumber           AS work_orders_number,
        a.wotasktitle                                AS work_order_task_title,
        s.assetnumber                                AS asset_number,

        /* Keep as CLOB to avoid implicit VARCHAR2(4000) conversion */
        cli.additional_information_clob              AS additional_information,

        /* Facility information */
        e1.epdrdrainagefacilityoi                    AS facilityoi,
        e1.facilityid                                AS facility_id,
        e1.facilitytype                              AS facility_type,
        e.epdrfacility_oi,
        e.epdrfacilityworkhistoryoi,
        e.createdate_dttm,
        e.lastupdate_dttm,

        /* PIPE TYPE (structure type label) */
        CASE
            WHEN e1.facilitytype = 10 AND ep2.unknfacType = 3 THEN 'Manhole'
            WHEN e1.facilitytype = 10 AND ep2.unknfacType = 2 THEN 'Catch Basin'
            WHEN e1.facilitytype = 1 THEN 'Manhole'
            WHEN e1.facilitytype = 2 THEN 'Catch Basin'
            ELSE 'Unknown Facility'
        END AS ACCESS_TYPE,

        /* Normalized structure number */
        CASE
            WHEN e1.facilitytype = 1 THEN REGEXP_REPLACE(e1.facilityid, '^MH', '')
            WHEN e1.facilitytype = 2 THEN REGEXP_REPLACE(e1.facilityid, '^CB', '')
            WHEN e1.facilitytype = 10 AND ep2.unknfacType = 3 THEN REGEXP_REPLACE(e1.facilityid, '^MH', '')
            WHEN e1.facilitytype = 10 AND ep2.unknfacType = 2 THEN REGEXP_REPLACE(e1.facilityid, '^CB', '')
            ELSE NULL
        END AS manhole_number,

        /* UUIDs / flags */
        a.uuid   AS work_order_task_uuid,
        e.uuid   AS dr_uuid,
        ep2.unknfacType AS unknown_type,

        /* Inspection type (MACP only) */
        CASE
            WHEN e1.facilitytype IN (1, 2) THEN 'MACP'
            WHEN e1.facilitytype = 10 AND ep2.unknfacType IN (2, 3) THEN 'MACP'
            ELSE NULL
        END AS inspection_type,

        /* --- MACP source tables --- */
        /* Manhole attributes */
        mh.wwtype          AS mh_wwtype,
        mh.neighbourhd     AS mh_neighbourhd,
        mh.location        AS mh_location,
        mh.cone            AS mh_cone,
        mh.bench           AS mh_bench,
        mh.channel         AS mh_channel,
        mh.shape           AS mh_shape,
        mh.diameter_fl     AS mh_diameter_fl,
        mh.depth_fl        AS mh_depth_fl,
        mh.groundelevat_fl AS mh_groundelevat_fl,

        /* Catch basin attributes */
        cb.wwtype          AS cb_wwtype,
        cb.neighbourhd     AS cb_neighbourhd,
        cb.location        AS cb_location,
        cb.shape           AS cb_shape,
        cb.diameter_fl     AS cb_diameter_fl,   -- for wall_bysize
        cb.depth_fl        AS cb_depth_fl,      -- for wall_depth
        cb.framecover      AS cb_framecover,    -- for frame_material

        /* >>> New computed column (as requested): facility_id only for CB / Unknown(2 or 3) <<< */
        CASE
          WHEN (e1.facilitytype = 2 OR (e1.facilitytype = 10 AND ep2.unknfacType IN (2, 3)))
            THEN e1.facilityid
          ELSE NULL
        END AS pipe_segment_reference

    FROM mnt.workordertask a
    LEFT JOIN mnt.workorders wo ON a.workorder_oi = wo.workordersoi
    LEFT JOIN mnt.asset s       ON a.asset_oi     = s.assetoi

    JOIN customerdata.epdrfacworkhistory e
         ON e.wotask_oi = a.workordertaskoi

    LEFT JOIN customerdata.epdrdrainfacility e1
           ON e.epdrfacility_oi = e1.epdrdrainagefacilityoi

    LEFT JOIN customerdata.epdrunknfac ep2
           ON e1.epdrunknownf_oi = ep2.epdrunknownfacilityoi

    LEFT JOIN customerdata.epdrmanhole mh
           ON mh.manholeid = e1.facilityid
    LEFT JOIN customerdata.epdrcatchbasin cb
           ON cb.catchbasinid = e1.facilityid

    LEFT JOIN clean_longdesc cli
           ON cli.workordertaskoi = a.workordertaskoi

    /* Keep only MACP rows */
    WHERE (e1.facilitytype IN (1, 2) OR (e1.facilitytype = 10 AND ep2.unknfacType IN (2, 3)))
)
SELECT
    b.work_orders_number,
    b.work_order_task_title,
    b.asset_number,

    /* Full CLOB (safe) */
    b.additional_information,

    b.facilityoi,
    b.facility_id,
    b.facility_type,
    b.epdrfacility_oi,
    b.epdrfacilityworkhistoryoi,
    b.createdate_dttm,
    b.lastupdate_dttm,

    b.ACCESS_TYPE,

    b.manhole_number,
    b.work_order_task_uuid,
    b.dr_uuid,
    b.unknown_type,
    b.inspection_type,

    /* STREET & DRAINAGE_AREA resolve by structure type */
    CASE
        WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type = 2))
            THEN b.cb_location
        ELSE b.mh_location
    END AS street,

    CASE
        WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type = 2))
            THEN b.cb_neighbourhd
        ELSE b.mh_neighbourhd
    END AS drainage_area,

    /* MH_USE: show Pioneer code */
    mhuse_map.pioneers_code AS mh_use,

    /* COVER_SHAPE via shape code table — NULL if no match */
    CASE
        WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type = 2))
            THEN sc_cb.pioneers_code
        ELSE sc_mh.pioneers_code
    END AS cover_shape,

    /* ---------- Materials as codes for Manholes; NULL for CB and Unknown (2/3) ---------- */
    CASE
      WHEN b.facility_type = 2
        OR (b.facility_type = 10 AND b.unknown_type IN (2, 3))
        THEN NULL
      ELSE mat_wall.pioneers_code
    END AS wall_material,

    CASE
      WHEN b.facility_type = 2
        OR (b.facility_type = 10 AND b.unknown_type IN (2, 3))
        THEN NULL
      ELSE mat_bench.pioneers_code
    END AS bench_material,

    CASE
      WHEN b.facility_type = 2
        OR (b.facility_type = 10 AND b.unknown_type IN (2, 3))
        THEN NULL
      ELSE mat_chan.pioneers_code
    END AS channel_material,

    /* Size/depth/elevation with Catch Basin overrides */
    CASE
      WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type = 2)) THEN b.cb_diameter_fl
      ELSE b.mh_diameter_fl
    END AS wall_bysize,

    CASE
      WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type = 2)) THEN b.cb_depth_fl
      ELSE b.mh_depth_fl
    END AS wall_depth,

    CASE
      WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type = 2)) THEN NULL
      ELSE b.mh_groundelevat_fl
    END AS elevation,

    /* Frame material for Catch Basins (text) */
    CASE
      WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type = 2))
        THEN b.cb_framecover
      ELSE NULL
    END AS frame_material,

    /* Expose the computed column */
    b.pipe_segment_reference

FROM base b

/* Shape lookups for MACP */
LEFT JOIN CUSTOMERDATA.EPSEWERAI_SHAPE_CODE sc_mh
  ON UPPER(TRIM(sc_mh.ivara_shape)) = UPPER(TRIM(b.mh_shape))
LEFT JOIN CUSTOMERDATA.EPSEWERAI_SHAPE_CODE sc_cb
  ON UPPER(TRIM(sc_cb.ivara_shape)) = UPPER(TRIM(b.cb_shape))

/* MH_USE code mapping join (case/space-insensitive) */
LEFT JOIN (
  SELECT UPPER(TRIM(ivara_mh_use)) AS ivara_mh_use, pioneers_code
  FROM mh_use_map
) mhuse_map
  ON UPPER(TRIM(
       CASE
         WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type = 2))
           THEN b.cb_wwtype
         ELSE b.mh_wwtype
       END
     )) = mhuse_map.ivara_mh_use

/* Material code mapping joins (normalize with UPPER/TRIM) */
LEFT JOIN CUSTOMERDATA.EPSEWERAI_MATERIAL_CODE mat_wall
  ON UPPER(TRIM(mat_wall.ivara_material)) = UPPER(TRIM(
       CASE
         WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type IN (2, 3)))
           THEN NULL
         ELSE b.mh_cone
       END
     ))
LEFT JOIN CUSTOMERDATA.EPSEWERAI_MATERIAL_CODE mat_bench
  ON UPPER(TRIM(mat_bench.ivara_material)) = UPPER(TRIM(
       CASE
         WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type IN (2, 3)))
           THEN NULL
         ELSE b.mh_bench
       END
     ))
LEFT JOIN CUSTOMERDATA.EPSEWERAI_MATERIAL_CODE mat_chan
  ON UPPER(TRIM(mat_chan.ivara_material)) = UPPER(TRIM(
       CASE
         WHEN (b.facility_type = 2 OR (b.facility_type = 10 AND b.unknown_type IN (2, 3)))
           THEN NULL
         ELSE b.mh_channel
       END
     ))

/* No extra filters added except your original one */
WHERE b.WORK_ORDERS_NUMBER = '279596.1'
ORDER BY
    b.facility_type,
    b.work_orders_number DESC;
