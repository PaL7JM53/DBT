-- dbt Model: mart_root_floc_exceptions
-- This model consolidates all physical location (Floc) details with various
-- audit exceptions and flag tables, ensuring a single source of truth for all
-- facility-level issues and attributes.
-- It is designed to identify exception with the Root Floc data structure.

SELECT
    rf.root_floc,
    rf.tech_object_type,
    rf.api_no10,
    
    -- Facility Flags: Use COALESCE(1, 0) logic if these flags are BOOLEAN/INT based
    oil.oil_facility,
    gas.gas_facility,
    water.water_facility,
    compress.compressfac,
    inj.injfac,

    -- Exception Column Consolidation
    LTRIM(
        CONCAT(
            -- Concatenate all exception notes/codes into a single audit column
            COALESCE(e_oil.exception, ''),
            COALESCE(e_water.exception, ''),
            COALESCE(e_gas.exception, ''),
            COALESCE(e_compress.exception, ''),
            COALESCE(e_inj.exception, ''),
            COALESCE(e_dist.exception, ''),
            COALESCE(e_multisat.exception, ''),
            COALESCE(e_sap.exception, ''),
            COALESCE(e_stop.exception, ''),
            COALESCE(e_alloc.exception, '')
        )
    ) AS exception_notes

FROM
    -- Base Model: The central dimension for all joins
    {{ ref('RootFloc') }} AS rf

--
-- 1. FACILITY DETAIL JOINS (Linking main attributes)
--
LEFT JOIN {{ ref('Oil_Fac_Lvl1') }} AS oil
    ON rf.root_floc = oil.RootFloc
LEFT JOIN {{ ref('Gas_Fac_Lvl1') }} AS gas
    ON rf.root_floc = gas.RootFloc
LEFT JOIN {{ ref('Water_Fac_Lvl1') }} AS water
    ON rf.root_floc = water.RootFloc
LEFT JOIN {{ ref('CompressFac') }} AS compress
    ON rf.root_floc = compress.RootFloc
LEFT JOIN {{ ref('InjFac') }} AS inj
    ON rf.root_floc = inj.RootFloc

--
-- 2. EXCEPTION/AUDIT FLAG JOINS (Identifying issues)
-- (All these models are expected to only return rows where an exception exists)
--
LEFT JOIN {{ ref('NoLvl1Oil') }} AS e_oil
    ON rf.root_floc = e_oil.RootFloc
LEFT JOIN {{ ref('NoLvl1Water') }} AS e_water
    ON rf.root_floc = e_water.RootFloc
LEFT JOIN {{ ref('NoLvl1Gas') }} AS e_gas
    ON rf.root_floc = e_gas.RootFloc
LEFT JOIN {{ ref('NoCompress') }} AS e_compress
    ON rf.root_floc = e_compress.RootFloc
LEFT JOIN {{ ref('NoInjFac') }} AS e_inj
    ON rf.root_floc = e_inj.RootFloc
LEFT JOIN {{ ref('BigDistance') }} AS e_dist
    ON rf.root_floc = e_dist.RootFloc
LEFT JOIN {{ ref('DistMultiSat') }} AS e_multisat
    ON rf.root_floc = e_multisat.RootFloc
LEFT JOIN {{ ref('NoMatchSAP') }} AS e_sap
    ON rf.root_floc = e_sap.RootFloc
LEFT JOIN {{ ref('multiStop') }} AS e_stop
    ON rf.root_floc = e_stop.RootFloc
LEFT JOIN {{ ref('oilAllocation') }} AS e_alloc
    ON rf.root_floc = e_alloc.RootFloc