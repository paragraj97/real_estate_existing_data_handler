/*---------------------------------------------------------
  1) ENUM TYPE FOR STATUS FIELD
----------------------------------------------------------*/
-- These are the possible processing states for the document range.
CREATE TYPE real_estate_enm AS ENUM (
    'Not Started',
    'In Progress',
    'Completed Webhook Recieved',
    'Doc Folder Retrieved',
    'Doc Extraction Pending',
    'Doc Extraction InProgress',
    'Extraction Completed'
);


CREATE TABLE public.tblongoingdocno (
    intongoingid BIGSERIAL PRIMARY KEY,

    chrdistrict varchar NULL,
    chrsro varchar NULL,
    intstartrange int8 NULL,
    intendrange int8 NULL,
    intmaxrange int8 NULL,
    chryear text NULL,

    enmstatus public.real_estate_enm DEFAULT 'Not Started'::real_estate_enm NULL,
    chrip text NULL,

    dtmaddedon timestamp DEFAULT now() NULL,
    dtmupdatedon timestamp DEFAULT now() NULL,
    dtmlastrunon timestamp NULL,

    intintervalhours numeric NULL,
    dtmcompletedwebhookreceivedon timestamp NULL,
    dtmlastcompletionattempted timestamp NULL,
    intcompletedattempt int4 NULL,

    dtm_startedon timestamp NULL,
    intcrawlingattempted int4 NULL,
    dtmlastcrawlingattempted timestamp NULL,

    chrthreadname text NULL,
    dtmextractedon timestamp NULL,
    dtmcnrgeneratedon timestamp NULL,
    dtmdestroyedon timestamp NULL,
    dtmfolderretrievedon timestamp NULL,
    dtmextractionstartedon timestamp NULL,

    chrdistrictenglish varchar NULL
);












-- ============================================
--  ENUM DEFINITIONS
-- ============================================

-- Crawling status enum
CREATE TYPE re_crawling_status_enum AS ENUM (
    'Found',
    'Not Found',
    'Error'
);

-- Extraction status enum
CREATE TYPE re_extraction_status_enum AS ENUM (
    'Pending',     -- Default
    'Success',
    'Fail',
    'Duplicate'
);


-- ============================================
--  TABLE: Tblongoingdocnorecords
-- ============================================

CREATE TABLE tblongoingdocnorecords (
    -- Auto-increment primary key
    intid BIGSERIAL PRIMARY KEY,                        -- internal record id

    -- Foreign key referencing Tblongoingdocno.intongoingid
    intongoingid BIGINT NOT NULL,                   -- maps to parent table

    -- District name (Marathi)
    chrdistrict VARCHAR,                            -- example: 'मुंबई_जिल्हा'

    -- District name English (nullable)
    chrdistrictenglish VARCHAR,                     -- optional English district

    -- Registration type
    chrregistrationtype VARCHAR,                    -- Sale / Mortgage / Leave License etc.

    -- SRO Name
    chrsro VARCHAR,                                 -- example: Joint_S.R._Mumbai_1_(...)

    -- Document number
    intdocno BIGINT NOT NULL,                       -- doc_no from inserts

    -- Year
    chryear VARCHAR,                                -- example: '2010'

    -- Parent range start/end (copied from Tblongoingdocno)
    intstartrange BIGINT,                           -- min_doc_no
    intendrange BIGINT,                             -- max_doc_no

    -- Crawling stage: Found / Not Found / Error
    crawling_status re_crawling_status_enum DEFAULT NULL,

    -- Extraction stage: Pending / Success / Fail / Duplicate
    extraction_status re_extraction_status_enum DEFAULT 'Pending',

    -- File paths
    text_htmlpath TEXT,
    text_jsonpath TEXT,

    -- For logging failures
    text_error TEXT,

    -- Timestamps
    dtmaddedon TIMESTAMP DEFAULT NOW(),                 -- created
    dtmupdatedon TIMESTAMP DEFAULT NOW()                -- updated
);
