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



/*---------------------------------------------------------
  2) MAIN TABLE: Tblongoingdocno
----------------------------------------------------------*/
CREATE TABLE Tblongoingdocno (

    /*---------------------------------------------------------
        Primary Key (Artificial/Surrogate Key)
        Auto-incrementing unique ID for each record
    ----------------------------------------------------------*/
    intid BIGSERIAL PRIMARY KEY,

    /*---------------------------------------------------------
        intongoingid:
        - Mapped from sub_dir_id in your insert data
        - Unique identifier for each year-range batch
    ----------------------------------------------------------*/
    intongoingid BIGINT,

    /*---------------------------------------------------------
        chrdistrict:
        - District name (e.g., मुंबई_जिल्हा)
    ----------------------------------------------------------*/
    chrdistrict VARCHAR,

    /*---------------------------------------------------------
        chrsro:
        - Sub Registrar Office name
        - Example: 'Joint_S.R._Mumbai_1_(Mumbai_City_1_(Fort))'
    ----------------------------------------------------------*/
    chrsro VARCHAR,

    /*---------------------------------------------------------
        intstartrange / intendrange:
        - Document number range (Start → End)
        - Example: 1 to 2000, 2001 to 4000, etc.
    ----------------------------------------------------------*/
    intstartrange BIGINT,
    intendrange BIGINT,

    /*---------------------------------------------------------
        chryear:
        - Document registration year (e.g., 2009, 2010, 2011)
    ----------------------------------------------------------*/
    chryear TEXT,

    /*---------------------------------------------------------
        enmstatus:
        - Processing status of the document range
        - Uses ENUM real_estate_enm defined above
        - Default = 'Not Started'
    ----------------------------------------------------------*/
    enmstatus real_estate_enm DEFAULT 'Not Started',

    /*---------------------------------------------------------
        chrip:
        - System/server IP address (if applicable)
    ----------------------------------------------------------*/
    chrip TEXT,

    /*---------------------------------------------------------
        Timestamps
        All default to NOW()
        These track processing lifecycle of each document range
    ----------------------------------------------------------*/
    dtmaddedon TIMESTAMP DEFAULT NOW(),                   -- When added
    dtmupdatedon TIMESTAMP DEFAULT NOW(),                 -- When last updated
    dtmlastrunon TIMESTAMP DEFAULT NOW(),                 -- Last execution time
    intintervalhours NUMERIC,                             -- Interval between runs (hours)

    dtmcompletedwebhookreceivedon TIMESTAMP DEFAULT NOW(), -- When webhook result received
    dtmlastcompletionattempted TIMESTAMP DEFAULT NOW(),    -- When last completion attempt happened
    intcompletedattempt INTEGER,                           -- How many times completion was attempted

    dtm_startedon TIMESTAMP DEFAULT NOW(),                 -- Processing start timestamp
    intcrawlingattempted INTEGER,                          -- Crawling attempt count
    dtmlastcrawlingattempted TIMESTAMP DEFAULT NOW(),      -- Last crawling attempt timestamp

    chrthreadname TEXT,                                    -- Worker/thread name handling this job
    dtmextractedon TIMESTAMP DEFAULT NOW(),                -- Extraction completed timestamp
    dtmcnrgeneratedon TIMESTAMP DEFAULT NOW(),             -- CNR generated timestamp
    dtmdestroyedon TIMESTAMP DEFAULT NOW(),                -- Destroyed timestamp (if deleted)
    dtmfolderretrievedon TIMESTAMP DEFAULT NOW(),          -- Folder retrieved timestamp
    dtmextractionstartedon TIMESTAMP DEFAULT NOW()         -- Extraction started timestamp
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

CREATE TABLE Tblongoingdocnorecords (
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
