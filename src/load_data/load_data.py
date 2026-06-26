#----------------------------------------------------------------------------------
#
# Copyright 2023 by Canadian Wildlife Federation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#----------------------------------------------------------------------------------

#
# This script loads gdb files into postgis database, create by the create_db.py script
#
import subprocess
import appconfig
import os
from psycopg2.extras import RealDictCursor

streamTable = appconfig.config['DATABASE']['stream_table']
roadTable = appconfig.config['CREATE_LOAD_SCRIPT']['road_table']
trailTable = appconfig.config['CREATE_LOAD_SCRIPT']['trail_table']
watershedTable = appconfig.watershedTable

file = appconfig.config['CREATE_LOAD_SCRIPT']['raw_data']
watershedfile = appconfig.watershedfile
temptable = appconfig.dataSchema + ".temp"

sheds = appconfig.config['HABITAT_STATS']['watersheds'].split(",")


def loadWatersheds(conn):
    print("Loading Watershed Boundaries")
    layer = watershedTable
    datatable = appconfig.dataSchema + "." + watershedTable
    orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"
    pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nlt geometry -nln "' + datatable + '" -nlt CONVERT_TO_LINEAR -lco GEOMETRY_NAME=geometry "' + watershedfile + '" ' + layer
    subprocess.run(pycmd)

    query = f"""
    ALTER TABLE {appconfig.dataSchema}.{watershedTable} OWNER TO cwf_analyst;
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

def loadStreams(conn):
    print("Loading stream data")

    publicSchema = "public"

    flowpath = "chyf_flowpath"
    flowpathProperties = "chyf_flowpath_properties"
    flowpathNames = "chyf_names"
    aoi = "chyf_aoi"

    flowpathTable = publicSchema + "." + flowpath
    flowpathPropertiesTable = publicSchema + "." + flowpathProperties
    flowpathNamesTable = publicSchema + "." + flowpathNames
    aoiTable = publicSchema + "." + aoi

    aois = str(sheds)[1:-1].upper()
    query = f"""
    SELECT id::varchar FROM {aoiTable} WHERE short_name IN ({aois});
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    # Puts aoiTuple in a bracketed string to be used in SQL where clauses
    if len(rows) == 1:
        aoiTuple = f"('{rows[0]['id']}')"
    else:
        aoiTuple = tuple([row['id'] for row in rows])

    # Create stream tables within AOI boundaries
    query = f"""
    DROP TABLE IF EXISTS {appconfig.dataSchema}.{streamTable};
    DROP TABLE IF EXISTS {appconfig.dataSchema}.{flowpathProperties};
    
    CREATE TABLE {appconfig.dataSchema}.{streamTable} as SELECT * FROM {flowpathTable} WHERE aoi_id IN {aoiTuple} AND ef_type != 2 AND rank = 1;
    CREATE TABLE {appconfig.dataSchema}.{flowpathProperties} as SELECT * FROM {flowpathPropertiesTable} WHERE aoi_id IN {aoiTuple};

    ALTER TABLE {appconfig.dataSchema}.{streamTable} ALTER COLUMN geometry TYPE geometry(LineString, {appconfig.dataSrid}) USING ST_Transform(geometry, {appconfig.dataSrid});
    
    CREATE INDEX {appconfig.dataSchema}_{streamTable}_geometry on {appconfig.dataSchema}.{streamTable} using gist(geometry); 
    CREATE INDEX {appconfig.dataSchema}_{streamTable}_id on {appconfig.dataSchema}.{streamTable} (id);
    CREATE INDEX {appconfig.dataSchema}_{flowpathProperties}_id on {appconfig.dataSchema}.{flowpathProperties} (id);

    ALTER TABLE {appconfig.dataSchema}.{streamTable} ADD PRIMARY KEY (id);
    
    ANALYZE {appconfig.dataSchema}.{flowpathProperties};
    ANALYZE {appconfig.dataSchema}.{streamTable};

    ALTER TABLE {appconfig.dataSchema}.{streamTable} OWNER TO cwf_analyst;
    ALTER TABLE {appconfig.dataSchema}.{flowpathProperties} OWNER TO cwf_analyst;
    """
    # print(query)
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

    query = f"""
    ALTER TABLE {appconfig.dataSchema}.{streamTable} ADD COLUMN rivername1 varchar;
    ALTER TABLE {appconfig.dataSchema}.{streamTable} ADD COLUMN rivername2 varchar;
    ALTER TABLE {appconfig.dataSchema}.{streamTable} ADD COLUMN strahler_order integer;
    ALTER TABLE {appconfig.dataSchema}.{streamTable} ADD COLUMN watershed_name varchar;

    UPDATE {appconfig.dataSchema}.{streamTable} SET rivername1 = a.name_en FROM {flowpathNamesTable} a WHERE rivernameid1 IS NOT NULL AND rivernameid1 = a.name_id;
    UPDATE {appconfig.dataSchema}.{streamTable} SET rivername2 = a.name_en FROM {flowpathNamesTable} a WHERE rivernameid2 IS NOT NULL AND rivernameid2 = a.name_id;
    UPDATE {appconfig.dataSchema}.{streamTable} b SET strahler_order = a.strahler_order FROM {appconfig.dataSchema}.{flowpathProperties} a WHERE b.id = a.id;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

def loadRoads(conn):
    print("Loading Roads")
    layer = "road"
    datatable = appconfig.dataSchema + "." + roadTable
    wshedtable = appconfig.dataSchema + "." + watershedTable
    orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"

    pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nlt CONVERT_TO_LINEAR  -nln "' + temptable + '" -lco GEOMETRY_NAME=geometry "' + file + '" ' + layer
    subprocess.run(pycmd)

    query = f"""
    TRUNCATE TABLE {datatable};

    INSERT INTO {datatable}(
        id,
        name,
        geometry)       
    SELECT
        gen_random_uuid(),
        t1.name,
        CASE
            WHEN ST_WITHIN(t1.geometry,t2.geometry)
            THEN t1.geometry
            ELSE ST_Intersection(t1.geometry, t2.geometry)
            END AS geometry 
    FROM
    {temptable} t1
    JOIN {wshedtable} t2 ON ST_Intersects(t1.geometry, t2.geometry);

    UPDATE {datatable} SET name = NULL WHERE name = 'Placemark';
    UPDATE {datatable} SET name = NULL WHERE length(trim(name)) = 0;
    UPDATE {datatable} SET name = trim(name);

    ALTER TABLE {datatable} ADD COLUMN IF NOT EXISTS wshed_name varchar;
    UPDATE {datatable} t1 SET wshed_name = t2.name FROM {wshedtable} t2 WHERE ST_Contains(t2.geometry, t1.geometry);
    
    DROP table {temptable};

    ALTER TABLE {appconfig.dataSchema}.{roadTable} ADD COLUMN IF NOT EXISTS watershed_name varchar;

    ALTER TABLE {appconfig.dataSchema}.{roadTable} OWNER TO cwf_analyst;
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

def loadTrails(conn):       
    print("Loading Trails")
    layer = "trail"
    datatable = appconfig.dataSchema + "." + trailTable
    wshedtable = appconfig.dataSchema + "." + watershedTable
    orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"
    pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nlt geometry -nln "' + temptable + '" -nlt CONVERT_TO_LINEAR -nlt PROMOTE_TO_MULTI -lco GEOMETRY_NAME=geometry "' + file + '" ' + layer
    subprocess.run(pycmd)
    query = f"""
    TRUNCATE TABLE {datatable};

    INSERT INTO {datatable} (
        id,
        name,
        status,
        zone,
        geometry
    ) 
    SELECT
        gen_random_uuid(),
        t1.name,
        status,
        zone,
        CASE
            WHEN ST_WITHIN(t1.geometry,t2.geometry)
            THEN t1.geometry
            ELSE ST_Intersection(t1.geometry, t2.geometry)
            END AS geometry
    FROM
    {temptable} t1
    JOIN {wshedtable} t2 ON ST_Intersects(t1.geometry, t2.geometry);

    ALTER TABLE {datatable} ADD COLUMN IF NOT EXISTS wshed_name varchar;
    UPDATE {datatable} t1 SET wshed_name = t2.name FROM {wshedtable} t2 WHERE ST_Contains(t2.geometry, t1.geometry);

    DROP table {temptable};

    ALTER TABLE {appconfig.dataSchema}.{trailTable} ADD COLUMN IF NOT EXISTS watershed_name varchar;

    ALTER TABLE {appconfig.dataSchema}.{trailTable} OWNER TO cwf_analyst;
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

def main():

    print("Connecting to database")

    conn = appconfig.connectdb()
    loadWatersheds(conn)
    loadStreams(conn)
    loadRoads(conn)
    loadTrails(conn)

    print("Loading PEI dataset complete")

if __name__ == "__main__":
    main()