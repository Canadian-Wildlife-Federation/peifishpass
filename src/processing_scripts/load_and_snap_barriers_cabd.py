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
# Loads dam barriers from the CABD API into local database and beaver activity from local partners
#
import subprocess
import json
import urllib.request
import appconfig
from appconfig import dataSchema

iniSection = appconfig.args.args[0]

dbTargetSchema = appconfig.config[iniSection]['output_schema']
dbWatershedId = appconfig.config[iniSection]['watershed_id']
beaverData = appconfig.config[iniSection]['beaver_data']
dbTempTable = 'beaver_activity_' + dbWatershedId
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']
workingWatershedId = appconfig.config[iniSection]['watershed_id']
nhnWatershedId = appconfig.config[iniSection]['nhn_watershed_id']

dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']
snapDistance = appconfig.config['CABD_DATABASE']['snap_distance']

def tableExists(conn):

    query = f"""
    SELECT EXISTS(SELECT 1 FROM information_schema.tables 
    WHERE table_catalog='{appconfig.dbName}' AND 
        table_schema='{dbTargetSchema}' AND 
        table_name='{dbBarrierTable}');
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
        result = result[0]

    return result

def createTable(conn):

    result = tableExists(conn)

    if result:

        # create an archive table so we can keep barrier beach and beaver activity ids stable

        query = f"""
        
        DROP TABLE IF EXISTS {dbTargetSchema}.{dbBarrierTable}_archive;
        CREATE TABLE {dbTargetSchema}.{dbBarrierTable}_archive 
        AS SELECT * FROM {dbTargetSchema}.{dbBarrierTable};

        ALTER TABLE {dbTargetSchema}.{dbBarrierTable}_archive OWNER TO cwf_analyst;

        DROP TABLE IF EXISTS {dbTargetSchema}.{dbBarrierTable};

        create table if not exists {dbTargetSchema}.{dbBarrierTable} (
            id uuid not null default gen_random_uuid(),
            cabd_id uuid,
            modelled_id uuid,
            update_id varchar,
            original_point geometry(POINT, {appconfig.dataSrid}),
            snapped_point geometry(POINT, {appconfig.dataSrid}),
            name varchar(256),
            type varchar(32),
            assessment_type varchar(256),
            owner varchar,
            passability_status varchar,
            passability_status_notes varchar,

            dam_use varchar,

            stream_name varchar,
            strahler_order integer,
            stream_id uuid,
            wshed_name varchar,
            transport_feature_name varchar,
            
            crossing_status varchar CHECK (crossing_status in ('MODELLED', 'PRESENCE CONFIRMED', 'ASSESSED', 'HABITAT_CONFIRMATION', 'DESIGN', 'REMEDIATED')),
            crossing_feature_type varchar CHECK (crossing_feature_type IN ('ROAD', 'RAIL', 'TRAIL')),
            crossing_type varchar,
            crossing_subtype varchar,
            
            culvert_number varchar,
            structure_id varchar,
            date_examined date,
            culvert_type varchar,
            culvert_condition varchar,
            action_items varchar,

            primary key (id)
        );
    
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} OWNER TO cwf_analyst;
        """

        with conn.cursor() as cursor:
            cursor.execute(query)

    else:

        # creates barriers table with attributes from CABD and crossings table
        query = f"""
            DROP TABLE IF EXISTS {dbTargetSchema}.{dbBarrierTable};

            create table if not exists {dbTargetSchema}.{dbBarrierTable} (
                id uuid not null default gen_random_uuid(),
                cabd_id uuid,
                modelled_id uuid,
                update_id varchar,
                original_point geometry(POINT, {appconfig.dataSrid}),
                snapped_point geometry(POINT, {appconfig.dataSrid}),
                name varchar(256),
                type varchar(32),
                assessment_type varchar(256),
                owner varchar,
                passability_status varchar,
                passability_status_notes varchar,

                dam_use varchar,

                stream_name varchar,
                strahler_order integer,
                stream_id uuid,
                wshed_name varchar,
                transport_feature_name varchar,
                
                crossing_status varchar CHECK (crossing_status in ('MODELLED', 'PRESENCE CONFIRMED', 'ASSESSED', 'HABITAT_CONFIRMATION', 'DESIGN', 'REMEDIATED')),
                crossing_feature_type varchar CHECK (crossing_feature_type IN ('ROAD', 'RAIL', 'TRAIL')),
                crossing_type varchar,
                crossing_subtype varchar,
                
                culvert_number varchar,
                structure_id varchar,
                date_examined date,
                culvert_type varchar,
                culvert_condition varchar,
                action_items varchar,

                primary key (id)
            );
        
        ALTER TABLE {dbTargetSchema}.{dbBarrierTable} OWNER TO cwf_analyst;
            
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

def getCABD(conn):

    # retrieve barrier data from CABD API
    url = f"https://cabd-web.azurewebsites.net/cabd-api/features/dams?&filter=nhn_watershed_id:eq:{nhnWatershedId}&filter=use_analysis:eq:true"
    response = urllib.request.urlopen(url)
    data = json.loads(response.read())

    feature_data = data["features"]
    output_data = []

    for feature in feature_data:
        output_feature = []
        output_feature.append(feature["properties"]["cabd_id"])
        output_feature.append(feature["geometry"]["coordinates"][0])
        output_feature.append(feature["geometry"]["coordinates"][1])
        output_feature.append(feature["properties"]["dam_name_en"])
        output_feature.append(feature["properties"]["owner"])
        output_feature.append(feature["properties"]["dam_use"])
        output_feature.append(feature["properties"]["passability_status"])
        output_data.append(output_feature)

    insertquery = f"""
        INSERT INTO {dbTargetSchema}.{dbBarrierTable} (
            cabd_id, 
            original_point,
            name,
            owner,
            dam_use,
            passability_status,
            type)
        VALUES (%s, ST_Transform(ST_GeomFromText('POINT(%s %s)',4617),{appconfig.dataSrid}), %s, %s, %s, UPPER(%s), 'dam');
    """
    with conn.cursor() as cursor:
        for feature in output_data:
            cursor.execute(insertquery, feature)
    conn.commit()

    # snaps barrier features to network
    query = f"""
        CREATE OR REPLACE FUNCTION public.snap_to_network(src_schema varchar, src_table varchar, raw_geom varchar, snapped_geom varchar, max_distance_m double precision) RETURNS VOID AS $$
        DECLARE    
            pnt_rec RECORD;
            fp_rec RECORD;
        BEGIN
            FOR pnt_rec IN EXECUTE format('SELECT id, %I as rawg FROM %I.%I WHERE %I is not null', raw_geom, src_schema, src_table,raw_geom) 
            LOOP 
                FOR fp_rec IN EXECUTE format ('SELECT fp.geometry as geometry, st_distance(%L::geometry, fp.geometry) AS distance FROM {dbTargetSchema}.{dbTargetStreamTable} fp WHERE st_expand(%L::geometry, %L) && fp.geometry and st_distance(%L::geometry, fp.geometry) < %L ORDER BY distance ', pnt_rec.rawg, pnt_rec.rawg, max_distance_m, pnt_rec.rawg, max_distance_m)
                LOOP
                    EXECUTE format('UPDATE %I.%I SET %I = ST_LineInterpolatePoint(%L::geometry, ST_LineLocatePoint(%L::geometry, %L::geometry) ) WHERE id = %L', src_schema, src_table, snapped_geom,fp_rec.geometry, fp_rec.geometry, pnt_rec.rawg, pnt_rec.id);
                    EXIT;
                END LOOP;
            END LOOP;
        END;
        $$ LANGUAGE plpgsql;
        
        SELECT public.snap_to_network('{dbTargetSchema}', '{dbBarrierTable}', 'original_point', 'snapped_point', '{snapDistance}');

        --remove any dam features not snapped to streams
        --because using nhn_watershed_id can cover multiple HUC8 watersheds
        DELETE FROM {dbTargetSchema}.{dbBarrierTable}
        WHERE snapped_point IS NULL
        AND type = 'dam';
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

    # set the id for all dams to be the cabd_id so that it's a stable id
    query = f"""
        UPDATE {dbTargetSchema}.{dbBarrierTable} SET id = cabd_id WHERE type = 'dam';
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

    print("Loading barriers from CABD dataset complete")

def getBeaverData(conn):

    # add beaver activity data and snap to network

    orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"

    pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nln "' + dbTargetSchema + '.' + dbTempTable + '" -lco GEOMETRY_NAME=geometry "' + beaverData + '" -oo EMPTY_STRING_AS_NULL=YES'
    # print(pycmd)
    subprocess.run(pycmd)

    query = f"""
        INSERT INTO {dbTargetSchema}.{dbBarrierTable} (
            original_point,
            passability_status,
            type)
        SELECT
            ST_Force2D(geometry),
            'BARRIER',
            'beaver_activity'
        FROM
            {dbTargetSchema}.{dbTempTable};

        SELECT public.snap_to_network('{dbTargetSchema}', '{dbBarrierTable}', 'original_point', 'snapped_point', '{snapDistance}');

        DROP TABLE IF EXISTS {dbTargetSchema}.{dbTempTable};
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

    print("Loading beaver activity data complete")

def addPassability(conn, specCodes):

    # add species-specific passability fields
    for species in specCodes:
        code = species[0]

        colname = "passability_status_" + code

        query = f"""
            alter table {dbTargetSchema}.{dbBarrierTable} 
            add column if not exists {colname} numeric;

            update {dbTargetSchema}.{dbBarrierTable}
            set {colname} = 
                CASE
                WHEN passability_status = 'BARRIER' THEN 0
                WHEN passability_status = 'UNKNOWN' THEN 0
                WHEN passability_status = 'PARTIAL BARRIER' THEN 0.5
                WHEN passability_status = 'PASSABLE' THEN 1
                ELSE NULL END;
        """

        with conn.cursor() as cursor:
            cursor.execute(query)

    query = f"""
        alter table {dbTargetSchema}.{dbBarrierTable} 
        drop column if exists passability_status;
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

def main():

    with appconfig.connectdb() as conn:

        print("Loading barrier data")

        query = f"""
        SELECT code
        FROM {dataSchema}.{appconfig.fishSpeciesTable};
        """

        with conn.cursor() as cursor:
            cursor.execute(query)
            specCodes = cursor.fetchall()

        print("  creating tables")
        createTable(conn)

        print("  fetching barriers from CABD")
        getCABD(conn)

        # print("  loading beaver activity data")
        # getBeaverData(conn)

        print("  adding passability status")
        addPassability(conn, specCodes)

    print("Loading barrier data complete")

if __name__ == "__main__":
    main()
