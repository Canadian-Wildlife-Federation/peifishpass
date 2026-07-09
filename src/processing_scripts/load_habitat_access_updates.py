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
# This script loads updates to habitat and accessibility information into the database
#
import subprocess
import appconfig

iniSection = appconfig.args.args[0]
streamTable = appconfig.config['DATABASE']['stream_table']
dbTargetSchema = appconfig.config[iniSection]['output_schema']
dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']
file = appconfig.config[iniSection]['habitat_access_updates']

datatable = "habitat_access_updates"

snapDistance = 125

def main():

    with appconfig.connectdb() as conn:

        query = f"""
            CREATE TABLE IF NOT EXISTS {dbTargetSchema}.{datatable}
            (
                update_source character varying COLLATE pg_catalog."default",
                update_date date,
                update_type character varying COLLATE pg_catalog."default",
                notes character varying COLLATE pg_catalog."default",
                species character varying COLLATE pg_catalog."default",
                pair_id integer,
                upstream boolean,
                downstream boolean,
                habitat_type character varying COLLATE pg_catalog."default",
                latitude double precision,
                longitude double precision,
                geom geometry(Point,2961),
                id uuid primary key,
                snapped_point geometry(Point,2961),
                stream_measure numeric,
                stream_id_up uuid,
                stream_id_down uuid
            )

            TABLESPACE pg_default;

            ALTER TABLE IF EXISTS {dbTargetSchema}.{datatable}
                OWNER to cwf_analyst;

            REVOKE ALL ON TABLE {dbTargetSchema}.{datatable} FROM PUBLIC;

            GRANT SELECT ON TABLE {dbTargetSchema}.{datatable} TO PUBLIC;

            GRANT ALL ON TABLE {dbTargetSchema}.{datatable} TO andrewp;

            GRANT ALL ON TABLE {dbTargetSchema}.{datatable} TO cwf_tech;

            GRANT ALL ON TABLE {dbTargetSchema}.{datatable} TO cwf_analyst;
        

            -- Index: habitat_access_updates_geometry_geom_idx

            -- DROP INDEX IF EXISTS {dbTargetSchema}.{datatable}_geometry_geom_idx;

            CREATE INDEX IF NOT EXISTS habitat_access_updates_geometry_geom_idx
                ON {dbTargetSchema}.{datatable} USING gist
                (geom)
                TABLESPACE pg_default;
            -- Index: habitat_access_updates_snapped_point_idx

            -- DROP INDEX IF EXISTS {dbTargetSchema}.{datatable}_snapped_point_idx;

            CREATE INDEX IF NOT EXISTS habitat_access_updates_snapped_point_idx
                ON {dbTargetSchema}.{datatable} USING gist
                (snapped_point)
                TABLESPACE pg_default;
        """

        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()  

        print("Loading habitat and accessibility updates")
        layer = "habitat_access_updates"
        orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"
        pycmd = '"' + appconfig.ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nlt CONVERT_TO_LINEAR  -nln "' + dbTargetSchema + '.' + datatable + '" -lco GEOMETRY_NAME=geom "' + file + '" ' + layer
        subprocess.run(pycmd)

        query = f"""
        ALTER TABLE {dbTargetSchema}.{datatable} DROP COLUMN IF EXISTS id;
        ALTER TABLE {dbTargetSchema}.{datatable} add column id uuid;
        UPDATE {dbTargetSchema}.{datatable} set id = gen_random_uuid();
        
        ALTER TABLE {dbTargetSchema}.{datatable} DROP COLUMN IF EXISTS snapped_point;
        ALTER TABLE {dbTargetSchema}.{datatable} add column snapped_point geometry(POINT, {appconfig.dataSrid});
        
        SELECT public.snap_to_network('{dbTargetSchema}', '{datatable}', 'geom', 'snapped_point', '{snapDistance}');

        CREATE INDEX {datatable}_snapped_point_idx ON {dbTargetSchema}.{datatable} USING gist (snapped_point);
        
        ALTER TABLE {dbTargetSchema}.{datatable} DROP COLUMN IF EXISTS stream_id;
        ALTER TABLE {dbTargetSchema}.{datatable} DROP COLUMN IF EXISTS stream_measure;
        ALTER TABLE {dbTargetSchema}.{datatable} add column stream_id uuid;
        ALTER TABLE {dbTargetSchema}.{datatable} add column stream_measure numeric;
        
        with match as (
        SELECT a.id as stream_id, b.id as pntid, st_linelocatepoint(a.geometry, b.snapped_point) as streammeasure
        FROM {dbTargetSchema}.{dbTargetStreamTable} a, {dbTargetSchema}.{datatable} b
        WHERE st_intersects(a.geometry, st_buffer(b.snapped_point, 0.0001))
        )
        UPDATE {dbTargetSchema}.{datatable}
        SET stream_id = a.stream_id, stream_measure = a.streammeasure
        FROM match a WHERE a.pntid = {dbTargetSchema}.{datatable}.id;
        """

        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

    print("Loading habitat and accessibility updates complete")


if __name__ == "__main__":
    main()
