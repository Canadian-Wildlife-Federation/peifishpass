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

dataTable = "habitat_access_updates"

snapDistance = appconfig.config['CABD_DATABASE']['snap_distance']

def main():

    with appconfig.connectdb() as conn:

        query = f"""DROP TABLE IF EXISTS {dbTargetSchema}.{dataTable};"""
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

        print("Loading habitat and accessibility updates")
        layer = "hab_access_updates"
        orgDb="dbname='" + appconfig.dbName + "' host='"+ appconfig.dbHost+"' port='"+appconfig.dbPort+"' user='"+appconfig.dbUser+"' password='"+ appconfig.dbPassword+"'"
        pycmd = '"' + appconfig.ogr + '" -overwrite -f "PostgreSQL" PG:"' + orgDb + '" -t_srs EPSG:' + appconfig.dataSrid + ' -nlt CONVERT_TO_LINEAR  -nln "' + dbTargetSchema + '.' + dataTable + '" -lco GEOMETRY_NAME=geometry "' + file + '" ' + layer
        subprocess.run(pycmd)

        query = f"""

        ALTER TABLE {dbTargetSchema}.{dataTable} add column id uuid;
        UPDATE {dbTargetSchema}.{dataTable} set id = gen_random_uuid();
        
        ALTER TABLE {dbTargetSchema}.{dataTable} add column snapped_point geometry(POINT, {appconfig.dataSrid});
        
        SELECT public.snap_to_network('{dbTargetSchema}', '{dataTable}', 'geometry', 'snapped_point', '{snapDistance}');

        CREATE INDEX {dataTable}_snapped_point_idx ON {dbTargetSchema}.{dataTable} USING gist (snapped_point);
        
        ALTER TABLE {dbTargetSchema}.{dataTable} add column stream_id uuid;
        ALTER TABLE {dbTargetSchema}.{dataTable} add column stream_measure numeric;
        
        with match as (
        SELECT a.id as stream_id, b.id as pntid, st_linelocatepoint(a.geometry, b.snapped_point) as streammeasure
        FROM {dbTargetSchema}.{dbTargetStreamTable} a, {dbTargetSchema}.{dataTable} b
        WHERE st_intersects(a.geometry, st_buffer(b.snapped_point, 0.0001))
        )
        UPDATE {dbTargetSchema}.{dataTable}
        SET stream_id = a.stream_id, stream_measure = a.streammeasure
        FROM match a WHERE a.pntid = {dbTargetSchema}.{dataTable}.id;
        """

        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

if __name__ == "__main__":
    main()
