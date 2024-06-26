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
# This script creates the database tables that follow the structure
# in the gdb file.
#
 
import appconfig

roadTable = appconfig.config['CREATE_LOAD_SCRIPT']['road_table']
trailTable = appconfig.config['CREATE_LOAD_SCRIPT']['trail_table']

query = f"""
    drop schema if exists {appconfig.dataSchema} cascade;

    CREATE EXTENSION IF NOT EXISTS postgis;
    
    create schema {appconfig.dataSchema};
    
    create table {appconfig.dataSchema}.{appconfig.streamTable} (
        id uuid not null,
        watershed_id varchar,
        stream_name varchar,
        strahler_order integer,
        fish_species varchar,
        ef_type smallint,
        ef_subtype smallint,
        rank smallint,
        length double precision,
        aoi_id varchar,
        from_nexus_id uuid,
        to_nexus_id uuid,
        ecatchment_id uuid,
        graph_id integer,
        mainstem_id uuid,
        max_uplength double precision,
        hack_order integer,
        horton_order integer,
        shreve_order integer,
        objectid integer,
        enabled integer,
        hydroid integer,
        hydrocode varchar(30),
        reachcode varchar(30),
        name varchar(100),
        lengthkm double precision,
        lengthdown double precision,
        flowdir integer,
        ftype varchar(30),
        edgetype integer,
        shape_leng double precision,
        primary_ varchar(50),
        secondary varchar(50),
        tertiary varchar(50),
        label varchar(50),
        source varchar(50),
        picture varchar(100),
        field_date date,
        stream_sou varchar(50),
        comment varchar(75),
        flipped varchar(10),
        from_node integer,
        to_node integer,
        nextdownid integer,
        fromelev double precision,
        toelev double precision,
        geometry geometry(linestring, {appconfig.dataSrid}) not null,
        primary key(id)
    );

    ALTER TABLE {appconfig.dataSchema}.{appconfig.streamTable} OWNER TO cwf_analyst;

    create index {appconfig.streamTable}_geom2d_idx on {appconfig.dataSchema}.{appconfig.streamTable} using gist(geometry);
    
    create table {appconfig.dataSchema}.{roadTable} ( 
        id uuid not null,
        name varchar,
        wshed_name varchar,
        geometry geometry(geometry, {appconfig.dataSrid}) not null,
        primary key(id)
    );
    create index {roadTable}_geom_idx on {appconfig.dataSchema}.{roadTable} using gist(geometry);

    ALTER TABLE {appconfig.dataSchema}.{roadTable} OWNER TO cwf_analyst;
    
    create table {appconfig.dataSchema}.{trailTable} ( 
        id uuid not null,
        name varchar,
        wshed_name varchar,
        status varchar(100),
        zone varchar(100),
        geometry geometry(geometry, {appconfig.dataSrid}) not null,
        primary key(id)
    );
    create index {trailTable}_geom_idx on {appconfig.dataSchema}.{trailTable} using gist(geometry);

    ALTER TABLE {appconfig.dataSchema}.{trailTable} OWNER TO cwf_analyst;
    
"""

with appconfig.connectdb() as conn:
    with conn.cursor() as cursor:
        cursor.execute(query)

print (f"""Database schema {appconfig.dataSchema} created and ready for data """)
    