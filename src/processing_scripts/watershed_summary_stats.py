#----------------------------------------------------------------------------------
#
# Copyright 2022 by Canadian Wildlife Federation, Alberta Environment and Parks
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
# This script summarizes watershed data creating a statistic createTable
# and populating it with various stats 
#
#
 

import appconfig
import sys


iniSection = appconfig.iniSection

wsStreamTable = appconfig.config['PROCESSING']['stream_table']
sheds = appconfig.config[iniSection]['output_schema'].split(",")
dbTargetSchema = appconfig.config[iniSection]['output_schema']

species = []
sec_sheds = []
statTable = 'habitat_stats'

def createTable():
    query = f"""
        DROP TABLE IF EXISTS {dbTargetSchema}_wcrp.{statTable};
        
        CREATE TABLE IF NOT EXISTS {dbTargetSchema}_wcrp.{statTable}(
            watershed_id varchar,
            total_km double precision,
            total_habitat_all_km double precision,

            primary key (watershed_id)
        );

        ALTER TABLE  {dbTargetSchema}_wcrp.{statTable} OWNER TO cwf_analyst;
        GRANT SELECT ON TABLE {dbTargetSchema}_wcrp.habitat_stats TO cwf_user;
    """
    with appconfig.connectdb() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            connection.commit()

def makeAccessClause(allFishAccess, fish, access, spawn=False, rear=False, habitat=False):
    """
    Creates the WHERE clause for the SQL query related to accessibility.
    This method builds a query and can be called multiple times to build the conditions for each fish in a similar way. 
    It is intended to declutter the script.

    :allFishAccess: you can pass an empty string or an existing clause upon which this method can add similar conditions with different parameters (different fish in the case of this script)
    :fish: string argument for species that the condition applies to
    :spawn: boolean, default: False; whether to include spawning habitat in the condition
    :rear: boolean, default: False; whether to include rearing habitat in the condition
    :habitat: boolean, default: False; indicates whether the clause includes only accessible habitat or all streams regardless of habitat
    :return: the where clause as a string
    """
    if allFishAccess is None:
        allFishAccess = ""
    else:
        allFishAccess = allFishAccess + " OR "

    allFishAccess = allFishAccess + f"({fish}_accessibility = '{access}' "

    if not habitat:
        return f"{allFishAccess})"
    elif spawn and not rear:
        return f'{allFishAccess} AND habitat_spawn_{fish} = true)'
    elif rear and not spawn:
        return f'{allFishAccess} AND habitat_rear_{fish} = true)'
    elif rear and spawn:
        return f'{allFishAccess} AND habitat_{fish} = true)'
    
def makeHabitatClause(clause, fish, spawn=False, rear=False):
    """
    Creates the WHERE clause for the SQL query related to habitat.
    This method builds a query and can be called multiple times to build the conditions for each fish in a similar way. 
    It is intended to declutter the script.

    :clause: you can pass an empty string or an existing clause upon which this method can add similar conditions with different parameters (different fish in the case of this script)
    :fish: string argument for species that the condition applies to
    :spawn: boolean, default: False; whether to include spawning habitat in the condition
    :rear: boolean, default: False; whether to include rearing habitat in the condition
    :return: the where clause as a string
    """
    if clause is None:
        clause = ""
    else:
        clause = clause + " OR "

    if spawn and rear:
        return clause + f"habitat_{fish} = true"
    elif spawn:
        return clause + f"habitat_spawn_{fish} = true"
    elif rear:
        return clause + f"habitat_rear_{fish} = true"
    else:
        return


def runStats():
    """
    Main engine of the script which runs all the stats queries
    """
    global species
    for shed in sheds:
        species = appconfig.getSpecies()
        q_wshed_id = f"SELECT DISTINCT watershed_id FROM {shed}.{wsStreamTable}"
        with appconfig.connectdb() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(q_wshed_id )
                    row = cursor.fetchone()
                    watershed_id = row[0]
        query = f"""
            INSERT INTO {dbTargetSchema}_wcrp.{statTable} (watershed_id)
            VALUES ('{watershed_id}');
        """
        with connection.cursor() as cursor:
            cursor.execute(query)
            connection.commit()
        
        col_query = ''
        fishaccess_query = ''
        allfishaccess_query = ''
        fishhabitat_query = ''
        connectivity_status_query = ''

        allfishaccess = None
        allfishpotentialaccess = None
        allfishaccesshabitat = None
        allfishhabitat = None
        allfishpotentialaccesshabitat = None

        for fish in species:
            col_query = f"""
                {col_query}
                ALTER TABLE {dbTargetSchema}_wcrp.{statTable}
                ADD COLUMN IF NOT EXISTS {fish}_connected_naturally_accessible_habitat_km double precision,
                ADD COLUMN IF NOT EXISTS {fish}_disconnected_naturally_accessible_habitat_km double precision,
                ADD COLUMN IF NOT EXISTS {fish}_total_habitat_km double precision,
                ADD COLUMN IF NOT EXISTS {fish}_connectivity_status double precision;
            """

            fishaccess_query = f"""
                {fishaccess_query}  
                UPDATE {dbTargetSchema}_wcrp.{statTable} 
                SET
                    {fish}_connected_naturally_accessible_habitat_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE {fish}_accessibility = '{appconfig.Accessibility.ACCESSIBLE.value}' AND habitat_{fish} = true), 0) FROM {shed}.{wsStreamTable})
                    ,{fish}_disconnected_naturally_accessible_habitat_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE {fish}_accessibility = '{appconfig.Accessibility.POTENTIAL.value}' AND dci_{fish} = 0 AND habitat_{fish} = true), 0) FROM {shed}.{wsStreamTable})
                WHERE watershed_id = '{watershed_id}';

            
                -- ADD connected and disconnected portions of partially connected streams to measures
                UPDATE {dbTargetSchema}_wcrp.{statTable} 
                SET
                    {fish}_connected_naturally_accessible_habitat_km = 
                        {fish}_connected_naturally_accessible_habitat_km 
                        + (SELECT coalesce(sum(con_func_upstr_hab_{fish}) 
                            FILTER (WHERE total_upstr_hab_{fish} > 0 AND passability_status_{fish} NOT IN ('0','1')), 0) FROM {shed}_wcrp.barrier_passability_view),
                    {fish}_disconnected_naturally_accessible_habitat_km = 
                        {fish}_disconnected_naturally_accessible_habitat_km 
                        + (SELECT coalesce(sum(discon_func_upstr_hab_{fish}) 
                            FILTER (WHERE total_upstr_hab_{fish} > 0 AND passability_status_{fish} NOT IN ('0','1')), 0) FROM {shed}_wcrp.barrier_passability_view)
                WHERE watershed_id = '{watershed_id}'; 
            """

            fishhabitat_query = f"""
                {fishhabitat_query}
                UPDATE {dbTargetSchema}_wcrp.{statTable}
                SET
                    {fish}_total_habitat_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE habitat_{fish} = true), 0) FROM {shed}.{wsStreamTable})
                    ,{fish}_connectivity_status = 
                        (SELECT 
                            (
                                ({fish}_connected_naturally_accessible_habitat_km) 
                                    / ({fish}_connected_naturally_accessible_habitat_km + {fish}_disconnected_naturally_accessible_habitat_km)
                            )*100 
                        FROM {dbTargetSchema}_wcrp.{statTable})
                WHERE watershed_id = '{watershed_id}';
            """

            allfishaccess = makeAccessClause(allfishaccess, fish, appconfig.Accessibility.ACCESSIBLE.value, habitat=False)
            allfishpotentialaccess = makeAccessClause(allfishpotentialaccess, fish, appconfig.Accessibility.POTENTIAL.value, habitat=False)
            allfishaccesshabitat = makeAccessClause(allfishaccesshabitat, fish, appconfig.Accessibility.ACCESSIBLE.value, spawn=True, rear=True, habitat=True)
            allfishpotentialaccesshabitat = makeAccessClause(allfishpotentialaccesshabitat, fish, appconfig.Accessibility.POTENTIAL.value, spawn=True, rear=True, habitat=True)

            allfishhabitat = makeHabitatClause(allfishhabitat, fish, spawn=True, rear=True)

            allfishaccess_query = f"""
                UPDATE {dbTargetSchema}_wcrp.{statTable}
                SET 
                    total_habitat_all_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE ({allfishhabitat})), 0) FROM {shed}.{wsStreamTable})
                    --,disconnected_naturally_accessible_habitat_all_km = (SELECT coalesce(sum(segment_length) FILTER (WHERE ({allfishpotentialaccesshabitat})), 0) FROM {shed}.{wsStreamTable})
                WHERE watershed_id = '{watershed_id}';
            """

            query = f"""
                UPDATE {dbTargetSchema}_wcrp.{statTable} SET total_km =
                (SELECT coalesce(sum(segment_length), 0) FROM {shed}.{wsStreamTable})
                WHERE watershed_id = '{watershed_id}';
                
                {col_query}
                {fishaccess_query}
                {fishhabitat_query}
                {allfishaccess_query}
            """
            # print(query)
            with connection.cursor() as cursor:
                cursor.execute(query)
                connection.commit()

            

def main():
    print('Computing Summary Statistics')
    createTable()
    runStats()
    print ("Computing Summary Statistics Complete")

if __name__ == "__main__":
    main()