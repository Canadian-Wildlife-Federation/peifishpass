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
# this script computes upstream/downstream barrier counts, ids, and fish species for 
# fish survey and stocking information
#
#
import appconfig
import shapely.wkb
from collections import deque
import psycopg2.extras


dbTargetSchema = appconfig.config['PROCESSING']['output_schema']
watershed_id = appconfig.config['PROCESSING']['watershed_id']

dbTargetStreamTable = appconfig.config['PROCESSING']['stream_table']
dbBarrierTable = appconfig.config['BARRIER_PROCESSING']['barrier_table']
dbModelledCrossingsTable = appconfig.config['MODELLED_CROSSINGS']['modelled_crossings_table']


edges = []
nodes = dict()

class Node:
    
    def __init__(self, x, y):
        self.inedges = []
        self.outedges = []
        self.x = x
        self.y = y
        self.barrierids = set()
   
    def addInEdge(self, edge):
        self.inedges.append(edge)
   
    def addOutEdge(self, edge):
        self.outedges.append(edge)
    
   
    
class Edge:
    def __init__(self, fromnode, tonode, fid, ls):
        self.fromNode = fromnode
        self.toNode = tonode
        self.ls = ls
        self.fid = fid
        self.visited = False
        self.upbarriers = set()
        self.downbarriers = set()
        self.stockedge = set()
        self.stockup = set()
        self.stockdown = set()
        self.surveyedge = set()
        self.surveyup = set()
        self.surveydown = set()
        
def createNetwork(connection):
    
    query = f"""
        SELECT a.{appconfig.dbIdField} as id, a.{appconfig.dbGeomField}
        FROM {dbTargetSchema}.{dbTargetStreamTable} a
    """
   
    #load geometries and create a network
    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
        
        
        for feature in features:
            fid = feature[0]
            geom = shapely.wkb.loads(feature[1] , hex=True)
            
            startc = geom.coords[0]
            endc = geom.coords[len(geom.coords)-1]
            
            startt = (startc[0], startc[1])
            endt = (endc[0], endc[1])            
            
            if (startt in nodes.keys()):
                fromNode = nodes[startt]
            else:
                #create new node
                fromNode = Node(startc[0], startc[1])
                nodes[startt] = fromNode
            
            if (endt in nodes.keys()):
                toNode = nodes[endt]
            else:
                #create new node
                toNode = Node(endc[0], endc[1])
                nodes[endt] = toNode
            
            edge = Edge(fromNode, toNode, fid, geom)
            edges.append(edge)
            
            fromNode.addOutEdge(edge)
            toNode.addInEdge(edge)     
            
    #add barriers
    query = f"""
        select 'up', a.id, b.id 
        from {dbTargetSchema}.{dbBarrierTable} a, {dbTargetSchema}.{dbTargetStreamTable} b
        where b.geometry && st_buffer(a.snapped_point, 0.0000001)
            and st_distance(st_startpoint(b.geometry), a.snapped_point) < 0.00000001 
        union 
        select 'down', a.id, b.id 
        from {dbTargetSchema}.{dbBarrierTable} a, {dbTargetSchema}.{dbTargetStreamTable} b
        where b.geometry && st_buffer(a.snapped_point, 0.0000001)
            and st_distance(st_endpoint(b.geometry), a.snapped_point) < 0.00000001        
    """
   
    #load geometries and create a network
    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
        
        
        for feature in features:
            etype = feature[0]
            bid = feature[1]
            sid = feature[2]
            
            for edge in edges:
                if (edge.fid == sid):
                    if (etype == 'up'):
                        edge.fromNode.barrierids.add(bid)
                    elif (etype == 'down'):
                        edge.toNode.barrierids.add(bid)
                        
                        

    #add species and stocking details
    query = f"""
        select a.stream_id, a.spec_code
        FROM {dbTargetSchema}.fish_stocking a
        WHERE spec_code IS NOT NULL
    """ 
    
    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
        
        
        for feature in features:
            sid = feature[0]
            speccode = feature[1]
            
            for edge in edges:
                if (edge.fid == sid):
                    edge.stockedge.add(speccode)
    
    query = f"""
        select a.stream_id, a.spec_code
        FROM {dbTargetSchema}.fish_survey a
        WHERE spec_code IS NOT NULL
    """
   
    #add species and stocking details
    with connection.cursor() as cursor:
        cursor.execute(query)
        features = cursor.fetchall()
        
        
        for feature in features:
            sid = feature[0]
            speccode = feature[1]
            
            for edge in edges:
                if (edge.fid == sid):
                    edge.surveyedge.add(speccode)
                                            

def processNodes():
    
    
    #walk down network        
    toprocess = deque()
    for edge in edges:
        edge.visited = False
        
    for node in nodes.values():
        if (len(node.inedges) == 0):
            toprocess.append(node)
            
    while (toprocess):
        node = toprocess.popleft()
        
        allvisited = True
        
        upbarriers = set()
        
        stockup = set()
        surveyup = set()
         
        for inedge in node.inedges:
               
            if not inedge.visited:
                allvisited = False;
                break;
            else:
                upbarriers.update(inedge.upbarriers)
                stockup.update(inedge.stockup)
                stockup.update(inedge.stockedge)
                surveyup.update(inedge.surveyup)
                surveyup.update(inedge.surveyedge)
                
        if not allvisited:
            toprocess.append(node)
        else:
            upbarriers.update(node.barrierids)
        
            for outedge in node.outedges:
                outedge.upbarriers.update(upbarriers)
                outedge.stockup.update(stockup)
                outedge.surveyup.update(surveyup)
                
                outedge.visited = True
                if (not outedge.toNode in toprocess):
                    toprocess.append(outedge.toNode)
            
            
    #walk up computing mainstem id
    for edge in edges:
        edge.visited = False
        
    toprocess = deque()
    for node in nodes.values():
        if (len(node.outedges) == 0):
            toprocess.append(node)
    
    while (toprocess):
        node = toprocess.popleft()
        
        if (len(node.inedges) == 0):
            continue
        
        downbarriers = set()
        downbarriers.update(node.barrierids)
        
        stockdown = set()
        surveydown = set()
        
        allvisited = True
        
        for outedge in node.outedges:
            if not inedge.visited:
                allvisited = False;
                break;
            else:
                downbarriers.update(outedge.downbarriers)
                stockdown.update(outedge.stockdown)
                stockdown.update(outedge.stockedge)
                surveydown.update(outedge.surveydown)
                surveydown.update(outedge.surveyedge)
        
        if not allvisited:
            toprocess.append(node)
        else:
            for inedge in node.inedges:
                inedge.downbarriers.update(downbarriers)
                inedge.stockdown.update(stockdown)                
                inedge.surveydown.update(surveydown)
                inedge.visited = True
                if (not outedge.toNode in toprocess):
                    toprocess.append(inedge.fromNode)
    
        
def writeResults(connection):
      
    updatequery = f"""
        UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET 
            barrier_up_cnt = %s,
            barrier_down_cnt = %s,
            barriers_up = %s,
            barriers_down = %s,
            fish_stock_up = %s,
            fish_stock_down = %s,
            fish_survey_up = %s,
            fish_survey_down = %s
            
        WHERE id = %s;
    """
    
    newdata = []
    
    for edge in edges:
        upbarriersstr = (list(edge.upbarriers),)  
        downbarriersstr = (list(edge.downbarriers),)  
        upstockstr = (list(edge.stockup),)  
        downstockstr = (list(edge.stockdown),) 
        upsurveystr = (list(edge.surveyup),)  
        downsurveystr = (list(edge.surveydown),) 
        
        newdata.append( (len(edge.upbarriers), len(edge.downbarriers), upbarriersstr, downbarriersstr, upstockstr, downstockstr, upsurveystr, downsurveystr, edge.fid))

    
    with connection.cursor() as cursor:    
        psycopg2.extras.execute_batch(cursor, updatequery, newdata);
            
    connection.commit()


#--- main program ---
def main():
    
    edges.clear()
    nodes.clear()
        
    with appconfig.connectdb() as conn:
        
        conn.autocommit = False
        
        print("Computing Upstream/Downstream Barriers")
        print("  creating output column")
        #add a new geometry column for output removing existing one
        query = f"""
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS barrier_up_cnt;
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS barrier_down_cnt;
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS barriers_up;
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS barriers_down;
            
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS fish_stock_up;
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS fish_stock_down;
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS fish_survey_up;
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} DROP COLUMN IF EXISTS fish_survey_down;
            
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN barrier_up_cnt int;
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN barrier_down_cnt int;
            
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN barriers_up varchar[];
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN barriers_down varchar[];
            
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN fish_stock_up varchar[];
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN fish_stock_down varchar[];
            
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN fish_survey_up varchar[];
            ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD COLUMN fish_survey_down varchar[];
        """
        
        with conn.cursor() as cursor:
            cursor.execute(query)
        
        print("  creating network")
        createNetwork(conn)
        
        print("  processing nodes")
        processNodes()
            
        print("  writing results")
        writeResults(conn)
        
    print("done")
    
if __name__ == "__main__":
    main()      