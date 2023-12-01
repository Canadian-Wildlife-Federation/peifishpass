# Dealing with accessibility comments from workshops:
# If comment is that this is a "limit" for species - split stream, and make all segments upstream NOT accessible to that species. Would it be possible to add these into the break_points table? We might just need to add special handling for a new "accessibility_limit" type of break point. Then we could handle it when we break streams at barriers. If we take that approach though, we also need to handle it separately when we run compute_updown_barriers_fish.py.
# It might then be better to have a separate script or function that does this - and that runs as part of break_streams_at_barriers.py?
# Ok yeah we probably want to add this to break_streams_at_barriers.py and have this be a new accessibility limit thing. Then in compute_updown_barriers_fish.py, we calculate how many accessibility limits are downstream of the stream segment --> if this is greater than 1 we mark as not accessible when we get to compute_accessibility.py

# What about habitat comments from workshops?
# We likely need to handle these in a similar way - break streams and then do something either with all upstream segments or all downstream segments.
# So perhaps we need to take the network traversal in compute_updown_barriers_fish.py and adapt it for this new purpose
# Where we have a comment that says something like "limit for x" or "no habitat for x upstream" we then need to know which segments are upstream of that point (assuming the stream segment is broken at that point)
# Will we ever have comments that indicate there is no habitat for a species downstream? We do probably need to account for this.

# Atlantic salmon

# Accessibility base calculation: based on positions of gradient barriers and human/natural barriers
# Accessibility modified by: updates from workshop

# Habitat base calculation: all areas assumed to be habitat
# Habitat modified by: updates from workshop, redd surveys (redd locations are marked as spawning habitat)
# Updates to habitat modifications: we might want to be more selective about the areas marked as spawning from the redd surveys?

# American eel

# Accessibility base calculation: based on positions of gradient barriers and human/natural barriers
# Accessibility modified by: updates from workshop

# Habitat base calculation: no spawning habitat, but all streams where strahler_order >= 2 are marked as rearing habitat
# Habitat modified by: updates from workshop

# Smelt

# Accessibility base calculation: based on positions of gradient barriers and human/natural barriers
# Accessibility modified by: updates from workshop
# Updates to accessibility: awaiting information from Keila / SAB about smelt accessibility limits in other watersheds

# Habitat base calculation: historically accessible areas (i.e., accessible or potentially accessible) and where the gradient is within the smelt gradient limits
# Habitat modified by: updates from workshop

import getpass
import psycopg2 as pg2
from psycopg2.extras import DictCursor
import appconfig

dbHost = "cabd-postgres.postgres.database.azure.com"
dbPort = "5432"
dbName = "peifishpass"

dbUser = input(f"""Enter username to access {dbName}:\n""")
dbPassword = getpass.getpass(f"""Enter password to access {dbName}:\n""")

dbSourceStreamTable = "ws01cd000.streams"
dbSourcePointTable = "ws01cd000.habitat_access_updates"

sourceStreams = "'25046a92-cfeb-406e-9627-7179cdc869a0', '9f5adbee-0266-4db0-9205-0a888fdde6f2', '37def8a1-7999-4399-8a97-3c842eb05b08', '7689847c-ec5b-4d6f-b464-3478effafdb4', 'ec2a92de-b329-42d0-81de-a0aaf8fce6ba', 'bd66f9fb-86aa-483f-8385-2ff3aae3a598', '203aafaf-2389-43fc-a2f6-c0ce7f8255e9', '4d32ce23-773e-4fd1-8243-1116a8bdefde', 'f724aaa6-0b42-4900-965e-16ef2ed1dbd0', '530ac3d2-c44b-48f0-9dd7-68426e4ac9d4', '73369fbd-b7a8-43d6-ba0c-70837a1e3ee3', 'dff6e827-34e4-46c2-84f6-5e539d123f41', 'bc11baf8-a535-4fd8-bed7-c8755ad714c0', 'f395ca7d-f7eb-4c3c-8c84-26a1ee617c98', '380adc9b-a004-4f28-a0ec-bdfb0e5a5253', '865e5579-61bf-4b03-8bfb-7c5a1bfd0213', '3c33ebf3-7d08-4645-b47c-02b435629746', '9d70f497-9d12-4f38-8a8c-b77c6c14742e', '35c59f6f-f819-424a-b7f8-961eabb1a63a', 'ec55dead-3e69-47be-8879-0dec85a64e61', '3990ff69-198f-4c08-860c-7272ca310b49', 'd93c71bd-c729-4d0d-905c-801c05420eeb', '6fbe06e0-f21f-44c2-9e67-58f34edc8cd3', '1c3e3807-a96e-471a-b718-35e011a478ca', '08c24139-5acf-4c61-95cd-eaf7ffb4ef24', 'f4e5a590-338c-4501-adce-f65fd5939b82', '402ce346-1c72-4440-835f-a9df31ded662', 'f8c8eb97-1177-4c26-ae83-63a4168d348f', 'feb52012-6aa8-4afd-aba4-0783108e3083', '598100e0-dfeb-4683-9c53-75f9bf1c05db', '7e7ef410-a773-4b72-bad8-6f0fa49bfc15', '699b1eea-8834-43d7-939a-30fef2187821', '13e6bab2-39a9-4061-87fe-2c48e5badacd', '649c7d79-a32b-41b8-aab1-fd639b49edcf', '2fd62e35-af8e-453a-8687-ba4db4107810', '0d250f58-743d-45f3-9c2b-6cd4c9f67188', '35c59f6f-f819-424a-b7f8-961eabb1a63a', '8c9c15d4-8535-4fc3-9c2a-76941b97bb30', '35c59f6f-f819-424a-b7f8-961eabb1a63a', 'cae32028-e2e6-406a-a2eb-6b674f8fa893', 'b9fc47c7-bf91-46be-9041-1ac93833d91e', 'fdb424d2-c6df-45f9-9399-b5ecb5ab07d8', '0d250f58-743d-45f3-9c2b-6cd4c9f67188', '2b028dc9-0845-4f50-a3aa-6fa7144ac960', 'bfdefb10-4a7f-44ef-881a-b859ad630153', '08bbf232-04ce-42d7-b775-fdcc614d3a87', 'de13bf1f-853f-44a7-a49e-e908d826cb78', '589576a9-de52-4249-ae6c-d7e84885ccdd', '70ced307-05c7-430b-be44-6299a8a14ba8', '203aafaf-2389-43fc-a2f6-c0ce7f8255e9', 'b0e1b121-d523-4158-b440-a05cd5b4147c', '74461ecd-bc75-49e8-83a9-b504cec0404c', 'ed4dc419-6584-413e-8c8b-a710b11c550f', '29da6e2c-a749-42b9-b837-543de6550339', 'a80f760b-4477-4cfe-b9ab-834d8f1ca97b', 'b0e1b121-d523-4158-b440-a05cd5b4147c', '0c235861-5f62-42f8-8f19-e85b8a0e5279', '12a6a2bc-ca00-4e27-b8f4-156b025a138f', '35c59f6f-f819-424a-b7f8-961eabb1a63a', '863b3cff-1b72-4f55-b754-accddb8e15df', 'd87cfee4-8534-4849-8655-1e41957008a1', '8c6ecd0d-53c8-494d-a207-6409b5e65391', 'cff59cbd-6e38-4e1e-945b-cd6ceecdc64c', '4d454c6f-e1f5-4e9c-b5f5-bef38cbc9237', '494740c2-ff86-4763-bf11-83e2c20a95ed', '81048fa9-7517-4f1d-8fe9-90a4790bcab8', '35c59f6f-f819-424a-b7f8-961eabb1a63a', '9e276ecc-1fbb-45db-9cfd-cff7bf50cb68', '35c59f6f-f819-424a-b7f8-961eabb1a63a', '9e7c9777-f79c-44b7-9985-e1b8213e8a98', 'b24e4032-4ff9-49e8-a7d5-b574d4290453', 'b0e1b121-d523-4158-b440-a05cd5b4147c', '08c24139-5acf-4c61-95cd-eaf7ffb4ef24', '36161eba-10da-4bb4-bdde-230815340a4a', 'ed91a264-96ba-470f-8698-e53b1c210e70', '649c7d79-a32b-41b8-aab1-fd639b49edcf', 'a9566d0c-cbb6-4c48-a04a-f005d670fe0b', 'acc07aa2-2875-470c-b70f-d451f52a121d', 'c06b0b37-ccdd-4da6-bd80-ad3db084024e', '863b3cff-1b72-4f55-b754-accddb8e15df', '18eba235-047e-4332-888d-bf114b9cdcbb', 'f056056c-c0c2-486a-980e-edc89cf681a5', '29da6e2c-a749-42b9-b837-543de6550339', 'e87abe73-4e10-4dcc-ba24-9be9e979f439', '3c512bf2-e226-4d39-b76a-cc4407eb97a5', '7c0748cb-6c7c-4d92-a763-4734e2fc8589', 'e23d52e4-30d9-42e4-b5f4-3c70b62c4fe8', '0d576fcc-2407-4de0-8359-1d9030faae7b', '08637781-1835-4acb-ad5f-f3537d83fd3c', '7767718f-2b7d-4e94-bc29-16c0603101c1', '494740c2-ff86-4763-bf11-83e2c20a95ed', 'f1bbea4d-fa75-44e5-a243-e59100d90459', '0d133b2b-88c7-4758-9566-132a29963b17', '1bcf208c-39e3-4150-ab23-5f8ccd6b8178', 'f3b35926-ae87-432c-ab71-cafa3ddee76e', '0e59b061-d620-4318-a41a-fa6c0371c539', 'b9fc47c7-bf91-46be-9041-1ac93833d91e', 'c4b24cd9-2ec3-409f-b872-a5e89cf942b9', '21252e86-437e-494c-af70-93e24f686a71', '4feee273-0f92-4bf2-a79d-94c629504aaf', '598100e0-dfeb-4683-9c53-75f9bf1c05db', 'befa0b16-930c-4581-96ae-f32d6e983d5d', '0fbb7af3-18e4-4a43-8ed5-82e24b019b2f', '70fd7aa1-78a1-436f-86ee-d85b7211c0c5', '82367142-675b-4454-b6f7-ea82c95ad3b5', '0b1975d0-dad4-4040-940a-9373adffd6ad', '0bf7861e-e508-4166-92c5-3d9dcf6e8a89', 'bc781b2f-a044-4178-b026-44efdbbcca49', '7a3e5289-dd77-4a46-a602-45aa2daa8bfb', 'ea7dd342-47a2-411f-b96c-11e6bee865c8', '00408118-7a9d-460f-8738-ed0ff78d2198', '7dadff1c-2042-42a2-a855-c74165ee0925', '589576a9-de52-4249-ae6c-d7e84885ccdd', 'fa196d6b-2584-48d6-b08e-3f41a61b2a0c', '5df2ade4-b285-4fb4-8c7d-454d4e4e1b61', 'e7176439-3c6a-44e2-ab39-cee277c4b940', '86993ed0-0370-4cd3-863d-31cb789c0582', '530ac3d2-c44b-48f0-9dd7-68426e4ac9d4', '54c89f1f-35f1-4395-8066-80c88ba9566f', '67b8f3a4-c486-4af4-baea-4596a87662ff', '08c24139-5acf-4c61-95cd-eaf7ffb4ef24', '2e7dd261-450a-463e-85d4-6826326d6ddb', 'e0252407-cd55-4605-864d-7f6188ccb0a8', '35c59f6f-f819-424a-b7f8-961eabb1a63a', '4750ddf5-1a9e-416d-a1f3-398f851c0160', 'c727d1f1-ddb3-48ad-bb40-3b096e2d96a8', '380adc9b-a004-4f28-a0ec-bdfb0e5a5253', '598100e0-dfeb-4683-9c53-75f9bf1c05db', '1d976631-fe96-44c5-8713-34ff83d017cb', 'b24e4032-4ff9-49e8-a7d5-b574d4290453', '7a5f2c8f-4f94-4277-b6ff-ff5bdf2b8039', '75bfee24-638a-4a5d-a2e8-33c143bd0bcb', 'd56cbe9d-b45c-4914-a196-498e2918d627', '6b999686-0b00-437a-a707-f8d93275cfc3', 'ed578891-d1d4-4ac5-add4-54f50bb46f2a', 'ebcef347-e3ce-458b-9de1-9b7510dda787', 'fa6b69d2-735f-4aaa-b494-01a6711a6f6b', '86993ed0-0370-4cd3-863d-31cb789c0582', '12a6a2bc-ca00-4e27-b8f4-156b025a138f', '08c24139-5acf-4c61-95cd-eaf7ffb4ef24', '4680cd85-b968-4b6f-b9df-bea998c5cadd', '37def8a1-7999-4399-8a97-3c842eb05b08', '02dbe5f1-92b9-4017-ba34-428293f148bc', 'e7dafa60-64a4-42b8-a6d6-a1e21fa72c2a', '9030a80e-5a7a-4511-818b-199c5b8f0dc9', '70ced307-05c7-430b-be44-6299a8a14ba8', '72cb79d3-0d07-4a62-a3ba-1385862a12cb', '494740c2-ff86-4763-bf11-83e2c20a95ed', '7056fcc9-38be-43c5-9290-c47c67157a36', '8f0f1b07-3faa-488d-8bc4-e31759fde80c', '7144d8e0-1305-43c4-a593-dc2115de72bf', 'c776ab8c-a043-4814-a4bf-cdfe8476d2b7', '548c5119-5364-4fbb-981d-f2b2063cdd62', '0d250f58-743d-45f3-9c2b-6cd4c9f67188', '2dee19d4-45b9-4b00-ac7c-921fb6fced81', '4d32ce23-773e-4fd1-8243-1116a8bdefde', '63701519-4766-449c-8c9a-213c7509a887', 'bacc317b-73f5-4e6e-9080-40a51f69841c', 'bfdefb10-4a7f-44ef-881a-b859ad630153', '5fe777f7-bbc4-4c20-8bbd-eb532d2d8570', 'fe52d649-cd44-4a7b-8c4b-8027c9e773ec', '1e3dd5fa-7a2c-4e69-a7a2-f2d3d9d89d3e', '589576a9-de52-4249-ae6c-d7e84885ccdd', 'e1797a85-3d21-476a-a638-7ecb9e7df172', '8cdfbcad-5dcf-4849-86f8-1d0f4bce0389', 'b4bb15b8-c718-4e9a-95a4-4e478f9a1e0b', 'f724aaa6-0b42-4900-965e-16ef2ed1dbd0', 'cd5d0810-9faa-4a92-a28a-127dfa881814', '08c24139-5acf-4c61-95cd-eaf7ffb4ef24', '8bea0b36-4b88-49e1-9eea-00a4950112bd', 'b68fa474-3f46-40be-afca-4b0f86fea7cd', '34a7bf32-f8e1-4bac-8a8a-49e954db49b8', 'cd49467d-3fea-4e3c-a938-c821cdb694c2', '12a6a2bc-ca00-4e27-b8f4-156b025a138f', '6ae5462c-1d69-4d91-acb4-69dfca075631', '5f397dbf-7314-4b67-82d9-ff6789e948cc', '08637781-1835-4acb-ad5f-f3537d83fd3c', '6b999686-0b00-437a-a707-f8d93275cfc3', '8f0f1b07-3faa-488d-8bc4-e31759fde80c', '598100e0-dfeb-4683-9c53-75f9bf1c05db', 'e9e2dcf1-7ab6-4aa4-b4bf-40e829bf125c', '649c7d79-a32b-41b8-aab1-fd639b49edcf', 'd72ddcdf-13b1-42dd-bd53-1eb170303d1a', 'a67853e7-6df5-4071-b3ee-946147bcd340', '7e5f941d-f19a-46ae-b235-c5eb7bb48117', '9a78e659-9819-432d-b3a9-647b71821535', '052a4ce6-57e5-4390-8432-a356e3fca740', '104afafa-3615-4586-8c41-7632d11e9166', 'd191d77b-5717-483e-8a1e-9b34665ed4d2', '9caacd87-4dda-41d8-88ce-72757b1e643a', '9e276ecc-1fbb-45db-9cfd-cff7bf50cb68', '9012d86e-a542-4e29-8aa9-79a5f19dd775', '3ceeb3f8-d35b-4275-9865-3210ce98bd8f', 'f0c7ef33-3aa6-4869-af86-33c99fb10c5b', '46822ac7-e75d-4646-8327-4f4942e1bd08', 'c9aa4b1b-39ba-42da-b13f-5d655816fea4', '81bbd16e-9886-4914-9afa-617fecb0dc36', 'd086d7ba-9904-4c14-b07a-5f817e0d3461', '52d40fc6-2714-4f80-9da6-caf03edcfe93', '1975d161-f39b-4df4-8a88-6484ae3410ea', 'fe5b7cb9-e971-4db4-9f2a-f145ee0b2fd8', 'b11971e5-dfc2-4270-b889-42c22419f92a', '29f59438-06bc-461a-9ded-6913f33c66cf', 'bf99b18e-8beb-499e-93fc-2bcdc26e119a', '5c1b5b3f-7a08-4b71-b6b4-4401906ed4bf', 'ed43d510-62f8-4d4e-978a-fb150ca6fff7', 'edef26a7-d71e-47e6-880b-7717922130d2', '6ae5462c-1d69-4d91-acb4-69dfca075631', '9ebe44d4-1be7-4568-a4e3-cc28efd41ae5', '494740c2-ff86-4763-bf11-83e2c20a95ed', '7dadff1c-2042-42a2-a855-c74165ee0925', '494740c2-ff86-4763-bf11-83e2c20a95ed', 'c776ab8c-a043-4814-a4bf-cdfe8476d2b7', '08c24139-5acf-4c61-95cd-eaf7ffb4ef24', '6a22d5ba-7ff7-4ef6-afea-55f673e262f2', '7a271585-5315-4f1f-add5-1fe76b68747d', 'f6f5262a-3427-4370-b005-7a5ea4471a64', '7c0748cb-6c7c-4d92-a763-4734e2fc8589', '0d133b2b-88c7-4758-9566-132a29963b17', '70ced307-05c7-430b-be44-6299a8a14ba8', '5c1b5b3f-7a08-4b71-b6b4-4401906ed4bf', 'b68fa474-3f46-40be-afca-4b0f86fea7cd', '35c59f6f-f819-424a-b7f8-961eabb1a63a', 'db852447-6335-4374-ac09-b7ebeddf82fe', '8cdfbcad-5dcf-4849-86f8-1d0f4bce0389', '264b3ab2-6fed-489c-b6fd-420692e40e59', '29f59438-06bc-461a-9ded-6913f33c66cf', 'f3b35926-ae87-432c-ab71-cafa3ddee76e', 'f8c8eb97-1177-4c26-ae83-63a4168d348f', 'c1e5c931-04fc-46b8-87ed-619b090c50af', 'c4b24cd9-2ec3-409f-b872-a5e89cf942b9', '5c1b5b3f-7a08-4b71-b6b4-4401906ed4bf', '7890f95f-5d97-40a9-8810-45986ef2cc07', '494740c2-ff86-4763-bf11-83e2c20a95ed', '509d4a74-826e-4e56-bd7d-14039b600380', 'b0e1b121-d523-4158-b440-a05cd5b4147c', '18eba235-047e-4332-888d-bf114b9cdcbb', '35c59f6f-f819-424a-b7f8-961eabb1a63a', 'b6a644c0-7697-4053-816b-a6806d727b43', 'fde90f7c-bfc9-4348-8087-0c1f8782221a', '13989501-2f89-4a94-9c6b-c9abddede4a1', 'b9edb7a5-786f-4736-a449-08672ac0c223', 'b68fa474-3f46-40be-afca-4b0f86fea7cd', '0d133b2b-88c7-4758-9566-132a29963b17', '13e6bab2-39a9-4061-87fe-2c48e5badacd', 'bad36509-4237-44ca-8971-277d2ef1c075', '35c59f6f-f819-424a-b7f8-961eabb1a63a', '649c7d79-a32b-41b8-aab1-fd639b49edcf', 'e4ac5193-3156-4149-8bd6-d58feafba70d', 'c776ab8c-a043-4814-a4bf-cdfe8476d2b7', '7dadff1c-2042-42a2-a855-c74165ee0925', 'bfeedaf5-5dda-45c9-8550-30e5c07106fe'"
sourcePoints = "'133', '2440'"

dbTargetSchema = "unit_testing"
dbTargetStreamTable = "streams"
dbTargetPointTable = "points"

dbIdField = "id"
dbWatershedIdField = "watershed_id"
dbTargetGeom = "geometry"

#maximum distance for snapping points to stream network in meters
snapDistance = 20

def getPoints(conn):

    query = f"""
    SELECT * FROM {dbTargetSchema}.{dbTargetPointTable};
    """

    # use DictCursor so we can access by column name instead of index
    # helpful if we have data structure changes
    with conn.cursor(cursor_factory=DictCursor) as cursor:
        cursor.execute(query)
        points = [dict(row) for row in cursor.fetchall()]

    return points

def getStreams(conn):

    query = f"""
    SELECT * FROM {dbTargetSchema}.{dbTargetStreamTable};
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
        streams = cursor.fetchall()

    return streams

def loadData(conn):

    print("Loading data for unit tests")

    # clear any data in the schema and target tables
    query = f"""
    CREATE SCHEMA IF NOT EXISTS {dbTargetSchema};
    DROP TABLE IF EXISTS {dbTargetSchema}.{dbTargetStreamTable};
    DROP TABLE IF EXISTS {dbTargetSchema}.{dbTargetPointTable};

    CREATE TABLE {dbTargetSchema}.{dbTargetStreamTable} AS SELECT * FROM {dbSourceStreamTable} WHERE source_id IN ({sourceStreams});
    CREATE TABLE {dbTargetSchema}.{dbTargetPointTable} AS SELECT * FROM {dbSourcePointTable} WHERE fid IN ({sourcePoints});

    ALTER TABLE {dbTargetSchema}.{dbTargetStreamTable} ADD PRIMARY KEY (id);
    ALTER TABLE {dbTargetSchema}.{dbTargetPointTable} ADD PRIMARY KEY (id);
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

def breakStreams(conn):

    print("breaking streams")

    # this will save changes in the geometry field
    query = f"""
        CREATE TEMPORARY TABLE newstreamlines AS
        
        with breakpoints as (
            SELECT a.{dbIdField} as id, 
                a.geometry,
                st_collect(st_lineinterpolatepoint(a.geometry, st_linelocatepoint(a.geometry, b.snapped_point))) as rawpnt
            FROM 
                {dbTargetSchema}.{dbTargetStreamTable} a,  
                {dbTargetSchema}.{dbTargetPointTable} b 
            WHERE st_distance(st_force2d(a.geometry_smoothed3d), b.snapped_point) < 0.01
            GROUP BY a.{dbIdField}, a.geometry
        ),
        newlines as (
            SELECT {dbIdField},
                st_split(st_snap(geometry, rawpnt, 0.01), rawpnt) as geometry
            FROM breakpoints 
        )
        
        SELECT z.{dbIdField},
                y.source_id,
                y.{dbWatershedIdField},
                y.stream_name,
                y.strahler_order,
                y.mainstem_id,
                st_geometryn(z.geometry, generate_series(1, st_numgeometries(z.geometry))) as geometry
        FROM newlines z JOIN {dbTargetSchema}.{dbTargetStreamTable} y 
             ON y.{dbIdField} = z.{dbIdField};
        
        DELETE FROM {dbTargetSchema}.{dbTargetStreamTable} 
        WHERE {dbIdField} IN (SELECT {dbIdField} FROM newstreamlines);
        
              
        INSERT INTO {dbTargetSchema}.{dbTargetStreamTable} 
            (id, source_id, {dbWatershedIdField}, stream_name, strahler_order, 
            segment_length,
            mainstem_id, geometry)
        SELECT gen_random_uuid(), a.source_id, a.{dbWatershedIdField}, 
            a.stream_name, a.strahler_order,
            st_length2d(a.geometry) / 1000.0, 
            mainstem_id, a.geometry
        FROM newstreamlines a;

        DROP INDEX IF EXISTS {dbTargetSchema}."geom_idx";
        CREATE INDEX geom_idx ON {dbTargetSchema}.{dbTargetStreamTable} USING gist(geometry);
        
        DROP TABLE newstreamlines;
    """

    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

def getUpstreamDownstream(conn):

    query = f"""
        ALTER TABLE {dbTargetSchema}.{dbTargetPointTable} DROP COLUMN IF EXISTS stream_id;
        ALTER TABLE {dbTargetSchema}.{dbTargetPointTable} ADD COLUMN IF NOT EXISTS stream_id_up uuid;
        
        UPDATE {dbTargetSchema}.{dbTargetPointTable} SET stream_id_up = null;
        
        WITH ids AS (
            SELECT a.id as stream_id, b.id as barrier_id
            FROM {dbTargetSchema}.{dbTargetStreamTable} a,
                {dbTargetSchema}.{dbTargetPointTable} b
            WHERE a.geometry && st_buffer(b.snapped_point, 0.01) and
                st_intersects(st_endpoint(a.geometry), st_buffer(b.snapped_point, 0.01))
        )
        UPDATE {dbTargetSchema}.{dbTargetPointTable}
            SET stream_id_up = a.stream_id
            FROM ids a
            WHERE a.barrier_id = {dbTargetSchema}.{dbTargetPointTable}.id;
            
        ALTER TABLE {dbTargetSchema}.{dbTargetPointTable} ADD COLUMN IF NOT EXISTS stream_id_down uuid;

        UPDATE {dbTargetSchema}.{dbTargetPointTable} SET stream_id_down = null;
        
        WITH ids AS (
            SELECT a.id as stream_id, b.id as barrier_id
            FROM {dbTargetSchema}.{dbTargetStreamTable} a,
                {dbTargetSchema}.{dbTargetPointTable} b
            WHERE a.geometry && st_buffer(b.snapped_point, 0.01) and
                st_intersects(st_startpoint(a.geometry), st_buffer(b.snapped_point, 0.01))
        )
        UPDATE {dbTargetSchema}.{dbTargetPointTable}
            SET stream_id_down = a.stream_id
            FROM ids a
            WHERE a.barrier_id = {dbTargetSchema}.{dbTargetPointTable}.id;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

    query = f"""
        DROP FUNCTION IF EXISTS public.downstream;
        CREATE OR REPLACE FUNCTION public.downstream(id uuid, limit_id uuid default null)
        RETURNS TABLE (stream_id uuid)
        LANGUAGE sql
        AS '
        WITH RECURSIVE walk_network(id, geometry) AS (
            SELECT id, geometry FROM {dbTargetSchema}.{dbTargetStreamTable} WHERE id = $1
        UNION ALL
            SELECT n.id, n.geometry
            FROM {dbTargetSchema}.{dbTargetStreamTable} n, walk_network w
            WHERE ST_DWithin(ST_EndPoint(w.geometry),ST_StartPoint(n.geometry),0.01)
			and n.id != $2
        )
        SELECT id FROM walk_network;
        '
        IMMUTABLE;

        DROP FUNCTION IF EXISTS public.upstream;
        CREATE OR REPLACE FUNCTION public.upstream(id uuid, limit_id uuid default null)
        RETURNS TABLE (stream_id uuid)
        LANGUAGE sql
        AS '
        WITH RECURSIVE walk_network(id, geometry) AS (
            SELECT id, geometry FROM {dbTargetSchema}.{dbTargetStreamTable} WHERE id = $1
        UNION ALL
            SELECT n.id, n.geometry
            FROM {dbTargetSchema}.{dbTargetStreamTable} n, walk_network w
            WHERE ST_DWithin(ST_StartPoint(w.geometry),ST_EndPoint(n.geometry),0.01)
			and n.id != $2
        )
        SELECT id FROM walk_network;
        '
        IMMUTABLE;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

def processStreams(points, conn):

    for point in points:
        stream_id_up = point['stream_id_up']
        stream_id_down = point['stream_id_down']
        comments = point['comments']

        if "POINT A" in comments:

            match_point = point['match_id']

            query = f"""
            SELECT stream_id_up, stream_id_down FROM {dbTargetSchema}.{dbTargetPointTable} WHERE id = '{match_point}';
            """

            # use DictCursor so we can access by column name instead of index
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query)
                result = [dict(row) for row in cursor.fetchall()]

            for r in result:

                match_stream_id_up = r['stream_id_up']
                match_stream_id_down = r['stream_id_down']
                
                query = f"""
                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET sm_accessibility = '{appconfig.Accessibility.NOT.value}'
                    WHERE {dbIdField} IN (SELECT public.upstream('{stream_id_up}', '{match_stream_id_up}'));

                    --UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET sm_accessibility = '{appconfig.Accessibility.ACCESSIBLE.value}'
                    --WHERE {dbIdField} IN (SELECT public.downstream('{stream_id_down}', '{match_stream_id_down}'));
                """

                with conn.cursor() as cursor:
                    cursor.execute(query)
                conn.commit()

        elif "POINT B" in comments:
            
            match_point = point['match_id']

            query = f"""
            SELECT stream_id_up, stream_id_down FROM {dbTargetSchema}.{dbTargetPointTable} WHERE id = '{match_point}';
            """

            # use DictCursor so we can access by column name instead of index
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query)
                result = [dict(row) for row in cursor.fetchall()]

            for r in result:

                match_stream_id_up = r['stream_id_up']
                match_stream_id_down = r['stream_id_down']
                
                query = f"""
                    --UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET sm_accessibility = '{appconfig.Accessibility.NOT.value}'
                    --WHERE {dbIdField} IN (SELECT public.upstream('{stream_id_up}', '{match_stream_id_up}'));

                    UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET sm_accessibility = '{appconfig.Accessibility.ACCESSIBLE.value}'
                    WHERE {dbIdField} IN (SELECT public.downstream('{stream_id_down}', '{match_stream_id_down}'));
                """

                with conn.cursor() as cursor:
                    cursor.execute(query)
                conn.commit()

        else:

            query = f"""
                UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET sm_accessibility = '{appconfig.Accessibility.NOT.value}'
                WHERE {dbIdField} IN (SELECT public.upstream('{stream_id_up}'));

                UPDATE {dbTargetSchema}.{dbTargetStreamTable} SET sm_accessibility = '{appconfig.Accessibility.ACCESSIBLE.value}'
                WHERE {dbIdField} IN (SELECT public.downstream('{stream_id_down}'));
            """

            with conn.cursor() as cursor:
                cursor.execute(query)
            conn.commit()

def main():

    conn = pg2.connect(database=dbName,
                user=dbUser,
                host=dbHost,
                password=dbPassword,
                port=dbPort)

    loadData(conn)
    breakStreams(conn)
    getUpstreamDownstream(conn)
    points = getPoints(conn)
    processStreams(points, conn)
    
    print("Done!")

if __name__ == "__main__":
    main()
