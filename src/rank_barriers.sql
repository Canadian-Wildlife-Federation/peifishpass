----------------------------------------------------------------------------------------------
-- @Date: January 8, 2024
-- @Author: Katherine O'Hearn

-- UPDATES
-- @Date: April 12, 2024
-- @Author: Andrew Pozzuoli
-- Updated for automatic group id based on greatest avg gain per barrier

-- this set of queries will generate a set of all barriers and their
-- downstream barriers, along with a group_id

----------------------------------------------------------------------------------------------

--replace 'as' with a different species code if you need results for another species

DROP TABLE IF EXISTS ws01cd000.ranked_barriers_as;

SELECT b.* INTO ws01cd000.ranked_barriers_as
FROM ws01cd000.barriers b
WHERE b.passability_status_as != 1
	AND b.total_upstr_hab_as != 0
ORDER BY dci_as DESC;

ALTER TABLE IF EXISTS ws01cd000.ranked_barriers_as
    ALTER COLUMN id SET NOT NULL;
ALTER TABLE IF EXISTS ws01cd000.ranked_barriers_as
    ADD COLUMN group_id numeric;
ALTER TABLE IF EXISTS ws01cd000.ranked_barriers_as
    ADD PRIMARY KEY (id);

--TO FIX: some group_ids need to get combined - e.g., multiple branches of river
ALTER TABLE ws01cd000.ranked_barriers_as ADD COLUMN IF NOT EXISTS mainstem_id uuid;
UPDATE ws01cd000.ranked_barriers_as SET mainstem_id = t.mainstem_id FROM ws01cd000.streams t WHERE t.id = stream_id_up;

CREATE INDEX ranked_barriers_as_idx_mainstem ON ws01cd000.ranked_barriers_as (mainstem_id);
CREATE INDEX ranked_barriers_as_idx_group_id ON ws01cd000.ranked_barriers_as (group_id);
CREATE INDEX ranked_barriers_as_idx_id ON ws01cd000.ranked_barriers_as (id);

WITH mainstems AS (
SELECT DISTINCT mainstem_id, row_number() OVER () AS group_id
FROM ws01cd000.ranked_barriers_as
)

UPDATE ws01cd000.ranked_barriers_as a SET group_id = m.group_id FROM mainstems m WHERE m.mainstem_id = a.mainstem_id;
	

DO $$
DECLARE
	continue_loop BOOLEAN := TRUE;
	i INT := 1;
	grp_offset INT := (SELECT COUNT(*)*10 FROM ws01cd000.ranked_barriers_as);
BEGIN
	WHILE continue_loop LOOP
		
		i := i + 1;
	
		perform id, group_id
		from ws01cd000.ranked_barriers_as
		where group_id < grp_offset
		LIMIT 1;

		IF NOT FOUND THEN
			continue_loop := FALSE;
		ELSE
			with avgVals as (
				select id, mainstem_id, group_id, barrier_cnt_upstr_as, func_upstr_hab_as
					,AVG(func_upstr_hab_as) OVER(PARTITION BY group_id ORDER BY barrier_cnt_upstr_as DESC) as average
					,ROW_NUMBER() OVER(PARTITION BY group_id ORDER BY barrier_cnt_upstr_as DESC) as row_num
				from ws01cd000.ranked_barriers_as
				where group_id < grp_offset
				order by group_id, barrier_cnt_upstr_as DESC
			),
			max_grp_gain as (
				select 
					group_id
					,max(average) as best_gain
				from avgVals
				group by group_id
				order by group_id
			),
			part as (
				select mx.*, av.row_num 
				from max_grp_gain mx
				join avgVals av on mx.best_gain = av.average
			),
			new_grps as (
				select av.id
					,CASE 
						WHEN av.row_num <= part.row_num THEN (av.group_id*grp_offset) + i
						ELSE av.group_id
					END as new_group_id
				from avgVals av
				join part on av.group_id = part.group_id
			)

			update ws01cd000.ranked_barriers_as
			set group_id = new_grps.new_group_id
			from new_grps
			where ws01cd000.ranked_barriers_as.id = new_grps.id
			AND new_grps.new_group_id > grp_offset;

		END IF;
	END LOOP;
END $$;

-- select id, mainstem_id, group_id, barrier_cnt_upstr_as, func_upstr_hab_as
-- 	,AVG(func_upstr_hab_as) OVER(PARTITION BY group_id ORDER BY barrier_cnt_upstr_as DESC) as average
-- 	,ROW_NUMBER() OVER(PARTITION BY group_id ORDER BY barrier_cnt_upstr_as DESC) as row_num
-- from ws01cd000.ranked_barriers_as
-- order by group_id, barrier_cnt_upstr_as DESC;


	
----------------- CALCULATE GROUP GAINS -------------------------	
	
alter table ws01cd000.ranked_barriers_as add column total_hab_gain_group numeric;
alter table ws01cd000.ranked_barriers_as add column num_barriers_group integer;
alter table ws01cd000.ranked_barriers_as add column avg_gain_per_barrier numeric;

with temp as (
	SELECT sum(func_upstr_hab_as) AS sum, group_id
	from ws01cd000.ranked_barriers_as
	group by group_id
)

update ws01cd000.ranked_barriers_as a SET total_hab_gain_group = t.sum FROM temp t WHERE t.group_id = a.group_id;
update ws01cd000.ranked_barriers_as SET total_hab_gain_group = func_upstr_hab_as WHERE group_id IS NULL;

with temp as (
	SELECT count(*) AS cnt, group_id
	from ws01cd000.ranked_barriers_as
	group by group_id
)


update ws01cd000.ranked_barriers_as a SET num_barriers_group = t.cnt FROM temp t WHERE t.group_id = a.group_id;
update ws01cd000.ranked_barriers_as SET num_barriers_group = 1 WHERE group_id IS NULL;

update ws01cd000.ranked_barriers_as SET avg_gain_per_barrier = total_hab_gain_group / num_barriers_group;

---------------GET DOWNSTREAM GROUP IDs----------------------------

ALTER TABLE ws01cd000.ranked_barriers_as ADD downstr_group_ids varchar[];

WITH downstr_barriers AS (
	SELECT rb.id, rb.group_id
		,UNNEST(barriers_downstr_as) AS barriers_downstr_as
	FROM ws01cd000.ranked_barriers_as rb
),
downstr_group AS (
	SELECT db_.id, db_.group_id as current_group, db_.barriers_downstr_as
		,rb.group_id
	FROM downstr_barriers AS db_
	JOIN ws01cd000.ranked_barriers_as rb
		ON rb.id = db_.barriers_downstr_as::uuid
	WHERE db_.group_id != rb.group_id
), 
dg_arrays AS (
	SELECT dg.id, ARRAY_AGG(DISTINCT dg.group_id)::varchar[] as downstr_group_ids
	FROM downstr_group dg
	GROUP BY dg.id
)
UPDATE ws01cd000.ranked_barriers_as
SET downstr_group_ids = dg_arrays.downstr_group_ids
FROM dg_arrays
WHERE ws01cd000.ranked_barriers_as.id = dg_arrays.id;


----------------- ASSIGN RANK ID  -------------------------	

-- Rank based on average gain per barrier in a group
ALTER TABLE ws01cd000.ranked_barriers_as ADD rank_avg_gain_per_barrier numeric;

WITH ranks AS
(
	select group_id, avg_gain_per_barrier
		,DENSE_RANK() OVER(order by avg_gain_per_barrier DESC) as rank_id
	from ws01cd000.ranked_barriers_as
)
update ws01cd000.ranked_barriers_as 
SET rank_avg_gain_per_barrier = ranks.rank_id
FROM ranks
WHERE ws01cd000.ranked_barriers_as.group_id = ranks.group_id;

-- Rank based on first sorting the barriers into tiers by number of downstream barriers then by avg gain per barrier within those tiers (immediate gain)
ALTER TABLE ws01cd000.ranked_barriers_as 
ADD rank_avg_gain_tiered numeric;

WITH sorted AS (
	SELECT id, group_id, barrier_cnt_upstr_as, barrier_cnt_downstr_as, total_hab_gain_group, avg_gain_per_barrier
		,ROW_NUMBER() OVER(ORDER BY barrier_cnt_downstr_as, avg_gain_per_barrier DESC) as row_num
	FROM ws01cd000.ranked_barriers_as
	WHERE avg_gain_per_barrier >= 0.5
	UNION ALL
	SELECT id, group_id, barrier_cnt_upstr_as, barrier_cnt_downstr_as, total_hab_gain_group, avg_gain_per_barrier
		,(SELECT MAX(row_num) FROM (
			SELECT ROW_NUMBER() OVER(ORDER BY barrier_cnt_downstr_as, avg_gain_per_barrier DESC) as row_num
			FROM ws01cd000.ranked_barriers_as
			WHERE avg_gain_per_barrier >= 0.5
		) AS subquery) + ROW_NUMBER() OVER(ORDER BY barrier_cnt_downstr_as, avg_gain_per_barrier DESC) as row_num 
	FROM ws01cd000.ranked_barriers_as
	WHERE avg_gain_per_barrier < 0.5
	
),
ranks AS (
	SELECT id
		,FIRST_VALUE(row_num) OVER(PARTITION BY group_id ORDER BY barrier_cnt_downstr_as) as ranks
		,FIRST_VALUE(barrier_cnt_downstr_as) OVER (PARTITION BY group_id ORDER BY barrier_cnt_downstr_as) as tier
	FROM sorted
	ORDER BY group_id, barrier_cnt_downstr_as, avg_gain_per_barrier DESC
)
UPDATE ws01cd000.ranked_barriers_as 
SET rank_avg_gain_tiered = ranks.ranks
FROM ranks
WHERE ws01cd000.ranked_barriers_as.id = ranks.id;

-- Rank based on total habitat upstream (potential gain)
ALTER TABLE ws01cd000.ranked_barriers_as 
ADD rank_total_upstr_hab numeric;

WITH sorted AS (
	SELECT id, group_id, barrier_cnt_upstr_as, barrier_cnt_downstr_as, total_upstr_hab_as, total_hab_gain_group, avg_gain_per_barrier
		,ROW_NUMBER() OVER(ORDER BY total_upstr_hab_as DESC) as row_num
	FROM ws01cd000.ranked_barriers_as
),
ranks AS (
	SELECT id, group_id, barrier_cnt_upstr_as, barrier_cnt_downstr_as, total_upstr_hab_as, total_hab_gain_group, avg_gain_per_barrier
		,FIRST_VALUE(row_num) OVER(PARTITION BY group_id ORDER BY row_num) as relative_rank
	FROM sorted
	ORDER BY group_id, barrier_cnt_downstr_as, avg_gain_per_barrier DESC
),
densify AS (
	SELECT id, group_id, barrier_cnt_upstr_as, barrier_cnt_downstr_as, total_upstr_hab_as, total_hab_gain_group, avg_gain_per_barrier
		,DENSE_RANK() OVER(ORDER BY relative_rank) as ranks
	FROM ranks
)
UPDATE ws01cd000.ranked_barriers_as 
SET rank_total_upstr_hab = densify.ranks
FROM densify
WHERE ws01cd000.ranked_barriers_as.id = densify.id;

-- Composite Rank of potential and immediate gain with upstream habitat cutoff
ALTER TABLE ws01cd000.ranked_barriers_as 
ADD rank_combined numeric;

WITH ranks AS (
	SELECT id, group_id, barrier_cnt_upstr_as, barrier_cnt_downstr_as, total_upstr_hab_as, total_hab_gain_group, avg_gain_per_barrier
		,rank_avg_gain_tiered
		,rank_total_upstr_hab
		,rank_avg_gain_per_barrier
		,DENSE_RANK() OVER(ORDER BY rank_avg_gain_tiered + rank_total_upstr_hab, group_id ASC) as rank_composite
	FROM ws01cd000.ranked_barriers_as
	ORDER BY rank_composite ASC
)
UPDATE ws01cd000.ranked_barriers_as
SET rank_combined = ranks.rank_composite
FROM ranks
WHERE ws01cd000.ranked_barriers_as.id = ranks.id;

-- Potential and immediate with weight by downstream barriers
ALTER TABLE ws01cd000.ranked_barriers_as 
ADD tier_combined varchar;

WITH ranks AS (
	SELECT id, group_id, barrier_cnt_upstr_as, barrier_cnt_downstr_as, total_upstr_hab_as, total_hab_gain_group, avg_gain_per_barrier
		,rank_avg_gain_tiered
		,rank_total_upstr_hab
		,rank_avg_gain_per_barrier
		,rank_combined
		,DENSE_RANK() OVER(ORDER BY LEAST(rank_avg_gain_tiered, rank_total_upstr_hab), group_id ASC) as rank_composite
	FROM ws01cd000.ranked_barriers_as
)
UPDATE ws01cd000.ranked_barriers_as
SET tier_combined = case
			when r.rank_combined <= 10 then 'A'
			when r.rank_combined <= 20 then 'B'
			when r.rank_combined <= 30 then 'C'
			else 'D'
		end
FROM ranks r
WHERE ws01cd000.ranked_barriers_as.id = r.id;
