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
# This script runs all the steps to process and calculate connectivity for a watershed.
#

from datetime import datetime
import appconfig
from processing_scripts import load_parameters
from processing_scripts import preprocess_watershed
from processing_scripts import load_and_snap_barriers_cabd
from processing_scripts import load_and_snap_fishobservation
from processing_scripts import load_habitat_access_updates
from processing_scripts import compute_modelled_crossings
from processing_scripts import load_barrier_updates
from processing_scripts import compute_mainstems
from processing_scripts import assign_raw_z
from processing_scripts import smooth_z
from processing_scripts import compute_vertex_gradient
from processing_scripts import compute_segment_gradient
from processing_scripts import break_streams_at_barriers
from processing_scripts import compute_updown_barriers_fish
from processing_scripts import compute_accessibility
from processing_scripts import assign_habitat
from processing_scripts import process_habitat_access_updates
from processing_scripts import compute_barriers_upstream_values
from processing_scripts import compute_barrier_dci

startTime = datetime.now()

iniSection = appconfig.args.args[0]

workingWatershedId = appconfig.config[iniSection]['watershed_id']

print ("Processing: " + workingWatershedId)

load_parameters.main()
preprocess_watershed.main()
load_and_snap_barriers_cabd.main()
load_and_snap_fishobservation.main()
compute_modelled_crossings.main()
load_barrier_updates.main()
compute_mainstems.main()
assign_raw_z.main()
smooth_z.main()
compute_vertex_gradient.main()
load_habitat_access_updates.main()
break_streams_at_barriers.main()
print ("Recalculating elevations on broken streams: " + workingWatershedId)
#re-assign elevations to broken streams
assign_raw_z.main()
smooth_z.main()
compute_segment_gradient.main()
compute_updown_barriers_fish.main()
compute_accessibility.main()
assign_habitat.main()
process_habitat_access_updates.main()
compute_barriers_upstream_values.main()
compute_barrier_dci.main()

print ("Processing Complete: " + workingWatershedId)
print("Runtime: " + str((datetime.now() - startTime)))
