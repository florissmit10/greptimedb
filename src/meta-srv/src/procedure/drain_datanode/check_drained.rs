// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;

use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::info;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::procedure::drain_datanode::dispatch::Dispatch;
use crate::procedure::drain_datanode::drain_datanode_end::DrainDatanodeEnd;
use crate::procedure::drain_datanode::{Context, State, utils};

/// Rescans the datanode and decides whether the drain is finished.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CheckDrained;

#[async_trait::async_trait]
#[typetag::serde]
impl State for CheckDrained {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        utils::ensure_drain_deadline(ctx)?;

        let node_id = ctx.persistent_ctx.node_id;
        // The table routes are authoritative: rescan them instead of trusting the progress
        // of this run, so that a region taken over by a failover is not reported as drained
        // while the datanode still leads it.
        ctx.rebuild_plan().await?;
        // Regions given up by `DrainDatanodeStrategy::TryAll` are still led by the datanode;
        // they must not trigger another rescan round.
        for region_id in ctx.volatile_ctx.failed_regions.clone() {
            ctx.forget_leader_region(region_id);
        }

        let migrated = ctx.volatile_ctx.migrated;
        let rescan_round = ctx.volatile_ctx.rescan_round;
        let remaining_leader_regions = ctx.volatile_ctx.leader_regions().len();
        if remaining_leader_regions == 0 {
            let follower_regions = ctx.volatile_ctx.follower_regions().len();
            info!(
                "Drained datanode {}, migrated regions: {}, follower regions: {}",
                node_id, migrated, follower_regions
            );
            return Ok((Box::new(DrainDatanodeEnd), Status::executing(false)));
        }

        info!(
            "Datanode {} still leads {} regions, rescan round: {}",
            node_id, remaining_leader_regions, rescan_round
        );

        Ok((Box::new(Dispatch), Status::executing(false)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
