//! The engine driver: one thread owns the [`Engine`] and advances sim time
//! in step with the wall clock. Everything else talks to it through
//! [`DriverHandle`]: commands go in over a channel, and what the UI should
//! see comes out on a `watch`.

use control::client::ClusterView;
use control::proto::{ApiOp, ApiResult};
use serde::Serialize;
use sim::engine::Engine;
use sim::faults::Fault;
use sim::presets::Preset;
use sim::scenario::Scenario;
use sim::truth::GroundTruth;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, watch};
use ts_rs::TS;

/// Wall time per driver step.
pub const TICK: Duration = Duration::from_millis(20);
/// The engine's seed: runs are repeatable.
const SEED: u64 = 1;
/// Wall time between publishes: at most 4 per second.
pub const PUBLISH_EVERY: Duration = Duration::from_millis(250);

/// The speeds the UI offers; 0 pauses.
pub const SPEEDS: [u32; 5] = [0, 1, 5, 20, 60];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
#[ts(export)]
pub struct SimInfo {
    #[ts(type = "number")]
    pub now: u64,
    pub speed: u32,
    pub paused: bool,
}

/// Everything the UI is shown, as of one moment.
#[derive(Debug, Clone)]
pub struct Published {
    pub view: ClusterView,
    pub truth: GroundTruth,
    pub sim: SimInfo,
}

/// What became of an API operation.
#[derive(Debug, Clone, PartialEq)]
pub enum OpOutcome {
    /// The simulation is paused; the operation was not submitted.
    Paused,
    Done(ApiResult),
}

pub enum EngineCmd {
    Op { op: ApiOp, reply: oneshot::Sender<OpOutcome> },
    Fault { fault: Fault, reply: oneshot::Sender<Result<(), String>> },
    Scenario { scenario: Scenario, reply: oneshot::Sender<Result<(), ScenarioError>> },
    Preset { preset: Preset, reply: oneshot::Sender<()> },
    Speed { speed: u32, reply: oneshot::Sender<()> },
}

/// Why a scenario did not run.
#[derive(Debug, Clone, PartialEq)]
pub enum ScenarioError {
    Paused,
    Failed(String),
}

/// The driver is gone: the process is shutting down.
#[derive(Debug)]
pub struct DriverGone;

#[derive(Clone)]
pub struct DriverHandle {
    cmds: mpsc::Sender<EngineCmd>,
    published: watch::Receiver<Arc<Published>>,
}

impl DriverHandle {
    /// Starts the driver thread with `preset` loaded, running at `speed`.
    /// The thread ends when the last handle is dropped.
    pub fn spawn(preset: Preset, speed: u32) -> Self {
        let (cmds, rx) = mpsc::channel();
        let mut driver = Driver::new(preset, speed);
        let (tx, published) = watch::channel(Arc::new(driver.snapshot()));
        std::thread::Builder::new()
            .name("engine-driver".into())
            .spawn(move || driver.run(rx, tx))
            .expect("spawn the engine driver thread");
        Self { cmds, published }
    }

    pub fn subscribe(&self) -> watch::Receiver<Arc<Published>> {
        self.published.clone()
    }

    async fn ask<T>(&self, cmd: impl FnOnce(oneshot::Sender<T>) -> EngineCmd) -> Result<T, DriverGone> {
        let (reply, answer) = oneshot::channel();
        self.cmds.send(cmd(reply)).map_err(|_| DriverGone)?;
        answer.await.map_err(|_| DriverGone)
    }

    pub async fn op(&self, op: ApiOp) -> Result<OpOutcome, DriverGone> {
        self.ask(|reply| EngineCmd::Op { op, reply }).await
    }

    pub async fn fault(&self, fault: Fault) -> Result<Result<(), String>, DriverGone> {
        self.ask(|reply| EngineCmd::Fault { fault, reply }).await
    }

    pub async fn scenario(&self, scenario: Scenario) -> Result<Result<(), ScenarioError>, DriverGone> {
        self.ask(|reply| EngineCmd::Scenario { scenario, reply }).await
    }

    pub async fn preset(&self, preset: Preset) -> Result<(), DriverGone> {
        self.ask(|reply| EngineCmd::Preset { preset, reply }).await
    }

    pub async fn speed(&self, speed: u32) -> Result<(), DriverGone> {
        self.ask(|reply| EngineCmd::Speed { speed, reply }).await
    }
}

struct Driver {
    engine: Engine,
    speed: u32,
    /// Requests submitted to the engine and not yet answered.
    pending: HashMap<u64, oneshot::Sender<OpOutcome>>,
}

impl Driver {
    fn new(preset: Preset, speed: u32) -> Self {
        let mut engine = Engine::new(SEED);
        engine.load_preset(preset);
        Self { engine, speed, pending: HashMap::new() }
    }

    fn snapshot(&self) -> Published {
        Published {
            view: self.engine.client_view().clone(),
            truth: self.engine.truth(),
            sim: SimInfo { now: self.engine.now(), speed: self.speed, paused: self.speed == 0 },
        }
    }

    fn run(&mut self, cmds: Receiver<EngineCmd>, published: watch::Sender<Arc<Published>>) {
        let mut last_publish = Instant::now();
        loop {
            let started = Instant::now();
            loop {
                match cmds.try_recv() {
                    Ok(cmd) => self.handle(cmd),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return,
                }
            }
            if self.speed > 0 {
                self.engine.run_for(TICK.as_millis() as u64 * u64::from(self.speed));
            }
            self.resolve_pending();
            if last_publish.elapsed() >= PUBLISH_EVERY {
                published.send_replace(Arc::new(self.snapshot()));
                last_publish = Instant::now();
            }
            // A step that overran its tick is not made up for: the next one starts at once
            // and covers the same amount of sim time.
            if let Some(rest) = TICK.checked_sub(started.elapsed()) {
                std::thread::sleep(rest);
            }
        }
    }

    fn handle(&mut self, cmd: EngineCmd) {
        match cmd {
            EngineCmd::Op { op, reply } => {
                if self.speed == 0 {
                    let _ = reply.send(OpOutcome::Paused);
                } else {
                    let req = self.engine.api(op);
                    self.pending.insert(req, reply);
                }
            }
            EngineCmd::Fault { fault, reply } => {
                let _ = reply.send(self.engine.fault(fault));
            }
            EngineCmd::Scenario { scenario, reply } => {
                // Scenarios run the simulation while they work, so they need it unpaused.
                let result = if self.speed == 0 {
                    Err(ScenarioError::Paused)
                } else {
                    self.engine.scenario(scenario).map_err(ScenarioError::Failed)
                };
                let _ = reply.send(result);
            }
            EngineCmd::Preset { preset, reply } => {
                self.engine.load_preset(preset);
                // The new client never heard of these requests.
                for (_, reply) in self.pending.drain() {
                    let _ = reply.send(OpOutcome::Done(ApiResult::Unavailable));
                }
                let _ = reply.send(());
            }
            EngineCmd::Speed { speed, reply } => {
                self.speed = speed;
                let _ = reply.send(());
            }
        }
    }

    fn resolve_pending(&mut self) {
        for (req, result) in self.engine.take_api_results() {
            if let Some(reply) = self.pending.remove(&req) {
                let _ = reply.send(OpOutcome::Done(result));
            }
        }
    }
}
