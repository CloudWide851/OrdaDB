use std::sync::{Arc, Mutex};

use ordadb_types::{DbError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FaultPoint {
    BeforeWalFlush,
    AfterWalFlush,
    BeforeCommitFlush,
    AfterCommitFlush,
    AfterCheckpointBeginFlush,
    BeforeCheckpointEndAppend,
    AfterCheckpointEndFlush,
    BeforeDataPageWrite,
    AfterDataPageWrite,
    BeforeDataResize,
    AfterDataResize,
    BeforeDataSync,
    AfterDataSync,
    AfterCompensationFlush,
    BeforeCompensationApply,
}

pub trait FaultInjector: Send + Sync + 'static {
    fn check(&self, point: FaultPoint) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct NoFaultInjector;

impl FaultInjector for NoFaultInjector {
    fn check(&self, _point: FaultPoint) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct DeterministicFaultInjector {
    armed: Mutex<Option<ArmedFault>>,
}

#[derive(Debug, Clone, Copy)]
struct ArmedFault {
    point: FaultPoint,
    remaining_occurrences: usize,
}

impl DeterministicFaultInjector {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn arm(&self, point: FaultPoint, occurrence: usize) -> Result<()> {
        if occurrence == 0 {
            return Err(DbError::new(
                "22023",
                "fault occurrence must be greater than zero",
            ));
        }
        let mut armed = self.lock()?;
        *armed = Some(ArmedFault {
            point,
            remaining_occurrences: occurrence,
        });
        Ok(())
    }

    pub fn clear(&self) -> Result<()> {
        *self.lock()? = None;
        Ok(())
    }

    pub fn is_armed(&self) -> Result<bool> {
        Ok(self.lock()?.is_some())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Option<ArmedFault>>> {
        self.armed.lock().map_err(|_| {
            DbError::internal("deterministic fault injector lock is poisoned")
                .with_hint("create a fresh injector before retrying the test")
        })
    }
}

impl FaultInjector for DeterministicFaultInjector {
    fn check(&self, point: FaultPoint) -> Result<()> {
        let mut armed = self.lock()?;
        let Some(fault) = armed.as_mut() else {
            return Ok(());
        };
        if fault.point != point {
            return Ok(());
        }
        fault.remaining_occurrences -= 1;
        if fault.remaining_occurrences != 0 {
            return Ok(());
        }
        *armed = None;
        Err(DbError::new(
            "58030",
            format!("injected deterministic failure at {point:?}"),
        )
        .with_hint("retry from the persisted state to verify failure convergence"))
    }
}

#[cfg(test)]
mod tests {
    use super::{DeterministicFaultInjector, FaultInjector, FaultPoint};

    #[test]
    fn injector_fails_only_the_selected_occurrence_and_then_disarms() {
        let injector = DeterministicFaultInjector::new();
        injector
            .arm(FaultPoint::BeforeDataPageWrite, 2)
            .expect("arm injector");
        injector
            .check(FaultPoint::BeforeDataSync)
            .expect("other point passes");
        injector
            .check(FaultPoint::BeforeDataPageWrite)
            .expect("first page passes");
        let error = injector
            .check(FaultPoint::BeforeDataPageWrite)
            .expect_err("second page fails");
        assert_eq!(error.sql_state, "58030");
        assert!(!injector.is_armed().expect("injector state"));
        injector
            .check(FaultPoint::BeforeDataPageWrite)
            .expect("disarmed injector passes");
    }
}
