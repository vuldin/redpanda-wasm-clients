//! A minimal pre-trade notional exposure check - the same logic as
//! ../../risk/risk.go, reimplemented in Rust. See orderbook.rs's own
//! doc comment for why this exists.

use std::collections::HashMap;

#[derive(Debug)]
pub struct LimitExceeded {
    pub participant: String,
    pub current_exposure: i64,
    pub additional_notional: i64,
    pub limit: i64,
}

#[derive(Default)]
pub struct Checker {
    limit: i64,
    exposure: HashMap<String, i64>,
}

impl Checker {
    /// A limit of 0 disables the check entirely (every order passes).
    pub fn new(limit: i64) -> Self {
        Self {
            limit,
            exposure: HashMap::new(),
        }
    }

    pub fn check(&self, participant: &str, notional: i64) -> Result<(), LimitExceeded> {
        if self.limit <= 0 {
            return Ok(());
        }
        let current = *self.exposure.get(participant).unwrap_or(&0);
        if current + notional > self.limit {
            return Err(LimitExceeded {
                participant: participant.to_string(),
                current_exposure: current,
                additional_notional: notional,
                limit: self.limit,
            });
        }
        Ok(())
    }

    pub fn record(&mut self, participant: &str, delta: i64) {
        let entry = self.exposure.entry(participant.to_string()).or_insert(0);
        *entry += delta;
        if *entry <= 0 {
            self.exposure.remove(participant);
        }
    }

    pub fn exposure(&self, participant: &str) -> i64 {
        *self.exposure.get(participant).unwrap_or(&0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passes_under_limit() {
        let c = Checker::new(1000);
        assert!(c.check("alice", 500).is_ok());
    }

    #[test]
    fn rejects_over_limit() {
        let c = Checker::new(1000);
        assert!(c.check("alice", 1001).is_err());
    }

    #[test]
    fn zero_limit_disables() {
        let c = Checker::new(0);
        assert!(c.check("alice", i64::MAX).is_ok());
    }

    #[test]
    fn record_accumulates_and_clears() {
        let mut c = Checker::new(1000);
        c.record("alice", 500);
        assert_eq!(c.exposure("alice"), 500);
        c.record("alice", -500);
        assert_eq!(c.exposure("alice"), 0);
    }

    #[test]
    fn exposure_is_per_participant() {
        let mut c = Checker::new(1000);
        c.record("alice", 500);
        assert_eq!(c.exposure("bob"), 0);
    }
}
