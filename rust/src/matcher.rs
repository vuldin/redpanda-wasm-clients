//! Wires orderbook and risk together into the single processing step -
//! the same shape as ../../matcher/matcher.go. See that file's doc
//! comment for why this exists as its own module.

use crate::orderbook::{self, Book, Side};
use crate::risk::Checker;
use crate::wire::{self, MessageType};

pub struct Matcher {
    book: Book,
    risk: Checker,
    seq: u64,
}

#[derive(Debug)]
pub enum Error {
    Wire(wire::Error),
    Book(orderbook::Error),
    RiskLimitExceeded(crate::risk::LimitExceeded),
}

impl Matcher {
    pub fn new(risk_limit: i64) -> Self {
        Self {
            book: Book::new(),
            risk: Checker::new(risk_limit),
            seq: 0,
        }
    }

    /// seq, if non-zero, overrides this Matcher's own internal sequence
    /// counter for time-priority purposes - pass the input record's own
    /// offset here in any real deployment, same as matcher.go's Process.
    pub fn process(&mut self, input: &[u8], seq: u64) -> Result<Vec<u8>, Error> {
        match wire::decode_message_type(input).map_err(Error::Wire)? {
            MessageType::NewOrder => self.process_new_order(input, seq),
            MessageType::Cancel => self.process_cancel(input),
        }
    }

    fn process_new_order(&mut self, input: &[u8], mut seq: u64) -> Result<Vec<u8>, Error> {
        let o = wire::decode_new_order(input).map_err(Error::Wire)?;
        let notional = o.price * o.qty;
        self.risk
            .check(&o.participant, notional)
            .map_err(Error::RiskLimitExceeded)?;
        if seq == 0 {
            self.seq += 1;
            seq = self.seq;
        }
        let side = if o.side == 0 { Side::Buy } else { Side::Sell };
        let fills = self
            .book
            .submit(orderbook::Order {
                id: o.order_id,
                participant: o.participant.clone(),
                side,
                price: o.price,
                qty: o.qty,
                sequence: seq,
            })
            .map_err(Error::Book)?;
        self.risk.record(&o.participant, notional);
        for f in &fills {
            let filled_notional = f.price * f.qty;
            self.risk.record(&f.aggressor_party, -filled_notional);
            self.risk.record(&f.resting_party, -filled_notional);
        }
        Ok(wire::encode_fills(&to_wire_fills(&fills)))
    }

    fn process_cancel(&mut self, input: &[u8]) -> Result<Vec<u8>, Error> {
        let c = wire::decode_cancel(input).map_err(Error::Wire)?;
        let removed = self.book.cancel(&c.order_id).map_err(Error::Book)?;
        // Release the canceled order's remaining notional - see
        // matcher.go's own comment on why this matters.
        self.risk
            .record(&removed.participant, -(removed.price * removed.qty));
        Ok(wire::encode_fills(&[]))
    }
}

fn to_wire_fills(fills: &[orderbook::Fill]) -> Vec<wire::Fill> {
    fills
        .iter()
        .map(|f| wire::Fill {
            aggressor_id: f.aggressor_id.clone(),
            resting_id: f.resting_id.clone(),
            aggressor_party: f.aggressor_party.clone(),
            resting_party: f.resting_party.clone(),
            price: f.price,
            qty: f.qty,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{decode_fills, encode_new_order, NewOrder};

    fn new_order(id: &str, party: &str, side: u8, price: i64, qty: i64) -> Vec<u8> {
        encode_new_order(&NewOrder {
            order_id: id.into(),
            participant: party.into(),
            side,
            price,
            qty,
        })
    }

    #[test]
    fn rests_with_no_fill() {
        let mut m = Matcher::new(0);
        let out = m.process(&new_order("1", "alice", 0, 100, 10), 1).unwrap();
        assert!(decode_fills(&out).unwrap().is_empty());
    }

    #[test]
    fn matching_produces_fill() {
        let mut m = Matcher::new(0);
        m.process(&new_order("1", "alice", 0, 100, 10), 1).unwrap();
        let out = m.process(&new_order("2", "bob", 1, 100, 10), 2).unwrap();
        assert_eq!(decode_fills(&out).unwrap().len(), 1);
    }

    #[test]
    fn risk_rejects_over_limit() {
        let mut m = Matcher::new(500);
        let err = m
            .process(&new_order("1", "alice", 0, 100, 10), 1)
            .unwrap_err();
        assert!(matches!(err, Error::RiskLimitExceeded(_)));
    }

    #[test]
    fn risk_released_on_full_fill() {
        let mut m = Matcher::new(1000);
        m.process(&new_order("1", "alice", 0, 100, 10), 1).unwrap();
        m.process(&new_order("2", "bob", 1, 100, 10), 2).unwrap();
        // alice's exposure should be back to 0 - she can submit at full
        // limit again.
        m.process(&new_order("3", "alice", 0, 100, 10), 3).unwrap();
    }

    #[test]
    fn risk_released_on_cancel() {
        use crate::wire::{encode_cancel, Cancel};
        let mut m = Matcher::new(1000);
        m.process(&new_order("1", "alice", 0, 100, 10), 1).unwrap();
        let buf = encode_cancel(&Cancel {
            order_id: "1".into(),
        });
        m.process(&buf, 0).unwrap();
        m.process(&new_order("2", "alice", 0, 100, 10), 2).unwrap();
    }

    #[test]
    fn partial_fill_leaves_correct_residual_exposure() {
        let mut m = Matcher::new(1000);
        m.process(&new_order("1", "alice", 0, 100, 10), 1).unwrap();
        m.process(&new_order("2", "bob", 1, 100, 4), 2).unwrap();
        assert_eq!(m.risk.exposure("alice"), 600); // 1000 - 400 filled
    }
}
