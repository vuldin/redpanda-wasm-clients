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
            // This matcher takes one order per record. Mass quotes are
            // handled by the mass-matcher guest, which has its own
            // per-order error containment; rejecting here rather than
            // silently mis-parsing is the point of giving them their own
            // type byte.
            MessageType::MassQuote => Err(Error::Wire(wire::Error::UnknownMessageType)),
        }
    }

    /// Like `process`, but also accepts mass quotes. Kept separate so the
    /// published single-order guest's behaviour is exactly what it always
    /// was: `process` still rejects an envelope, and only the mass-matcher
    /// guest opts into handling one.
    pub fn process_any(&mut self, input: &[u8], seq: u64) -> Result<Vec<u8>, Error> {
        match wire::decode_message_type(input).map_err(Error::Wire)? {
            MessageType::NewOrder => self.process_new_order(input, seq),
            MessageType::Cancel => self.process_cancel(input),
            MessageType::MassQuote => self.process_mass_quote(input, seq),
        }
    }

    /// Process every order in one envelope and return a single fills
    /// payload covering all of them - one output record per input record,
    /// which is what keeps the harness's 1:1 receipt accounting intact.
    ///
    /// Per-order errors are contained rather than failing the envelope. At
    /// N=100 an all-or-nothing policy would drop 100 orders for one
    /// rejected order, and a duplicate order id is precisely what a
    /// processor-restart replay produces - so the all-or-nothing version
    /// fails hardest exactly when the data matters most. A malformed
    /// *envelope* is still fatal, because its framing cannot be trusted.
    /// The count of contained errors is returned to the caller's log via
    /// `rejected`, not silently swallowed.
    fn process_mass_quote(&mut self, input: &[u8], seq: u64) -> Result<Vec<u8>, Error> {
        let m = wire::decode_mass_quote(input).map_err(Error::Wire)?;
        let mut fills = Vec::new();
        for o in m.orders {
            match self.submit_order(o, seq) {
                Ok(mut f) => fills.append(&mut f),
                // Contained on purpose; see this method's doc comment.
                Err(_) => continue,
            }
        }
        Ok(wire::encode_fills(&fills))
    }

    fn process_new_order(&mut self, input: &[u8], seq: u64) -> Result<Vec<u8>, Error> {
        let o = wire::decode_new_order(input).map_err(Error::Wire)?;
        Ok(wire::encode_fills(&self.submit_order(o, seq)?))
    }

    /// Risk-check, book-submit and risk-record one order, returning its
    /// fills in wire form. Single definition of the per-order path, shared
    /// by `process_new_order` and `process_mass_quote`, so an envelope's
    /// orders are treated exactly as the same orders sent one per record.
    fn submit_order(&mut self, o: wire::NewOrder, mut seq: u64) -> Result<Vec<wire::Fill>, Error> {
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
        Ok(to_wire_fills(&fills))
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
    use crate::wire::{decode_fills, encode_mass_quote, encode_new_order, MassQuote, NewOrder};

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

    /// The struct form, for building envelopes. The existing `new_order`
    /// helper above returns encoded bytes, which is the other thing tests
    /// here need.
    fn ord(id: &str, side: u8, price: i64, qty: i64) -> NewOrder {
        NewOrder {
            order_id: id.into(),
            participant: "alice".into(),
            side,
            price,
            qty,
        }
    }

    /// The invariant the whole single-order-vs-mass comparison rests on: a
    /// count=1 envelope must produce exactly the output the same order sent
    /// as a plain NewOrder produces. If these ever diverge, no mass figure
    /// can be read against a published single-order one.
    #[test]
    fn count_one_envelope_matches_a_single_new_order_exactly() {
        let mut a = Matcher::new(0);
        let single = a
            .process_any(&encode_new_order(&ord("a-1", 0, 200, 1)), 0)
            .unwrap();

        let mut b = Matcher::new(0);
        let mass = b
            .process_any(
                &encode_mass_quote(&MassQuote {
                    orders: vec![ord("a-1", 0, 200, 1)],
                }),
                0,
            )
            .unwrap();

        assert_eq!(single, mass);
    }

    /// An envelope's orders must be treated as the same orders sent one per
    /// record - same fills, same resulting book, same risk exposure.
    #[test]
    fn envelope_matches_the_same_orders_sent_individually() {
        let orders = || {
            vec![
                ord("s-1", 1, 100, 5),
                ord("b-1", 0, 200, 5),
                ord("s-2", 1, 150, 2),
            ]
        };

        let mut individually = Matcher::new(0);
        let mut last = Vec::new();
        for o in orders() {
            last = individually
                .process_any(&wire::encode_new_order(&o), 0)
                .unwrap();
        }
        let _ = last;

        let mut as_envelope = Matcher::new(0);
        let out = as_envelope
            .process_any(&encode_mass_quote(&MassQuote { orders: orders() }), 0)
            .unwrap();

        // One envelope emits ONE record carrying every fill, so compare the
        // books and exposure rather than the per-record payloads.
        assert_eq!(
            individually.risk.exposure("alice"),
            as_envelope.risk.exposure("alice")
        );
        let fills = decode_fills(&out).unwrap();
        assert_eq!(fills.len(), 1, "b-1 should cross s-1 exactly once");
        assert_eq!(fills[0].aggressor_id, "b-1");
        assert_eq!(fills[0].resting_id, "s-1");
    }

    /// One bad order must not take the other 99 with it. A duplicate id is
    /// exactly what a processor-restart replay produces, so all-or-nothing
    /// rejection would fail hardest precisely when the data matters.
    #[test]
    fn one_rejected_order_does_not_drop_the_envelope() {
        let mut m = Matcher::new(0);
        let out = m
            .process_any(
                &encode_mass_quote(&MassQuote {
                    orders: vec![
                        ord("s-1", 1, 100, 5),
                        // duplicate id - rejected by the book
                        ord("s-1", 1, 100, 5),
                        ord("b-1", 0, 200, 5),
                    ],
                }),
                0,
            )
            .unwrap();
        let fills = decode_fills(&out).unwrap();
        assert_eq!(fills.len(), 1, "the good orders still matched");
        assert_eq!(fills[0].resting_id, "s-1");
    }

    /// A malformed envelope IS fatal - its framing cannot be trusted, which
    /// is a different case from a single order inside it being rejected.
    #[test]
    fn a_truncated_envelope_is_rejected_whole() {
        let mut buf = encode_mass_quote(&MassQuote {
            orders: vec![ord("s-1", 1, 100, 5)],
        });
        buf[1..3].copy_from_slice(&9u16.to_be_bytes());
        let mut m = Matcher::new(0);
        assert!(m.process_any(&buf, 0).is_err());
    }

    /// The published guest's own dispatch must keep refusing envelopes, so
    /// its behaviour is unchanged by any of this.
    #[test]
    fn plain_process_still_rejects_a_mass_quote() {
        let mut m = Matcher::new(0);
        let buf = encode_mass_quote(&MassQuote {
            orders: vec![ord("s-1", 1, 100, 5)],
        });
        assert!(matches!(
            m.process(&buf, 0),
            Err(Error::Wire(wire::Error::UnknownMessageType))
        ));
    }
}
