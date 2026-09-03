//! A price/time-priority central limit order book (CLOB) - the same
//! algorithm as ../../orderbook/orderbook.go, reimplemented in Rust
//! (not translated mechanically) to measure guest-language overhead
//! independent of the matching logic itself. See ../README.md.

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Clone, Debug)]
pub struct Order {
    pub id: String,
    pub participant: String,
    pub side: Side,
    pub price: i64,
    pub qty: i64,
    pub sequence: u64,
}

#[derive(Clone, Debug)]
pub struct Fill {
    pub aggressor_id: String,
    pub resting_id: String,
    pub aggressor_party: String,
    pub resting_party: String,
    pub price: i64,
    pub qty: i64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    OrderNotFound,
    DuplicateOrder,
}

#[derive(Default)]
pub struct Book {
    bids: Vec<Order>, // descending by price, then ascending by sequence
    asks: Vec<Order>, // ascending by price, then ascending by sequence
}

impl Book {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn submit(&mut self, mut incoming: Order) -> Result<Vec<Fill>, Error> {
        if self.find(&incoming.id).is_some() {
            return Err(Error::DuplicateOrder);
        }
        let fills = if incoming.side == Side::Buy {
            Self::match_against(&mut incoming, &mut self.asks)
        } else {
            Self::match_against(&mut incoming, &mut self.bids)
        };
        if incoming.qty > 0 {
            self.rest(incoming);
        }
        Ok(fills)
    }

    pub fn cancel(&mut self, id: &str) -> Result<Order, Error> {
        let (side, idx) = self.find(id).ok_or(Error::OrderNotFound)?;
        let slice = self.side_slice_mut(side);
        Ok(slice.remove(idx))
    }

    fn find(&self, id: &str) -> Option<(Side, usize)> {
        if let Some(idx) = self.bids.iter().position(|o| o.id == id) {
            return Some((Side::Buy, idx));
        }
        if let Some(idx) = self.asks.iter().position(|o| o.id == id) {
            return Some((Side::Sell, idx));
        }
        None
    }

    fn side_slice_mut(&mut self, side: Side) -> &mut Vec<Order> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    fn rest(&mut self, o: Order) {
        let side = o.side;
        let target = self.side_slice_mut(side);
        let idx = target.partition_point(|resting| ahead_of(resting, &o, side));
        target.insert(idx, o);
    }

    /// Crosses `incoming` against `resting_side` (the opposite side of
    /// incoming.side), consuming resting_side's best-priced orders while
    /// incoming still has quantity to trade and the top of resting_side
    /// is still crossable.
    fn match_against(incoming: &mut Order, resting_side: &mut Vec<Order>) -> Vec<Fill> {
        let mut fills = Vec::new();
        let mut consumed = 0;
        for resting in resting_side.iter_mut() {
            if incoming.qty <= 0 || !crosses(incoming, resting) {
                break;
            }
            let trade_qty = incoming.qty.min(resting.qty);
            fills.push(Fill {
                aggressor_id: incoming.id.clone(),
                resting_id: resting.id.clone(),
                aggressor_party: incoming.participant.clone(),
                resting_party: resting.participant.clone(),
                price: resting.price,
                qty: trade_qty,
            });
            incoming.qty -= trade_qty;
            resting.qty -= trade_qty;
            if resting.qty == 0 {
                consumed += 1;
            }
        }
        if consumed > 0 {
            resting_side.drain(0..consumed);
        }
        fills
    }
}

fn crosses(incoming: &Order, resting: &Order) -> bool {
    if incoming.side == Side::Buy {
        incoming.price >= resting.price
    } else {
        incoming.price <= resting.price
    }
}

/// Whether `a` has priority over `b` on the given side: a strictly
/// better price, or the same price with a strictly earlier sequence.
fn ahead_of(a: &Order, b: &Order, s: Side) -> bool {
    if a.price != b.price {
        return if s == Side::Buy {
            a.price > b.price
        } else {
            a.price < b.price
        };
    }
    a.sequence < b.sequence
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order(id: &str, party: &str, side: Side, price: i64, qty: i64, seq: u64) -> Order {
        Order {
            id: id.to_string(),
            participant: party.to_string(),
            side,
            price,
            qty,
            sequence: seq,
        }
    }

    #[test]
    fn rests_with_no_cross() {
        let mut b = Book::new();
        let fills = b
            .submit(order("1", "alice", Side::Buy, 100, 10, 1))
            .unwrap();
        assert!(fills.is_empty());
    }

    #[test]
    fn full_fill() {
        let mut b = Book::new();
        b.submit(order("1", "alice", Side::Buy, 100, 10, 1))
            .unwrap();
        let fills = b.submit(order("2", "bob", Side::Sell, 100, 10, 2)).unwrap();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].qty, 10);
        assert_eq!(fills[0].price, 100);
    }

    #[test]
    fn partial_fill_leaves_residual() {
        let mut b = Book::new();
        b.submit(order("1", "alice", Side::Buy, 100, 10, 1))
            .unwrap();
        let fills = b.submit(order("2", "bob", Side::Sell, 100, 4, 2)).unwrap();
        assert_eq!(fills[0].qty, 4);
        let removed = b.cancel("1").unwrap();
        assert_eq!(removed.qty, 6);
    }

    #[test]
    fn price_then_time_priority() {
        let mut b = Book::new();
        b.submit(order("1", "alice", Side::Buy, 100, 5, 1)).unwrap();
        b.submit(order("2", "alice2", Side::Buy, 101, 5, 2))
            .unwrap();
        let fills = b.submit(order("3", "bob", Side::Sell, 90, 5, 3)).unwrap();
        assert_eq!(fills[0].resting_id, "2"); // better price wins first
    }

    #[test]
    fn duplicate_rejected() {
        let mut b = Book::new();
        b.submit(order("1", "alice", Side::Buy, 100, 10, 1))
            .unwrap();
        assert_eq!(
            b.submit(order("1", "alice", Side::Buy, 100, 10, 2))
                .unwrap_err(),
            Error::DuplicateOrder
        );
    }

    #[test]
    fn cancel_releases_order() {
        let mut b = Book::new();
        b.submit(order("1", "alice", Side::Buy, 100, 10, 1))
            .unwrap();
        let removed = b.cancel("1").unwrap();
        assert_eq!(removed.qty, 10);
        assert_eq!(b.cancel("1").unwrap_err(), Error::OrderNotFound);
    }
}
