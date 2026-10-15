//! A price/time-priority book built to hold depth: the same matching rules
//! as orderbook.rs, with data structures that do not degrade as the book
//! grows.
//!
//! Why a second book rather than a fix to orderbook.rs: that file is part
//! of the published guest, whose md5 is the comparability anchor for every
//! published figure, so it stays exactly as it is. It is also the reference
//! this one is tested against - the differential test in
//! bin/book-matcher.rs drives both with the same random operations and
//! requires identical fills, cancels and resting state.
//!
//! What made the old book collapse between 100 and 1,000 resting orders per
//! side was two linear scans per order: a duplicate-id check over every
//! resting order, and a Vec insert that shifts everything behind the new
//! order's position. Here:
//!
//! - each side is a BTreeMap of price levels, so the best level is O(log L)
//!   and inserting at a new price never moves other orders;
//! - each level is a FIFO linked through a slab of slots, so resting at an
//!   existing price is O(1) and a cancel unlinks in O(1) without shifting;
//! - an id index (FNV-hashed, see below) makes the duplicate check and the
//!   cancel lookup O(1).
//!
//! Time priority within a level is arrival order. The old book orders by
//! (price, sequence) instead, and the two agree whenever sequences increase
//! with arrival, which is the only way either matcher assigns them.

use std::collections::{BTreeMap, HashMap};
use std::hash::{BuildHasherDefault, Hasher};

use crate::orderbook::{Error, Fill, Order, Side};

/// 64-bit FNV-1a. A fixed hasher on purpose: std's RandomState seeds from
/// WASI random_get, and nothing about a book should depend on what a host
/// returns for entropy. Order ids here are not attacker-chosen keys in any
/// deployment this guest is benchmarked in, so HashDoS resistance buys
/// nothing.
#[derive(Default, Clone, Copy)]
pub struct Fnv(u64);

impl Hasher for Fnv {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        let mut h = if self.0 == 0 { 0xcbf29ce484222325 } else { self.0 };
        for b in bytes {
            h ^= *b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        self.0 = h;
    }
}

pub type FnvMap<K, V> = HashMap<K, V, BuildHasherDefault<Fnv>>;

const NIL: u32 = u32::MAX;

struct Slot {
    order: Order,
    prev: u32,
    next: u32,
}

#[derive(Clone, Copy)]
struct Level {
    head: u32,
    tail: u32,
}

#[derive(Default)]
pub struct Book {
    slots: Vec<Option<Slot>>,
    free: Vec<u32>,
    bids: BTreeMap<i64, Level>, // best = highest key
    asks: BTreeMap<i64, Level>, // best = lowest key
    index: FnvMap<String, u32>,
}

impl Book {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn submit(&mut self, mut incoming: Order) -> Result<Vec<Fill>, Error> {
        if self.index.contains_key(&incoming.id) {
            return Err(Error::DuplicateOrder);
        }
        let fills = self.match_incoming(&mut incoming);
        if incoming.qty > 0 {
            self.rest(incoming);
        }
        Ok(fills)
    }

    pub fn cancel(&mut self, id: &str) -> Result<Order, Error> {
        let idx = self.index.remove(id).ok_or(Error::OrderNotFound)?;
        Ok(self.unlink(idx))
    }

    /// Every resting order in priority order: bids best to worst, then
    /// asks best to worst. This is the snapshot's serialisation order, and
    /// restoring by `rest`-ing them back in this order reproduces the same
    /// time priority within each level.
    pub fn orders(&self) -> impl Iterator<Item = &Order> {
        let bids = self.bids.values().rev().flat_map(move |l| self.walk(*l));
        let asks = self.asks.values().flat_map(move |l| self.walk(*l));
        bids.chain(asks)
    }

    fn walk(&self, level: Level) -> impl Iterator<Item = &Order> {
        let mut cur = level.head;
        std::iter::from_fn(move || {
            if cur == NIL {
                return None;
            }
            let s = self.slots[cur as usize].as_ref().expect("linked slot is live");
            cur = s.next;
            Some(&s.order)
        })
    }

    /// Append to the tail of its price level. Used by `submit` and by
    /// snapshot restore, which is why it does not check for duplicates
    /// itself - restore trusts a crc-validated snapshot.
    pub fn rest(&mut self, o: Order) {
        let price = o.price;
        let side = o.side;
        let id = o.id.clone();
        let idx = match self.free.pop() {
            Some(i) => i,
            None => {
                self.slots.push(None);
                (self.slots.len() - 1) as u32
            }
        };
        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let prev = match levels.get_mut(&price) {
            Some(l) => {
                let prev = l.tail;
                l.tail = idx;
                prev
            }
            None => {
                levels.insert(price, Level { head: idx, tail: idx });
                NIL
            }
        };
        if prev != NIL {
            self.slots[prev as usize].as_mut().unwrap().next = idx;
        }
        self.slots[idx as usize] = Some(Slot {
            order: o,
            prev,
            next: NIL,
        });
        self.index.insert(id, idx);
    }

    /// Remove a live slot from its level and the slab. The caller has
    /// already removed it from the index.
    fn unlink(&mut self, idx: u32) -> Order {
        let slot = self.slots[idx as usize].take().expect("indexed slot is live");
        self.free.push(idx);
        if slot.prev != NIL {
            self.slots[slot.prev as usize].as_mut().unwrap().next = slot.next;
        }
        if slot.next != NIL {
            self.slots[slot.next as usize].as_mut().unwrap().prev = slot.prev;
        }
        let levels = match slot.order.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let price = slot.order.price;
        let l = levels.get_mut(&price).expect("order's level exists");
        if l.head == idx {
            l.head = slot.next;
        }
        if l.tail == idx {
            l.tail = slot.prev;
        }
        if l.head == NIL {
            levels.remove(&price);
        }
        slot.order
    }

    /// Cross `incoming` against the opposite side, best level first and
    /// FIFO within a level, while it has quantity and the best level still
    /// crosses. Fills are priced at the resting order, as in orderbook.rs.
    fn match_incoming(&mut self, incoming: &mut Order) -> Vec<Fill> {
        let mut fills = Vec::new();
        while incoming.qty > 0 {
            let best = match incoming.side {
                Side::Buy => self.asks.first_key_value().map(|(p, l)| (*p, *l)),
                Side::Sell => self.bids.last_key_value().map(|(p, l)| (*p, *l)),
            };
            let Some((price, level)) = best else { break };
            let crosses = match incoming.side {
                Side::Buy => incoming.price >= price,
                Side::Sell => incoming.price <= price,
            };
            if !crosses {
                break;
            }
            let idx = level.head;
            let resting = &mut self.slots[idx as usize].as_mut().unwrap().order;
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
                let id = resting.id.clone();
                self.index.remove(&id);
                self.unlink(idx);
            }
        }
        fills
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn o(id: &str, side: Side, price: i64, qty: i64, seq: u64) -> Order {
        Order {
            id: id.into(),
            participant: "p".into(),
            side,
            price,
            qty,
            sequence: seq,
        }
    }

    #[test]
    fn fifo_within_a_level_and_best_price_first() {
        let mut b = Book::new();
        b.submit(o("a1", Side::Sell, 101, 1, 1)).unwrap();
        b.submit(o("a2", Side::Sell, 100, 1, 2)).unwrap();
        b.submit(o("a3", Side::Sell, 100, 1, 3)).unwrap();
        let f = b.submit(o("b1", Side::Buy, 101, 3, 4)).unwrap();
        let ids: Vec<_> = f.iter().map(|f| f.resting_id.as_str()).collect();
        assert_eq!(ids, ["a2", "a3", "a1"]);
        assert_eq!(b.len(), 0);
    }

    #[test]
    fn cancel_from_the_middle_of_a_level_keeps_the_rest_linked() {
        let mut b = Book::new();
        for (i, id) in ["x", "y", "z"].iter().enumerate() {
            b.submit(o(id, Side::Buy, 50, 1, i as u64)).unwrap();
        }
        assert_eq!(b.cancel("y").unwrap().id, "y");
        assert_eq!(b.cancel("y").err(), Some(Error::OrderNotFound));
        let ids: Vec<_> = b.orders().map(|o| o.id.as_str()).collect();
        assert_eq!(ids, ["x", "z"]);
        b.cancel("x").unwrap();
        b.cancel("z").unwrap();
        assert!(b.bids.is_empty(), "empty level is removed");
    }

    #[test]
    fn duplicate_id_is_rejected() {
        let mut b = Book::new();
        b.submit(o("a", Side::Buy, 50, 1, 1)).unwrap();
        assert!(matches!(
            b.submit(o("a", Side::Sell, 60, 1, 2)),
            Err(Error::DuplicateOrder)
        ));
    }

    #[test]
    fn freed_slots_are_reused() {
        let mut b = Book::new();
        for i in 0..1000 {
            let id = format!("o{i}");
            b.submit(o(&id, Side::Buy, 50, 1, i)).unwrap();
            b.cancel(&id).unwrap();
        }
        assert_eq!(b.slots.len(), 1);
    }
}
