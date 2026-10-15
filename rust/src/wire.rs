//! The exact same fixed-layout binary encoding as ../../wire/wire.go -
//! byte-for-byte compatible, not just logically equivalent, so the same
//! bench harness traffic works against either guest unmodified. See
//! wire.go's own doc comment for why this exists and isn't JSON/protobuf.

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum MessageType {
    NewOrder,
    Cancel,
    /// Several orders in one record, the way an exchange's mass-quote
    /// message does. A distinct type byte rather than an extension of
    /// `NewOrder` is deliberate: trailing bytes after the fixed layout are
    /// filler every decoder must ignore, so extending type 0 would make a
    /// padded single-order record decode as an envelope full of garbage.
    MassQuote,
}

#[derive(Debug)]
pub enum Error {
    ShortBuffer,
    UnknownMessageType,
}

pub struct NewOrder {
    pub order_id: String,
    pub participant: String,
    pub side: u8, // 0 = buy, 1 = sell
    pub price: i64,
    pub qty: i64,
}

pub struct Cancel {
    pub order_id: String,
}

/// One record carrying several orders. Each order keeps its own id, so
/// per-order delivery accounting survives the batching; per-order *timing*
/// does not, because one record has one send instant and earns one match
/// stamp and one receipt.
pub struct MassQuote {
    pub orders: Vec<NewOrder>,
}

pub struct Fill {
    pub aggressor_id: String,
    pub resting_id: String,
    pub aggressor_party: String,
    pub resting_party: String,
    pub price: i64,
    pub qty: i64,
}

// layout: [type:1][len(orderID):2][orderID][len(participant):2][participant][side:1][price:8][qty:8]
pub fn encode_new_order(o: &NewOrder) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + new_order_body_size(o));
    buf.push(MessageType::NewOrder as u8);
    append_new_order_body(&mut buf, o);
    buf
}

/// The type-byte-less body of a `NewOrder`. Single definition of that field
/// layout, shared with `encode_mass_quote` so the two cannot drift.
fn append_new_order_body(buf: &mut Vec<u8>, o: &NewOrder) {
    append_string(buf, &o.order_id);
    append_string(buf, &o.participant);
    buf.push(o.side);
    buf.extend_from_slice(&o.price.to_be_bytes());
    buf.extend_from_slice(&o.qty.to_be_bytes());
}

fn new_order_body_size(o: &NewOrder) -> usize {
    2 + o.order_id.len() + 2 + o.participant.len() + 1 + 8 + 8
}

// layout: [type:1][count:2] then, per order, the same body encode_new_order writes
pub fn encode_mass_quote(m: &MassQuote) -> Vec<u8> {
    let size: usize = 1 + 2 + m.orders.iter().map(new_order_body_size).sum::<usize>();
    let mut buf = Vec::with_capacity(size);
    buf.push(MessageType::MassQuote as u8);
    buf.extend_from_slice(&(m.orders.len() as u16).to_be_bytes());
    for o in &m.orders {
        append_new_order_body(&mut buf, o);
    }
    buf
}

/// Decode a mass quote. A count of zero is legal. A count larger than the
/// bytes supplied is `ShortBuffer` rather than a partial result, so a
/// truncated record is never processed as a shorter envelope. Trailing
/// bytes after the last order are filler and ignored.
pub fn decode_mass_quote(buf: &[u8]) -> Result<MassQuote, Error> {
    if buf.is_empty() || buf[0] != MessageType::MassQuote as u8 {
        return Err(Error::UnknownMessageType);
    }
    if buf.len() < 3 {
        return Err(Error::ShortBuffer);
    }
    let count = u16::from_be_bytes(buf[1..3].try_into().unwrap());
    let mut rest = &buf[3..];
    let mut orders = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (o, r) = read_new_order_body(rest)?;
        orders.push(o);
        rest = r;
    }
    Ok(MassQuote { orders })
}

// layout: [type:1][len(orderID):2][orderID]
pub fn encode_cancel(c: &Cancel) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 2 + c.order_id.len());
    buf.push(MessageType::Cancel as u8);
    append_string(&mut buf, &c.order_id);
    buf
}

pub fn decode_message_type(buf: &[u8]) -> Result<MessageType, Error> {
    match buf.first() {
        None => Err(Error::ShortBuffer),
        Some(0) => Ok(MessageType::NewOrder),
        Some(1) => Ok(MessageType::Cancel),
        Some(2) => Ok(MessageType::MassQuote),
        Some(_) => Err(Error::UnknownMessageType),
    }
}

pub fn decode_new_order(buf: &[u8]) -> Result<NewOrder, Error> {
    if buf.is_empty() || buf[0] != MessageType::NewOrder as u8 {
        return Err(Error::UnknownMessageType);
    }
    // Trailing bytes after the body are filler and ignored - that is what
    // makes the harness's -payload-bytes padding work.
    let (o, _) = read_new_order_body(&buf[1..])?;
    Ok(o)
}

/// Decode the type-byte-less body of a `NewOrder`, returning what follows.
/// Single definition of the field layout on the decode side.
fn read_new_order_body(buf: &[u8]) -> Result<(NewOrder, &[u8]), Error> {
    let (order_id, rest) = read_string(buf)?;
    let (participant, rest) = read_string(rest)?;
    if rest.len() < 1 + 8 + 8 {
        return Err(Error::ShortBuffer);
    }
    let side = rest[0];
    let price = i64::from_be_bytes(rest[1..9].try_into().unwrap());
    let qty = i64::from_be_bytes(rest[9..17].try_into().unwrap());
    Ok((
        NewOrder {
            order_id,
            participant,
            side,
            price,
            qty,
        },
        &rest[17..],
    ))
}

pub fn decode_cancel(buf: &[u8]) -> Result<Cancel, Error> {
    if buf.is_empty() || buf[0] != MessageType::Cancel as u8 {
        return Err(Error::UnknownMessageType);
    }
    let (order_id, _) = read_string(&buf[1..])?;
    Ok(Cancel { order_id })
}

// layout: [count:2] then, per fill: [len(aggressorID):2][aggressorID][len(restingID):2][restingID][len(aggressorParty):2][aggressorParty][len(restingParty):2][restingParty][price:8][qty:8]
pub fn encode_fills(fills: &[Fill]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(2 + 64 * fills.len());
    buf.extend_from_slice(&(fills.len() as u16).to_be_bytes());
    for f in fills {
        append_string(&mut buf, &f.aggressor_id);
        append_string(&mut buf, &f.resting_id);
        append_string(&mut buf, &f.aggressor_party);
        append_string(&mut buf, &f.resting_party);
        buf.extend_from_slice(&f.price.to_be_bytes());
        buf.extend_from_slice(&f.qty.to_be_bytes());
    }
    buf
}

pub fn decode_fills(buf: &[u8]) -> Result<Vec<Fill>, Error> {
    if buf.len() < 2 {
        return Err(Error::ShortBuffer);
    }
    let count = u16::from_be_bytes(buf[0..2].try_into().unwrap());
    let mut rest = &buf[2..];
    let mut fills = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (aggressor_id, r) = read_string(rest)?;
        let (resting_id, r) = read_string(r)?;
        let (aggressor_party, r) = read_string(r)?;
        let (resting_party, r) = read_string(r)?;
        if r.len() < 16 {
            return Err(Error::ShortBuffer);
        }
        let price = i64::from_be_bytes(r[0..8].try_into().unwrap());
        let qty = i64::from_be_bytes(r[8..16].try_into().unwrap());
        rest = &r[16..];
        fills.push(Fill {
            aggressor_id,
            resting_id,
            aggressor_party,
            resting_party,
            price,
            qty,
        });
    }
    Ok(fills)
}

fn append_string(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u16).to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn read_string(buf: &[u8]) -> Result<(String, &[u8]), Error> {
    if buf.len() < 2 {
        return Err(Error::ShortBuffer);
    }
    let n = u16::from_be_bytes(buf[0..2].try_into().unwrap()) as usize;
    let buf = &buf[2..];
    if buf.len() < n {
        return Err(Error::ShortBuffer);
    }
    let s = String::from_utf8_lossy(&buf[..n]).into_owned();
    Ok((s, &buf[n..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_order_round_trip() {
        let o = NewOrder {
            order_id: "abc".into(),
            participant: "alice".into(),
            side: 1,
            price: 100,
            qty: 42,
        };
        let buf = encode_new_order(&o);
        let decoded = decode_new_order(&buf).unwrap();
        assert_eq!(decoded.order_id, "abc");
        assert_eq!(decoded.participant, "alice");
        assert_eq!(decoded.side, 1);
        assert_eq!(decoded.price, 100);
        assert_eq!(decoded.qty, 42);
    }

    fn order(id: &str, side: u8, price: i64, qty: i64) -> NewOrder {
        NewOrder {
            order_id: id.into(),
            participant: "alice".into(),
            side,
            price,
            qty,
        }
    }

    #[test]
    fn mass_quote_round_trip() {
        let m = MassQuote {
            orders: vec![
                order("a-1", 0, 200, 1),
                order("a-2", 1, -42, 7),
                order("a-3", 0, 100, 3),
            ],
        };
        let decoded = decode_mass_quote(&encode_mass_quote(&m)).unwrap();
        assert_eq!(decoded.orders.len(), 3);
        assert_eq!(decoded.orders[1].order_id, "a-2");
        // Signed price survives the envelope, same trap EncodeFills has.
        assert_eq!(decoded.orders[1].price, -42);
        assert_eq!(decoded.orders[2].qty, 3);
    }

    #[test]
    fn mass_quote_count_zero_is_legal() {
        let m = MassQuote { orders: vec![] };
        let buf = encode_mass_quote(&m);
        assert_eq!(buf.len(), 3);
        assert!(decode_mass_quote(&buf).unwrap().orders.is_empty());
    }

    #[test]
    fn mass_quote_ignores_trailing_padding() {
        // -payload-bytes pads records; every decoder must stop at the
        // declared count rather than trying to consume the filler.
        let mut buf = encode_mass_quote(&MassQuote {
            orders: vec![order("a-1", 0, 200, 1)],
        });
        buf.extend_from_slice(&[0u8; 64]);
        assert_eq!(decode_mass_quote(&buf).unwrap().orders.len(), 1);
    }

    #[test]
    fn mass_quote_count_larger_than_payload_is_short_buffer() {
        // A truncated envelope must fail, never decode as a shorter one.
        let mut buf = encode_mass_quote(&MassQuote {
            orders: vec![order("a-1", 0, 200, 1), order("a-2", 1, 100, 1)],
        });
        buf[1..3].copy_from_slice(&5u16.to_be_bytes());
        assert!(matches!(decode_mass_quote(&buf), Err(Error::ShortBuffer)));
    }

    #[test]
    fn mass_quote_body_matches_single_new_order_byte_for_byte() {
        // The invariant the whole backward-compatibility claim rests on: a
        // count=1 envelope carries exactly the bytes encode_new_order would
        // emit, minus its type byte. If these two ever drift, every
        // single-order figure stops being comparable to a mass one.
        let o = order("a-1", 0, 200, 1);
        let single = encode_new_order(&o);
        let mass = encode_mass_quote(&MassQuote {
            orders: vec![order("a-1", 0, 200, 1)],
        });
        assert_eq!(&mass[3..], &single[1..]);
        assert_eq!(mass[0], MessageType::MassQuote as u8);
        assert_eq!(single[0], MessageType::NewOrder as u8);
    }

    #[test]
    fn a_mass_quote_is_not_a_new_order() {
        // Type-byte separation is what makes a padded single-order record
        // from a published run fail loudly instead of decoding as garbage.
        let buf = encode_mass_quote(&MassQuote {
            orders: vec![order("a-1", 0, 200, 1)],
        });
        assert!(matches!(
            decode_new_order(&buf),
            Err(Error::UnknownMessageType)
        ));
        let single = encode_new_order(&order("a-1", 0, 200, 1));
        assert!(matches!(
            decode_mass_quote(&single),
            Err(Error::UnknownMessageType)
        ));
    }

    // Cross-implementation fixture. The Go harness decodes bytes this
    // encoder produced; see WIRE-FORMAT.md's Conformance section. Run
    //   REGEN_WIRE_FIXTURES=1 cargo test emit_mass_quote_fixture
    // to rewrite it, then copy it into the bench repo. Never hand-edit
    // either copy - that makes the test agree with whichever side is wrong.
    #[test]
    fn emit_mass_quote_fixture() {
        let m = MassQuote {
            orders: vec![
                order("a-1", 0, 10150, 7),
                // Negative price: both sides encode i64 big-endian, and a
                // decoder that reads it unsigned passes every
                // positive-only test and then produces 2^64-42 in
                // production. Same trap the fills fixture exists to catch.
                order("a-2", 1, -42, 1),
                order("a-3", 0, 100, 3),
            ],
        };
        let hex: String = encode_mass_quote(&m)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/testdata/mass-quote-from-rust-guest.hex"
        );
        if std::env::var("REGEN_WIRE_FIXTURES").is_ok() {
            std::fs::create_dir_all(std::path::Path::new(path).parent().unwrap()).unwrap();
            std::fs::write(path, format!("{hex}\n")).unwrap();
            return;
        }
        let checked_in = std::fs::read_to_string(path)
            .expect("fixture missing; regenerate with REGEN_WIRE_FIXTURES=1");
        assert_eq!(
            checked_in.trim(),
            hex,
            "this encoder no longer produces the checked-in fixture"
        );
    }

    #[test]
    fn cancel_round_trip() {
        let c = Cancel {
            order_id: "xyz".into(),
        };
        let buf = encode_cancel(&c);
        let decoded = decode_cancel(&buf).unwrap();
        assert_eq!(decoded.order_id, "xyz");
    }

    #[test]
    fn fills_round_trip_including_empty() {
        assert!(decode_fills(&encode_fills(&[])).unwrap().is_empty());
        let f = Fill {
            aggressor_id: "1".into(),
            resting_id: "2".into(),
            aggressor_party: "a".into(),
            resting_party: "b".into(),
            price: 100,
            qty: 5,
        };
        let decoded = decode_fills(&encode_fills(&[f])).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].price, 100);
        assert_eq!(decoded[0].qty, 5);
    }

    #[test]
    fn short_buffer_rejected() {
        assert!(matches!(decode_new_order(&[0]), Err(Error::ShortBuffer)));
    }

    #[test]
    fn wrong_message_type_rejected() {
        let c = encode_cancel(&Cancel {
            order_id: "1".into(),
        });
        assert!(matches!(
            decode_new_order(&c),
            Err(Error::UnknownMessageType)
        ));
    }
}
