//! The exact same fixed-layout binary encoding as ../../wire/wire.go -
//! byte-for-byte compatible, not just logically equivalent, so the same
//! bench harness traffic works against either guest unmodified. See
//! wire.go's own doc comment for why this exists and isn't JSON/protobuf.

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum MessageType {
    NewOrder,
    Cancel,
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
    let mut buf =
        Vec::with_capacity(1 + 2 + o.order_id.len() + 2 + o.participant.len() + 1 + 8 + 8);
    buf.push(MessageType::NewOrder as u8);
    append_string(&mut buf, &o.order_id);
    append_string(&mut buf, &o.participant);
    buf.push(o.side);
    buf.extend_from_slice(&o.price.to_be_bytes());
    buf.extend_from_slice(&o.qty.to_be_bytes());
    buf
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
        Some(_) => Err(Error::UnknownMessageType),
    }
}

pub fn decode_new_order(buf: &[u8]) -> Result<NewOrder, Error> {
    if buf.is_empty() || buf[0] != MessageType::NewOrder as u8 {
        return Err(Error::UnknownMessageType);
    }
    let rest = &buf[1..];
    let (order_id, rest) = read_string(rest)?;
    let (participant, rest) = read_string(rest)?;
    if rest.len() < 1 + 8 + 8 {
        return Err(Error::ShortBuffer);
    }
    let side = rest[0];
    let price = i64::from_be_bytes(rest[1..9].try_into().unwrap());
    let qty = i64::from_be_bytes(rest[9..17].try_into().unwrap());
    Ok(NewOrder {
        order_id,
        participant,
        side,
        price,
        qty,
    })
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
