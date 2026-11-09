//! The pack format: many small objects in one stored file, so each costs one range of a file
//! instead of one file (an S3 object, or a file and a directory on a local disk).
//!
//! ```text
//! "SCPK" 0x01                       header
//! stored bytes of each object       back to back, as a loose object would store them
//! per object: id, offset, len       trailer index (offset and len as u64 LE)
//! count (u32 LE) "SCPK"             footer
//! ```
//!
//! Readers find objects through the pack locations in the metadata store and fetch only their
//! range. The trailer makes a pack self-describing, so it can be rewritten (redaction purge) or
//! its locations rebuilt without the metadata store.

use bytes::Bytes;
use scion_object::{ID_LEN, ObjectId};
use scion_store::PackedObject;

use crate::delta::Base;

const MAGIC: &[u8; 4] = b"SCPK";
const VERSION: u8 = 1;
const HEADER: usize = MAGIC.len() + 1;
const FOOTER: usize = 4 + MAGIC.len();
const INDEX_ENTRY: usize = ID_LEN + 16;

/// A pack being written.
pub(crate) struct PackBuilder {
    buf: Vec<u8>,
    objects: Vec<PackedObject>,
    /// Objects by id, and by offset (a delta in the same pack names its base by where it is).
    by_id: std::collections::HashMap<ObjectId, usize>,
    by_offset: std::collections::HashMap<u64, usize>,
}

impl PackBuilder {
    pub(crate) fn new() -> Self {
        let mut buf = Vec::with_capacity(1 << 16);
        buf.extend_from_slice(MAGIC);
        buf.push(VERSION);
        Self {
            buf,
            objects: Vec::new(),
            by_id: std::collections::HashMap::new(),
            by_offset: std::collections::HashMap::new(),
        }
    }

    /// Bytes of object data so far.
    pub(crate) fn data_len(&self) -> usize {
        self.buf.len() - HEADER
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }

    /// Where `id` is in this pack, if it was added: how a delta in the same pack names it.
    pub(crate) fn place(&self, id: &ObjectId) -> Option<Base> {
        let o = &self.objects[*self.by_id.get(id)?];
        Some(Base::At {
            offset: o.offset,
            len: o.len,
        })
    }

    /// Adds an object's stored bytes (whole, or a delta that names its base). A delta naming its
    /// base by where it is must come after that base.
    pub(crate) fn add(&mut self, id: ObjectId, stored: &[u8]) {
        let base = match crate::delta::base_of(stored) {
            Some(Base::Id(b)) => Some(b),
            // Nothing there: whole content that only starts like a delta
            Some(Base::At { offset, len }) => self
                .by_offset
                .get(&offset)
                .map(|&i| &self.objects[i])
                .filter(|o| o.len == len)
                .map(|o| o.id),
            None => None,
        };
        self.by_id.entry(id).or_insert(self.objects.len());
        self.by_offset.insert(self.buf.len() as u64, self.objects.len());
        self.objects.push(PackedObject {
            id,
            offset: self.buf.len() as u64,
            len: stored.len() as u64,
            base,
        });
        self.buf.extend_from_slice(stored);
    }

    /// The pack bytes, its name (BLAKE3 of the bytes) and where each object is in it.
    pub(crate) fn finish(mut self) -> (Bytes, [u8; 32], Vec<PackedObject>) {
        for o in &self.objects {
            self.buf.extend_from_slice(&o.id.to_bytes());
            self.buf.extend_from_slice(&o.offset.to_le_bytes());
            self.buf.extend_from_slice(&o.len.to_le_bytes());
        }
        let count = u32::try_from(self.objects.len()).unwrap_or(u32::MAX);
        self.buf.extend_from_slice(&count.to_le_bytes());
        self.buf.extend_from_slice(MAGIC);
        let name = *blake3::hash(&self.buf).as_bytes();
        (Bytes::from(self.buf), name, self.objects)
    }
}

/// The objects in a whole pack, from its trailer. None when the bytes are not a valid pack.
pub(crate) fn parse(pack: &[u8]) -> Option<Vec<PackedObject>> {
    if pack.len() < HEADER || &pack[..4] != MAGIC || pack[4] != VERSION {
        return None;
    }
    parse_tail(pack, pack.len() as u64)
}

/// How many bytes at the end of a pack hold its trailer, from at least its last [`FOOTER`] bytes.
pub(crate) fn trailer_len(tail: &[u8]) -> Option<usize> {
    if tail.len() < FOOTER || !tail.ends_with(MAGIC) {
        return None;
    }
    let at = tail.len() - FOOTER;
    let count = u32::from_le_bytes(tail[at..at + 4].try_into().ok()?) as usize;
    count.checked_mul(INDEX_ENTRY)?.checked_add(FOOTER)
}

/// The objects in a pack of `total` bytes, from its last bytes `tail` (at least its trailer).
pub(crate) fn parse_tail(tail: &[u8], total: u64) -> Option<Vec<PackedObject>> {
    let need = trailer_len(tail)?;
    if need > tail.len() || (total as usize) < HEADER + need || tail.len() as u64 > total {
        return None;
    }
    let count_at = tail.len() - FOOTER;
    let index_at = tail.len() - need;
    let data_end = total - need as u64;
    let mut out = Vec::with_capacity((need - FOOTER) / INDEX_ENTRY);
    for e in tail[index_at..count_at].as_chunks::<INDEX_ENTRY>().0 {
        let id = ObjectId::from_bytes(&e[..ID_LEN]).ok()?;
        let offset = u64::from_le_bytes(e[ID_LEN..ID_LEN + 8].try_into().ok()?);
        let len = u64::from_le_bytes(e[ID_LEN + 8..].try_into().ok()?);
        let end = offset.checked_add(len)?;
        if offset < HEADER as u64 || end > data_end {
            return None;
        }
        out.push(PackedObject {
            id,
            offset,
            len,
            base: None,
        });
    }
    Some(out)
}

/// The key of a pack relative to the store prefix: `packs/{hh}/{hex}`.
pub(crate) fn pack_key(name: &[u8; 32]) -> String {
    let hex: String = name.iter().map(|b| format!("{b:02x}")).collect();
    format!("packs/{}/{hex}", &hex[..2])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(objs: &[&[u8]]) -> (Bytes, [u8; 32], Vec<PackedObject>) {
        let mut b = PackBuilder::new();
        assert!(b.is_empty());
        for o in objs {
            b.add(ObjectId::of_blob(o), o);
        }
        b.finish()
    }

    #[test]
    fn roundtrip_and_ranges() {
        let objs: [&[u8]; 3] = [b"alpha", b"", b"gamma gamma"];
        let (bytes, name, placed) = build(&objs);
        assert_eq!(name, *blake3::hash(&bytes).as_bytes());
        assert_eq!(parse(&bytes), Some(placed.clone()));
        for (o, p) in objs.iter().zip(&placed) {
            assert_eq!(p.id, ObjectId::of_blob(o));
            assert_eq!(&bytes[p.offset as usize..(p.offset + p.len) as usize], *o);
        }
        let mut b = PackBuilder::new();
        b.add(ObjectId::of_blob(b"abc"), b"abc");
        assert_eq!(b.data_len(), 3);
        assert!(!b.is_empty());
    }

    #[test]
    fn rejects_damaged_packs() {
        let (bytes, _, _) = build(&[b"one", b"two"]);
        assert_eq!(parse(&[]), None);
        assert_eq!(parse(&bytes[..bytes.len() - 1]), None, "truncated");
        let mut bad = bytes.to_vec();
        bad[4] = 9;
        assert_eq!(parse(&bad), None, "unknown version");
        let mut bad = bytes.to_vec();
        bad[0] = b'X';
        assert_eq!(parse(&bad), None, "bad magic");
        // count larger than the pack
        let mut bad = bytes.to_vec();
        let at = bad.len() - FOOTER;
        bad[at..at + 4].copy_from_slice(&1000u32.to_le_bytes());
        assert_eq!(parse(&bad), None, "count too large");
        // an entry pointing past the data
        let mut bad = bytes.to_vec();
        let entry = bad.len() - FOOTER - INDEX_ENTRY;
        bad[entry + ID_LEN + 8..entry + ID_LEN + 16].copy_from_slice(&u64::MAX.to_le_bytes());
        assert_eq!(parse(&bad), None, "range out of bounds");
        // an entry pointing into the header
        let mut bad = bytes.to_vec();
        bad[entry + ID_LEN..entry + ID_LEN + 8].copy_from_slice(&0u64.to_le_bytes());
        assert_eq!(parse(&bad), None, "range in header");
        // a corrupt id
        let mut bad = bytes.to_vec();
        bad[entry] = 0xff;
        assert_eq!(parse(&bad), None, "corrupt id");
        // an empty pack is valid
        let (empty, _, placed) = PackBuilder::new().finish();
        assert_eq!(parse(&empty), Some(placed));
    }

    #[test]
    fn index_from_the_tail_alone() {
        let objs: [&[u8]; 4] = [b"one", b"two", b"three", b"four"];
        let (bytes, _, placed) = build(&objs);
        let need = trailer_len(&bytes[bytes.len() - FOOTER..]).expect("footer");
        assert_eq!(need, FOOTER + 4 * INDEX_ENTRY);
        let total = bytes.len() as u64;
        assert_eq!(parse_tail(&bytes[bytes.len() - need..], total), Some(placed.clone()));
        assert_eq!(parse_tail(&bytes[bytes.len() - need - 3..], total), Some(placed));
        assert_eq!(
            parse_tail(&bytes[bytes.len() - need + 1..], total),
            None,
            "trailer cut short"
        );
        assert_eq!(
            parse_tail(&bytes[bytes.len() - need..], need as u64),
            None,
            "pack too small"
        );
        assert_eq!(parse_tail(&bytes, total - 1), None, "tail longer than the pack");
        assert_eq!(trailer_len(b"SCPK"), None);
        assert_eq!(trailer_len(b"\0\0\0\0XXXX"), None);
    }

    #[test]
    fn key_layout() {
        let mut name = [0u8; 32];
        name[0] = 0xab;
        let key = pack_key(&name);
        assert!(key.starts_with("packs/ab/ab00"));
        assert_eq!(key.len(), "packs/ab/".len() + 64);
    }
}
