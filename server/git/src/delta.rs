//! Computing git deltas (the format [`crate::unpack::apply_delta`] reads): the base is indexed by
//! 16-byte blocks, the target is scanned for blocks it shares, and each match is extended both
//! ways into a copy; the bytes between matches are inserts.

use std::collections::HashMap;

/// Matches shorter than this are not worth a copy instruction.
const BLOCK: usize = 16;
/// Files larger than this are sent whole: their index would cost more memory than the delta saves.
pub const MAX_DELTA_INPUT: usize = 32 << 20;
/// A copy instruction carries at most three size bytes.
const MAX_COPY: usize = 0x00ff_ffff;
/// Candidates kept per block hash: enough for repeated text, bounded for pathological input.
const MAX_CANDIDATES: usize = 8;

fn block_hash(b: &[u8]) -> u64 {
    let v = u128::from_le_bytes(b[..BLOCK].try_into().expect("a block"));
    let x = (v as u64) ^ ((v >> 64) as u64).rotate_left(29);
    x.wrapping_mul(0x9e37_79b9_7f4a_7c15)
}

fn varint(out: &mut Vec<u8>, mut v: usize) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

fn flush_insert(out: &mut Vec<u8>, pending: &[u8]) {
    for chunk in pending.chunks(127) {
        out.push(chunk.len() as u8);
        out.extend_from_slice(chunk);
    }
}

fn copy(out: &mut Vec<u8>, mut off: usize, mut len: usize) {
    while len > 0 {
        let n = len.min(MAX_COPY);
        let mut op = 0x80u8;
        let mut args = Vec::with_capacity(7);
        for i in 0..4 {
            let b = ((off >> (8 * i)) & 0xff) as u8;
            if b != 0 {
                op |= 1 << i;
                args.push(b);
            }
        }
        // A size of 0x10000 is written as no size bytes
        if n != 0x10000 {
            for i in 0..3 {
                let b = ((n >> (8 * i)) & 0xff) as u8;
                if b != 0 {
                    op |= 1 << (4 + i);
                    args.push(b);
                }
            }
        }
        out.push(op);
        out.extend_from_slice(&args);
        off += n;
        len -= n;
    }
}

/// A delta from `base` to `target`, or `None` when it would not be clearly smaller than the
/// target itself (or the input is too large to index).
pub fn delta(base: &[u8], target: &[u8]) -> Option<Vec<u8>> {
    if base.len() < BLOCK || target.len() < BLOCK || base.len() > MAX_DELTA_INPUT || target.len() > MAX_DELTA_INPUT {
        return None;
    }
    let mut index: HashMap<u64, Vec<usize>> = HashMap::with_capacity(base.len() / BLOCK + 1);
    for start in (0..=base.len() - BLOCK).step_by(BLOCK) {
        let c = index.entry(block_hash(&base[start..])).or_default();
        if c.len() < MAX_CANDIDATES {
            c.push(start);
        }
    }
    let mut out = Vec::with_capacity(target.len() / 4);
    varint(&mut out, base.len());
    varint(&mut out, target.len());
    // The budget: give up once the delta is no longer worth sending
    let limit = target.len() - target.len() / 8;
    let mut pending: Vec<u8> = Vec::new();
    let mut i = 0;
    while i < target.len() {
        let mut best: Option<(usize, usize, usize)> = None; // (base start, target start, length)
        if i + BLOCK <= target.len()
            && let Some(cands) = index.get(&block_hash(&target[i..]))
        {
            for &b in cands {
                if base[b..b + BLOCK] != target[i..i + BLOCK] {
                    continue;
                }
                let mut len = BLOCK;
                while b + len < base.len() && i + len < target.len() && base[b + len] == target[i + len] {
                    len += 1;
                }
                // Extend backwards into bytes waiting to be inserted
                let mut back = 0;
                while back < pending.len() && back < b && base[b - back - 1] == target[i - back - 1] {
                    back += 1;
                }
                if best.is_none_or(|(_, _, l)| len + back > l) {
                    best = Some((b - back, i - back, len + back));
                }
            }
        }
        match best {
            Some((b, t, len)) => {
                pending.truncate(pending.len() - (i - t));
                flush_insert(&mut out, &pending);
                pending.clear();
                copy(&mut out, b, len);
                i = t + len;
            }
            None => {
                pending.push(target[i]);
                i += 1;
            }
        }
        if out.len() + pending.len() > limit {
            return None;
        }
    }
    flush_insert(&mut out, &pending);
    (out.len() <= limit).then_some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unpack::apply_delta;

    fn roundtrip(base: &[u8], target: &[u8]) -> Option<usize> {
        let d = delta(base, target)?;
        assert_eq!(apply_delta(base, &d, u64::MAX).expect("apply"), target);
        Some(d.len())
    }

    /// A deterministic pseudo-random byte stream.
    fn noise(seed: u64, n: usize) -> Vec<u8> {
        let mut x = seed | 1;
        (0..n)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    }

    #[test]
    fn small_edits_make_small_deltas() {
        let base: Vec<u8> = (0..2000).flat_map(|i| format!("line {i}\n").into_bytes()).collect();
        let mut target = base.clone();
        target.splice(5000..5010, b"an edit in the middle".iter().copied());
        target.extend_from_slice(b"and a new last line\n");
        let n = roundtrip(&base, &target).expect("a delta");
        assert!(n < 100, "{n} bytes");
        // Inserted at the very start, and deleted at the end
        let mut t2 = b"new first line\n".to_vec();
        t2.extend_from_slice(&base[..base.len() - 300]);
        assert!(roundtrip(&base, &t2).expect("a delta") < 100);
    }

    #[test]
    fn unrelated_or_tiny_content_is_sent_whole() {
        assert_eq!(delta(&noise(1, 4096), &noise(2, 4096)), None);
        assert_eq!(delta(b"short", b"short too"), None);
        assert_eq!(delta(&noise(1, 100), b"tiny"), None);
    }

    #[test]
    fn long_copies_and_offsets_are_encoded() {
        // A copy longer than 0x10000 and one of exactly 0x10000, at offsets needing 3 bytes
        let base = noise(7, 0x30000);
        let mut target = base[0x12345..0x12345 + 0x10000].to_vec();
        target.extend_from_slice(b"--");
        target.extend_from_slice(&base[0x100..0x100 + 0x18000]);
        assert!(roundtrip(&base, &target).expect("a delta") < 64);
        // Repeated content in the base keeps several candidates
        let rep: Vec<u8> = b"abcdefghijklmnop".repeat(64);
        let mut t = rep.clone();
        t.extend_from_slice(b"tail");
        assert!(roundtrip(&rep, &t).is_some());
    }

    #[test]
    fn copies_over_the_three_byte_limit_split() {
        let mut out = Vec::new();
        copy(&mut out, 0, MAX_COPY + 10);
        let mut d = Vec::new();
        varint(&mut d, MAX_COPY + 10);
        varint(&mut d, MAX_COPY + 10);
        d.extend_from_slice(&out);
        let base = vec![5u8; MAX_COPY + 10];
        assert_eq!(apply_delta(&base, &d, u64::MAX).expect("apply"), base);
    }
}
