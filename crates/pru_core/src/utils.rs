use byteorder::{LittleEndian as LE, ReadBytesExt, WriteBytesExt};
use std::io::{self, Read, Write};

pub fn crc32(data: &[u8]) -> u32 { crc32fast::hash(data) }

pub fn uvarint_encode(mut n: u64, out: &mut Vec<u8>) {
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

pub fn uvarint_decode(mut data: &[u8]) -> (u64, &[u8]) {
    let mut x = 0u64; let mut s = 0u32;
    loop {
        let b = data[0]; data = &data[1..];
        if b < 0x80 { return (x | ((b as u64) << s), data); }
        x |= ((b & 0x7F) as u64) << s; s += 7;
    }
}

pub fn write_u64<W: Write>(w: &mut W, v: u64) -> io::Result<()> { w.write_u64::<LE>(v) }
pub fn write_u32<W: Write>(w: &mut W, v: u32) -> io::Result<()> { w.write_u32::<LE>(v) }
pub fn read_u64<R: Read>(r: &mut R) -> io::Result<u64> { r.read_u64::<LE>() }
pub fn read_u32<R: Read>(r: &mut R) -> io::Result<u32> { r.read_u32::<LE>() }
