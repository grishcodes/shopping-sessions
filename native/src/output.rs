//! Hand-rolled minimal Parquet writer (P9).
//!
//! Replaces parquet-rs for the two output tables. Emits PLAIN encoding, one
//! row group per file, no statistics, no dictionary, one uncompressed Data
//! Page v1 per column, writing directly from the `UserDay` / `Credit`
//! slices into raw-byte scratch buffers — no per-column `Vec<T>`
//! materialization, no schema parsing, no per-file writer setup.
//!
//! Format (verified against parquet-format / parquet-rs 54.3.1 sources):
//!   `PAR1` + column chunks (page header + page payload each) + Thrift
//!   compact-protocol `FileMetaData` + u32 LE footer length + `PAR1`.
//! A DataPageV1 payload is `[rep levels][def levels][PLAIN values]`; the
//! level sections are RLE/bit-packed-hybrid streams with the standard
//! 4-byte little-endian length prefix, present only when the column's max
//! level > 0 (so required columns have no level section at all).
//! `definition_levels_byte_length` is a DataPageV2-only field and is NOT
//! present here. `data_page_offset` is the offset of the page header;
//! column `total_*_size` values include page headers, matching parquet-rs
//! `ColumnWriter` metrics; `RowGroup.total_byte_size` is their sum.
//! Schema carries both `converted_type` (UTF8=0, DATE=6) and `logicalType`
//! (STRING field 1 / DATE field 6), as parquet-rs does.

use crate::{
    input::{Key, Names},
    tables::{Credit, UserDay},
};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

// Thrift compact-protocol type ids.
const CT_I32: u8 = 5;
const CT_I64: u8 = 6;
const CT_BINARY: u8 = 8;
const CT_LIST: u8 = 9;
const CT_STRUCT: u8 = 12;

// Parquet thrift enum values (parquet-format).
const TYPE_INT32: i32 = 1;
const TYPE_INT64: i32 = 2;
const TYPE_BYTE_ARRAY: i32 = 6;
const ENCODING_PLAIN: i32 = 0;
const ENCODING_RLE: i32 = 3;
const CODEC_UNCOMPRESSED: i32 = 0;
const PAGE_DATA: i32 = 0;
const REP_REQUIRED: i32 = 0;
const REP_OPTIONAL: i32 = 1;
const CONV_UTF8: i32 = 0;
const CONV_DATE: i32 = 6;

/// Minimal Thrift compact-protocol struct writer. Each struct gets its own
/// writer (field ids must be written in ascending order); nested structs
/// are embedded as finished byte buffers.
struct W {
    b: Vec<u8>,
    prev: i16,
}

impl W {
    fn new() -> Self {
        Self {
            b: Vec::new(),
            prev: 0,
        }
    }

    fn stop(&mut self) {
        self.b.push(0);
    }

    fn hdr(&mut self, id: i16, ctype: u8) {
        let delta = id - self.prev;
        if (1..=15).contains(&delta) {
            self.b.push(((delta as u8) << 4) | ctype);
        } else {
            // Long form: delta 0, then the field id as a zigzag varint.
            self.b.push(ctype);
            self.zz(i64::from(id));
        }
        self.prev = id;
    }

    fn uv(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }

    fn zz(&mut self, v: i64) {
        self.uv(((v << 1) ^ (v >> 63)) as u64);
    }

    fn i32(&mut self, id: i16, v: i32) {
        self.hdr(id, CT_I32);
        self.zz(i64::from(v));
    }

    fn i64(&mut self, id: i16, v: i64) {
        self.hdr(id, CT_I64);
        self.zz(v);
    }

    fn bytes(&mut self, id: i16, v: &[u8]) {
        self.hdr(id, CT_BINARY);
        self.uv(v.len() as u64);
        self.b.extend_from_slice(v);
    }

    fn list_hdr(&mut self, id: i16, elem: u8, n: usize) {
        self.hdr(id, CT_LIST);
        if n < 15 {
            self.b.push(((n as u8) << 4) | elem);
        } else {
            self.b.push(0xF0 | elem);
            self.uv(n as u64);
        }
    }

    /// Embed a finished struct as a field value.
    fn strukt(&mut self, id: i16, inner: &W) {
        self.hdr(id, CT_STRUCT);
        self.b.extend_from_slice(&inner.b);
    }
}

fn schema_element(
    ty: Option<i32>,
    rep: Option<i32>,
    name: &str,
    num_children: Option<i32>,
    converted: Option<i32>,
    logical_field: Option<i16>,
) -> W {
    let mut w = W::new();
    if let Some(t) = ty {
        w.i32(1, t);
    }
    if let Some(r) = rep {
        w.i32(3, r);
    }
    w.bytes(4, name.as_bytes());
    if let Some(n) = num_children {
        w.i32(5, n);
    }
    if let Some(c) = converted {
        w.i32(6, c);
    }
    if let Some(field) = logical_field {
        // LogicalType union: one empty struct under the logical field id.
        let mut inner = W::new();
        inner.stop();
        let mut lt = W::new();
        lt.strukt(field, &inner);
        lt.stop();
        w.strukt(10, &lt);
    }
    w.stop();
    w
}

fn leaf(
    ty: i32,
    rep: i32,
    name: &str,
    converted: Option<i32>,
    logical_field: Option<i16>,
) -> W {
    schema_element(Some(ty), Some(rep), name, None, converted, logical_field)
}

fn utf8_col(name: &str) -> W {
    leaf(TYPE_BYTE_ARRAY, REP_REQUIRED, name, Some(CONV_UTF8), Some(1))
}

fn date_col(name: &str) -> W {
    leaf(TYPE_INT32, REP_REQUIRED, name, Some(CONV_DATE), Some(6))
}

fn int64_col(name: &str) -> W {
    leaf(TYPE_INT64, REP_REQUIRED, name, None, None)
}

/// One column chunk's worth of metadata, collected while writing pages.
struct ColMeta {
    ty: i32,
    name: &'static str,
    num_values: i64,
    total_size: i64, // header + payload, uncompressed == compressed
    data_page_offset: i64,
    has_def_levels: bool,
}

impl ColMeta {
    fn thrift(&self) -> W {
        let mut m = W::new();
        m.i32(1, self.ty);
        m.list_hdr(2, CT_I32, if self.has_def_levels { 2 } else { 1 });
        m.zz(i64::from(ENCODING_PLAIN));
        if self.has_def_levels {
            m.zz(i64::from(ENCODING_RLE));
        }
        m.list_hdr(3, CT_BINARY, 1);
        m.uv(self.name.len() as u64);
        m.b.extend_from_slice(self.name.as_bytes());
        m.i32(4, CODEC_UNCOMPRESSED);
        m.i64(5, self.num_values);
        m.i64(6, self.total_size);
        m.i64(7, self.total_size);
        m.i64(9, self.data_page_offset);
        m.stop();
        m
    }
}

/// A single output file: `PAR1`, column chunks, compact footer, `PAR1`.
struct FileWriter {
    out: BufWriter<File>,
    /// File offset of the next byte to be written.
    pos: i64,
    cols: Vec<ColMeta>,
    row_group_start: i64,
}

impl FileWriter {
    fn create(path: &Path) -> Result<Self> {
        let mut out = BufWriter::with_capacity(1 << 20, File::create(path)?);
        out.write_all(b"PAR1")?;
        Ok(Self {
            out,
            pos: 4,
            cols: Vec::new(),
            row_group_start: 4,
        })
    }

    /// Write one PLAIN DataPageV1 column. `def_levels` is the complete
    /// RLE/bit-packed-hybrid level section INCLUDING its 4-byte length
    /// prefix (empty for required columns); `payload` is the PLAIN values.
    fn column(
        &mut self,
        ty: i32,
        name: &'static str,
        num_values: i64,
        def_levels: &[u8],
        payload: &[u8],
    ) -> Result<()> {
        let mut page = W::new();
        page.i32(1, PAGE_DATA);
        page.i32(2, (def_levels.len() + payload.len()) as i32);
        page.i32(3, (def_levels.len() + payload.len()) as i32);
        let mut dph = W::new();
        dph.i32(1, num_values as i32);
        dph.i32(2, ENCODING_PLAIN);
        dph.i32(3, ENCODING_RLE);
        dph.i32(4, ENCODING_RLE);
        dph.stop();
        page.strukt(5, &dph);
        page.stop();
        let header_len = page.b.len() as i64;
        let data_page_offset = self.pos;
        self.out.write_all(&page.b)?;
        self.out.write_all(def_levels)?;
        self.out.write_all(payload)?;
        self.pos += header_len + def_levels.len() as i64 + payload.len() as i64;
        self.cols.push(ColMeta {
            ty,
            name,
            num_values,
            total_size: header_len + def_levels.len() as i64 + payload.len() as i64,
            data_page_offset,
            has_def_levels: !def_levels.is_empty(),
        });
        Ok(())
    }

    /// Finish: write the footer and trailing magic. `schema` is the full
    /// flattened schema list (root first), `num_rows` the file's row count.
    fn finish(mut self, schema: Vec<W>, num_rows: i64) -> Result<()> {
        let total_byte_size: i64 = self.cols.iter().map(|c| c.total_size).sum();
        let mut rg = W::new();
        rg.list_hdr(1, CT_STRUCT, self.cols.len());
        for c in &self.cols {
            let mut chunk = W::new();
            chunk.i64(2, c.data_page_offset);
            chunk.strukt(3, &c.thrift());
            chunk.stop();
            rg.b.extend_from_slice(&chunk.b);
        }
        rg.i64(2, total_byte_size);
        rg.i64(3, num_rows);
        rg.i64(5, self.row_group_start);
        rg.i64(6, total_byte_size);
        rg.stop();

        let mut meta = W::new();
        meta.i32(1, 1); // format version
        meta.list_hdr(2, CT_STRUCT, schema.len());
        for s in &schema {
            meta.b.extend_from_slice(&s.b);
        }
        meta.i64(3, num_rows);
        if self.cols.is_empty() {
            // No row groups: an empty file still needs a valid footer.
            meta.list_hdr(4, CT_STRUCT, 0);
        } else {
            meta.list_hdr(4, CT_STRUCT, 1);
            meta.b.extend_from_slice(&rg.b);
        }
        meta.bytes(6, b"shopping-sessions hand-rolled 1.0");
        meta.stop();

        self.out.write_all(&meta.b)?;
        self.out.write_all(&(meta.b.len() as u32).to_le_bytes())?;
        self.out.write_all(b"PAR1")?;
        self.out.flush()?;
        Ok(())
    }
}

/// RLE/bit-packed-hybrid definition levels for a max level of 1, as one
/// bit-packed run: varint header `(groups << 1) | 1`, then `groups` bytes,
/// LSB-first. Prefixed with the 4-byte LE length of the hybrid stream.
/// `defs` is the per-row 0/1 definition level.
fn def_levels_rle(defs: &[u8]) -> Vec<u8> {
    if defs.is_empty() {
        // parquet-rs emits only the 4-byte zero length prefix for no values.
        return vec![0; 4];
    }
    let groups = defs.len().div_ceil(8);
    let mut stream = Vec::with_capacity(4 + groups + 4);
    stream.extend_from_slice(&[0; 4]); // length prefix, patched below
    // Varint header for a bit-packed run of `groups` groups of 8.
    let mut h = ((groups as u64) << 1) | 1;
    while h >= 0x80 {
        stream.push((h as u8) | 0x80);
        h >>= 7;
    }
    stream.push(h as u8);
    for chunk in defs.chunks(8) {
        let mut byte = 0u8;
        for (i, &d) in chunk.iter().enumerate() {
            byte |= d << i;
        }
        stream.push(byte);
    }
    let len = (stream.len() - 4) as u32;
    stream[..4].copy_from_slice(&len.to_le_bytes());
    stream
}

/// Append one little-endian i32 / i64 / length-prefixed byte string to a
/// raw-bytes scratch buffer.
fn push_i32(buf: &mut Vec<u8>, v: i32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn push_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn push_bytes(buf: &mut Vec<u8>, v: &[u8]) {
    buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
    buf.extend_from_slice(v);
}

pub fn users(
    path: &Path,
    shards: &[Vec<UserDay>],
    names: &Names,
    categories: &[Key],
) -> Result<()> {
    let total: usize = shards.iter().map(Vec::len).sum();
    // One row group per file. Single pass over the rows fills all six
    // column payloads; the file is then written column by column.
    let mut ids: Vec<u8> = Vec::with_capacity(total * 13);
    let mut days: Vec<u8> = Vec::with_capacity(total * 4);
    let mut sessions: Vec<u8> = Vec::with_capacity(total * 8);
    let mut events: Vec<u8> = Vec::with_capacity(total * 8);
    let mut revenue: Vec<u8> = Vec::with_capacity(total * 8);
    let mut tops: Vec<u8> = Vec::with_capacity(total * 6);
    let mut defs: Vec<u8> = Vec::with_capacity(total);
    let mut scratch = [0u8; 9];
    for batch in shards {
        for r in batch {
            push_bytes(&mut ids, names.bytes(r.user, &mut scratch));
            push_i32(&mut days, r.day);
            push_i64(&mut sessions, r.sessions);
            push_i64(&mut events, r.events);
            push_i64(&mut revenue, r.revenue);
            match r.top {
                Some(c) => {
                    defs.push(1);
                    push_bytes(
                        &mut tops,
                        names.bytes(categories[c as usize], &mut scratch),
                    );
                }
                None => defs.push(0),
            }
        }
    }
    let n = total as i64;
    let mut f = FileWriter::create(path)?;
    f.column(TYPE_BYTE_ARRAY, "user_id", n, &[], &ids)?;
    f.column(TYPE_INT32, "day", n, &[], &days)?;
    f.column(TYPE_INT64, "sessions", n, &[], &sessions)?;
    f.column(TYPE_INT64, "events", n, &[], &events)?;
    f.column(TYPE_INT64, "revenue_cents", n, &[], &revenue)?;
    let levels = def_levels_rle(&defs);
    f.column(TYPE_BYTE_ARRAY, "top_category", n, &levels, &tops)?;
    let schema = vec![
        schema_element(None, None, "users", Some(6), None, None),
        utf8_col("user_id"),
        date_col("day"),
        int64_col("sessions"),
        int64_col("events"),
        int64_col("revenue_cents"),
        leaf(
            TYPE_BYTE_ARRAY,
            REP_OPTIONAL,
            "top_category",
            Some(CONV_UTF8),
            Some(1),
        ),
    ];
    f.finish(schema, n)
}

pub fn campaigns(
    path: &Path,
    days: Vec<Credit>,
    day0: i32,
    n_days: usize,
    names: &Names,
    labels: &[(Key, Key)],
) -> Result<()> {
    // Count rows first (cheap: labels.len() * n_days cells, ~150k).
    let mut n = 0i64;
    for i in 0..labels.len() {
        for d in 0..n_days {
            let c = days[i * n_days + d];
            if c.sessions > 0 || c.assists > 0 {
                n += 1;
            }
        }
    }
    let mut ids: Vec<u8> = Vec::with_capacity(n as usize * 10);
    let mut channels: Vec<u8> = Vec::with_capacity(n as usize * 10);
    let mut day_col: Vec<u8> = Vec::with_capacity(n as usize * 4);
    let mut sess: Vec<u8> = Vec::with_capacity(n as usize * 8);
    let mut purch: Vec<u8> = Vec::with_capacity(n as usize * 8);
    let mut rev: Vec<u8> = Vec::with_capacity(n as usize * 8);
    let mut assist: Vec<u8> = Vec::with_capacity(n as usize * 8);
    let mut scratch = [0u8; 9];
    for i in 0..labels.len() {
        let (id, channel) = labels[i];
        for d in 0..n_days {
            let c = days[i * n_days + d];
            if c.sessions > 0 || c.assists > 0 {
                push_bytes(&mut ids, names.bytes(id, &mut scratch));
                push_bytes(&mut channels, names.bytes(channel, &mut scratch));
                push_i32(&mut day_col, day0 + d as i32);
                push_i64(&mut sess, c.sessions);
                push_i64(&mut purch, c.purchases);
                push_i64(&mut rev, c.revenue);
                push_i64(&mut assist, c.assists);
            }
        }
    }
    let mut f = FileWriter::create(path)?;
    f.column(TYPE_BYTE_ARRAY, "campaign_id", n, &[], &ids)?;
    f.column(TYPE_BYTE_ARRAY, "channel", n, &[], &channels)?;
    f.column(TYPE_INT32, "day", n, &[], &day_col)?;
    f.column(TYPE_INT64, "sessions", n, &[], &sess)?;
    f.column(TYPE_INT64, "purchasing_sessions", n, &[], &purch)?;
    f.column(TYPE_INT64, "revenue_cents", n, &[], &rev)?;
    f.column(TYPE_INT64, "assisted_sessions", n, &[], &assist)?;
    let schema = vec![
        schema_element(None, None, "campaigns", Some(7), None, None),
        utf8_col("campaign_id"),
        utf8_col("channel"),
        date_col("day"),
        int64_col("sessions"),
        int64_col("purchasing_sessions"),
        int64_col("revenue_cents"),
        int64_col("assisted_sessions"),
    ];
    f.finish(schema, n)
}
