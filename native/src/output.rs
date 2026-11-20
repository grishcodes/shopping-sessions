use crate::{
    input::{Key, Names},
    tables::{CampaignDays, UserDay},
};
use parquet::{
    data_type::{ByteArray, ByteArrayType, DataType, Int32Type, Int64Type},
    errors::Result,
    file::{
        properties::{EnabledStatistics, WriterProperties},
        writer::{SerializedFileWriter, SerializedRowGroupWriter},
    },
    schema::parser::parse_message_type,
};
use std::{fs::File, path::Path, sync::Arc};

const USERS: &str = "message users { REQUIRED BINARY user_id (UTF8); REQUIRED INT32 day (DATE);
 REQUIRED INT64 sessions; REQUIRED INT64 events; REQUIRED INT64 revenue_cents; OPTIONAL BINARY top_category (UTF8); }";
const CAMPAIGNS: &str = "message campaigns { REQUIRED BINARY campaign_id (UTF8); REQUIRED BINARY channel (UTF8);
 REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 purchasing_sessions; REQUIRED INT64 revenue_cents;
 REQUIRED INT64 assisted_sessions; }";

fn file(path: &Path, schema: &str) -> Result<SerializedFileWriter<File>> {
    let properties = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::None)
        .set_dictionary_enabled(false)
        .build();
    SerializedFileWriter::new(
        File::create(path)?,
        Arc::new(parse_message_type(schema)?),
        Arc::new(properties),
    )
}
fn column<D: DataType>(
    group: &mut SerializedRowGroupWriter<File>,
    values: Vec<D::T>,
    defs: Option<&[i16]>,
) -> Result<()> {
    let mut col = group.next_column()?.unwrap();
    col.typed::<D>().write_batch(&values, defs, None)?;
    col.close()?;
    Ok(())
}
fn string(names: &Names, id: Key) -> ByteArray {
    ByteArray::from(names.bytes(id, &mut [0; 9]).to_vec())
}
pub fn users(
    path: &Path,
    shards: &[Vec<UserDay>],
    names: &Names,
    categories: &[Key],
) -> Result<()> {
    let mut out = file(path, USERS)?;
    let labels: Vec<_> = categories.iter().map(|&k| string(names, k)).collect();
    for batch in shards.iter().flat_map(|rows| rows.chunks(65536)) {
        let mut g = out.next_row_group()?;
        column::<ByteArrayType>(
            &mut g,
            batch.iter().map(|r| string(names, r.user)).collect(),
            None,
        )?;
        column::<Int32Type>(&mut g, batch.iter().map(|r| r.day).collect(), None)?;
        column::<Int64Type>(&mut g, batch.iter().map(|r| r.sessions).collect(), None)?;
        column::<Int64Type>(&mut g, batch.iter().map(|r| r.events).collect(), None)?;
        column::<Int64Type>(&mut g, batch.iter().map(|r| r.revenue).collect(), None)?;
        let defs: Vec<_> = batch.iter().map(|r| i16::from(r.top.is_some())).collect();
        column::<ByteArrayType>(
            &mut g,
            batch
                .iter()
                .filter_map(|r| r.top.map(|c| labels[c as usize].clone()))
                .collect(),
            Some(&defs),
        )?;
        g.close()?;
    }
    out.close()?;
    Ok(())
}
pub fn campaigns(
    path: &Path,
    days: CampaignDays,
    names: &Names,
    labels: &[(Key, Key)],
) -> Result<()> {
    let mut out = file(path, CAMPAIGNS)?;
    let rows: Vec<_> = days.into_iter().collect();
    if !rows.is_empty() {
        let mut g = out.next_row_group()?;
        for field in [0, 1] {
            column::<ByteArrayType>(
                &mut g,
                rows.iter()
                    .map(|((i, _), _)| {
                        let (id, channel) = labels[*i as usize];
                        string(names, if field == 0 { id } else { channel })
                    })
                    .collect(),
                None,
            )?;
        }
        column::<Int32Type>(&mut g, rows.iter().map(|((_, d), _)| *d).collect(), None)?;
        for f in [
            |c: &crate::tables::Credit| c.sessions,
            |c: &crate::tables::Credit| c.purchases,
            |c: &crate::tables::Credit| c.revenue,
            |c: &crate::tables::Credit| c.assists,
        ] {
            column::<Int64Type>(&mut g, rows.iter().map(|(_, c)| f(c)).collect(), None)?;
        }
        g.close()?;
    }
    out.close()?;
    Ok(())
}
