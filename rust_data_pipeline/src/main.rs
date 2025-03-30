use flate2::read::GzDecoder;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use postgres::binary_copy::BinaryCopyInWriter;
use postgres::types::Type;
use postgres::{Client, NoTls};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::str::FromStr;
use time::format_description::BorrowedFormatItem;
use time::macros::format_description;
use time::{OffsetDateTime, UtcDateTime, UtcOffset};

// Date-time formats:
const METEO_ARCHIVE_FILENAME_DATETIME_FORMAT: &[BorrowedFormatItem] =
    format_description!("[year]-[month]-[day]T[hour]-[minute]-[second]");
const ARSO_XML_DATETIME_FORMAT: &[BorrowedFormatItem] =
    format_description!("[day].[month].[year] [hour padding:none]:[minute] UTC");

#[derive(Debug, Deserialize, Serialize, Default)]
struct MetData {
    location_code: String,

    #[serde(serialize_with = "time::serde::iso8601::option::serialize")]
    ts_interval_end: Option<OffsetDateTime>,

    #[serde(serialize_with = "time::serde::iso8601::option::serialize")]
    ts_interval_start: Option<OffsetDateTime>,

    #[serde(serialize_with = "time::serde::iso8601::option::serialize")]
    ts_downloaded: Option<OffsetDateTime>,

    #[serde(default)]
    precipitation_sum_10min: Option<f32>,

    #[serde(default)]
    precipitation_sum_1h: Option<f32>,

    #[serde(default)]
    precipitation_sum_12h: Option<f32>,

    #[serde(default)]
    precipitation_sum_24h: Option<f32>,

    #[serde(default)]
    temperature_air_avg: Option<f32>,
}

struct ThreadLocalBuffers {
    line: String,
}

impl ThreadLocalBuffers {
    fn new() -> Self {
        Self {
            line: String::new(),
        }
    }
}

fn list_files(dir: &str) -> Result<Vec<(String, OffsetDateTime)>, std::io::Error> {
    let files = std::fs::read_dir(dir)?
        .filter_map(|entry| {
            entry.ok().and_then(|e| {
                let file_name = e.file_name().into_string().ok()?;
                if file_name.starts_with("meteo_data_archive_")
                    && (file_name.ends_with(".json") || file_name.ends_with(".json.gz"))
                {
                    let path = e.path();
                    let basename = path.file_name()?.to_str()?;
                    let timestamp = basename
                        .replace("meteo_data_archive_", "")
                        .replace(".json.gz", "")
                        .replace(".json", "");
                    let filename_first_part = timestamp.split('.').next()?;
                    let downloaded_datetime = UtcDateTime::parse(
                        &filename_first_part,
                        METEO_ARCHIVE_FILENAME_DATETIME_FORMAT,
                    )
                    .ok()?
                    .to_offset(UtcOffset::UTC);
                    path.into_os_string()
                        .into_string()
                        .ok()
                        .map(|p| (p, downloaded_datetime))
                } else {
                    None
                }
            })
        })
        .collect();
    Ok(files)
}

fn extract_datetime(
    reader: &mut quick_xml::Reader<&[u8]>,
    e: &quick_xml::events::BytesStart,
) -> Option<OffsetDateTime> {
    let s = reader.read_text(e.name()).unwrap();
    let parsed = UtcDateTime::parse(s.as_ref(), ARSO_XML_DATETIME_FORMAT);
    if !parsed.is_ok() {
        println!("Error parsing date: '{}'", s);
    }
    parsed.map(|v| v.to_offset(UtcOffset::UTC)).ok()
}

fn extract_f32(
    reader: &mut quick_xml::Reader<&[u8]>,
    e: &quick_xml::events::BytesStart,
) -> Option<f32> {
    let s = reader.read_text(e.name()).unwrap();
    f32::from_str(s.as_ref()).ok()
}

fn extract_xml_streaming(xml: &str, ts_downloaded: &OffsetDateTime, buf: &mut Vec<MetData>) {
    let mut reader = quick_xml::Reader::from_str(xml);
    let mut met_data = MetData::default();
    loop {
        match reader.read_event() {
            Ok(quick_xml::events::Event::Start(ref e))
                if e.name() == quick_xml::name::QName(b"metData") =>
            {
                met_data = MetData::default();
                met_data.ts_downloaded = Some(ts_downloaded.clone());
            }
            Ok(quick_xml::events::Event::Start(ref e))
                if e.name() == quick_xml::name::QName(b"domain_meteosiId") =>
            {
                met_data.location_code = reader.read_text(e.name()).unwrap().to_string();
            }
            Ok(quick_xml::events::Event::Start(ref e))
                if e.name() == quick_xml::name::QName(b"rr_val") =>
            {
                met_data.precipitation_sum_10min = extract_f32(&mut reader, e);
            }
            Ok(quick_xml::events::Event::Start(ref e))
                if e.name() == quick_xml::name::QName(b"rr_val") =>
            {
                met_data.precipitation_sum_10min = extract_f32(&mut reader, e);
            }
            Ok(quick_xml::events::Event::Start(ref e))
                if e.name() == quick_xml::name::QName(b"tp_1h_acc") =>
            {
                met_data.precipitation_sum_1h = extract_f32(&mut reader, e);
            }
            Ok(quick_xml::events::Event::Start(ref e))
                if e.name() == quick_xml::name::QName(b"tp_12h_acc") =>
            {
                met_data.precipitation_sum_12h = extract_f32(&mut reader, e);
            }
            Ok(quick_xml::events::Event::Start(ref e))
                if e.name() == quick_xml::name::QName(b"tp_24h_acc") =>
            {
                met_data.precipitation_sum_24h = extract_f32(&mut reader, e);
            }
            Ok(quick_xml::events::Event::Start(ref e))
                if e.name() == quick_xml::name::QName(b"tavg") =>
            {
                met_data.temperature_air_avg = extract_f32(&mut reader, e);
            }
            Ok(quick_xml::events::Event::Start(ref e))
                if e.name() == quick_xml::name::QName(b"validStart") =>
            {
                met_data.ts_interval_start = extract_datetime(&mut reader, e);
            }
            Ok(quick_xml::events::Event::Start(ref e))
                if e.name() == quick_xml::name::QName(b"validEnd") =>
            {
                met_data.ts_interval_end = extract_datetime(&mut reader, e);
            }
            Ok(quick_xml::events::Event::End(ref e))
                if e.name() == quick_xml::name::QName(b"metData") =>
            {
                buf.push(met_data);
                met_data = MetData::default();
            }
            Ok(quick_xml::events::Event::Eof) => break,
            _ => (),
        }
    }
}

fn extract_xml_from_json_line_streaming(
    line: &str,
    file_name: &str,
    buf: &mut Vec<MetData>,
    ts_downloaded: &OffsetDateTime,
) {
    let parsed_json: Option<serde_json::Value> = serde_json::from_str(line).ok();
    if let Some(v) = parsed_json {
        let xml_str = v["xml"].as_str().unwrap();
        extract_xml_streaming(xml_str, ts_downloaded, buf);
    } else {
        println!("Error parsing JSON line in file: {}", file_name);
    }
}

fn read_met_data_from_file_streaming(
    file_name: &str,
    downloaded_datetime: &OffsetDateTime,
    buffers: &mut ThreadLocalBuffers,
) -> Result<Vec<MetData>, std::io::Error> {
    let file = File::open(file_name)?;
    let reader: &mut dyn BufRead = if file_name.ends_with(".json") {
        &mut BufReader::new(file)
    } else {
        &mut BufReader::new(GzDecoder::new(&file))
    };

    let mut all_data = vec![];
    while let Ok(line) = reader.read_line(&mut buffers.line) {
        if line == 0 {
            break;
        }
        extract_xml_from_json_line_streaming(
            &buffers.line,
            file_name,
            &mut all_data,
            downloaded_datetime,
        );
        buffers.line.clear();
    }

    Ok(all_data)
}

fn main_parallel(files: &Vec<(String, OffsetDateTime)>, out_csv_path: &str) {
    let m = MultiProgress::new();
    let sty = ProgressStyle::with_template(
        "[{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>4}/{len:4} {msg}",
    )
    .unwrap()
    .progress_chars("##-");

    let global_pb = m.add(ProgressBar::new(files.len() as u64));
    global_pb.set_style(sty);
    global_pb.set_message("All Files");

    let all_met_data: Vec<Vec<MetData>> = files
        .par_iter()
        .map_init(
            || ThreadLocalBuffers::new(),
            |buffers, (filename, downloaded_datetime)| {
                let result =
                    read_met_data_from_file_streaming(filename, downloaded_datetime, buffers).ok();
                global_pb.inc(1);
                result
            },
        )
        .filter_map(|v| v)
        .collect();
    global_pb.finish_with_message("All files loaded.");

    println!(
        "All data count: {}",
        all_met_data.iter().map(|l| l.len()).sum::<usize>()
    );

    let mut all_data_by_timestamp_and_location: std::collections::HashMap<
        (String, OffsetDateTime),
        MetData,
    > = std::collections::HashMap::new();

    for met_data in all_met_data.into_iter().flat_map(|l| l.into_iter()) {
        if let Some(ts_interval_end) = met_data.ts_interval_end {
            let key = (met_data.location_code.clone(), ts_interval_end);
            let existing = all_data_by_timestamp_and_location.get(&key);
            if existing.is_none() {
                all_data_by_timestamp_and_location.insert(key, met_data);
            } else {
                let existing_data = existing.unwrap();
                if existing_data.ts_downloaded < met_data.ts_downloaded {
                    all_data_by_timestamp_and_location.insert(key, met_data);
                }
            }
        }
    }

    println!(
        "All data count: {}",
        all_data_by_timestamp_and_location.len()
    );

    let mut values = all_data_by_timestamp_and_location
        .values()
        .collect::<Vec<_>>();
    values.sort_by_key(|v| v.ts_interval_end);
    // let mut wtr = csv::Writer::from_path(out_csv_path).unwrap();
    // for met_data in &values {
    //     wtr.serialize(met_data).expect("Error writing CSV");
    // }

    let mut client = Client::connect(
        "host=localhost port=26433 user=slo_weather password=slo_weather dbname=slo_weather",
        NoTls,
    )
    .unwrap();

    client
        .batch_execute("DROP TABLE IF EXISTS measurements_v2;")
        .expect("Error dropping table in database");

    client
        .batch_execute(
            "
create table measurements_v2
(
    location_code         text        not null,
    ts_interval_end       TIMESTAMP WITH TIME ZONE not null,
    ts_interval_start     TIMESTAMP WITH TIME ZONE not null,
    ts_downloaded         TIMESTAMP WITH TIME ZONE not null,
    precipitation_sum_10m REAL,
    precipitation_sum_1h  REAL,
    precipitation_sum_12h REAL,
    precipitation_sum_24h REAL,
    temperature_air_avg   REAL
);
",
        )
        .expect("Error creating table in database");
    client
        .batch_execute(
            "
create unique index measurements_v2_location_code_ts_interval_end_uindex
    on measurements_v2 (location_code, ts_interval_end);
",
        )
        .expect("Error creating index in database");


    let pgsql_pb = m.add(ProgressBar::new(values.len() as u64));
    pgsql_pb.set_style(ProgressStyle::with_template(
        "[{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>7}/{len:7} {msg}",
    )
        .unwrap()
        .progress_chars("##-"));
    pgsql_pb.set_message("Loading into DB");

    let writer = client
        .copy_in("COPY measurements_v2 FROM STDIN BINARY")
        .unwrap();

    let mut writer = BinaryCopyInWriter::new(
        writer,
        &[
            Type::TEXT,
            Type::TIMESTAMPTZ,
            Type::TIMESTAMPTZ,
            Type::TIMESTAMPTZ,
            Type::FLOAT4,
            Type::FLOAT4,
            Type::FLOAT4,
            Type::FLOAT4,
            Type::FLOAT4,
        ],
    );
    for met_data in values {
        writer
            .write(&[
                &met_data.location_code,
                &met_data.ts_interval_end,
                &met_data.ts_interval_start,
                &met_data.ts_downloaded,
                &met_data.precipitation_sum_10min,
                &met_data.precipitation_sum_1h,
                &met_data.precipitation_sum_12h,
                &met_data.precipitation_sum_24h,
                &met_data.temperature_air_avg,
            ])
            .unwrap();
        pgsql_pb.inc(1);
    }
    writer.finish().unwrap();
    pgsql_pb.finish_with_message("All data loaded into DB.");
}

fn main() {
    let data_dir = "/home/drola/work/slo-weather/data";
    let out_csv_path = "/home/drola/work/slo-weather/rust_data_pipeline/out.csv";
    let files = list_files(data_dir).unwrap();

    // rayon::ThreadPoolBuilder::new().num_threads(1).build_global().unwrap();
    main_parallel(&files, out_csv_path);

    // TODO: Intern strings https://docs.rs/internment/0.3.6/internment/index.html
}
