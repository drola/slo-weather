use flate2::read::GzDecoder;
use quick_xml::de::from_str;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io;
use std::io::BufRead;
use time::macros::format_description;
use time::{OffsetDateTime, UtcDateTime, UtcOffset};


#[derive(Debug, Deserialize)]
#[serde(rename = "data")]
struct WeatherData {
    language: String,
    credit: String,
    credit_url: String,
    image_url: String,
    suggested_pickup: String,
    suggested_pickup_period: u32,
    webcam_url_base: String,
    icon_url_base: String,
    icon_format: String,
    docs_url: String,
    disclaimer_url: String,
    copyright_url: String,
    privacy_policy_url: String,
    managing_editor: String,
    web_master: String,
    generator: String,
    meteosi_url: Option<String>,
    two_day_history_url: String,
    #[serde(rename = "metData")]
    met_data: Vec<MetData>,
}

#[derive(Debug, Deserialize, Serialize)]
struct MetData {
    #[serde(rename(deserialize = "domain_meteosiId"))]
    location_code: String,

    #[serde(
        deserialize_with = "time::serde::rfc2822::deserialize",
        serialize_with = "time::serde::iso8601::serialize",
        rename(deserialize = "tsValid_issued_RFC822")
    )]
    ts_interval_end: OffsetDateTime,

    #[serde(default, serialize_with = "time::serde::iso8601::option::serialize")]
    ts_downloaded: Option<OffsetDateTime>,

    #[serde(
        default,
        deserialize_with = "deserialize_empty_option_f32",
        rename(deserialize = "rr_val")
    )]
    precipitation_sum_10min: Option<f32>,

    #[serde(
        default,
        deserialize_with = "deserialize_empty_option_f32",
        rename(deserialize = "tavg")
    )]
    temperature_air_avg: Option<f32>,
}

fn deserialize_empty_option_f32<'de, D>(deserializer: D) -> Result<Option<f32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::deserialize(deserializer)?.and_then(|s: &str| s.parse().ok()))
}

fn list_files(dir: &str) -> Result<Vec<(String, UtcDateTime)>, std::io::Error> {
    // Example filename: meteo_data_archive_2025-03-15T17-04-02.835121+00-00.json.gz
    let my_format = format_description!("[year]-[month]-[day]T[hour]-[minute]-[second]");

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
                    let downloaded_datetime =
                        UtcDateTime::parse(&filename_first_part, my_format).unwrap();
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

fn extract_xml_from_json_line(line: &str, file_name: &str) -> Vec<MetData> {
    let parsed_json: Option<serde_json::Value> = serde_json::from_str(line).ok();
    if let Some(v) = parsed_json {
        let xml_str = v["xml"].as_str().unwrap();
        let met_data = from_str::<WeatherData>(xml_str).unwrap().met_data;
        return met_data;
    } else {
        println!("Error parsing JSON line in file: {}", file_name);
    }

    vec![]
}

fn read_met_data_from_file(
    file_name: &str,
    downloaded_datetime: &UtcDateTime,
) -> Result<Vec<MetData>, std::io::Error> {
    let file = File::open(file_name)?;
    println!("{}", file_name);

    let buf_reader: &mut dyn BufRead = if file_name.ends_with(".json") {
        &mut io::BufReader::new(file)
    } else {
        &mut io::BufReader::new(GzDecoder::new(io::BufReader::new(file)))
    };

    let mut line = String::new();
    let mut all_data = vec![];
    while (buf_reader.read_line(&mut line)?) != 0 {
        let mut data_from_line = extract_xml_from_json_line(&line, file_name);
        for mut met_data in data_from_line.iter_mut() {
            met_data.ts_downloaded = Some(downloaded_datetime.to_offset(UtcOffset::UTC));
        }
        all_data.append(&mut data_from_line);
        line.clear();
    }

    Ok(all_data)
}

fn main_parallel(files: &Vec<(String, UtcDateTime)>, out_csv_path: &str) {
    let all_met_data: Vec<Vec<MetData>> = files
        .par_iter()
        .filter_map(|(filename, downloaded_datetime)| {
            read_met_data_from_file(filename, downloaded_datetime).ok()
        })
        .collect();

    println!(
        "All data count: {}",
        all_met_data.iter().map(|l| l.len()).sum::<usize>()
    );

    let mut all_data_by_timestamp_and_location: std::collections::HashMap<
        (String, OffsetDateTime),
        MetData,
    > = std::collections::HashMap::new();

    for met_data in all_met_data.into_iter().flat_map(|l| l.into_iter()) {
        let key = (
            met_data.location_code.clone(),
            met_data.ts_interval_end.clone(),
        );
        let existing = all_data_by_timestamp_and_location.get(&key);
        if existing.is_none() {
            all_data_by_timestamp_and_location.insert(key, met_data);
        } else {
            let existing_data = existing.unwrap();
            if (existing_data.ts_downloaded < met_data.ts_downloaded) {
                all_data_by_timestamp_and_location.insert(key, met_data);
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
    let mut wtr = csv::Writer::from_path(out_csv_path).unwrap();
    for met_data in values {
        wtr.serialize(met_data);
    }
}

fn main() {
    let data_dir = "/home/drola/work/slo-weather/data";
    let out_csv_path = "/home/drola/work/slo-weather/rust_data_pipeline/out.csv";
    let files = list_files(data_dir).unwrap();
    main_parallel(&files, out_csv_path);
}
