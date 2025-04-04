use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use postgres::{Client, NoTls};
use serde::Serialize;
use std::fs::File;
use std::io::{BufWriter, Write};
use time::OffsetDateTime;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DataDescriptor {
    stations: Vec<String>,
    #[serde(with = "time::serde::iso8601")]
    start: OffsetDateTime,
    #[serde(with = "time::serde::iso8601")]
    end: OffsetDateTime,
    columns: usize,
    rows: usize,
}

fn main_parallel() {
    let m = MultiProgress::new();

    let mut client = Client::connect(
        "host=localhost port=26433 user=slo_weather password=slo_weather dbname=slo_weather",
        NoTls,
    )
    .unwrap();

    let row = client.query_one("SELECT MIN(ts_interval_end), MAX(ts_interval_end) FROM measurements_v2 WHERE precipitation_sum_1h IS NOT NULL", &[]).unwrap();
    let min_ts: OffsetDateTime = row.get(0);
    let max_ts: OffsetDateTime = row.get(1);
    println!("From: {:?}", min_ts);
    println!("To: {:?}", max_ts);
    let hours: u64 = ((max_ts - min_ts).as_seconds_f64() / 3600.0).round() as u64;
    println!("Count (hours): {}", hours);

    let stations = client
        .query("SELECT DISTINCT(location_code) from measurements_v2", &[])
        .unwrap()
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect::<Vec<_>>();
    println!("Stations: {:?}", stations);

    let data_descriptor = DataDescriptor {
        stations: stations.clone(),
        start: min_ts,
        end: max_ts,
        columns: 128,
        rows: hours as usize,
    };

    let pgsql_pb = m.add(ProgressBar::new(hours));
    pgsql_pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>7}/{len:7} {msg}",
        )
        .unwrap()
        .progress_chars("##-"),
    );
    pgsql_pb.set_message("Reading from DB");

    let mut precipitation = vec![f32::NAN; data_descriptor.rows * data_descriptor.columns];
    let mut temperature = vec![f32::NAN; data_descriptor.rows * data_descriptor.columns];

    let rows_ = client.query("SELECT location_code, ts_interval_end, precipitation_sum_1h, temperature_air_avg FROM measurements_v2 ORDER BY ts_interval_end ASC", &[]).unwrap();
    pgsql_pb.set_message("Processing data");
    for row in rows_ {
        let location_code: String = row.get(0);
        let ts_interval_end: OffsetDateTime = row.get(1);
        let precipitation_sum_1h: Option<f32> = row.get(2);
        let temperature_air_avg: Option<f32> = row.get(3);

        let index = ((ts_interval_end - min_ts).as_seconds_f64() / 3600.0) as usize;
        if index >= data_descriptor.rows {
            continue;
        }
        pgsql_pb.set_position(index as u64);

        let station_index = stations.iter().position(|s| s == &location_code).unwrap();
        if let Some(precipitation_sum_1h) = precipitation_sum_1h {
            precipitation[index * data_descriptor.columns + station_index] =
                precipitation_sum_1h;
        }
        if let Some(temperature_air_avg) = temperature_air_avg {
            temperature[index * data_descriptor.columns + station_index] =
                temperature_air_avg;
        }
    }

    pgsql_pb.finish_with_message("All data dumped.");

    let json = serde_json::to_string_pretty(&data_descriptor).unwrap();

    let mut file = File::create("data_descriptor.json").unwrap();
    std::io::Write::write_all(&mut file, json.as_bytes()).unwrap();

    pgsql_pb.set_message("Writing precipitation.bin");
    let mut writer = BufWriter::new(File::create("precipitation.bin").unwrap());
    for nr in precipitation {
        writer.write_all(&nr.to_le_bytes()).unwrap();
    }
    writer.flush().unwrap();

    pgsql_pb.set_message("Writing temperatures.bin");
    let mut writer = BufWriter::new(File::create("temperature.bin").unwrap());
    for nr in temperature {
        writer.write_all(&nr.to_le_bytes()).unwrap();
    }
    writer.flush().unwrap();
}

fn main() {
    main_parallel();
}
