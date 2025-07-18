use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashMap;

use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Deserialize)]
pub struct BookTickerUpdate {
    pub stream: String,
    pub data: BookTickerUpdateData,
}

#[derive(Deserialize)]
pub struct BookTickerUpdateData {
    pub e: String,
    #[serde(rename = "E")]
    pub e2: i64,
    pub s: String,
    pub t: i64,
    pub p: String,
    pub q: String,
    #[serde(rename = "T")]
    pub t2: i64,
    pub m: bool,
}

#[tokio::main]
async fn main() {
    
    let url_str =
        String::from("wss://fstream.binance.com/stream?streams=ethusdt@trade");
        
    let (ws_stream, _) = connect_async(url_str).await.expect("Failed to connect");
    let (_write, mut read) = ws_stream.split();

    let bar_interval_ns = 60_000_000_000u128;
    let mut current_bar_start: Option<u128> = None;
    let mut current_bar_stamps: Vec<BookTickerUpdate> = Vec::new();

    while let Some(message) = read.next().await {
        
        let now = SystemTime::now();
        let duration_since_epoch = now.duration_since(UNIX_EPOCH).unwrap();
        let timestamp_nanos = duration_since_epoch.as_nanos();

        let bar_start = (timestamp_nanos / bar_interval_ns) * bar_interval_ns;

        match message {
            Ok(Message::Text(text)) => {
                let tick_data: Value = serde_json::from_str(&text).unwrap();
                let msg_type = &tick_data["data"]["e"];

                if msg_type == "trade" {
                    let book_stamp: BookTickerUpdate = serde_json::from_str(&text).unwrap();
                    
                    current_bar_stamps.push(book_stamp);
                    
                    if let Some(prev_bar_start) = current_bar_start {
                        if bar_start != prev_bar_start {
                            let fpb = process_footprint_bar(&current_bar_stamps);
                            current_bar_stamps.clear();

                            println!("FootPrint bar: \nTimeStamp close: {}\nOpen: {}\nHigh: {}\nLow: {}\nClose: {}\nVolume: {}\nFootPrint: {}\n", timestamp_nanos, fpb.open, fpb.high, fpb.low, fpb.close, fpb.volume, fpb.footprint_data);
                        }
                    }

                    current_bar_start = Some(bar_start);
                }
            }
            Ok(Message::Ping(_))
            | Ok(Message::Pong(_))
            | Ok(Message::Binary(_))
            | Ok(Message::Frame(_)) => {}
            Ok(Message::Close(_)) => {
                println!("Connection closed.");
                return;
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }
    }
}

#[derive(Debug)]
struct FootprintBar {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    footprint_data: String,
}

fn process_footprint_bar(book_stamp_vec: &Vec<BookTickerUpdate>) -> FootprintBar {

    if book_stamp_vec.is_empty() {
        return FootprintBar {
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            volume: 0.0,
            footprint_data: "{}".to_string(),
        };
    }

    let mut footprint_map: HashMap<String, (f64, f64)> = HashMap::new();

    let first_price = book_stamp_vec[0].data.p.parse::<f64>().unwrap_or(0.0);
    let last_price = book_stamp_vec[book_stamp_vec.len() - 1].data.p.parse::<f64>().unwrap_or(0.0);
    
    let mut high = first_price;
    let mut low = first_price;
    let mut total_volume = 0f64;

    for stamp in book_stamp_vec {

        let price = stamp.data.p.parse::<f64>().unwrap_or(0.0);
        let size = stamp.data.q.parse::<f64>().unwrap_or(0.0);

        // println!("Trade: price={}, size={}, m={}", price, size, stamp.data.m);

        if price > high { high = price; } 
        if price < low { low = price; }

        total_volume += size;

        let price_key = format!("{:.4}", price);
        let entry = footprint_map.entry(price_key).or_insert((0.0, 0.0));

        if stamp.data.m {
            // m = true: buyer is market maker (passive), so seller was aggressive
            entry.1 += size; // Sell side
        } else {
            // m = false: buyer is market taker (aggressive)
            entry.0 += size; // Buy side
        }
    }

    let footprint_json = serde_json::to_string(&footprint_map).unwrap_or_else(|_| "{}".to_string());

    FootprintBar {
        open: first_price,
        high,
        low,
        close: last_price,
        volume: total_volume,
        footprint_data: footprint_json,
    }
}
