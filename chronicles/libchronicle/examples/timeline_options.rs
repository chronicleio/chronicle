use libchronicle::chronicle::{Chronicle, ChronicleOptions};
use libchronicle::{Event, TimelineOptions, Writer};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = Arc::new(
        catalog::build_catalog(&catalog::CatalogOptions::default()).await?,
    );

    // Chronicle with custom connection options
    let chronicle = Chronicle::new(
        catalog,
        ChronicleOptions::new()
            .conns_per_unit(8)
            .connect_timeout(Duration::from_secs(10))
            .request_timeout(Duration::from_secs(60))
            .keep_alive_interval(Duration::from_secs(15))
            .keep_alive_timeout(Duration::from_secs(10)),
    );

    // Timeline with replication
    let mut replicated = chronicle
        .open_timeline(
            "replicated-timeline",
            TimelineOptions::new().replication_factor(3),
        )
        .await?;
    println!("opened replicated timeline (rf=3)");

    // Timeline with schema and compaction
    let mut compacted = chronicle
        .open_timeline(
            "compacted-timeline",
            TimelineOptions::new()
                .replication_factor(1)
                .schema_id("org.example.Order/v1")
                .compaction(true),
        )
        .await?;
    println!("opened compacted timeline with schema");

    // Timeline with retention
    let mut retained = chronicle
        .open_timeline(
            "retained-timeline",
            TimelineOptions::new()
                .replication_factor(1)
                .retention(Duration::from_secs(7 * 24 * 3600)),
        )
        .await?;
    println!("opened retained timeline (7 days)");

    // Timeline with custom batching
    let mut fast = chronicle
        .open_timeline(
            "fast-timeline",
            TimelineOptions::new()
                .replication_factor(1)
                .max_batch_size(1024)
                .linger(Duration::from_millis(1)),
        )
        .await?;
    println!("opened fast timeline (batch=1024, linger=1ms)");

    // Write to each
    replicated.record(Event::new(b"replicated".to_vec())).await?;
    compacted
        .record(
            Event::new(b"latest value".to_vec())
                .with_key(b"key-1".to_vec()),
        )
        .await?;
    retained.record(Event::new(b"will expire".to_vec())).await?;
    fast.record(Event::new(b"fast write".to_vec())).await?;

    println!("all writes succeeded");

    replicated.close().await;
    compacted.close().await;
    retained.close().await;
    fast.close().await;

    Ok(())
}
