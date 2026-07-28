//! Fluvius CLI — real-time geospatial stream processor.

use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use fluvius_cli::topology_run;
use fluvius_connectors::file;
use fluvius_core::event::Event;
use fluvius_core::operator::FilterOperator;
use fluvius_core::pipeline::Pipeline;
use fluvius_core::topology::{TopologyConfig, load_topology};
use fluvius_geo::geofence::{GeofenceOperator, GeofenceZone};
use fluvius_geo::proximity::ProximityOperator;
use fluvius_geo::trajectory::TrajectoryOperator;
use tokio::sync::mpsc;

#[derive(Parser)]
#[command(name = "fluvius", about = "Real-time geospatial stream processor")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Process events from a JSON lines file, or run a full TOML topology
    Run {
        /// Input JSONL file (required unless --topology is given)
        #[arg(short, long)]
        input: Option<PathBuf>,
        /// Output JSONL file (required unless --topology is given)
        #[arg(short, long)]
        output: Option<PathBuf>,
        /// Filter: minimum speed in m/s
        #[arg(long)]
        min_speed: Option<f64>,
        /// Run a declarative pipeline from a TOML topology file
        #[arg(long)]
        topology: Option<PathBuf>,
    },
    /// Apply a TOML topology to live events over WebSocket
    Serve {
        /// Topology file declaring the operator chain
        #[arg(long)]
        topology: PathBuf,
        /// Bind address for incoming events
        #[arg(long, default_value = "127.0.0.1:9001")]
        source_bind: String,
        /// Bind address for output broadcast
        #[arg(long, default_value = "127.0.0.1:9002")]
        sink_bind: String,
    },
    /// Run geofence monitoring from file input
    Geofence {
        /// Input JSONL file
        #[arg(short, long)]
        input: PathBuf,
        /// Geofence bounds: min_lon,min_lat,max_lon,max_lat
        #[arg(long)]
        bounds: String,
        /// Zone name
        #[arg(long, default_value = "zone")]
        zone_name: String,
    },
    /// Detect proximity alerts from file input
    Proximity {
        /// Input JSONL file
        #[arg(short, long)]
        input: PathBuf,
        /// Distance threshold in meters
        #[arg(long, default_value = "100.0")]
        threshold: f64,
    },
    /// Analyze trajectories from file input
    Trajectory {
        /// Input JSONL file
        #[arg(short, long)]
        input: PathBuf,
        /// Max expected speed in m/s (alerts above this)
        #[arg(long, default_value = "50.0")]
        max_speed: f64,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Run {
            input,
            output,
            min_speed,
            topology,
        } => cmd_run(input, output, min_speed, topology).await,
        Command::Serve {
            topology,
            source_bind,
            sink_bind,
        } => cmd_serve(&topology, &source_bind, &sink_bind).await,
        Command::Geofence {
            input,
            bounds,
            zone_name,
        } => cmd_geofence(&input, &bounds, &zone_name).await,
        Command::Proximity { input, threshold } => cmd_proximity(&input, threshold).await,
        Command::Trajectory { input, max_speed } => cmd_trajectory(&input, max_speed).await,
    }
}

async fn cmd_run(
    input: Option<PathBuf>,
    output: Option<PathBuf>,
    min_speed: Option<f64>,
    topology: Option<PathBuf>,
) {
    if let Some(topology_path) = topology {
        let config = load_config(&topology_path);
        if let Err(e) = topology_run::run_topology(config).await {
            eprintln!("Error running topology: {e}");
            std::process::exit(1);
        }
        return;
    }

    let (Some(input), Some(output)) = (input, output) else {
        eprintln!("Error: --input and --output are required unless --topology is given");
        std::process::exit(1);
    };

    cmd_run_file(&input, &output, min_speed).await;
}

fn load_config(path: &std::path::Path) -> TopologyConfig {
    load_topology(path).unwrap_or_else(|e| {
        eprintln!("Error loading topology: {e}");
        std::process::exit(1);
    })
}

async fn cmd_run_file(input: &std::path::Path, output: &std::path::Path, min_speed: Option<f64>) {
    let events = file::read_jsonl(input).await.unwrap_or_else(|e| {
        eprintln!("Error reading input: {e}");
        std::process::exit(1);
    });

    let mut pipeline = Pipeline::new("file_pipeline");

    if let Some(speed) = min_speed {
        pipeline.add_operator(Arc::new(FilterOperator::new(
            "speed_filter",
            move |e: &Event| e.speed.unwrap_or(0.0) >= speed,
        )));
    } else {
        pipeline.add_operator(Arc::new(FilterOperator::new("pass_all", |_| true)));
    }

    let (tx_in, rx_in) = mpsc::channel(10000);
    let (tx_out, mut rx_out) = mpsc::channel(10000);

    // Feed events
    let feed_handle = tokio::spawn(async move {
        for event in events {
            tx_in.send(event).await.unwrap();
        }
    });

    // Collect outputs
    let output_path = output.to_path_buf();
    let collect_handle = tokio::spawn(async move {
        let mut outputs = Vec::new();
        while let Some(out) = rx_out.recv().await {
            outputs.push(out);
        }
        file::write_jsonl(&output_path, &outputs).await.unwrap();
        outputs.len()
    });

    let metrics = pipeline.run(rx_in, tx_out).await;
    feed_handle.await.unwrap();
    let output_count = collect_handle.await.unwrap();

    println!("Pipeline complete:");
    println!("  Events received: {}", metrics.events_received);
    println!("  Events emitted: {}", metrics.events_emitted);
    println!("  Events filtered: {}", metrics.events_filtered);
    println!("  Outputs written: {output_count}");
}

async fn cmd_serve(topology: &std::path::Path, source_bind: &str, sink_bind: &str) {
    let config = load_config(topology);
    if let Err(e) = topology_run::serve(config, source_bind, sink_bind).await {
        eprintln!("Error serving topology: {e}");
        std::process::exit(1);
    }
}

async fn cmd_geofence(input: &std::path::Path, bounds: &str, zone_name: &str) {
    let events = file::read_jsonl(input).await.unwrap_or_else(|e| {
        eprintln!("Error reading input: {e}");
        std::process::exit(1);
    });

    let parts: Vec<f64> = bounds
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    if parts.len() != 4 {
        eprintln!("Bounds must be: min_lon,min_lat,max_lon,max_lat");
        std::process::exit(1);
    }

    let exterior = geo::LineString::from(vec![
        geo::Coord {
            x: parts[0],
            y: parts[1],
        },
        geo::Coord {
            x: parts[2],
            y: parts[1],
        },
        geo::Coord {
            x: parts[2],
            y: parts[3],
        },
        geo::Coord {
            x: parts[0],
            y: parts[3],
        },
        geo::Coord {
            x: parts[0],
            y: parts[1],
        },
    ]);

    let mut op = GeofenceOperator::new("geofence");
    op.add_zone(GeofenceZone {
        name: zone_name.into(),
        polygon: geo::Polygon::new(exterior, vec![]),
    });

    let mut total_alerts = 0;
    for event in &events {
        use fluvius_core::operator::StatefulOperator;
        let outputs = op.process(event);
        for out in &outputs {
            println!(
                "[{}] {} — {} zone '{}'",
                out.source_event.timestamp,
                out.source_event.entity_id,
                out.payload["event"],
                out.payload["zone"]
            );
        }
        total_alerts += outputs.len();
    }

    println!(
        "\nProcessed {} events, {} geofence alerts",
        events.len(),
        total_alerts
    );
}

async fn cmd_proximity(input: &std::path::Path, threshold: f64) {
    let events = file::read_jsonl(input).await.unwrap_or_else(|e| {
        eprintln!("Error reading input: {e}");
        std::process::exit(1);
    });

    let mut op = ProximityOperator::new("proximity", threshold);
    let mut total_alerts = 0;

    for event in &events {
        use fluvius_core::operator::StatefulOperator;
        let outputs = op.process(event);
        for out in &outputs {
            println!(
                "[{}] PROXIMITY: {} ↔ {} ({:.1}m)",
                out.source_event.timestamp,
                out.payload["entity_a"],
                out.payload["entity_b"],
                out.payload["distance_meters"].as_f64().unwrap_or(0.0)
            );
        }
        total_alerts += outputs.len();
    }

    println!(
        "\nProcessed {} events, {} proximity alerts",
        events.len(),
        total_alerts
    );
}

async fn cmd_trajectory(input: &std::path::Path, max_speed: f64) {
    let events = file::read_jsonl(input).await.unwrap_or_else(|e| {
        eprintln!("Error reading input: {e}");
        std::process::exit(1);
    });

    let mut op = TrajectoryOperator::new("trajectory", 1000, max_speed);
    let mut anomalies = 0;

    for event in &events {
        use fluvius_core::operator::StatefulOperator;
        let outputs = op.process(event);
        for out in &outputs {
            if out.payload["alert"] == "speed_anomaly" {
                println!(
                    "[{}] ANOMALY: {} speed={:.1} m/s (max={:.1})",
                    out.source_event.timestamp,
                    out.payload["entity_id"],
                    out.payload["computed_speed_mps"].as_f64().unwrap_or(0.0),
                    max_speed
                );
                anomalies += 1;
            }
        }
    }

    // Final summaries
    use fluvius_core::operator::StatefulOperator;
    let summaries = op.on_window_close();
    for s in &summaries {
        println!(
            "  {} — {} points, {:.0}m distance, {:.1} m/s avg",
            s.payload["entity_id"],
            s.payload["point_count"],
            s.payload["total_distance_meters"].as_f64().unwrap_or(0.0),
            s.payload["avg_speed_mps"].as_f64().unwrap_or(0.0)
        );
    }

    println!(
        "\nProcessed {} events, {} anomalies, {} entities",
        events.len(),
        anomalies,
        summaries.len()
    );
}
