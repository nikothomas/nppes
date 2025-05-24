use clap::{Parser, Subcommand, Args, ValueEnum};
use nppes::prelude::*;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "npcli")]
#[command(about = "NPPES Data CLI - Query, analyze, and export NPPES healthcare provider data", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show summary statistics for a dataset
    Stats(StatsArgs),
    /// Query providers by state, specialty, or NPI
    Query(QueryArgs),
    /// Export data to JSON, CSV, or SQL
    Export(ExportArgs),
    /// Download the latest NPPES data (if enabled)
    #[cfg(feature = "download")]
    Download(DownloadArgs),
}

#[derive(Args)]
struct StatsArgs {
    /// Path to the directory containing NPPES data files
    #[arg(short, long)]
    data_dir: PathBuf,
}

#[derive(Args)]
struct QueryArgs {
    /// Path to the directory containing NPPES data files
    #[arg(short, long)]
    data_dir: PathBuf,
    /// State code (e.g. CA, NY)
    #[arg(long)]
    state: Option<String>,
    /// Specialty (taxonomy display name, e.g. Cardiology)
    #[arg(long)]
    specialty: Option<String>,
    /// NPI number
    #[arg(long)]
    npi: Option<String>,
    /// Only show active providers
    #[arg(long)]
    active: bool,
    /// Limit number of results
    #[arg(long, default_value_t = 20)]
    limit: usize,
}

#[derive(Args)]
struct ExportArgs {
    /// Path to the directory containing NPPES data files
    #[arg(short, long)]
    data_dir: PathBuf,
    /// Output file path
    #[arg(short, long)]
    output: PathBuf,
    /// Export format
    #[arg(long, value_enum, default_value_t = ExportFormatOpt::Json)]
    format: ExportFormatOpt,
    /// State filter
    #[arg(long)]
    state: Option<String>,
    /// Specialty filter
    #[arg(long)]
    specialty: Option<String>,
    /// Only export active providers
    #[arg(long)]
    active: bool,
}

#[cfg(feature = "download")]
#[derive(Args)]
struct DownloadArgs {
    /// Output directory for downloaded files
    #[arg(short, long)]
    out_dir: PathBuf,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum ExportFormatOpt {
    Json,
    Csv,
    Sql,
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Stats(args) => cmd_stats(args),
        Commands::Query(args) => cmd_query(args),
        Commands::Export(args) => cmd_export(args),
        #[cfg(feature = "download")]
        Commands::Download(args) => cmd_download(args),
    }
}

fn cmd_stats(args: StatsArgs) {
    match NppesDataset::load_standard(&args.data_dir) {
        Ok(dataset) => {
            let stats = dataset.statistics();
            stats.print_summary();
        }
        Err(e) => {
            eprintln!("Error loading dataset: {}", e);
            std::process::exit(1);
        }
    }
}

fn cmd_query(args: QueryArgs) {
    match NppesDataset::load_standard(&args.data_dir) {
        Ok(dataset) => {
            let mut query = dataset.query();
            if let Some(state) = args.state.as_deref() {
                query = query.state(state);
            }
            if let Some(specialty) = args.specialty.as_deref() {
                query = query.specialty(specialty);
            }
            if args.active {
                query = query.active_only();
            }
            let mut results = query.execute();
            if let Some(npi) = args.npi.as_deref() {
                results = results.into_iter().filter(|p| p.npi.as_str() == npi).collect();
            }
            for provider in results.iter().take(args.limit) {
                println!("{} | {} | {} | {}", provider.npi, provider.display_name(), provider.entity_type.option_display(), provider.mailing_address.state.as_ref().map(|s| s.as_code()).unwrap_or(""));
            }
            println!("Total matches: {}", results.len());
        }
        Err(e) => {
            eprintln!("Error loading dataset: {}", e);
            std::process::exit(1);
        }
    }
}

fn cmd_export(args: ExportArgs) {
    match NppesDataset::load_standard(&args.data_dir) {
        Ok(dataset) => {
            let filter = |p: &NppesRecord| {
                let mut ok = true;
                if let Some(state) = args.state.as_deref() {
                    ok &= p.mailing_address.state.as_ref().map(|s| s.as_code()) == Some(state);
                }
                if let Some(specialty) = args.specialty.as_deref() {
                    ok &= p.taxonomy_codes.iter().any(|t| {
                        dataset.get_taxonomy_description(&t.code)
                            .and_then(|desc| desc.display_name.as_deref())
                            .map(|name| name.to_lowercase().contains(&specialty.to_lowercase()))
                            .unwrap_or(false)
                    });
                }
                if args.active {
                    ok &= p.is_active();
                }
                ok
            };
            let format = match args.format {
                ExportFormatOpt::Json => ExportFormat::Json,
                ExportFormatOpt::Csv => ExportFormat::Csv,
                ExportFormatOpt::Sql => ExportFormat::Sql,
            };
            match dataset.export_subset(&args.output, filter, format) {
                Ok(_) => println!("Exported to {}", args.output.display()),
                Err(e) => {
                    eprintln!("Export error: {}", e);
                    std::process::exit(1);
                }
            }
        }
        Err(e) => {
            eprintln!("Error loading dataset: {}", e);
            std::process::exit(1);
        }
    }
}

#[cfg(feature = "download")]
fn cmd_download(args: DownloadArgs) {
    use nppes::download::NppesDownloader;
    use tokio::runtime::Runtime;
    let mut downloader = NppesDownloader::new();
    let rt = Runtime::new().expect("Failed to create tokio runtime");
    match rt.block_on(downloader.download_latest_nppes()) {
        Ok(files) => {
            println!("Download and extraction complete: {}", files.summary());
            println!("Files saved to {}", files.directory.display());
        }
        Err(e) => {
            eprintln!("Download error: {}", e);
            std::process::exit(1);
        }
    }
} 