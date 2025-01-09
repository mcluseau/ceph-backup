use clap::{Parser, Subcommand};

mod rbd;

#[derive(Parser)]
#[command()]
struct Cli {
    /// log filters (see https://docs.rs/env_logger/latest/env_logger/index.html#enabling-logging)
    #[arg(long, default_value = "info", env = "LOG")]
    log: String,
    /// log style (see https://docs.rs/env_logger/latest/env_logger/index.html#disabling-colors)
    #[arg(long, default_value = "auto", env = "LOG_STYLE")]
    log_style: String,

    /// Ceph cluster
    #[arg(short = 'c', long, default_value = "ceph", env = "CEPH_CLUSTER")]
    cluster: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Rbd {
        /// source pool
        pool: String,
        /// backup destination
        #[arg(long, default_value = DEFAULT_BIND)]
        dest: String,
        /// zstd compression level (1-22) for transmission
        #[arg(short = 'z', long, default_value = "3")]
        compress_level: i32,
        /// image name filter
        #[arg(short = 'F', long, default_value = "*")]
        filter: String,
    },
    RbdTarget {
        /// target pool
        pool: String,
        /// bind (listen) address
        #[arg(long, default_value = DEFAULT_BIND)]
        bind_addr: String,
        /// days before a snapshot is considered expired
        #[arg(long, default_value = "30")]
        expire_days: u16,
    },
}

const DEFAULT_BIND: &str = "127.0.0.1:3310";

fn main() -> eyre::Result<()> {
    let cli = Cli::parse();

    env_logger::builder()
        .parse_filters(cli.log.as_str())
        .parse_write_style(cli.log_style.as_str())
        .format_timestamp_millis()
        .init();

    match cli.command {
        Commands::Rbd {
            pool,
            dest,
            compress_level,
            filter,
        } => rbd::source::run(&cli.cluster, &pool, &dest, compress_level, &filter),
        Commands::RbdTarget {
            pool,
            bind_addr,
            expire_days,
        } => rbd::target::run(&cli.cluster, &pool, &bind_addr, expire_days),
    }
}
