use clap::{Parser, Subcommand};

use ceph_backup::rbd;

#[derive(Parser)]
#[command()]
struct Cli {
    /// log filters (see https://docs.rs/env_logger/latest/env_logger/index.html#enabling-logging)
    #[arg(long, default_value = "info", env = "LOG")]
    log: String,
    /// log style (see https://docs.rs/env_logger/latest/env_logger/index.html#disabling-colors)
    #[arg(long, default_value = "auto", env = "LOG_STYLE")]
    log_style: String,

    /// client id (without 'client.' prefix)
    #[arg(long, default_value = "admin", env = "CEPH_CLIENT_ID")]
    id: String,

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
        /// send buffer size in KiB
        #[arg(long, default_value = "4096")]
        buffer_size: usize,
        /// zstd compression level (1-22) for transmission
        #[arg(short = 'z', long, default_value = "3")]
        compress_level: i32,
        /// image name filter
        #[arg(short = 'F', long, default_value = "*")]
        filter: String,
        /// Parallel snapshot creation operations
        #[arg(long, default_value = "4")]
        parallel_snap_create: u8,
        /// Parallel import operations
        #[arg(long, default_value = "2")]
        parallel_import: u8,
        /// Parallel rollback operations
        #[arg(long, default_value = "1")]
        parallel_rollback: u8,
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
        /// Parallel expire operations
        #[arg(long, default_value = "2")]
        parallel_expire: u8,
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

    ctrlc::set_handler(|| {
        if ceph_backup::terminated() {
            eprintln!("already got termination signal, exiting immediately");
            std::process::exit(1);
        }
        ceph_backup::sigterm();
    })?;

    match cli.command {
        Commands::Rbd {
            pool,
            dest,
            buffer_size,
            compress_level,
            filter,
            parallel_snap_create,
            parallel_import,
            parallel_rollback,
        } => rbd::source::run(
            &cli.id,
            &cli.cluster,
            &pool,
            &dest,
            buffer_size << 10,
            compress_level,
            &filter,
            rbd::source::Parallel {
                snap_create: parallel_snap_create,
                import: parallel_import,
                rollback: parallel_rollback,
            },
        ),
        Commands::RbdTarget {
            pool,
            bind_addr,
            expire_days,
            parallel_expire,
        } => rbd::target::run(
            &cli.id,
            &cli.cluster,
            &pool,
            &bind_addr,
            expire_days,
            rbd::target::Parallel {
                expire: parallel_expire,
            },
        ),
    }
}
