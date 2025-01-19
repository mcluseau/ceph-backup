use eyre::{format_err, Result};
use log::{info, warn};
use std::time::Duration;
use std::{
    collections::BTreeMap as Map,
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    thread,
};

use crate::rbd;

pub fn run(cluster: &str, pool: &str, bind_addr: &str, expire_days: u16) -> Result<()> {
    let listener = TcpListener::bind(bind_addr)?;
    info!("listening on {bind_addr}");

    let (tx, rx) = crossbeam_channel::bounded(0);

    thread::spawn(move || loop {
        let r = listener.accept();
        let exit = r.is_ok();
        if tx.send(r).is_err() {
            break;
        }
        if exit {
            break;
        }
    });

    thread::scope(|scope| loop {
        let (stream, remote) = loop {
            if crate::terminated() {
                return Err(format_err!("terminated"));
            }
            use crossbeam_channel::RecvTimeoutError::*;
            break match rx.recv_timeout(Duration::from_secs(1)) {
                Ok(e) => e?,
                Err(Timeout) => {
                    continue;
                }
                Err(Disconnected) => {
                    return Err(format_err!("recv disconnected"));
                }
            };
        };

        let cluster = cluster.to_string();
        let pool = pool.to_string();

        scope.spawn(move || {
            let rbd = rbd::Local::new(&cluster, &pool);
            if let Err(e) = handle_connection(remote, &stream, rbd, expire_days) {
                warn!("{remote}: failed: {e}");
            }
        });
    })
}

fn handle_connection(
    remote: SocketAddr,
    mut stream: &TcpStream,
    rbd: rbd::Local,
    expire_days: u16,
) -> Result<()> {
    stream.set_read_timeout(Some(std::time::Duration::from_secs(30)))?;
    stream.set_nodelay(false)?;

    let mut reader = BufReader::new(stream);

    let mut line = String::new();
    reader.read_line(&mut line)?;

    let line = line.trim_ascii();

    let mut split = line.split(' ');

    info!("command from {remote}: {line:?}");

    let cmd = split.next().ok_or(format_err!("no command"))?;

    match cmd {
        "ls" => {
            write_json(stream, rbd.ls())?;
        }
        "snap_ls" => {
            let img = split.next().ok_or(format_err!("no image"))?;
            write_json(stream, rbd.snap_ls(img))?;
        }
        "trash_move" => {
            let img = split.next().ok_or(format_err!("no image"))?;
            write_result(stream, rbd.trash_move(img))?;
        }
        "import" => {
            let img = split.next().ok_or(format_err!("no image"))?;
            let snap_name = split.next().ok_or(format_err!("no snapshot name"))?;

            let mut zstd_in = zstd::Decoder::new(reader)?;
            write_result(stream, rbd.import(img, snap_name, &mut zstd_in))?;
        }
        "prepare_import_diff" => {
            let img = split.next().ok_or(format_err!("no image"))?;
            write_result(stream, rbd.prepare_import_diff(img))?;
        }
        "import_diff" => {
            let img = split.next().ok_or(format_err!("no image"))?;

            let mut zstd_in = zstd::Decoder::new(reader)?;
            write_result(stream, rbd.import_diff(img, &mut zstd_in))?;
        }
        "expire" => {
            write_result(stream, expire_backups(rbd, expire_days))?;
        }
        "meta_sync" => {
            let img = split.next().ok_or(format_err!("no image"))?;

            let mut kvs = Map::new();
            for kv in split {
                let (k, v) = kv.split_once('=').unwrap_or_else(|| (kv, ""));
                kvs.insert(k.to_string(), v.to_string());
            }

            write_result(stream, rbd.meta_sync(img, &kvs))?;
        }
        _ => {
            stream.write(b"ERR\n")?;
            return Err(format_err!("unknown command: {cmd:?}"));
        }
    };

    Ok(())
}

fn expire_backups(rbd: rbd::Local, expire_days: u16) -> Result<()> {
    let deadline = chrono::Utc::now().naive_utc() - chrono::TimeDelta::days(expire_days as i64);

    for img in rbd.ls()? {
        for snap in rbd.snap_ls(&img)? {
            let Ok(ts) = snap.timestamp() else {
                continue;
            };

            if ts >= deadline {
                continue;
            }

            let snap_name = snap.name;
            info!("{img}: removing snapshot {snap_name}");
            if let Err(e) = rbd.snap_remove(&img, &snap_name) {
                warn!("{img}: failed to remove snapshot {snap_name}: {e}");
            }
        }

        if rbd.snap_ls(&img)?.is_empty() {
            info!("{img}: no more snapshots, removing");
            if let Err(e) = rbd.remove(&img) {
                warn!("{img}: failed to remove: {e}");
            }
        }
    }

    Ok(())
}

fn write_json<T: serde::Serialize>(stream: &TcpStream, result: Result<T>) -> Result<()> {
    let result = write_result(stream, result)?;
    serde_json::to_writer(stream, &result)?;
    Ok(())
}

fn write_result<T>(mut stream: &TcpStream, result: Result<T>) -> Result<T> {
    let write_result = match result {
        Ok(_) => stream.write(b"OK \n"),
        Err(_) => stream.write(b"ERR\n"),
    };
    if result.is_err() {
        return result;
    }
    write_result?;
    result
}

#[derive(Clone)]
pub struct Client<'t> {
    remote: &'t str,
    compress_level: i32,
}
impl<'t> Client<'t> {
    /// Creates a new client.
    ///
    /// `compress_level`: zstd compression level (1-22). `0` uses zstd's default (currently `3`).
    pub fn new(remote: &'t str, compress_level: i32) -> Self {
        Self {
            remote,
            compress_level,
        }
    }

    fn dial(&self, cmd_line: String) -> Result<TcpStream> {
        let mut stream = TcpStream::connect(self.remote)?;
        writeln!(stream, "{cmd_line}")?;
        Ok(stream)
    }

    fn dial_noout(&self, cmd_line: String) -> Result<()> {
        let stream = self.dial(cmd_line)?;
        self.result(&stream)
    }

    fn dial_json<V: serde::de::DeserializeOwned>(&self, cmd_line: String) -> Result<V> {
        let stream = self.dial(cmd_line)?;
        self.result(&stream)?;
        Ok(serde_json::from_reader(stream)?)
    }

    fn result(&self, mut stream: &TcpStream) -> Result<()> {
        let mut result = [0; 4];
        stream.read_exact(&mut result)?;
        match &result {
            b"OK \n" => Ok(()),
            b"ERR\n" => Err(format_err!("remote failed")),
            _ => Err(format_err!("invalid result")),
        }
    }

    pub fn ls(&self) -> Result<Vec<String>> {
        self.dial_json("ls".to_string())
    }

    pub fn snap_ls(&self, img: &str) -> Result<Vec<rbd::Snapshot>> {
        self.dial_json(format!("snap_ls {img}"))
    }

    pub fn trash_move(&self, img: &str) -> Result<()> {
        self.dial_noout(format!("trash_move {img}"))
    }

    pub fn import(&self, img: &str, snap_name: &str, mut reader: impl Read) -> Result<()> {
        let stream = self.dial(format!("import {img} {snap_name}"))?;

        zstd::stream::copy_encode(&mut reader, &stream, self.compress_level)?;

        stream.shutdown(std::net::Shutdown::Write)?;
        self.result(&stream)
    }

    pub fn prepare_import_diff(&self, img: &str) -> Result<()> {
        self.dial_noout(format!("prepare_import_diff {img}"))
    }

    pub fn import_diff(&self, img: &str, mut reader: impl Read) -> Result<()> {
        let stream = self.dial(format!("import_diff {img}"))?;

        zstd::stream::copy_encode(&mut reader, &stream, self.compress_level)?;

        stream.shutdown(std::net::Shutdown::Write)?;
        self.result(&stream)
    }

    pub fn meta_sync(&self, img: &str, kvs: &Map<String, String>) -> Result<()> {
        let mut command = String::from("meta_sync ");
        command.push_str(img);
        for (k, v) in kvs {
            command.push(' ');
            command.push_str(k);
            command.push('=');
            command.push_str(v);
        }

        self.dial_noout(command)
    }

    pub fn expire(&self) -> Result<()> {
        self.dial_noout(format!("expire"))
    }
}
