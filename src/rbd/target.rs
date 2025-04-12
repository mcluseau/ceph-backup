use eyre::{format_err, Result};
use log::{error, info, warn};
use std::time::Duration;
use std::{
    collections::BTreeMap as Map,
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    thread,
};

use crate::rbd;

pub struct Parallel {
    pub expire: u8,
}

pub fn run(
    client_id: &str,
    cluster: &str,
    pool: &str,
    bind_addr: &str,
    expire_days: u16,
    buf_size: usize,
    parallel: Parallel,
) -> Result<()> {
    info!("target: cluster {cluster}, pool {pool}");

    let bind_addr = bind_addr.to_socket_addrs()?.next().unwrap();
    info!("listening on {bind_addr}");

    let socket = new_socket(bind_addr)?;
    socket.bind(&bind_addr.into())?;
    socket.listen(2)?;

    let listener: TcpListener = socket.into();

    let (tx, rx) = crossbeam_channel::bounded(0);

    thread::spawn(move || loop {
        let r = listener.accept();
        let exit = r.is_err();
        if tx.send(r).is_err() {
            break;
        }
        if exit {
            break;
        }
    });

    let rbd = rbd::Local::new(client_id, cluster, pool, buf_size);

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

        let rbd = &rbd;
        let parallel = &parallel;

        scope.spawn(move || {
            match handle_connection(remote, &stream, rbd, parallel, expire_days) { Err(e) => {
                warn!("{remote}: failed: {e}");
            } _ => {
                info!("{remote}: done");
            }}
        });
    })
}

fn new_socket(addr: SocketAddr) -> io::Result<socket2::Socket> {
    use socket2::{Domain, Socket, Type};
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, None)?;

    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(30))
        .with_interval(Duration::from_secs(30));
    socket.set_tcp_keepalive(&keepalive)?;
    socket.set_nodelay(false)?;

    Ok(socket)
}

fn handle_connection(
    remote: SocketAddr,
    mut stream: &TcpStream,
    rbd: &rbd::Local,
    parallel: &Parallel,
    expire_days: u16,
) -> Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;

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
        "need_rollback" => {
            let img = split.next().ok_or(format_err!("no image"))?;
            write_json(stream, rbd.need_rollback(img))?;
        }
        "rollback" => {
            let img = split.next().ok_or(format_err!("no image"))?;
            write_result(stream, rbd.rollback(img))?;
        }
        "import_diff" => {
            let img = split.next().ok_or(format_err!("no image"))?;

            let mut zstd_in = zstd::Decoder::new(reader)?;
            write_result(stream, rbd.import_diff(img, &mut zstd_in))?;
        }
        "expire" => {
            write_result(stream, expire_backups(rbd, parallel.expire, expire_days))?;
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

fn expire_backups(rbd: &rbd::Local, parallel: u8, expire_days: u16) -> Result<()> {
    let deadline = chrono::Utc::now().naive_utc() - chrono::TimeDelta::days(expire_days as i64);

    let images = (rbd.ls()).map_err(|e| format_err!("failed to list images: {e}"))?;

    let results = crate::parallel_process(parallel, images, |img| -> bool {
        rbd.expire_backups(&img, &deadline)
            .inspect_err(|e| error!("{img}: {e}"))
            .is_ok()
    });

    let n_err = results.into_iter().filter(|ok| !ok).count();
    if n_err == 0 {
        Ok(())
    } else {
        Err(format_err!("{n_err} errors"))
    }
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
    match (result, write_result) {
        (Ok(v), Ok(_)) => Ok(v),
        (Err(e), _) => Err(e),
        (_, Err(e)) => Err(format_err!("write result failed: {e}")),
    }
}

#[derive(Clone)]
pub struct Client {
    remote: SocketAddr,
    compress_level: i32,
}
impl Client {
    /// Creates a new client.
    ///
    /// `compress_level`: zstd compression level (1-22). `0` uses zstd's default (currently `3`).
    pub fn new(remote: SocketAddr, compress_level: i32) -> Self {
        Self {
            remote,
            compress_level,
        }
    }

    fn dial(&self, cmd_line: String) -> Result<TcpStream> {
        let socket = new_socket(self.remote)?;
        socket
            .connect(&self.remote.into())
            .map_err(|e| format_err!("connect to {} failed: {e}", self.remote))?;
        let mut stream: TcpStream = socket.into();
        writeln!(stream, "{cmd_line}")?;
        stream.flush()?;
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

        let mut w = BufWriter::new(&stream);
        zstd::stream::copy_encode(&mut reader, &mut w, self.compress_level)?;
        w.flush()?;

        stream.shutdown(std::net::Shutdown::Write)?;
        self.result(&stream)
    }

    pub fn need_rollback(&self, img: &str) -> Result<bool> {
        self.dial_json(format!("need_rollback {img}"))
    }

    pub fn rollback(&self, img: &str) -> Result<()> {
        self.dial_noout(format!("rollback {img}"))
    }

    pub fn import_diff(&self, img: &str, mut reader: impl Read) -> Result<()> {
        let stream = self.dial(format!("import_diff {img}"))?;

        let mut w = BufWriter::new(&stream);
        zstd::stream::copy_encode(&mut reader, &mut w, self.compress_level)?;
        w.flush()?;

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
