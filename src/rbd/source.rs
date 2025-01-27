use eyre::{format_err, Result};
use glob_match::glob_match;
use log::{error, info, warn};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::rbd;

pub fn run(
    client_id: &str,
    cluster: &str,
    pool: &str,
    dest: &str,
    buffer_size: usize,
    compress_level: i32,
    filter: &str,
) -> Result<()> {
    info!("source: cluster {cluster}, pool {pool}");

    use std::net::ToSocketAddrs;
    let dest = dest.to_socket_addrs()?.next().unwrap();

    let src = rbd::Local::new(client_id, cluster, pool);
    let tgt = rbd::target::Client::new(dest, buffer_size, compress_level);

    BackupRun::new(src, tgt).run(filter)
}

struct BackupRun<'t> {
    src: rbd::Local<'t>,
    tgt: rbd::target::Client,
    error_count: AtomicUsize,
    n_steps: AtomicUsize,
    n_done: AtomicUsize,
    stage: &'static str,
}
impl<'t> BackupRun<'t> {
    fn new(src: rbd::Local<'t>, tgt: rbd::target::Client) -> Self {
        let zero = || AtomicUsize::new(0);
        Self {
            src,
            tgt,
            n_steps: zero(),
            n_done: zero(),
            error_count: zero(),
            stage: "init",
        }
    }

    fn run(&mut self, filter: &str) -> Result<()> {
        let today = now().date_naive();

        let images = self.src.ls()?;
        let images: Vec<_> = images
            .into_iter()
            .filter(|img| glob_match(filter, img))
            .collect();

        // --------------------------------------------------------------
        self.stage("creating snapshots");
        let results = self.steps(images, |img| -> Result<()> {
            let snapshots = self.src.snap_ls(img)?;
            let latest = snapshots.iter().filter_map(|s| s.timestamp().ok()).max();

            if latest.is_none_or(|t| t.date() != today) {
                let snap_name = now().format("bck-%Y%m%d_%H%M%S");
                info!("{img}: creating today's snapshot: {snap_name}");
                self.src.snap_create(img, &snap_name.to_string())?;
            }
            Ok(())
        });

        // --------------------------------------------------------------
        self.stage("creating missing backups");

        let tgt_images = self.tgt.ls()?;

        // dispatch existing and missing backups
        let mut missing_backups = Vec::new();
        let mut images = Vec::with_capacity(results.len());
        for img in results.all_ok() {
            if tgt_images.contains(&img) {
                images.push(img);
            } else {
                missing_backups.push(img);
            }
        }

        let results = self.chrono_steps(missing_backups, |img| -> Result<()> {
            let mut src_snaps = self.src.snap_ls(img)?;
            src_snaps.sort();

            let Some(src_snap) = src_snaps.first() else {
                return Err(format_err!("no snapshots found"));
            };

            let snap_name = &src_snap.name;

            info!("{img}: backup is missing, creating from {snap_name}");

            let mut export = self.src.export(&format!("{img}@{snap_name}"))?;
            self.tgt.import(img, snap_name, &mut export)
        });

        // reintroduce successfully created backups
        images.extend(results.all_ok());
        images.sort();

        // --------------------------------------------------------------
        self.stage("backup snapshots");
        let results = self.chrono_steps(images, |img| -> Result<()> { self.backup_snapshots(img) });
        let maybe_partials = results.all_err().collect();

        // --------------------------------------------------------------
        self.stage("expiring remote backups");
        if let Err(e) = self.tgt.expire() {
            error!("expiration failed: {e}");
            self.error();
        }

        // --------------------------------------------------------------
        self.stage("rollback partials diffs");
        self.chrono_steps(maybe_partials, |img| -> Result<()> {
            self.tgt.prepare_import_diff(img)
        });

        // --------------------------------------------------------------
        let error_count = self.error_count();
        if error_count == 0 {
            info!("backups done without errors");
            Ok(())
        } else {
            warn!("backups done with {error_count} errors");
            Err(format_err!("{error_count} errors"))
        }
    }

    fn backup_snapshots(&self, img: &str) -> Result<()> {
        let mut src_snaps = self.src.snap_ls(img)?;
        src_snaps.sort();

        if src_snaps.is_empty() {
            warn!("{img}: no snapshots found");
            return Ok(());
        }

        let mut tgt_snaps = self.tgt.snap_ls(img)?;
        tgt_snaps.sort();
        let from_snap = tgt_snaps.into_iter().rev().next();
        let mut from_snap = from_snap.map(|s| s.name);

        if !(src_snaps.iter()).any(|s| Some(&s.name) == from_snap.as_ref()) {
            if let Some(from_snap) = from_snap {
                // latest snapshot on target is not on source anymore
                warn!("{img}: cannot resume snapshots from {from_snap} as is does not exists on source anymore, recreating");
            } else {
                // no snapshots on target
                warn!("{img}: no snapshots on target, recreating");
            }

            self.tgt.trash_move(img)?;

            let snap_name = src_snaps.first().unwrap().name.clone();

            let mut export = self.src.export(&format!("{img}@{snap_name}"))?;
            self.tgt.import(img, &snap_name, &mut export)?;

            from_snap = Some(snap_name);
        }

        let mut from_snap = from_snap.unwrap();

        let src_snaps_to_send: Vec<_> = (src_snaps.iter())
            .skip_while(|s| s.name != from_snap)
            .skip(1)
            .collect();

        if !src_snaps_to_send.is_empty() {
            info!("{img}: preparing for diff import");
            self.tgt.prepare_import_diff(img)?;
        }

        for to_snap in src_snaps_to_send {
            let to_snap = to_snap.name.clone();

            let mut send_try = 0;
            loop {
                info!("{img}: sending diff {from_snap} -> {to_snap}");

                let mut export = self.src.export_diff(img, &from_snap, &to_snap)?;
                let result = self.tgt.import_diff(img, &mut export);
                if let Err(e) = result {
                    if send_try == 3 {
                        return Err(e);
                    }
                    send_try += 1;
                    warn!("{img}: try {send_try} failed, reverting and retrying: {e}");
                    self.tgt.prepare_import_diff(img)?;
                    continue;
                }
                break;
            }

            from_snap = to_snap;
        }

        let mut tgt_snaps = self.tgt.snap_ls(img)?;
        tgt_snaps.sort();
        let tgt_snaps: Vec<_> = (tgt_snaps.iter())
            .take_while(|s| s.name != from_snap)
            .collect();

        for snap in (src_snaps.iter()).filter(|s| tgt_snaps.iter().any(|ts| ts.name == s.name)) {
            let snap = &snap.name;
            info!("{img}: removing source snapshot {snap}");
            self.src.snap_remove(img, snap)?;
        }

        self.tgt.meta_sync(img, &self.src.meta_list(img)?)?;

        Ok(())
    }

    fn n_steps(&self) -> usize {
        self.n_steps.load(Ordering::Relaxed)
    }
    fn set_n_steps(&self, n: usize) {
        self.n_steps.store(n, Ordering::Relaxed);
    }

    fn steps<F: Fn(&str) -> Result<()> + Send + Sync + Copy>(
        &self,
        steps: Vec<String>,
        action: F,
    ) -> Vec<(String, bool)> {
        self.set_n_steps(steps.len());
        crate::parallel_process(steps, |step| {
            let ok = self.step(&step, action);
            (step, ok)
        })
    }

    /// process given steps, returning successful ones.
    fn chrono_steps<F: Fn(&str) -> Result<()> + Send + Sync + Copy>(
        &self,
        steps: Vec<String>,
        action: F,
    ) -> Vec<(String, bool)> {
        self.set_n_steps(steps.len());
        crate::parallel_process(steps, |step| -> (String, bool) {
            let ok = self.chrono_step(&step, action);
            (step, ok)
        })
    }

    fn stage(&mut self, stage: &'static str) {
        info!("{stage}");
        self.stage = stage;
        self.n_steps.store(0, Ordering::Relaxed);
        self.n_done.store(0, Ordering::Relaxed);
    }

    fn error(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }
    fn error_count(&self) -> usize {
        self.error_count.load(Ordering::Relaxed)
    }

    fn step<F>(&self, img: &str, action: F) -> bool
    where
        F: Fn(&str) -> Result<()>,
    {
        let result = action(img);
        self.step_result(result, |_| {}, |p, e| error!("{p}{img}: failed: {e}"))
    }
    fn chrono_step<F>(&self, img: &str, action: F) -> bool
    where
        F: Fn(&str) -> Result<()>,
    {
        let start = now();
        let result = action(img);
        let elapsed = format_duration(now() - start);

        self.step_result(
            result,
            |p| info!("{p}{img}: ok after {elapsed}"),
            |p, e| error!("{p}{img}: failed after {elapsed}: {e}"),
        )
    }

    fn step_result<F: Fn(String), E: Fn(String, eyre::Report)>(
        &self,
        result: Result<()>,
        f_ok: F,
        f_err: E,
    ) -> bool {
        let done = self.n_done.fetch_add(1, Ordering::Relaxed) + 1;
        let total = self.n_steps();

        let stage = self.stage;
        let prefix = if total == 0 {
            format!("{stage} [{done}]: ")
        } else {
            format!("{stage} [{done}/{total}]: ")
        };

        if let Err(e) = result {
            self.error();
            f_err(prefix, e);
            false
        } else {
            f_ok(prefix);
            true
        }
    }
}

fn format_duration(td: chrono::TimeDelta) -> String {
    let ms = td.num_milliseconds();
    let (s, ms) = (ms / 1000, ms % 1000);
    let (m, s) = (s / 60, s % 60);
    let (h, m) = (m / 60, m % 60);
    let (d, h) = (h / 24, h % 24);
    let (w, d) = (d / 7, d % 7);

    if w != 0 {
        format!("{w}w{d}d{h}h{m}m{s}.{ms:0>3}s")
    } else if d != 0 {
        format!("{d}d{h}h{m}m{s}.{ms:0>3}s")
    } else if h != 0 {
        format!("{h}h{m}m{s}.{ms:0>3}s")
    } else if m != 0 {
        format!("{m}m{s}.{ms:0>3}s")
    } else {
        format!("{s}.{ms:0>3}s")
    }
}

fn now() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now()
}

trait StepResults {
    fn all_ok(self) -> impl Iterator<Item = String>;
    fn all_err(self) -> impl Iterator<Item = String>;
}
impl StepResults for Vec<(String, bool)> {
    fn all_ok(self) -> impl Iterator<Item = String> {
        self.into_iter().filter_map(|(img, ok)| ok.then_some(img))
    }
    fn all_err(self) -> impl Iterator<Item = String> {
        self.into_iter()
            .filter_map(|(img, ok)| (!ok).then_some(img))
    }
}
