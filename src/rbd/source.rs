use eyre::{format_err, Result};
use glob_match::glob_match;
use log::{error, info, warn};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub fn run(cluster: &str, pool: &str, dest: &str, compress_level: i32, filter: &str) -> Result<()> {
    info!("cluster {cluster}, pool {pool}");

    let today = now().date_naive();

    let src = Arc::new(super::Local::new(cluster, pool));

    let images = src.ls()?;
    let images: Vec<_> = images
        .into_iter()
        .filter(|img| glob_match(filter, img))
        .collect();

    let error_count = AtomicUsize::new(0);
    let inc_error = || {
        error_count.fetch_add(1, Ordering::SeqCst);
    };

    info!("checking snapshots");
    crate::parallel_process(images.clone(), |img| {
        let result = || -> Result<()> {
            let snapshots = src.snap_ls(&img)?;
            let latest = snapshots.iter().filter_map(|s| s.timestamp().ok()).max();

            if latest.is_none_or(|t| t.date() != today) {
                let snap_name = now().format("bck-%Y%m%d_%H%M%S");
                info!("{img}: creating today's snapshot: {snap_name}");
                src.snap_create(&img, &snap_name.to_string())?;
            }
            Ok(())
        }();

        if let Err(e) = result {
            error!("{img}: snapshot failed: {e}");
            inc_error();
        }
    });

    let tgt = Arc::new(super::target::Client::new(dest, compress_level));

    info!("checking remote snapshots");

    let tgt_images = tgt.ls()?;

    crate::parallel_process(images.clone(), |img| {
        let src = src.clone();
        let tgt = tgt.clone();

        let start = now();
        let result = backup_image(&src, &tgt, &tgt_images, &img);
        let elapsed = now() - start;

        if let Err(e) = result {
            error!("{img}: backup failed after {elapsed}: {e}");
            inc_error();
        } else {
            info!("{img}: backup ok after {elapsed}");
        }
    });

    info!("expiring remote backups");
    tgt.expire()?;

    let error_count = error_count.load(Ordering::Relaxed);
    if error_count != 0 {
        warn!("backups done with {error_count} errors");
        Err(format_err!("{error_count} errors"))
    } else {
        info!("backups done");
        Ok(())
    }
}

fn backup_image(
    src: &super::Local,
    tgt: &super::target::Client,
    tgt_images: &Vec<String>,
    img: &String,
) -> Result<()> {
    let mut src_snaps = src.snap_ls(img)?;
    src_snaps.sort();

    if src_snaps.is_empty() {
        warn!("{img}: no snapshots found");
        return Ok(());
    }

    if !tgt_images.contains(img) {
        let snap_name = src_snaps.first().map(|s| s.name.as_str()).unwrap();

        info!("{img}: backup is missing, creating from {snap_name}");

        let mut export = src.export(&format!("{img}@{snap_name}"))?;
        tgt.import(img, &snap_name, &mut export)?;
    }

    let mut tgt_snaps = tgt.snap_ls(img)?;

    tgt_snaps.sort();
    let mut from_snap = tgt_snaps.last().map(|s| s.name.to_string());

    if !src_snaps.iter().any(|s| Some(s.name.clone()) == from_snap) {
        if let Some(from_snap) = from_snap {
            // latest snapshot on target is not on source anymore
            warn!("{img}: cannot resume snapshots from {from_snap} as is does not exists on source anymore, recreating");
        } else {
            // no snapshots on target
            warn!("{img}: no snapshots on target, recreating");
        }

        tgt.trash_move(img)?;

        let snap_name = src_snaps.first().unwrap().name.clone();

        let mut export = src.export(&format!("{img}@{snap_name}"))?;
        tgt.import(img, &snap_name, &mut export)?;

        from_snap = Some(snap_name);
    }

    let mut from_snap = from_snap.unwrap();

    let src_snaps_to_send: Vec<_> = src_snaps
        .iter()
        .skip_while(|s| s.name != from_snap)
        .skip(1)
        .collect();

    for to_snap in src_snaps_to_send {
        let to_snap = to_snap.name.clone();
        info!("{img}: sending diff {from_snap} -> {to_snap}");

        tgt.snap_rollback(img, &from_snap)?;

        let mut export = src.export_diff(img, &from_snap, &to_snap)?;
        tgt.import_diff(img, &mut export)?;

        from_snap = to_snap;
    }

    let mut tgt_snaps = tgt.snap_ls(img)?;
    tgt_snaps.sort();
    let tgt_snaps: Vec<_> = tgt_snaps
        .iter()
        .take_while(|s| s.name != from_snap)
        .collect();

    for snap in src_snaps
        .iter()
        .filter(|s| tgt_snaps.iter().any(|ts| ts.name == s.name))
    {
        let snap = &snap.name;
        info!("{img}: removing source snapshot {snap}");
        src.snap_remove(img, snap)?;
    }

    Ok(())
}

fn now() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now()
}
