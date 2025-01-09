use eyre::{format_err, Result};
use glob_match::glob_match;
use log::{error, info, warn};

pub fn run(cluster: &str, pool: &str, dest: &str, compress_level: i32, filter: &str) -> Result<()> {
    info!("cluster {cluster}, pool {pool}");

    let today = now().date_naive();

    let rbd = super::Local::new(cluster, pool);

    let images = rbd.ls()?;
    let images: Vec<_> = images
        .into_iter()
        .filter(|img| glob_match(filter, img))
        .collect();

    info!("checking snapshots");
    for img in &images {
        let snapshots = rbd.snap_ls(img)?;
        let latest = snapshots.iter().filter_map(|s| s.timestamp().ok()).max();

        if Some(today) != latest.map(|t| t.date()) {
            let snap_name = now().format("bck-%Y%m%d_%H%M%S");
            info!("{img}: creating today's snapshot: {snap_name}");
            rbd.snap_create(img, &snap_name.to_string())?;
        }
    }

    let tgt = super::target::Client::new(dest, compress_level);

    info!("checking remote snapshots");

    let tgt_images = tgt.ls()?;

    let mut had_errors = false;

    for img in &images {
        if let Err(e) = backup_image(&rbd, &tgt, &tgt_images, &img) {
            error!("{img}: backup failed: {e}");
            had_errors = true;
        } else {
            info!("{img}: backup ok");
        }
    }

    info!("expiring remote backups");
    tgt.expire()?;

    if had_errors {
        info!("done with errors");
        Err(format_err!("errors happened"))
    } else {
        info!("done");
        Ok(())
    }
}

fn backup_image(
    rbd: &super::Local,
    tgt: &super::target::Client,
    tgt_images: &Vec<String>,
    img: &String,
) -> Result<()> {
    let mut src_snaps = rbd.snap_ls(img)?;
    src_snaps.sort();

    if src_snaps.is_empty() {
        warn!("{img}: no snapshots found");
        return Ok(());
    }

    if !tgt_images.contains(img) {
        let snap_name = src_snaps.first().map(|s| s.name.as_str()).unwrap();

        info!("{img}: backup is missing, creating from {snap_name}");

        let mut export = rbd.export(&format!("{img}@{snap_name}"))?;
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

        let mut export = rbd.export(&format!("{img}@{snap_name}"))?;
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

        let mut export = rbd.export_diff(img, &from_snap, &to_snap)?;
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
        rbd.snap_remove(img, snap)?;
    }

    Ok(())
}

fn now() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now()
}
