pub mod source;
pub mod target;

use chrono::NaiveDateTime;
use eyre::{format_err, Result};
use log::{error, info, warn};
use std::collections::BTreeMap as Map;
use std::io::Read;

const KEY_PARTIAL: &str = "bck-partial";

#[derive(Clone)]
pub struct Local<'t> {
    client_id: &'t str,
    cluster: &'t str,
    pool: &'t str,
    buf_size: usize,
}

impl<'t> Local<'t> {
    pub fn new(client_id: &'t str, cluster: &'t str, pool: &'t str, buf_size: usize) -> Self {
        Self {
            client_id,
            cluster,
            pool,
            buf_size,
        }
    }

    fn rbd_cmd(&self, args: Vec<&str>) -> (&str, Vec<String>) {
        let mut cmd_args = Vec::with_capacity(2 + args.len());

        for arg in ["--id", self.client_id, "--cluster", self.cluster] {
            cmd_args.push(arg.to_string());
        }

        for arg in args {
            cmd_args.push(arg.to_string());
        }

        ("rbd", cmd_args)
    }

    pub fn run(&self, rbd_args: &[&str]) -> Result<()> {
        let mut args = vec!["-p", self.pool];
        args.extend_from_slice(&rbd_args);

        let (cmd, args) = self.rbd_cmd(args);
        duct::cmd(cmd, args.as_slice()).run()?;
        Ok(())
    }

    pub fn raw(&self, rbd_args: &[&str]) -> Result<Vec<u8>> {
        let mut args = vec!["-p", self.pool];
        args.extend_from_slice(&rbd_args);

        let (cmd, args) = self.rbd_cmd(args);
        let cmd = duct::cmd(cmd, args.as_slice());

        Ok(cmd.stdout_capture().run()?.stdout)
    }

    pub fn json<T: serde::de::DeserializeOwned>(&self, rbd_args: &[&str]) -> Result<T> {
        let mut args = vec!["-p", self.pool, "--format=json"];
        args.extend_from_slice(&rbd_args);

        let (cmd, args) = self.rbd_cmd(args);
        let cmd = duct::cmd(cmd, args.as_slice());

        let out = cmd.stdout_capture().run()?;
        Ok(serde_json::from_slice(&out.stdout.as_slice())?)
    }

    pub fn ls(&self) -> Result<Vec<String>> {
        self.json(&["ls"])
    }

    pub fn remove(&self, img: &str) -> Result<()> {
        self.run(&["remove", "--no-progress", img])
    }

    pub fn snap_ls(&self, img: &str) -> Result<Vec<Snapshot>> {
        let list: Vec<Snapshot> = self.json(&["snap", "ls", img])?;
        Ok(list
            .into_iter()
            .filter(|s| s.name.starts_with("bck-"))
            .collect())
    }

    pub fn snap_create(&self, img: &str, snap_name: &str) -> Result<()> {
        self.run(&[
            "snap",
            "create",
            "--no-progress",
            &format!("{img}@{snap_name}"),
        ])
    }
    pub fn snap_rollback(&self, img: &str, snap_name: &str) -> Result<()> {
        self.run(&[
            "snap",
            "rollback",
            "--no-progress",
            &format!("{img}@{snap_name}"),
        ])
    }
    pub fn snap_remove(&self, img: &str, snap_name: &str) -> Result<()> {
        self.run(&[
            "snap",
            "remove",
            "--no-progress",
            &format!("{img}@{snap_name}"),
        ])
    }

    pub fn trash_move(&self, img: &str) -> Result<()> {
        self.run(&["trash", "move", "--expires-at=30 days", img])
    }

    pub fn export(&self, src_spec: &str) -> Result<impl Read> {
        let (cmd, args) = self.rbd_cmd(vec![
            "-p",
            self.pool,
            "export",
            "--no-progress",
            src_spec,
            "-",
        ]);
        Ok(self.reader(duct::cmd(cmd, args).reader()?))
    }

    pub fn export_diff(&self, img: &str, from_snap: &str, to_snap: &str) -> Result<impl Read> {
        let (cmd, args) = self.rbd_cmd(vec![
            "-p",
            self.pool,
            "export-diff",
            "--no-progress",
            "--from-snap",
            from_snap,
            &format!("{img}@{to_snap}"),
            "-",
        ]);
        Ok(self.reader(duct::cmd(cmd, args).reader()?))
    }

    fn reader<R: std::io::Read>(&self, input: R) -> impl Read {
        std::io::BufReader::with_capacity(self.buf_size, input)
    }

    pub fn import(&self, img: &str, snap_name: &str, input: &mut impl std::io::Read) -> Result<()> {
        let (cmd, args) = self.rbd_cmd(vec![
            "--dest-pool",
            self.pool,
            "import",
            "--no-progress",
            "-",
            img,
        ]);

        let mut rbd_import = std::process::Command::new(cmd)
            .args(args)
            .stdin(std::process::Stdio::piped())
            .spawn()?;

        let result = || -> Result<()> {
            let mut input = self.reader(input);
            let stdin = rbd_import.stdin.as_mut().unwrap();

            let copy_result =
                std::io::copy(&mut input, stdin).map_err(|e| format_err!("copy failed: {e}"));

            let status = rbd_import
                .wait()
                .map_err(|e| format_err!("rbd import failed: {e}"))?;

            copy_result?;

            if !status.success() {
                return Err(eyre::format_err!(
                    "rbd import failed: status code: {:?}",
                    status.code()
                ));
            }

            self.snap_create(img, snap_name)
                .map_err(|e| format_err!("snapshot create failed: {e}"))
        }();

        if result.is_err() {
            if let Err(e) = self.remove(img) {
                error!("{img}: failed to remove: {e}");
            }
            return result;
        }

        if let Err(e) = self.meta_set(img, KEY_PARTIAL, &false) {
            warn!("{img}: failed to set image-meta: {e}");
        }

        Ok(())
    }

    pub fn meta_list(&self, img: &str) -> Result<Map<String, String>> {
        let data = self.raw(&["--format=json", "image-meta", "list", img])?;
        if data.is_empty() {
            // returns empty output if none (not "{}")
            return Ok(Map::new());
        }
        Ok(serde_json::from_slice(data.as_slice())
            .map_err(|e| format_err!("failed to parse image-meta list: {e}"))?)
    }
    pub fn meta_get<T: serde::de::DeserializeOwned>(
        &self,
        img: &str,
        key: &str,
    ) -> Result<Option<T>> {
        // when coding, it's better this way than using image-meta get
        let kvs = self.meta_list(img)?;
        let value = kvs.get(key);
        let Some(value) = value else {
            return Ok(None);
        };
        Ok(serde_json::from_str(&value)?)
    }
    pub fn meta_set<T: serde::Serialize>(&self, img: &str, key: &str, value: &T) -> Result<()> {
        let value = serde_json::to_string(value)?;
        self.run(&["image-meta", "set", img, key, &value])
    }
    pub fn meta_rm(&self, img: &str, key: &str) -> Result<()> {
        self.run(&["image-meta", "remove", img, key])
    }

    pub fn meta_sync(&self, img: &str, kvs: &Map<String, String>) -> Result<()> {
        let current = self.meta_list(img)?;

        for (k, v) in kvs {
            if k.starts_with("bck-") {
                continue; // bck- prefix is reserved
            }
            if current.get(k) != Some(v) {
                self.meta_set(img, k, v)?;
            }
        }

        for k in current.keys() {
            if k.starts_with("bck-") {
                continue; // bck- prefix is reserved
            }
            if kvs.get(k).is_none() {
                self.meta_rm(img, k)?;
            }
        }

        Ok(())
    }

    pub fn need_rollback(&self, img: &str) -> Result<bool> {
        Ok(match self.meta_get(img, KEY_PARTIAL)? {
            // partial import detected, rollback to snapshot
            // if no partial import info, assume it is partial too
            None | Some(true) => true,
            // no partial import
            Some(false) => false,
        })
    }

    pub fn rollback(&self, img: &str) -> Result<()> {
        let mut rollback_snap = self.snap_ls(img)?;
        rollback_snap.sort();
        let rollback_snap = rollback_snap.last().map(|s| &s.name).unwrap(); // assume a snapshot exists

        self.snap_rollback(img, rollback_snap)?;
        self.meta_set(img, KEY_PARTIAL, &false)
    }

    pub fn import_diff(&self, img: &str, input: &mut impl std::io::Read) -> Result<()> {
        if self.meta_get(img, KEY_PARTIAL)?.unwrap_or(true) {
            return Err(format_err!(
                "image may have a partial import, prepare for import first"
            ));
        }

        self.meta_set(img, KEY_PARTIAL, &true)?;

        let (cmd, args) = self.rbd_cmd(vec![
            "--pool",
            self.pool,
            "import-diff",
            "--no-progress",
            "-",
            img,
        ]);

        let mut rbd_import = std::process::Command::new(cmd)
            .args(args)
            .stdin(std::process::Stdio::piped())
            .spawn()?;

        let mut input = self.reader(input);
        let stdin = rbd_import.stdin.as_mut().unwrap();

        let copy_result =
            std::io::copy(&mut input, stdin).map_err(|e| format_err!("copy failed: {e}"));

        let status = rbd_import
            .wait()
            .map_err(|e| format_err!("rbd import-diff failed: {e}"))?;

        copy_result?;

        if !status.success() {
            return Err(eyre::format_err!(
                "rbd import-diff failed: status code: {:?}",
                status.code()
            ));
        }

        self.meta_set(img, KEY_PARTIAL, &false)
    }

    fn expire_backups(&self, img: &str, deadline: &chrono::NaiveDateTime) -> Result<()> {
        let snaps = self
            .snap_ls(&img)
            .map_err(|e| format_err!("failed to list snapshots: {e}"))?;

        let mut removed = 0;
        for snap in &snaps {
            let Ok(ts) = snap.timestamp() else {
                continue;
            };
            if &ts >= deadline {
                continue;
            }

            let snap_name = &snap.name;
            info!("{img}: removing snapshot {snap_name}");
            self.snap_remove(&img, &snap.name)
                .map_err(|e| format_err!("failed to remove snapshot {snap_name}: {e}"))?;

            removed += 1;
        }

        if removed == snaps.len() {
            info!("{img}: no more snapshots, removing");
            if let Err(e) = self.remove(&img) {
                warn!("{img}: failed to remove: {e}");
            }
        }
        Ok(())
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Eq, Ord, PartialEq, PartialOrd)]
pub struct Snapshot {
    pub id: u64,
    pub name: String,
    protected: String,
    pub size: u64,
    timestamp: String,
}
impl Snapshot {
    #[allow(unused)]
    pub fn protected(&self) -> bool {
        self.protected == "true"
    }
    pub fn timestamp(&self) -> Result<NaiveDateTime> {
        Ok(NaiveDateTime::parse_from_str(
            &self.timestamp,
            "%a %b %d %H:%M:%S %Y",
        )?)
    }
}
