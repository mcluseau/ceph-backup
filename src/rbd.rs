pub mod source;
pub mod target;

use chrono::NaiveDateTime;
use eyre::{format_err, Result};
use log::error;

#[derive(Clone)]
pub struct Local<'t> {
    cluster: &'t str,
    pool: &'t str,
}

impl<'t> Local<'t> {
    pub fn new(cluster: &'t str, pool: &'t str) -> Self {
        Self { cluster, pool }
    }

    fn rbd_cmd(&self, args: Vec<&str>) -> (&str, Vec<String>) {
        let mut cmd_args = Vec::with_capacity(2 + args.len());

        for arg in ["--cluster", self.cluster] {
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

    pub fn export(&self, src_spec: &str) -> Result<duct::ReaderHandle> {
        let (cmd, args) = self.rbd_cmd(vec![
            "-p",
            self.pool,
            "export",
            "--no-progress",
            src_spec,
            "-",
        ]);
        Ok(duct::cmd(cmd, args).reader()?)
    }

    pub fn export_diff(
        &self,
        img: &str,
        from_snap: &str,
        to_snap: &str,
    ) -> Result<duct::ReaderHandle> {
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
        Ok(duct::cmd(cmd, args).reader()?)
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
            let stdin = rbd_import.stdin.as_mut().unwrap();

            let copy_result =
                std::io::copy(input, stdin).map_err(|e| format_err!("copy failed: {e}"));

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
                error!("failed to remove {img}: {e}");
            }
        }
        result
    }

    pub fn import_diff(&self, img: &str, input: &mut impl std::io::Read) -> Result<()> {
        let mut rollback_snap = self.snap_ls(img)?;
        rollback_snap.sort();
        let rollback_snap = rollback_snap.last().map(|s| &s.name).unwrap(); // assume a snapshot exists

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

        let result = || -> Result<()> {
            let stdin = rbd_import.stdin.as_mut().unwrap();

            let copy_result =
                std::io::copy(input, stdin).map_err(|e| format_err!("copy failed: {e}"));

            let status = rbd_import
                .wait()
                .map_err(|e| format_err!("rbd import-diff failed: {e}"))?;

            copy_result?;

            if !status.success() {
                return Err(eyre::format_err!(
                    "rbd import failed: status code: {:?}",
                    status.code()
                ));
            }
            Ok(())
        }();

        if result.is_err() {
            if let Err(e) = self.snap_rollback(img, rollback_snap) {
                error!("failed to rollback to {img}@{rollback_snap}: {e}");
            }
        }
        result
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
