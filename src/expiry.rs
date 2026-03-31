use chrono::{DateTime, Datelike, NaiveDate, Utc};
use std::collections::{btree_map::Entry, BTreeMap as Map, BTreeSet as Set};

pub struct Expirer {
    pub days: u16,
    pub weeks: u8,
    pub months: u8,
}

impl Expirer {
    pub fn expired<'t, T>(&self, t0: DateTime<Utc>, set: &'t [(DateTime<Utc>, T)]) -> Vec<&'t T> {
        let d0 = t0.date_naive();

        let mut by_bucket = Map::new();
        for (idx, (dt, _)) in set.iter().enumerate() {
            let Some(b) = self.date_bucket(d0, dt.date_naive()) else {
                continue;
            };

            match by_bucket.entry(b) {
                Entry::Vacant(e) => {
                    e.insert(idx);
                }
                Entry::Occupied(mut e) if *dt > set[*e.get()].0 => {
                    e.insert(idx);
                }
                _ => {}
            }
        }

        let idx_to_keep: Set<_> = by_bucket.into_values().collect();

        set.iter()
            .enumerate()
            .filter(|(idx, _)| !idx_to_keep.contains(idx))
            .map(|(_, (_, t))| t)
            .collect()
    }

    fn date_bucket(&self, d0: NaiveDate, d: NaiveDate) -> Option<Bucket> {
        let age_days = (d0 - d).num_days();

        if age_days < 0 {
            return None; // invalid
        }

        if age_days < self.days as i64 {
            return Some(Bucket::Day(d));
        }

        if age_days < self.days as i64 + 7 * self.weeks as i64 {
            let iso = d.iso_week();
            return Some(Bucket::Week(iso.year(), iso.week()));
        }

        let months_delta = (d0.year() - d.year()) * 12 + (d0.month() as i32 - d.month() as i32);

        if months_delta < self.months as i64 as i32 {
            return Some(Bucket::Month(d.year(), d.month()));
        }

        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Bucket {
    Day(NaiveDate),
    Week(i32, u32),  // (year, week)
    Month(i32, u32), // (year, month)
}

#[test]
fn retains_expected_backups_over_time() {
    use chrono::{Duration, Utc};

    let expirer = Expirer {
        days: 7,
        weeks: 5,
        months: 3,
    };

    let max = 7 + 5 + 3;
    let max_range = (max - 2)..=max;

    let mut t0 = Utc::now();
    let mut n = 0;

    let mut prev_count = 0;

    enum Phase {
        Growth,
        Plateau,
    }

    let mut phase = Phase::Growth;

    // simulate daily backups & expiry over 10 years
    let mut backups = Vec::new();
    for d in 0..(365 * 10) {
        t0 += Duration::days(1);
        n += 1;

        backups.push((t0, n));

        let expired = expirer.expired(t0, &backups);
        let expired: Set<_> = expired.into_iter().cloned().collect();

        backups.retain(|(_, n)| !expired.contains(n));

        let count = backups.len();
        let in_plateau = max_range.contains(&count);

        assert!(count <= max, "day {d}: {count} > {max}");

        match phase {
            Phase::Growth => {
                assert!(
                    count >= prev_count,
                    "day {d}: count decreased: {prev_count} -> {count}"
                );
                prev_count = count;

                if in_plateau {
                    phase = Phase::Plateau
                }
            }
            Phase::Plateau => {
                assert!(
                    in_plateau,
                    "day {d}: count got outside plateau: {count} not in {max_range:?}"
                );
            }
        }

        let mut seen = Set::new();
        for (dt, _) in &backups {
            let Some(b) = expirer.date_bucket(t0.date_naive(), dt.date_naive()) else {
                panic!("day {d}: unwanted backup kept");
            };
            assert!(seen.insert(b.clone()), "day {d}: duplicate bucket: {b:?}");
        }
    }

    assert!(
        max_range.contains(&backups.len()),
        "final count {} not in {:?}",
        backups.len(),
        max_range
    );
}
