use chronicle_proto::pb_catalog::{UnitRegistration, UnitStatus};
use std::collections::HashMap;

fn pressure(u: &UnitRegistration) -> f64 {
    0.4 * u.cpu_usage + 0.4 * u.memory_usage + 0.2 * u.disk_usage
}

/// Select an ensemble of `rf` units with a three-tier priority:
///
/// 1. **Previous ensemble** — keep members from the prior ensemble if still
///    writable (minimizes data movement on term changes)
/// 2. **Zone diversity** — round-robin across zones so replicas span
///    failure domains
/// 3. **Lowest pressure** — within each zone, pick the unit with the
///    lowest pressure (CPU/memory/disk)
///
/// - `previous`: addresses from the prior ensemble — preferred if still writable
/// - `exclude`: addresses to never select
pub fn select_ensemble(
    units: &[UnitRegistration],
    rf: usize,
    previous: &[String],
    exclude: &[String],
) -> Option<Vec<String>> {
    let candidates: Vec<&UnitRegistration> = units
        .iter()
        .filter(|u| u.status() == UnitStatus::Writable && !exclude.contains(&u.address))
        .collect();

    if candidates.len() < rf {
        return None;
    }

    let mut ensemble: Vec<String> = Vec::with_capacity(rf);
    let mut used_zones: HashMap<&str, usize> = HashMap::new();

    // Phase 1: retain previous ensemble members that are still writable
    for addr in previous {
        if ensemble.len() >= rf {
            break;
        }
        if let Some(u) = candidates.iter().find(|u| u.address == *addr) {
            ensemble.push(u.address.clone());
            let zone = if u.zone.is_empty() { "default" } else { u.zone.as_str() };
            *used_zones.entry(zone).or_default() += 1;
        }
    }

    if ensemble.len() >= rf {
        return Some(ensemble);
    }

    // Phase 2: fill remaining slots with zone-diverse, low-pressure units
    let remaining: Vec<&UnitRegistration> = candidates
        .iter()
        .filter(|u| !ensemble.contains(&u.address))
        .copied()
        .collect();

    // Group by zone, sort each group by pressure (ascending)
    let mut by_zone: HashMap<&str, Vec<&UnitRegistration>> = HashMap::new();
    for u in &remaining {
        let zone = if u.zone.is_empty() { "default" } else { u.zone.as_str() };
        by_zone.entry(zone).or_default().push(u);
    }
    for group in by_zone.values_mut() {
        group.sort_by(|a, b| {
            pressure(a)
                .partial_cmp(&pressure(b))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    // Sort zones: prefer zones with fewer already-selected units
    let mut zone_keys: Vec<&str> = by_zone.keys().copied().collect();
    zone_keys.sort_by(|a, b| {
        let count_a = used_zones.get(a).copied().unwrap_or(0);
        let count_b = used_zones.get(b).copied().unwrap_or(0);
        count_a.cmp(&count_b).then(a.cmp(b))
    });

    let mut zone_cursors = vec![0usize; zone_keys.len()];

    let mut passes = 0;
    while ensemble.len() < rf {
        let mut added_this_pass = false;
        for (zi, zone) in zone_keys.iter().enumerate() {
            if ensemble.len() >= rf {
                break;
            }
            let group = &by_zone[zone];
            if zone_cursors[zi] < group.len() {
                ensemble.push(group[zone_cursors[zi]].address.clone());
                zone_cursors[zi] += 1;
                added_this_pass = true;
            }
        }
        if !added_this_pass {
            break;
        }
        passes += 1;
        if passes > rf {
            break;
        }
    }

    if ensemble.len() >= rf {
        Some(ensemble)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unit(addr: &str, zone: &str) -> UnitRegistration {
        UnitRegistration {
            address: addr.into(),
            zone: zone.into(),
            status: UnitStatus::Writable as i32,
            cpu_usage: 0.0,
            memory_usage: 0.0,
            disk_usage: 0.0,
        }
    }

    fn unit_with_load(addr: &str, zone: &str, cpu: f64, mem: f64, disk: f64) -> UnitRegistration {
        UnitRegistration {
            address: addr.into(),
            zone: zone.into(),
            status: UnitStatus::Writable as i32,
            cpu_usage: cpu,
            memory_usage: mem,
            disk_usage: disk,
        }
    }

    #[test]
    fn basic_select() {
        let units = vec![
            unit("a", "us-east-1"),
            unit("b", "us-west-2"),
            unit("c", "eu-west-1"),
        ];
        let result = select_ensemble(&units, 2, &[], &[]).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn previous_ensemble_preferred() {
        let units = vec![
            unit("a", "zone-a"),
            unit("b", "zone-b"),
            unit("c", "zone-c"),
            unit("d", "zone-d"),
        ];
        let prev = vec!["b".into(), "c".into()];
        let result = select_ensemble(&units, 3, &prev, &[]).unwrap();
        assert!(result.contains(&"b".to_string()));
        assert!(result.contains(&"c".to_string()));
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn previous_skips_non_writable() {
        let mut b = unit("b", "zone-b");
        b.status = UnitStatus::Readonly as i32;
        let units = vec![
            unit("a", "zone-a"),
            b,
            unit("c", "zone-c"),
            unit("d", "zone-d"),
        ];
        let prev = vec!["b".into(), "c".into()];
        let result = select_ensemble(&units, 2, &prev, &[]).unwrap();
        assert!(!result.contains(&"b".to_string()));
        assert!(result.contains(&"c".to_string()));
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn zone_spread() {
        let units = vec![
            unit("a1", "zone-a"),
            unit("a2", "zone-a"),
            unit("b1", "zone-b"),
            unit("b2", "zone-b"),
            unit("c1", "zone-c"),
        ];
        let result = select_ensemble(&units, 3, &[], &[]).unwrap();
        let zones: Vec<&str> = result
            .iter()
            .map(|addr| {
                let u = units.iter().find(|u| u.address == *addr).unwrap();
                u.zone.as_str()
            })
            .collect();
        let mut unique = zones.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), 3, "should pick from 3 different zones");
    }

    #[test]
    fn prefers_low_pressure() {
        let units = vec![
            unit_with_load("hot", "zone-a", 0.9, 0.8, 0.5),
            unit_with_load("cold", "zone-a", 0.1, 0.1, 0.1),
            unit_with_load("other", "zone-b", 0.2, 0.2, 0.1),
        ];
        let result = select_ensemble(&units, 2, &[], &[]).unwrap();
        assert!(result.contains(&"cold".to_string()));
        assert!(result.contains(&"other".to_string()));
    }

    #[test]
    fn zone_diversity_after_previous() {
        let units = vec![
            unit("a1", "zone-a"),
            unit("a2", "zone-a"),
            unit("a3", "zone-a"),
            unit("b1", "zone-b"),
        ];
        let prev = vec!["a1".into(), "a2".into()];
        let result = select_ensemble(&units, 3, &prev, &[]).unwrap();
        assert!(result.contains(&"a1".to_string()));
        assert!(result.contains(&"a2".to_string()));
        assert!(result.contains(&"b1".to_string()));
    }

    #[test]
    fn respects_exclude() {
        let units = vec![
            unit("a", "zone-a"),
            unit("b", "zone-b"),
            unit("c", "zone-c"),
        ];
        let result = select_ensemble(&units, 2, &[], &["a".into()]).unwrap();
        assert!(!result.contains(&"a".to_string()));
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn insufficient_units() {
        let units = vec![unit("a", "zone-a")];
        assert!(select_ensemble(&units, 2, &[], &[]).is_none());
    }

    #[test]
    fn skips_non_writable() {
        let mut u = unit("a", "zone-a");
        u.status = UnitStatus::Readonly as i32;
        let units = vec![u, unit("b", "zone-b")];
        assert!(select_ensemble(&units, 2, &[], &[]).is_none());
    }

    #[test]
    fn single_zone_fallback() {
        let units = vec![
            unit("a", "zone-a"),
            unit("b", "zone-a"),
            unit("c", "zone-a"),
        ];
        let result = select_ensemble(&units, 3, &[], &[]).unwrap();
        assert_eq!(result.len(), 3);
    }
}
