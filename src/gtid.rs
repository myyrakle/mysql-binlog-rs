//! GTID (Global Transaction ID) 관리
//!
//! GTID 형식: UUID:sequence-number
//! 여러 서버의 GTID 집합을 추적: "uuid1:1-100,uuid2:1-50"

use crate::error::{CdcError, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use regex::Regex;

/// GTID 범위 (sequence 범위)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct GtidRange {
    pub start: u64,
    pub end: u64,
}

impl GtidRange {
    pub fn new(start: u64, end: u64) -> Result<Self> {
        if start > end {
            return Err(CdcError::GtidError("Invalid range: start > end".to_string()));
        }
        Ok(GtidRange { start, end })
    }

    pub fn contains(&self, value: u64) -> bool {
        value >= self.start && value <= self.end
    }

    pub fn merge(&self, other: &GtidRange) -> Option<GtidRange> {
        // 연접한 범위 병합
        if self.end + 1 >= other.start && other.end + 1 >= self.start {
            Some(GtidRange {
                start: self.start.min(other.start),
                end: self.end.max(other.end),
            })
        } else {
            None
        }
    }
}

/// UUID별 GTID 범위들
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UUIDGtidSet {
    pub uuid: String,
    pub ranges: Vec<GtidRange>,
}

impl UUIDGtidSet {
    pub fn new(uuid: String) -> Self {
        UUIDGtidSet {
            uuid,
            ranges: Vec::new(),
        }
    }

    pub fn add_gtid(&mut self, sequence: u64) -> Result<()> {
        let range = GtidRange::new(sequence, sequence)?;

        // 기존 범위와 병합 시도
        for i in 0..self.ranges.len() {
            if let Some(merged) = self.ranges[i].merge(&range) {
                self.ranges[i] = merged;
                // 다음 범위와도 병합 가능한지 확인
                if i + 1 < self.ranges.len() {
                    if let Some(merged_again) = self.ranges[i].merge(&self.ranges[i + 1]) {
                        self.ranges[i] = merged_again;
                        self.ranges.remove(i + 1);
                    }
                }
                return Ok(());
            }
        }

        // 새 범위 추가
        self.ranges.push(range);
        self.ranges.sort();
        Ok(())
    }

    pub fn contains(&self, sequence: u64) -> bool {
        self.ranges.iter().any(|r| r.contains(sequence))
    }

    pub fn to_string(&self) -> String {
        let range_strs: Vec<String> = self.ranges.iter()
            .map(|r| {
                if r.start == r.end {
                    r.start.to_string()
                } else {
                    format!("{}-{}", r.start, r.end)
                }
            })
            .collect();
        range_strs.join(",")
    }
}

/// 전체 GTID 집합 (여러 UUID)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GtidSet {
    pub sets: BTreeMap<String, UUIDGtidSet>,
}

impl GtidSet {
    pub fn new() -> Self {
        GtidSet {
            sets: BTreeMap::new(),
        }
    }

    /// GTID 문자열 파싱 (format: "uuid1:1-100,200,uuid2:1-50")
    pub fn parse(gtid_str: &str) -> Result<Self> {
        let mut gtid_set = GtidSet::new();

        if gtid_str.is_empty() || gtid_str == "NULL" {
            return Ok(gtid_set);
        }

        // 간단한 파싱 방식: split(':')과 ','를 이용
        let mut i = 0;
        let chars: Vec<char> = gtid_str.chars().collect();

        while i < chars.len() {
            // UUID 파싱
            let uuid_start = i;
            while i < chars.len() && chars[i] != ':' {
                i += 1;
            }

            if i >= chars.len() {
                break;
            }

            let uuid = chars[uuid_start..i].iter().collect::<String>();
            i += 1; // ':'  skip

            // 범위 파싱
            let ranges_start = i;
            while i < chars.len() && chars[i] != ',' || (i > 0 && i + 1 < chars.len() && is_uuid_start(&chars, i + 1)) {
                if chars[i] == ',' && is_uuid_start(&chars, i + 1) {
                    break;
                }
                i += 1;
            }

            let ranges_str = chars[ranges_start..i].iter().collect::<String>();
            let mut uuid_gtid_set = UUIDGtidSet::new(uuid);

            // 범위 문자열 파싱 (1-100, 200, 300-400)
            for range_part in ranges_str.split(',') {
                let range_part = range_part.trim();
                if range_part.is_empty() {
                    continue;
                }
                if range_part.contains('-') && !range_part.starts_with('-') {
                    let parts: Vec<&str> = range_part.split('-').collect();
                    if parts.len() == 2 {
                        let start = parts[0].parse::<u64>()
                            .map_err(|_| CdcError::GtidError(format!("Invalid range: {}", range_part)))?;
                        let end = parts[1].parse::<u64>()
                            .map_err(|_| CdcError::GtidError(format!("Invalid range: {}", range_part)))?;
                        uuid_gtid_set.ranges.push(GtidRange::new(start, end)?);
                    }
                } else {
                    let seq = range_part.parse::<u64>()
                        .map_err(|_| CdcError::GtidError(format!("Invalid sequence: {}", range_part)))?;
                    uuid_gtid_set.add_gtid(seq)?;
                }
            }

            gtid_set.sets.insert(uuid_gtid_set.uuid.clone(), uuid_gtid_set);

            if i < chars.len() && chars[i] == ',' {
                i += 1;
            }
        }

        Ok(gtid_set)
    }

    pub fn add_gtid(&mut self, gtid: &str) -> Result<()> {
        // gtid format: "uuid:sequence"
        let parts: Vec<&str> = gtid.split(':').collect();
        if parts.len() != 2 {
            return Err(CdcError::GtidError(format!("Invalid GTID format: {}", gtid)));
        }

        let uuid = parts[0].to_string();
        let sequence = parts[1].parse::<u64>()
            .map_err(|_| CdcError::GtidError(format!("Invalid sequence: {}", parts[1])))?;

        let uuid_set = self.sets.entry(uuid.clone())
            .or_insert_with(|| UUIDGtidSet::new(uuid.clone()));
        uuid_set.add_gtid(sequence)
    }

    pub fn contains(&self, gtid: &str) -> bool {
        let parts: Vec<&str> = gtid.split(':').collect();
        if parts.len() != 2 {
            return false;
        }

        let uuid = parts[0];
        if let Ok(sequence) = parts[1].parse::<u64>() {
            if let Some(uuid_set) = self.sets.get(uuid) {
                return uuid_set.contains(sequence);
            }
        }
        false
    }

    pub fn subtract(&self, other: &GtidSet) -> GtidSet {
        let mut result = self.clone();

        for (uuid, other_set) in &other.sets {
            if let Some(result_set) = result.sets.get_mut(uuid) {
                // 각 범위를 빼기
                for other_range in &other_set.ranges {
                    let mut new_ranges = Vec::new();
                    for range in &result_set.ranges {
                        if range.end < other_range.start || range.start > other_range.end {
                            // 겹치지 않음
                            new_ranges.push(*range);
                        } else {
                            // 겹침 - 부분 제거
                            if range.start < other_range.start {
                                new_ranges.push(GtidRange::new(range.start, other_range.start - 1).unwrap());
                            }
                            if range.end > other_range.end {
                                new_ranges.push(GtidRange::new(other_range.end + 1, range.end).unwrap());
                            }
                        }
                    }
                    result_set.ranges = new_ranges;
                }
            }
        }

        result
    }

    pub fn to_string(&self) -> String {
        if self.sets.is_empty() {
            return String::new();
        }

        let mut parts = Vec::new();
        for (uuid, uuid_set) in &self.sets {
            let ranges_str = uuid_set.to_string();
            if !ranges_str.is_empty() {
                parts.push(format!("{}:{}", uuid, ranges_str));
            }
        }
        parts.join(",")
    }

    pub fn is_empty(&self) -> bool {
        self.sets.iter().all(|(_, set)| set.ranges.is_empty())
    }
}

/// UUID 시작 여부 확인 (간단한 휴리스틱)
fn is_uuid_start(chars: &[char], pos: usize) -> bool {
    if pos + 3 >= chars.len() {
        return false;
    }

    // UUID는 보통 16진수와 '-'를 포함하고 ':' 전에 여러 문자를 가짐
    let mut hex_count = 0;
    for i in pos..pos.saturating_add(10).min(chars.len()) {
        if chars[i].is_ascii_hexdigit() || chars[i] == '-' {
            hex_count += 1;
        } else if chars[i] == ':' {
            return hex_count > 8;
        } else {
            return false;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gtid_parse() {
        let gtid_str = "550e8400-e29b-41d4-a716-446655440000:1-100,200,300-400";
        let gtid_set = GtidSet::parse(gtid_str).unwrap();
        assert!(!gtid_set.sets.is_empty());
    }

    #[test]
    fn test_gtid_contains() {
        let mut gtid_set = GtidSet::new();
        gtid_set.add_gtid("550e8400-e29b-41d4-a716-446655440000:50").unwrap();
        assert!(gtid_set.contains("550e8400-e29b-41d4-a716-446655440000:50"));
        assert!(!gtid_set.contains("550e8400-e29b-41d4-a716-446655440000:51"));
    }
}
