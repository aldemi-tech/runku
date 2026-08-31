//! Canonical UTC Cron names and five-field schedules used by Release manifests.

use std::{fmt, str::FromStr};

use runku_value::TimestampMicros;

use crate::ReleaseError;

const MAX_CRON_NAME_BYTES: usize = 64;
const MAX_EXPRESSION_BYTES: usize = 512;
const MICROS_PER_MINUTE: i64 = 60_000_000;
const MINUTES_PER_DAY: i64 = 1_440;
const MAX_SEARCH_MINUTES: u64 = 8 * 366 * 24 * 60;

/// Stable logical name of one Cron definition inside a Release.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CronName(String);

impl CronName {
    /// Returns the canonical name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CronName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for CronName {
    type Err = ReleaseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut bytes = value.bytes();
        let first = bytes.next().ok_or(ReleaseError::InvalidManifest)?;
        if value.len() > MAX_CRON_NAME_BYTES
            || !first.is_ascii_lowercase()
            || !bytes.all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'-' | b'_' | b'.')
            })
        {
            return Err(ReleaseError::InvalidManifest);
        }
        Ok(Self(value.to_owned()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CronField {
    bits: u64,
    minimum: u8,
    maximum: u8,
}

impl CronField {
    fn parse(value: &str, minimum: u8, maximum: u8) -> Result<Self, ReleaseError> {
        if value.is_empty() {
            return Err(ReleaseError::InvalidManifest);
        }
        let mut bits = 0_u64;
        for item in value.split(',') {
            let mut step_parts = item.split('/');
            let base = step_parts.next().ok_or(ReleaseError::InvalidManifest)?;
            let step = step_parts
                .next()
                .map(parse_number)
                .transpose()?
                .unwrap_or(1);
            if step_parts.next().is_some() || step == 0 || step > maximum - minimum + 1 {
                return Err(ReleaseError::InvalidManifest);
            }
            let (start, end) = if base == "*" {
                (minimum, maximum)
            } else if let Some((start, end)) = base.split_once('-') {
                let start = parse_number(start)?;
                let end = parse_number(end)?;
                if start < minimum || end > maximum || start >= end {
                    return Err(ReleaseError::InvalidManifest);
                }
                (start, end)
            } else {
                if step != 1 {
                    return Err(ReleaseError::InvalidManifest);
                }
                let number = parse_number(base)?;
                if number < minimum || number > maximum {
                    return Err(ReleaseError::InvalidManifest);
                }
                (number, number)
            };
            let mut current = start;
            loop {
                bits |= 1_u64 << current;
                let Some(next) = current.checked_add(step) else {
                    break;
                };
                if next > end {
                    break;
                }
                current = next;
            }
        }
        if bits == 0 {
            return Err(ReleaseError::InvalidManifest);
        }
        Ok(Self {
            bits,
            minimum,
            maximum,
        })
    }

    const fn contains(self, value: u8) -> bool {
        self.bits & (1_u64 << value) != 0
    }

    fn is_wildcard(self) -> bool {
        (self.minimum..=self.maximum).all(|value| self.contains(value))
    }

    fn canonical(self) -> String {
        if self.is_wildcard() {
            return "*".to_owned();
        }
        (self.minimum..=self.maximum)
            .filter(|value| self.contains(*value))
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// Canonical five-field, minute-granularity Cron schedule evaluated in UTC.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CronSchedule {
    canonical: String,
    minute: CronField,
    hour: CronField,
    day_of_month: CronField,
    month: CronField,
    day_of_week: CronField,
}

impl CronSchedule {
    /// Returns the normalized expression used by the manifest codec.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.canonical
    }

    /// Computes the first matching whole UTC minute strictly after `timestamp`.
    ///
    /// Search is bounded to eight leap-sized years so malformed/impossible schedules cannot hold a
    /// materializer forever.
    ///
    /// # Errors
    ///
    /// Returns a stable limit error on timestamp overflow or if no occurrence exists in the bound.
    pub fn next_after(&self, timestamp: TimestampMicros) -> Result<TimestampMicros, ReleaseError> {
        let current_minute = timestamp.get().div_euclid(MICROS_PER_MINUTE);
        for offset in 1..=MAX_SEARCH_MINUTES {
            let offset = i64::try_from(offset).map_err(|_| ReleaseError::Internal)?;
            let candidate_minute = current_minute
                .checked_add(offset)
                .ok_or(ReleaseError::LimitExceeded)?;
            if self.matches_minute(candidate_minute) {
                return candidate_minute
                    .checked_mul(MICROS_PER_MINUTE)
                    .map(TimestampMicros::new)
                    .ok_or(ReleaseError::LimitExceeded);
            }
        }
        Err(ReleaseError::LimitExceeded)
    }

    fn matches_minute(&self, unix_minute: i64) -> bool {
        let days = unix_minute.div_euclid(MINUTES_PER_DAY);
        let minute_of_day = unix_minute.rem_euclid(MINUTES_PER_DAY);
        let hour = u8::try_from(minute_of_day / 60).unwrap_or(0);
        let minute = u8::try_from(minute_of_day % 60).unwrap_or(0);
        let (_, month, day) = civil_from_days(days);
        let weekday = u8::try_from((days + 4).rem_euclid(7)).unwrap_or(0);
        let month_matches = self.month.contains(month);
        let day_of_month_matches = self.day_of_month.contains(day);
        let day_of_week_matches = self.day_of_week.contains(weekday);
        let day_matches = match (
            self.day_of_month.is_wildcard(),
            self.day_of_week.is_wildcard(),
        ) {
            (true, true) => true,
            (true, false) => day_of_week_matches,
            (false, true) => day_of_month_matches,
            (false, false) => day_of_month_matches || day_of_week_matches,
        };
        self.minute.contains(minute) && self.hour.contains(hour) && month_matches && day_matches
    }
}

impl fmt::Display for CronSchedule {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.canonical)
    }
}

impl FromStr for CronSchedule {
    type Err = ReleaseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() > MAX_EXPRESSION_BYTES || value.trim() != value || value.contains("  ") {
            return Err(ReleaseError::InvalidManifest);
        }
        let fields = value.split(' ').collect::<Vec<_>>();
        let [minute, hour, day_of_month, month, day_of_week] = fields.as_slice() else {
            return Err(ReleaseError::InvalidManifest);
        };
        let minute = CronField::parse(minute, 0, 59)?;
        let hour = CronField::parse(hour, 0, 23)?;
        let day_of_month = CronField::parse(day_of_month, 1, 31)?;
        let month = CronField::parse(month, 1, 12)?;
        let day_of_week = CronField::parse(day_of_week, 0, 6)?;
        let canonical = [
            minute.canonical(),
            hour.canonical(),
            day_of_month.canonical(),
            month.canonical(),
            day_of_week.canonical(),
        ]
        .join(" ");
        if canonical.len() > MAX_EXPRESSION_BYTES {
            return Err(ReleaseError::LimitExceeded);
        }
        Ok(Self {
            canonical,
            minute,
            hour,
            day_of_month,
            month,
            day_of_week,
        })
    }
}

fn parse_number(value: &str) -> Result<u8, ReleaseError> {
    if value.is_empty()
        || value.len() > 2
        || value.len() > 1 && value.starts_with('0')
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(ReleaseError::InvalidManifest);
    }
    value.parse().map_err(|_| ReleaseError::InvalidManifest)
}

fn civil_from_days(days_since_epoch: i64) -> (i64, u8, u8) {
    let shifted = days_since_epoch + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (
        year,
        u8::try_from(month).unwrap_or(1),
        u8::try_from(day).unwrap_or(1),
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn grammar_normalization_and_boundaries_are_exact() -> Result<(), Box<dyn Error>> {
        let schedule: CronSchedule = "*/15 0-2 * 1,12 1-5".parse()?;
        assert_eq!(schedule.as_str(), "0,15,30,45 0,1,2 * 1,12 1,2,3,4,5");
        for invalid in [
            "",
            "* * * *",
            "*  * * * *",
            "60 * * * *",
            "* 24 * * *",
            "* * 0 * *",
            "* * * 13 *",
            "* * * * 7",
            "*/0 * * * *",
            "1/2 * * * *",
            "01 * * * *",
            "JAN * * * *",
            "* * * * * *",
        ] {
            assert!(
                invalid.parse::<CronSchedule>().is_err(),
                "accepted {invalid}"
            );
        }
        Ok(())
    }

    #[test]
    fn utc_calendar_leap_year_and_dom_dow_or_are_stable() -> Result<(), Box<dyn Error>> {
        let every_minute: CronSchedule = "* * * * *".parse()?;
        assert_eq!(
            every_minute.next_after(TimestampMicros::new(0))?,
            TimestampMicros::new(MICROS_PER_MINUTE)
        );
        let leap_day: CronSchedule = "0 0 29 2 *".parse()?;
        let before_2024 = TimestampMicros::new(1_672_531_200_000_000);
        assert_eq!(
            leap_day.next_after(before_2024)?,
            TimestampMicros::new(1_709_164_800_000_000)
        );
        let first_of_month_or_monday: CronSchedule = "0 0 1 * 1".parse()?;
        let sunday_2024_09_01 = TimestampMicros::new(1_725_148_800_000_000 - 60_000_000);
        assert_eq!(
            first_of_month_or_monday.next_after(sunday_2024_09_01)?,
            TimestampMicros::new(1_725_148_800_000_000)
        );
        Ok(())
    }

    #[test]
    fn names_are_bounded_and_canonical() -> Result<(), Box<dyn Error>> {
        assert_eq!(
            "billing.daily".parse::<CronName>()?.as_str(),
            "billing.daily"
        );
        for invalid in ["", "Daily", "1daily", "daily/run", "daily cron"] {
            assert!(invalid.parse::<CronName>().is_err());
        }
        Ok(())
    }
}
