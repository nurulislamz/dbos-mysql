//! Five-field UTC cron: `minute hour day-of-month month day-of-week`.
//!
//! A star in both day fields means every day. Setting both is rejected, because the two
//! conventions (and, or) disagree and none of the schedules that need this use both.

use crate::Error;

const MINUTE_MS: i64 = 60_000;
// Feb 29 repeats every four years, so a match further away than that does not exist.
const SEARCH_MINUTES: usize = 4 * 366 * 24 * 60;

#[derive(Clone, Debug)]
pub struct Cron {
    minute: u64,
    hour: u64,
    dom: u64,
    month: u64,
    dow: u64,
    dom_star: bool,
    dow_star: bool,
}

impl Cron {
    pub fn parse(expression: &str) -> Result<Self, Error> {
        let fields: Vec<&str> = expression.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(invalid(expression));
        }
        let cron = Self {
            minute: field(fields[0], 0, 59).map_err(|()| invalid(expression))?,
            hour: field(fields[1], 0, 23).map_err(|()| invalid(expression))?,
            dom: field(fields[2], 1, 31).map_err(|()| invalid(expression))?,
            month: field(fields[3], 1, 12).map_err(|()| invalid(expression))?,
            dow: weekday(fields[4]).map_err(|()| invalid(expression))?,
            dom_star: fields[2] == "*",
            dow_star: fields[4] == "*",
        };
        if !cron.dom_star && !cron.dow_star {
            return Err(Error::InvalidConfig(format!(
                "cron {expression:?} sets both a day of the month and a day of the week. Set one of them"
            )));
        }
        if cron.next_after(0).is_none() {
            return Err(invalid(expression));
        }
        Ok(cron)
    }

    /// The latest tick at or before `now_ms` and strictly after `last_fired_ms`.
    pub fn latest_due(&self, last_fired_ms: i64, now_ms: i64) -> Option<i64> {
        let minute = floor_minute(now_ms);
        if last_fired_ms >= minute {
            return None;
        }
        let mut cursor = Minute::from_unix_ms(minute);
        for _ in 0..SEARCH_MINUTES {
            let tick = cursor.unix_ms();
            if tick <= last_fired_ms {
                return None;
            }
            if self.matches(&cursor) && tick <= now_ms {
                return Some(tick);
            }
            if !cursor.retreat() {
                return None;
            }
        }
        None
    }

    fn next_after(&self, after_ms: i64) -> Option<i64> {
        let mut cursor = Minute::from_unix_ms(floor_minute(after_ms));
        if !cursor.advance() {
            return None;
        }
        for _ in 0..SEARCH_MINUTES {
            if self.matches(&cursor) {
                return Some(cursor.unix_ms());
            }
            if !cursor.advance() {
                return None;
            }
        }
        None
    }

    fn matches(&self, minute: &Minute) -> bool {
        bit(self.minute, minute.minute)
            && bit(self.hour, minute.hour)
            && bit(self.month, minute.month)
            && if self.dom_star {
                bit(self.dow, minute.weekday())
            } else {
                bit(self.dom, minute.day)
            }
    }
}

fn invalid(expression: &str) -> Error {
    Error::InvalidConfig(format!(
        "cron {expression:?} is not a five-field UTC expression. Use minute, hour, day of month, month, and day of week"
    ))
}

fn bit(field: u64, value: u32) -> bool {
    value < 64 && field & (1 << value) != 0
}

fn weekday(text: &str) -> Result<u64, ()> {
    let mut bits = field(text, 0, 7)?;
    if bits & (1 << 7) != 0 {
        bits |= 1;
        bits &= !(1 << 7);
    }
    Ok(bits)
}

fn field(text: &str, min: u32, max: u32) -> Result<u64, ()> {
    if text.is_empty() {
        return Err(());
    }
    let mut bits = 0_u64;
    for part in text.split(',') {
        bits |= part_bits(part, min, max)?;
    }
    if bits == 0 {
        return Err(());
    }
    Ok(bits)
}

fn part_bits(part: &str, min: u32, max: u32) -> Result<u64, ()> {
    let (range, step) = part.split_once('/').unwrap_or((part, "1"));
    let step: u32 = step.parse().map_err(|_: std::num::ParseIntError| ())?;
    if step == 0 {
        return Err(());
    }
    let (start, end) = if range == "*" {
        (min, max)
    } else if let Some((left, right)) = range.split_once('-') {
        (number(left, min, max)?, number(right, min, max)?)
    } else {
        let value = number(range, min, max)?;
        (value, value)
    };
    if start > end {
        return Err(());
    }
    let mut bits = 0_u64;
    let mut value = start;
    loop {
        bits |= 1 << value;
        let Some(next) = value.checked_add(step) else {
            break;
        };
        if next > end {
            break;
        }
        value = next;
    }
    Ok(bits)
}

fn number(text: &str, min: u32, max: u32) -> Result<u32, ()> {
    let value: u32 = text.parse().map_err(|_: std::num::ParseIntError| ())?;
    if value < min || value > max {
        return Err(());
    }
    Ok(value)
}

fn floor_minute(ms: i64) -> i64 {
    ms - ms.rem_euclid(MINUTE_MS)
}

struct Minute {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
}

impl Minute {
    fn from_unix_ms(ms: i64) -> Self {
        let seconds = ms.div_euclid(1000);
        let days = seconds.div_euclid(86_400);
        let second_of_day = seconds.rem_euclid(86_400) as u32;
        let (year, month, day) = civil_from_days(i32::try_from(days).unwrap_or(i32::MAX));
        Self {
            year,
            month,
            day,
            hour: second_of_day / 3600,
            minute: (second_of_day % 3600) / 60,
        }
    }

    fn unix_ms(&self) -> i64 {
        let days = i64::from(days_from_civil(self.year, self.month, self.day));
        (days * 86_400 + i64::from(self.hour) * 3600 + i64::from(self.minute) * 60) * 1000
    }

    fn weekday(&self) -> u32 {
        // 1970-01-01 was a Thursday, which is 4 when Sunday is 0.
        (days_from_civil(self.year, self.month, self.day) + 4).rem_euclid(7) as u32
    }

    fn advance(&mut self) -> bool {
        self.minute += 1;
        if self.minute == 60 {
            self.minute = 0;
            self.hour += 1;
        }
        if self.hour == 24 {
            self.hour = 0;
            self.day += 1;
            if self.day > days_in_month(self.year, self.month) {
                self.day = 1;
                self.month += 1;
                if self.month == 13 {
                    self.month = 1;
                    self.year += 1;
                }
            }
        }
        self.year < 10_000
    }

    fn retreat(&mut self) -> bool {
        if self.minute > 0 {
            self.minute -= 1;
            return true;
        }
        self.minute = 59;
        if self.hour > 0 {
            self.hour -= 1;
            return true;
        }
        self.hour = 23;
        if self.day > 1 {
            self.day -= 1;
            return true;
        }
        if self.month == 1 {
            self.month = 12;
            self.year -= 1;
        } else {
            self.month -= 1;
        }
        self.day = days_in_month(self.year, self.month);
        self.year > 0
    }
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(year) => 29,
        2 => 28,
        _ => 30,
    }
}

fn is_leap(year: i32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn days_from_civil(mut year: i32, month: u32, day: u32) -> i32 {
    year -= i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = (year - era * 400) as u32;
    let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era as i32 - 719_468
}

fn civil_from_days(mut days: i32) -> (i32, u32, u32) {
    days += 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = (days - era * 146_097) as u32;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146_096) / 365;
    let year = year_of_era as i32 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_part = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_part + 2) / 5 + 1;
    let month = if month_part < 10 {
        month_part + 3
    } else {
        month_part - 9
    };
    (year + i32::from(month <= 2), month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> i64 {
        Minute {
            year,
            month,
            day,
            hour,
            minute,
        }
        .unix_ms()
    }

    #[test]
    fn unix_epoch_and_a_known_instant_round_trip() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        let epoch = Minute::from_unix_ms(0);
        assert_eq!(
            (
                epoch.year,
                epoch.month,
                epoch.day,
                epoch.hour,
                epoch.minute,
                epoch.weekday()
            ),
            (1970, 1, 1, 0, 0, 4)
        );
        let known = Minute::from_unix_ms(1_699_999_980_000);
        assert_eq!(
            (known.year, known.month, known.day, known.hour, known.minute),
            (2023, 11, 14, 22, 13)
        );
        assert_eq!(known.unix_ms(), 1_699_999_980_000);
    }

    #[test]
    fn jobby_crons_pick_the_utc_minute() {
        let daily = Cron::parse("0 8 * * *").unwrap();
        assert_eq!(
            daily.latest_due(at(2026, 11, 5, 7, 0), at(2026, 11, 5, 8, 0) + 30_000),
            Some(at(2026, 11, 5, 8, 0))
        );
        assert_eq!(
            daily.latest_due(at(2026, 11, 5, 8, 0), at(2026, 11, 5, 8, 30)),
            None
        );

        let hourly_quarter = Cron::parse("15 * * * *").unwrap();
        assert_eq!(
            hourly_quarter.latest_due(at(2026, 11, 5, 10, 14), at(2026, 11, 5, 10, 15) + 1),
            Some(at(2026, 11, 5, 10, 15))
        );

        let every_six = Cron::parse("0 */6 * * *").unwrap();
        assert_eq!(
            every_six.latest_due(at(2026, 11, 5, 1, 0), at(2026, 11, 5, 7, 30)),
            Some(at(2026, 11, 5, 6, 0))
        );
        assert_eq!(
            every_six.next_after(at(2026, 11, 5, 7, 30)),
            Some(at(2026, 11, 5, 12, 0))
        );

        // 2026-11-05 was a Thursday. The next Monday is the 9th, and the 2nd is the previous one.
        let mondays = Cron::parse("0 0 * * 1").unwrap();
        assert_eq!(
            mondays.latest_due(at(2026, 11, 1, 0, 0), at(2026, 11, 5, 12, 0)),
            Some(at(2026, 11, 2, 0, 0))
        );
        assert_eq!(
            mondays.next_after(at(2026, 11, 5, 12, 0)),
            Some(at(2026, 11, 9, 0, 0))
        );
    }

    #[test]
    fn a_missed_run_keeps_only_the_latest_tick() {
        let hourly = Cron::parse("0 * * * *").unwrap();
        assert_eq!(
            hourly.latest_due(at(2026, 11, 5, 7, 0), at(2026, 11, 5, 10, 30)),
            Some(at(2026, 11, 5, 10, 0))
        );
    }

    #[test]
    fn impossible_expressions_are_rejected() {
        assert!(Cron::parse("").is_err());
        assert!(Cron::parse("* * *").is_err());
        assert!(Cron::parse("60 * * * *").is_err());
        assert!(Cron::parse("0 0 31 2 *").is_err());
        assert!(Cron::parse("0 0 1 * 1").is_err());
        assert!(Cron::parse("*/0 * * * *").is_err());
    }
}
