use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, Lines},
    iter::Skip,
    path::Path,
    str::FromStr,
};

use crate::FunctionId;

#[derive(Debug, ::thiserror::Error)]
pub enum SlowdownsCsvError {
    #[error("unexpected CSV format: {msg}")]
    UnexpectedCsvFormat {
        msg: Box<str>,
        #[source]
        source: Option<Box<dyn ::std::error::Error + Send + Sync + 'static>>,
    },

    #[error("failed to parse token in CSV line")]
    ParseFloatError(#[from] ::std::num::ParseFloatError),

    #[error("I/O error: {msg}")]
    Io {
        msg: Box<str>,
        #[source]
        source: io::Error,
    },
}

#[derive(Debug)]
pub struct SlowdownsCsvEntry {
    pub bench: FunctionId,
    pub sd_spmem: f64,
    pub sd_soptan: f64,
    pub sd_sflash: f64,
    pub sd_cold: f64,
}

impl FromStr for SlowdownsCsvEntry {
    type Err = SlowdownsCsvError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut tokens = s.split(',');
        Ok(Self {
            bench: FunctionId::from_str(tokens.next().ok_or_else(|| {
                SlowdownsCsvError::UnexpectedCsvFormat {
                    msg: "failed to parse FunctionId".into(),
                    source: None,
                }
            })?)
            .expect("Infallible"),
            sd_spmem: tokens
                .nth(2)
                .ok_or_else(|| SlowdownsCsvError::UnexpectedCsvFormat {
                    msg: "failed to parse sd_spmem".into(),
                    source: None,
                })?
                .parse()?,
            sd_soptan: tokens
                .next()
                .ok_or_else(|| SlowdownsCsvError::UnexpectedCsvFormat {
                    msg: "failed to parse sd_soptan".into(),
                    source: None,
                })?
                .parse()?,
            sd_sflash: tokens
                .next()
                .ok_or_else(|| SlowdownsCsvError::UnexpectedCsvFormat {
                    msg: "failed to parse sd_sflash".into(),
                    source: None,
                })?
                .parse()?,
            sd_cold: tokens
                .next()
                .ok_or_else(|| SlowdownsCsvError::UnexpectedCsvFormat {
                    msg: "failed to parse sd_cold".into(),
                    source: None,
                })?
                .parse()?,
        })
    }
}

#[cfg_attr(test, derive(Debug))]
pub struct SlowdownsCsvReadIter {
    lines: Skip<Lines<BufReader<File>>>,
}

impl SlowdownsCsvReadIter {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, SlowdownsCsvError> {
        let lines = BufReader::new(OpenOptions::new().read(true).open(&path).map_err(|err| {
            SlowdownsCsvError::Io {
                msg: format!("failed to open CSV file {:?}", path.as_ref()).into_boxed_str(),
                source: err,
            }
        })?)
        .lines()
        .skip(1); // skip header

        Ok(Self { lines })
    }
}

impl Iterator for SlowdownsCsvReadIter {
    type Item = Result<SlowdownsCsvEntry, SlowdownsCsvError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.lines.next().map(|csv_line| {
            csv_line
                .map_err(|err| SlowdownsCsvError::Io {
                    msg: "failed to read next CSV line".into(),
                    source: err,
                })?
                .parse()
        })
    }
}

pub fn read_function_ids(
    path: impl AsRef<Path>,
) -> Result<HashSet<FunctionId, crate::BuildHasher>, SlowdownsCsvError> {
    let mut ret: HashSet<FunctionId, crate::BuildHasher> = Default::default();

    for line in BufReader::new(OpenOptions::new().read(true).open(&path).map_err(|err| {
        SlowdownsCsvError::Io {
            msg: format!(
                "failed to open Function IDs' file '{}'",
                path.as_ref().display()
            )
            .into_boxed_str(),
            source: err,
        }
    })?)
    .lines()
    {
        let line = line.map_err(|err| SlowdownsCsvError::Io {
            msg: "failed to read next Function ID line".into(),
            source: err,
        })?;
        ret.insert(FunctionId::from(line.trim()));
    }

    Ok(ret)
}

#[cfg(test)]
mod tests {
    use anyhow::{Context, Result};
    use tracing::trace;
    use tracing_test::traced_test;

    use super::SlowdownsCsvReadIter;

    // Run with `-- --nocapture --ignored`
    #[ignore = "temporary; using specific paths on rootfs of icy2"]
    #[test]
    #[traced_test]
    fn sd_static_utils_01() -> Result<()> {
        let slowdowns =
            SlowdownsCsvReadIter::new("/opt/ckatsak/basenums05/runs02/slowdowns_rns_p50.csv")
                .context("failed to construct SlowdownsCsvReader")?;
        trace!(?slowdowns);

        slowdowns.for_each(|sd| trace!(?sd));

        Ok(())
    }
}
