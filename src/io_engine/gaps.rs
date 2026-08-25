/// Represents either a contiguous run of blocks or a gap between runs.
///
/// Each variant contains a first and last position. Both bounds are inclusive,
/// in order to describe a run that reaches the last representable block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunOp {
    /// Represents a contiguous run of blocks from first (inclusive) to last (inclusive)
    Run(u64, u64),
    /// Represents a gap between runs from first (inclusive) to last (inclusive)
    Gap(u64, u64),
}

fn find_runs(blocks: &[u64], gap_threshold: u64) -> Vec<RunOp> {
    use RunOp::*;

    let mut runs: Vec<RunOp> = Vec::with_capacity(16);
    let mut current: Option<(u64, u64)> = None;

    for b in blocks {
        if let Some((begin, last)) = current {
            #[allow(clippy::comparison_chain)]
            let next = last.saturating_add(1);
            if *b > next {
                let len = b - last - 1;
                if len > gap_threshold {
                    runs.push(Run(begin, last));
                    current = Some((*b, *b));
                } else {
                    runs.push(Run(begin, last));
                    runs.push(Gap(next, *b - 1));
                    current = Some((*b, *b));
                }
            } else if *b == next {
                current = Some((begin, *b));
            } else {
                runs.push(Run(begin, last));
                current = Some((*b, *b));
            }
        } else {
            current = Some((*b, *b));
        }
    }

    if let Some((begin, last)) = current {
        runs.push(Run(begin, last));
    }

    runs
}

#[cfg(test)]
mod find_runs_tests {
    use super::*;

    use RunOp::*;

    #[test]
    fn single_run() {
        let bs = vec![1, 2, 3, 4, 5, 6, 7];

        let runs = find_runs(&bs, 0);
        assert_eq!(runs, vec![Run(1, 7)]);
    }

    #[test]
    fn two_runs() {
        let bs = vec![1, 2, 3, 5, 6, 7];

        let runs = find_runs(&bs, 0);
        assert_eq!(runs, vec![Run(1, 3), Run(5, 7)]);
    }

    #[test]
    fn three_runs() {
        let bs = vec![1, 2, 3, 5, 6, 7, 100, 101, 102];

        let runs = find_runs(&bs, 0);
        assert_eq!(runs, &[Run(1, 3), Run(5, 7), Run(100, 102)]);
    }

    #[test]
    fn large_gap() {
        let bs = vec![1, 2, 3, 5, 6, 7, 100, 101, 102];

        let runs = find_runs(&bs, 100);
        assert_eq!(
            runs,
            vec![Run(1, 3), Gap(4, 4), Run(5, 7), Gap(8, 99), Run(100, 102)]
        );
    }

    #[test]
    fn small_gap() {
        let bs = vec![1, 2, 3, 5, 6, 7, 10, 11, 12, 20, 21, 22, 23];
        let runs = find_runs(&bs, 4);
        assert_eq!(
            runs,
            vec![
                Run(1, 3),
                Gap(4, 4),
                Run(5, 7),
                Gap(8, 9),
                Run(10, 12),
                Run(20, 23)
            ]
        );
    }

    #[test]
    fn unordered() {
        let bs = vec![5, 6, 7, 1, 2, 3, 10, 11, 12, 20, 21, 22, 23];
        let runs = find_runs(&bs, 10);
        assert_eq!(
            runs,
            vec![
                Run(5, 7),
                Run(1, 3),
                Gap(4, 9),
                Run(10, 12),
                Gap(13, 19),
                Run(20, 23)
            ]
        );
    }

    #[test]
    fn singletons() {
        let bs = vec![50, 70, 10, 30, 100, 120, 210, 230];
        let runs = find_runs(&bs, 2);
        assert_eq!(
            runs,
            vec![
                Run(50, 50),
                Run(70, 70),
                Run(10, 10),
                Run(30, 30),
                Run(100, 100),
                Run(120, 120),
                Run(210, 210),
                Run(230, 230),
            ]
        );
    }

    #[test]
    fn max_value_alone() {
        let bs = vec![u64::MAX];
        let runs = find_runs(&bs, 10);
        assert_eq!(runs, vec![Run(u64::MAX, u64::MAX)]);
    }

    #[test]
    fn run_reaching_max_value() {
        let bs = vec![u64::MAX - 2, u64::MAX - 1, u64::MAX];
        let runs = find_runs(&bs, 10);
        assert_eq!(runs, vec![Run(u64::MAX - 2, u64::MAX)]);
    }

    #[test]
    fn max_value_after_gap() {
        let bs = vec![u64::MAX - 5, u64::MAX];
        let runs = find_runs(&bs, 10);
        assert_eq!(
            runs,
            vec![
                Run(u64::MAX - 5, u64::MAX - 5),
                Gap(u64::MAX - 4, u64::MAX - 1),
                Run(u64::MAX, u64::MAX),
            ]
        );
    }

    // Ensuring that u64::MAX has no successor, so the next value must start
    // a new run rather than wrapping the 'last'.
    #[test]
    fn no_wrap_past_max_value() {
        let bs = vec![u64::MAX - 1, u64::MAX, 0];
        let runs = find_runs(&bs, 10);
        assert_eq!(runs, vec![Run(u64::MAX - 1, u64::MAX), Run(0, 0),]);
    }

    #[test]
    fn break_by_max_value() {
        let bs = vec![0, 1, u64::MAX, 2, 3];
        let runs = find_runs(&bs, 10);
        assert_eq!(runs, vec![Run(0, 1), Run(u64::MAX, u64::MAX), Run(2, 3)]);
    }
}

//-----------------------------------------

fn batch_adjacent(runs: &[RunOp]) -> Batches {
    use RunOp::*;

    let mut result: Vec<Vec<RunOp>> = Vec::new();
    let mut batch: Vec<RunOp> = Vec::new();
    let mut last: Option<u64> = None;

    for r in runs {
        match (last, r) {
            (None, Run(b, e)) => {
                batch.push(Run(*b, *e));
                last = Some(*e);
            }
            (None, Gap(b, e)) => {
                batch.push(Gap(*b, *e));
                last = Some(*e);
            }
            (Some(l), Run(b, e)) => {
                if *b == l {
                    batch.push(Run(*b, *e));
                } else {
                    result.push(std::mem::take(&mut batch));
                    batch.push(Run(*b, *e));
                }
                last = Some(*e);
            }
            (Some(l), Gap(b, e)) => {
                if *b == l {
                    batch.push(Gap(*b, *e));
                } else {
                    let mut tmp = Vec::new();
                    std::mem::swap(&mut tmp, &mut batch);
                    result.push(tmp);
                    batch.push(Gap(*b, *e));
                }
                last = Some(*e);
            }
        }
    }

    if !batch.is_empty() {
        result.push(batch);
    }

    result
}

#[cfg(test)]
mod batch_adjacent_tests {
    use super::*;

    use RunOp::*;

    #[test]
    fn batch_zero() {
        let runs = vec![];
        let batches = batch_adjacent(&runs);
        assert!(batches.is_empty());
    }

    #[test]
    fn batch_one() {
        let runs = vec![Run(1, 5), Gap(5, 8), Run(8, 100)];
        let batches = batch_adjacent(&runs);
        assert_eq!(batches, vec![vec![Run(1, 5), Gap(5, 8), Run(8, 100)]]);
    }

    #[test]
    fn batch_two() {
        let runs = vec![
            Run(1, 5),
            Gap(5, 8),
            Run(8, 100),
            Run(500, 501),
            Gap(501, 513),
            Run(513, 600),
        ];
        let batches = batch_adjacent(&runs);
        assert_eq!(
            batches,
            vec![
                vec![Run(1, 5), Gap(5, 8), Run(8, 100)],
                vec![Run(500, 501), Gap(501, 513), Run(513, 600)]
            ]
        );
    }

    #[test]
    fn singletons() {
        let runs = vec![
            Run(50, 51),
            Run(70, 71),
            Run(10, 11),
            Run(30, 31),
            Run(100, 101),
            Run(120, 121),
            Run(210, 211),
            Run(230, 231),
        ];
        let batches = batch_adjacent(&runs);
        assert_eq!(
            batches,
            vec![
                vec![Run(50, 51)],
                vec![Run(70, 71)],
                vec![Run(10, 11)],
                vec![Run(30, 31)],
                vec![Run(100, 101)],
                vec![Run(120, 121)],
                vec![Run(210, 211)],
                vec![Run(230, 231)],
            ]
        );
    }
}

//-----------------------------------------

// Returns the remainder
fn split_op(op: &RunOp, remaining: u64) -> (RunOp, Option<RunOp>, u64) {
    use RunOp::*;

    let (b, e) = match op {
        Run(b, e) | Gap(b, e) => (*b, *e),
    };

    // Test (e - b + 1 <= remaining) as (e - b < remaining) to prevent overflow,
    // although a RunOp's length cannot exceed u64::MAX since it's constructed
    // from a slice.
    if e - b < remaining {
        return (op.clone(), None, remaining - (e - b) - 1);
    }

    let split = b + remaining;
    match op {
        Run(..) => (Run(b, split - 1), Some(Run(split, e)), 0),
        Gap(..) => (Gap(b, split - 1), Some(Gap(split, e)), 0),
    }
}

fn split_contiguous(runs: Vec<RunOp>, max: u64, result: &mut Batches) {
    let mut remaining = max;

    let mut batch = Vec::new();
    let mut ops = runs.into_iter();
    let mut op = ops.next();

    while op.is_some() {
        let (first, rest, rem) = split_op(op.as_ref().unwrap(), remaining);
        batch.push(first);
        op = rest;
        remaining = rem;

        if remaining == 0 {
            result.push(std::mem::take(&mut batch));
            remaining = max;
        }

        if op.is_none() {
            op = ops.next();
        }
    }

    if !batch.is_empty() {
        result.push(batch);
    }
}

/// A type alias representing a collection of batches, where each batch is a vector of `RunOp`s.
/// Batches are used to group related runs and gaps together for processing.
type Batches = Vec<Vec<RunOp>>;

fn split_batches(batches: Batches, max: u64) -> Batches {
    let mut result = Vec::new();

    for b in batches {
        split_contiguous(b, max, &mut result);
    }

    result
}

#[cfg(test)]
mod split_tests {
    use super::*;
    use RunOp::*;

    #[test]
    fn single() {
        let runs = vec![vec![Run(1, 99)]];
        let batches = split_batches(runs, 21);
        assert_eq!(
            batches,
            [
                [Run(1, 21)],
                [Run(22, 42)],
                [Run(43, 63)],
                [Run(64, 84)],
                [Run(85, 99)]
            ]
        );
    }

    #[test]
    fn singletons() {
        let runs = vec![
            vec![Run(50, 50)],
            vec![Run(70, 70)],
            vec![Run(10, 10)],
            vec![Run(30, 30)],
            vec![Run(100, 100)],
            vec![Run(120, 120)],
            vec![Run(210, 210)],
            vec![Run(230, 230)],
        ];
        let runs_copy = runs.clone();
        let batches = split_batches(runs, 100);
        assert_eq!(batches, runs_copy);
    }
}

//-----------------------------------------

/// Generates batches of runs from a list of block numbers, considering gaps and maximum batch sizes.
///
/// # Arguments
///
/// * `blocks` - A slice of block numbers to process
/// * `gap_threshold` - The maximum size of gap that will be included in a run rather than split
/// * `max` - The maximum size of a single run batch
///
/// # Returns
///
/// Returns a `Batches` containing vectors of `RunOp`s representing the runs and gaps
pub fn generate_runs(blocks: &[u64], gap_threshold: u64, max: u64) -> Batches {
    split_batches(batch_adjacent(&find_runs(blocks, gap_threshold)), max)
}

/// Counts the total number of gaps across all batches.
///
/// # Arguments
///
/// * `batches` - A reference to a `Batches` containing runs and gaps
///
/// # Returns
///
/// Returns the total number of gaps found across all batches
pub fn count_gaps(batches: &Batches) -> u64 {
    let mut count = 0;
    for batch in batches {
        for op in batch {
            match op {
                RunOp::Gap(b, e) => {
                    // The maximum length of a gap is (u64::MAX - 1),
                    count += e - b + 1;
                }
                RunOp::Run(..) => {
                    // do nothing
                }
            }
        }
    }

    count
}

//-----------------------------------------
