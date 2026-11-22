//! Replays frames recorded from Hyperliquid's live `l2` feed for BTC and
//! checks the invariants the book must hold after every message.

use cross_venue_margin::book::Book;
use cross_venue_margin::venue::hyperliquid::{Event, decode};

const FRAMES: &str = include_str!("fixtures/hl_l2_btc.jsonl");

#[test]
fn recorded_session_replays_cleanly() {
    let mut book = Book::new("BTC");
    let (mut snapshots, mut diffs) = (0, 0);

    for (line_no, frame) in FRAMES.lines().enumerate() {
        match decode(frame).unwrap_or_else(|e| panic!("frame {line_no}: {e}")) {
            Event::Snapshot {
                coin,
                time,
                bids,
                asks,
            } => {
                assert_eq!(coin, "BTC");
                book.apply_snapshot(time, bids, asks).unwrap();
                snapshots += 1;
            }
            Event::Diff {
                coin,
                time,
                bids,
                asks,
            } => {
                assert_eq!(coin, "BTC");
                book.apply_diff(time, &bids, &asks)
                    .unwrap_or_else(|e| panic!("frame {line_no}: {e}"));
                diffs += 1;
            }
            _ => continue,
        }

        // Invariants after every applied message.
        assert!(book.is_live());
        assert!(
            book.bids().windows(2).all(|w| w[0].px > w[1].px),
            "bids not descending"
        );
        assert!(
            book.asks().windows(2).all(|w| w[0].px < w[1].px),
            "asks not ascending"
        );
        assert!(
            book.best_bid().unwrap().px < book.best_ask().unwrap().px,
            "crossed"
        );
        assert!(
            book.bids()
                .iter()
                .chain(book.asks())
                .all(|l| !l.sz.is_zero())
        );
        // The venue keeps this feed at a fixed depth of 20 levels per side.
        assert_eq!(book.bids().len(), 20, "frame {line_no}");
        assert_eq!(book.asks().len(), 20, "frame {line_no}");
    }

    assert_eq!(snapshots, 1);
    assert!(
        diffs > 20,
        "fixture should contain a real run of diffs, got {diffs}"
    );
}
