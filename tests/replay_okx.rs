//! Replays frames recorded from OKX's live `books` channel for
//! BTC-USDT-SWAP and checks the sequence chain and the invariants the book
//! must hold after every message.

use cross_venue_margin::book::{Book, SideDiff};
use cross_venue_margin::venue::okx::{Event, decode, in_coin, instrument};

const FRAMES: &str = include_str!("fixtures/okx_books_btc.jsonl");

#[test]
fn recorded_session_replays_cleanly() {
    let contract_value = instrument("BTC").unwrap().contract_value;
    let mut book = Book::new("BTC");
    let mut last_seq = None;
    let (mut snapshots, mut updates) = (0, 0);

    for (line_no, frame) in FRAMES.lines().enumerate() {
        match decode(frame).unwrap_or_else(|e| panic!("frame {line_no}: {e}")) {
            Event::Snapshot {
                inst_id,
                time,
                seq,
                bids,
                asks,
            } => {
                assert_eq!(inst_id, "BTC-USDT-SWAP");
                assert_eq!((bids.len(), asks.len()), (400, 400));
                book.apply_snapshot(
                    time,
                    in_coin(bids, contract_value),
                    in_coin(asks, contract_value),
                )
                .unwrap();
                last_seq = Some(seq);
                snapshots += 1;
            }
            Event::Update {
                time,
                seq,
                prev_seq,
                bids,
                asks,
                ..
            } => {
                // Every update must name the message before it.
                assert_eq!(Some(prev_seq), last_seq, "frame {line_no}: sequence gap");
                let diff = |upserts| SideDiff {
                    removed: Vec::new(),
                    upserts,
                };
                book.apply_diff(
                    time,
                    &diff(in_coin(bids, contract_value)),
                    &diff(in_coin(asks, contract_value)),
                )
                .unwrap_or_else(|e| panic!("frame {line_no}: {e}"));
                last_seq = Some(seq);
                updates += 1;
            }
            _ => continue,
        }

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
        // The venue keeps this feed at 400 levels per side.
        assert_eq!(book.bids().len(), 400, "frame {line_no}");
        assert_eq!(book.asks().len(), 400, "frame {line_no}");
    }

    assert_eq!(snapshots, 1);
    assert_eq!(updates, 120);
}
