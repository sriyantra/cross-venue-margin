//! Signing for Hyperliquid testnet orders.
//!
//! An order is signed in three steps. The action is encoded as MessagePack
//! and hashed with the nonce. That hash goes into an EIP-712 `Agent`
//! message. The message is signed with the wallet's secp256k1 key.
//!
//! Everything here is pure. The chain is fixed to testnet: the `Agent`
//! source is always `b`, so a signature made here does not recover to this
//! wallet on mainnet. An order sent there by mistake is not this wallet's.

use k256::ecdsa::SigningKey;
use serde::Serialize;
use sha3::{Digest, Keccak256};
use thiserror::Error;

/// The `Agent` source that marks a testnet action.
const TESTNET_SOURCE: &str = "b";
/// The chain id Hyperliquid uses in the domain of every order signature.
const DOMAIN_CHAIN_ID: u64 = 1337;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SignError {
    #[error("the key must be 32 bytes of hex")]
    InvalidKey,
    #[error("the action did not encode: {0}")]
    Encode(String),
}

/// One order as the venue's wire format spells it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OrderWire {
    /// The asset's index in the venue's universe.
    pub a: u32,
    /// True for a buy.
    pub b: bool,
    /// Limit price.
    pub p: String,
    /// Size.
    pub s: String,
    /// Reduce only.
    pub r: bool,
    pub t: OrderType,
    /// The sender's own id for the order, which the venue keeps with it.
    pub c: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OrderType {
    pub limit: Limit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Limit {
    pub tif: &'static str,
}

/// The `order` action. Field order matters: the venue hashes the same
/// MessagePack bytes, and MessagePack keeps the order fields were written.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OrderAction {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub orders: Vec<OrderWire>,
    pub grouping: &'static str,
}

impl OrderAction {
    /// One order that fills what it can at once and cancels the rest.
    /// `order` is the sender's id for it.
    pub fn immediate(asset: u32, buy: bool, px: String, size: String, order: u64) -> Self {
        Self {
            kind: "order",
            orders: vec![OrderWire {
                a: asset,
                b: buy,
                p: px,
                s: size,
                r: false,
                t: OrderType {
                    limit: Limit { tif: "Ioc" },
                },
                c: client_id(order),
            }],
            grouping: "na",
        }
    }
}

/// An order id the way the venue wants a client's id written: 16 bytes
/// of hex.
pub fn client_id(order: u64) -> String {
    format!("0x{order:032x}")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Signature {
    pub r: String,
    pub s: String,
    pub v: u8,
}

fn keccak(parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize().into()
}

fn word(value: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[24..].copy_from_slice(&value.to_be_bytes());
    out
}

/// The hash the venue calls the connection id: the encoded action, the
/// nonce, and one zero byte that says no vault is involved.
pub fn action_hash(action: &OrderAction, nonce: u64) -> Result<[u8; 32], SignError> {
    let packed = rmp_serde::to_vec_named(action).map_err(|e| SignError::Encode(e.to_string()))?;
    Ok(keccak(&[&packed, &nonce.to_be_bytes(), &[0u8]]))
}

/// The EIP-712 digest of an `Agent` message carrying `connection_id`.
fn digest(connection_id: &[u8; 32]) -> [u8; 32] {
    let domain_type = keccak(&[
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    ]);
    let domain = keccak(&[
        &domain_type,
        &keccak(&[b"Exchange"]),
        &keccak(&[b"1"]),
        &word(DOMAIN_CHAIN_ID),
        &[0u8; 32],
    ]);
    let agent_type = keccak(&[b"Agent(string source,bytes32 connectionId)"]);
    let agent = keccak(&[
        &agent_type,
        &keccak(&[TESTNET_SOURCE.as_bytes()]),
        connection_id,
    ]);
    keccak(&[&[0x19, 0x01], &domain, &agent])
}

/// A wallet that can sign testnet orders.
pub struct Wallet {
    key: SigningKey,
}

impl Wallet {
    /// Reads a private key written as 64 hex digits, with or without `0x`.
    pub fn from_hex(key: &str) -> Result<Self, SignError> {
        let bytes =
            hex::decode(key.trim().trim_start_matches("0x")).map_err(|_| SignError::InvalidKey)?;
        let key = SigningKey::from_slice(&bytes).map_err(|_| SignError::InvalidKey)?;
        Ok(Self { key })
    }

    /// The wallet's address, lower case with `0x`.
    pub fn address(&self) -> String {
        let point = self.key.verifying_key().to_sec1_point(false);
        let hash = keccak(&[&point.as_bytes()[1..]]);
        format!("0x{}", hex::encode(&hash[12..]))
    }

    pub fn sign(&self, action: &OrderAction, nonce: u64) -> Result<Signature, SignError> {
        let digest = digest(&action_hash(action, nonce)?);
        let (signature, recovery) = self.key.sign_prehash_recoverable(&digest);
        let bytes = signature.to_bytes();
        Ok(Signature {
            r: format!("0x{}", hex::encode(&bytes[..32])),
            s: format!("0x{}", hex::encode(&bytes[32..])),
            v: 27 + recovery.to_byte(),
        })
    }
}

impl std::fmt::Debug for Wallet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wallet")
            .field("address", &self.address())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The expected values below come from the venue's reference client,
    // given the same key, action and nonce.
    const KEY: &str = "0x0101010101010101010101010101010101010101010101010101010101010101";
    const NONCE: u64 = 1_700_000_000_000;

    fn action() -> OrderAction {
        OrderAction::immediate(3, true, "64560".into(), "0.001".into(), NONCE)
    }

    #[test]
    fn address_matches_the_reference_client() {
        let wallet = Wallet::from_hex(KEY).unwrap();
        assert_eq!(
            wallet.address(),
            "0x1a642f0e3c3af545e7acbd38b07251b3990914f1"
        );
    }

    #[test]
    fn action_encodes_to_the_reference_bytes() {
        let packed = rmp_serde::to_vec_named(&action()).unwrap();
        assert_eq!(
            hex::encode(packed),
            "83a474797065a56f72646572a66f72646572739187a16103a162c3a170a53634353630\
             a173a5302e303031a172c2a17481a56c696d697481a3746966a3496f63a163d9223078\
             3030303030303030303030303030303030303030303138626366653536383030a86772\
             6f7570696e67a26e61"
        );
    }

    #[test]
    fn action_hash_matches_the_reference_client() {
        assert_eq!(
            hex::encode(action_hash(&action(), NONCE).unwrap()),
            "193d76ccff859834f92fd26e19db78a6decd4191f2b1cea0280f57baff1fb872"
        );
    }

    #[test]
    fn signature_matches_the_reference_client() {
        let signature = Wallet::from_hex(KEY)
            .unwrap()
            .sign(&action(), NONCE)
            .unwrap();
        assert_eq!(
            signature,
            Signature {
                r: "0xf9fab368b34da14edd9b8c43a2ad956993e1f245cb79f19cf249184a4d6bd70f".into(),
                s: "0x133a3cddb9bbd74e3836ba840a8fc1b2ade9ec922be41323bbfb06bc1dffa00d".into(),
                v: 27,
            }
        );
    }

    #[test]
    fn a_bad_key_is_refused() {
        assert_eq!(
            Wallet::from_hex("0x1234").unwrap_err(),
            SignError::InvalidKey
        );
        assert_eq!(
            Wallet::from_hex("not hex").unwrap_err(),
            SignError::InvalidKey
        );
    }

    #[test]
    fn debug_output_does_not_show_the_key() {
        let shown = format!("{:?}", Wallet::from_hex(KEY).unwrap());
        assert!(!shown.contains("0101010101"));
    }
}
